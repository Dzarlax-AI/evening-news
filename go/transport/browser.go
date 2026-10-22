package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp/remote"
)

const MobileUserAgent = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"

var ErrBrowserEventOverflow = errors.New("prototype_browser_event_overflow")
var ErrNativeOwnershipLost = errors.New("native_browser_ownership_lost")

// detachedOwnedSession distinguishes the session being detached in the event
// params from the envelope session on which Chrome delivered that event.
func detachedOwnedSession(message *cdproto.Message, owned string) (bool, bool) {
	if message == nil || string(message.Method) != "Target.detachedFromTarget" {
		return false, false
	}
	envelope := string(message.SessionID)
	if envelope != "" && envelope != owned {
		return false, false
	}
	var event struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(message.Params, &event); err != nil || event.SessionID == "" {
		return false, true
	}
	return event.SessionID == owned, false
}

// Child targets cannot share the single session's interception or byte budget.
// Apply this additional CSP before parsing rather than replacing page globals.
const browserIsolationPolicy = "connect-src http: https:; worker-src 'none'; frame-src 'none'; sandbox allow-scripts allow-same-origin"

// BrowserOptions can only tighten the isolated prototype's fixed limits.
type BrowserOptions struct {
	Timeout      time.Duration
	MaxWireBytes int64
	MaxDOMBytes  int64
	MinSettle    time.Duration
}

// RenderedDocument preserves metadata, attributes and script-rendered HTML.
type RenderedDocument struct {
	Document
	DOMBytes int
}

// BrowserFetch connects to an existing loopback debugging endpoint. It creates
// exactly one owned tab in a disposable context, and never closes the browser.
// HTTP(S) requests are restricted to loopback. An additional response CSP blocks
// WebSockets, workers, frames and popups, which cannot share the owned session's
// interception and byte accounting. This adapter is for controlled fixtures.
func BrowserFetch(ctx context.Context, endpoint, rawURL string, opts BrowserOptions) (doc RenderedDocument, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return doc, err
	}
	if err = validateLoopbackURL(u); err != nil {
		return doc, err
	}
	wireLimit, err := pageLimit(opts.MaxWireBytes)
	if err != nil {
		return doc, err
	}
	domLimit, err := pageLimit(opts.MaxDOMBytes)
	if err != nil {
		return doc, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	if timeout < 0 || timeout > 30*time.Second || opts.MinSettle < 0 || opts.MinSettle > 5*time.Second {
		return doc, errors.New("prototype_browser_invalid_timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	wsURL, err := browserEndpoint(ctx, endpoint)
	if err != nil {
		return doc, err
	}
	client, err := dialBrowser(ctx, wsURL)
	if err != nil {
		return doc, err
	}
	var browserContextID, targetID, sessionID string
	// Cleanup deliberately outlives caller cancellation. DisposeOnDetach also
	// covers a lost CreateTarget reply or a broken debugging connection.
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		cleanupErr := disposeOwnedBrowser(cleanup, client, browserContextID, targetID, sessionID)
		client.close()
		cleanupErr = errors.Join(cleanupErr, client.eventError())
		if cleanupErr != nil {
			doc = RenderedDocument{}
			err = errors.Join(err, fmt.Errorf("prototype_browser_cleanup: %w", cleanupErr))
		}
	}()
	// Setup requests use a short independent context so an outer cancellation
	// cannot discard the IDs of successfully created browser resources.
	setup, setupDone := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer setupDone()
	var createdContext struct {
		BrowserContextID string `json:"browserContextId"`
	}
	if err = client.callAllocation(setup, "", "Target.createBrowserContext", map[string]any{"disposeOnDetach": true}, &createdContext); err != nil {
		return doc, err
	}
	browserContextID = createdContext.BrowserContextID
	var createdTarget struct {
		TargetID string `json:"targetId"`
	}
	if err = client.callAllocation(setup, "", "Target.createTarget", map[string]any{"url": "about:blank", "browserContextId": browserContextID}, &createdTarget); err != nil {
		return doc, err
	}
	targetID = createdTarget.TargetID
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err = client.callAllocation(setup, "", "Target.attachToTarget", map[string]any{"targetId": targetID, "flatten": true}, &attached); err != nil {
		return doc, err
	}
	sessionID = attached.SessionID
	// Avoid chromedp.Run: its DOM domain automatically requests an entire node
	// tree before a size guard can run. This adapter uses only scalar CDP calls.
	for _, command := range []struct {
		method string
		params any
	}{
		{"Network.enable", map[string]any{}},
		// Defense in depth only: Chrome's Network URL filter does not reliably
		// block WebSocket handshakes. The response CSP below enforces that rule.
		{"Network.setBlockedURLs", map[string]any{"urls": []string{"ws://*", "wss://*"}, "urlPatterns": []any{
			map[string]any{"urlPattern": "ws://*:*/*", "block": true},
			map[string]any{"urlPattern": "wss://*:*/*", "block": true},
		}}},
		{"Network.setCacheDisabled", map[string]any{"cacheDisabled": true}},
		{"Network.setBypassServiceWorker", map[string]any{"bypass": true}},
		{"Network.setUserAgentOverride", map[string]any{"userAgent": MobileUserAgent}},
		{"Page.enable", map[string]any{}},
		{"Fetch.enable", map[string]any{"patterns": []any{
			map[string]any{"urlPattern": "*", "requestStage": "Request"},
			map[string]any{"urlPattern": "*", "requestStage": "Response"},
		}}},
	} {
		if err = client.call(ctx, sessionID, command.method, command.params, nil); err != nil {
			return doc, err
		}
	}
	work, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var responseStatus atomic.Int64
	eventsDone := make(chan struct{})
	defer func() { stop(nil); <-eventsDone }()
	go func() {
		defer close(eventsDone)
		var wireBytes float64
		networkRequests := make(map[string]bool)
		for {
			select {
			case <-work.Done():
				return
			case <-client.eventFailure:
				stop(client.eventError())
				return
			case message := <-client.events:
				if detached, malformed := detachedOwnedSession(message, sessionID); malformed {
					stop(ErrNativePolicy)
					return
				} else if detached {
					stop(ErrNativeOwnershipLost)
					return
				}
				if string(message.SessionID) != sessionID {
					continue
				}
				switch string(message.Method) {
				case "Network.requestWillBeSent":
					var request struct {
						RequestID string `json:"requestId"`
						Request   struct {
							URL string `json:"url"`
						} `json:"request"`
					}
					if json.Unmarshal(message.Params, &request) == nil {
						requestURL, parseErr := url.Parse(request.Request.URL)
						networkRequests[request.RequestID] = parseErr == nil && validateLoopbackURL(requestURL) == nil
					}
				case "Fetch.requestPaused":
					var request struct {
						RequestID          string `json:"requestId"`
						ResourceType       string `json:"resourceType"`
						ResponseStatusCode int    `json:"responseStatusCode"`
						ResponseHeaders    []struct {
							Name  string `json:"name"`
							Value string `json:"value"`
						} `json:"responseHeaders"`
						Request struct {
							URL string `json:"url"`
						} `json:"request"`
					}
					if decodeErr := json.Unmarshal(message.Params, &request); decodeErr != nil {
						stop(decodeErr)
						return
					}
					requestURL, parseErr := url.Parse(request.Request.URL)
					allowed := parseErr == nil && validateLoopbackURL(requestURL) == nil
					blocked := request.ResourceType == "Image" || request.ResourceType == "Media" || request.ResourceType == "Font"
					var rejection error
					for _, header := range request.ResponseHeaders {
						if strings.EqualFold(header.Name, "Content-Length") {
							if size, parseErr := strconv.ParseInt(header.Value, 10, 64); parseErr == nil && size > wireLimit {
								rejection = extractor.ErrWireLimit
							}
						}
					}
					if request.ResourceType == "Document" && request.ResponseStatusCode != 0 && (request.ResponseStatusCode < 300 || request.ResponseStatusCode >= 400) {
						responseStatus.Store(int64(request.ResponseStatusCode))
						if request.ResponseStatusCode != http.StatusOK {
							rejection = fmt.Errorf("prototype_browser_status: %d", request.ResponseStatusCode)
						}
					}
					method := "Fetch.continueRequest"
					params := map[string]any{"requestId": request.RequestID}
					if !allowed || blocked || rejection != nil {
						method = "Fetch.failRequest"
						params["errorReason"] = "BlockedByClient"
					} else if request.ResourceType == "Document" && request.ResponseStatusCode == http.StatusOK {
						// Preserve existing headers and CSP; an additional CSP header
						// can only tighten the policy before any page script runs.
						headers := make([]any, 0, len(request.ResponseHeaders)+1)
						for _, header := range request.ResponseHeaders {
							// The response stream is decoded by Chrome. Fulfill with
							// explicit bytes and a fresh length to apply the CSP before
							// parsing; header-only continuation is ignored by Chrome154.
							if !strings.EqualFold(header.Name, "Content-Length") && !strings.EqualFold(header.Name, "Content-Encoding") && !strings.EqualFold(header.Name, "Transfer-Encoding") {
								headers = append(headers, header)
							}
						}
						body, bodyErr := browserResponseBody(work, client, sessionID, request.RequestID, wireLimit)
						if bodyErr != nil {
							stop(bodyErr)
							return
						}
						headers = append(headers, map[string]string{"name": "Content-Security-Policy", "value": browserIsolationPolicy})
						headers = append(headers, map[string]string{"name": "Content-Length", "value": strconv.Itoa(len(body))})
						method = "Fetch.fulfillRequest"
						params["responseCode"] = request.ResponseStatusCode
						params["responseHeaders"] = headers
						params["body"] = base64.StdEncoding.EncodeToString(body)
					}
					if callErr := client.call(work, sessionID, method, params, nil); callErr != nil {
						stop(callErr)
						return
					}
					if !allowed {
						stop(ErrNonLoopback)
						return
					}
					if rejection != nil {
						stop(rejection)
						return
					}
				case "Network.dataReceived":
					var data struct {
						RequestID         string  `json:"requestId"`
						EncodedDataLength float64 `json:"encodedDataLength"`
					}
					if json.Unmarshal(message.Params, &data) == nil && networkRequests[data.RequestID] {
						wireBytes += data.EncodedDataLength
						if wireBytes > float64(wireLimit) {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Network.loadingFinished":
					var data struct {
						RequestID         string  `json:"requestId"`
						EncodedDataLength float64 `json:"encodedDataLength"`
					}
					if json.Unmarshal(message.Params, &data) == nil && networkRequests[data.RequestID] && data.EncodedDataLength > float64(wireLimit) {
						stop(extractor.ErrWireLimit)
						return
					}
				}
			}
		}
	}()
	var navigation struct {
		ErrorText string `json:"errorText"`
	}
	if err = client.call(work, sessionID, "Page.navigate", map[string]any{"url": u.String()}, &navigation); err != nil {
		return doc, browserClientCause(work, client, err)
	}
	if navigation.ErrorText != "" {
		if cause := context.Cause(work); cause != nil {
			return doc, cause
		}
		return doc, fmt.Errorf("prototype_browser_navigation: %s", navigation.ErrorText)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	started, stableSince := time.Now(), time.Now()
	previousHTML := ""
	for {
		// Both guards execute in Chrome before HTML is transferred. The UTF-16
		// character count avoids encoding a huge DOM; TextEncoder enforces the
		// actual UTF-8 byte bound, including multi-byte text. Check these before
		// innerText, which can force expensive layout of an oversized text node.
		expression := fmt.Sprintf(`(() => {
 if (!document.body || document.readyState !== 'complete') return {ready:false};
 if (document.body.textContent.length > %d) return {ready:true,tooLarge:true};
 const html = document.documentElement.outerHTML;
 if (html.length > %d) return {ready:true,tooLarge:true};
 const bytes = new TextEncoder().encode(html).length;
 if (bytes > %d) return {ready:true,tooLarge:true};
 const wire = [...performance.getEntriesByType('navigation'), ...performance.getEntriesByType('resource')].reduce((sum, entry) => sum + entry.encodedBodySize, 0);
 if (wire > %d) return {ready:true,wireTooLarge:true};
 if (!document.body.innerText.trim()) return {ready:false};
 return {ready:true,html:html,bytes:bytes,url:location.href};
})()`, domLimit, domLimit, domLimit, wireLimit)
		var evaluation struct {
			Result struct {
				Value struct {
					Ready        bool   `json:"ready"`
					TooLarge     bool   `json:"tooLarge"`
					WireTooLarge bool   `json:"wireTooLarge"`
					HTML         string `json:"html"`
					Bytes        int    `json:"bytes"`
					URL          string `json:"url"`
				} `json:"value"`
			} `json:"result"`
			ExceptionDetails json.RawMessage `json:"exceptionDetails"`
		}
		if err = client.call(work, sessionID, "Runtime.evaluate", map[string]any{"expression": expression, "returnByValue": true}, &evaluation); err != nil {
			return doc, browserClientCause(work, client, err)
		}
		if len(evaluation.ExceptionDetails) > 0 {
			return doc, errors.New("prototype_browser_evaluation")
		}
		value := evaluation.Result.Value
		if value.Ready {
			if cause := client.eventError(); cause != nil {
				return doc, cause
			}
			if value.WireTooLarge {
				return doc, extractor.ErrWireLimit
			}
			if value.TooLarge || int64(value.Bytes) > domLimit || int64(len(value.HTML)) > domLimit {
				return doc, extractor.ErrOutputLimit
			}
			if cause := context.Cause(work); cause != nil {
				return doc, cause
			}
			if opts.MinSettle > 0 {
				if value.HTML != previousHTML {
					previousHTML = value.HTML
					stableSince = time.Now()
				}
				if time.Since(started) < max(500*time.Millisecond, opts.MinSettle) || time.Since(stableSince) < 250*time.Millisecond {
					select {
					case <-work.Done():
						return doc, context.Cause(work)
					case <-ticker.C:
					}
					continue
				}
			}
			return RenderedDocument{Document: Document{HTML: []byte(value.HTML), StatusCode: int(responseStatus.Load()), Encoding: "utf-8", URL: value.URL}, DOMBytes: value.Bytes}, nil
		}
		select {
		case <-work.Done():
			return doc, context.Cause(work)
		case <-ticker.C:
		}
	}
}

func browserCause(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return err
}

func browserClientCause(ctx context.Context, client *browserCDP, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if cause := client.eventError(); cause != nil {
		return cause
	}
	return err
}

// Read the intercepted document in bounded chunks. getResponseBody would return
// an unbounded CDP message, before Go could reject an oversized source document.
func browserResponseBody(ctx context.Context, client *browserCDP, sessionID, requestID string, limit int64) ([]byte, error) {
	var stream struct {
		Stream string `json:"stream"`
	}
	if err := client.call(ctx, sessionID, "Fetch.takeResponseBodyAsStream", map[string]any{"requestId": requestID}, &stream); err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.callCleanup(cleanup, sessionID, "IO.close", map[string]any{"handle": stream.Stream}, nil)
	}()
	var body []byte
	for {
		var chunk struct {
			Data          string `json:"data"`
			Base64Encoded bool   `json:"base64Encoded"`
			EOF           bool   `json:"eof"`
		}
		if err := client.call(ctx, sessionID, "IO.read", map[string]any{"handle": stream.Stream, "size": min(int64(64*1024), limit-int64(len(body))+1)}, &chunk); err != nil {
			return nil, err
		}
		data := []byte(chunk.Data)
		if chunk.Base64Encoded {
			var err error
			data, err = base64.StdEncoding.DecodeString(chunk.Data)
			if err != nil {
				return nil, err
			}
		}
		if int64(len(body))+int64(len(data)) > limit {
			return nil, extractor.ErrWireLimit
		}
		body = append(body, data...)
		if chunk.EOF {
			return body, nil
		}
	}
}

func browserEndpoint(ctx context.Context, endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if u.Scheme == "ws" || u.Scheme == "wss" {
		copyURL := *u
		copyURL.Scheme = "http"
		if err := validateLoopbackURL(&copyURL); err != nil {
			return "", err
		}
		if !strings.HasPrefix(u.Path, "/devtools/browser/") {
			return "", errors.New("prototype_browser_invalid_endpoint")
		}
		return u.String(), nil
	}
	if err := validateLoopbackURL(u); err != nil {
		return "", err
	}
	u.Path, u.RawQuery, u.Fragment = "/json/version", "", ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	tr := &http.Transport{Proxy: nil, DialContext: dialLoopback}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("prototype_browser_endpoint_redirect") }}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("prototype_browser_endpoint_status: %d", response.StatusCode)
	}
	body, err := readBounded(response.Body, 64*1024, extractor.ErrWireLimit)
	if err != nil {
		return "", err
	}
	var version struct {
		URL string `json:"webSocketDebuggerUrl"`
	}
	if err := json.Unmarshal(body, &version); err != nil {
		return "", err
	}
	wsURL, err := url.Parse(version.URL)
	if err != nil || (wsURL.Scheme != "ws" && wsURL.Scheme != "wss") {
		return "", errors.New("prototype_browser_invalid_endpoint")
	}
	return browserEndpoint(ctx, version.URL)
}

// browserCDP is a small session-aware protocol connection. Keeping the protocol
// scalar avoids automatic DOM materialization and attaching to foreign targets.
type browserCDP struct {
	conn         *remote.Conn
	mu           sync.Mutex
	next         int64
	pending      map[int64]chan *cdproto.Message
	writes       chan browserCDPWrite
	writerDone   chan struct{}
	events       chan *cdproto.Message
	eventFailure chan struct{}
	done         chan struct{}
	readErr      error
	eventErr     error
}

type browserCDPWrite struct {
	ctx     context.Context
	msg     *cdproto.Message
	result  chan error
	cleanup bool
}

// browserEventNeeded is the union consumed by BrowserFetch and NativeBrowser.
// Filter before enqueueing so unused domain notifications cannot crowd out
// policy, resource accounting, frame ownership or browser failure events.
func browserEventNeeded(method cdproto.MethodType) bool {
	switch string(method) {
	case "Browser.downloadWillBegin",
		"Page.frameAttached", "Page.frameNavigated", "Page.frameDetached", "Page.windowOpen",
		"Fetch.authRequired", "Fetch.requestPaused",
		"Network.requestWillBeSent", "Network.loadingFinished", "Network.loadingFailed",
		"Network.dataReceived", "Network.responseReceived", "Network.webSocketCreated",
		"Target.attachedToTarget", "Target.detachedFromTarget", "Inspector.targetCrashed":
		return true
	default:
		return false
	}
}

func dialBrowser(ctx context.Context, endpoint string) (*browserCDP, error) {
	conn, err := remote.DialContext(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	c := &browserCDP{conn: conn, pending: make(map[int64]chan *cdproto.Message), writes: make(chan browserCDPWrite, 1), writerDone: make(chan struct{}), events: make(chan *cdproto.Message, 256), eventFailure: make(chan struct{}), done: make(chan struct{})}
	go c.writeLoop()
	go func() {
		defer close(c.done)
		for {
			message := new(cdproto.Message)
			if err := conn.Read(context.Background(), message); err != nil {
				c.mu.Lock()
				c.readErr = err
				c.mu.Unlock()
				return
			}
			if message.ID != 0 {
				c.mu.Lock()
				reply := c.pending[message.ID]
				c.mu.Unlock()
				if reply != nil {
					select {
					case reply <- message:
					default: // Duplicate or late reply; never block the shared reader.
					}
				}
			} else {
				if !browserEventNeeded(message.Method) {
					continue
				}
				c.mu.Lock()
				overloaded := c.eventErr != nil
				c.mu.Unlock()
				if overloaded {
					continue
				}
				// Never block replies behind unsolicited browser events.
				select {
				case c.events <- message:
				default:
					c.mu.Lock()
					if c.eventErr == nil {
						c.eventErr = ErrBrowserEventOverflow
						close(c.eventFailure)
					}
					c.mu.Unlock()
				}
			}
		}
	}()
	return c, nil
}

func (c *browserCDP) call(ctx context.Context, session, method string, params, result any) error {
	return c.callMode(ctx, session, method, params, result, false)
}

// callAllocation lets an already-started allocation finish decoding its ID
// after an event overload, while refusing to start later allocation steps.
func (c *browserCDP) callAllocation(ctx context.Context, session, method string, params, result any) error {
	if !browserAllocationMethod(method) {
		return errors.New("prototype_browser_invalid_allocation_command")
	}
	return c.callMode(ctx, session, method, params, result, true)
}

// callCleanup is the only command path available after policy processing has
// failed. Its allowlist contains resource stop/disposal, receipts, and existing
// stream/target cleanup operations only.
func (c *browserCDP) callCleanup(ctx context.Context, session, method string, params, result any) error {
	if !browserCleanupMethod(method) {
		return errors.New("prototype_browser_cleanup_command_denied")
	}
	stopClose := context.AfterFunc(ctx, func() { _ = c.conn.Close() })
	defer stopClose()
	return c.callMode(ctx, session, method, params, result, true)
}

// writeLoop is the connection's only CDP writer. A one-entry queue bounds
// admitted writes while allowing callMode to return promptly on cancellation.
// A cleanup deadline closes the shared connection and releases a stalled Write.
func (c *browserCDP) writeLoop() {
	defer close(c.writerDone)
	for {
		select {
		case <-c.done:
			return
		case request := <-c.writes:
			if err := request.ctx.Err(); err != nil {
				request.result <- err
				continue
			}
			if !request.cleanup {
				if err := c.eventError(); err != nil {
					request.result <- err
					continue
				}
			}
			err := c.conn.Write(request.ctx, request.msg)
			if request.ctx.Err() != nil {
				err = request.ctx.Err()
			}
			request.result <- err
		}
	}
}

func browserAllocationMethod(method string) bool {
	switch method {
	case "Target.createBrowserContext", "Target.createTarget", "Target.attachToTarget":
		return true
	default:
		return false
	}
}

func browserCleanupMethod(method string) bool {
	switch method {
	case "Browser.getVersion", "Page.stopLoading", "Target.disposeBrowserContext", "Target.closeTarget",
		"Target.getBrowserContexts", "Target.getTargets", "Target.detachFromTarget", "IO.close":
		return true
	default:
		return false
	}
}

func (c *browserCDP) callMode(ctx context.Context, session, method string, params, result any, preserveReply bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !preserveReply {
		select {
		case <-c.eventFailure:
			return c.eventError()
		default:
		}
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return err
	}
	reply := make(chan *cdproto.Message, 1)
	c.mu.Lock()
	if !preserveReply && c.eventErr != nil || browserAllocationMethod(method) && c.eventErr != nil {
		c.mu.Unlock()
		return c.eventErr
	}
	c.next++
	id := c.next
	c.pending[id] = reply
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	if err := ctx.Err(); err != nil {
		return err
	}
	cleanup := preserveReply && browserCleanupMethod(method)
	request := browserCDPWrite{ctx: ctx, msg: &cdproto.Message{ID: id, SessionID: target.SessionID(session), Method: cdproto.MethodType(method), Params: encoded}, result: make(chan error, 1), cleanup: cleanup}
	var eventFailure <-chan struct{}
	if !cleanup {
		eventFailure = c.eventFailure
	}
	select {
	case c.writes <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-eventFailure:
		return c.eventError()
	case <-c.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return c.readError()
	}
	select {
	case writeErr := <-request.result:
		if writeErr != nil {
			return writeErr
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-c.eventFailure:
		if !preserveReply {
			return c.eventError()
		}
		// Allocation receipts matter if the command was sent before overload.
		// Wait for the bounded writer to say whether it was sent or rejected.
		select {
		case writeErr := <-request.result:
			if writeErr != nil {
				return writeErr
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			if err := ctx.Err(); err != nil {
				return err
			}
			return c.readError()
		}
	case <-c.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return c.readError()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.eventFailure:
		if preserveReply {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.done:
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.mu.Lock()
				readErr := c.readErr
				c.mu.Unlock()
				if readErr == nil {
					readErr = io.EOF
				}
				return readErr
			case message := <-reply:
				if message.Error != nil {
					return message.Error
				}
				if result != nil {
					return json.Unmarshal(message.Result, result)
				}
				return nil
			}
		}
		return c.eventError()
	case <-c.done:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.mu.Lock()
		err := c.readErr
		c.mu.Unlock()
		if err == nil {
			err = io.EOF
		}
		return err
	case message := <-reply:
		if !preserveReply {
			if eventErr := c.eventError(); eventErr != nil {
				return eventErr
			}
		}
		if message.Error != nil {
			return message.Error
		}
		if result != nil {
			if err := json.Unmarshal(message.Result, result); err != nil {
				return err
			}
		}
		if !preserveReply {
			if eventErr := c.eventError(); eventErr != nil {
				return eventErr
			}
		}
		return nil
	}
}

func (c *browserCDP) readError() error {
	c.mu.Lock()
	err := c.readErr
	c.mu.Unlock()
	if err == nil {
		return io.EOF
	}
	return err
}

func (c *browserCDP) close() { _ = c.conn.Close(); <-c.done; <-c.writerDone }

func (c *browserCDP) connected() bool {
	c.mu.Lock()
	readErr := c.readErr
	c.mu.Unlock()
	if readErr != nil {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

func (c *browserCDP) eventError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.eventErr
}
