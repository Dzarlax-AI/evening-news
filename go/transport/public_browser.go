package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

// BrowserFetchPublic creates only owned disposable targets on an external,
// server-configured CDP browser. A fail-closed dead proxy in that context prevents Chrome
// sockets from bypassing request interception. Go fulfills each admitted GET
// using its own DNS-pinned transport. CSP blocks workers, child frames, popups,
// and WebSockets, which cannot participate in the owned request byte budget.
func BrowserFetchPublic(ctx context.Context, endpoint, rawURL string, opts BrowserOptions) (doc RenderedDocument, err error) {
	return browserFetchPublic(ctx, endpoint, rawURL, opts, publicGetMode)
}

func browserFetchPublic(ctx context.Context, endpoint, rawURL string, opts BrowserOptions, get func(context.Context, string, Options, bool) (publicResponse, error)) (doc RenderedDocument, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return doc, err
	}
	if err = ValidatePublicURL(u.String()); err != nil {
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
	if timeout < 0 || timeout > 30*time.Second {
		return doc, errors.New("prototype_browser_invalid_timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	wsURL, err := configuredBrowserEndpoint(ctx, endpoint)
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
	if err = client.callAllocation(setup, "", "Target.createBrowserContext", map[string]any{"disposeOnDetach": true, "proxyServer": "http://127.0.0.1:9", "proxyBypassList": "<-loopback>"}, &createdContext); err != nil {
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
		}}},
	} {
		if err = client.call(ctx, sessionID, command.method, command.params, nil); err != nil {
			return doc, err
		}
	}
	work, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var responseStatus atomic.Int64
	var lastRequest atomic.Int64
	lastRequest.Store(time.Now().UnixNano())
	eventsDone := make(chan struct{})
	defer func() { stop(nil); <-eventsDone }()
	go func() {
		defer close(eventsDone)
		var wireBytes, decodedBytes int64
		requests := 0
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
				if string(message.SessionID) != sessionID || string(message.Method) != "Fetch.requestPaused" {
					continue
				}
				var request struct {
					RequestID    string `json:"requestId"`
					ResourceType string `json:"resourceType"`
					Request      struct {
						URL    string `json:"url"`
						Method string `json:"method"`
					} `json:"request"`
				}
				if e := json.Unmarshal(message.Params, &request); e != nil {
					stop(e)
					return
				}
				params := map[string]any{"requestId": request.RequestID, "errorReason": "BlockedByClient"}
				blocked := request.Request.Method != "GET" || request.ResourceType == "Image" || request.ResourceType == "Media" || request.ResourceType == "Font"
				admission := ValidatePublicURL(request.Request.URL)
				if blocked || admission != nil {
					if e := client.call(work, sessionID, "Fetch.failRequest", params, nil); e != nil {
						stop(e)
						return
					}
					if request.ResourceType == "Document" && admission != nil {
						stop(admission)
						return
					}
					continue
				}
				requests++
				lastRequest.Store(time.Now().UnixNano())
				if requests > 64 || wireBytes >= wireLimit || decodedBytes >= domLimit {
					stop(extractor.ErrWireLimit)
					return
				}
				// Chrome never opens the destination socket. Each paused GET is
				// fetched by Go's DNS-pinned public transport and fulfilled.
				response, e := get(work, request.Request.URL, Options{MaxWireBytes: wireLimit - wireBytes, MaxDecodedBytes: domLimit - decodedBytes, Timeout: timeout}, false)
				if e != nil {
					_ = client.call(work, sessionID, "Fetch.failRequest", params, nil)
					if request.ResourceType == "Document" || errors.Is(e, extractor.ErrWireLimit) || errors.Is(e, extractor.ErrOutputLimit) {
						stop(e)
						return
					}
					continue
				}
				wireBytes += response.wire
				decodedBytes += int64(len(response.body))
				headers := make([]any, 0, len(response.header)+2)
				for name, values := range response.header {
					if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Content-Encoding") || strings.EqualFold(name, "Transfer-Encoding") || strings.EqualFold(name, "Set-Cookie") {
						continue
					}
					for _, value := range values {
						headers = append(headers, map[string]string{"name": name, "value": value})
					}
				}
				if request.ResourceType == "Document" {
					responseStatus.Store(int64(response.status))
					headers = append(headers, map[string]string{"name": "Content-Security-Policy", "value": browserIsolationPolicy})
				}
				headers = append(headers, map[string]string{"name": "Content-Length", "value": strconv.Itoa(len(response.body))})
				params = map[string]any{"requestId": request.RequestID, "responseCode": response.status, "responseHeaders": headers, "body": base64.StdEncoding.EncodeToString(response.body)}
				if e = client.call(work, sessionID, "Fetch.fulfillRequest", params, nil); e != nil {
					stop(e)
					return
				}
				lastRequest.Store(time.Now().UnixNano())
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
	started := time.Now()
	stableSince := time.Now()
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
			if value.HTML != previousHTML {
				previousHTML = value.HTML
				stableSince = time.Now()
			}
			// Let short asynchronous fetches and timers populate an existing shell.
			// A finite settle window is bounded by the shared request deadline.
			if time.Since(started) < 500*time.Millisecond || time.Since(stableSince) < 250*time.Millisecond || time.Since(time.Unix(0, lastRequest.Load())) < 250*time.Millisecond {
				select {
				case <-work.Done():
					return doc, context.Cause(work)
				case <-ticker.C:
				}
				continue
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

// configuredBrowserEndpoint is only for trusted server configuration. Discovery
// may supply a browser target path, never an alternate authority or scheme.
// Existing prototype endpoint admission remains loopback-only.
func configuredBrowserEndpoint(ctx context.Context, endpoint string) (string, error) {
	return configuredBrowserEndpointWithResolver(ctx, endpoint, net.DefaultResolver)
}

// ProbeConfiguredBrowser validates bounded Chrome discovery, including the
// browser websocket path. The endpoint is trusted server configuration.
func ProbeConfiguredBrowser(ctx context.Context, endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("browser_invalid_configured_endpoint")
	}
	_, err = configuredBrowserEndpoint(ctx, endpoint)
	return err
}

func configuredBrowserEndpointWithResolver(ctx context.Context, endpoint string, resolver publicResolver) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", errors.New("browser_invalid_configured_endpoint")
	}
	if u.Scheme == "ws" || u.Scheme == "wss" {
		if !validBrowserTargetPath(u.Path) {
			return "", errors.New("browser_invalid_configured_endpoint")
		}
		return u.String(), nil
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("browser_invalid_configured_endpoint")
	}
	u.Path = "/json/version"
	u.RawPath = ""
	// Chrome's discovery endpoint rejects DNS-name Host headers (including
	// Compose service names). Resolve the trusted configured authority once,
	// pin the discovery socket to that IP and use its numeric Host header.
	// Keep the URL authority for TLS SNI/certificate validation and pin the
	// discovered websocket path back to that configured authority below.
	ips, err := resolver.LookupNetIP(ctx, "ip", u.Hostname())
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", errors.New("browser_configured_dns_empty")
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	address := net.JoinHostPort(ips[0].Unmap().String(), port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Host = address
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	tr := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, address)
	}, ResponseHeaderTimeout: 3 * time.Second, MaxResponseHeaderBytes: 64 * 1024}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("browser_discovery_redirect_denied") }}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", errors.New("browser_discovery_status")
	}
	body, err := readBounded(resp.Body, 64*1024, extractor.ErrWireLimit)
	if err != nil {
		return "", err
	}
	var version struct {
		URL string `json:"webSocketDebuggerUrl"`
	}
	if err = json.Unmarshal(body, &version); err != nil {
		return "", err
	}
	ws, err := url.Parse(version.URL)
	if err != nil || ws.Host == "" || (ws.Scheme != "ws" && ws.Scheme != "wss") || !validBrowserTargetPath(ws.Path) || ws.RawQuery != "" || ws.Fragment != "" || ws.User != nil {
		return "", errors.New("browser_invalid_discovered_endpoint")
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = ws.Path
	u.RawPath = ""
	return u.String(), nil
}

func validBrowserTargetPath(path string) bool {
	const prefix = "/devtools/browser/"
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	id := strings.TrimPrefix(path, prefix)
	return id != "" && id != "." && id != ".." && !strings.Contains(id, "/")
}
