package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

var (
	ErrNativeNotReady      = errors.New("native_browser_not_ready")
	ErrNativeBusy          = errors.New("native_browser_busy")
	ErrNativeOwnershipLost = errors.New("native_browser_ownership_lost")
	ErrNativePolicy        = errors.New("native_browser_policy_denied")
	ErrNativeUnsupported   = errors.New("native_browser_unsupported")
)

// NativeBrowserConfig is server configuration, never part of an article payload.
// NetworkIsolationVerified attests an independently verified network namespace:
// this library cannot establish firewall/UDP/DNS isolation using CDP alone.
type NativeBrowserConfig struct {
	Endpoint                 string
	ProxyURL                 string
	ProxyToken               string
	NetworkIsolationVerified bool
}

type NativeBrowserOptions struct {
	BrowserOptions
	ReadySelector string
}

// NativeBrowser owns only disposable resources on a dedicated external browser.
// Readiness starts false and ownership loss permanently taints this generation.
// It never launches, restarts or globally closes the browser process.
type NativeBrowser struct {
	cfg                    NativeBrowserConfig
	mu                     sync.Mutex
	ready, tainted, closed bool
	active                 bool
	cancel                 context.CancelFunc
	idle                   chan struct{}
	generation             string
}

func NewNativeBrowser(cfg NativeBrowserConfig) (*NativeBrowser, error) {
	if !cfg.NetworkIsolationVerified {
		return nil, errors.New("native_browser_network_isolation_required")
	}
	endpoint, e := url.Parse(cfg.Endpoint)
	if e != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Opaque != "" {
		return nil, errors.New("native_browser_invalid_endpoint")
	}
	proxy, e := url.Parse(cfg.ProxyURL)
	if e != nil || proxy.Scheme != "http" || proxy.Hostname() == "" || proxy.User != nil || (proxy.Path != "" && proxy.Path != "/") || proxy.RawQuery != "" || proxy.Fragment != "" || proxy.Opaque != "" || len(cfg.ProxyToken) < 16 || len(cfg.ProxyToken) > 4096 {
		return nil, errors.New("native_browser_invalid_proxy_configuration")
	}
	cfg.ProxyURL = strings.TrimRight(cfg.ProxyURL, "/")
	idle := make(chan struct{})
	close(idle)
	return &NativeBrowser{cfg: cfg, idle: idle}, nil
}

func (w *NativeBrowser) Ready() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ready && !w.tainted && !w.closed
}

// CheckReady revalidates discovery without allocating targets or probing an
// active job. It can clear readiness, but never restores it or changes generation.
func (w *NativeBrowser) CheckReady(ctx context.Context) bool {
	w.mu.Lock()
	if !w.ready || w.tainted || w.closed {
		w.mu.Unlock()
		return false
	}
	generation := w.generation
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	ws, err := configuredBrowserEndpoint(ctx, w.cfg.Endpoint)
	if err != nil {
		w.unavailable()
		return false
	}
	w.mu.Lock()
	if generation == "" || ws != generation {
		w.ready = false
		w.tainted = true
		cancelJob := w.cancel
		w.mu.Unlock()
		if cancelJob != nil {
			cancelJob()
		}
		return false
	}
	ready := w.ready && !w.tainted && !w.closed && w.generation == generation
	w.mu.Unlock()
	return ready
}

func (w *NativeBrowser) begin(ctx context.Context, probe bool) (context.Context, func(), error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tainted {
		return nil, nil, ErrNativeOwnershipLost
	}
	if w.closed || (!probe && !w.ready) {
		return nil, nil, ErrNativeNotReady
	}
	if w.active {
		return nil, nil, ErrNativeBusy
	}
	ctx, cancel := context.WithCancel(ctx)
	w.active = true
	w.cancel = cancel
	w.idle = make(chan struct{})
	return ctx, func() {
		cancel()
		w.mu.Lock()
		w.active = false
		w.cancel = nil
		close(w.idle)
		w.mu.Unlock()
	}, nil
}

func (w *NativeBrowser) taint() { w.mu.Lock(); w.ready = false; w.tainted = true; w.mu.Unlock() }

func (w *NativeBrowser) unavailable() { w.mu.Lock(); w.ready = false; w.mu.Unlock() }

// Probe checks the actual configured CDP context/target/session/disposal methods.
// It performs no origin navigation and does not prove network firewall policy.
func (w *NativeBrowser) Probe(ctx context.Context) error {
	ctx, done, err := w.begin(ctx, true)
	if err != nil {
		return err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resources, err := w.open(ctx)
	if err != nil {
		return err
	}
	capabilityErr := resources.configure(ctx)
	if err = resources.cleanup(); err != nil {
		w.taint()
		return errors.Join(ErrNativeOwnershipLost, err)
	}
	if capabilityErr != nil {
		w.taint()
		return errors.Join(ErrNativeNotReady, capabilityErr)
	}
	w.mu.Lock()
	if !w.closed && !w.tainted {
		w.ready = true
		w.generation = resources.generation
	}
	w.mu.Unlock()
	return nil
}

func (w *NativeBrowser) Close(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	w.ready = false
	cancel, idle := w.cancel, w.idle
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type nativeResources struct {
	client                         *browserCDP
	contextID, targetID, sessionID string
	mainFrameID                    string
	generation                     string
}

func (w *NativeBrowser) open(ctx context.Context) (res *nativeResources, err error) {
	ws, err := configuredBrowserEndpoint(ctx, w.cfg.Endpoint)
	if err != nil {
		w.unavailable()
		return nil, errors.Join(ErrNativeNotReady, err)
	}
	w.mu.Lock()
	changed := w.generation != "" && w.generation != ws
	w.mu.Unlock()
	if changed {
		w.taint()
		return nil, ErrNativeOwnershipLost
	}
	client, err := dialBrowser(ctx, ws)
	if err != nil {
		w.unavailable()
		return nil, errors.Join(ErrNativeNotReady, err)
	}
	res = &nativeResources{client: client, generation: ws}
	owned := res
	defer func() {
		if err != nil {
			w.taint()
			_ = owned.cleanup()
		}
	}()
	// Capture receipts even when the caller cancels midway through allocation.
	setup, done := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer done()
	var created struct {
		ID string `json:"browserContextId"`
	}
	if err = client.call(setup, "", "Target.createBrowserContext", map[string]any{"disposeOnDetach": true, "proxyServer": w.cfg.ProxyURL, "proxyBypassList": "<-loopback>"}, &created); err != nil {
		return nil, errors.Join(ErrNativeOwnershipLost, err)
	}
	res.contextID = created.ID
	if created.ID == "" {
		return nil, ErrNativeOwnershipLost
	}
	var target struct {
		ID string `json:"targetId"`
	}
	if err = client.call(setup, "", "Target.createTarget", map[string]any{"url": "about:blank", "browserContextId": res.contextID, "background": true}, &target); err != nil {
		return nil, errors.Join(ErrNativeOwnershipLost, err)
	}
	res.targetID = target.ID
	if target.ID == "" {
		return nil, ErrNativeOwnershipLost
	}
	var attached struct {
		ID string `json:"sessionId"`
	}
	if err = client.call(setup, "", "Target.attachToTarget", map[string]any{"targetId": res.targetID, "flatten": true}, &attached); err != nil {
		return nil, errors.Join(ErrNativeOwnershipLost, err)
	}
	res.sessionID = attached.ID
	if attached.ID == "" {
		return nil, ErrNativeOwnershipLost
	}
	return res, nil
}

func (r *nativeResources) cleanup() error {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	var errs []error
	if r.sessionID != "" {
		// Navigation may already have stopped after a denied redirect. Disposal
		// and the two absence receipts below establish ownership cleanup.
		_ = r.client.call(ctx, r.sessionID, "Page.stopLoading", map[string]any{}, nil)
	}
	if r.contextID != "" {
		// Dispose the owned context atomically. Closing its target first races
		// disposal on the pinned Cloak build; disposal closes all child targets.
		errs = append(errs, r.client.call(ctx, "", "Target.disposeBrowserContext", map[string]any{"browserContextId": r.contextID}, nil))
	} else if r.targetID != "" {
		errs = append(errs, r.client.call(ctx, "", "Target.closeTarget", map[string]any{"targetId": r.targetID}, nil))
	}
	var contexts struct {
		IDs []string `json:"browserContextIds"`
	}
	if err := r.client.call(ctx, "", "Target.getBrowserContexts", map[string]any{}, &contexts); err != nil {
		errs = append(errs, err)
	} else {
		for _, id := range contexts.IDs {
			if id == r.contextID {
				errs = append(errs, ErrNativeOwnershipLost)
			}
		}
	}
	var targets struct {
		Infos []struct {
			ID        string `json:"targetId"`
			ContextID string `json:"browserContextId"`
		} `json:"targetInfos"`
	}
	if err := r.client.call(ctx, "", "Target.getTargets", map[string]any{}, &targets); err != nil {
		errs = append(errs, err)
	} else {
		for _, t := range targets.Infos {
			if t.ID == r.targetID || (r.contextID != "" && t.ContextID == r.contextID) {
				errs = append(errs, ErrNativeOwnershipLost)
			}
		}
	}
	r.client.close()
	return errors.Join(errs...)
}

func (r *nativeResources) configure(ctx context.Context) error {
	for _, command := range []struct {
		method string
		params any
	}{
		{"Page.enable", map[string]any{}},
		{"Network.enable", map[string]any{"maxPostDataSize": 1}},
		{"Network.setBlockedURLs", map[string]any{"urls": []string{"http://*", "ws://*", "wss://*", "file://*", "ftp://*"}}},
		{"Browser.setDownloadBehavior", map[string]any{"behavior": "deny", "browserContextId": r.contextID, "eventsEnabled": true}},
		{"Target.setAutoAttach", map[string]any{"autoAttach": true, "waitForDebuggerOnStart": true, "flatten": true}},
		{"Fetch.enable", map[string]any{"handleAuthRequests": true, "patterns": []any{map[string]any{"urlPattern": "*", "requestStage": "Request"}}}},
	} {
		session := r.sessionID
		if command.method == "Browser.setDownloadBehavior" {
			session = ""
		}
		if err := r.client.call(ctx, session, command.method, command.params, nil); err != nil {
			return err
		}
	}
	var tree struct {
		FrameTree struct {
			Frame struct {
				ID string `json:"id"`
			} `json:"frame"`
		} `json:"frameTree"`
	}
	if err := r.client.call(ctx, r.sessionID, "Page.getFrameTree", map[string]any{}, &tree); err != nil {
		return err
	}
	if tree.FrameTree.Frame.ID == "" {
		return errors.New("native_browser_main_frame_unavailable")
	}
	r.mainFrameID = tree.FrameTree.Frame.ID
	return nil
}

func nativeURL(raw string) error {
	if err := ValidatePublicURL(raw); err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	if u.Scheme != "https" || (u.Port() != "" && u.Port() != "443") {
		return ErrNativeUnsupported
	}
	return nil
}

func nativeProxyChallenge(origin string, cfg NativeBrowserConfig) bool {
	u, e := url.Parse(origin)
	if e != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	p, _ := url.Parse(cfg.ProxyURL)
	port := func(u *url.URL) string {
		if u.Port() != "" {
			return u.Port()
		}
		return "80"
	}
	return u.Scheme == p.Scheme && strings.EqualFold(u.Hostname(), p.Hostname()) && port(u) == port(p)
}

// Fetch leaves all origin request bytes, TLS, headers, cookies and bodies in the
// browser. It never fulfills responses or overrides UA, hints or viewport.
func (w *NativeBrowser) Fetch(ctx context.Context, raw string, opts NativeBrowserOptions) (doc RenderedDocument, err error) {
	if err = nativeURL(raw); err != nil {
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
	if timeout < 0 || timeout > 30*time.Second || len(opts.ReadySelector) > 2048 {
		return doc, errors.New("native_browser_invalid_options")
	}
	ctx, done, err := w.begin(ctx, false)
	if err != nil {
		return doc, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := w.open(ctx)
	if err != nil {
		return doc, err
	}
	defer func() {
		if cleanupErr := res.cleanup(); cleanupErr != nil {
			w.taint()
			err = errors.Join(err, ErrNativeOwnershipLost)
		}
	}()
	client, session := res.client, res.sessionID
	if err = res.configure(ctx); err != nil {
		w.taint()
		return doc, errors.Join(ErrNativeNotReady, err)
	}
	work, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var status atomic.Int64
	var mainFrame atomic.Value
	mainFrame.Store(res.mainFrameID)
	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	var activeRequests atomic.Int64
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		var wire float64
		requests := map[string]bool{}
		documentFrames := map[string]string{}
		encodedByRequest := map[string]float64{}
		ownedFrames := map[string]bool{res.mainFrameID: true}
		totalRequests, totalAuth := 0, 0
		authenticated := map[string]bool{}
		for {
			select {
			case <-work.Done():
				return
			case <-client.done:
				w.taint()
				stop(ErrNativeOwnershipLost)
				return
			case message := <-client.events:
				method := string(message.Method)
				if method == "Browser.downloadWillBegin" {
					var event struct {
						FrameID string `json:"frameId"`
					}
					if json.Unmarshal(message.Params, &event) == nil && (ownedFrames[event.FrameID] || event.FrameID == mainFrame.Load().(string)) {
						stop(ErrNativeUnsupported)
						return
					}
				}
				if string(message.SessionID) != session {
					continue
				}
				lastActivity.Store(time.Now().UnixNano())
				switch method {
				case "Page.frameAttached":
					var event struct {
						ID     string `json:"frameId"`
						Parent string `json:"parentFrameId"`
					}
					if json.Unmarshal(message.Params, &event) == nil && (ownedFrames[event.Parent] || event.Parent == mainFrame.Load().(string)) {
						ownedFrames[event.ID] = true
						if len(ownedFrames) > 128 {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Page.frameNavigated":
					var event struct {
						Frame struct {
							ID     string `json:"id"`
							Parent string `json:"parentId"`
						} `json:"frame"`
					}
					if json.Unmarshal(message.Params, &event) == nil && event.Frame.ID != "" && event.Frame.Parent == "" {
						mainFrame.Store(event.Frame.ID)
						ownedFrames[event.Frame.ID] = true
					}
				case "Page.frameDetached":
					var event struct {
						ID string `json:"frameId"`
					}
					if json.Unmarshal(message.Params, &event) == nil {
						delete(ownedFrames, event.ID)
					}
				case "Fetch.authRequired":
					totalAuth++
					if totalAuth > 256 {
						stop(extractor.ErrWireLimit)
						return
					}
					var event struct {
						ID        string                          `json:"requestId"`
						Challenge struct{ Source, Origin string } `json:"authChallenge"`
					}
					if json.Unmarshal(message.Params, &event) != nil {
						stop(ErrNativePolicy)
						return
					}
					response := map[string]any{"response": "CancelAuth"}
					allowed := event.Challenge.Source == "Proxy" && nativeProxyChallenge(event.Challenge.Origin, w.cfg) && !authenticated[event.ID]
					if allowed {
						response = map[string]any{"response": "ProvideCredentials", "username": "extractor", "password": w.cfg.ProxyToken}
						authenticated[event.ID] = true
					}
					if e := client.call(work, session, "Fetch.continueWithAuth", map[string]any{"requestId": event.ID, "authChallengeResponse": response}, nil); e != nil {
						stop(e)
						return
					}
					if !allowed {
						code := 401
						if event.Challenge.Source == "Proxy" {
							code = 407
						}
						stop(&PublicHTTPStatusError{StatusCode: code})
						return
					}
				case "Fetch.requestPaused":
					var event struct {
						ID      string `json:"requestId"`
						Request struct {
							URL string `json:"url"`
						} `json:"request"`
					}
					if json.Unmarshal(message.Params, &event) != nil {
						stop(ErrNativePolicy)
						return
					}
					if policyErr := nativeURL(event.Request.URL); policyErr != nil {
						_ = client.call(work, session, "Fetch.failRequest", map[string]any{"requestId": event.ID, "errorReason": "BlockedByClient"}, nil)
						stop(policyErr)
						return
					}
					if e := client.call(work, session, "Fetch.continueRequest", map[string]any{"requestId": event.ID}, nil); e != nil {
						stop(e)
						return
					}
				case "Network.requestWillBeSent":
					var event struct {
						ID      string `json:"requestId"`
						FrameID string `json:"frameId"`
						Type    string `json:"type"`
					}
					if json.Unmarshal(message.Params, &event) == nil {
						totalRequests++
						if totalRequests > 128 {
							stop(extractor.ErrWireLimit)
							return
						}
						if event.Type == "Document" {
							documentFrames[event.ID] = event.FrameID
						}
						if requests[event.ID] {
							continue
						}
						requests[event.ID] = true
						activeRequests.Add(1)
						if len(requests) > 128 {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Network.loadingFinished", "Network.loadingFailed":
					var event struct {
						ID      string  `json:"requestId"`
						Encoded float64 `json:"encodedDataLength"`
					}
					if json.Unmarshal(message.Params, &event) == nil && requests[event.ID] {
						wire += max(0, event.Encoded-encodedByRequest[event.ID])
						delete(encodedByRequest, event.ID)
						if wire > float64(wireLimit) {
							stop(extractor.ErrWireLimit)
							return
						}
						delete(requests, event.ID)
						activeRequests.Add(-1)
					}
				case "Network.dataReceived":
					var event struct {
						ID      string  `json:"requestId"`
						Encoded float64 `json:"encodedDataLength"`
					}
					if json.Unmarshal(message.Params, &event) == nil {
						wire += event.Encoded
						encodedByRequest[event.ID] += event.Encoded
						if wire > float64(wireLimit) {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Network.responseReceived":
					var event struct {
						Type     string `json:"type"`
						ID       string `json:"requestId"`
						FrameID  string `json:"frameId"`
						Response struct {
							Status  int            `json:"status"`
							Headers map[string]any `json:"headers"`
						} `json:"response"`
					}
					if json.Unmarshal(message.Params, &event) != nil {
						stop(ErrNativePolicy)
						return
					}
					frameID := event.FrameID
					if frameID == "" {
						frameID = documentFrames[event.ID]
					}
					if event.Type == "Document" && frameID != "" && frameID == mainFrame.Load().(string) {
						status.Store(int64(event.Response.Status))
						var retry *int
						for name, value := range event.Response.Headers {
							if strings.EqualFold(name, "Retry-After") {
								retry = parseRetryAfter(fmt.Sprint(value), time.Now())
							}
						}
						if event.Response.Status != 200 {
							stop(&PublicHTTPStatusError{StatusCode: event.Response.Status, RetryAfterSeconds: retry})
							return
						}
					}
				case "Network.webSocketCreated", "Page.windowOpen", "Target.attachedToTarget":
					stop(ErrNativeUnsupported)
					return
				case "Inspector.targetCrashed", "Target.detachedFromTarget":
					w.taint()
					stop(ErrNativeOwnershipLost)
					return
				}
			}
		}
	}()
	defer func() { stop(nil); <-eventsDone }()
	var navigation struct {
		Error      string `json:"errorText"`
		FrameID    string `json:"frameId"`
		IsDownload bool   `json:"isDownload"`
	}
	if err = client.call(work, session, "Page.navigate", map[string]any{"url": raw}, &navigation); err != nil {
		return doc, browserCause(work, err)
	}
	if navigation.IsDownload {
		return doc, ErrNativeUnsupported
	}
	if navigation.Error != "" {
		if cause := context.Cause(work); cause != nil {
			return doc, cause
		}
		return doc, errors.New("native_browser_navigation_failed")
	}
	if navigation.FrameID != "" {
		mainFrame.Store(navigation.FrameID)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	selector, _ := json.Marshal(opts.ReadySelector)
	started, stableSince := time.Now(), time.Now()
	previous := ""
	for {
		expression := fmt.Sprintf(`(() => {
 if (!document.body || document.readyState !== 'complete') return {ready:false};
 if (document.body.textContent.length > %d) return {ready:true,tooLarge:true};
 const html=document.documentElement.outerHTML;
 if (html.length>%d) return {ready:true,tooLarge:true};
 const bytes=new TextEncoder().encode(html).length;
 if (bytes>%d) return {ready:true,tooLarge:true};
 const selector=%s;
 if (selector && !document.querySelector(selector)) return {ready:false};
 if (!document.body.innerText.trim()) return {ready:false};
 return {ready:true,html:html,bytes:bytes,url:location.href};
})()`, domLimit, domLimit, domLimit, selector)
		var evaluated struct {
			Result struct {
				Value struct {
					Ready, TooLarge bool
					HTML            string `json:"html"`
					Bytes           int    `json:"bytes"`
					URL             string `json:"url"`
				} `json:"value"`
			} `json:"result"`
			Exception json.RawMessage `json:"exceptionDetails"`
		}
		if err = client.call(work, session, "Runtime.evaluate", map[string]any{"expression": expression, "returnByValue": true}, &evaluated); err != nil {
			return doc, browserCause(work, err)
		}
		if len(evaluated.Exception) > 0 {
			return doc, errors.New("native_browser_evaluation_failed")
		}
		value := evaluated.Result.Value
		if value.Ready {
			if value.TooLarge || int64(len(value.HTML)) > domLimit || int64(value.Bytes) > domLimit {
				return doc, extractor.ErrOutputLimit
			}
			if err = nativeURL(value.URL); err != nil {
				return doc, err
			}
			if value.HTML != previous {
				previous = value.HTML
				stableSince = time.Now()
			}
			if time.Since(started) >= 500*time.Millisecond && time.Since(stableSince) >= 250*time.Millisecond && time.Since(time.Unix(0, lastActivity.Load())) >= 250*time.Millisecond && activeRequests.Load() == 0 && status.Load() == 200 {
				if cause := context.Cause(work); cause != nil {
					return doc, cause
				}
				return RenderedDocument{Document: Document{HTML: []byte(value.HTML), URL: value.URL, StatusCode: 200, Encoding: "utf-8"}, DOMBytes: value.Bytes}, nil
			}
		}
		select {
		case <-work.Done():
			return doc, context.Cause(work)
		case <-ticker.C:
		}
	}
}
