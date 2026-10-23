package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/chromedp/cdproto"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type nativeFixture struct {
	mu                                                                                                                 sync.Mutex
	contexts, targets                                                                                                  map[string]bool
	methods                                                                                                            []string
	proxyAuth                                                                                                          map[string]any
	status                                                                                                             int
	childStatus                                                                                                        int
	downloadFrame                                                                                                      string
	navigateDownload                                                                                                   bool
	requestBurst                                                                                                       int
	pausedBurst                                                                                                        int
	pauseEventFlood                                                                                                    int
	pauseEventInterval                                                                                                 time.Duration
	continueDelay                                                                                                      time.Duration
	failContinueRequest                                                                                                bool
	requestURL                                                                                                         string
	detachAfterPaused                                                                                                  bool
	detachOverride                                                                                                     bool
	detachMalformed                                                                                                    bool
	detachEnvelope, detachSession                                                                                      string
	overflowAfterRequestBudget                                                                                         bool
	eventFlood                                                                                                         int
	cleanupEventFlood                                                                                                  int
	authSource, authOrigin                                                                                             string
	empty, dropReply, failCleanup, failStopLoading, failContextReceipt, failTargetReceipt, disconnect                  bool
	discoveryUnavailable                                                                                               bool
	discoveryID                                                                                                        string
	discoveryDelay                                                                                                     time.Duration
	contextsDelay                                                                                                      time.Duration
	lateAppendAfter                                                                                                    time.Duration
	evaluationStarted                                                                                                  time.Time
	targetCrashed                                                                                                      bool
	foreignDetachOnNavigate                                                                                            bool
	changeGenerationOnCrash, disconnectOnCrash, unavailableOnCrash, foreignTargetCrash, hangStopLoading, detachOnCrash bool
	navigated                                                                                                          chan struct{}
	navigateStarts                                                                                                     chan struct{}
	navigateDelay                                                                                                      time.Duration
	resourceIDs                                                                                                        atomic.Uint64
	server                                                                                                             *httptest.Server
}

func newNativeFixture(t *testing.T) *nativeFixture {
	t.Helper()
	f := &nativeFixture{contexts: map[string]bool{}, targets: map[string]bool{"foreign-sentinel": true}, status: 200, navigated: make(chan struct{}), navigateStarts: make(chan struct{}, 16), discoveryID: "native"}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/json/version" {
			f.mu.Lock()
			unavailable := f.discoveryUnavailable
			id, delay := f.discoveryID, f.discoveryDelay
			f.mu.Unlock()
			if unavailable {
				w.WriteHeader(502)
				return
			}
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-r.Context().Done():
					return
				}
			}
			fmt.Fprintf(w, `{"webSocketDebuggerUrl":"ws://untrusted.example/devtools/browser/%s"}`, id)
			return
		}
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		write := func(v any) bool {
			data, e := json.Marshal(v)
			return e == nil && wsutil.WriteServerMessage(conn, ws.OpText, data) == nil
		}
		id := f.resourceIDs.Add(1)
		contextID, targetID, sessionID := fmt.Sprintf("owned-context-%d", id), fmt.Sprintf("owned-target-%d", id), "owned-session"
		emittedRequests := 0
		emitPaused := func(id string) bool {
			requestURL := f.requestURL
			if requestURL == "" {
				requestURL = "https://origin.example/fixture"
			}
			return write(map[string]any{"method": "Network.requestWillBeSent", "sessionId": sessionID, "params": map[string]string{"requestId": id, "type": "XHR", "frameId": "main-frame"}}) &&
				write(map[string]any{"method": "Fetch.requestPaused", "sessionId": sessionID, "params": map[string]any{"requestId": id, "request": map[string]string{"url": requestURL}}})
		}
		nextRequest := func() bool {
			if emittedRequests >= f.requestBurst {
				return true
			}
			id := fmt.Sprint(emittedRequests)
			emittedRequests++
			return emitPaused(id)
		}
		for {
			data, _, e := wsutil.ReadClientData(conn)
			if e != nil {
				return
			}
			var msg struct {
				ID      int64          `json:"id"`
				Session string         `json:"sessionId"`
				Method  string         `json:"method"`
				Params  map[string]any `json:"params"`
			}
			if json.Unmarshal(data, &msg) != nil {
				return
			}
			f.mu.Lock()
			f.methods = append(f.methods, msg.Method)
			result := map[string]any{}
			reply := true
			protocolErr := false
			responseDelay := time.Duration(0)
			switch msg.Method {
			case "Target.createBrowserContext":
				if msg.Params["proxyServer"] != "http://gateway.test:3128" || msg.Params["proxyBypassList"] != "<-loopback>" || msg.Params["disposeOnDetach"] != true {
					t.Errorf("missing native proxy/context isolation: %+v", msg.Params)
				}
				f.contexts[contextID] = true
				result["browserContextId"] = contextID
				if f.dropReply {
					reply = false
				}
			case "Target.createTarget":
				f.targets[targetID] = true
				result["targetId"] = targetID
			case "Target.attachToTarget":
				result["sessionId"] = sessionID
			case "Page.getFrameTree":
				result["frameTree"] = map[string]any{"frame": map[string]string{"id": "main-frame"}}
			case "Target.disposeBrowserContext":
				for i := 0; i < f.cleanupEventFlood; i++ {
					if !write(map[string]any{"method": "Network.dataReceived", "sessionId": sessionID, "params": map[string]any{"requestId": fmt.Sprint(i), "encodedDataLength": 0}}) {
						f.mu.Unlock()
						return
					}
				}
				f.cleanupEventFlood = 0
				if f.failCleanup {
					protocolErr = true
				} else {
					delete(f.contexts, contextID)
					delete(f.targets, targetID)
				}
			case "Page.stopLoading":
				protocolErr = f.failStopLoading
				if f.hangStopLoading {
					reply = false
				}
			case "Target.closeTarget":
				if msg.Params["targetId"] == "foreign-sentinel" {
					t.Error("foreign target closed")
				}
				delete(f.targets, fmt.Sprint(msg.Params["targetId"]))
			case "Target.getBrowserContexts":
				responseDelay = f.contextsDelay
				protocolErr = f.failContextReceipt
				ids := []string{}
				for id := range f.contexts {
					ids = append(ids, id)
				}
				result["browserContextIds"] = ids
			case "Browser.getVersion":
				result["product"] = "Chrome/123.0.0.0"
			case "Target.getTargets":
				protocolErr = f.failTargetReceipt
				infos := []any{}
				for id := range f.targets {
					infos = append(infos, map[string]string{"targetId": id})
				}
				result["targetInfos"] = infos
			case "Page.navigate":
				responseDelay = f.navigateDelay
				select {
				case f.navigateStarts <- struct{}{}:
				default:
				}
				result["frameId"] = "main-frame"
				if f.navigateDownload {
					result["isDownload"] = true
					result["errorText"] = "net::ERR_ABORTED"
				}
				select {
				case <-f.navigated:
				default:
					close(f.navigated)
				}
				event := map[string]any{"method": "Network.responseReceived", "sessionId": sessionID, "params": map[string]any{"requestId": "main-request", "frameId": "main-frame", "type": "Document", "response": map[string]any{"status": f.status, "headers": map[string]string{"Retry-After": "30"}}}}
				if f.targetCrashed {
					f.targetCrashed = false
					if f.changeGenerationOnCrash {
						f.discoveryID = "replacement"
					}
					if f.unavailableOnCrash {
						f.discoveryUnavailable = true
					}
					eventSession := sessionID
					if f.foreignTargetCrash {
						eventSession = "foreign-session"
					}
					event = map[string]any{"method": "Inspector.targetCrashed", "sessionId": eventSession, "params": map[string]any{}}
				}
				if f.authSource != "" {
					event = map[string]any{"method": "Fetch.authRequired", "sessionId": sessionID, "params": map[string]any{"requestId": "auth", "authChallenge": map[string]string{"source": f.authSource, "origin": f.authOrigin}}}
				}
				if !write(event) {
					f.mu.Unlock()
					return
				}
				if f.foreignDetachOnNavigate {
					if !write(map[string]any{"method": "Target.detachedFromTarget", "sessionId": "foreign-session", "params": map[string]string{"sessionId": sessionID}}) {
						f.mu.Unlock()
						return
					}
					f.foreignDetachOnNavigate = false
				}
				if event["method"] == "Inspector.targetCrashed" && f.foreignTargetCrash {
					if !write(map[string]any{"method": "Network.responseReceived", "sessionId": sessionID, "params": map[string]any{"requestId": "main-request", "frameId": "main-frame", "type": "Document", "response": map[string]any{"status": f.status, "headers": map[string]string{}}}}) {
						f.mu.Unlock()
						return
					}
				}
				if event["method"] == "Inspector.targetCrashed" && f.detachOnCrash {
					if !write(map[string]any{"method": "Target.detachedFromTarget", "sessionId": sessionID, "params": map[string]string{"sessionId": sessionID}}) {
						f.mu.Unlock()
						return
					}
				}
				if f.disconnectOnCrash && event["method"] == "Inspector.targetCrashed" {
					f.mu.Unlock()
					return
				}
				if f.childStatus != 0 || f.downloadFrame == "child-frame" {
					write(map[string]any{"method": "Page.frameAttached", "sessionId": sessionID, "params": map[string]string{"frameId": "child-frame", "parentFrameId": "main-frame"}})
				}
				if f.childStatus != 0 {
					write(map[string]any{"method": "Network.responseReceived", "sessionId": sessionID, "params": map[string]any{"requestId": "child-request", "frameId": "child-frame", "type": "Document", "response": map[string]any{"status": f.childStatus, "headers": map[string]string{}}}})
				}
				if f.downloadFrame != "" {
					write(map[string]any{"method": "Browser.downloadWillBegin", "params": map[string]string{"frameId": f.downloadFrame}})
				}
				if !nextRequest() {
					f.mu.Unlock()
					return
				}
				if f.overflowAfterRequestBudget {
					for i := 0; i < 129; i++ {
						if !write(map[string]any{"method": "Network.requestWillBeSent", "sessionId": sessionID, "params": map[string]string{"requestId": fmt.Sprintf("budget-%d", i), "type": "XHR", "frameId": "main-frame"}}) {
							f.mu.Unlock()
							return
						}
					}
					for i := 0; i < 1024; i++ {
						if !write(map[string]any{"method": "Network.dataReceived", "sessionId": sessionID, "params": map[string]any{"requestId": fmt.Sprint(i), "encodedDataLength": 0}}) {
							f.mu.Unlock()
							return
						}
					}
					f.overflowAfterRequestBudget = false
				}
				for i := 0; i < f.pausedBurst; i++ {
					if !emitPaused(fmt.Sprintf("burst-%d", i)) {
						f.mu.Unlock()
						return
					}
				}
				f.pausedBurst = 0
				if f.detachAfterPaused {
					envelope, detached := sessionID, sessionID
					if f.detachOverride {
						envelope, detached = f.detachEnvelope, f.detachSession
					}
					params := map[string]string{"sessionId": detached}
					if f.detachMalformed {
						params = map[string]string{}
					}
					write(map[string]any{"method": "Target.detachedFromTarget", "sessionId": envelope, "params": params})
					f.detachAfterPaused = false
				}
			case "Fetch.continueRequest":
				responseDelay = f.continueDelay
				protocolErr = f.failContinueRequest
				f.failContinueRequest = false
				// Emit a burst while the request decision RPC is outstanding.
				for i := 0; i < f.pauseEventFlood; i++ {
					if !write(map[string]any{"method": "Network.dataReceived", "sessionId": sessionID, "params": map[string]any{"requestId": msg.Params["requestId"], "encodedDataLength": 0}}) {
						f.mu.Unlock()
						return
					}
					if f.pauseEventInterval > 0 {
						time.Sleep(f.pauseEventInterval)
					}
				}
				f.pauseEventFlood = 0
				// Pace completed zero-byte requests through protocol acknowledgements.
				// A burst before the navigate reply tests queue overload instead of
				// the cumulative request budget, depending on reader scheduling.
				if f.requestBurst > 0 {
					if !write(map[string]any{"method": "Network.loadingFinished", "sessionId": sessionID, "params": map[string]any{"requestId": msg.Params["requestId"], "encodedDataLength": 0}}) || !nextRequest() {
						f.mu.Unlock()
						return
					}
				}
			case "Fetch.continueWithAuth":
				f.proxyAuth = msg.Params["authChallengeResponse"].(map[string]any)
				if f.proxyAuth["response"] == "ProvideCredentials" {
					write(map[string]any{"method": "Network.responseReceived", "sessionId": sessionID, "params": map[string]any{"frameId": "main-frame", "requestId": "main-request", "type": "Document", "response": map[string]any{"status": 200, "headers": map[string]string{}}}})
				}
			case "Runtime.evaluate":
				if f.disconnect {
					f.mu.Unlock()
					return
				}
				if f.eventFlood > 0 && !write(map[string]any{"method": "Fetch.authRequired", "sessionId": sessionID, "params": map[string]any{"requestId": "flood-auth", "authChallenge": map[string]string{"source": "Proxy", "origin": "http://gateway.test:3128"}}}) {
					f.mu.Unlock()
					return
				}
				for i := 0; i < f.eventFlood; i++ {
					if !write(map[string]any{"method": "Network.dataReceived", "sessionId": sessionID, "params": map[string]any{"requestId": fmt.Sprint(i), "encodedDataLength": 0}}) {
						f.mu.Unlock()
						return
					}
				}
				f.eventFlood = 0
				value := map[string]any{"ready": false}
				if !f.empty {
					now := time.Now()
					if f.evaluationStarted.IsZero() {
						f.evaluationStarted = now
					}
					html := `<html><body><article>Native browser fixture.</article></body></html>`
					if f.lateAppendAfter > 0 && now.Sub(f.evaluationStarted) >= f.lateAppendAfter {
						html = `<html><body><article>Native browser fixture.</article><article>Late second post.</article></body></html>`
					}
					value = map[string]any{"ready": true, "html": html, "bytes": len(html), "url": "https://origin.example/final"}
				}
				result["result"] = map[string]any{"value": value}
			case "Fetch.fulfillRequest", "Network.setUserAgentOverride", "Emulation.setDeviceMetricsOverride", "Browser.close":
				t.Errorf("native path changed browser identity/body or closed browser: %s", msg.Method)
			}
			f.mu.Unlock()
			if reply {
				if responseDelay > 0 {
					time.Sleep(responseDelay)
				}
				response := map[string]any{"id": msg.ID, "result": result}
				if protocolErr {
					delete(response, "result")
					response["error"] = map[string]any{"code": -32000, "message": "cleanup refused"}
				}
				if !write(response) {
					return
				}
			}
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *nativeFixture) worker(t *testing.T) *NativeBrowser {
	t.Helper()
	w, err := NewNativeBrowser(NativeBrowserConfig{Endpoint: f.server.URL, ProxyURL: "http://gateway.test:3128", ProxyToken: "sixteen-byte-token", NetworkIsolationVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestNativeBrowserConfigurationAndInitialReadiness(t *testing.T) {
	if _, err := NewNativeBrowser(NativeBrowserConfig{}); err == nil {
		t.Fatal("unverified network isolation accepted")
	}
	f := newNativeFixture(t)
	w := f.worker(t)
	if w.Ready() {
		t.Fatal("ready before capability probe")
	}
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); !errors.Is(err, ErrNativeNotReady) {
		t.Fatal(err)
	}
	for _, raw := range []string{"http://origin.example/", "https://origin.example:8443/", "https://127.0.0.1/"} {
		if _, err := w.Fetch(context.Background(), raw, NativeBrowserOptions{}); err == nil {
			t.Fatalf("unsupported native URL accepted: %s", raw)
		}
	}
}

func TestNativeLifecycleAuthAndNoFulfill(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.authSource = "Proxy"
	f.authOrigin = "http://gateway.test:3128"
	f.mu.Unlock()
	got, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "https://origin.example/final" || !strings.Contains(string(got.HTML), "Native browser fixture") || !w.Ready() {
		t.Fatalf("invalid native result: %+v ready=%v", got, w.Ready())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.proxyAuth["username"] != "extractor" || f.proxyAuth["password"] != "sixteen-byte-token" {
		t.Fatal("proxy credentials not provided")
	}
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("owned resources leaked or foreign target harmed: %v %v", f.contexts, f.targets)
	}
}

func TestNativeSlowContextReceiptCompletesOwnedCleanup(t *testing.T) {
	f := newNativeFixture(t)
	f.contextsDelay = 2100 * time.Millisecond
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatalf("slow cleanup receipt tainted healthy worker: %v", err)
	}
	if !w.Ready() {
		t.Fatal("worker not ready after confirmed cleanup")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("owned cleanup not confirmed: %v %v", f.contexts, f.targets)
	}
	if len(f.methods) == 0 || f.methods[len(f.methods)-1] != "Target.getTargets" {
		t.Fatalf("cleanup did not complete target verification: %v", f.methods)
	}
}

func TestNativeStopLoadingFailureDoesNotInvalidateConfirmedCleanup(t *testing.T) {
	f := newNativeFixture(t)
	f.failStopLoading = true
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatalf("probe failed despite confirmed cleanup: %v", err)
	}
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); err != nil {
		t.Fatalf("fetch failed despite confirmed cleanup: %v", err)
	}
	if !w.Ready() {
		t.Fatal("confirmed cleanup tainted worker")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("owned resources remain or foreign target was removed: %v %v", f.contexts, f.targets)
	}
}

func TestNativeContextReceiptFailureTaintsWorker(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failContextReceipt = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("unconfirmed ownership remained ready: %v", err)
	}
}

func TestNativeTargetReceiptFailureTaintsWorker(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failTargetReceipt = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("unconfirmed target receipt remained ready: %v", err)
	}
}

func TestNativeEventOverflowTaintsEvenAfterOwnedCleanup(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.overflowAfterRequestBudget = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrBrowserEventOverflow) || !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("native overload did not fail and taint generation: %v ready=%v", err, w.Ready())
	}
	if fatal, ok := <-w.Fatal(); !ok || !errors.Is(fatal, ErrNativeWorkerFatal) {
		t.Fatalf("event overflow did not trigger fatal lifecycle: %v open=%v", fatal, ok)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("owned cleanup leaked or changed a foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativePausedRequestDrainsEventsWhileRPCIsPending(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestBurst = 1
	f.pauseEventFlood = 300
	f.pauseEventInterval = time.Millisecond
	f.continueDelay = 100 * time.Millisecond
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if err != nil || !w.Ready() {
		t.Fatalf("asynchronous request decision filled the event queue: %v ready=%v", err, w.Ready())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("async request processing leaked owned resources or changed foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativeEventRPCAdmissionIsBoundedAndResourceLimitIsRecoverable(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.pausedBurst = nativeEventRPCMaxInFlight + 1
	f.continueDelay = 100 * time.Millisecond
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, extractor.ErrWireLimit) || errors.Is(err, context.Canceled) || !w.Ready() {
		t.Fatalf("RPC cap did not fail as a recoverable resource limit: %v ready=%v", err, w.Ready())
	}
	f.mu.Lock()
	f.continueDelay = 0
	f.mu.Unlock()
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); err != nil || !w.Ready() {
		t.Fatalf("ordinary request failed after RPC admission limit: %v ready=%v", err, w.Ready())
	}
}

func TestNativeAsyncRequestRPCErrorStopsFetch(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestBurst = 1
	f.failContinueRequest = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	var protocolErr *cdproto.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != -32000 || !w.Ready() {
		t.Fatalf("asynchronous CDP failure was lost or tainted worker: %v ready=%v", err, w.Ready())
	}
}

func TestNativeDeniedPausedRequestIsNeverContinued(t *testing.T) {
	f := newNativeFixture(t)
	f.requestURL = "http://127.0.0.1/denied"
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestBurst = 1
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrUnsafePublicURL) || !w.Ready() {
		t.Fatalf("denied request did not fail with policy error: %v ready=%v", err, w.Ready())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	failed, continued := false, false
	for _, method := range f.methods {
		failed = failed || method == "Fetch.failRequest"
		continued = continued || method == "Fetch.continueRequest"
	}
	if !failed || continued {
		t.Fatalf("denied request decision failed=%v continued=%v methods=%v", failed, continued, f.methods)
	}
}

func TestNativeTerminalRequestDecisionStillProcessesOwnershipEvents(t *testing.T) {
	f := newNativeFixture(t)
	f.requestURL = "http://127.0.0.1/denied"
	f.detachAfterPaused = true
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestBurst = 1
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("ownership loss was ignored while denied request was settling: %v ready=%v", err, w.Ready())
	}
}

func TestNativeDetachedTargetUsesDetachedSessionNotEnvelope(t *testing.T) {
	tests := []struct {
		name, envelope, detached string
		malformed                bool
		wantErr                  error
		wantReady                bool
		wantOwnershipError       bool
	}{
		{name: "child detach delivered on owned envelope", envelope: "owned-session", detached: "child-session", wantReady: true},
		{name: "root scoped owned detach", envelope: "", detached: "owned-session", wantErr: ErrNativeOwnershipLost, wantOwnershipError: true},
		{name: "owned envelope owned detach", envelope: "owned-session", detached: "owned-session", wantErr: ErrNativeOwnershipLost, wantOwnershipError: true},
		{name: "foreign envelope cannot claim owned detach", envelope: "foreign-session", detached: "owned-session", wantReady: true},
		{name: "malformed root detach fails closed", envelope: "", malformed: true,
			wantErr: ErrNativePolicy, wantOwnershipError: true},
		{name: "malformed owned-envelope detach fails closed", envelope: "owned-session", malformed: true,
			wantErr: ErrNativePolicy, wantOwnershipError: true},
		{name: "malformed foreign-envelope detach ignored", envelope: "foreign-session", malformed: true, wantReady: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newNativeFixture(t)
			f.mu.Lock()
			f.requestBurst = 1
			f.detachAfterPaused = true
			f.detachOverride = true
			f.detachEnvelope = test.envelope
			f.detachSession = test.detached
			f.detachMalformed = test.malformed
			f.mu.Unlock()
			w := f.worker(t)
			var diagnostics [][2]string
			w.cfg.Diagnostic = func(stage, reason string) {
				diagnostics = append(diagnostics, [2]string{stage, reason})
			}
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Fetch error = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && err != nil {
				t.Fatalf("Fetch failed for unrelated detach: %v", err)
			}
			if w.Ready() != test.wantReady {
				t.Fatalf("Ready() = %v, want %v", w.Ready(), test.wantReady)
			}
			if test.wantOwnershipError && !errors.Is(err, ErrNativeOwnershipLost) {
				t.Fatalf("ambiguous/owned detach did not taint the generation: %v", err)
			}
			if test.wantErr == nil && len(doc.HTML) == 0 {
				t.Fatal("successful child/foreign detach should preserve the document")
			}
			if test.wantErr != nil && len(doc.HTML) != 0 {
				t.Fatalf("document escaped after detach failure: %d bytes", len(doc.HTML))
			}
			if test.wantOwnershipError {
				if fatal, ok := <-w.Fatal(); !ok || !errors.Is(fatal, ErrNativeWorkerFatal) {
					t.Fatalf("permanent detach did not trigger fatal lifecycle: %v open=%v", fatal, ok)
				}
				wantReason := "owned_detach"
				if test.malformed {
					wantReason = "malformed_detach"
				}
				if len(diagnostics) != 1 || diagnostics[0] != [2]string{"events", wantReason} {
					t.Fatalf("unexpected finite detach diagnostic: %v", diagnostics)
				}
				f.mu.Lock()
				methodsAfterFailure := len(f.methods)
				f.mu.Unlock()
				if _, nextErr := w.Fetch(context.Background(), "https://origin.example/next", NativeBrowserOptions{}); !errors.Is(nextErr, ErrNativeOwnershipLost) {
					t.Fatalf("tainted browser accepted a later fetch: %v", nextErr)
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				if len(f.methods) != methodsAfterFailure {
					t.Fatalf("tainted browser allocated resources on a later fetch: before=%d after=%d methods=%v",
						methodsAfterFailure, len(f.methods), f.methods)
				}
			}
		})
	}
}

func TestNativeCleanupOverflowClearsCompletedHTML(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.cleanupEventFlood = 1024
	f.mu.Unlock()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrBrowserEventOverflow) || !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("cleanup overload was not surfaced and tainted: %v ready=%v", err, w.Ready())
	}
	if len(doc.HTML) != 0 {
		t.Fatalf("HTML escaped after cleanup overload: %d bytes", len(doc.HTML))
	}
}

func TestNativePageMonitorMinimumSettleCapturesLateArticle(t *testing.T) {
	f := newNativeFixture(t)
	f.lateAppendAfter = 1400 * time.Millisecond
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{BrowserOptions: BrowserOptions{MinSettle: 2 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < 2*time.Second {
		t.Fatalf("minimum settle returned too early: %v", time.Since(started))
	}
	if !strings.Contains(string(doc.HTML), "Late second post.") {
		t.Fatalf("late post lost: %s", doc.HTML)
	}
}

func TestNativeNeverSendsProxySecretToOriginOrOtherProxy(t *testing.T) {
	for _, tc := range []struct{ source, origin string }{{"Server", "https://origin.example"}, {"Proxy", "http://other-gateway.test:3128"}} {
		f := newNativeFixture(t)
		w := f.worker(t)
		if err := w.Probe(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.authSource = tc.source
		f.authOrigin = tc.origin
		f.mu.Unlock()
		_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
		var status *PublicHTTPStatusError
		if !errors.As(err, &status) {
			t.Fatalf("auth challenge not rejected: %v", err)
		}
		f.mu.Lock()
		if f.proxyAuth["response"] != "CancelAuth" || f.proxyAuth["password"] != nil {
			t.Error("proxy secret leaked to unauthorized challenge")
		}
		f.mu.Unlock()
	}
}

func TestNativeRateLimitAndCancellationCleanup(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.status = 429
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	var status *PublicHTTPStatusError
	if !errors.As(err, &status) || status.StatusCode != 429 || status.RetryAfterSeconds == nil || *status.RetryAfterSeconds != 30 {
		t.Fatalf("rate-limit metadata lost: %v", err)
	}
	f.mu.Lock()
	f.status = 200
	f.empty = true
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = w.Fetch(ctx, "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, context.DeadlineExceeded) || !w.Ready() {
		f.mu.Lock()
		methods := append([]string(nil), f.methods...)
		contexts := len(f.contexts)
		targets := len(f.targets)
		f.mu.Unlock()
		t.Fatalf("cancel cleanup failed: %v ready=%v methods=%v contexts=%d targets=%d", err, w.Ready(), methods, contexts, targets)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 {
		t.Fatalf("cancel leaked owned resources: %v %v", f.contexts, f.targets)
	}
}

func TestNativeOwnershipLossPermanentlyDisablesGeneration(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failCleanup = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("unconfirmed ownership remained ready: %v", err)
	}
	if err := w.Probe(context.Background()); !errors.Is(err, ErrNativeOwnershipLost) {
		t.Fatalf("tainted worker retried: %v", err)
	}
}

func TestNativeBrowserPinnedCloakProbe(t *testing.T) {
	endpoint := os.Getenv("GO_NATIVE_BROWSER_TEST_ENDPOINT")
	if endpoint == "" {
		if os.Getenv("GO_NATIVE_BROWSER_TEST_REQUIRED") == "1" {
			t.Fatal("GO_NATIVE_BROWSER_TEST_ENDPOINT required")
		}
		t.Skip("set dedicated pinned Cloak endpoint for native capability probe")
	}
	proxy := os.Getenv("GO_NATIVE_BROWSER_TEST_PROXY")
	if proxy == "" {
		proxy = "http://127.0.0.1:9"
	}
	token := os.Getenv("GO_NATIVE_BROWSER_TEST_PROXY_TOKEN")
	if token == "" {
		token = "probe-only-token-16"
	}
	w, err := NewNativeBrowser(NativeBrowserConfig{Endpoint: endpoint, ProxyURL: proxy, ProxyToken: token, NetworkIsolationVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err = w.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	if !w.Ready() {
		t.Fatal("probe did not establish readiness")
	}
	if err = w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Ready() {
		t.Fatal("closed native worker ready")
	}
	if w.CheckReady(context.Background()) {
		t.Fatal("closed native worker recovered readiness")
	}
}

func TestNativeBrowserControlledOrigin(t *testing.T) {
	endpoint, proxy, raw := os.Getenv("GO_NATIVE_BROWSER_TEST_ENDPOINT"), os.Getenv("GO_NATIVE_BROWSER_TEST_PROXY"), os.Getenv("GO_NATIVE_BROWSER_TEST_URL")
	if endpoint == "" || proxy == "" || raw == "" {
		t.Skip("requires dedicated Cloak, isolated authenticated gateway and HTTPS fixture")
	}
	w, err := NewNativeBrowser(NativeBrowserConfig{Endpoint: endpoint, ProxyURL: proxy, ProxyToken: os.Getenv("GO_NATIVE_BROWSER_TEST_PROXY_TOKEN"), NetworkIsolationVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = w.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := w.Fetch(ctx, raw, NativeBrowserOptions{ReadySelector: os.Getenv("GO_NATIVE_BROWSER_TEST_SELECTOR")})
	if err != nil {
		t.Fatal(err)
	}
	if got.StatusCode != 200 || len(got.HTML) == 0 || got.DOMBytes != len(got.HTML) {
		t.Fatalf("invalid controlled native result: %+v", got)
	}
	if anchor := os.Getenv("GO_NATIVE_BROWSER_TEST_ANCHOR"); anchor != "" && !strings.Contains(string(got.HTML), anchor) {
		t.Fatalf("controlled fixture did not reach expected rendered state")
	}
	if !w.Ready() {
		t.Fatal("cleaned successful job lost readiness")
	}
}

func TestNativeDOMByteLimitRechecksScalarReply(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{BrowserOptions: BrowserOptions{MaxDOMBytes: 32}})
	if !errors.Is(err, extractor.ErrOutputLimit) || !w.Ready() {
		t.Fatalf("DOM guard or cleanup failed: %v", err)
	}
}

func TestNativeDisconnectLosesReadinessWithoutReplay(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	var diagnostics [][2]string
	w.cfg.Diagnostic = func(stage, reason string) {
		diagnostics = append(diagnostics, [2]string{stage, reason})
	}
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.disconnect = true
	f.mu.Unlock()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() || len(doc.HTML) != 0 {
		t.Fatalf("lost connection remained ready: %v", err)
	}
	disconnected, transportFailure, cleanupFailure := false, false, false
	for _, diagnostic := range diagnostics {
		disconnected = disconnected || diagnostic == [2]string{"events", "client_disconnected"}
		transportFailure = transportFailure || diagnostic == [2]string{"events", "transport_error"} || diagnostic == [2]string{"dom", "transport_error"} || diagnostic == [2]string{"navigate", "transport_error"}
		cleanupFailure = cleanupFailure || diagnostic == [2]string{"cleanup", "ownership_loss"}
	}
	// The event consumer or pending RPC can observe the socket closing first;
	// cleanup may then report the same lost connection as ownership loss.
	if !disconnected && !(transportFailure && cleanupFailure) {
		t.Fatalf("unexpected finite disconnect diagnostic: %v", diagnostics)
	}
	f.mu.Lock()
	methods := len(f.methods)
	f.mu.Unlock()
	if _, err := w.Fetch(context.Background(), "https://origin.example/next", NativeBrowserOptions{}); !errors.Is(err, ErrNativeOwnershipLost) {
		t.Fatalf("disconnected generation accepted another job: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.methods) != methods {
		t.Fatal("disconnected generation allocated resources on the next job")
	}
}

func TestNativeTargetCrashRecoversOnlyAfterOwnedCleanupAndBrowserHealthCheck(t *testing.T) {
	f := newNativeFixture(t)
	f.mu.Lock()
	f.targetCrashed = true
	f.mu.Unlock()
	w := f.worker(t)
	var diagnostics [][2]string
	w.cfg.Diagnostic = func(stage, reason string) {
		diagnostics = append(diagnostics, [2]string{stage, reason})
	}
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeTargetCrashed) || errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || !w.Ready() {
		t.Fatalf("owned target crash did not recover for the next job: err=%v ready=%v", err, w.Ready())
	}
	if len(diagnostics) != 1 || diagnostics[0] != [2]string{"events", "target_crashed"} {
		t.Fatalf("unexpected finite crash diagnostic: %v", diagnostics)
	}
	if _, err := w.Fetch(context.Background(), "https://origin.example/next", NativeBrowserOptions{}); err != nil || !w.Ready() {
		t.Fatalf("next independent job failed after recovered crash: %v ready=%v", err, w.Ready())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("crash recovery leaked owned resources or changed a foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativeTargetCrashSkipsUnresponsivePageStopButDisposesContext(t *testing.T) {
	f := newNativeFixture(t)
	f.mu.Lock()
	f.targetCrashed = true
	f.mu.Unlock()
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.hangStopLoading = true
	stopsBefore := 0
	for _, method := range f.methods {
		if method == "Page.stopLoading" {
			stopsBefore++
		}
	}
	f.mu.Unlock()
	started := time.Now()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeTargetCrashed) || errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || !w.Ready() {
		t.Fatalf("crashed page stop prevented safe context cleanup: err=%v ready=%v", err, w.Ready())
	}
	if time.Since(started) > time.Second {
		t.Fatal("crashed renderer consumed the cleanup deadline before disposal")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stopsAfter, disposals := 0, 0
	for _, method := range f.methods {
		if method == "Page.stopLoading" {
			stopsAfter++
		}
		if method == "Target.disposeBrowserContext" {
			disposals++
		}
	}
	if stopsAfter != stopsBefore || disposals != 2 || len(f.contexts) != 0 || len(f.targets) != 1 {
		t.Fatalf("crash cleanup did not dispose only owned context: stops=%d/%d disposals=%d contexts=%v targets=%v", stopsBefore, stopsAfter, disposals, f.contexts, f.targets)
	}
}

func TestNativeTargetCrashRequiresCleanReceiptsAndSameGeneration(t *testing.T) {
	for _, mode := range []string{"cleanup", "context_receipt", "target_receipt", "generation", "discovery", "disconnected", "owned_detach"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeFixture(t)
			f.mu.Lock()
			f.targetCrashed = true
			f.mu.Unlock()
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			switch mode {
			case "cleanup":
				f.failCleanup = true
			case "context_receipt":
				f.failContextReceipt = true
			case "target_receipt":
				f.failTargetReceipt = true
			case "generation":
				f.changeGenerationOnCrash = true
			case "disconnected":
				f.disconnectOnCrash = true
			case "discovery":
				f.unavailableOnCrash = true
			case "owned_detach":
				f.detachOnCrash = true
			}
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
			if !errors.Is(err, ErrNativeTargetCrashed) || !errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || w.Ready() {
				t.Fatalf("unsafe crash recovery for %s: err=%v ready=%v", mode, err, w.Ready())
			}
			if _, err := w.Fetch(context.Background(), "https://origin.example/next", NativeBrowserOptions{}); !errors.Is(err, ErrNativeOwnershipLost) {
				t.Fatalf("terminal crash state allowed another job: %v", err)
			}
		})
	}
}

func TestNativeForeignTargetCrashDoesNotTaintOwnedWorker(t *testing.T) {
	f := newNativeFixture(t)
	f.mu.Lock()
	f.targetCrashed = true
	f.foreignTargetCrash = true
	f.mu.Unlock()
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if err != nil || len(doc.HTML) == 0 || !w.Ready() {
		t.Fatalf("foreign crash affected owned page: err=%v ready=%v", err, w.Ready())
	}
}

func TestNativeTargetCrashCannotRestoreReadinessAfterClose(t *testing.T) {
	f := newNativeFixture(t)
	f.mu.Lock()
	f.targetCrashed = true
	f.mu.Unlock()
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	crashSeen := make(chan struct{}, 1)
	w.cfg.Diagnostic = func(stage, reason string) {
		if stage == "events" && reason == "target_crashed" {
			select {
			case crashSeen <- struct{}{}:
			default:
			}
		}
	}
	f.mu.Lock()
	f.contextsDelay = 100 * time.Millisecond
	f.mu.Unlock()
	type fetchResult struct {
		doc RenderedDocument
		err error
	}
	fetched := make(chan fetchResult, 1)
	go func() {
		doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
		fetched <- fetchResult{doc: doc, err: err}
	}()
	select {
	case <-crashSeen:
	case <-time.After(time.Second):
		t.Fatal("owned target crash was not observed")
	}
	closed := make(chan error, 1)
	go func() { closed <- w.Close(context.Background()) }()
	select {
	case result := <-fetched:
		if result.err == nil || len(result.doc.HTML) != 0 || w.Ready() {
			t.Fatalf("close race restored readiness or returned content: err=%v ready=%v", result.err, w.Ready())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch did not finish during close")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not join the active fetch")
	}
}

func TestNativeTargetCrashCombinedWithEventOverflowStaysTerminal(t *testing.T) {
	f := newNativeFixture(t)
	f.mu.Lock()
	f.targetCrashed = true
	f.mu.Unlock()
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.cleanupEventFlood = 1024
	f.mu.Unlock()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeTargetCrashed) || !errors.Is(err, ErrBrowserEventOverflow) || !errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || w.Ready() {
		t.Fatalf("crash plus overflow did not remain terminal: err=%v ready=%v", err, w.Ready())
	}
}

func TestNativeCrashRecoveryRejectsMixedLateRPCFailures(t *testing.T) {
	lateRPC := errors.New("late protocol failure")
	for _, tc := range []struct {
		name       string
		err        error
		connected  bool
		cleanupErr error
		want       bool
	}{
		{name: "pure crash", err: ErrNativeTargetCrashed, connected: true, want: true},
		{name: "late rpc error", err: errors.Join(ErrNativeTargetCrashed, lateRPC), connected: true},
		{name: "ownership failure", err: errors.Join(ErrNativeTargetCrashed, ErrNativeOwnershipLost), connected: true},
		{name: "event overflow", err: errors.Join(ErrNativeTargetCrashed, ErrBrowserEventOverflow), connected: true},
		{name: "disconnected cdp", err: ErrNativeTargetCrashed},
		{name: "cleanup failure", err: ErrNativeTargetCrashed, connected: true, cleanupErr: ErrNativeOwnershipLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nativeCrashRecoveryAllowed(true, tc.err, tc.connected, tc.cleanupErr); got != tc.want {
				t.Fatalf("recovery eligibility = %v, want %v", got, tc.want)
			}
		})
	}
	if !nativeCrashCancellation(context.Canceled, ErrNativeTargetCrashed, true) {
		t.Fatal("expected cancellation caused by the owned crash was treated as a late RPC failure")
	}
	if nativeCrashCancellation(lateRPC, ErrNativeTargetCrashed, true) ||
		nativeCrashCancellation(errors.Join(context.Canceled, lateRPC), ErrNativeTargetCrashed, true) ||
		nativeCrashCancellation(context.Canceled, context.Canceled, true) {
		t.Fatal("substantive or unrelated RPC failure was hidden")
	}
}

func TestNativeLostCreateReceiptRequiresSupervisorRecovery(t *testing.T) {
	f := newNativeFixture(t)
	f.mu.Lock()
	f.dropReply = true
	f.mu.Unlock()
	w := f.worker(t)
	if err := w.Probe(context.Background()); !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("unknown created context was treated as clean: %v", err)
	}
	if w.CheckReady(context.Background()) {
		t.Fatal("worker with an unconfirmed cleanup receipt recovered readiness")
	}
	if err := w.Probe(context.Background()); !errors.Is(err, ErrNativeOwnershipLost) {
		t.Fatalf("lost ownership retried: %v", err)
	}
}

func TestNativeBusyAndCloseDrainOwnedJob(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.empty = true
	f.mu.Unlock()
	finished := make(chan error, 1)
	go func() {
		_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
		finished <- err
	}()
	select {
	case <-f.navigated:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not navigate")
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	queued := make(chan error, 1)
	go func() {
		_, err := w.Fetch(waitCtx, "https://origin.example/second", NativeBrowserOptions{})
		queued <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("drain did not cancel owned job: %v", err)
	}
	if err := <-queued; !errors.Is(err, ErrNativeNotReady) {
		t.Fatalf("queued operation did not stop on close: %v", err)
	}
	if w.Ready() {
		t.Fatal("drained worker ready")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("drain did not clean only owned resources: %v %v", f.contexts, f.targets)
	}
}

func TestNativeBrowserConcurrentRendersUseIndependentTargets(t *testing.T) {
	f := newNativeFixture(t)
	w, err := NewNativeBrowser(NativeBrowserConfig{Endpoint: f.server.URL, ProxyURL: "http://gateway.test:3128", ProxyToken: "sixteen-byte-token", NetworkIsolationVerified: true, MaxConcurrent: 2, MaxWaiters: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.navigateDelay = 150 * time.Millisecond
	f.mu.Unlock()
	results := make(chan error, 3)
	for _, raw := range []string{"https://origin.example/one", "https://origin.example/two"} {
		go func(raw string) {
			_, err := w.Fetch(context.Background(), raw, NativeBrowserOptions{BrowserOptions: BrowserOptions{Timeout: 3 * time.Second}})
			results <- err
		}(raw)
	}
	for range 2 {
		select {
		case <-f.navigateStarts:
		case <-time.After(time.Second):
			t.Fatal("two render operations did not reach navigation concurrently")
		}
	}
	if len(w.slots) != 2 {
		t.Fatalf("native render slots active=%d, want 2", len(w.slots))
	}
	go func() {
		_, err := w.Fetch(context.Background(), "https://origin.example/three", NativeBrowserOptions{BrowserOptions: BrowserOptions{Timeout: 3 * time.Second}})
		results <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		w.mu.Lock()
		waiters := len(w.waiters)
		w.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("third operation did not wait for native render admission")
		}
		time.Sleep(time.Millisecond)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("parallel render failed: %v", err)
		}
	}
	select {
	case <-f.navigateStarts:
	case <-time.After(time.Second):
		t.Fatal("third operation did not run after a slot became available")
	}
	if err := <-results; err != nil {
		t.Fatalf("queued third render failed: %v", err)
	}
	if !w.Ready() {
		t.Fatal("one operation tainted readiness after sibling success")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("parallel cleanup leaked or touched a foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativeBrowserWaiterIsBoundedAndCancelable(t *testing.T) {
	f := newNativeFixture(t)
	w, err := NewNativeBrowser(NativeBrowserConfig{Endpoint: f.server.URL, ProxyURL: "http://gateway.test:3128", ProxyToken: "sixteen-byte-token", NetworkIsolationVerified: true, MaxConcurrent: 1, MaxWaiters: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.navigateDelay = 250 * time.Millisecond
	f.mu.Unlock()
	active := make(chan error, 1)
	go func() {
		_, err := w.Fetch(context.Background(), "https://origin.example/active", NativeBrowserOptions{})
		active <- err
	}()
	select {
	case <-f.navigateStarts:
	case <-time.After(time.Second):
		t.Fatal("active render did not start")
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	queued := make(chan error, 1)
	go func() {
		_, err := w.Fetch(waitCtx, "https://origin.example/queued", NativeBrowserOptions{})
		queued <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		w.mu.Lock()
		waiters := len(w.waiters)
		w.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second operation did not enter bounded wait queue")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := w.Fetch(context.Background(), "https://origin.example/rejected", NativeBrowserOptions{}); !errors.Is(err, ErrNativeBusy) {
		t.Fatalf("waiter cap did not reject excess caller: %v", err)
	}
	cancelWait()
	if err := <-queued; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued render ignored cancellation: %v", err)
	}
	if err := <-active; err != nil {
		t.Fatalf("active render was affected by sibling cancellation: %v", err)
	}
	if !w.Ready() {
		t.Fatal("canceling a waiter tainted the browser generation")
	}
}

func TestNativeBrowserGenerationTaintCancelsEveryActiveOperation(t *testing.T) {
	f := newNativeFixture(t)
	w, err := NewNativeBrowser(NativeBrowserConfig{Endpoint: f.server.URL, ProxyURL: "http://gateway.test:3128", ProxyToken: "sixteen-byte-token", NetworkIsolationVerified: true, MaxConcurrent: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.empty = true
	f.navigateDelay = time.Second
	f.contexts["foreign-context"] = true
	f.mu.Unlock()
	results := make(chan error, 3)
	for _, raw := range []string{"https://origin.example/one", "https://origin.example/two"} {
		go func(raw string) {
			_, err := w.Fetch(context.Background(), raw, NativeBrowserOptions{})
			results <- err
		}(raw)
	}
	for range 2 {
		select {
		case <-f.navigateStarts:
		case <-time.After(time.Second):
			t.Fatal("both operations did not become active")
		}
	}
	go func() {
		_, err := w.Fetch(context.Background(), "https://origin.example/waiting", NativeBrowserOptions{})
		results <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		w.mu.Lock()
		waiters := len(w.waiters)
		w.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiting operation did not enter admission queue")
		}
		time.Sleep(time.Millisecond)
	}
	f.mu.Lock()
	f.contextsDelay = time.Second
	f.mu.Unlock()
	w.taint()
	select {
	case err := <-results:
		if !errors.Is(err, ErrNativeOwnershipLost) {
			t.Fatalf("admission waiter did not wake on generation loss: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("admission waiter remained blocked while active cleanup drained")
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("generation loss did not cancel active render")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("active render did not drain after generation loss")
		}
	}
	if w.Ready() || len(w.operations) != 0 || len(w.slots) != 0 {
		t.Fatal("generation taint left active operations or slots")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 1 || !f.contexts["foreign-context"] || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("generation taint leaked owned resources or harmed foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativeBrowserIgnoresSiblingSessionDetach(t *testing.T) {
	f := newNativeFixture(t)
	f.foreignDetachOnNavigate = true
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); err != nil || !w.Ready() {
		t.Fatalf("foreign session detach tainted owned render: err=%v ready=%v", err, w.Ready())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("sibling detach leaked resources or touched foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativeDiscoveryLossClearsReadiness(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.discoveryUnavailable = true
	f.mu.Unlock()
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); !errors.Is(err, ErrNativeNotReady) || w.Ready() {
		t.Fatalf("unavailable browser retained readiness: %v ready=%v", err, w.Ready())
	}
}

func TestNativeDiagnosticReportsOnlyFiniteDiscoveryFailure(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	var diagnostics [][2]string
	w.cfg.Diagnostic = func(stage, reason string) {
		diagnostics = append(diagnostics, [2]string{stage, reason})
	}
	f.mu.Lock()
	f.discoveryUnavailable = true
	f.mu.Unlock()
	if err := w.Probe(context.Background()); !errors.Is(err, ErrNativeNotReady) {
		t.Fatalf("expected discovery failure, got %v", err)
	}
	if len(diagnostics) != 1 || diagnostics[0] != [2]string{"discovery", "operation_failed"} {
		t.Fatalf("unexpected safe diagnostics: %v", diagnostics)
	}
	for _, err := range []error{context.DeadlineExceeded, context.Canceled,
		ErrBrowserEventOverflow, ErrNativeOwnershipLost} {
		reason := nativeDiagnosticReason(err)
		if strings.Contains(reason, " ") || strings.Contains(reason, "https") || reason == "" {
			t.Fatalf("unsafe diagnostic reason %q", reason)
		}
	}
}

func TestNativeIdleReadinessRejectsUnavailableOrReplacedBrowser(t *testing.T) {
	for _, mode := range []string{"unavailable", "replacement", "timeout"} {
		f := newNativeFixture(t)
		w := f.worker(t)
		if err := w.Probe(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !w.CheckReady(context.Background()) {
			t.Fatal("healthy verified generation not ready")
		}
		f.mu.Lock()
		switch mode {
		case "unavailable":
			f.discoveryUnavailable = true
		case "replacement":
			f.discoveryID = "new-browser-generation"
		case "timeout":
			f.discoveryDelay = time.Second
		}
		f.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		started := time.Now()
		ready := w.CheckReady(ctx)
		cancel()
		if ready || w.Ready() {
			t.Fatalf("idle health accepted %s", mode)
		}
		if mode == "timeout" && time.Since(started) > 500*time.Millisecond {
			t.Fatal("idle health exceeded caller budget")
		}
		f.mu.Lock()
		f.discoveryUnavailable = false
		f.discoveryID = "native"
		f.discoveryDelay = 0
		f.mu.Unlock()
		if mode == "replacement" {
			if w.CheckReady(context.Background()) || w.Ready() {
				t.Fatal("replacement generation recovered the worker")
			}
			if fatal, ok := <-w.Fatal(); !ok || !errors.Is(fatal, ErrNativeWorkerFatal) {
				t.Fatalf("generation replacement did not trigger fatal lifecycle: %v open=%v", fatal, ok)
			}
		} else if !w.CheckReady(context.Background()) || !w.Ready() {
			t.Fatalf("same verified generation did not recover after %s", mode)
		}
	}
}

func TestNativeTransientDiscoveryTimeoutRecoversOnlySameVerifiedGeneration(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if w.CheckReady(context.Background()) {
		t.Fatal("CheckReady bootstrapped readiness before an active probe")
	}
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !w.CheckReady(context.Background()) {
		t.Fatal("healthy verified generation not ready")
	}

	f.mu.Lock()
	f.discoveryDelay = time.Second
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if w.CheckReady(ctx) {
		cancel()
		t.Fatal("readiness check accepted a discovery timeout")
	}
	cancel()

	f.mu.Lock()
	f.discoveryDelay = 0
	f.mu.Unlock()
	current, err := configuredBrowserEndpoint(context.Background(), f.server.URL)
	if err != nil {
		t.Fatalf("fixture discovery did not recover: %v", err)
	}
	w.mu.Lock()
	original := w.generation
	ready, tainted := w.ready, w.tainted
	w.mu.Unlock()
	if current != original || ready || tainted {
		t.Fatalf("expected unchanged generation with cleared readiness: same=%v ready=%v tainted=%v", current == original, ready, tainted)
	}
	if !w.CheckReady(context.Background()) || !w.Ready() {
		t.Fatal("CheckReady did not recover after successful discovery of the unchanged generation")
	}
}

func TestNativeFatalSignalIsOneShotAndConcurrentTaintIsIdempotent(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	fatal := w.Fatal()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.taint()
		}()
	}
	wg.Wait()
	select {
	case err, ok := <-fatal:
		if !ok || !errors.Is(err, ErrNativeWorkerFatal) {
			t.Fatalf("fatal notification = %v, open=%v", err, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("permanent taint did not notify")
	}
	if _, ok := <-fatal; ok {
		t.Fatal("fatal channel emitted more than one notification")
	}
}

func TestNativeNonFatalLifecycleCasesDoNotSignalFatal(t *testing.T) {
	t.Run("normal close", func(t *testing.T) {
		f := newNativeFixture(t)
		w := f.worker(t)
		if err := w.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		select {
		case err, ok := <-w.Fatal():
			t.Fatalf("normal close signaled fatal: %v open=%v", err, ok)
		default:
		}
	})
	t.Run("transient discovery", func(t *testing.T) {
		f := newNativeFixture(t)
		w := f.worker(t)
		if err := w.Probe(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.discoveryUnavailable = true
		f.mu.Unlock()
		if w.CheckReady(context.Background()) {
			t.Fatal("unavailable discovery remained ready")
		}
		select {
		case err, ok := <-w.Fatal():
			t.Fatalf("transient discovery signaled fatal: %v open=%v", err, ok)
		default:
		}
	})
	t.Run("owned crash recovery", func(t *testing.T) {
		f := newNativeFixture(t)
		f.mu.Lock()
		f.targetCrashed = true
		f.mu.Unlock()
		w := f.worker(t)
		if err := w.Probe(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); !errors.Is(err, ErrNativeTargetCrashed) || !w.Ready() {
			t.Fatalf("owned crash did not recover: err=%v ready=%v", err, w.Ready())
		}
		select {
		case err, ok := <-w.Fatal():
			t.Fatalf("owned crash signaled fatal: %v open=%v", err, ok)
		default:
		}
	})
}

func TestNativeCheckReadyPreservesHealthyActiveWorkerButDoesNotRecoverDuringJob(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.discoveryDelay = time.Second
	f.mu.Unlock()

	w.mu.Lock()
	w.operations[0] = func(error) {}
	w.mu.Unlock()
	started := time.Now()
	if !w.CheckReady(context.Background()) || !w.Ready() {
		t.Fatal("healthy active worker became unavailable")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("active readiness unnecessarily waited on discovery")
	}

	w.mu.Lock()
	w.ready = false // Simulate a transient failure observed by the active job.
	w.mu.Unlock()
	if w.CheckReady(context.Background()) || w.Ready() {
		t.Fatal("active worker recovered after readiness had been cleared")
	}
	w.mu.Lock()
	delete(w.operations, 0)
	w.mu.Unlock()
	f.mu.Lock()
	f.discoveryDelay = 0
	f.mu.Unlock()
	if !w.CheckReady(context.Background()) || !w.Ready() {
		t.Fatal("same generation did not recover after the active job ended")
	}
}

func TestNativeAtomicContextDisposalDoesNotCloseTargetFirst(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	seenDispose := false
	for _, method := range f.methods {
		if method == "Target.closeTarget" || method == "Target.detachFromTarget" {
			t.Fatalf("racy pre-disposal target/session closure: %s", method)
		}
		seenDispose = seenDispose || method == "Target.disposeBrowserContext"
	}
	if !seenDispose || len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("atomic cleanup unconfirmed: %v %v", f.contexts, f.targets)
	}
}

func TestNativeMainFrameStatusIgnoresIframeDocument(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.childStatus = 429
	f.mu.Unlock()
	got, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if err != nil || got.StatusCode != 200 {
		t.Fatalf("iframe document replaced main status: %+v %v", got, err)
	}
}

func TestNativeDownloadsUseOwnedFrameIDsNotTargetIDs(t *testing.T) {
	for _, frame := range []string{"main-frame", "child-frame", "foreign-frame"} {
		f := newNativeFixture(t)
		w := f.worker(t)
		if err := w.Probe(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.downloadFrame = frame
		f.mu.Unlock()
		_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
		if frame == "foreign-frame" {
			if err != nil {
				t.Fatalf("foreign download affected own job: %v", err)
			}
		} else if !errors.Is(err, ErrNativeUnsupported) {
			t.Fatalf("owned frame download not denied: %s %v", frame, err)
		}
	}
}

func TestNativeNavigateDownloadWithoutEventIsUnsupported(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.navigateDownload = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeUnsupported) || !w.Ready() {
		t.Fatalf("download result not classified without event: %v ready=%v", err, w.Ready())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("download leaked owned resources or changed foreign target: %v %v", f.contexts, f.targets)
	}
}

func TestNativeCumulativeRequestBudgetIncludesFinishedZeroByteRequests(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestBurst = 129
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, extractor.ErrWireLimit) {
		t.Fatalf("sequential zero-byte requests bypassed job budget: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	continued := 0
	for _, method := range f.methods {
		if method == "Fetch.continueRequest" {
			continued++
		}
	}
	if continued != 128 {
		t.Fatalf("continued %d requests before cumulative rejection, want 128", continued)
	}
	if !w.Ready() || len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatal("request-budget rejection lost readiness or owned cleanup")
	}
}
