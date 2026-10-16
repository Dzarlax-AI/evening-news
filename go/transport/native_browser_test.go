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
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type nativeFixture struct {
	mu                                        sync.Mutex
	contexts, targets                         map[string]bool
	methods                                   []string
	proxyAuth                                 map[string]any
	status                                    int
	childStatus                               int
	downloadFrame                             string
	navigateDownload                          bool
	requestBurst                              int
	authSource, authOrigin                    string
	empty, dropReply, failCleanup, failStopLoading, failContextReceipt, disconnect bool
	discoveryUnavailable                      bool
	discoveryID                               string
	discoveryDelay                            time.Duration
	contextsDelay                             time.Duration
	navigated                                 chan struct{}
	server                                    *httptest.Server
}

func newNativeFixture(t *testing.T) *nativeFixture {
	t.Helper()
	f := &nativeFixture{contexts: map[string]bool{}, targets: map[string]bool{"foreign-sentinel": true}, status: 200, navigated: make(chan struct{}), discoveryID: "native"}
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
		contextID, targetID, sessionID := "owned-context", "owned-target", "owned-session"
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
				if f.failCleanup {
					protocolErr = true
				} else {
					delete(f.contexts, contextID)
					delete(f.targets, targetID)
				}
			case "Page.stopLoading":
				protocolErr = f.failStopLoading
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
			case "Target.getTargets":
				infos := []any{}
				for id := range f.targets {
					infos = append(infos, map[string]string{"targetId": id})
				}
				result["targetInfos"] = infos
			case "Page.navigate":
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
				if f.authSource != "" {
					event = map[string]any{"method": "Fetch.authRequired", "sessionId": sessionID, "params": map[string]any{"requestId": "auth", "authChallenge": map[string]string{"source": f.authSource, "origin": f.authOrigin}}}
				}
				if !write(event) {
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
				for i := 0; i < f.requestBurst; i++ {
					id := fmt.Sprint(i)
					if !write(map[string]any{"method": "Network.requestWillBeSent", "sessionId": sessionID, "params": map[string]string{"requestId": id, "type": "XHR", "frameId": "main-frame"}}) {
						break
					}
					write(map[string]any{"method": "Network.loadingFinished", "sessionId": sessionID, "params": map[string]any{"requestId": id, "encodedDataLength": 0}})
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
				value := map[string]any{"ready": false}
				if !f.empty {
					value = map[string]any{"ready": true, "html": `<html><body><article>Native browser fixture.</article></body></html>`, "bytes": 78, "url": "https://origin.example/final"}
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
		t.Fatalf("cancel cleanup failed: %v ready=%v", err, w.Ready())
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
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.disconnect = true
	f.mu.Unlock()
	_, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || w.Ready() {
		t.Fatalf("lost connection remained ready: %v", err)
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
	if _, err := w.Fetch(context.Background(), "https://origin.example/second", NativeBrowserOptions{}); !errors.Is(err, ErrNativeBusy) {
		t.Fatalf("unbounded native admission: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("drain did not cancel owned job: %v", err)
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
		if w.CheckReady(context.Background()) {
			t.Fatal("discovery alone silently recovered worker")
		}
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
}
