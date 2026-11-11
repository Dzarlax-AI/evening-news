package transport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type nativeFixtureChild struct {
	kind, context, target, session, envelope, activity string
	paused, malformed                                  bool
}

// TestNativePausedWorkersPreserveMainPageAndCleanup keeps worker code paused while enforcing
// unchanged main-page readiness and owned cleanup.
func TestNativePausedWorkersPreserveMainPageAndCleanup(t *testing.T) {
	for _, kind := range []string{"service_worker"} {
		for _, late := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/navigation", true: "/runtime"}[late], func(t *testing.T) {
				f := newNativeFixture(t)
				w := f.worker(t)
				if err := w.Probe(context.Background()); err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				f.childEvents = []nativeFixtureChild{{kind: kind, paused: true}}
				f.childEventsAtRuntime = late
				f.mu.Unlock()
				doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
				if err != nil || doc.StatusCode != 200 || !strings.Contains(string(doc.HTML), "Native browser fixture") {
					t.Fatalf("paused %s discarded main page: status=%d err=%v", kind, doc.StatusCode, err)
				}
				assertNativeChildrenNeverResumed(t, f)
				if !w.Ready() {
					t.Fatal("successful owned cleanup lost readiness")
				}
			})
		}
	}
}

// assertNativeChildrenNeverResumed checks that held worker sessions never receive a resume
// command.
func assertNativeChildrenNeverResumed(t *testing.T, f *nativeFixture) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, method := range f.methods {
		switch method {
		case "Runtime.runIfWaitingForDebugger", "Target.detachFromTarget", "Browser.close":
			t.Errorf("paused child may escape ownership: %s", method)
		}
	}
	if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
		t.Fatalf("cleanup leaked owned resources or touched foreign target: contexts=%d targets=%v", len(f.contexts), f.targets)
	}
}

// TestNativeChildTargetsFailClosed rejects ambiguous child identity, pause state and unsupported
// target kinds.
func TestNativeChildTargetsFailClosed(t *testing.T) {
	cases := []struct {
		name, reason string
		children     []nativeFixtureChild
	}{
		{"running worker", "child_target_unpaused", []nativeFixtureChild{{kind: "service_worker"}}},
		{"foreign context", "child_target_invalid", []nativeFixtureChild{{kind: "service_worker", paused: true, context: "foreign-context"}}},
		{"missing session", "child_target_invalid", []nativeFixtureChild{{kind: "service_worker", paused: true, malformed: true}}},
		{"parent session", "child_target_invalid", []nativeFixtureChild{{kind: "service_worker", paused: true, session: "owned-session"}}},
		{"unknown target type", "child_target_unsupported", []nativeFixtureChild{{kind: "UNTRUSTED_CANARY", paused: true}}},
		{"child page", "child_page_denied", []nativeFixtureChild{{kind: "page", paused: true}}},
		{"unpaused iframe", "child_target_unpaused", []nativeFixtureChild{{kind: "iframe"}}},
		{"dedicated worker", "child_target_unsupported", []nativeFixtureChild{{kind: "worker", paused: true}}},
		{"shared worker", "child_target_unsupported", []nativeFixtureChild{{kind: "shared_worker", paused: true}}},
		{"duplicate session", "child_target_invalid", []nativeFixtureChild{{kind: "service_worker", paused: true, session: "duplicate"}, {kind: "service_worker", paused: true, session: "duplicate"}}},
		{"duplicate target", "child_target_invalid", []nativeFixtureChild{{kind: "service_worker", paused: true, target: "duplicate"}, {kind: "service_worker", paused: true, target: "duplicate"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.childEvents = tc.children
			f.mu.Unlock()
			diagnostic := NewPageMonitorDiagnostic(strings.Repeat("a", 64), 1)
			doc, err := w.Fetch(WithPageMonitorDiagnostic(context.Background(), diagnostic), "https://origin.example/", NativeBrowserOptions{})
			stage, reason := diagnostic.Details()
			if err == nil || len(doc.HTML) != 0 || stage != "events" || reason != tc.reason {
				t.Fatalf("child outcome err=%v bytes=%d diagnostic=%s/%s, want events/%s", err, len(doc.HTML), stage, reason, tc.reason)
			}
			if strings.Contains(err.Error(), "CANARY") {
				t.Fatal("target metadata leaked into error")
			}
			assertNativeChildrenNeverResumed(t, f)
		})
	}
}

// TestNativePausedChildCountIsBounded prevents publisher-created children from exceeding the
// render target allowance.
func TestNativePausedChildCountIsBounded(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	for range 33 {
		f.childEvents = append(f.childEvents, nativeFixtureChild{kind: "service_worker", paused: true})
	}
	f.mu.Unlock()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if err == nil || len(doc.HTML) != 0 || NativeResourceLimitReason(err) != "child_target_count" {
		t.Fatalf("unbounded paused children: err=%v reason=%s", err, NativeResourceLimitReason(err))
	}
	assertNativeChildrenNeverResumed(t, f)
}

// TestNativePausedChildLeakFencesWorker requires every owned child to disappear before the
// controller remains ready.
func TestNativePausedChildLeakFencesWorker(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.childEvents = []nativeFixtureChild{{kind: "service_worker", paused: true}}
	f.leakChild = true
	f.mu.Unlock()
	doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
	if !errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || w.Ready() {
		t.Fatalf("unconfirmed child cleanup accepted: err=%v ready=%v", err, w.Ready())
	}
}

// TestNativeForeignChildEventDoesNotAffectPage ignores unrelated session events without mutating
// foreign browser state.
func TestNativeForeignChildEventDoesNotAffectPage(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.childEvents = []nativeFixtureChild{{kind: "page", envelope: "foreign-session", context: "foreign-context"}}
	f.mu.Unlock()
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); err != nil {
		t.Fatal(err)
	}
	assertNativeChildrenNeverResumed(t, f)
}

// TestNativePausedChildActivityFencesWorker treats unexpected activity from a held worker as
// lost ownership.
func TestNativePausedChildActivityFencesWorker(t *testing.T) {
	for _, method := range []string{"Network.requestWillBeSent", "Fetch.requestPaused", "Network.webSocketCreated", "Target.attachedToTarget"} {
		t.Run(method, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.childEvents = []nativeFixtureChild{{kind: "service_worker", paused: true, activity: method}}
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
			if !errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || w.Ready() || nativeEventReason(err) != "child_target_activity" {
				t.Fatalf("paused child activity escaped: err=%v ready=%v", err, w.Ready())
			}
			assertNativeChildrenNeverResumed(t, f)
		})
	}
}

// TestNativeWebSocketsAndPopupsRemainDenied preserves existing unsupported-operation policy for
// owned page events.
func TestNativeWebSocketsAndPopupsRemainDenied(t *testing.T) {
	for method, reason := range map[string]string{"Network.webSocketCreated": "websocket_denied", "Page.windowOpen": "popup_denied"} {
		t.Run(method, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.unsafeEvent = method
			f.mu.Unlock()
			diagnostic := NewPageMonitorDiagnostic(strings.Repeat("a", 64), 1)
			doc, err := w.Fetch(WithPageMonitorDiagnostic(context.Background(), diagnostic), "https://origin.example/", NativeBrowserOptions{})
			stage, got := diagnostic.Details()
			if !errors.Is(err, ErrNativeUnsupported) || len(doc.HTML) != 0 || stage != "events" || got != reason {
				t.Fatalf("unsupported event err=%v diagnostic=%s/%s", err, stage, got)
			}
			assertNativeChildrenNeverResumed(t, f)
		})
	}
}

// TestNativeGuardedIframeAndHeldServiceWorker permits guarded frame execution while keeping
// admitted service workers paused.
func TestNativeGuardedIframeAndHeldServiceWorker(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "nested"}[nested], func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.childEvents = []nativeFixtureChild{{kind: "service_worker", paused: true}, {kind: "iframe", paused: true}}
			f.pendingChildRequests = true
			f.childNested = nested
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{BrowserOptions: BrowserOptions{Timeout: 3 * time.Second}})
			if err != nil || doc.StatusCode != 200 {
				t.Fatalf("guarded child failed: %v status=%d", err, doc.StatusCode)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			want := 1
			if nested {
				want = 2
			}
			if f.childResumes != want {
				t.Fatalf("wrong resumed frames: %d want %d", f.childResumes, want)
			}
			for _, c := range f.commands {
				if c.method == "Runtime.runIfWaitingForDebugger" && c.session == "child-session-0" {
					t.Fatal("service worker resumed")
				}
			}
			if len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
				t.Fatal("child cleanup failed")
			}
		})
	}
}

// TestNativeFrameGuardFailureNeverResumes prevents child execution when any required frame guard
// cannot be confirmed.
func TestNativeFrameGuardFailureNeverResumes(t *testing.T) {
	for _, method := range []string{"Page.enable", "Network.enable", "Network.setBlockedURLs", "Fetch.enable", "Target.setAutoAttach", "Page.getFrameTree"} {
		t.Run(method, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.childEvents = []nativeFixtureChild{{kind: "iframe", paused: true}}
			f.childGuardFail = method
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{})
			if !errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || w.Ready() {
				t.Fatalf("unconfirmed frame guards accepted: %v", err)
			}
			f.mu.Lock()
			resumes := f.childResumes
			f.mu.Unlock()
			if resumes != 0 {
				t.Fatal("frame resumed before guards succeeded")
			}
			assertNativeChildrenNeverResumed(t, f)
		})
	}
}

// TestNativeFrameSharesPolicyAndBudgets applies the parent URL and resource restrictions to
// child-frame traffic.
func TestNativeFrameSharesPolicyAndBudgets(t *testing.T) {
	for _, mode := range []string{"private", "http", "websocket", "wire", "requests"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.childEvents = []nativeFixtureChild{{kind: "iframe", paused: true}}
			switch mode {
			case "private":
				f.childRequestURL = "https://127.0.0.1/"
			case "http":
				f.childRequestURL = "http://origin.example/"
			case "websocket":
				f.childRequestURL = "wss://origin.example/"
			case "wire":
				f.childWire = 6000
			case "requests":
				f.childRequestBurst = 129
			}
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{BrowserOptions: BrowserOptions{MaxWireBytes: 5000}})
			if err == nil || len(doc.HTML) != 0 {
				t.Fatalf("child bypassed shared policy: %v", err)
			}
			if mode == "wire" && NativeResourceLimitReason(err) != "wire_bytes" {
				t.Fatalf("wire charged outside shared budget: %v", err)
			}
			if mode == "requests" && NativeResourceLimitReason(err) != "request_count" {
				t.Fatalf("requests outside shared budget: %v", err)
			}
		})
	}
}

// TestNativeFrameDetachRequiresTargetAbsence releases child request state only after confirming
// that its target is gone.
func TestNativeFrameDetachRequiresTargetAbsence(t *testing.T) {
	for _, mode := range []string{"gone", "gone-root", "live"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.childEvents = []nativeFixtureChild{{kind: "iframe", paused: true}}
			f.childDetach = mode
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{BrowserOptions: BrowserOptions{Timeout: 3 * time.Second}})
			if (mode == "gone" || mode == "gone-root") && (err != nil || doc.StatusCode != 200) {
				t.Fatalf("verified frame removal blocked main: %v", err)
			}
			if mode == "live" && (!errors.Is(err, ErrNativeOwnershipLost) || len(doc.HTML) != 0 || w.Ready()) {
				t.Fatalf("live unmonitored target accepted: %v", err)
			}
		})
	}
}
