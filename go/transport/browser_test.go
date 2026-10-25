package transport

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/chromedp/cdproto"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestBrowserCDPEventFiltering(t *testing.T) {
	required := []string{
		"Browser.downloadWillBegin", "Page.frameAttached", "Page.frameNavigated", "Page.frameDetached",
		"Fetch.authRequired", "Fetch.requestPaused", "Network.requestWillBeSent",
		"Network.loadingFinished", "Network.loadingFailed", "Network.dataReceived", "Network.responseReceived",
		"Network.webSocketCreated", "Page.windowOpen", "Target.attachedToTarget",
		"Inspector.targetCrashed", "Target.detachedFromTarget",
	}
	for _, overload := range []bool{false, true} {
		t.Run(fmt.Sprint("required_overload=", overload), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, _, _, err := ws.UpgradeHTTP(r, w)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				data, _, err := wsutil.ReadClientData(conn)
				if err != nil {
					return
				}
				var command struct {
					ID int64 `json:"id"`
				}
				if json.Unmarshal(data, &command) != nil {
					return
				}
				write := func(value any) bool {
					payload, err := json.Marshal(value)
					return err == nil && wsutil.WriteServerMessage(conn, ws.OpText, payload) == nil
				}
				// No consumer drains events while the command waits for its reply.
				for i := 0; i < 1024; i++ {
					method := []string{"Network.requestWillBeSentExtraInfo", "Network.responseReceivedExtraInfo",
						"Page.lifecycleEvent", "Runtime.consoleAPICalled"}[i%4]
					if overload {
						method = "Fetch.requestPaused"
					}
					if !write(map[string]any{"method": method, "sessionId": "owned-session", "params": map[string]any{}}) {
						return
					}
				}
				for _, method := range required {
					if !write(map[string]any{"method": method, "sessionId": "owned-session", "params": map[string]any{}}) {
						return
					}
				}
				if !write(map[string]any{"id": command.ID, "result": map[string]bool{"received": true}}) {
					return
				}
				// Keep the reader alive until the client closes it.
				_, _, _ = wsutil.ReadClientData(conn)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
			if err != nil {
				t.Fatal(err)
			}
			defer client.close()
			var result struct {
				Received bool `json:"received"`
			}
			err = client.call(ctx, "", "Browser.getVersion", map[string]any{}, &result)
			if overload {
				if err == nil || !strings.Contains(err.Error(), "event_overflow") {
					t.Fatalf("required-event overload must fail closed: %v", err)
				}
				if len(client.events) != cap(client.events) {
					t.Fatal("event queue is not bounded")
				}
				return
			}
			if err != nil || !result.Received {
				t.Fatalf("irrelevant events blocked command reply: %v", err)
			}
			if len(client.events) != len(required) {
				t.Fatalf("queued %d events, want %d", len(client.events), len(required))
			}
			for _, method := range required {
				event := <-client.events
				if string(event.Method) != method || string(event.SessionID) != "owned-session" {
					t.Fatalf("required event changed: %s", event.Method)
				}
			}
		})
	}
}

func TestDetachedOwnedSessionClassification(t *testing.T) {
	tests := []struct {
		name, envelope, detached string
		malformed                bool
		wantOwned, wantMalformed bool
	}{
		{name: "child on owned envelope", envelope: "owned", detached: "child"},
		{name: "root scoped owned", detached: "owned", wantOwned: true},
		{name: "owned envelope owned", envelope: "owned", detached: "owned", wantOwned: true},
		{name: "foreign envelope", envelope: "foreign", detached: "owned"},
		{name: "malformed root", malformed: true, wantMalformed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := `{"sessionId":"` + test.detached + `"}`
			if test.malformed {
				params = `{}`
			}
			payload, err := json.Marshal(map[string]any{
				"method": "Target.detachedFromTarget", "sessionId": test.envelope,
				"params": json.RawMessage(params),
			})
			if err != nil {
				t.Fatal(err)
			}
			var message cdproto.Message
			if err := json.Unmarshal(payload, &message); err != nil {
				t.Fatal(err)
			}
			owned, malformed := detachedOwnedSession(&message, "owned")
			if owned != test.wantOwned || malformed != test.wantMalformed {
				t.Fatalf("classification = (%v, %v), want (%v, %v)", owned, malformed, test.wantOwned, test.wantMalformed)
			}
		})
	}
}

func TestBrowserCDPOverflowKeepsCleanupRepliesReadable(t *testing.T) {
	commandReceived := make(chan struct{})
	cleanupReceived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		readCommand := func() (int64, string, bool) {
			data, _, err := wsutil.ReadClientData(conn)
			if err != nil {
				return 0, "", false
			}
			var command struct {
				ID     int64  `json:"id"`
				Method string `json:"method"`
			}
			if json.Unmarshal(data, &command) != nil {
				return 0, "", false
			}
			return command.ID, command.Method, true
		}
		write := func(value any) bool {
			payload, err := json.Marshal(value)
			return err == nil && wsutil.WriteServerMessage(conn, ws.OpText, payload) == nil
		}
		id, _, ok := readCommand()
		if !ok {
			return
		}
		close(commandReceived)
		for i := 0; i < 257; i++ {
			if !write(map[string]any{"method": "Fetch.requestPaused", "sessionId": "owned-session", "params": map[string]any{"requestId": fmt.Sprint(i)}}) {
				return
			}
		}
		if !write(map[string]any{"id": id, "result": map[string]bool{"received": true}}) {
			return
		}
		id, method, ok := readCommand()
		if !ok || method != "Target.disposeBrowserContext" {
			return
		}
		close(cleanupReceived)
		for i := 0; i < 4096; i++ {
			if !write(map[string]any{"method": "Network.dataReceived", "sessionId": "owned-session", "params": map[string]any{"requestId": fmt.Sprint(i)}}) {
				return
			}
		}
		_ = write(map[string]any{"id": id, "result": map[string]bool{}})
		_, _, _ = wsutil.ReadClientData(conn)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()

	var result struct {
		Received bool `json:"received"`
	}
	err = client.call(ctx, "", "Browser.getVersion", map[string]any{}, &result)
	if err == nil || !strings.Contains(err.Error(), "event_overflow") {
		t.Fatalf("overloaded command should fail with terminal event error, got result=%+v err=%v", result, err)
	}
	select {
	case <-commandReceived:
	default:
		t.Fatal("fixture did not receive the ordinary command")
	}
	if err := client.callCleanup(ctx, "", "Target.disposeBrowserContext", map[string]any{"browserContextId": "owned"}, nil); err != nil {
		t.Fatalf("cleanup reply was lost after event overload: %v", err)
	}
	select {
	case <-cleanupReceived:
	default:
		t.Fatal("fixture did not receive the cleanup command")
	}
}

func TestBrowserCDPAllocationReplySurvivesOverflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		data, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			return
		}
		var command struct {
			ID int64 `json:"id"`
		}
		if json.Unmarshal(data, &command) != nil {
			return
		}
		write := func(value any) bool {
			payload, err := json.Marshal(value)
			return err == nil && wsutil.WriteServerMessage(conn, ws.OpText, payload) == nil
		}
		for i := 0; i < 257; i++ {
			if !write(map[string]any{"method": "Fetch.requestPaused", "sessionId": "owned", "params": map[string]any{}}) {
				return
			}
		}
		_ = write(map[string]any{"id": command.ID, "result": map[string]string{"browserContextId": "owned-context"}})
		data, _, err = wsutil.ReadClientData(conn)
		if err != nil {
			return
		}
		if !write(map[string]any{"id": json.RawMessage(extractCommandID(data)), "result": map[string]bool{}}) {
			return
		}
		_, _, _ = wsutil.ReadClientData(conn)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	var contextReply struct {
		ID string `json:"browserContextId"`
	}
	if err := client.callAllocation(ctx, "", "Target.createBrowserContext", map[string]any{}, &contextReply); err != nil {
		t.Fatalf("in-flight allocation reply was discarded: %v", err)
	}
	if contextReply.ID != "owned-context" {
		t.Fatalf("allocation ID not captured: %+v", contextReply)
	}
	if err := client.callAllocation(ctx, "", "Target.createTarget", map[string]any{}, nil); !errors.Is(err, ErrBrowserEventOverflow) {
		t.Fatalf("next allocation was not rejected after overload: %v", err)
	}
	if err := client.callCleanup(ctx, "", "Target.disposeBrowserContext", map[string]any{"browserContextId": contextReply.ID}, nil); err != nil {
		t.Fatalf("owned context could not be disposed after overload: %v", err)
	}
}

func TestBrowserCDPLateAndDuplicateRepliesDoNotBlockReader(t *testing.T) {
	firstReceived := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		read := func() (int64, bool) {
			data, _, err := wsutil.ReadClientData(conn)
			if err != nil {
				return 0, false
			}
			return int64(commandID(data)), true
		}
		write := func(id int64) bool {
			payload, err := json.Marshal(map[string]any{"id": id, "result": map[string]bool{"ok": true}})
			return err == nil && wsutil.WriteServerMessage(conn, ws.OpText, payload) == nil
		}
		first, ok := read()
		if !ok {
			return
		}
		close(firstReceived)
		<-releaseFirst
		if !write(first) || !write(first) {
			return
		}
		second, ok := read()
		if !ok {
			return
		}
		_ = write(second)
		_, _, _ = wsutil.ReadClientData(conn)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstResult := make(chan error, 1)
	go func() { firstResult <- client.call(firstCtx, "", "Browser.getVersion", map[string]any{}, nil) }()
	<-firstReceived
	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled first call returned %v", err)
	}
	close(releaseFirst)
	var result struct {
		OK bool `json:"ok"`
	}
	if err := client.call(ctx, "", "Browser.getVersion", map[string]any{}, &result); err != nil || !result.OK {
		t.Fatalf("late/duplicate reply blocked following command: result=%+v err=%v", result, err)
	}
}

func commandID(data []byte) int {
	var command struct {
		ID int `json:"id"`
	}
	_ = json.Unmarshal(data, &command)
	return command.ID
}

func TestBrowserCDPCleanupAllowlistAndDeadline(t *testing.T) {
	commandReceived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, _, err := wsutil.ReadClientData(conn); err == nil {
			close(commandReceived)
		}
		_, _, _ = wsutil.ReadClientData(conn)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if err := client.callCleanup(ctx, "", "Runtime.evaluate", map[string]any{}, nil); err == nil || !strings.Contains(err.Error(), "cleanup_command_denied") {
		t.Fatalf("cleanup path accepted a page command: %v", err)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cleanupCancel()
	err = client.callCleanup(cleanupCtx, "owned-session", "Page.stopLoading", map[string]any{}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unanswered cleanup did not honor its deadline: %v", err)
	}
	select {
	case <-commandReceived:
	default:
		t.Fatal("fixture did not receive allowlisted cleanup command")
	}
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("deadline did not close a stalled cleanup connection")
	}
}

func TestBrowserCDPBlockedWriteHonorsContext(t *testing.T) {
	releaseHandler := make(chan struct{})
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		<-releaseHandler // Deliberately never read the client's command frame.
	}))
	defer func() {
		close(releaseHandler)
		server.Close()
		select {
		case <-handlerDone:
		case <-time.After(time.Second):
			t.Error("blocked-write fixture handler did not exit")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	writeCtx, cancelWrite := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelWrite()
	started := time.Now()
	err = client.call(writeCtx, "", "Browser.getVersion", map[string]any{"padding": strings.Repeat("x", 8<<20)}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked protocol write lost deadline classification: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("blocked protocol write prevented prompt cancellation: %v", elapsed)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cleanupCancel()
	cleanupErr := client.callCleanup(cleanupCtx, "", "Page.stopLoading", map[string]any{}, nil)
	if !errors.Is(cleanupErr, context.DeadlineExceeded) {
		t.Fatalf("bounded cleanup did not report its deadline: %v", cleanupErr)
	}
	select {
	case <-client.done:
	case <-time.After(3 * time.Second):
		t.Fatal("bounded cleanup did not release the shared reader")
	}
	select {
	case <-client.writerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("bounded cleanup did not release the writer")
	}
	client.mu.Lock()
	pending := len(client.pending)
	client.mu.Unlock()
	if pending != 0 {
		t.Fatalf("canceled callers leaked %d pending entries", pending)
	}
}

func TestBrowserCDPPhysicalEOFIsSeparateFromEventFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = wsutil.ReadClientData(conn)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if err := client.call(ctx, "", "Browser.getVersion", map[string]any{}, nil); err == nil {
		t.Fatal("physical EOF was reported as command success")
	}
	select {
	case <-client.done:
	case <-ctx.Done():
		t.Fatal("physical reader completion was not observable")
	}
	if client.eventError() != nil {
		t.Fatalf("physical EOF was confused with event overload: %v", client.eventError())
	}
}

func extractCommandID(data []byte) json.RawMessage {
	var command struct {
		ID json.RawMessage `json:"id"`
	}
	_ = json.Unmarshal(data, &command)
	return command.ID
}

func browserTestEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("GO_BROWSER_TEST_ENDPOINT")
	if endpoint == "" {
		if os.Getenv("GO_BROWSER_TEST_REQUIRED") == "1" {
			t.Fatal("GO_BROWSER_TEST_REQUIRED=1 requires GO_BROWSER_TEST_ENDPOINT")
		}
		t.Skip("set GO_BROWSER_TEST_ENDPOINT to a dedicated loopback Chrome debugger; tests never launch Chrome")
	}
	return endpoint
}

type cloakDiagnosticCommand struct {
	method  string
	params  map[string]any
	browser bool
}

type cloakDiagnosticProfile struct {
	name       string
	context    map[string]any
	background bool
	commands   []cloakDiagnosticCommand
}

// TestBrowserCloakCommandDiagnostic is opt-in and sends only about:blank CDP setup commands.
func TestBrowserCloakCommandDiagnostic(t *testing.T) {
	if os.Getenv("GO_CLOAK_COMMAND_DIAGNOSTIC") != "1" {
		t.Skip("set GO_CLOAK_COMMAND_DIAGNOSTIC=1 with a dedicated Cloak endpoint")
	}
	endpoint := browserTestEndpoint(t)
	profiles := []cloakDiagnosticProfile{
		{name: "browser-fetch", commands: []cloakDiagnosticCommand{
			{"Network.enable", map[string]any{}, false},
			{"Network.setBlockedURLs", map[string]any{"urls": []string{"ws://*", "wss://*"}, "urlPatterns": []any{
				map[string]any{"urlPattern": "ws://*:*/*", "block": true}, map[string]any{"urlPattern": "wss://*:*/*", "block": true}}}, false},
			{"Network.setCacheDisabled", map[string]any{"cacheDisabled": true}, false},
			{"Network.setBypassServiceWorker", map[string]any{"bypass": true}, false},
			{"Network.setUserAgentOverride", map[string]any{"userAgent": MobileUserAgent}, false},
			{"Page.enable", map[string]any{}, false},
			{"Fetch.enable", map[string]any{"patterns": []any{
				map[string]any{"urlPattern": "*", "requestStage": "Request"},
				map[string]any{"urlPattern": "*", "requestStage": "Response"}}}, false},
		}},
		{name: "native-browser", context: map[string]any{"proxyServer": "http://127.0.0.1:9", "proxyBypassList": "<-loopback>"}, background: true, commands: []cloakDiagnosticCommand{
			{"Page.enable", map[string]any{}, false},
			{"Network.enable", map[string]any{"maxPostDataSize": 1}, false},
			{"Network.setBlockedURLs", map[string]any{"urls": []string{"http://*", "ws://*", "wss://*", "file://*", "ftp://*"}}, false},
			{"Browser.setDownloadBehavior", map[string]any{"behavior": "deny", "eventsEnabled": true}, true},
			{"Target.setAutoAttach", map[string]any{"autoAttach": true, "waitForDebuggerOnStart": true, "flatten": true}, false},
			{"Fetch.enable", map[string]any{"handleAuthRequests": true, "patterns": []any{map[string]any{"urlPattern": "*", "requestStage": "Request"}}}, false},
			{"Page.getFrameTree", map[string]any{}, false},
		}},
	}
	for _, p := range profiles {
		if !runCloakCommandDiagnostic(t, endpoint, p.name, p.context, p.background, p.commands) {
			return
		}
	}
}

func runCloakCommandDiagnostic(t *testing.T, endpoint, profile string, contextParams map[string]any,
	background bool, commands []cloakDiagnosticCommand) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	wsURL, err := browserEndpoint(ctx, endpoint)
	if err != nil {
		t.Errorf("cloak-cdp profile=%s stage=discovery outcome=failed", profile)
		return false
	}
	client, err := dialBrowser(ctx, wsURL)
	if err != nil {
		t.Errorf("cloak-cdp profile=%s stage=dial outcome=failed", profile)
		return false
	}
	contextID := ""
	defer func() {
		if contextID != "" {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
			started := time.Now()
			err := client.callCleanup(cleanup, "", "Target.disposeBrowserContext", map[string]any{"browserContextId": contextID}, nil)
			done()
			logCloakDiagnostic(t, profile, "cleanup", "Target.disposeBrowserContext", started, err)
			if err != nil {
				t.Errorf("cloak-cdp profile=%s cleanup=unconfirmed", profile)
			}
		}
		client.close()
	}()
	call := func(stage, session, method string, params, result any, allocation bool) bool {
		callCtx, done := context.WithTimeout(ctx, 5*time.Second)
		started := time.Now()
		if allocation {
			err = client.callAllocation(callCtx, session, method, params, result)
		} else {
			err = client.call(callCtx, session, method, params, result)
		}
		done()
		logCloakDiagnostic(t, profile, stage, method, started, err)
		if err != nil {
			t.Fail()
		}
		return err == nil
	}
	params := map[string]any{"disposeOnDetach": true}
	for k, v := range contextParams {
		params[k] = v
	}
	var createdContext struct {
		ID string `json:"browserContextId"`
	}
	if !call("allocation", "", "Target.createBrowserContext", params, &createdContext, true) {
		return false
	}
	contextID = createdContext.ID
	if contextID == "" {
		t.Errorf("cloak-cdp profile=%s stage=allocation method=Target.createBrowserContext outcome=missing_receipt", profile)
		return false
	}
	var contexts struct {
		IDs []string `json:"browserContextIds"`
	}
	if !call("receipt", "", "Target.getBrowserContexts", map[string]any{}, &contexts, false) {
		return false
	}
	targetParams := map[string]any{"url": "about:blank", "browserContextId": contextID}
	if background {
		targetParams["background"] = true
	}
	var target struct {
		ID string `json:"targetId"`
	}
	if !call("allocation", "", "Target.createTarget", targetParams, &target, true) {
		return false
	}
	if target.ID == "" {
		t.Errorf("cloak-cdp profile=%s stage=allocation method=Target.createTarget outcome=missing_receipt", profile)
		return false
	}
	var attached struct {
		ID string `json:"sessionId"`
	}
	if !call("allocation", "", "Target.attachToTarget", map[string]any{"targetId": target.ID, "flatten": true}, &attached, true) {
		return false
	}
	if attached.ID == "" {
		t.Errorf("cloak-cdp profile=%s stage=allocation method=Target.attachToTarget outcome=missing_receipt", profile)
		return false
	}
	for _, c := range commands {
		session := attached.ID
		if c.browser {
			session = ""
			c.params["browserContextId"] = contextID
		}
		if !call("configure", session, c.method, c.params, nil, false) {
			return false
		}
	}
	return true
}

func logCloakDiagnostic(t *testing.T, profile, stage, method string, started time.Time, err error) {
	t.Helper()
	outcome := "ok"
	if err != nil {
		outcome = "protocol_or_transport"
		if errors.Is(err, context.DeadlineExceeded) {
			outcome = "deadline"
		} else if errors.Is(err, ErrBrowserEventOverflow) {
			outcome = "event_overflow"
		}
	}
	t.Logf("cloak-cdp profile=%s stage=%s method=%s elapsed_ms=%d outcome=%s", profile, stage, method, time.Since(started).Milliseconds(), outcome)
}

func TestBrowserInputBounds(t *testing.T) {
	for _, tc := range []struct {
		endpoint, page string
		opts           BrowserOptions
	}{
		{"http://example.com:9222", "http://127.0.0.1/", BrowserOptions{}},
		{"http://127.0.0.1:9222", "https://example.com/", BrowserOptions{}},
		{"http://127.0.0.1:9222", "file:///etc/passwd", BrowserOptions{}},
		{"http://127.0.0.1:9222", "http://127.0.0.1/", BrowserOptions{Timeout: 31 * time.Second}},
		{"http://127.0.0.1:9222", "http://127.0.0.1/", BrowserOptions{MaxDOMBytes: MaxPageBytes + 1}},
		{"http://127.0.0.1:9222", "http://127.0.0.1/", BrowserOptions{MaxWireBytes: -1}},
	} {
		if _, err := BrowserFetch(context.Background(), tc.endpoint, tc.page, tc.opts); err == nil {
			t.Errorf("accepted invalid input: %+v", tc)
		}
	}
}

func TestBrowserLive(t *testing.T) {
	endpoint := browserTestEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL, err := browserEndpoint(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := dialBrowser(ctx, wsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(observer.close)
	var foreign struct {
		TargetID string `json:"targetId"`
	}
	if err := observer.call(ctx, "", "Target.createTarget", map[string]any{"url": "about:blank"}, &foreign); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		if err := observer.call(cleanup, "", "Target.closeTarget", map[string]any{"targetId": foreign.TargetID}, nil); err != nil {
			t.Error(err)
		}
	})
	baseline := browserTargets(t, observer)
	if !baseline[foreign.TargetID] {
		t.Fatal("observer foreign target missing")
	}
	var imageRequests, fontRequests, mediaRequests, websocketRequests, workerRequests, popupRequests, frameRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		switch r.URL.Path {
		case "/rendered":
			fmt.Fprint(w, `<!doctype html><html lang="en"><head><title>JS fixture</title><meta property="og:title" content="Preserved metadata"><style>@font-face{font-family:blocked;src:url('/font.woff2')}body{font-family:blocked}</style></head><body data-origin="fixture"><img src="/image.png"><audio src="/media.mp3" preload="auto"></audio><script>setTimeout(()=>{document.body.insertAdjacentHTML('beforeend','<article data-rendered="yes"><h1>Rendered article</h1><p>Text inserted by local JavaScript.</p></article>');document.body.dataset.ua=navigator.userAgent;},120)</script></body></html>`)
		case "/image.png":
			imageRequests.Add(1)
		case "/font.woff2":
			fontRequests.Add(1)
		case "/media.mp3":
			mediaRequests.Add(1)
		case "/websocket":
			fmt.Fprint(w, `<!doctype html><html><body><script>const socket = new WebSocket('ws://' + location.host + '/ws');socket.onerror=()=>{document.body.textContent='WebSocket blocked'};socket.onopen=()=>{document.body.textContent='WebSocket opened'};</script></body></html>`)
		case "/ws":
			websocketRequests.Add(1)
			http.Error(w, "unexpected websocket handshake", http.StatusBadRequest)
		case "/children":
			fmt.Fprint(w, `<!doctype html><html><body><script>
 const attempt=code=>{try{code()}catch(error){}};
 attempt(()=>new Worker(URL.createObjectURL(new Blob(["fetch('http://"+location.host+"/worker-data')"],{type:'text/javascript'}))));
 attempt(()=>new SharedWorker(URL.createObjectURL(new Blob(["fetch('http://"+location.host+"/worker-data')"],{type:'text/javascript'}))));
 attempt(()=>window.open('/popup-data'));
 attempt(()=>{const frame=document.createElement('iframe');frame.src='/frame-data';document.body.append(frame)});
 setTimeout(()=>document.body.insertAdjacentHTML('beforeend','<p>Children blocked</p>'),400);
</script></body></html>`)
		case "/worker-data":
			workerRequests.Add(1)
			fmt.Fprint(w, strings.Repeat("worker bytes", 4096))
		case "/popup-data":
			popupRequests.Add(1)
			fmt.Fprint(w, "popup")
		case "/frame-data":
			frameRequests.Add(1)
			fmt.Fprint(w, "frame")
		case "/gzip-rendered":
			w.Header().Set("Content-Encoding", "gzip")
			compressed := gzip.NewWriter(w)
			fmt.Fprint(compressed, `<!doctype html><html><body><script>document.body.textContent='Decoded gzip text'</script></body></html>`)
			_ = compressed.Close()
		case "/strict-policy":
			w.Header().Set("Content-Security-Policy", "script-src 'none'")
			fmt.Fprint(w, `<!doctype html><html><body><p>Existing policy text</p><script>document.body.dataset.escaped='yes'</script></body></html>`)
		case "/empty":
			fmt.Fprint(w, `<!doctype html><html><body></body></html>`)
		case "/large-dom":
			fmt.Fprint(w, `<!doctype html><html><body><script>document.body.textContent='界'.repeat(1000)</script></body></html>`)
		case "/huge-dom":
			fmt.Fprint(w, `<!doctype html><html><body><script>document.body.textContent='x'.repeat(6*1024*1024)</script></body></html>`)
		case "/large-wire":
			fmt.Fprint(w, `<html><body>`+strings.Repeat("wire body ", 1200)+`</body></html>`)
		case "/chunked-wire":
			fmt.Fprint(w, `<html><body>`)
			w.(http.Flusher).Flush()
			fmt.Fprint(w, strings.Repeat("wire body ", 1200)+`</body></html>`)
		case "/external":
			http.Redirect(w, r, "http://192.0.2.1/forbidden", http.StatusFound)
		default:
			http.Error(w, "missing", http.StatusNotFound)
		}
	}))
	defer server.Close()
	t.Run("javascript metadata mobile headers and blocked resources", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/rendered", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{`property="og:title" content="Preserved metadata"`, `data-rendered="yes"`, `lang="en"`, `data-origin="fixture"`, `Rendered article`, `Mobile Safari`} {
			if !strings.Contains(string(doc.HTML), expected) {
				t.Errorf("missing %q in rendered HTML", expected)
			}
		}
		if doc.DOMBytes != len(doc.HTML) || doc.Encoding != "utf-8" || doc.URL != server.URL+"/rendered" || doc.StatusCode != 200 {
			t.Errorf("invalid metadata: %+v", doc)
		}
		if imageRequests.Load() != 0 || fontRequests.Load() != 0 || mediaRequests.Load() != 0 {
			t.Errorf("blocked resources reached server: images=%d fonts=%d media=%d", imageRequests.Load(), fontRequests.Load(), mediaRequests.Load())
		}
	})
	t.Run("UTF8 byte guard before transfer", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/large-dom", BrowserOptions{Timeout: 5 * time.Second, MaxDOMBytes: 1500})
		if !errors.Is(err, extractor.ErrOutputLimit) {
			t.Fatalf("expected UTF8 DOM limit, got %v", err)
		}
	})
	t.Run("WebSocket handshake blocked before request", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/websocket", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc.HTML), "WebSocket blocked") {
			t.Fatalf("WebSocket error callback did not render: %s", doc.HTML)
		}
		if got := websocketRequests.Load(); got != 0 {
			t.Fatalf("WebSocket handshakes reached loopback server: %d", got)
		}
	})
	t.Run("workers frames and popups blocked before child requests", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/children", BrowserOptions{Timeout: 5 * time.Second, MaxWireBytes: 4096})
		if err != nil {
			t.Fatalf("%v worker=%d popup=%d frame=%d", err, workerRequests.Load(), popupRequests.Load(), frameRequests.Load())
		}
		if !strings.Contains(string(doc.HTML), "Children blocked") {
			t.Fatal("page did not render after blocked child attempts")
		}
		if workerRequests.Load() != 0 || popupRequests.Load() != 0 || frameRequests.Load() != 0 {
			t.Fatalf("child request escaped: worker=%d popup=%d frame=%d", workerRequests.Load(), popupRequests.Load(), frameRequests.Load())
		}
		assertBrowserTargets(t, observer, baseline)
	})
	t.Run("compressed response decoded before CSP fulfillment", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/gzip-rendered", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc.HTML), "<body>Decoded gzip text</body>") {
			t.Fatalf("compressed response corrupted: %s", doc.HTML)
		}
	})
	t.Run("existing CSP remains enforced", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/strict-policy", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc.HTML), "Existing policy text") || strings.Contains(string(doc.HTML), `data-escaped="yes"`) {
			t.Fatalf("existing CSP was relaxed: %s", doc.HTML)
		}
	})
	t.Run("character guard before transfer", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/huge-dom", BrowserOptions{Timeout: 5 * time.Second})
		if !errors.Is(err, extractor.ErrOutputLimit) {
			t.Fatalf("expected DOM limit, got %v", err)
		}
	})
	t.Run("wire bound", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/large-wire", BrowserOptions{Timeout: 5 * time.Second, MaxWireBytes: 1024})
		if !errors.Is(err, extractor.ErrWireLimit) {
			t.Fatalf("expected wire limit, got %v", err)
		}
	})
	t.Run("chunked wire bound", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/chunked-wire", BrowserOptions{Timeout: 5 * time.Second, MaxWireBytes: 1024})
		if !errors.Is(err, extractor.ErrWireLimit) {
			t.Fatalf("expected chunked wire limit, got %v", err)
		}
	})
	t.Run("HTTP failure", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/missing", BrowserOptions{Timeout: 5 * time.Second})
		if err == nil || !strings.Contains(err.Error(), "prototype_browser_status: 404") {
			t.Fatalf("expected HTTP failure, got %v", err)
		}
	})
	t.Run("redirect isolation", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/external", BrowserOptions{Timeout: 5 * time.Second})
		if !errors.Is(err, ErrNonLoopback) {
			t.Fatalf("expected non-loopback rejection, got %v", err)
		}
	})
	t.Run("caller cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(300*time.Millisecond, cancel)
		defer timer.Stop()
		defer cancel()
		_, err := BrowserFetch(ctx, endpoint, server.URL+"/empty", BrowserOptions{Timeout: 5 * time.Second})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected caller cancellation, got %v", err)
		}
	})
	t.Run("repeated success failure deadline cleanup", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			path := "/rendered"
			opts := BrowserOptions{Timeout: 3 * time.Second}
			if i%3 == 1 {
				path = "/empty"
				opts.Timeout = 250 * time.Millisecond
			}
			if i%3 == 2 {
				path = "/large-dom"
				opts.MaxDOMBytes = 1500
			}
			_, err := BrowserFetch(context.Background(), endpoint, server.URL+path, opts)
			if i%3 == 0 && err != nil {
				t.Fatalf("cycle %d: %v", i, err)
			}
			if i%3 != 0 && err == nil {
				t.Fatalf("cycle %d should fail", i)
			}
			assertBrowserTargets(t, observer, baseline)
		}
	})
	assertBrowserTargets(t, observer, baseline)
}

func browserTargets(t *testing.T, c *browserCDP) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var response struct {
		TargetInfos []struct {
			TargetID string `json:"targetId"`
		} `json:"targetInfos"`
	}
	if err := c.call(ctx, "", "Target.getTargets", map[string]any{}, &response); err != nil {
		t.Fatal(err)
	}
	result := make(map[string]bool)
	for _, info := range response.TargetInfos {
		result[info.TargetID] = true
	}
	return result
}

func assertBrowserTargets(t *testing.T, c *browserCDP, want map[string]bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := browserTargets(t, c)
		if fmt.Sprint(sortedTargetIDs(got)) == fmt.Sprint(sortedTargetIDs(want)) {
			return
		}
		if time.Now().After(deadline) {
			before, _ := json.Marshal(want)
			after, _ := json.Marshal(got)
			t.Fatalf("target leak or foreign target closed: baseline=%s current=%s", before, after)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func sortedTargetIDs(ids map[string]bool) string {
	// JSON marshaling sorts map keys, making the complete ID set comparable.
	encoded, _ := json.Marshal(ids)
	return string(encoded)
}
