package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/pipeline"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestHealthcheckAndBrowserCapability(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path != "/health/ready" && r.URL.Path != "/json/version" {
			w.WriteHeader(404)
		}
		if r.URL.Path == "/json/version" {
			w.Write([]byte(`{"webSocketDebuggerUrl":"ws://untrusted.example/devtools/browser/owned"}`))
		}
	}))
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")
	if e := checkHealth(addr); e != nil {
		t.Fatal(e)
	}
	if !chromeReady(ts.URL) {
		t.Fatal("browser capability unavailable")
	}
	ready.Store(false)
	if e := checkHealth(addr); e == nil {
		t.Fatal("unready healthcheck passed")
	}
	if chromeReady(ts.URL) {
		t.Fatal("browser failure not reported")
	}
	if chromeReady("") {
		t.Fatal("empty endpoint available")
	}
}

func TestBrowserCapabilityRejectsMalformedDiscovery(t *testing.T) {
	for _, body := range []string{"", `{}`, `{"webSocketDebuggerUrl":"ws://chrome/not-a-browser-target"}`, `{"webSocketDebuggerUrl":"ws://chrome/devtools/browser/"}`} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
		if chromeReady(ts.URL) {
			t.Errorf("malformed200 accepted: %s", body)
		}
		ts.Close()
	}
}
func TestConfigBounds(t *testing.T) {
	t.Setenv("EXTRACTOR_MAX_CONCURRENT", "0")
	if _, e := envInt("EXTRACTOR_MAX_CONCURRENT", 5); e == nil {
		t.Fatal("accepted zero concurrency")
	}
	t.Setenv("EXTRACTOR_MAX_CONCURRENT", "101")
	if _, e := envInt("EXTRACTOR_MAX_CONCURRENT", 5); e == nil {
		t.Fatal("accepted unbounded concurrency")
	}
	t.Setenv("EXTRACTOR_MAX_CONCURRENT", "")
	if n, e := envInt("EXTRACTOR_MAX_CONCURRENT", 5); e != nil || n != 5 {
		t.Fatal(n, e)
	}
}

func TestHTTPOnlyIsSafeStartupDefault(t *testing.T) {
	for _, raw := range []string{"", "true"} {
		if only, err := httpOnlyConfig(raw, ""); err != nil || !only {
			t.Fatalf("HTTP-only default lost: %q %v %v", raw, only, err)
		}
		if _, err := httpOnlyConfig(raw, "http://chrome:9222"); err == nil {
			t.Fatal("contradictory Chrome configuration accepted")
		}
	}
	if only, err := httpOnlyConfig("false", "http://chrome:9222"); err != nil || only {
		t.Fatal("explicit experimental browser mode rejected")
	}
	if _, err := httpOnlyConfig("not-a-boolean", ""); err == nil {
		t.Fatal("invalid mode accepted")
	}
}

func TestRuntimeFailureWirePreservesStatusAndRetryTiming(t *testing.T) {
	retry := 90
	for _, code := range []string{"browser_required", "upstream_forbidden", "upstream_auth_required", "upstream_rate_limited", "public_url_denied", "resource_limit", "cancelled"} {
		out := pipeline.Outcome{Result: extractor.Result{State: "failed", Error: code}, UpstreamStatus: 429, RetryAfterSeconds: &retry, Reason: "upstream_challenge"}
		resp := toServiceResponse(out)
		wire, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Error == nil || resp.Error.Code != code || resp.Error.UpstreamStatus != 429 || resp.Error.RetryAfterSeconds == nil || !strings.Contains(string(wire), `"retry_after_seconds":90`) || !strings.Contains(string(wire), `"upstream_status":429`) {
			t.Fatalf("wire failure lost: %s", wire)
		}
		if (code == "cancelled" || code == "resource_limit" || code == "public_url_denied" || code == "upstream_auth_required") && resp.Error.Retryable {
			t.Fatalf("terminal result made retryable: %s", wire)
		}
	}
}

func TestAdmissionUnavailableWireIsRetryableWithoutBrowserDelegation(t *testing.T) {
	resp := toServiceResponse(pipeline.Outcome{Result: extractor.Result{State: "failed", Error: "admission_unavailable"}})
	if resp.Outcome != "failed" || resp.Error == nil || resp.Error.Code != "admission_unavailable" || !resp.Error.Retryable || resp.NeedsSelectorStudy {
		t.Fatalf("invalid admission outage contract: %+v", resp)
	}
}

func TestShutdownExtractionServerDrainsBeforeHTTPShutdown(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-finish
		w.WriteHeader(http.StatusNoContent)
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(listener)
	response := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			resp.Body.Close()
		}
		response <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	drainCalled := false
	done := make(chan error, 1)
	go func() {
		done <- shutdownExtractionServer(server, func(context.Context) error {
			drainCalled = true
			close(finish)
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bounded HTTP shutdown did not finish")
	}
	if !drainCalled {
		t.Fatal("admission drain was not invoked")
	}
	select {
	case err := <-response:
		if err != nil {
			t.Fatalf("active request did not finish before shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active request did not complete")
	}
}

// This test entrypoint runs the actual service main in a subprocess. Keep the
// helper inert during the parent test process.
func TestExtractServiceWorkerHelperProcess(t *testing.T) {
	if os.Getenv("EXTRACT_SERVICE_TEST_HELPER") != "1" {
		return
	}
	main()
}

type lifecycleCDPFixture struct {
	server      *httptest.Server
	mu          sync.Mutex
	generation  string
	disposed    int
	globalClose int
}

func newLifecycleCDPFixture(t *testing.T) *lifecycleCDPFixture {
	t.Helper()
	f := &lifecycleCDPFixture{generation: "worker-a"}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/json/version" {
			f.mu.Lock()
			generation := f.generation
			f.mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"webSocketDebuggerUrl":"ws://%s/devtools/browser/%s"}`, r.Host, generation)
			return
		}
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			data, _, err := wsutil.ReadClientData(conn)
			if err != nil {
				return
			}
			var request struct {
				ID     int64  `json:"id"`
				Method string `json:"method"`
			}
			if json.Unmarshal(data, &request) != nil {
				return
			}
			result := map[string]any{}
			switch request.Method {
			case "Target.createBrowserContext":
				result["browserContextId"] = "owned-context"
			case "Target.createTarget":
				result["targetId"] = "owned-target"
			case "Target.attachToTarget":
				result["sessionId"] = "owned-session"
			case "Page.getFrameTree":
				result["frameTree"] = map[string]any{"frame": map[string]string{"id": "main-frame"}}
			case "Target.disposeBrowserContext":
				f.mu.Lock()
				f.disposed++
				f.mu.Unlock()
			case "Target.getBrowserContexts":
				result["browserContextIds"] = []string{}
			case "Target.getTargets":
				result["targetInfos"] = []any{}
			case "Browser.close":
				f.mu.Lock()
				f.globalClose++
				f.mu.Unlock()
			}
			if wsutil.WriteServerMessage(conn, ws.OpText, []byte(fmt.Sprintf(`{"id":%d,"result":%s}`, request.ID, jsonString(result)))) != nil {
				return
			}
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func jsonString(value any) string {
	b, _ := json.Marshal(value)
	return string(b)
}

type nativeServiceProcess struct {
	cmd     *exec.Cmd
	output  *bytes.Buffer
	addr    string
	done    chan struct{}
	waitErr error
}

func startNativeServiceProcess(t *testing.T, browserEndpoint string) *nativeServiceProcess {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	output := &bytes.Buffer{}
	cmd := exec.Command(executable, "-test.run=^TestExtractServiceWorkerHelperProcess$")
	cmd.Stdout, cmd.Stderr = output, output
	cmd.Env = helperEnv(map[string]string{
		"EXTRACT_SERVICE_TEST_HELPER":       "1",
		"LISTEN_ADDR":                       addr,
		"EXTRACTOR_HTTP_ONLY":               "false",
		"EXTRACTOR_BROWSER_BACKEND":         "native",
		"CHROME_ENDPOINT":                   browserEndpoint,
		"EXTRACTOR_NATIVE_PROXY_URL":        "http://127.0.0.1:3128",
		"EXTRACTOR_NATIVE_PROXY_TOKEN":      "synthetic-lifecycle-token",
		"EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true",
		"EXTRACTOR_SERVICE_TOKEN":           "synthetic-service-token",
		"EXTRACTOR_BROWSER_CONCURRENT":      "1",
		"EXTRACTOR_NATIVE_DIAGNOSTICS":      "",
		"EXTRACTOR_NATIVE_READY_SELECTOR":   "",
	})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	process := &nativeServiceProcess{cmd: cmd, output: output, addr: addr, done: make(chan struct{})}
	go func() {
		process.waitErr = cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		select {
		case <-process.done:
		default:
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-process.done
		}
	})
	waitReady(t, process)
	return process
}

func helperEnv(overrides map[string]string) []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replace := overrides[key]; !replace {
			env = append(env, entry)
		}
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

func waitReady(t *testing.T, process *nativeServiceProcess) {
	t.Helper()
	client := &http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + process.addr + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-process.done:
			t.Fatalf("service exited before readiness: %v %s", process.waitErr, process.output.String())
		}
	}
	t.Fatalf("service did not become ready: %s", process.output.String())
}

func waitNativeProcess(t *testing.T, process *nativeServiceProcess) error {
	t.Helper()
	select {
	case <-process.done:
		return process.waitErr
	case <-time.After(10 * time.Second):
		_ = process.cmd.Process.Kill()
		t.Fatalf("service did not exit: %s", process.output.String())
		return nil
	}
}

func TestNativeWorkerFatalExitsAndFreshProcessReusesBrowser(t *testing.T) {
	fixture := newLifecycleCDPFixture(t)
	first := startNativeServiceProcess(t, fixture.server.URL)
	fixture.mu.Lock()
	fixture.generation = "worker-b"
	fixture.mu.Unlock()
	resp, err := http.Get("http://" + first.addr + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("replacement generation readiness status = %d, want 503", resp.StatusCode)
	}
	if err := waitNativeProcess(t, first); err == nil {
		t.Fatalf("fatal worker exited successfully: %s", first.output.String())
	} else {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			t.Fatalf("fatal worker exit = %v, want exit 1; output=%s", err, first.output.String())
		}
	}
	if !strings.Contains(first.output.String(), "native_browser_worker_fatal") || strings.Contains(first.output.String(), fixture.server.URL) {
		t.Fatalf("fatal exit was not sanitized: %s", first.output.String())
	}
	fixture.mu.Lock()
	disposedAfterFirst, globalCloseAfterFirst := fixture.disposed, fixture.globalClose
	fixture.mu.Unlock()
	if disposedAfterFirst != 1 || globalCloseAfterFirst != 0 {
		t.Fatalf("first worker cleanup/global browser state: disposed=%d globalClose=%d", disposedAfterFirst, globalCloseAfterFirst)
	}

	second := startNativeServiceProcess(t, fixture.server.URL)
	if err := second.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := waitNativeProcess(t, second); err != nil {
		t.Fatalf("fresh worker did not stop cleanly: %v output=%s", err, second.output.String())
	}
	fixture.mu.Lock()
	disposedAfterSecond, globalCloseAfterSecond := fixture.disposed, fixture.globalClose
	fixture.mu.Unlock()
	if disposedAfterSecond != 2 || globalCloseAfterSecond != 0 {
		t.Fatalf("fresh worker cleanup/global browser state: disposed=%d globalClose=%d", disposedAfterSecond, globalCloseAfterSecond)
	}
	resp, err = http.Get(fixture.server.URL + "/json/version")
	if err != nil {
		t.Fatalf("shared fake browser unavailable after worker exits: %v", err)
	}
	resp.Body.Close()
}
