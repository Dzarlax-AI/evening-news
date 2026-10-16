package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/pipeline"
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
