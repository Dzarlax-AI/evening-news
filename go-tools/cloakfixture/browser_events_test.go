package cloakfixture

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestBrowserEventOriginDistinguishesInitialScriptFromWorkerExecution keeps script download
// counters separate from proof of executed worker code.
func TestBrowserEventOriginDistinguishesInitialScriptFromWorkerExecution(t *testing.T) {
	o, err := NewOrigin(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"worker", "shared-worker", "service-worker"} {
		w := httptest.NewRecorder()
		o.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/browser-events/"+kind, nil))
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Synthetic PageMonitor story") || !strings.Contains(w.Body.String(), "/event-script/"+kind) {
			t.Fatalf("missing %s synthetic page", kind)
		}
		w = httptest.NewRecorder()
		o.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/event-script/"+kind, nil))
		if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/javascript" || !strings.Contains(w.Body.String(), "/event-executed/"+kind) {
			t.Fatalf("missing %s execution witness", kind)
		}
	}
	p := &Profiles{StateDir: o.dir}
	s, err := p.state()
	if err != nil || s.EventExecutions != 0 || len(s.EventScripts) != 3 {
		t.Fatalf("initial worker downloads counted as executions: %+v %v", s, err)
	}
	w := httptest.NewRecorder()
	o.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/event-executed/service-worker", nil))
	s, err = p.state()
	if err != nil || w.Code != http.StatusNoContent || s.EventExecutions != 1 {
		t.Fatal("worker execution was not persisted for the live gate")
	}
	b, err := json.Marshal(BrowserEventsResult{Scripts: s.EventScripts, Executions: s.EventExecutions})
	if err != nil || len(b) > 4096 {
		t.Fatal("invalid bounded browser-event receipt")
	}
}

// TestBrowserEventReceiptsIncludeFinalTrimmedLogLine prevents a missing terminal newline from
// hiding the last diagnostic witness.
func TestBrowserEventReceiptsIncludeFinalTrimmedLogLine(t *testing.T) {
	logs := "native_browser_diagnostic stage=events reason=worker_paused\n" +
		"native_browser_diagnostic stage=events reason=service_worker_paused"
	counts := pausedWorkerReceipts(logs)
	if counts["worker_paused"] != 1 || counts["service_worker_paused"] != 1 || counts["shared_worker_paused"] != 0 {
		t.Fatalf("lost final diagnostic: %v", counts)
	}
	reasons := nativeFailureReceipts("native_browser_diagnostic stage=events reason=child_target_invalid")
	if len(reasons) != 1 || reasons[0] != "child_target_invalid" {
		t.Fatalf("lost final failure: %v", reasons)
	}
}

// TestClosedWorkerReceiptsAndProtocolCodesAreFinite rejects metadata appended to finite worker
// and protocol-error witnesses.
func TestClosedWorkerReceiptsAndProtocolCodesAreFinite(t *testing.T) {
	logs := "native_browser_diagnostic stage=events reason=worker_closed"
	if closedWorkerReceipts(logs)["worker_closed"] != 1 {
		t.Fatal("closed witness lost")
	}
	reasons := nativeFailureReceipts("reason=protocol_error_-32000\nreason=protocol_error_-32000_CANARY\nnative_browser_diagnostic stage=events reason=child_target_close_rejected")
	if len(reasons) != 2 || reasons[0] != "child_target_close_rejected" || reasons[1] != "protocol_error_-32000" {
		t.Fatalf("unsafe or missing finite reason: %v", reasons)
	}
}

// TestPaddedGzipSeparatesEncodedAndDecodedBudgets demonstrates that valid tiny decoded content
// can exceed the encoded byte allowance.
func TestPaddedGzipSeparatesEncodedAndDecodedBudgets(t *testing.T) {
	body := []byte("<html>Synthetic tiny document</html>")
	encoded := paddedFixtureGzip(body, 6*1024*1024)
	if len(encoded) <= 6*1024*1024 {
		t.Fatal("encoded probe below budget")
	}
	reader, err := gzip.NewReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(decoded, body) {
		t.Fatalf("invalid encoded probe: %v", err)
	}
}

// TestReplayProbeReceiptsExcludeUntrustedMetadata keeps prototype evidence limited to validated
// counters and booleans.
func TestReplayProbeReceiptsExcludeUntrustedMetadata(t *testing.T) {
	rows := replayProbeReceipts("native_replay_probe {\"event\":\"stream\",\"decoded\":20,\"url\":\"CANARY\",\"body\":\"CANARY\"}\nnative_replay_probe {\"event\":\"CANARY\"}")
	raw, _ := json.Marshal(rows)
	if rows["streams"] != 1 || strings.Contains(string(raw), "CANARY") {
		t.Fatal("unbounded metadata escaped probe")
	}
}
