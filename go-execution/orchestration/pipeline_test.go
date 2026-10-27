package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
)

const testInstance = "10000000-0000-4000-8000-000000000001"
const testJob = "20000000-0000-4000-8000-000000000002"
const testOwner = "30000000-0000-4000-8000-000000000003"
const testExecutor = "40000000-0000-4000-8000-000000000004"

type fakeDomainOperation struct {
	req     domainRequest
	result  any
	state   string
	errCode *string
}
type fakeServices struct {
	mu                      sync.Mutex
	ops                     map[string]fakeDomainOperation
	calls                   []string
	publishCount, postCount int
	failMedia               bool
	lostAck                 bool
	domainUUID              string
	pendingProcessing       bool
	statusFailures          int
	statusCount             int
	cancelCount             int
	foreignReceipt          bool
	foreignReceiptField     string
	restartAfterPrepare     bool
	restartOnLostPublishAck bool
	pendingDomain           bool
}

func newFakeServer(t *testing.T, f *fakeServices) *httptest.Server {
	t.Helper()
	if f.ops == nil {
		f.ops = map[string]fakeDomainOperation{}
	}
	if f.domainUUID == "" {
		f.domainUUID = testInstance
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/v1/status", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.statusCount++
		if f.statusFailures > 0 {
			f.statusFailures--
			f.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		instance := f.domainUUID
		f.mu.Unlock()
		json.NewEncoder(w).Encode(DomainStatus{instance, 0, true})
	})
	mux.HandleFunc("POST /internal/v1/domain-operations", func(w http.ResponseWriter, r *http.Request) {
		var req domainRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			w.WriteHeader(400)
			return
		}
		if req.DomainInstanceID != f.domainUUID {
			w.WriteHeader(http.StatusConflict)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if req.DomainInstanceID != f.domainUUID {
			w.WriteHeader(http.StatusConflict)
			return
		}
		f.postCount++
		f.calls = append(f.calls, req.Operation)
		key := opKey(req.Envelope.JobID, req.Envelope.AttemptGeneration, req.Operation, req.OperationID)
		result := domainResult(req.Operation, req.Input)
		if req.Operation == "media_refresh" && f.failMedia {
			f.ops[key] = fakeDomainOperation{req: req, state: "failed", errCode: stringPtr("provider_timeout")}
		} else {
			state := "succeeded"
			if f.pendingDomain {
				state = "accepted"
				result = nil
			}
			f.ops[key] = fakeDomainOperation{req: req, state: state, result: result}
		}
		if f.foreignReceipt {
			f.ops[key] = fakeDomainOperation{req: req, state: "succeeded", result: result}
			op := f.ops[key]
			field := f.foreignReceiptField
			if field == "" {
				field = "job_id"
			}
			switch field {
			case "job_id":
				op.req.Envelope.JobID = "70000000-0000-4000-8000-000000000007"
			case "attempt_generation":
				op.req.Envelope.AttemptGeneration++
			case "operation":
				op.req.Operation = "media_refresh"
			case "operation_id":
				op.req.OperationID = "70000000-0000-4000-8000-000000000007"
			case "domain_instance_id":
				op.req.DomainInstanceID = "70000000-0000-4000-8000-000000000007"
			case "input_fingerprint":
				op.req.InputFingerprint = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			}
			f.ops[key] = op
		}
		if req.Operation == "digest_prepare" && f.restartAfterPrepare {
			f.domainUUID = "60000000-0000-4000-8000-000000000006"
		}
		if f.lostAck && (!f.restartOnLostPublishAck || req.Operation == "digest_publish") {
			f.lostAck = false
			if req.Operation == "digest_publish" && f.restartOnLostPublishAck {
				delete(f.ops, key)
				f.domainUUID = "60000000-0000-4000-8000-000000000006"
			}
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, e := hj.Hijack()
				if e == nil {
					conn.Close()
					return
				}
			}
		}
		w.WriteHeader(http.StatusAccepted)
		writeDomainReceipt(w, f.ops[key])
	})
	mux.HandleFunc("GET /internal/v1/domain-operations/{job}/{attempt}/{operation}/{id}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("job") + "/" + r.PathValue("attempt") + "/" + r.PathValue("operation") + "/" + r.PathValue("id")
		f.mu.Lock()
		defer f.mu.Unlock()
		op, ok := f.ops[key]
		if !ok {
			w.WriteHeader(404)
			return
		}
		if r.Header.Get("X-Domain-Instance-ID") != op.req.DomainInstanceID {
			w.WriteHeader(409)
			return
		}
		writeDomainReceipt(w, op)
	})
	mux.HandleFunc("POST /internal/v1/ingestion/runs", func(w http.ResponseWriter, r *http.Request) {
		var e serviceEnvelope
		_ = json.NewDecoder(r.Body).Decode(&e)
		id := deterministicRunID("/internal/v1/ingestion/runs", e.JobID, e.AttemptGeneration)
		f.mu.Lock()
		f.calls = append(f.calls, "ingestion")
		f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"run_id": id, "state": "running"})
	})
	mux.HandleFunc("GET /internal/v1/ingestion/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		json.NewEncoder(w).Encode(map[string]any{"run_id": id, "job_id": testJob, "attempt_generation": 1, "state": "succeeded", "result": map[string]any{"status": "warning", "sources_completed": 3, "articles_fetched": 6, "errors_count": 2, "source_outcomes": map[string]int{"provider_timeout": 2}}})
	})
	mux.HandleFunc("POST /internal/v1/processing/runs", func(w http.ResponseWriter, r *http.Request) {
		var e serviceEnvelope
		_ = json.NewDecoder(r.Body).Decode(&e)
		id := deterministicRunID("/internal/v1/processing/runs", e.JobID, e.AttemptGeneration)
		f.mu.Lock()
		f.calls = append(f.calls, "processing")
		f.mu.Unlock()
		state := "succeeded"
		if f.pendingProcessing {
			state = "running"
		}
		json.NewEncoder(w).Encode(map[string]any{"id": id, "job_id": testJob, "attempt_generation": 1, "state": state, "result": map[string]any{"status": "success", "articles_processed": 4, "errors_count": 1, "article_outcomes": []any{map[string]any{"article_id": 1, "state": "rejected"}}}})
	})
	mux.HandleFunc("GET /internal/v1/processing/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id"), "job_id": testJob, "attempt_generation": 1, "state": "running", "result": map[string]any{}})
	})
	mux.HandleFunc("POST /internal/v1/processing/runs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.cancelCount++
		f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id"), "job_id": testJob, "attempt_generation": 1, "state": "cancelled", "result": map[string]any{}})
	})
	return httptest.NewServer(mux)
}

func foreignOperation(op fakeDomainOperation) fakeDomainOperation {
	op.req.Envelope.JobID = "70000000-0000-4000-8000-000000000007"
	return op
}

func domainResult(op string, input json.RawMessage) any {
	switch op {
	case "summary_status":
		return map[string]any{"summaries_count": 0}
	case "summaries_generate":
		return map[string]any{"summaries_generated": 3}
	case "digest_prepare":
		return map[string]any{"prepared_token": "50000000-0000-4000-8000-000000000005"}
	case "digest_publish":
		return map[string]any{"success": true, "parts_sent": 2, "parts_total": 2, "rich_message": false}
	case "source_test":
		return map[string]any{"success": false}
	case "backup":
		return map[string]any{"backup_bytes": 1234}
	case "stats_publish":
		return map[string]any{"published": true}
	case "media_refresh":
		return map[string]any{"refreshed": true}
	}
	return map[string]any{}
}
func opKey(job string, attempt int64, op, id string) string {
	return fmt.Sprintf("%s/%d/%s/%s", job, attempt, op, id)
}
func writeDomainReceipt(w http.ResponseWriter, op fakeDomainOperation) {
	var result json.RawMessage
	if op.result != nil {
		result, _ = json.Marshal(op.result)
	}
	json.NewEncoder(w).Encode(map[string]any{"version": 1, "job_id": op.req.Envelope.JobID, "attempt_generation": op.req.Envelope.AttemptGeneration, "operation": op.req.Operation, "operation_id": op.req.OperationID, "domain_instance_id": op.req.DomainInstanceID, "input_fingerprint": op.req.InputFingerprint, "state": op.state, "result": result, "error_code": op.errCode})
}
func stringPtr(s string) *string { return &s }
func testFence() execution.ExecutionContext {
	return execution.ExecutionContext{Version: 1, JobID: testJob, AttemptGeneration: 1, OwnerID: testOwner, Generation: 3, BackendPID: 123, ExecutorInstanceID: testExecutor, DeadlineAt: time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339Nano)}
}
func testPipeline(t *testing.T, f *fakeServices) *Pipeline {
	t.Helper()
	srv := newFakeServer(t, f)
	t.Cleanup(srv.Close)
	cfg := Config{Domain: DomainConfig{BaseURL: srv.URL, Token: "job-token", PollInterval: time.Millisecond}, Ingestion: ServiceConfig{URL: srv.URL, Token: "job-token", PollInterval: time.Millisecond}, Processing: ServiceConfig{URL: srv.URL, Token: "job-token", PollInterval: time.Millisecond}}
	p, e := NewPipeline(cfg)
	if e != nil {
		t.Fatal(e)
	}
	return p
}

func TestPipelineReadyRecoversFromTransientDomainFailure(t *testing.T) {
	f := &fakeServices{statusFailures: 1}
	srv := newFakeServer(t, f)
	defer srv.Close()
	p, err := NewPipeline(Config{
		Domain:     DomainConfig{BaseURL: srv.URL, Token: "domain-token"},
		Ingestion:  ServiceConfig{URL: srv.URL, Token: "job-token"},
		Processing: ServiceConfig{URL: srv.URL, Token: "job-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Ready(context.Background()); err == nil {
		t.Fatal("expected first readiness probe to observe the transient 503")
	}
	if err = p.Ready(context.Background()); err != nil {
		t.Fatalf("second readiness probe did not recover: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusCount != 2 || f.postCount != 0 || len(f.calls) != 0 {
		t.Fatalf("status probes=%d leaf posts=%d job calls=%v", f.statusCount, f.postCount, f.calls)
	}
}

func TestPipelineAllJobKindsAndOrdering(t *testing.T) {
	cases := []struct {
		kind, payload string
		want          []string
	}{
		{"news_processing", `{}`, []string{"ingestion", "processing", "stats_publish"}},
		{"news_digest", `{"run_processing":false,"send_telegram":false}`, []string{}},
		{"news_digest", `{"run_processing":false}`, []string{"summary_status", "summaries_generate", "digest_prepare", "digest_publish"}},
		{"telegram_digest", `{}`, []string{"summary_status", "summaries_generate", "digest_prepare", "digest_publish"}},
		{"summaries_generate", `{}`, []string{"summaries_generate"}},
		{"backup", `{}`, []string{"backup"}},
		{"reprocess_failed", `{"limit":20,"dry_run":true}`, []string{"processing"}},
		{"article_reprocess", `{"article_ids":[2,1],"force":true,"reextract_media":true}`, []string{"media_refresh", "media_refresh", "processing"}},
		{"source_test", `{"source_id":7}`, []string{"source_test"}},
	}
	for _, tc := range cases {
		t.Run(tc.kind+tc.payload, func(t *testing.T) {
			f := &fakeServices{}
			p := testPipeline(t, f)
			got, err := p.Execute(context.Background(), testFence(), tc.kind, json.RawMessage(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			var output map[string]any
			if json.Unmarshal(got.Value, &output) != nil {
				t.Fatal("invalid output")
			}
			if tc.kind == "news_processing" {
				if output["status"] != "warning" || output["errors_count"] != float64(3) || output["articles_fetched"] != float64(6) {
					t.Fatalf("aggregate status/count projection mismatch: %s", got.Value)
				}
				f.mu.Lock()
				stats := f.ops[opKey(testJob, 1, "stats_publish", operationUUID(testFence(), "stats_publish", "stats_publish"))].req.Input
				f.mu.Unlock()
				var payload map[string]any
				_ = json.Unmarshal(stats, &payload)
				fields := payload["stats"].(map[string]any)
				if len(fields) != 5 || fields["errors_count"] != float64(3) || fields["api_calls_made"] != float64(0) {
					t.Fatalf("stats leaf payload does not match Python's bounded stats contract: %v", fields)
				}
				if _, ok := output["article_outcomes"]; ok {
					t.Fatalf("full-cycle receipt leaked selected article outcomes: %s", got.Value)
				}
			}
			if tc.kind == "article_reprocess" {
				if output["status"] != "warning" || output["article_outcomes_total"] != float64(1) {
					t.Fatalf("selected article receipt fields were not preserved: %s", got.Value)
				}
				if _, ok := output["article_outcomes"]; !ok {
					t.Fatalf("article outcomes missing: %s", got.Value)
				}
			}
			if tc.kind == "source_test" && (output["connection_ok"] != false || output["status"] != "warning") {
				t.Fatalf("source test result not safely projected: %s", got.Value)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.calls) != len(tc.want) {
				t.Fatalf("calls=%v want=%v", f.calls, tc.want)
			}
			for i := range tc.want {
				if f.calls[i] != tc.want[i] {
					t.Fatalf("calls=%v want=%v", f.calls, tc.want)
				}
			}
		})
	}
}

func TestNewsProcessingStopsOnIngestionFailure(t *testing.T) {
	f := &fakeServices{}
	srv := newFakeServer(t, f)
	defer srv.Close()
	muxURL := srv.URL
	// A closed local HTTP endpoint models a rejected ingestion service without external calls.
	p, err := NewPipeline(Config{Domain: DomainConfig{BaseURL: muxURL, Token: "x"}, Ingestion: ServiceConfig{URL: "http://127.0.0.1:1", Token: "x"}, Processing: ServiceConfig{URL: muxURL, Token: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Execute(context.Background(), testFence(), "news_processing", json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected ingestion failure")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, call := range f.calls {
		if call == "processing" {
			t.Fatal("processing ran after ingestion failure")
		}
	}
}

func TestDomainLostAckReconcilesWithoutResend(t *testing.T) {
	f := &fakeServices{lostAck: true}
	srv := newFakeServer(t, f)
	defer srv.Close()
	d, err := NewDomainClient(DomainConfig{BaseURL: srv.URL, Token: "t", PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"article_id": 9}
	raw, _ := json.Marshal(input)
	sum := sha256.Sum256(raw)
	fp := hex.EncodeToString(sum[:])
	_ = fp
	result, err := d.Execute(context.Background(), testFence(), "backup", "backup", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("missing result")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postCount != 1 {
		t.Fatalf("post count=%d want 1", f.postCount)
	}
}

func TestDomainLostAckRejectsMismatchedSucceededReceipt(t *testing.T) {
	for _, field := range []string{"job_id", "attempt_generation", "operation", "operation_id", "domain_instance_id", "input_fingerprint"} {
		t.Run(field, func(t *testing.T) {
			f := &fakeServices{lostAck: true, foreignReceipt: true, foreignReceiptField: field}
			srv := newFakeServer(t, f)
			defer srv.Close()
			d, err := NewDomainClient(DomainConfig{BaseURL: srv.URL, Token: "t", PollInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			_, err = d.Execute(context.Background(), testFence(), "backup", "backup", map[string]any{})
			if err == nil {
				t.Fatalf("mismatched succeeded receipt after lost POST acknowledgement was accepted (%s)", field)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.postCount != 1 {
				t.Fatalf("POST count=%d want exactly 1", f.postCount)
			}
		})
	}
}

func TestStatsInputFingerprintMatchesPythonFractionalDurationVector(t *testing.T) {
	if got := durationSeconds(1234 * time.Millisecond); got != 1.234 {
		t.Fatalf("duration rounding=%v want 1.234", got)
	}
	f := &fakeServices{}
	srv := newFakeServer(t, f)
	defer srv.Close()
	d, err := NewDomainClient(DomainConfig{BaseURL: srv.URL, Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"stats": map[string]any{
		"api_calls_made": 0, "articles_fetched": 6, "articles_processed": 4,
		"duration_seconds": durationSeconds(1234 * time.Millisecond), "errors_count": 3,
	}}
	if _, err = d.Execute(context.Background(), testFence(), "stats_publish", "stats_publish", input); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := opKey(testJob, 1, "stats_publish", operationUUID(testFence(), "stats_publish", "stats_publish"))
	if got := f.ops[key].req.InputFingerprint; got != "3b46c9082094f484d942821ac1e0ca6da7b2f708a381ea6d779a08956560168f" {
		t.Fatalf("fractional duration fingerprint=%s", got)
	}
}

func TestDomainRejectsForeignReceiptAndInstancePin(t *testing.T) {
	f := &fakeServices{}
	srv := newFakeServer(t, f)
	defer srv.Close()
	d, _ := NewDomainClient(DomainConfig{BaseURL: srv.URL, Token: "t", PollInterval: time.Millisecond})
	ctx := context.Background()
	status, err := d.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.ExecutePinned(ctx, testFence(), "50000000-0000-4000-8000-000000000005", "backup", "backup", map[string]any{}); err == nil {
		t.Fatal("expected 404/unknown operation")
	}
	if status.DomainInstanceID != testInstance {
		t.Fatal("unexpected pin")
	}
}

func TestDomainOperationIDWireVector(t *testing.T) {
	data, err := os.ReadFile("testdata/domain-operation-id-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Version     int      `json:"version"`
		Namespace   string   `json:"namespace"`
		Tuple       []string `json:"tuple"`
		OperationID string   `json:"operation_id"`
	}
	if json.Unmarshal(data, &fixture) != nil || fixture.Version != 1 || len(fixture.Tuple) != 4 {
		t.Fatal("operation id fixture is invalid")
	}
	if fixture.Namespace != domainOperationIDNamespace {
		t.Fatalf("operation id namespace changed: %q", fixture.Namespace)
	}
	attempt, err := strconv.ParseInt(fixture.Tuple[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	f := execution.ExecutionContext{JobID: fixture.Tuple[0], AttemptGeneration: attempt}
	got := operationUUID(f, fixture.Tuple[2], fixture.Tuple[3])
	if got != fixture.OperationID {
		t.Fatalf("operation id vector changed: %s", got)
	}
	if got == operationUUID(testFence(), "backup", "other") || got == operationUUID(testFence(), "stats_publish", "backup") {
		t.Fatal("logical operation identity was not included in operation id")
	}
}

func TestProcessingCancellationIsConfirmedWithoutResubmit(t *testing.T) {
	f := &fakeServices{pendingProcessing: true}
	srv := newFakeServer(t, f)
	defer srv.Close()
	c, err := newServiceRunClient(ServiceConfig{URL: srv.URL, Token: "job-token", PollInterval: time.Second}, "/internal/v1/processing/runs")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.execute(ctx, testFence()); done <- e }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	err = <-done
	if err == nil || err.Error() != "context canceled" {
		t.Fatalf("cancellation result=%v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	posts := 0
	for _, v := range f.calls {
		if v == "processing" {
			posts++
		}
	}
	if posts != 1 || f.cancelCount != 1 {
		t.Fatalf("posts=%d cancels=%d", posts, f.cancelCount)
	}
}

func TestDomainRejectsForeignOperationReceipt(t *testing.T) {
	f := &fakeServices{foreignReceipt: true}
	srv := newFakeServer(t, f)
	defer srv.Close()
	d, err := NewDomainClient(DomainConfig{BaseURL: srv.URL, Token: "t", PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Execute(context.Background(), testFence(), "backup", "backup", map[string]any{})
	if err == nil {
		t.Fatal("foreign receipt accepted")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postCount != 1 {
		t.Fatalf("unexpected resubmission count=%d", f.postCount)
	}
}

func TestDigestStopsWhenDomainRestartsAfterPreparation(t *testing.T) {
	f := &fakeServices{restartAfterPrepare: true}
	p := testPipeline(t, f)
	if _, err := p.Execute(context.Background(), testFence(), "telegram_digest", json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected domain restart to invalidate prepared digest")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range f.calls {
		if op == "digest_publish" {
			t.Fatal("digest publish crossed domain instance restart")
		}
	}
}

func TestDigestLostPublishAckAfterRestartIsNeverResent(t *testing.T) {
	f := &fakeServices{lostAck: true, restartOnLostPublishAck: true}
	p := testPipeline(t, f)
	if _, err := p.Execute(context.Background(), testFence(), "telegram_digest", json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected unknown publication after restart")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	published := 0
	for _, op := range f.calls {
		if op == "digest_publish" {
			published++
		}
	}
	if published != 1 {
		t.Fatalf("digest publish attempts=%d want 1", published)
	}
}

func TestDomainPollingCancellationDoesNotResubmit(t *testing.T) {
	f := &fakeServices{pendingDomain: true}
	srv := newFakeServer(t, f)
	defer srv.Close()
	d, err := NewDomainClient(DomainConfig{BaseURL: srv.URL, Token: "t", PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := d.Execute(ctx, testFence(), "backup", "backup", map[string]any{}); done <- e }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	err = <-done
	if err == nil || err.Error() != "context canceled" {
		t.Fatalf("cancellation result=%v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postCount != 1 {
		t.Fatalf("domain operation POST count=%d", f.postCount)
	}
}
