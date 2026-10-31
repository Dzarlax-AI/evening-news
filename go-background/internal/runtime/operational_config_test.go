package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	execution "github.com/Dzarlax-AI/evening-news/go-execution"
	"github.com/Dzarlax-AI/evening-news/go-execution/orchestration"
)

const snapshotJob = "20000000-0000-4000-8000-000000000002"

func operationalWireFixture(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../../tests/fixtures/operational-snapshot-v1/job-configuration.json")
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestOperationalCallbacksWireCompatibility(t *testing.T) {
	for _, name := range []string{"legacy", "viber_enabled", "viber_disabled", "unknown_top_level", "unknown_viber_field"} {
		t.Run(name, func(t *testing.T) {
			wire := operationalWireFixture(t)
			switch name {
			case "legacy":
				delete(wire, "viber")
			case "viber_disabled":
				wire["viber"] = map[string]any{"revision": 0, "delivery_enabled": false, "channel_id": "", "channel_name": "", "sender_id": "", "credential_revision": 0, "verified_credential_revision": 0, "webhook_ready": false, "verified_at": nil, "enabled_from": nil}
			case "unknown_top_level":
				wire["unexpected"] = true
			case "unknown_viber_field":
				wire["viber"].(map[string]any)["unexpected"] = true
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/internal/v1/job-configuration" || r.Header.Get("Authorization") != "Bearer fixture-token" {
					http.Error(w, "invalid fixture request", http.StatusBadRequest)
					return
				}
				if r.Method == http.MethodGet && (r.URL.Query().Get("job_id") != snapshotJob || r.URL.Query().Get("attempt_generation") != "1") {
					http.Error(w, "wrong snapshot identity", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(wire)
			}))
			defer server.Close()
			prepare, read := operationalCallbacks(server.URL, "fixture-token")
			pinned, postErr := prepare(context.Background(), execution.ExecutionContext{JobID: snapshotJob, AttemptGeneration: 1})
			loaded, getErr := read(context.Background(), snapshotJob, 1)
			if strings.HasPrefix(name, "unknown_") {
				if postErr == nil || getErr == nil {
					t.Fatal("unknown fields must remain rejected on both callback paths")
				}
				return
			}
			if postErr != nil || getErr != nil {
				t.Fatalf("valid snapshot rejected: prepare=%v read=%v", postErr, getErr)
			}
			encoded, err := json.Marshal(pinned)
			if err != nil {
				t.Fatal(err)
			}
			var roundTrip map[string]any
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			var expected map[string]any
			wireJSON, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(wireJSON, &expected); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(roundTrip, expected) || !loaded.SerbianEnabled || loaded.MaxArticles != 50 {
				t.Fatal("snapshot fields changed or were lost in the consumer")
			}
		})
	}
}

func TestProcessingPipelineAcceptsProducerOperationalSnapshot(t *testing.T) {
	wire := operationalWireFixture(t)
	var mu sync.Mutex
	var stages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/internal/v1/job-configuration":
			mu.Lock()
			stages = append(stages, "prepare")
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(wire)
		case r.URL.Path == "/internal/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"domain_instance_id": "10000000-0000-4000-8000-000000000001", "active_operations": 0, "ready": true})
		case strings.HasPrefix(r.URL.Path, "/internal/v1/ingestion/runs"), strings.HasPrefix(r.URL.Path, "/internal/v1/processing/runs"):
			if r.Method == http.MethodPost {
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{}`))
				return
			}
			stage := "processing"
			if strings.Contains(r.URL.Path, "/ingestion/") {
				stage = "ingestion"
			}
			mu.Lock()
			stages = append(stages, stage)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"run_id": path.Base(r.URL.Path), "job_id": snapshotJob, "attempt_generation": 1, "state": "succeeded", "result": map[string]any{"status": "success", "articles_fetched": 2, "articles_processed": 2}})
		case r.URL.Path == "/internal/v1/domain-operations":
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "invalid fixture operation", http.StatusBadRequest)
				return
			}
			if request["operation"] != "stats_publish" {
				http.Error(w, "unexpected fixture operation", http.StatusBadRequest)
				return
			}
			envelope := request["envelope"].(map[string]any)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "job_id": envelope["job_id"], "attempt_generation": envelope["attempt_generation"], "operation": request["operation"], "operation_id": request["operation_id"], "domain_instance_id": request["domain_instance_id"], "input_fingerprint": request["input_fingerprint"], "state": "succeeded", "result": map[string]any{"success": true}, "error_code": nil})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	prepare, _ := operationalCallbacks(server.URL, "fixture-token")
	pipeline, err := orchestration.NewPipeline(orchestration.Config{Domain: orchestration.DomainConfig{BaseURL: server.URL, Token: "fixture-token"}, Ingestion: orchestration.ServiceConfig{URL: server.URL, Token: "fixture-token"}, Processing: orchestration.ServiceConfig{URL: server.URL, Token: "fixture-token"}, PrepareJob: prepare})
	if err != nil {
		t.Fatal(err)
	}
	fence := execution.ExecutionContext{Version: 1, JobID: snapshotJob, AttemptGeneration: 1, OwnerID: "30000000-0000-4000-8000-000000000003", Generation: 1, BackendPID: 1, ExecutorInstanceID: "40000000-0000-4000-8000-000000000004", DeadlineAt: time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)}
	result, err := pipeline.Execute(context.Background(), fence, "news_processing", []byte(`{}`))
	if err != nil {
		t.Fatalf("processing pipeline rejected producer snapshot: %v", err)
	}
	var metrics map[string]any
	if err := json.Unmarshal(result.Value, &metrics); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(stages, []string{"prepare", "ingestion", "processing"}) || metrics["articles_processed"] != float64(2) {
		t.Fatalf("pipeline did not complete collection and processing: stages=%v", stages)
	}
}
