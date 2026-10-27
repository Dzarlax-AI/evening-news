package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	execution "github.com/Dzarlax-AI/evening-news/go-execution"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func isLoopbackExecutionTestDSN(dsn string) bool {
	config, err := pgx.ParseConfig(dsn)
	if err != nil || config == nil || !isLoopbackExecutionTestHost(config.Host) {
		return false
	}
	for _, fallback := range config.Fallbacks {
		if fallback == nil || !isLoopbackExecutionTestHost(fallback.Host) {
			return false
		}
	}
	return true
}

func isLoopbackExecutionTestHost(host string) bool {
	host = strings.TrimSpace(host)
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func TestLoopbackExecutionTestDSNUsesEffectiveHosts(t *testing.T) {
	for _, tc := range []struct {
		name, dsn string
		want      bool
	}{
		{"direct IPv4", "postgres://user:pass@127.0.0.1:55446/db", true},
		{"direct IPv6", "postgres://user:pass@[::1]:55446/db", true},
		{"localhost", "postgres://user:pass@localhost:55446/db", true},
		{"loopback query override", "postgres://user:pass@db.internal:55446/db?host=127.0.0.1", true},
		{"remote query override", "postgres://user:pass@127.0.0.1:55446/db?host=db.internal", false},
		{"safe fallback hosts", "postgres://user:pass@127.0.0.1:55446/db?host=127.0.0.1,localhost", true},
		{"remote fallback host", "postgres://user:pass@127.0.0.1:55446/db?host=127.0.0.1,192.0.2.1", false},
		{"remote authority", "postgres://user:pass@192.0.2.1:55446/db", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLoopbackExecutionTestDSN(tc.dsn); got != tc.want {
				t.Fatalf("isLoopbackExecutionTestDSN(%q) = %v, want %v", tc.dsn, got, tc.want)
			}
		})
	}
}

func TestComposedExecutionRunnerKeepsPipelineAliveAfterInitialization(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL execution tests")
	}
	if !isLoopbackExecutionTestDSN(dsn) {
		t.Fatal("execution tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("background_execution_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+quoted+` CASCADE`) }()
	if _, err := admin.Exec(ctx, `SET search_path TO `+quoted); err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`CREATE TABLE background_worker_status(id integer PRIMARY KEY,generation integer NOT NULL,owner_id uuid,lock_backend_pid integer,heartbeat_at timestamp,status text NOT NULL)`,
		`CREATE TABLE background_jobs(id uuid PRIMARY KEY,kind text NOT NULL,payload jsonb NOT NULL,result jsonb,state text NOT NULL,owner_generation integer,attempt_generation integer NOT NULL,initialized_at timestamp)`,
		`CREATE TABLE background_job_executions(job_id uuid NOT NULL REFERENCES background_jobs(id),attempt_generation integer NOT NULL,envelope_sha256 text NOT NULL,executor_instance_id uuid NOT NULL,owner_id uuid,worker_generation integer,deadline_at timestamp,state text NOT NULL,accepted_at timestamp NOT NULL DEFAULT clock_timestamp(),started_at timestamp,finished_at timestamp,result jsonb,error_code text,legacy_completion_applied_at timestamp,PRIMARY KEY(job_id,attempt_generation))`,
		`CREATE TABLE articles(id bigint PRIMARY KEY,processing_state text,processing_attempts integer,next_attempt_at timestamp,last_processing_error text,processed boolean,summary_processed boolean,category_processed boolean,ad_processed boolean)`,
	} {
		if _, err := admin.Exec(ctx, ddl); err != nil {
			t.Fatalf("create schema table: %v", err)
		}
	}
	owner, job := uuid.New(), uuid.New()
	worker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close(context.Background())
	if _, err := worker.Exec(ctx, `SET search_path TO `+quoted); err != nil {
		t.Fatal(err)
	}
	var workerPID int32
	var acquired bool
	if err := worker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &workerPID); err != nil || !acquired {
		t.Fatalf("acquire worker advisory lease: acquired=%v err=%v", acquired, err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,9,$1,$2,clock_timestamp() AT TIME ZONE 'UTC','running')`, owner, workerPID); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'backup','{}','running',9,1)`, job); err != nil {
		t.Fatal(err)
	}

	domainID := "33333333-3333-4333-8333-333333333333"
	domain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/status":
			_, _ = fmt.Fprintf(w, `{"domain_instance_id":%q,"active_operations":0,"ready":true}`, domainID)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/domain-operations":
			var req struct {
				Version          int    `json:"version"`
				Operation        string `json:"operation"`
				OperationID      string `json:"operation_id"`
				DomainInstanceID string `json:"domain_instance_id"`
				InputFingerprint string `json:"input_fingerprint"`
				Envelope         struct {
					JobID             string `json:"job_id"`
					AttemptGeneration int64  `json:"attempt_generation"`
				} `json:"envelope"`
			}
			if json.NewDecoder(r.Body).Decode(&req) != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version": 1, "job_id": req.Envelope.JobID, "attempt_generation": req.Envelope.AttemptGeneration,
				"operation": req.Operation, "operation_id": req.OperationID, "domain_instance_id": req.DomainInstanceID,
				"input_fingerprint": req.InputFingerprint, "state": "succeeded", "result": map[string]any{"success": true}, "error_code": nil,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer domain.Close()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	cfg := testConfig()
	cfg.Execution = execution.Config{DatabaseURL: dsn, Schema: schema, Token: "fixture-background-token", GuardInterval: 20 * time.Millisecond, QueryTimeout: time.Second}
	cfg.ExecutionListenAddr = addr
	cfg.DomainServiceURL = domain.URL
	cfg.Ingestion.ListenAddr, cfg.Ingestion.Token = ":8093", "fixture-ingestion-token"
	cfg.Processing.ListenAddr, cfg.Processing.Token = ":8094", "fixture-processing-token"
	ready := make(chan bool, 2)
	done := make(chan error, 1)
	go func() { done <- runExecution(ctx, cfg, func(ok bool) { ready <- ok }) }()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("execution runner did not become ready")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("composed execution runner startup timed out")
	}
	deadline := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	body := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":9,"attempt_generation":1,"deadline_at":%q}`, job.String(), owner.String(), deadline)
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/internal/v1/job-executions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer fixture-background-token")
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("post execution: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("accept status=%d", resp.StatusCode)
	}
	deadlinePoll := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadlinePoll) {
		var state string
		if err := admin.QueryRow(ctx, `SELECT state FROM `+quoted+`.background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&state); err == nil && state == "succeeded" {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("composed runner shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("composed runner did not stop")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("pipeline was canceled or did not complete after initialization")
}
