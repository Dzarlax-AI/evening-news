package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLegacyAdapterPostgresFinalizationAndRecovery(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("EXECUTION_FIXTURE_DSN")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL (or EXECUTION_FIXTURE_DSN) is required for PostgreSQL orchestration tests")
	}
	if !isLoopbackPostgresDSN(dsn) {
		t.Fatal("orchestration PostgreSQL tests require a loopback-only test database URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "orch_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, _ = admin.Exec(cleanup, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	})
	worker := connectLockConn(t, ctx, dsn, schema, 0)
	defer worker.Close(ctx)
	guard := connectLockConn(t, ctx, dsn, schema, execution.ExecutionGuardXOR)
	defer guard.Close(ctx)
	var workerPID, guardPID int32
	_ = worker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&workerPID)
	_ = guard.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&guardPID)
	owner := uuid.NewString()
	job := uuid.NewString()
	executor := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)
	createSchemaTables(t, ctx, admin, schema)
	_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.background_worker_status VALUES(1,$1::uuid,3,$2,$3,'running')`, pgx.Identifier{schema}.Sanitize()), owner, workerPID, now)
	if err != nil {
		t.Fatal(err)
	}
	result, _ := json.Marshal(map[string]any{"status": "warning", "duration_seconds": 12.5})
	_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.background_jobs(id,state,attempt_generation,payload,result,error_code,finished_at,created_at,schedule_key) VALUES($1::uuid,'succeeded',1,'{"schedule_id":1}'::jsonb,$2::jsonb,NULL,$3,$3,'daily')`, pgx.Identifier{schema}.Sanitize()), job, result, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state,result) VALUES($1::uuid,1,repeat('a',64),$2::uuid,$3::uuid,3,$4,'succeeded',$5::jsonb)`, pgx.Identifier{schema}.Sanitize()), job, executor, owner, now.Add(time.Minute), result)
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.schedule_settings(id,task_name,enabled,schedule_type,hour,minute,weekdays,timezone,task_config,is_running,updated_at) VALUES(1,'news_processing',true,'interval',9,0,'[]','UTC','{}',true,$1)`, pgx.Identifier{schema}.Sanitize()), now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var eventCount int
	unknownAckJob := ""
	terminal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/domain-events/finalized" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			w.WriteHeader(401)
			return
		}
		var request struct {
			JobID             string `json:"job_id"`
			AttemptGeneration int64  `json:"attempt_generation"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode terminal request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		eventCount++
		if request.JobID == unknownAckJob {
			// Model a lost acknowledgement after the Python leaf durably began
			// alert delivery. Go must conservatively turn attempting into unknown.
			if _, err := pool.Exec(r.Context(), `UPDATE background_job_executions SET legacy_alert_state='attempting' WHERE job_id=$1::uuid AND attempt_generation=$2`, request.JobID, request.AttemptGeneration); err != nil {
				t.Errorf("simulate durable attempting state: %v", err)
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Python owns this durable state write; Go only reads it after the
		// terminal leaf returns.
		if _, err := pool.Exec(r.Context(), `UPDATE background_job_executions SET legacy_alert_state='delivered' WHERE job_id=$1::uuid AND attempt_generation=$2`, request.JobID, request.AttemptGeneration); err != nil {
			t.Errorf("simulate durable delivered state: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"job_id":%q,"attempt_generation":%d,"legacy_completion_applied":true,"alert_state":"delivered"}`, request.JobID, request.AttemptGeneration)))
	}))
	defer terminal.Close()
	domain, err := NewDomainClient(DomainConfig{BaseURL: terminal.URL, Token: "fixture-token"})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewLegacyAdapter(pool, domain, func(context.Context) (int32, error) { return guardPID, nil }, func() string { return executor })
	if err != nil {
		t.Fatal(err)
	}
	f := execution.ExecutionContext{Version: 1, JobID: job, AttemptGeneration: 1}
	if _, err = adapter.Finalize(ctx, f); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("terminal events=%d want 1", eventCount)
	}
	var applied *time.Time
	var alert string
	var schedStatus string
	var running bool
	var lastErr *string
	if err = pool.QueryRow(ctx, `SELECT legacy_completion_applied_at,legacy_alert_state FROM background_job_executions WHERE job_id=$1::uuid`, job).Scan(&applied, &alert); err != nil {
		t.Fatal(err)
	}
	if applied == nil || alert != "delivered" {
		t.Fatalf("completion marker=%v alert=%s", applied, alert)
	}
	if err = pool.QueryRow(ctx, `SELECT last_status,is_running,last_error FROM schedule_settings WHERE id=1`).Scan(&schedStatus, &running, &lastErr); err != nil {
		t.Fatal(err)
	}
	if schedStatus != "warning" || running || lastErr != nil {
		t.Fatalf("schedule status=%s running=%v error=%v", schedStatus, running, lastErr)
	}
	// Retrying finalization does not replay the terminal notification.
	replayed, err := adapter.Finalize(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	var replayBody map[string]any
	if err = json.Unmarshal(replayed, &replayBody); err != nil || replayBody["alert_state"] != "delivered" {
		t.Fatalf("replayed alert state=%s err=%v", replayed, err)
	}
	if eventCount != 1 {
		t.Fatalf("terminal event replayed: %d", eventCount)
	}

	// A failed terminal acknowledgement can happen after the leaf has moved
	// pending to attempting. Go must fence and mark that state unknown and must
	// never POST again on replay.
	unknownAckJob = uuid.NewString()
	_, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,state,attempt_generation,payload,result,error_code,finished_at,created_at,schedule_key,owner_generation) VALUES($1::uuid,'succeeded',1,'{"schedule_id":1}'::jsonb,$2::jsonb,NULL,$3,$3,'daily',3)`, unknownAckJob, result, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state,result,legacy_alert_state) VALUES($1::uuid,1,repeat('b',64),$2::uuid,$3::uuid,3,$4,'succeeded',$5::jsonb,'pending')`, unknownAckJob, executor, owner, now.Add(time.Minute), result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.Finalize(ctx, execution.ExecutionContext{Version: 1, JobID: unknownAckJob, AttemptGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	var unknownState string
	if err = pool.QueryRow(ctx, `SELECT legacy_alert_state FROM background_job_executions WHERE job_id=$1::uuid`, unknownAckJob).Scan(&unknownState); err != nil || unknownState != "unknown" {
		t.Fatalf("lost-ack alert state=%q err=%v", unknownState, err)
	}
	if _, err = adapter.Finalize(ctx, execution.ExecutionContext{Version: 1, JobID: unknownAckJob, AttemptGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 {
		t.Fatalf("terminal event resent after ambiguous acknowledgement: %d", eventCount)
	}
	_, err = pool.Exec(ctx, `UPDATE schedule_settings SET is_running=true,task_name='telegram_digest',next_run=clock_timestamp() AT TIME ZONE 'UTC'-interval '1 minute' WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := adapter.Recover(ctx, owner, 3, workerPID)
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	_ = json.Unmarshal(recovered, &response)
	if response["schedules_reconciled"] != float64(1) {
		t.Fatalf("recovery result=%s", recovered)
	}
	if err = pool.QueryRow(ctx, `SELECT last_status,is_running,last_error FROM schedule_settings WHERE id=1`).Scan(&schedStatus, &running, &lastErr); err != nil {
		t.Fatal(err)
	}
	if schedStatus != "delivery_unknown" || running || lastErr == nil {
		t.Fatalf("recovered schedule status=%s running=%v error=%v", schedStatus, running, lastErr)
	}
}

func TestLoopbackPostgresDSNValidation(t *testing.T) {
	for _, tc := range []struct {
		dsn  string
		want bool
	}{
		{"postgres://user:pass@127.0.0.1/db", true},
		{"postgresql://user:pass@[::1]:5432/db", true},
		{"postgres://user:pass@localhost/db", true},
		{"postgres://user:pass@db.internal/db", false},
		{"postgres://user:pass@192.0.2.1/db", false},
		{"not a url", false},
	} {
		if got := isLoopbackPostgresDSN(tc.dsn); got != tc.want {
			t.Errorf("isLoopbackPostgresDSN(%q)=%v want %v", tc.dsn, got, tc.want)
		}
	}
}

func isLoopbackPostgresDSN(dsn string) bool {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" {
		return false
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func connectLockConn(t *testing.T, ctx context.Context, dsn, schema string, xor int64) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if xor == 0 {
		_, err = c.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()))`)
	} else {
		_, err = c.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # $1)`, xor)
	}
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func createSchemaTables(t *testing.T, ctx context.Context, p *pgxpool.Pool, schema string) {
	t.Helper()
	q := pgx.Identifier{schema}.Sanitize()
	ddl := []string{
		`CREATE TABLE ` + q + `.background_worker_status(id int primary key,owner_id uuid,generation bigint,lock_backend_pid int,heartbeat_at timestamp,status text)`,
		`CREATE TABLE ` + q + `.background_jobs(id uuid primary key,state text,attempt_generation int,payload jsonb,result jsonb,error_code text,finished_at timestamp,created_at timestamp,schedule_key text,owner_generation int)`,
		`CREATE TABLE ` + q + `.background_job_executions(job_id uuid,attempt_generation int,envelope_sha256 text,executor_instance_id uuid,owner_id uuid,worker_generation int,deadline_at timestamp,state text,result jsonb,error_code text,finished_at timestamp,legacy_completion_applied_at timestamp,legacy_alert_state text,primary key(job_id,attempt_generation))`,
		`CREATE TABLE ` + q + `.schedule_settings(id int primary key,task_name text,enabled boolean,schedule_type text,hour int,minute int,weekdays jsonb,timezone text,task_config jsonb,is_running boolean,updated_at timestamp,last_finished_at timestamp,last_status text,last_error text,last_duration_seconds numeric,last_success_at timestamp,next_run timestamp)`,
	}
	for _, sql := range ddl {
		if _, err := p.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
}
