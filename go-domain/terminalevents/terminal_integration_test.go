package terminalevents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixtureGuard struct{}

func (fixtureGuard) Check(context.Context) error { return nil }

type fixtureAuthority struct{ id string }

func (f fixtureAuthority) Instance(context.Context) (string, error) { return f.id, nil }
func TestFinalizedSourceWarningAndNoReplayPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("loopback fixture required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("terminal_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+q+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = migration.Run(ctx, pool, true, false); err != nil {
		t.Fatal(err)
	}
	worker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Release()
	executor, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Release()
	var wp, ep int
	if err = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &wp); err != nil {
		t.Fatal(err)
	}
	if err = executor.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # $1::integer),pg_backend_pid()`, int32(0x454E4732)).Scan(new(any), &ep); err != nil {
		t.Fatal(err)
	}
	const owner = "20000000-0000-4000-8000-000000000002"
	const instance = "30000000-0000-4000-8000-000000000003"
	hash := strings.Repeat("a", 64)
	if _, err = pool.Exec(ctx, `UPDATE background_worker_status SET owner_id=$1,generation=1,status='running',lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`, owner, wp); err != nil {
		t.Fatal(err)
	}
	count := 0
	server := New("fixture-token", pool, fixtureGuard{}, fixtureAuthority{instance}, func(ctx context.Context, title, message string) bool {
		count++
		var n int
		if err := pool.QueryRow(ctx, `SELECT 1`).Scan(&n); err != nil {
			t.Error(err)
		}
		if (count == 1 && (title != "Scheduled source warning" || !strings.Contains(message, "transport_failed"))) || (count == 2 && (title != "Scheduled source recovered" || !strings.Contains(message, "recovered"))) {
			t.Errorf("unexpected alert %s %s", title, message)
		}
		return true
	})
	var last Event
	for i := 1; i <= 4; i++ {
		job := fmt.Sprintf("10000000-0000-4000-8000-%012d", i)
		run := fmt.Sprintf("40000000-0000-4000-8000-%012d", i)
		result := `{"status":"warning","errors_count":1,"sources_completed":1,"sources_total":1,"source_outcomes":{"transport_failed":1}}`
		sourceState := "failed"
		var sourceError any = "transport_failed"
		if i > 2 {
			result = `{"status":"success","errors_count":0,"sources_completed":1,"sources_total":1,"source_outcomes":{}}`
			sourceState = "succeeded"
			sourceError = nil
		}
		_, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation,result,schedule_key) VALUES($1,'news_processing','{}',$2,'succeeded',1,1,$3::jsonb,'fixture-schedule')`, job, job, result)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,state,legacy_completion_applied_at,legacy_alert_state) VALUES($1,1,$2,$3,'succeeded',now(),'pending')`, job, hash, instance)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO article_ingestion_runs(id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,backend_pid,executor_instance_id,deadline_at,state,result) VALUES($1,$2,1,$3,$4,1,$5,$6,now()+interval '5 minutes','succeeded',$7::jsonb)`, run, job, hash, owner, wp, instance, result)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO article_ingestion_source_runs(run_id,source_id,source_type,config_sha256,state,error_code) VALUES($1,10,'custom',$2,$3,$4)`, run, hash, sourceState, sourceError)
		if err != nil {
			t.Fatal(err)
		}
		last = Event{1, job, 1, owner, 1, wp, instance, ep}
		body, _ := json.Marshal(last)
		req := httptest.NewRequest("POST", "/internal/v1/domain-events/finalized", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer fixture-token")
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("step %d status=%d body=%s", i, w.Code, w.Body.String())
		}
	}
	if count != 2 {
		t.Fatalf("source alerts=%d", count)
	}
	body, _ := json.Marshal(last)
	req := httptest.NewRequest("POST", "/internal/v1/domain-events/finalized", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer fixture-token")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != 200 || count != 2 {
		t.Fatalf("replay status=%d count=%d", w.Code, count)
	}
	if _, err = pool.Exec(ctx, `UPDATE background_job_executions SET legacy_alert_state='attempting' WHERE job_id=$1`, last.JobID); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("POST", "/internal/v1/domain-events/finalized", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer fixture-token")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != 200 || count != 2 || !strings.Contains(w.Body.String(), `"alert_state":"unknown"`) {
		t.Fatalf("uncertain send status=%d count=%d body=%s", w.Code, count, w.Body.String())
	}
	// The provider accepts the request and disconnects before a response. The
	// persisted event must become unknown and replay must not send again.
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, e := w.(http.Hijacker).Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		_ = conn.Close()
	}))
	defer provider.Close()
	transport := telegramdelivery.Transport{Endpoint: provider.URL, Token: "fixture-token", Client: provider.Client()}
	networkServer := New("fixture-token", pool, fixtureGuard{}, fixtureAuthority{instance}, func(ctx context.Context, title, message string) bool {
		return transport.Send(ctx, "sendMessage", map[string]any{"chat_id": "fixture", "text": message}).State == telegramdelivery.Sent
	})
	networkJob := "10000000-0000-4000-8000-000000000005"
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation,result,schedule_key) VALUES($1::uuid,'source_test','{}',$2,'failed',1,1,'{"status":"warning"}'::jsonb,'fixture-network')`, networkJob, networkJob); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,state,legacy_completion_applied_at,legacy_alert_state) VALUES($1,1,$2,$3,'failed',now(),'pending')`, networkJob, hash, instance); err != nil {
		t.Fatal(err)
	}
	networkEvent := Event{1, networkJob, 1, owner, 1, wp, instance, ep}
	networkBody, _ := json.Marshal(networkEvent)
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/internal/v1/domain-events/finalized", bytes.NewReader(networkBody))
		r.Header.Set("Authorization", "Bearer fixture-token")
		out := httptest.NewRecorder()
		networkServer.ServeHTTP(out, r)
		if out.Code != 200 || !strings.Contains(out.Body.String(), `"alert_state":"unknown"`) || calls.Load() != 1 {
			t.Fatalf("network replay step=%d status=%d calls=%d", i, out.Code, calls.Load())
		}
	}
	if _, err = worker.Exec(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()))`); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("POST", "/internal/v1/domain-events/finalized", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer fixture-token")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != 409 || count != 2 {
		t.Fatalf("lost worker status=%d count=%d", w.Code, count)
	}

}
