package callbacks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimUsesActualMigratedFence(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("loopback test database required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("claim_migrated_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+q+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 6
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
	var workerPID, executorPID int
	if err = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &workerPID); err != nil {
		t.Fatal(err)
	}
	if err = executor.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # $1::integer),pg_backend_pid()`, int32(0x454E4732)).Scan(new(any), &executorPID); err != nil {
		t.Fatal(err)
	}
	const job = "10000000-0000-4000-8000-000000000001"
	const owner = "20000000-0000-4000-8000-000000000002"
	const instance = "30000000-0000-4000-8000-000000000003"
	const token = "50000000-0000-4000-8000-000000000005"
	deadline := time.Now().UTC().Add(4 * time.Minute).Truncate(time.Microsecond)
	env := authority.Envelope{Version: 1, JobID: job, AttemptGeneration: 1, OwnerID: owner, Generation: 1, BackendPID: workerPID, ExecutorInstanceID: instance, DeadlineAt: deadline.Format(time.RFC3339Nano)}
	rawEnv, _ := json.Marshal(env)
	digest := hash(rawEnv)
	run := runID(job, 1)
	input := map[string]any{"title": "fixture", "content": "article", "url": "https://example.test/story"}
	fingerprint, err := analysisFingerprint(input)
	if err != nil {
		t.Fatal(err)
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{`UPDATE background_worker_status SET owner_id=$1,generation=1,status='running',lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`, []any{owner, workerPID}},
		{`INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES ($1,'news_processing','{}','fixture','running',1,1)`, []any{job}},
		{`INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, []any{job, digest, instance, owner, deadline}},
		{`INSERT INTO articles(id,title,url,processing_attempts) VALUES(7,'fixture','https://example.test/story',1)`, nil},
		{`SELECT * FROM fence_article_processing_attempt($1,$2,1,$3,$4,1,$5,$6,$7)`, []any{run, job, digest, owner, workerPID, instance, deadline}},
		{`INSERT INTO article_processing_items(run_id,article_id,article_attempt,input_fingerprint,claim_token,state) VALUES($1,7,1,$2,$3,'running')`, []any{run, fingerprint, token}},
	}
	for _, v := range statements {
		if _, err = pool.Exec(ctx, v.sql, v.args...); err != nil {
			t.Fatal(err)
		}
	}
	claim := Claim{Fence: authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "news_processing", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: workerPID, ExecutorBackendPID: executorPID, Deadline: deadline, Payload: []byte(`{}`)}, RunID: run, EnvelopeDigest: digest, ArticleID: 7, ArticleAttempt: 1, InputFingerprint: fingerprint, URL: "https://example.test/story"}
	if err = claim.Validate(ctx); err != nil {
		t.Fatalf("live migrated claim: %v", err)
	}
	current := authority.ExecutionContext{Version: 1, Envelope: env, ExecutorBackendPID: executorPID, Kind: "news_processing", Payload: json.RawMessage(`{}`)}
	calc := &mutatingCompute{pool: pool}
	handler := Handler{Token: "fixture-token", Guard: migratedGuard{}, Authority: migratedAuthority{current}, Pool: pool, Admission: NewAdmission(16, 64, 1024*1024), Compute: calc}
	rawInput, _ := json.Marshal(input)
	request := Request{Version: 1, RunID: run, Envelope: env, ArticleID: 7, ArticleAttempt: 1, InputFingerprint: fingerprint, Operation: "analyze", Input: rawInput}
	rawReq, _ := json.Marshal(request)
	r := httptest.NewRequest("POST", "/internal/v1/article-ai", bytes.NewReader(rawReq))
	r.Header.Set("Authorization", "Bearer fixture-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 409 || calc.calls != 1 {
		t.Fatalf("mid-compute stale response status=%d calls=%d body=%s", w.Code, calc.calls, w.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE articles SET processing_attempts=2 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if err = claim.Validate(ctx); !errors.Is(err, authority.ErrStale) {
		t.Fatalf("stale migrated claim: %v", err)
	}
}

type migratedGuard struct{}

func (migratedGuard) Check(context.Context) error { return nil }

type migratedAuthority struct{ v authority.ExecutionContext }

func (a migratedAuthority) Get(context.Context, string, int) (authority.ExecutionContext, error) {
	return a.v, nil
}

type mutatingCompute struct {
	pool  *pgxpool.Pool
	calls int
}

func (m *mutatingCompute) Validate(string, json.RawMessage) error { return nil }
func (m *mutatingCompute) Run(ctx context.Context, _ Request, _ authority.Fence) (any, error) {
	m.calls++
	_, err := m.pool.Exec(ctx, `UPDATE articles SET processing_attempts=2 WHERE id=7`)
	return map[string]any{"value": "fixture"}, err
}
