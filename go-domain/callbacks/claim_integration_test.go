package callbacks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimPostgresRejectsChangedArticle(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("loopback database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("callback_claim_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
	sql := []string{
		`CREATE TABLE ` + q + `.background_worker_status(id integer primary key,owner_id uuid,generation integer,status text,lock_backend_pid integer,heartbeat_at timestamp)`,
		`CREATE TABLE ` + q + `.background_jobs(id uuid primary key,state text,owner_generation integer,attempt_generation integer,kind text,payload jsonb)`,
		`CREATE TABLE ` + q + `.background_job_executions(job_id uuid,attempt_generation integer,executor_instance_id uuid,owner_id uuid,worker_generation integer,deadline_at timestamp,state text)`,
		`CREATE TABLE ` + q + `.article_processing_runs(id uuid,job_id uuid,attempt_generation integer,envelope_sha256 text,state text)`,
		`CREATE TABLE ` + q + `.article_processing_items(run_id uuid,article_id bigint,article_attempt integer,input_fingerprint text,state text)`,
		`CREATE TABLE ` + q + `.articles(id bigint,processing_attempts integer,url text)`,
		`CREATE FUNCTION ` + q + `.fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp) RETURNS TABLE(run_id uuid,run_state text) LANGUAGE sql AS $$ SELECT id,state FROM ` + q + `.article_processing_runs WHERE id=$1 AND job_id=$2 AND attempt_generation=$3 AND envelope_sha256=$4 FOR UPDATE $$`,
	}
	for _, s := range sql {
		if _, err = admin.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
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
	const run = "40000000-0000-4000-8000-000000000004"
	deadline := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO ` + q + `.background_worker_status VALUES(1,$1,1,'running',$2,clock_timestamp() AT TIME ZONE 'UTC')`, []any{owner, workerPID}},
		{`INSERT INTO ` + q + `.background_jobs VALUES($1,'running',1,1,'news_processing','{}')`, []any{job}},
		{`INSERT INTO ` + q + `.background_job_executions VALUES($1,1,$2,$3,1,$4,'running')`, []any{job, instance, owner, deadline}},
		{`INSERT INTO ` + q + `.article_processing_runs VALUES($1,$2,1,'envelope','running')`, []any{run, job}},
		{`INSERT INTO ` + q + `.article_processing_items VALUES($1,7,1,'fingerprint','running')`, []any{run}},
		{`INSERT INTO ` + q + `.articles VALUES(7,1,'https://example.test/story')`, nil},
	}
	for _, s := range statements {
		if _, err = admin.Exec(ctx, s.sql, s.args...); err != nil {
			t.Fatal(err)
		}
	}
	claim := Claim{Fence: authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "news_processing", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: workerPID, ExecutorBackendPID: executorPID, Deadline: deadline, Payload: []byte(`{}`)}, RunID: run, EnvelopeDigest: "envelope", ArticleID: 7, ArticleAttempt: 1, InputFingerprint: "fingerprint", URL: "https://example.test/story"}
	if err = claim.Validate(ctx); err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.articles SET processing_attempts=2 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if err = claim.Validate(ctx); !errors.Is(err, authority.ErrStale) {
		t.Fatalf("changed article claim: %v", err)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.articles SET processing_attempts=1,url='https://example.test/changed' WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if err = claim.Validate(ctx); !errors.Is(err, authority.ErrStale) {
		t.Fatalf("changed URL claim: %v", err)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.articles SET url='https://example.test/story' WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.article_processing_items SET state='succeeded'`); err != nil {
		t.Fatal(err)
	}
	if err = claim.Validate(ctx); !errors.Is(err, authority.ErrStale) {
		t.Fatalf("changed item claim: %v", err)
	}

	// Exercise the real HTTP handler, authority fetch and PostgreSQL fences with
	// synthetic compute. The migration function above is deliberately simplified.
	env := authority.Envelope{Version: 1, JobID: job, AttemptGeneration: 1, OwnerID: owner, Generation: 1, BackendPID: workerPID, ExecutorInstanceID: instance, DeadlineAt: deadline.Format(time.RFC3339Nano)}
	envJSON, _ := json.Marshal(env)
	actualRun := runID(job, 1)
	fingerprint := strings.Repeat("a", 64)
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.article_processing_runs SET id=$1,envelope_sha256=$2`, actualRun, hash(envJSON)); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.article_processing_items SET run_id=$1,input_fingerprint=$2,state='running'`, actualRun, fingerprint); err != nil {
		t.Fatal(err)
	}
	guard, err := authority.AcquireGuard(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close(context.Background())
	authorityServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-executor" {
			t.Error("missing authority token")
		}
		_ = json.NewEncoder(w).Encode(authority.ExecutionContext{Version: 1, Envelope: env, ExecutorBackendPID: executorPID, Kind: "news_processing", Payload: json.RawMessage(`{}`)})
	}))
	defer authorityServer.Close()
	client, err := authority.NewContextClient(authorityServer.URL, "fixture-executor")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	compute := &fixtureCompute{}
	h := &Handler{Token: "fixture-ai", Guard: guard, Authority: client, Pool: pool, Admission: NewAdmission(16, 64, 1024*1024), Compute: compute}
	request := Request{Version: 1, RunID: actualRun, Envelope: env, ArticleID: 7, ArticleAttempt: 1, InputFingerprint: fingerprint, Operation: "extraction_context", Input: json.RawMessage(`{"url":"https://example.test/story"}`)}
	wire, _ := json.Marshal(request)
	call := func(token string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/internal/v1/article-ai", bytes.NewReader(wire))
		r.Header.Set("Authorization", "Bearer "+token)
		h.ServeHTTP(rec, r)
		return rec
	}
	if got := call("wrong"); got.Code != 401 {
		t.Fatalf("wrong token status=%d", got.Code)
	}
	if got := call("fixture-ai"); got.Code != 200 {
		t.Fatalf("first status=%d body=%s", got.Code, got.Body.String())
	}
	if got := call("fixture-ai"); got.Code != 200 || compute.calls != 1 {
		t.Fatalf("replay status=%d calls=%d", got.Code, compute.calls)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.articles SET processing_attempts=2 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if got := call("fixture-ai"); got.Code != 409 || compute.calls != 1 {
		t.Fatalf("stale replay status=%d calls=%d", got.Code, compute.calls)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.articles SET processing_attempts=1 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	h.Admission = NewAdmission(16, 64, 1024*1024)
	compute.during = func() {
		if _, err := admin.Exec(ctx, `UPDATE `+q+`.articles SET processing_attempts=2 WHERE id=7`); err != nil {
			t.Fatal(err)
		}
	}
	if got := call("fixture-ai"); got.Code != 409 || compute.calls != 2 {
		t.Fatalf("changed during compute status=%d calls=%d", got.Code, compute.calls)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.articles SET processing_attempts=1 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"summary\":\"ok\",\"categories\":[\"Business\"],\"is_advertisement\":false}"}]}}]}`))
	}))
	defer provider.Close()
	runtime := airuntime.NewServer(airuntime.Config{APIKey: "fixture", Endpoint: provider.URL + "/v1/models", Model: "fixture-model", RPS: 100}, nil)
	h.Compute = &ArticleCompute{Runtime: runtime, MaxTextChars: 100000}
	h.Admission = NewAdmission(16, 64, 1024*1024)
	input := map[string]any{"title": "Synthetic title", "content": strings.Repeat("synthetic content ", 6), "url": "https://example.test/story"}
	fingerprint, err = analysisFingerprint(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `UPDATE `+q+`.article_processing_items SET input_fingerprint=$1`, fingerprint); err != nil {
		t.Fatal(err)
	}
	inputJSON, _ := json.Marshal(input)
	request.Operation = "analyze"
	request.InputFingerprint = fingerprint
	request.Input = inputJSON
	wire, _ = json.Marshal(request)
	if got := call("fixture-ai"); got.Code != 200 || providerCalls.Load() != 1 || !strings.Contains(got.Body.String(), `"summary":"ok"`) {
		t.Fatalf("analysis status=%d provider calls=%d body=%s", got.Code, providerCalls.Load(), got.Body.String())
	}
}

type fixtureCompute struct {
	calls  int
	during func()
}

func (*fixtureCompute) Validate(string, json.RawMessage) error { return nil }
func (f *fixtureCompute) Run(context.Context, Request, authority.Fence) (any, error) {
	f.calls++
	if f.during != nil {
		f.during()
	}
	return map[string]any{"selector": nil, "method": nil}, nil
}
