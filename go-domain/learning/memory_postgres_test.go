package learning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/callbacks"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPatternLoadPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("loopback database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("learn_patterns_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
	_, err = admin.Exec(ctx, `CREATE TABLE `+q+`.extraction_patterns(domain text,selector_pattern text,extraction_strategy text,success_count integer,failure_count integer,quality_score_avg double precision,content_length_avg integer,discovered_by text,consecutive_successes integer,consecutive_failures integer,last_success_at timestamp)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.Exec(ctx, `INSERT INTO `+q+`.extraction_patterns VALUES ('example.test','main','readability',4,1,0.8,100,'manual',2,0,now()),('example.test','bad','readability',0,6,0,0,'manual',0,6,NULL),('example.test','new','readability',0,0,0.95,0,'ai',0,0,NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	m := New(pool)
	wire, _ := json.Marshal(map[string]string{"url": "https://example.test/story"})
	r := callbacks.Request{RunID: "run", ArticleID: 1, ArticleAttempt: 1, InputFingerprint: "fp", Input: wire}
	p, err := m.Context(ctx, r, authority.Fence{})
	if err != nil || p.Selector == nil || *p.Selector != "main" {
		t.Fatalf("pattern=%v err=%v", p, err)
	}
	if len(m.patterns["example.test"]) != 2 {
		t.Fatalf("eligible count=%d", len(m.patterns["example.test"]))
	}
}

func TestFirstAndConcurrentFeedbackCountsOnce(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, e := url.Parse(dsn)
	if e != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("loopback database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("learn_feedback_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+q); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
	for _, s := range []string{
		`CREATE TABLE ` + q + `.background_worker_status(id integer primary key,owner_id uuid,generation integer,status text,lock_backend_pid integer,heartbeat_at timestamp)`,
		`CREATE TABLE ` + q + `.background_jobs(id uuid primary key,state text,owner_generation integer,attempt_generation integer,kind text,payload jsonb)`,
		`CREATE TABLE ` + q + `.background_job_executions(job_id uuid,attempt_generation integer,executor_instance_id uuid,owner_id uuid,worker_generation integer,deadline_at timestamp,state text)`,
		`CREATE TABLE ` + q + `.article_processing_runs(id uuid,job_id uuid,attempt_generation integer,envelope_sha256 text,state text)`,
		`CREATE TABLE ` + q + `.article_processing_items(run_id uuid,article_id bigint,article_attempt integer,input_fingerprint text,state text)`,
		`CREATE TABLE ` + q + `.articles(id bigint,processing_attempts integer,url text)`,
		`CREATE TABLE ` + q + `.extraction_attempts(article_url text,domain text,extraction_strategy text,selector_used text,success boolean,content_length integer,quality_score double precision,extraction_time_ms integer,error_message text)`,
		`CREATE TABLE ` + q + `.extraction_patterns(domain text,selector_pattern text,extraction_strategy text,success_count integer,failure_count integer,quality_score_avg double precision,content_length_avg integer,discovered_by text,is_stable boolean,consecutive_successes integer,consecutive_failures integer,first_success_at timestamp,last_success_at timestamp,updated_at timestamp,unique(domain,selector_pattern,extraction_strategy))`,
		`CREATE FUNCTION ` + q + `.fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp) RETURNS TABLE(run_id uuid,run_state text) LANGUAGE sql AS $$ SELECT id,state FROM ` + q + `.article_processing_runs WHERE id=$1 AND job_id=$2 AND attempt_generation=$3 AND envelope_sha256=$4 FOR UPDATE $$`,
	} {
		if _, e = admin.Exec(ctx, s); e != nil {
			t.Fatal(e)
		}
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 6
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	worker, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer worker.Release()
	executor, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer executor.Release()
	var workerPID, executorPID int
	if e = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &workerPID); e != nil {
		t.Fatal(e)
	}
	if e = executor.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # $1::integer),pg_backend_pid()`, int32(0x454E4732)).Scan(new(any), &executorPID); e != nil {
		t.Fatal(e)
	}
	const job = "10000000-0000-4000-8000-000000000001"
	const owner = "20000000-0000-4000-8000-000000000002"
	const instance = "30000000-0000-4000-8000-000000000003"
	const run = "40000000-0000-4000-8000-000000000004"
	deadline := time.Now().UTC().Add(3 * time.Minute).Truncate(time.Microsecond)
	env := authority.Envelope{Version: 1, JobID: job, AttemptGeneration: 1, OwnerID: owner, Generation: 1, BackendPID: workerPID, ExecutorInstanceID: instance, DeadlineAt: deadline.Format(time.RFC3339Nano)}
	wire, _ := json.Marshal(env)
	digest := sha256.Sum256(wire)
	for _, s := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO ` + q + `.background_worker_status VALUES(1,$1,1,'running',$2,clock_timestamp() AT TIME ZONE 'UTC')`, []any{owner, workerPID}},
		{`INSERT INTO ` + q + `.background_jobs VALUES($1,'running',1,1,'news_processing','{}')`, []any{job}},
		{`INSERT INTO ` + q + `.background_job_executions VALUES($1,1,$2,$3,1,$4,'running')`, []any{job, instance, owner, deadline}},
		{`INSERT INTO ` + q + `.article_processing_runs VALUES($1,$2,1,$3,'running')`, []any{run, job, hex.EncodeToString(digest[:])}},
		{`INSERT INTO ` + q + `.article_processing_items VALUES($1,7,1,'fingerprint','running')`, []any{run}},
		{`INSERT INTO ` + q + `.articles VALUES(7,1,'https://example.test/story')`, nil},
	} {
		if _, e = admin.Exec(ctx, s.sql, s.args...); e != nil {
			t.Fatal(e)
		}
	}
	f := authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "news_processing", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: workerPID, ExecutorBackendPID: executorPID, Deadline: deadline, Payload: []byte(`{}`)}
	m := New(pool)
	m.stability = newStability(filepath.Join(t.TempDir(), "domain_stats.json"))
	request := callbacks.Request{RunID: run, Envelope: env, ArticleID: 7, ArticleAttempt: 1, InputFingerprint: "fingerprint"}
	selector := "main"
	v := callbacks.Feedback{URL: "https://example.test/story", Method: "readability", Success: true, SelectorUsed: &selector}
	if ok, err := m.Feedback(ctx, request, f, v); err != nil || !ok {
		t.Fatalf("first feedback: %v", err)
	}
	if got := m.patterns["example.test"][0].success; got != 1 {
		t.Fatalf("first feedback cached %d successes", got)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := m.Feedback(ctx, request, f, v); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var dbCount int
	if e = admin.QueryRow(ctx, `SELECT success_count FROM `+q+`.extraction_patterns WHERE domain='example.test' AND selector_pattern='main'`).Scan(&dbCount); e != nil {
		t.Fatal(e)
	}
	if got := m.patterns["example.test"][0].success; got != 3 || dbCount != 3 {
		t.Fatalf("successes cache=%d db=%d", got, dbCount)
	}
	before, err := os.ReadFile(m.stability.path)
	if err != nil {
		t.Fatal(err)
	}
	m.Guard = lostGuard{}
	if _, err = m.Feedback(ctx, request, f, v); !errors.Is(err, authority.ErrStale) {
		t.Fatalf("lost guard: %v", err)
	}
	after, err := os.ReadFile(m.stability.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("stale state published: %v", err)
	}
}

type lostGuard struct{}

func (lostGuard) Check(context.Context) error { return authority.ErrGuardUnavailable }
