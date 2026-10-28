package selectors

import (
	"context"
	"errors"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

type publicationGuard struct{}

func (publicationGuard) Check(context.Context) error { return nil }
func TestSourceCachePublicationHoldsFencePostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("loopback fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("selector_publish_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
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
	var wp, ep int
	if err = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &wp); err != nil {
		t.Fatal(err)
	}
	if err = executor.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # $1::integer),pg_backend_pid()`, int32(0x454E4732)).Scan(new(any), &ep); err != nil {
		t.Fatal(err)
	}
	const job = "10000000-0000-4000-8000-000000000001"
	const owner = "20000000-0000-4000-8000-000000000002"
	const instance = "30000000-0000-4000-8000-000000000003"
	deadline := time.Now().UTC().Add(4 * time.Minute).Truncate(time.Microsecond)
	hash := strings.Repeat("a", 64)
	statements := []struct {
		sql  string
		args []any
	}{
		{`UPDATE background_worker_status SET owner_id=$1,generation=1,status='running',lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`, []any{owner, wp}},
		{`INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','fixture','running',1,1)`, []any{job}},
		{`INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, []any{job, hash, instance, owner, deadline}},
		{`INSERT INTO sources(id,name,source_type,url,enabled,config) VALUES(9,'fixture','custom','https://example.test/news',true,'{}')`, nil},
		{`INSERT INTO article_ingestion_runs(id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,backend_pid,executor_instance_id,deadline_at,state) VALUES($1,$2,1,$3,$4,1,$5,$6,$7,'running')`, []any{ingestionRun(job, 1), job, hash, owner, wp, instance, deadline}},
	}
	for _, v := range statements {
		if _, err = pool.Exec(ctx, v.sql, v.args...); err != nil {
			t.Fatal(err)
		}
	}
	var config string
	if err = pool.QueryRow(ctx, `SELECT config::text FROM sources WHERE id=9`).Scan(&config); err != nil {
		t.Fatal(err)
	}
	f := SourceFence{Fence: authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "news_processing", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: wp, ExecutorBackendPID: ep, Deadline: deadline, Payload: []byte(`{}`)}, SourceID: 9, Fingerprint: SourceFingerprint(9, "fixture", "custom", "https://example.test/news", true, config)}
	h := Handler{Pool: pool, Guard: publicationGuard{}, Analyzer: NewAnalyzer(nil)}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- h.withPublicationFence(ctx, f, func() { close(entered); <-release }) }()
	<-entered
	attempt := make(chan error, 1)
	go func() {
		conn, e := pool.Acquire(ctx)
		if e != nil {
			attempt <- e
			return
		}
		defer conn.Release()
		tx, e := conn.Begin(ctx)
		if e != nil {
			attempt <- e
			return
		}
		defer tx.Rollback(context.Background())
		if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='150ms'`); e == nil {
			_, e = tx.Exec(ctx, `UPDATE sources SET url='https://example.test/changed' WHERE id=9`)
		}
		attempt <- e
	}()
	err = <-attempt
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		close(release)
		t.Fatalf("source update was not blocked: %v", err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE sources SET url='https://example.test/changed' WHERE id=9`); err != nil {
		t.Fatal(err)
	}
	analysis := &Analysis{ContentSelectors: []string{"article"}}
	if err = h.publishAnalysis(ctx, f, "https://example.test/news", analysis); !errors.Is(err, authority.ErrStale) {
		t.Fatalf("changed source accepted: %v", err)
	}
	if _, ok := h.Analyzer.get(cacheKey("https://example.test/news", "news")); ok {
		t.Fatal("stale source published cache")
	}
}
