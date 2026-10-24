package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) (*Store, context.Context, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL lease tests")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("PostgreSQL lease tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		cancel()
		t.Fatalf("connect test database: %v", err)
	}
	schema := fmt.Sprintf("go_worker_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		closeConn(admin)
		cancel()
		t.Fatalf("create test schema: %v", err)
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	poolConfig.MaxConns = 4
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("create isolated test pool: %v", err)
	}
	ddl := []string{
		`CREATE TABLE background_worker_status (
			id integer PRIMARY KEY, generation bigint NOT NULL DEFAULT 0,
			owner_id uuid, lock_backend_pid integer, heartbeat_at timestamp,
			status text NOT NULL DEFAULT 'stopped')`,
		`INSERT INTO background_worker_status (id) VALUES (1)`,
		`CREATE TABLE background_jobs (
			id uuid PRIMARY KEY, kind text NOT NULL, payload jsonb NOT NULL DEFAULT '{}',
			result jsonb, queue_position bigint NOT NULL, state text NOT NULL,
			owner_generation bigint, attempt_generation bigint NOT NULL DEFAULT 0,
			created_at timestamp NOT NULL DEFAULT clock_timestamp(), started_at timestamp,
			finished_at timestamp, error_code text, schedule_key text, schedule_revision bigint)`,
		`CREATE TABLE background_job_executions (
			job_id uuid NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
			attempt_generation bigint NOT NULL, envelope_sha256 text NOT NULL,
			executor_instance_id uuid NOT NULL, owner_id uuid, worker_generation bigint,
			deadline_at timestamptz, state text NOT NULL, result jsonb, error_code text,
			finished_at timestamp, legacy_completion_applied_at timestamp,
			PRIMARY KEY (job_id, attempt_generation))`,
		`CREATE TABLE scheduler_occurrence_receipts (
			occurrence_id uuid PRIMARY KEY, envelope_sha256 varchar(64) NOT NULL,
			job_id uuid NOT NULL, accepted_at timestamp NOT NULL DEFAULT clock_timestamp())`,
	}
	for _, statement := range ddl {
		if _, err := pool.Exec(ctx, statement); err != nil {
			pool.Close()
			closeConn(admin)
			cancel()
			t.Fatalf("create test schema tables: %v", err)
		}
	}
	store := &Store{Pool: pool}
	cleanup := func() {
		store.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		closeConn(admin)
		cancel()
	}
	return store, ctx, cleanup
}

func insertTestJob(t *testing.T, ctx context.Context, s *Store, state string, attempt int64) string {
	t.Helper()
	var id string
	err := s.Pool.QueryRow(ctx, `INSERT INTO background_jobs
		(id,kind,payload,queue_position,state,attempt_generation)
		VALUES (gen_random_uuid(),'news_processing','{}',1,$1,$2) RETURNING id::text`, state, attempt).Scan(&id)
	if err != nil {
		t.Fatalf("insert test job: %v", err)
	}
	return id
}

func TestLeaseFenceRejectsWritesAfterLeaseBackendDies(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	info, err := s.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, s, "running", 1)
	job := Job{ID: jobID, AttemptGeneration: 1}
	if _, err := s.Pool.Exec(ctx, `UPDATE background_jobs SET owner_generation=$2 WHERE id=$1`, jobID, info.Generation); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := s.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, info.BackendPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate lease backend: terminated=%v err=%v", terminated, err)
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Finish(writeCtx, job, "failed", map[string]any{"status": "incomplete"}, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Finish after lease backend death = %v, want ErrLeaseLost", err)
	}
}

func TestLeaseFenceRejectsStaleGenerationAfterTakeover(t *testing.T) {
	first, ctx, cleanup := testStore(t)
	defer cleanup()
	oldInfo, err := first.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, first, "running", 1)
	job := Job{ID: jobID, AttemptGeneration: 1}
	if _, err := first.Pool.Exec(ctx, `UPDATE background_jobs SET owner_generation=$2 WHERE id=$1`, jobID, oldInfo.Generation); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := first.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, oldInfo.BackendPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate old lease backend: terminated=%v err=%v", terminated, err)
	}
	second := &Store{Pool: first.Pool}
	newInfo, err := second.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire replacement generation: %v", err)
	}
	if newInfo.Generation <= oldInfo.Generation {
		t.Fatalf("replacement generation %d did not advance past %d", newInfo.Generation, oldInfo.Generation)
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := first.Finish(writeCtx, job, "failed", map[string]any{"status": "incomplete"}, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Finish = %v, want ErrLeaseLost", err)
	}
	_ = second.Release(context.Background())
}

func TestAttemptCapAndPendingCompletionRetention(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, s, "interrupted", 3)
	if _, err := s.FailExhaustedInterrupted(ctx, 3, 10); err != nil {
		t.Fatal(err)
	}
	var state, errorCode string
	if err := s.Pool.QueryRow(ctx, `SELECT state,error_code FROM background_jobs WHERE id=$1`, jobID).Scan(&state, &errorCode); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || errorCode != "worker_recovery_exhausted" {
		t.Fatalf("capped interrupted job = %s/%s", state, errorCode)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE background_jobs SET finished_at=clock_timestamp()-interval '2 days' WHERE id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
	var occurrenceID string
	if err := s.Pool.QueryRow(ctx, `INSERT INTO scheduler_occurrence_receipts
		(occurrence_id,envelope_sha256,job_id) VALUES (gen_random_uuid(),repeat('a',64),$1)
		RETURNING occurrence_id::text`, jobID).Scan(&occurrenceID); err != nil {
		t.Fatal(err)
	}
	removed, err := s.Prune(ctx, time.Hour, 10)
	if err != nil || removed != 0 {
		t.Fatalf("prune with pending completion receipt = %d, %v; want 0", removed, err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE background_job_executions SET legacy_completion_applied_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	removed, err = s.Prune(ctx, time.Hour, 10)
	if err != nil || removed != 1 {
		t.Fatalf("prune after completion event = %d, %v; want 1", removed, err)
	}
	var receiptCount int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM scheduler_occurrence_receipts WHERE occurrence_id=$1`, occurrenceID).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("scheduler occurrence receipt count = %d, err=%v; want 1", receiptCount, err)
	}
}

func TestClaimResumesInterruptedAttemptBelowCap(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, s, "interrupted", 1)
	changed, err := s.FailExhaustedInterrupted(ctx, 3, 10)
	if err != nil || changed != 0 {
		t.Fatalf("prematurely exhaust interrupted attempt: changed=%d err=%v", changed, err)
	}
	job, err := s.Claim(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != jobID || job.State != "running" || job.AttemptGeneration != 2 {
		t.Fatalf("resumed job = %#v; want same job running at attempt 2", job)
	}
}
