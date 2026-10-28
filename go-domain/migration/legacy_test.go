package migration

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The disposable database fixture must not be an application database.
func TestEarlyMigrationPreservesCompletedRowsAndRetryBudget(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	verifyLoopback(t, dsn)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	schema := fmt.Sprintf("migration_test_%d", time.Now().UnixNano())
	_, err = tx.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "SET LOCAL search_path TO "+pgx.Identifier{schema}.Sanitize())
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `CREATE TABLE sources (id integer PRIMARY KEY); CREATE TABLE articles (id integer PRIMARY KEY, fetched_at timestamp, published_at timestamp, source_id integer, is_advertisement boolean, processed boolean, summary_processed boolean, category_processed boolean, ad_processed boolean); CREATE TABLE schedule_settings (id integer); INSERT INTO articles VALUES (1,now(),now(),1,false,true,true,true,true),(2,now(),now(),1,false,false,false,false,false)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range append(earlyMigrations(), backgroundMigration(), schedulerMigration(), executionMigration(), ingestionMigration(), processingMigration(), editorialMigration(), deliveryMigration()) {
		needed, err := m.Needed(ctx, tx)
		if err != nil || !needed {
			t.Fatalf("%s needed=%v: %v", m.ID, needed, err)
		}
		if _, err = m.Apply(ctx, tx); err != nil {
			t.Fatalf("%s apply: %v", m.ID, err)
		}
		needed, err = m.Needed(ctx, tx)
		if err != nil || needed {
			t.Fatalf("%s recheck=%v: %v", m.ID, needed, err)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE articles SET processing_state='retry_wait', processing_attempts=4 WHERE id=2`)
	if err != nil {
		t.Fatal(err)
	}
	m := earlyMigrations()[2]
	if _, err = m.Apply(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var state string
	var attempts int
	err = tx.QueryRow(ctx, `SELECT processing_state,processing_attempts FROM articles WHERE id=2`).Scan(&state, &attempts)
	if err != nil || state != "retry_wait" || attempts != 4 {
		t.Fatalf("state=%s attempts=%d err=%v", state, attempts, err)
	}
	if err = tx.QueryRow(ctx, `SELECT processing_state FROM articles WHERE id=1`).Scan(&state); err != nil || state != "completed" {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

func TestBootstrapAndRerun(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	verifyLoopback(t, dsn)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("migration_full_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
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
	first, err := Run(ctx, pool, true, false)
	if err != nil || len(first.Errors) != 0 || len(first.MigrationsRun) != 7 {
		t.Fatalf("first run: %+v %v", first, err)
	}
	second, err := Run(ctx, pool, true, false)
	if err != nil || len(second.MigrationsRun) != 0 || len(second.Errors) != 0 {
		t.Fatalf("second run: %+v %v", second, err)
	}

	worker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Release()
	var pid int
	err = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &pid)
	if err != nil {
		t.Fatal(err)
	}
	const job = "10000000-0000-4000-8000-000000000001"
	const owner = "20000000-0000-4000-8000-000000000002"
	const instance = "30000000-0000-4000-8000-000000000003"
	const run = "40000000-0000-4000-8000-000000000004"
	hash := strings.Repeat("a", 64)
	deadline := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond)
	_, err = pool.Exec(ctx, `UPDATE background_worker_status SET owner_id=$1,generation=1,lock_backend_pid=$2,status='running',heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`, owner, pid)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','fixture','running',1,1)`, job)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, job, hash, instance, owner, deadline)
	if err != nil {
		t.Fatal(err)
	}
	var gotRun, gotState string
	call := `SELECT run_id,run_state FROM fence_article_processing_attempt($1,$2,1,$3,$4,1,$5,$6,$7)`
	err = pool.QueryRow(ctx, call, run, job, hash, owner, pid, instance, deadline).Scan(&gotRun, &gotState)
	if err != nil || gotRun != run || gotState != "running" {
		t.Fatalf("actual fence: run=%s state=%s err=%v", gotRun, gotState, err)
	}
	var publicExec bool
	err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE n.nspname=current_schema() AND p.proname='fence_article_processing_attempt' AND a.grantee=0 AND a.privilege_type='EXECUTE')`).Scan(&publicExec)
	if err != nil || publicExec {
		t.Fatalf("public execute=%v err=%v", publicExec, err)
	}
	_, err = worker.Exec(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()))`)
	if err != nil {
		t.Fatal(err)
	}
	err = pool.QueryRow(ctx, call, run, job, hash, owner, pid, instance, deadline).Scan(&gotRun, &gotState)
	if err == nil {
		t.Fatal("actual fence accepted lost worker lock")
	}

	var functions int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND p.proname IN ('fence_article_ingestion_attempt','fence_article_processing_attempt','reject_editorial_digest_mutation')`).Scan(&functions)
	if err != nil || functions != 3 {
		t.Fatalf("functions=%d err=%v", functions, err)
	}
}

func verifyLoopback(t *testing.T, dsn string) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("loopback test database required")
	}
}
