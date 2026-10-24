package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresFencedArticleCommitAndQueuePrivilegeBoundary(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL ingestion tests")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("ingestion PostgreSQL tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect local PostgreSQL: %v", err)
	}
	schema := fmt.Sprintf("ingestion_test_%d", time.Now().UnixNano())
	role := fmt.Sprintf("ingestion_test_role_%d", time.Now().UnixNano())
	qSchema, qRole := pgx.Identifier{schema}.Sanitize(), pgx.Identifier{role}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+qSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE ROLE "+qRole+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if err := setupFenceFixture(ctx, admin, schema, role); err != nil {
		t.Fatalf("setup isolated schema: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+qSchema+" CASCADE")
		_, _ = admin.Exec(context.Background(), "DROP ROLE "+qRole)
		_ = admin.Close(context.Background())
	}()

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.MaxConns, poolCfg.MinConns = 3, 1
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	poolCfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { _, err := c.Exec(ctx, "SET ROLE "+qRole); return err }
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{Pool: pool}
	defer pool.Close()

	lease, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close(context.Background())
	if _, err := lease.Exec(ctx, "SET search_path="+qSchema); err != nil {
		t.Fatal(err)
	}
	var pid int32
	if _, err := lease.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext($1))`, schema); err != nil {
		t.Fatal(err)
	}
	if err := lease.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	jobID, ownerID, executorID := "10000000-0000-4000-8000-000000000001", "20000000-0000-4000-8000-000000000001", "30000000-0000-4000-8000-000000000001"
	deadline := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Microsecond)
	if _, err := lease.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,7,$1,$2,clock_timestamp(),'running')`, ownerID, pid); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Exec(ctx, `INSERT INTO background_jobs(id,kind,state,owner_generation,attempt_generation,payload) VALUES($1,'news_processing','running',7,3,'{}')`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,3,$2,$3,7,$4,'running')`, jobID, executorID, ownerID, deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Exec(ctx, `INSERT INTO sources(id,name,source_type,url,enabled,config) VALUES(42,'Fixture News','rss','https://fixture.invalid/feed',true,'{"a": 1, "b": 2}')`); err != nil {
		t.Fatal(err)
	}
	configText := `{"a": 1, "b": 2}`
	f := Fence{RunID: RunID(jobID, 3), JobID: jobID, AttemptGeneration: 3, EnvelopeSHA256: strings.Repeat("a", 64), OwnerID: ownerID, WorkerGeneration: 7, BackendPID: pid, ExecutorInstance: executorID, DeadlineAt: deadline}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, _, err = s.FenceTx(ctx, tx, f); err != nil {
		t.Fatalf("positive active fence rejected: %v", err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	source := Source{ID: 42, Name: "Fixture News", Type: "rss", URL: "https://fixture.invalid/feed", Config: map[string]any{"a": float64(1), "b": float64(2)}, ConfigText: configText}
	source.Fingerprint = SourceFingerprint(source.ID, source.Name, source.Type, source.URL, true, configText)
	if _, ok, err := s.BeginSource(ctx, f, source); err != nil || !ok {
		t.Fatalf("begin source: ok=%v err=%v", ok, err)
	}
	content := "fixture body"
	counts, err := s.CommitSource(ctx, f, source, []Article{{Title: "Fixture article", URL: "https://fixture.invalid/story", Content: &content, MediaFiles: []byte(`[{"type":"image","url":"https://fixture.invalid/a.jpg","width":640,"contact_data":{"x":1}}]`), SourceType: "rss", SourceName: "Fixture News"}}, "success", "", nil)
	if err != nil || counts.Saved != 1 {
		t.Fatalf("commit article: counts=%+v err=%v", counts, err)
	}
	// A fresh Store after a process restart must reconstruct completed/source
	// and fetched metrics from durable source receipts, not reset them to zero.
	restartCfg := poolCfg.Copy()
	restartPool, err := pgxpool.NewWithConfig(ctx, restartCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restartPool.Close()
	restarted := &Store{Pool: restartPool}
	completed, fetched, _, failedSources, err := restarted.SourceTotals(ctx, f)
	if err != nil || completed != 1 || fetched != 1 || failedSources != 0 {
		t.Fatalf("restart lost committed source metrics: completed=%d fetched=%d failed=%d err=%v", completed, fetched, failedSources, err)
	}
	if _, err := lease.Exec(ctx, `INSERT INTO article_ingestion_source_runs(run_id,source_id,source_type,config_sha256,state,candidates,saved,error_code) VALUES($1,44,'custom',$2,'resource_limit',1,1,'resource_limit'),($1,45,'rss',$2,'failed',0,0,'fetch_error')`, f.RunID, strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	completed, fetched, warnings, failedSources, err := restarted.SourceTotals(ctx, f)
	if err != nil || completed != 3 || fetched != 2 || failedSources != 1 || warnings["resource_limit"] != 1 || warnings["fetch_error"] != 1 {
		t.Fatalf("partial prefix was confused with an empty source failure: completed=%d fetched=%d warnings=%v failed=%d err=%v", completed, fetched, warnings, failedSources, err)
	}
	// Replaying an exact committed source result must be idempotent.
	replayed, err := restarted.CommitSource(ctx, f, source, []Article{{Title: "Fixture article", URL: "https://fixture.invalid/story", Content: &content, SourceType: "rss", SourceName: "Fixture News"}}, "success", "", nil)
	if err != nil || replayed != (CommitCounts{}) {
		t.Fatalf("repeated source commit was not idempotent: counts=%+v err=%v", replayed, err)
	}
	var media string
	if err := lease.QueryRow(ctx, `SELECT media_files::text FROM articles WHERE url='https://fixture.invalid/story'`).Scan(&media); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(media, `"width": 640`) || !strings.Contains(media, "contact_data") {
		t.Fatalf("arbitrary media metadata lost: %s", media)
	}
	if _, err := lease.Exec(ctx, `INSERT INTO sources(id,name,source_type,url,enabled,config) VALUES(43,'Fixture Monitor','custom','https://fixture.invalid/list',true,'{"max_articles_per_check": 7}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Exec(ctx, `INSERT INTO source_collection_state(source_id,config_sha256,scan_limit,cursor_offset,scan_active,previous_hashes,scan_hashes,acknowledged_hashes,snapshot_at,content_sha256) VALUES(43,$1,5,4,true,'{"0":"old"}','{"4":"new"}','{}',NOW(),'old-content')`, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
	custom := Source{ID: 43, Name: "Fixture Monitor", Type: "custom", URL: "https://fixture.invalid/list", Config: map[string]any{"max_articles_per_check": float64(7)}, ConfigText: `{"max_articles_per_check": 7}`}
	custom.Fingerprint = SourceFingerprint(custom.ID, custom.Name, custom.Type, custom.URL, true, custom.ConfigText)
	page, ok, err := s.BeginSource(ctx, f, custom)
	if err != nil || !ok {
		t.Fatalf("begin changed custom source: ok=%v err=%v", ok, err)
	}
	if page.CursorOffset != 0 || page.ScanActive || page.ScanLimit != 7 || len(page.PreviousHashes) != 0 || page.SnapshotAt != nil || page.ContentSHA256 != "" {
		t.Fatalf("source config edit reused old scan state: %+v", page)
	}
	studyHTML := "<html><main>captured fixture</main></html>"
	studyDigest := sha256.Sum256([]byte(studyHTML))
	studyHash := hex.EncodeToString(studyDigest[:])
	if _, err := restarted.CommitSourceWithSelectorStudy(ctx, f, custom, nil, "success", "", &page, studyHash); err != nil {
		t.Fatalf("commit verified Page Monitor selector study: %v", err)
	}
	var persistedStudyHash string
	if err := lease.QueryRow(ctx, `SELECT selector_study_sha256 FROM article_ingestion_source_runs WHERE run_id=$1 AND source_id=$2`, f.RunID, custom.ID).Scan(&persistedStudyHash); err != nil {
		t.Fatal(err)
	}
	if persistedStudyHash != studyHash {
		t.Fatalf("selector study HTML digest not persisted atomically: got=%q want=%q", persistedStudyHash, studyHash)
	}
	if _, err := restarted.CommitSourceWithSelectorStudy(ctx, f, custom, nil, "success", "", &page, "not-a-sha256"); err == nil {
		t.Fatal("invalid selector study digest was accepted")
	}
	// UpdateRun must work through the runtime role's column-scoped UPDATE grant.
	// Use the migration's VARCHAR state columns to exercise PostgreSQL parameter
	// inference for both the progress and terminal CASE branches.
	if err := restarted.UpdateRun(ctx, f, "running", map[string]any{
		"status": "collecting", "articles_fetched": 1,
	}, ""); err != nil {
		t.Fatalf("restricted runtime role could not update run progress: %v", err)
	}
	var runState, runResult string
	var runFinishedAtIsNull bool
	if err := lease.QueryRow(ctx, `SELECT state,result::text,finished_at IS NULL FROM article_ingestion_runs WHERE id=$1`, f.RunID).Scan(&runState, &runResult, &runFinishedAtIsNull); err != nil {
		t.Fatal(err)
	}
	if runState != "running" || !runFinishedAtIsNull || !strings.Contains(runResult, `"articles_fetched": 1`) {
		t.Fatalf("progress update not persisted: state=%q result=%s finished_at_null=%v", runState, runResult, runFinishedAtIsNull)
	}
	if err := restarted.UpdateRun(ctx, f, "succeeded", map[string]any{
		"status": "success", "articles_fetched": 1,
	}, ""); err != nil {
		t.Fatalf("restricted runtime role could not finalize ingestion run: %v", err)
	}
	if err := lease.QueryRow(ctx, `SELECT state,result::text,finished_at IS NULL FROM article_ingestion_runs WHERE id=$1`, f.RunID).Scan(&runState, &runResult, &runFinishedAtIsNull); err != nil {
		t.Fatal(err)
	}
	if runState != "succeeded" || runFinishedAtIsNull || !strings.Contains(runResult, `"articles_fetched": 1`) {
		t.Fatalf("terminal run state/result not persisted: state=%q result=%s finished_at_null=%v", runState, runResult, runFinishedAtIsNull)
	}
	newFencedRun := func(jobID, executorID string, attempt int32) Fence {
		if _, err := lease.Exec(ctx, `INSERT INTO background_jobs(id,kind,state,owner_generation,attempt_generation,payload) VALUES($1,'news_processing','running',7,$2,'{}')`, jobID, attempt); err != nil {
			t.Fatalf("insert fenced test job: %v", err)
		}
		if _, err := lease.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,$2,$3,$4,7,$5,'running')`, jobID, attempt, executorID, ownerID, deadline); err != nil {
			t.Fatalf("insert fenced test execution: %v", err)
		}
		next := Fence{RunID: RunID(jobID, attempt), JobID: jobID, AttemptGeneration: attempt, EnvelopeSHA256: strings.Repeat("d", 64), OwnerID: ownerID, WorkerGeneration: 7, BackendPID: pid, ExecutorInstance: executorID, DeadlineAt: deadline}
		fenceTx, err := s.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin fresh fenced test run: %v", err)
		}
		if _, _, err := s.FenceTx(ctx, fenceTx, next); err != nil {
			_ = fenceTx.Rollback(ctx)
			t.Fatalf("create fresh fenced test run: %v", err)
		}
		if err := fenceTx.Commit(ctx); err != nil {
			t.Fatalf("commit fresh fenced test run: %v", err)
		}
		return next
	}
	cancelFence := newFencedRun("10000000-0000-4000-8000-000000000002", "30000000-0000-4000-8000-000000000002", 4)
	if _, err := lease.Exec(ctx, `UPDATE article_ingestion_runs SET state='cancelled' WHERE id=$1`, cancelFence.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.CommitSource(ctx, cancelFence, source, []Article{{Title: "Late article", URL: "https://fixture.invalid/late", Content: &content, SourceType: "rss", SourceName: "Fixture News"}}, "success", "", nil); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("cancelled run accepted late article commit: %v", err)
	}
	var articleCount int
	if err := lease.QueryRow(ctx, `SELECT COUNT(*) FROM articles`).Scan(&articleCount); err != nil || articleCount != 1 {
		t.Fatalf("late cancelled commit changed articles: count=%d err=%v", articleCount, err)
	}
	failedFence := newFencedRun("10000000-0000-4000-8000-000000000003", "30000000-0000-4000-8000-000000000003", 5)
	if err := restarted.UpdateRun(ctx, failedFence, "failed", map[string]any{"status": "error", "articles_fetched": 0}, "fixture_failure"); err != nil {
		t.Fatalf("restricted runtime role could not finalize failed ingestion run: %v", err)
	}
	var failedState, failedErrorCode string
	var failedFinishedAtIsNull bool
	if err := lease.QueryRow(ctx, `SELECT state,error_code,finished_at IS NULL FROM article_ingestion_runs WHERE id=$1`, failedFence.RunID).Scan(&failedState, &failedErrorCode, &failedFinishedAtIsNull); err != nil {
		t.Fatal(err)
	}
	if failedState != "failed" || failedErrorCode != "fixture_failure" || failedFinishedAtIsNull {
		t.Fatalf("failed run state not persisted: state=%q error_code=%q finished_at_null=%v", failedState, failedErrorCode, failedFinishedAtIsNull)
	}
	_, err = s.Pool.Exec(ctx, `UPDATE background_jobs SET state='failed' WHERE id=$1`, jobID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("runtime role unexpectedly mutated queue state: %v", err)
	}
	_, err = s.Pool.Exec(ctx, `UPDATE background_worker_status SET status='stopped' WHERE id=1`)
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("runtime role unexpectedly mutated worker owner: %v", err)
	}
}

func TestPostgresRunLockDoesNotConsumePoolConnection(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL ingestion tests")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("ingestion PostgreSQL tests require a loopback-only TEST_DATABASE_URL")
	}
	for _, maxConns := range []int32{1, 2} {
		t.Run(fmt.Sprintf("max_conns_%d", maxConns), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s, err := Open(ctx, dsn, maxConns)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer s.Close()

			release, acquired, err := s.AcquireRunLock(ctx, "fixture-single-pool-"+fmt.Sprint(maxConns))
			if err != nil || !acquired {
				t.Fatalf("acquire run lock: acquired=%v err=%v", acquired, err)
			}
			defer release()

			// The lock must use a dedicated session connection, leaving the
			// entire configured pool available for source reads and commits.
			var one int
			if err := s.Pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("pool query while run lock held: value=%d err=%v", one, err)
			}
			_, duplicate, err := s.AcquireRunLock(ctx, "fixture-single-pool-"+fmt.Sprint(maxConns))
			if err != nil || duplicate {
				t.Fatalf("duplicate process acquired run lock: acquired=%v err=%v", duplicate, err)
			}
		})
	}
}

func setupFenceFixture(ctx context.Context, admin *pgx.Conn, schema, role string) error {
	q := pgx.Identifier{schema}.Sanitize()
	roleQ := pgx.Identifier{role}.Sanitize()
	statements := []string{
		`CREATE TABLE ` + q + `.background_worker_status(id integer primary key,generation integer,owner_id uuid,lock_backend_pid integer,heartbeat_at timestamp,status text)`,
		`CREATE TABLE ` + q + `.background_jobs(id uuid primary key,kind text,state text,owner_generation integer,attempt_generation integer,payload jsonb)`,
		`CREATE TABLE ` + q + `.background_job_executions(job_id uuid,attempt_generation integer,executor_instance_id uuid,owner_id uuid,worker_generation integer,deadline_at timestamp,state text,primary key(job_id,attempt_generation))`,
		`CREATE TABLE ` + q + `.sources(id integer primary key,name text,source_type text,url text,enabled boolean,config jsonb,last_fetch timestamp)`,
		`CREATE TABLE ` + q + `.articles(id bigserial primary key,source_id integer,title text,url text unique,content text,summary text,image_url text,media_files jsonb,published_at timestamp,fetched_at timestamp not null,processed boolean not null,summary_processed boolean not null,category_processed boolean not null,processing_state text not null,processing_attempts integer not null,next_attempt_at timestamp,last_processing_error text,hash_content text,is_advertisement boolean not null,ad_confidence double precision not null,ad_type text,ad_reasoning text,ad_markers jsonb not null,ad_processed boolean not null)`,
		`CREATE TABLE ` + q + `.article_ingestion_runs(id uuid primary key,job_id uuid not null,attempt_generation integer not null,envelope_sha256 text not null,owner_id uuid not null,worker_generation integer not null,backend_pid integer not null,executor_instance_id uuid not null,deadline_at timestamp not null,state varchar(20) not null default 'running',started_at timestamp not null default current_timestamp,updated_at timestamp not null default current_timestamp,finished_at timestamp,result jsonb not null default '{}',error_code varchar(40),unique(job_id,attempt_generation))`,
		`CREATE TABLE ` + q + `.article_ingestion_source_runs(run_id uuid,source_id integer,source_type text,config_sha256 text,state varchar(20),candidates integer default 0,saved integer default 0,updated integer default 0,skipped integer default 0,fetched_at timestamp,error_code varchar(40),selector_study_sha256 varchar(64),primary key(run_id,source_id))`,
		`CREATE TABLE ` + q + `.source_collection_state(source_id integer primary key,config_sha256 text,scan_limit integer default 20,cursor_offset integer default 0,scan_active boolean default false,previous_hashes jsonb default '{}',scan_hashes jsonb default '{}',acknowledged_hashes jsonb default '{}',snapshot_at timestamp,content_sha256 text,updated_at timestamp default current_timestamp)`,
	}
	for _, sql := range statements {
		if _, err := admin.Exec(ctx, sql); err != nil {
			return err
		}
	}
	schemaLiteral := "'" + strings.ReplaceAll(schema, "'", "''") + "'"
	function := `CREATE FUNCTION ` + q + `.fence_article_ingestion_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp) RETURNS TABLE(run_id uuid,run_state text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog, ` + q + ` AS $f$ DECLARE v_now timestamp; v_owner uuid; v_generation integer; v_pid integer; v_heartbeat timestamp; v_status text; v_kind text; v_job_state text; v_job_owner integer; v_attempt integer; v_payload jsonb; v_executor uuid; v_receipt_owner uuid; v_receipt_generation integer; v_deadline timestamp; v_receipt_state text; v_existing ` + q + `.article_ingestion_runs%ROWTYPE; v_held boolean; BEGIN v_now:=clock_timestamp() AT TIME ZONE 'UTC'; SELECT owner_id,generation,lock_backend_pid,heartbeat_at,status INTO v_owner,v_generation,v_pid,v_heartbeat,v_status FROM ` + q + `.background_worker_status WHERE id=1 FOR UPDATE; IF NOT FOUND OR v_status<>'running' OR v_owner IS DISTINCT FROM $5 OR v_generation IS DISTINCT FROM $6 OR v_pid IS DISTINCT FROM $7 OR v_heartbeat IS NULL OR v_heartbeat>v_now OR v_now-v_heartbeat>INTERVAL '20 seconds' THEN RAISE EXCEPTION 'stale' USING ERRCODE='P0001'; END IF; SELECT EXISTS(SELECT 1 FROM pg_locks l WHERE l.locktype='advisory' AND l.granted AND l.pid=$7 AND l.objsubid=2 AND l.classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND l.objid=(((hashtext(` + schemaLiteral + `)::bigint+4294967296)%4294967296)::oid) AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())) INTO v_held; IF NOT v_held THEN RAISE EXCEPTION 'lease' USING ERRCODE='P0001'; END IF; SELECT kind,state,owner_generation,attempt_generation,payload INTO v_kind,v_job_state,v_job_owner,v_attempt,v_payload FROM ` + q + `.background_jobs WHERE id=$2 FOR UPDATE; IF NOT FOUND OR v_job_state<>'running' OR v_job_owner IS DISTINCT FROM $6 OR v_attempt IS DISTINCT FROM $3 OR (v_kind<>'news_processing' AND NOT(v_kind='news_digest' AND COALESCE(v_payload->'run_processing','true'::jsonb)='true'::jsonb)) THEN RAISE EXCEPTION 'job' USING ERRCODE='P0001'; END IF; SELECT * INTO v_existing FROM ` + q + `.article_ingestion_runs WHERE job_id=$2 AND attempt_generation=$3 FOR UPDATE; IF FOUND THEN IF v_existing.id<>$1 OR v_existing.envelope_sha256<>$4 THEN RAISE EXCEPTION 'conflict' USING ERRCODE='P0001'; END IF; ELSE INSERT INTO ` + q + `.article_ingestion_runs(id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,backend_pid,executor_instance_id,deadline_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9); SELECT * INTO v_existing FROM ` + q + `.article_ingestion_runs WHERE id=$1 FOR UPDATE; END IF; SELECT executor_instance_id,owner_id,worker_generation,deadline_at,state INTO v_executor,v_receipt_owner,v_receipt_generation,v_deadline,v_receipt_state FROM ` + q + `.background_job_executions WHERE job_id=$2 AND attempt_generation=$3 FOR UPDATE; v_now:=clock_timestamp() AT TIME ZONE 'UTC'; IF NOT FOUND OR v_executor IS DISTINCT FROM $8 OR v_receipt_owner IS DISTINCT FROM $5 OR v_receipt_generation IS DISTINCT FROM $6 OR v_deadline IS DISTINCT FROM $9 OR v_receipt_state<>'running' OR $9<=v_now OR v_heartbeat IS NULL OR v_now-v_heartbeat>INTERVAL '20 seconds' THEN RAISE EXCEPTION 'receipt' USING ERRCODE='P0001'; END IF; RETURN QUERY SELECT v_existing.id,v_existing.state; END $f$`
	function = strings.Replace(function, "RETURN QUERY SELECT v_existing.id,v_existing.state;", "RETURN QUERY SELECT v_existing.id,v_existing.state::text;", 1)
	if _, err := admin.Exec(ctx, function); err != nil {
		return err
	}
	grants := []string{
		`GRANT USAGE ON SCHEMA ` + q + ` TO ` + roleQ,
		`GRANT SELECT ON ` + q + `.background_worker_status,` + q + `.background_jobs,` + q + `.background_job_executions TO ` + roleQ,
		`GRANT SELECT ON ` + q + `.sources TO ` + roleQ,
		`GRANT UPDATE(last_fetch) ON ` + q + `.sources TO ` + roleQ,
		`GRANT SELECT,INSERT ON ` + q + `.article_ingestion_runs TO ` + roleQ,
		`GRANT UPDATE(state,result,error_code,updated_at,finished_at) ON ` + q + `.article_ingestion_runs TO ` + roleQ,
		`GRANT SELECT,INSERT,UPDATE ON ` + q + `.article_ingestion_source_runs,` + q + `.source_collection_state TO ` + roleQ,
		`GRANT SELECT,INSERT ON ` + q + `.articles TO ` + roleQ,
		`GRANT USAGE,SELECT ON SEQUENCE ` + q + `.articles_id_seq TO ` + roleQ,
		`GRANT UPDATE(title,content,url,image_url,published_at,hash_content) ON ` + q + `.articles TO ` + roleQ,
		`GRANT EXECUTE ON FUNCTION ` + q + `.fence_article_ingestion_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp) TO ` + roleQ,
	}
	for _, sql := range grants {
		if _, err := admin.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}
