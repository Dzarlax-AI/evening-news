package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCloseBoundsCancellationResistantPipelineAndFencesLateResult(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL execution tests")
	}
	if !isLoopbackPostgresTestDSN(dsn) {
		t.Fatal("execution tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer closeConn(admin)
	schema := fmt.Sprintf("go_execution_close_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("create isolated schema: %v", err)
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
			t.Fatalf("create fixture table: %v", err)
		}
	}
	owner, job := uuid.New(), uuid.New()
	worker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn(worker)
	if _, err := worker.Exec(ctx, `SET search_path TO `+quoted); err != nil {
		t.Fatal(err)
	}
	var pid int32
	var acquired bool
	if err = worker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &pid); err != nil || !acquired {
		t.Fatalf("acquire worker lease: acquired=%v err=%v", acquired, err)
	}
	if _, err = admin.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,7,$1,$2,clock_timestamp() AT TIME ZONE 'UTC','running')`, owner, pid); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','running',7,1)`, job); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	pipe := PipelineFunc(func(context.Context, ExecutionContext, string, json.RawMessage) (Result, error) {
		close(started)
		<-release // Deliberately ignores cancellation until the test releases it.
		return Result{Value: json.RawMessage(`{"status":"success","articles_processed":99}`)}, nil
	})
	srv, err := New(Config{DatabaseURL: dsn, Schema: schema, Token: "fixture-token", QueryTimeout: time.Second, GuardInterval: 20 * time.Millisecond}, Handlers{Pipeline: pipe})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Open(ctx); err != nil {
		t.Fatalf("open execution server: %v", err)
	}
	deadline := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	body := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":7,"attempt_generation":1,"deadline_at":%q}`, job.String(), owner.String(), deadline)
	req := httptest.NewRequest("POST", "/internal/v1/job-executions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer fixture-token")
	accepted := httptest.NewRecorder()
	srv.Handler().ServeHTTP(accepted, req)
	if accepted.Code != 202 {
		t.Fatalf("accept status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not start")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	before := time.Now()
	closeErr := srv.Close(closeCtx)
	closeCancel()
	if closeErr == nil || time.Since(before) > 500*time.Millisecond {
		t.Fatalf("Close did not honor its deadline: err=%v elapsed=%s", closeErr, time.Since(before))
	}
	unblock()
	workDone := make(chan struct{})
	go func() {
		srv.wg.Wait()
		close(workDone)
	}()
	deadlinePoll := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlinePoll) {
		var receiptState string
		var receiptResult *string
		var jobState string
		err := admin.QueryRow(ctx, `SELECT e.state,e.result::text,j.state FROM `+quoted+`.background_job_executions e JOIN `+quoted+`.background_jobs j ON j.id=e.job_id WHERE e.job_id=$1 AND e.attempt_generation=1`, job).Scan(&receiptState, &receiptResult, &jobState)
		if err == nil && (receiptState == "succeeded" || receiptResult != nil || jobState != "running") {
			t.Fatalf("late result crossed released guard: receipt=%s result=%v job=%s", receiptState, receiptResult, jobState)
		}
		if err == nil {
			select {
			case <-workDone:
			case <-time.After(time.Second):
				t.Fatal("late pipeline did not exit after release")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("receipt disappeared while checking late result")
}

func TestExpiredDeadlineBeforeRunningWritesTerminalReceiptWithoutPipeline(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL execution tests")
	}
	if !isLoopbackPostgresTestDSN(dsn) {
		t.Fatal("execution tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer closeConn(admin)
	schema := fmt.Sprintf("go_execution_deadline_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+quoted+` CASCADE`) }()
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	pc.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, ddl := range []string{
		`CREATE TABLE background_worker_status(id integer PRIMARY KEY,generation integer NOT NULL,owner_id uuid,lock_backend_pid integer,heartbeat_at timestamp,status text NOT NULL)`,
		`CREATE TABLE background_jobs(id uuid PRIMARY KEY,kind text NOT NULL,payload jsonb NOT NULL,result jsonb,state text NOT NULL,owner_generation integer,attempt_generation integer NOT NULL,initialized_at timestamp)`,
		`CREATE TABLE background_job_executions(job_id uuid NOT NULL REFERENCES background_jobs(id),attempt_generation integer NOT NULL,envelope_sha256 text NOT NULL,executor_instance_id uuid NOT NULL,owner_id uuid,worker_generation integer,deadline_at timestamp,state text NOT NULL,accepted_at timestamp NOT NULL DEFAULT clock_timestamp(),started_at timestamp,finished_at timestamp,result jsonb,error_code text,legacy_completion_applied_at timestamp,PRIMARY KEY(job_id,attempt_generation))`,
		`CREATE TABLE articles(id bigint PRIMARY KEY,processing_state text,processing_attempts integer,next_attempt_at timestamp,last_processing_error text,processed boolean,summary_processed boolean,category_processed boolean,ad_processed boolean)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("create fixture table: %v", err)
		}
	}
	owner, job := uuid.New(), uuid.New()
	worker, err := pgx.ConnectConfig(ctx, pc.ConnConfig.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn(worker)
	var pid int32
	var acquired bool
	if err = worker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &pid); err != nil || !acquired {
		t.Fatalf("acquire worker lease: acquired=%v err=%v", acquired, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,3,$1,$2,clock_timestamp() AT TIME ZONE 'UTC','running')`, owner, pid); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','running',3,1)`, job); err != nil {
		t.Fatal(err)
	}
	var pipelineCalls atomic.Int32
	srv, err := New(Config{DatabaseURL: dsn, Schema: schema, Token: "fixture-token", QueryTimeout: time.Second}, Handlers{Pipeline: PipelineFunc(func(context.Context, ExecutionContext, string, json.RawMessage) (Result, error) {
		pipelineCalls.Add(1)
		return Result{Value: json.RawMessage(`{"status":"success"}`)}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	srv.beforeSetRunning = func() { close(entered); <-release }
	defer func() { unblock(); _ = srv.Close(context.Background()) }()
	deadline := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	body := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":3,"attempt_generation":1,"deadline_at":%q}`, job.String(), owner.String(), deadline)
	req := httptest.NewRequest("POST", "/internal/v1/job-executions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer fixture-token")
	accepted := httptest.NewRecorder()
	srv.Handler().ServeHTTP(accepted, req)
	if accepted.Code != 202 {
		t.Fatalf("accept status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("attempt did not reach the deterministic start barrier")
	}
	if _, err := pool.Exec(ctx, `UPDATE background_job_executions SET deadline_at=clock_timestamp() AT TIME ZONE 'UTC' - interval '1 second' WHERE job_id=$1 AND attempt_generation=1`, job); err != nil {
		t.Fatal(err)
	}
	unblock()
	deadlinePoll := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlinePoll) {
		var state, code string
		err := pool.QueryRow(ctx, `SELECT state,error_code FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&state, &code)
		if err == nil && state == "failed" && code == "worker_error" {
			if got := pipelineCalls.Load(); got != 0 {
				t.Fatalf("pipeline started after database deadline expiration: calls=%d", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expired accepted deadline did not produce a fenced terminal receipt")
}

func TestWorkerGenerationTakeoverSerializesFencedWrite(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL execution tests")
	}
	if !isLoopbackPostgresTestDSN(dsn) {
		t.Fatal("execution tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer closeConn(admin)
	schema := fmt.Sprintf("go_execution_takeover_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("create isolated schema: %v", err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+quoted+` CASCADE`) }()
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	pc.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, ddl := range []string{
		`CREATE TABLE background_worker_status(id integer PRIMARY KEY,generation integer NOT NULL,owner_id uuid,lock_backend_pid integer,heartbeat_at timestamp,status text NOT NULL)`,
		`CREATE TABLE background_jobs(id uuid PRIMARY KEY,kind text NOT NULL,payload jsonb NOT NULL,result jsonb,state text NOT NULL,owner_generation integer,attempt_generation integer NOT NULL,initialized_at timestamp)`,
		`CREATE TABLE background_job_executions(job_id uuid NOT NULL REFERENCES background_jobs(id),attempt_generation integer NOT NULL,envelope_sha256 text NOT NULL,executor_instance_id uuid NOT NULL,owner_id uuid,worker_generation integer,deadline_at timestamp,state text NOT NULL,accepted_at timestamp NOT NULL DEFAULT clock_timestamp(),started_at timestamp,finished_at timestamp,result jsonb,error_code text,legacy_completion_applied_at timestamp,PRIMARY KEY(job_id,attempt_generation))`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("create isolated schema table: %v", err)
		}
	}
	oldOwner, newOwner, job := uuid.New(), uuid.New(), uuid.New()
	worker, err := pgx.ConnectConfig(ctx, pc.ConnConfig.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn(worker)
	var oldPID int32
	var acquired bool
	if err := worker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &oldPID); err != nil || !acquired {
		t.Fatalf("acquire old worker lease: acquired=%v err=%v", acquired, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,4,$1,$2,clock_timestamp() AT TIME ZONE 'UTC','running')`, oldOwner, oldPID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','running',4,1)`, job); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{DatabaseURL: dsn, Schema: schema, Token: "fixture-token", QueryTimeout: time.Second}, Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Open(ctx); err != nil {
		t.Fatalf("open execution server: %v", err)
	}
	defer func() {
		closeCtx, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_ = srv.Close(closeCtx)
	}()
	deadline := time.Now().UTC().Add(time.Minute)
	if _, err := pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,repeat('a',64),$2,$3,4,$4,'running')`, job, srv.instance, oldOwner, deadline); err != nil {
		t.Fatal(err)
	}
	e := Envelope{JobID: job, Attempt: 1, OwnerID: oldOwner, Generation: 4, Deadline: deadline}
	x, err := srv.executionContext(ctx, attemptKey{job, 1}, e)
	if err != nil {
		t.Fatalf("build old attempt context: %v", err)
	}
	oldTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.fenceTx(ctx, oldTx, attemptKey{job, 1}, x, true); err != nil {
		_ = oldTx.Rollback(context.Background())
		t.Fatalf("old transaction did not acquire authority row: %v", err)
	}
	var unlocked bool
	if err := worker.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()))`).Scan(&unlocked); err != nil || !unlocked {
		_ = oldTx.Rollback(context.Background())
		t.Fatalf("release old worker advisory lease: unlocked=%v err=%v", unlocked, err)
	}
	type takeoverResult struct {
		pid int32
		err error
	}
	started := make(chan takeoverResult, 1)
	done := make(chan error, 1)
	go func() {
		conn, openErr := pgx.ConnectConfig(ctx, pc.ConnConfig.Copy())
		if openErr != nil {
			started <- takeoverResult{err: openErr}
			done <- openErr
			return
		}
		defer closeConn(conn)
		var newPID int32
		var gotLock bool
		if openErr = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&gotLock, &newPID); openErr != nil || !gotLock {
			if openErr == nil {
				openErr = errors.New("replacement worker advisory lease unavailable")
			}
			started <- takeoverResult{err: openErr}
			done <- openErr
			return
		}
		started <- takeoverResult{pid: newPID}
		tx, txErr := conn.Begin(ctx)
		if txErr == nil {
			_, txErr = tx.Exec(ctx, `UPDATE background_worker_status SET owner_id=$1,generation=5,lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',status='running' WHERE id=1`, newOwner, newPID)
		}
		if txErr == nil {
			txErr = tx.Commit(ctx)
		} else if tx != nil {
			_ = tx.Rollback(context.Background())
		}
		done <- txErr
	}()
	replacement := <-started
	if replacement.err != nil {
		_ = oldTx.Rollback(context.Background())
		t.Fatalf("replacement worker setup: %v", replacement.err)
	}
	waitDeadline := time.Now().Add(2 * time.Second)
	waiting := false
	for time.Now().Before(waitDeadline) {
		var waitType *string
		err := admin.QueryRow(ctx, `SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1`, replacement.pid).Scan(&waitType)
		if err == nil && waitType != nil && *waitType == "Lock" {
			waiting = true
			break
		}
		select {
		case err := <-done:
			_ = oldTx.Rollback(context.Background())
			t.Fatalf("takeover committed before old fenced transaction released: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		_ = oldTx.Rollback(context.Background())
		t.Fatal("replacement worker did not wait on old authority row")
	}
	if err := oldTx.Commit(ctx); err != nil {
		t.Fatalf("commit old fenced transaction: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("replacement worker commit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement worker did not commit after old transaction released the row")
	}
	if err := srv.fencedUpdate(ctx, attemptKey{job, 1}, x, `UPDATE background_job_executions SET state='failed',error_code='worker_error' WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$3 AND state='running'`); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale old-generation write error=%v want ErrLeaseLost", err)
	}
	var state string
	var result *string
	if err := pool.QueryRow(ctx, `SELECT state,result::text FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&state, &result); err != nil {
		t.Fatal(err)
	}
	if state != "running" || result != nil {
		t.Fatalf("stale write committed after new authority: state=%s result=%v", state, result)
	}
}
