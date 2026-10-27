package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func isLoopbackPostgresTestDSN(dsn string) bool {
	config, err := pgx.ParseConfig(dsn)
	if err != nil || config == nil || !isLoopbackPostgresTestHost(config.Host) {
		return false
	}
	for _, fallback := range config.Fallbacks {
		if fallback == nil || !isLoopbackPostgresTestHost(fallback.Host) {
			return false
		}
	}
	return true
}

func isLoopbackPostgresTestHost(host string) bool {
	host = strings.TrimSpace(host)
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func TestLoopbackPostgresTestDSNUsesEffectivePrimaryAndFallbackHosts(t *testing.T) {
	for _, tc := range []struct {
		name, dsn string
		want      bool
	}{
		{"direct IPv4", "postgres://user:pass@127.0.0.1:55446/db", true},
		{"direct IPv6", "postgres://user:pass@[::1]:55446/db", true},
		{"localhost", "postgres://user:pass@localhost:55446/db", true},
		{"loopback query override", "postgres://user:pass@db.internal:55446/db?host=127.0.0.1", true},
		{"remote query override", "postgres://user:pass@127.0.0.1:55446/db?host=db.internal", false},
		{"safe fallback hosts", "postgres://user:pass@127.0.0.1:55446/db?host=127.0.0.1,localhost", true},
		{"remote fallback host", "postgres://user:pass@127.0.0.1:55446/db?host=127.0.0.1,192.0.2.1", false},
		{"remote authority", "postgres://user:pass@192.0.2.1:55446/db", false},
		{"invalid DSN", "not a postgres URL", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLoopbackPostgresTestDSN(tc.dsn); got != tc.want {
				t.Fatalf("isLoopbackPostgresTestDSN(%q) = %v, want %v", tc.dsn, got, tc.want)
			}
		})
	}
}

func TestPostgresAcceptFenceContextCancelAndDuplicate(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL execution tests")
	}
	if !isLoopbackPostgresTestDSN(dsn) {
		t.Fatal("execution tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer closeConn(admin)
	schema := fmt.Sprintf("go_execution_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	}()
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
			t.Fatalf("create isolated schema: %v", err)
		}
	}
	owner, job := uuid.New(), uuid.New()
	workerConnCfg := pc.ConnConfig.Copy()
	worker, err := pgx.ConnectConfig(ctx, workerConnCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn(worker)
	var pid int32
	var acquired bool
	if err = worker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &pid); err != nil || !acquired {
		t.Fatalf("acquire synthetic worker lease: acquired=%v err=%v", acquired, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,7,$1,$2,clock_timestamp() AT TIME ZONE 'UTC','running')`, owner, pid); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','running',7,1)`, job); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	pipelineContexts := make(chan ExecutionContext, 1)
	cancelSeen := make(chan struct{})
	releasePipeline := make(chan struct{})
	var releasePipelineOnce sync.Once
	release := func() { releasePipelineOnce.Do(func() { close(releasePipeline) }) }
	defer release()
	pipe := PipelineFunc(func(c context.Context, x ExecutionContext, kind string, _ json.RawMessage) (Result, error) {
		if kind == "article_reprocess" {
			return Result{Value: json.RawMessage(`{"status":"incomplete","success":false,"article_outcomes":[{"article_id":9,"state":"rejected","reason":"advertisement"}],"article_outcomes_total":1,"article_outcomes_omitted":0}`)}, nil
		}
		pipelineContexts <- x
		close(started)
		<-c.Done()
		close(cancelSeen)
		<-releasePipeline // Simulate a provider that does not stop when canceled.
		return Result{Value: json.RawMessage(`{"processed":99}`)}, nil
	})
	domainReady := false
	srv, err := New(Config{DatabaseURL: dsn, Schema: schema, Token: "fixture-token", QueryTimeout: time.Second, GuardInterval: 25 * time.Millisecond}, Handlers{
		Pipeline: pipe,
		Ready: func(context.Context) error {
			if !domainReady {
				return errors.New("fixture domain unavailable")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = srv.open(ctx); err != nil {
		t.Fatal(err)
	}
	root, stop := context.WithCancel(context.Background())
	srv.mu.Lock()
	srv.root = root
	srv.cancelRoot = stop
	srv.ready = true
	srv.mu.Unlock()
	defer func() { stop(); _ = srv.Close(context.Background()) }()
	deadline := time.Now().Add(2*time.Minute + 987654321*time.Nanosecond).In(time.FixedZone("fixture plus two", 2*60*60)).Format(time.RFC3339Nano)
	body := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":7,"attempt_generation":1,"deadline_at":%q}`, job.String(), owner.String(), deadline)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer fixture-token")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}
	if status := request("GET", "/internal/v1/status", ""); status.Code != 503 {
		t.Fatalf("status while domain is unavailable=%d body=%s", status.Code, status.Body.String())
	}
	if rejected := request("POST", "/internal/v1/job-executions", body); rejected.Code != 503 {
		t.Fatalf("new admission while domain is unavailable=%d body=%s", rejected.Code, rejected.Body.String())
	}
	var receiptCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&receiptCount); err != nil || receiptCount != 0 {
		t.Fatalf("unready domain consumed receipt: count=%d err=%v", receiptCount, err)
	}
	domainReady = true
	if status := request("GET", "/internal/v1/status", ""); status.Code != 200 {
		t.Fatalf("status after domain recovery=%d body=%s", status.Code, status.Body.String())
	}
	accepted := request("POST", "/internal/v1/job-executions", body)
	if accepted.Code != 202 {
		t.Fatalf("accept status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	var firstAcceptance struct {
		AcceptedAt string `json:"accepted_at"`
	}
	if err = json.Unmarshal(accepted.Body.Bytes(), &firstAcceptance); err != nil || firstAcceptance.AcceptedAt == "" {
		t.Fatalf("first acceptance omitted persisted accepted_at: body=%s err=%v", accepted.Body.String(), err)
	}
	duplicateAcceptance := request("POST", "/internal/v1/job-executions", body)
	var duplicateWire struct {
		AcceptedAt string `json:"accepted_at"`
		Duplicate  bool   `json:"duplicate"`
	}
	if duplicateAcceptance.Code != 202 || json.Unmarshal(duplicateAcceptance.Body.Bytes(), &duplicateWire) != nil || !duplicateWire.Duplicate || duplicateWire.AcceptedAt != firstAcceptance.AcceptedAt {
		t.Fatalf("duplicate acceptance timestamp differs: first=%q duplicate=%s", firstAcceptance.AcceptedAt, duplicateAcceptance.Body.String())
	}
	firstReceiptRead := request("GET", fmt.Sprintf("/internal/v1/job-executions/%s/1", job), "")
	var receiptWire struct {
		AcceptedAt string `json:"accepted_at"`
	}
	if firstReceiptRead.Code != 200 || json.Unmarshal(firstReceiptRead.Body.Bytes(), &receiptWire) != nil || receiptWire.AcceptedAt != firstAcceptance.AcceptedAt {
		t.Fatalf("GET receipt timestamp differs: first=%q read=%s", firstAcceptance.AcceptedAt, firstReceiptRead.Body.String())
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not start within bound")
	}
	contextResp := request("GET", fmt.Sprintf("/internal/v1/job-executions/%s/1/context", job), "")
	if contextResp.Code != 200 {
		t.Fatalf("context status=%d body=%s", contextResp.Code, contextResp.Body.String())
	}
	var envelope struct {
		Envelope ExecutionContext `json:"envelope"`
		GuardPID int32            `json:"executor_backend_pid"`
		Kind     string           `json:"kind"`
		Payload  json.RawMessage  `json:"payload"`
	}
	if err = json.Unmarshal(contextResp.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	pipelineContext := <-pipelineContexts
	if pipelineContext.DeadlineAt != envelope.Envelope.DeadlineAt || !strings.HasSuffix(pipelineContext.DeadlineAt, "Z") || strings.Contains(pipelineContext.DeadlineAt, "+02:00") {
		t.Fatalf("pipeline child-fence deadline differs from Go context endpoint: pipeline=%q context=%q", pipelineContext.DeadlineAt, envelope.Envelope.DeadlineAt)
	}
	if envelope.Envelope.BackendPID != pid || envelope.GuardPID <= 0 || envelope.Kind != "news_processing" {
		t.Fatalf("bad expanded context: %+v", envelope)
	}
	takeoverTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer takeoverTx.Rollback(context.Background())
	var workerStatusID int
	if err = takeoverTx.QueryRow(ctx, `SELECT id FROM background_worker_status WHERE id=1 FOR UPDATE`).Scan(&workerStatusID); err != nil {
		t.Fatal(err)
	}
	cancelResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		cancelResult <- request("POST", fmt.Sprintf("/internal/v1/job-executions/%s/1/cancel", job), "")
	}()
	select {
	case <-cancelSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation-resistant pipeline did not observe cancellation")
	}
	var beforeRelease string
	if err = pool.QueryRow(ctx, `SELECT state FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&beforeRelease); err != nil || beforeRelease != "running" {
		t.Fatalf("cancellation-resistant handler committed early: state=%q err=%v", beforeRelease, err)
	}
	// Replace the worker session/identity while the provider ignores cancel.
	if _, err = worker.Exec(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()))`); err != nil {
		t.Fatal(err)
	}
	newOwner := uuid.New()
	newWorker, err := pgx.ConnectConfig(ctx, workerConnCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn(newWorker)
	var newPID int32
	if err = newWorker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &newPID); err != nil || !acquired {
		t.Fatalf("take over synthetic worker lease: acquired=%v err=%v", acquired, err)
	}
	if _, err = takeoverTx.Exec(ctx, `UPDATE background_worker_status SET owner_id=$1,generation=8,lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',status='running' WHERE id=1`, newOwner, newPID); err != nil {
		t.Fatal(err)
	}
	if err = takeoverTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	release()
	var cancelResp *httptest.ResponseRecorder
	select {
	case cancelResp = <-cancelResult:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel request did not complete within bound")
	}
	if cancelResp.Code != 200 {
		t.Fatalf("cancel status=%d body=%s", cancelResp.Code, cancelResp.Body.String())
	}
	var state string
	if err = pool.QueryRow(ctx, `SELECT state FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&state); err != nil || state != "running" {
		t.Fatalf("late terminal receipt escaped worker takeover fence: state=%q err=%v", state, err)
	}
	var jobState string
	if err = pool.QueryRow(ctx, `SELECT state FROM background_jobs WHERE id=$1`, job).Scan(&jobState); err != nil || jobState != "running" {
		t.Fatalf("late fake changed job after takeover: state=%q err=%v", jobState, err)
	}
	dup := request("POST", "/internal/v1/job-executions", body)
	if dup.Code != 202 || !strings.Contains(dup.Body.String(), `"duplicate":true`) {
		t.Fatalf("duplicate response status=%d body=%s", dup.Code, dup.Body.String())
	}
	conflictingDeadline := time.Now().UTC().Add(3 * time.Minute).Format(time.RFC3339Nano)
	conflictBody := strings.Replace(body, deadline, conflictingDeadline, 1)
	conflict := request("POST", "/internal/v1/job-executions", conflictBody)
	if conflict.Code != 409 {
		t.Fatalf("conflict response status=%d body=%s", conflict.Code, conflict.Body.String())
	}

	// Selected article state is initialized once for the durable job, then left
	// intact on a later attempt so completed stages are preserved on retry.
	articleJob := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO articles(id,processing_state,processing_attempts,last_processing_error,processed,summary_processed,category_processed,ad_processed) VALUES(9,'completed',7,'old',true,true,true,true)`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'article_reprocess','{"article_ids":[9]}','running',8,1)`, articleJob); err != nil {
		t.Fatal(err)
	}
	articleDeadline := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	articleEnvelope := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":8,"attempt_generation":1,"deadline_at":%q}`, articleJob.String(), newOwner.String(), articleDeadline)
	if rr := request("POST", "/internal/v1/job-executions", articleEnvelope); rr.Code != 202 {
		t.Fatalf("article accept: %d %s", rr.Code, rr.Body.String())
	}
	waitReceipt := func(attempt int64, want string) {
		t.Helper()
		until := time.Now().Add(2 * time.Second)
		var got string
		for time.Now().Before(until) {
			_ = pool.QueryRow(ctx, `SELECT state FROM background_job_executions WHERE job_id=$1 AND attempt_generation=$2`, articleJob, attempt).Scan(&got)
			if got == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("article receipt attempt %d state=%q want %q", attempt, got, want)
	}
	waitReceipt(1, "succeeded")
	var projectedResult []byte
	if err = pool.QueryRow(ctx, `SELECT result FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, articleJob).Scan(&projectedResult); err != nil {
		t.Fatal(err)
	}
	wantProjected := `{"status":"incomplete","success":false,"article_outcomes":[{"article_id":9,"state":"rejected","reason":"advertisement"}],"article_outcomes_total":1,"article_outcomes_omitted":0}`
	var projected map[string]any
	var want any
	if json.Unmarshal(projectedResult, &projected) != nil || json.Unmarshal([]byte(wantProjected), &want) != nil {
		t.Fatalf("bounded article pipeline result was not valid JSON: %s", projectedResult)
	}
	if duration, ok := projected["duration_seconds"].(float64); !ok || duration < 0 || duration > 86400 {
		t.Fatalf("bounded article pipeline result has invalid duration: %s", projectedResult)
	}
	delete(projected, "duration_seconds")
	if !reflect.DeepEqual(projected, want) {
		t.Fatalf("bounded article pipeline result did not reach the terminal receipt: got=%s", projectedResult)
	}
	var processingState string
	var attempts int
	var processed, summary, category, ad bool
	var initialized *time.Time
	if err = pool.QueryRow(ctx, `SELECT processing_state,processing_attempts,processed,summary_processed,category_processed,ad_processed FROM articles WHERE id=9`).Scan(&processingState, &attempts, &processed, &summary, &category, &ad); err != nil {
		t.Fatal(err)
	}
	if processingState != "pending" || attempts != 0 || processed || summary || category || ad {
		t.Fatalf("article reset mismatch: %s %d %v %v %v %v", processingState, attempts, processed, summary, category, ad)
	}
	if err = pool.QueryRow(ctx, `SELECT initialized_at FROM background_jobs WHERE id=$1`, articleJob).Scan(&initialized); err != nil || initialized == nil {
		t.Fatalf("initialization marker missing: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE articles SET processing_state='completed',processing_attempts=4,processed=true,summary_processed=true,category_processed=true,ad_processed=true WHERE id=9`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE background_jobs SET attempt_generation=2,state='running' WHERE id=$1`, articleJob); err != nil {
		t.Fatal(err)
	}
	articleDeadline2 := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	articleEnvelope2 := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":8,"attempt_generation":2,"deadline_at":%q}`, articleJob.String(), newOwner.String(), articleDeadline2)
	if rr := request("POST", "/internal/v1/job-executions", articleEnvelope2); rr.Code != 202 {
		t.Fatalf("article retry accept: %d %s", rr.Code, rr.Body.String())
	}
	waitReceipt(2, "succeeded")
	if err = pool.QueryRow(ctx, `SELECT processing_state,processing_attempts,processed,summary_processed,category_processed,ad_processed FROM articles WHERE id=9`).Scan(&processingState, &attempts, &processed, &summary, &category, &ad); err != nil {
		t.Fatal(err)
	}
	if processingState != "completed" || attempts != 4 || !processed || !summary || !category || !ad {
		t.Fatalf("retry reset already initialized article: %s %d %v %v %v %v", processingState, attempts, processed, summary, category, ad)
	}

	// The context endpoint must preserve Python's normalized digest payload
	// verbatim. Execution defaults are applied by the pipeline, not by payload
	// normalization, so omitted and partial flags stay omitted in this contract.
	for i, tc := range []struct {
		payload  string
		expected string
	}{
		{payload: `{}`, expected: `{}`},
		{payload: `{"run_processing":false}`, expected: `{"run_processing":false}`},
		{payload: `{"send_telegram":false}`, expected: `{"send_telegram":false}`},
	} {
		digestJob := uuid.New()
		if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_digest',$2::jsonb,'running',8,1)`, digestJob, tc.payload); err != nil {
			t.Fatal(err)
		}
		startedDigest, releaseDigest := make(chan struct{}), make(chan struct{})
		var releaseDigestOnce sync.Once
		releaseDigestPipeline := func() { releaseDigestOnce.Do(func() { close(releaseDigest) }) }
		defer releaseDigestPipeline()
		srv.SetHandlers(Handlers{Pipeline: PipelineFunc(func(c context.Context, _ ExecutionContext, kind string, _ json.RawMessage) (Result, error) {
			if kind != "news_digest" {
				return Result{}, errors.New("unexpected fixture pipeline kind")
			}
			close(startedDigest)
			<-releaseDigest
			return Result{Value: json.RawMessage(`{"status":"success","parts_sent":0}`)}, nil
		})})
		digestDeadline := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
		digestEnvelope := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":8,"attempt_generation":1,"deadline_at":%q}`, digestJob.String(), newOwner.String(), digestDeadline)
		if rr := request("POST", "/internal/v1/job-executions", digestEnvelope); rr.Code != 202 {
			t.Fatalf("digest accept case %d: %d %s", i, rr.Code, rr.Body.String())
		}
		select {
		case <-startedDigest:
		case <-time.After(5 * time.Second):
			t.Fatalf("digest pipeline did not start for case %d", i)
		}
		contextResp := request("GET", fmt.Sprintf("/internal/v1/job-executions/%s/1/context", digestJob), "")
		if contextResp.Code != 200 {
			t.Fatalf("digest context case %d: %d %s", i, contextResp.Code, contextResp.Body.String())
		}
		var contextBody struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}
		var gotPayload, wantPayload any
		if json.Unmarshal(contextResp.Body.Bytes(), &contextBody) != nil || contextBody.Kind != "news_digest" ||
			json.Unmarshal(contextBody.Payload, &gotPayload) != nil || json.Unmarshal([]byte(tc.expected), &wantPayload) != nil ||
			!reflect.DeepEqual(gotPayload, wantPayload) {
			t.Fatalf("digest context payload case %d got=%s want=%s", i, contextBody.Payload, tc.expected)
		}
		releaseDigestPipeline()
		until := time.Now().Add(2 * time.Second)
		var digestState string
		for time.Now().Before(until) {
			_ = pool.QueryRow(ctx, `SELECT state FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, digestJob).Scan(&digestState)
			if digestState == "succeeded" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if digestState != "succeeded" {
			t.Fatalf("digest receipt case %d state=%q", i, digestState)
		}
	}

	// Late terminal writes are fenced after the worker lease becomes invalid.
	if _, err = pool.Exec(ctx, `UPDATE background_job_executions SET state='running',result=NULL,error_code=NULL,finished_at=NULL WHERE job_id=$1 AND attempt_generation=2`, articleJob); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE background_worker_status SET status='stopped' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	srv.finish(context.Background(), attemptKey{articleJob, 2}, "succeeded", "", json.RawMessage(`{"processed":77}`))
	var fencedState string
	var fencedResult []byte
	if err = pool.QueryRow(ctx, `SELECT state,result FROM background_job_executions WHERE job_id=$1 AND attempt_generation=2`, articleJob).Scan(&fencedState, &fencedResult); err != nil {
		t.Fatal(err)
	}
	if fencedState != "running" || fencedResult != nil {
		t.Fatalf("late result escaped lease fence: state=%s result=%s", fencedState, fencedResult)
	}
	if _, err = pool.Exec(ctx, `UPDATE background_worker_status SET status='running',heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`); err != nil {
		t.Fatal(err)
	}

	// Losing the executor's own session advisory guard revokes the context and
	// fences a cancellation-resistant handler's late terminal write.
	guardJob := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','running',8,1)`, guardJob); err != nil {
		t.Fatal(err)
	}
	guardStarted, guardCanceled, releaseLate := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseLateOnce sync.Once
	defer releaseLateOnce.Do(func() { close(releaseLate) })
	srv.SetHandlers(Handlers{Pipeline: PipelineFunc(func(c context.Context, _ ExecutionContext, _ string, _ json.RawMessage) (Result, error) {
		close(guardStarted)
		<-c.Done()
		close(guardCanceled)
		<-releaseLate
		return Result{Value: json.RawMessage(`{"processed":88}`)}, nil
	})})
	guardDeadline := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	guardEnvelope := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":8,"attempt_generation":1,"deadline_at":%q}`, guardJob.String(), newOwner.String(), guardDeadline)
	if rr := request("POST", "/internal/v1/job-executions", guardEnvelope); rr.Code != 202 {
		t.Fatalf("guard-loss accept: %d %s", rr.Code, rr.Body.String())
	}
	select {
	case <-guardStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("guard-loss pipeline did not start within bound")
	}
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	monitorDone := make(chan struct{})
	go func() { srv.MonitorGuard(monitorCtx, nil); close(monitorDone) }()
	srv.guardMu.Lock()
	var unlocked bool
	unlockErr := srv.guard.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()) # $1)`, ExecutionGuardXOR).Scan(&unlocked)
	srv.guardMu.Unlock()
	if unlockErr != nil || !unlocked {
		t.Fatalf("release fixture execution guard: unlocked=%v err=%v", unlocked, unlockErr)
	}
	select {
	case <-guardCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("guard-loss pipeline did not observe cancellation")
	}
	releaseLateOnce.Do(func() { close(releaseLate) })
	select {
	case <-monitorDone:
	case <-time.After(time.Second):
		t.Fatal("guard monitor did not report lock loss")
	}
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		srv.mu.Lock()
		_, active := srv.active[attemptKey{guardJob, 1}]
		srv.mu.Unlock()
		if !active {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var guardState string
	var guardResult []byte
	if err = pool.QueryRow(ctx, `SELECT state,result FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, guardJob).Scan(&guardState, &guardResult); err != nil {
		t.Fatal(err)
	}
	var guardJobState string
	if err = pool.QueryRow(ctx, `SELECT state FROM background_jobs WHERE id=$1`, guardJob).Scan(&guardJobState); err != nil {
		t.Fatal(err)
	}
	if guardState != "running" || guardResult != nil || guardJobState != "running" {
		t.Fatalf("late result escaped executor guard: receipt=%s result=%s job=%s", guardState, guardResult, guardJobState)
	}
}

func TestPostgresReceiptDeadlineIsUTCAcrossSessionTimezones(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL execution tests")
	}
	if !isLoopbackPostgresTestDSN(dsn) {
		t.Fatal("execution tests require a loopback-only TEST_DATABASE_URL")
	}
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	for _, zone := range []string{"Europe/Belgrade", "America/New_York"} {
		t.Run(zone, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			admin, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect fixture: %v", err)
			}
			defer closeConn(admin)
			schema := fmt.Sprintf("go_execution_tz_%d", time.Now().UnixNano())
			quotedSchema := pgx.Identifier{schema}.Sanitize()
			if _, err = admin.Exec(ctx, `CREATE SCHEMA `+quotedSchema); err != nil {
				t.Fatalf("create isolated schema: %v", err)
			}
			defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+quotedSchema+` CASCADE`) }()
			if _, err = admin.Exec(ctx, `SET search_path TO `+quotedSchema); err != nil {
				t.Fatal(err)
			}
			for _, ddl := range []string{
				`CREATE TABLE background_worker_status(id integer PRIMARY KEY,generation integer NOT NULL,owner_id uuid,lock_backend_pid integer,heartbeat_at timestamp,status text NOT NULL)`,
				`CREATE TABLE background_jobs(id uuid PRIMARY KEY,kind text NOT NULL,payload jsonb NOT NULL,result jsonb,state text NOT NULL,owner_generation integer,attempt_generation integer NOT NULL,initialized_at timestamp)`,
				`CREATE TABLE background_job_executions(job_id uuid NOT NULL REFERENCES background_jobs(id),attempt_generation integer NOT NULL,envelope_sha256 text NOT NULL,executor_instance_id uuid NOT NULL,owner_id uuid,worker_generation integer,deadline_at timestamp,state text NOT NULL,accepted_at timestamp NOT NULL DEFAULT clock_timestamp(),started_at timestamp,finished_at timestamp,result jsonb,error_code text,legacy_completion_applied_at timestamp,PRIMARY KEY(job_id,attempt_generation))`,
			} {
				if _, err = admin.Exec(ctx, ddl); err != nil {
					t.Fatalf("create isolated fixture table: %v", err)
				}
			}

			tzURL := *parsedURL
			query := tzURL.Query()
			query.Set("options", "-c TimeZone="+zone)
			tzURL.RawQuery = strings.ReplaceAll(query.Encode(), "+", "%20")
			tzDSN := tzURL.String()
			poolConfig, err := pgxpool.ParseConfig(tzDSN)
			if err != nil {
				t.Fatal(err)
			}
			poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
			poolConfig.MaxConns = 3
			worker, err := pgx.ConnectConfig(ctx, poolConfig.ConnConfig.Copy())
			if err != nil {
				t.Fatalf("connect timezone-configured worker: %v", err)
			}
			defer closeConn(worker)
			var workerPID int32
			var acquired bool
			if err = worker.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(&acquired, &workerPID); err != nil || !acquired {
				t.Fatalf("acquire fixture worker lease: acquired=%v err=%v", acquired, err)
			}
			owner, job := uuid.New(), uuid.New()
			if _, err = admin.Exec(ctx, `INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,7,$1,$2,clock_timestamp() AT TIME ZONE 'UTC','running')`, owner, workerPID); err != nil {
				t.Fatal(err)
			}
			if _, err = admin.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,'news_processing','{}','running',7,1)`, job); err != nil {
				t.Fatal(err)
			}

			started := make(chan struct{})
			release := make(chan struct{})
			srv, err := New(Config{DatabaseURL: tzDSN, Schema: schema, Token: "fixture-token", QueryTimeout: time.Second, GuardInterval: time.Hour}, Handlers{
				Pipeline: PipelineFunc(func(c context.Context, _ ExecutionContext, _ string, _ json.RawMessage) (Result, error) {
					close(started)
					select {
					case <-c.Done():
					case <-release:
					}
					return Result{Value: json.RawMessage(`{"status":"success"}`)}, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = srv.Open(ctx); err != nil {
				t.Fatalf("open timezone-configured execution server: %v", err)
			}
			defer func() { close(release); _ = srv.Close(context.Background()) }()
			var sessionZone string
			if err = srv.pool.QueryRow(ctx, `SELECT current_setting('TimeZone')`).Scan(&sessionZone); err != nil || sessionZone != zone {
				t.Fatalf("execution pool timezone=%q want=%q err=%v", sessionZone, zone, err)
			}

			deadline := time.Now().Add(3 * time.Minute).Truncate(time.Microsecond).In(time.FixedZone("fixture plus two", 2*60*60))
			deadlineRaw := deadline.Format(time.RFC3339Nano)
			expectedUTC := deadline.UTC().Format("2006-01-02T15:04:05.000000Z")
			body := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":7,"attempt_generation":1,"deadline_at":%q}`, job.String(), owner.String(), deadlineRaw)
			request := func(method, path, rawBody string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(method, path, strings.NewReader(rawBody))
				req.Header.Set("Authorization", "Bearer fixture-token")
				rr := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rr, req)
				return rr
			}
			accepted := request("POST", "/internal/v1/job-executions", body)
			if accepted.Code != 202 {
				t.Fatalf("accept status=%d body=%s", accepted.Code, accepted.Body.String())
			}
			var first struct {
				AcceptedAt string `json:"accepted_at"`
			}
			if err = json.Unmarshal(accepted.Body.Bytes(), &first); err != nil || first.AcceptedAt == "" {
				t.Fatalf("first receipt changed accepted/deadline projection: %+v body=%s err=%v", first, accepted.Body.String(), err)
			}
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("pipeline did not start")
			}

			for _, path := range []string{
				fmt.Sprintf("/internal/v1/job-executions/%s/1", job),
				fmt.Sprintf("/internal/v1/job-executions/%s/1/context", job),
			} {
				response := request("GET", path, "")
				if response.Code != 200 {
					t.Fatalf("GET %s status=%d body=%s", path, response.Code, response.Body.String())
				}
				var wire map[string]json.RawMessage
				if err = json.Unmarshal(response.Body.Bytes(), &wire); err != nil {
					t.Fatal(err)
				}
				var acceptedAt, deadlineAt string
				if strings.HasSuffix(path, "/context") {
					var contextWire struct {
						Envelope struct {
							DeadlineAt string `json:"deadline_at"`
						} `json:"envelope"`
					}
					if err = json.Unmarshal(response.Body.Bytes(), &contextWire); err != nil {
						t.Fatal(err)
					}
					deadlineAt = contextWire.Envelope.DeadlineAt
				} else {
					if json.Unmarshal(wire["accepted_at"], &acceptedAt) != nil || acceptedAt != first.AcceptedAt {
						t.Fatalf("GET %s accepted_at=%q want first=%q", path, acceptedAt, first.AcceptedAt)
					}
					if json.Unmarshal(wire["deadline_at"], &deadlineAt) != nil {
						t.Fatalf("GET %s omitted deadline_at", path)
					}
				}
				if deadlineAt != expectedUTC {
					t.Fatalf("GET %s deadline=%q want Python UTC form %q (session timezone %s)", path, deadlineAt, expectedUTC, zone)
				}
			}
			duplicate := request("POST", "/internal/v1/job-executions", body)
			var duplicateWire struct {
				AcceptedAt string `json:"accepted_at"`
				Duplicate  bool   `json:"duplicate"`
			}
			if duplicate.Code != 202 || json.Unmarshal(duplicate.Body.Bytes(), &duplicateWire) != nil || !duplicateWire.Duplicate || duplicateWire.AcceptedAt != first.AcceptedAt {
				t.Fatalf("duplicate acceptance timestamp mismatch: first=%q response=%s", first.AcceptedAt, duplicate.Body.String())
			}

			_, submittedDigest, err := parseEnvelope([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			var storedDigest string
			if err = srv.pool.QueryRow(ctx, `SELECT envelope_sha256 FROM background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&storedDigest); err != nil || storedDigest != submittedDigest {
				t.Fatalf("stored raw-offset envelope digest=%q want=%q err=%v", storedDigest, submittedDigest, err)
			}
			utcEnvelope := strings.Replace(body, deadlineRaw, expectedUTC, 1)
			_, utcDigest, err := parseEnvelope([]byte(utcEnvelope))
			if err != nil || utcDigest == submittedDigest {
				t.Fatalf("main envelope digest did not bind original deadline spelling: submitted=%q UTC=%q err=%v", submittedDigest, utcDigest, err)
			}
		})
	}
}
