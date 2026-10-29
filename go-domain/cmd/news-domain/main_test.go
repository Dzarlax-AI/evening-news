package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/backup"
	"github.com/Dzarlax-AI/evening-news/go-domain/domainops"
	"github.com/Dzarlax-AI/evening-news/go-domain/extractorclient"
	"github.com/Dzarlax-AI/evening-news/go-domain/media"
	"github.com/Dzarlax-AI/evening-news/go-domain/selectors"
	"github.com/Dzarlax-AI/evening-news/go-domain/sourceprobe"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRequestDrainRejectsNewAndWaitsForInflight(t *testing.T) {
	tracker := &requests{done: make(chan struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	h := tracker.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(204) }))
	go func() { h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)); close(finished) }()
	<-entered
	done := tracker.beginDrain()
	select {
	case <-done:
		t.Fatal("drained with request active")
	default:
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 503 {
		t.Fatalf("new request status=%d", rec.Code)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not drain")
	}
	<-finished
}
func TestPoolBudgetIncludesGuard(t *testing.T) {
	t.Setenv("DB_POOL_SIZE", "")
	t.Setenv("DB_MAX_OVERFLOW", "")
	if got, e := poolBudget(); e != nil || got != 14 {
		t.Fatalf("budget %d %v", got, e)
	}
	t.Setenv("DB_POOL_SIZE", "3")
	t.Setenv("DB_MAX_OVERFLOW", "2")
	if got, e := poolBudget(); e != nil || got != 4 {
		t.Fatalf("budget %d %v", got, e)
	}
	t.Setenv("DB_POOL_SIZE", "1")
	t.Setenv("DB_MAX_OVERFLOW", "1")
	if _, e := poolBudget(); e == nil {
		t.Fatal("accepted insufficient budget")
	}
}

func TestDomainProcessStartupAndDrainPostgres(t *testing.T) {
	if os.Getenv("NEWS_DOMAIN_HELPER") == "1" {
		if err := run(); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
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
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	schema := fmt.Sprintf("domain_start_%d", time.Now().UnixNano())
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err = db.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	statements := []string{
		`CREATE TABLE ` + ident + `.settings(key text primary key,value json not null,updated_at timestamp)`,
		`CREATE TABLE ` + ident + `.background_worker_status(id integer primary key,owner_id uuid,generation integer,status text,lock_backend_pid integer,heartbeat_at timestamp)`,
		`CREATE TABLE ` + ident + `.background_jobs(id uuid primary key,state text,owner_generation integer,attempt_generation integer,kind text,payload jsonb)`,
		`CREATE TABLE ` + ident + `.background_job_executions(job_id uuid,attempt_generation integer,executor_instance_id uuid,owner_id uuid,worker_generation integer,deadline_at timestamp,state text)`,
		`CREATE TABLE ` + ident + `.article_processing_runs(id uuid,job_id uuid,attempt_generation integer,envelope_sha256 text,state text)`,
		`CREATE TABLE ` + ident + `.article_processing_items(run_id uuid,article_id bigint,article_attempt integer,input_fingerprint text,state text)`,
		`CREATE TABLE ` + ident + `.article_ingestion_runs(id uuid,job_id uuid,attempt_generation integer,state text)`,
		`CREATE TABLE ` + ident + `.sources(id bigint,name text,source_type text,url text,enabled boolean,config jsonb)`,
		`CREATE TABLE ` + ident + `.editorial_digest_editions(edition_date date,locale text)`,
		`CREATE TABLE ` + ident + `.processing_stats(date date unique,articles_fetched integer,articles_processed integer,api_calls_made integer,errors_count integer,processing_time_seconds integer)`,
		`CREATE TABLE ` + ident + `.articles(id bigint,processing_attempts integer,url text,source_id bigint,media_files jsonb,image_url text)`,
		`CREATE TABLE ` + ident + `.extraction_attempts(article_url text,domain text,extraction_strategy text,selector_used text,success boolean,content_length integer,quality_score double precision,extraction_time_ms integer,error_message text)`,
		`CREATE TABLE ` + ident + `.extraction_patterns(domain text,selector_pattern text,extraction_strategy text,success_count integer DEFAULT 0,failure_count integer DEFAULT 0,quality_score_avg double precision,content_length_avg integer DEFAULT 0,discovered_by text,is_stable boolean,consecutive_successes integer DEFAULT 0,consecutive_failures integer DEFAULT 0,first_success_at timestamp,last_success_at timestamp,updated_at timestamp,unique(domain,selector_pattern,extraction_strategy))`,
		`CREATE FUNCTION ` + ident + `.fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp) RETURNS TABLE(run_id uuid,run_state text) LANGUAGE sql AS $$ SELECT id,state FROM ` + ident + `.article_processing_runs WHERE id=$1 AND job_id=$2 AND attempt_generation=$3 AND envelope_sha256=$4 FOR UPDATE $$`,
	}
	for _, statement := range statements {
		if _, err = db.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	worker, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close(context.Background())
	executor, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close(context.Background())
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
	deadline := time.Now().UTC().Add(3 * time.Minute).Truncate(time.Microsecond)
	env := authority.Envelope{Version: 1, JobID: job, AttemptGeneration: 1, OwnerID: owner, Generation: 1, BackendPID: workerPID, ExecutorInstanceID: instance, DeadlineAt: deadline.Format(time.RFC3339Nano)}
	envJSON, _ := json.Marshal(env)
	envelopeHash := sha256.Sum256(envJSON)
	runHash := sha256.Sum256([]byte("evening-news-processing-v1\x00" + job + "\x001"))
	runBytes := runHash[:16]
	runBytes[6] = (runBytes[6] & 0x0f) | 0x50
	runBytes[8] = (runBytes[8] & 0x3f) | 0x80
	runID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", runBytes[0:4], runBytes[4:6], runBytes[6:8], runBytes[8:10], runBytes[10:16])
	ingestionHash := sha256.Sum256([]byte("evening-news-ingestion-v1\x00" + job + "\x001"))
	ingestionBytes := ingestionHash[:16]
	ingestionBytes[6] = (ingestionBytes[6] & 0x0f) | 0x50
	ingestionBytes[8] = (ingestionBytes[8] & 0x3f) | 0x80
	ingestionID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", ingestionBytes[0:4], ingestionBytes[4:6], ingestionBytes[6:8], ingestionBytes[8:10], ingestionBytes[10:16])
	input := map[string]any{"title": "Synthetic title", "content": "synthetic content synthetic content synthetic content", "url": "https://example.test/story"}
	inputJSON, _ := json.Marshal(input)
	inputHash := sha256.Sum256(inputJSON)
	fingerprint := hex.EncodeToString(inputHash[:])
	seed := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO ` + ident + `.background_worker_status VALUES(1,$1,1,'running',$2,clock_timestamp() AT TIME ZONE 'UTC')`, []any{owner, workerPID}},
		{`INSERT INTO ` + ident + `.background_jobs VALUES($1,'running',1,1,'news_processing','{}')`, []any{job}},
		{`INSERT INTO ` + ident + `.background_job_executions VALUES($1,1,$2,$3,1,$4,'running')`, []any{job, instance, owner, deadline}},
		{`INSERT INTO ` + ident + `.article_processing_runs VALUES($1,$2,1,$3,'running')`, []any{runID, job, hex.EncodeToString(envelopeHash[:])}},
		{`INSERT INTO ` + ident + `.article_processing_items VALUES($1,7,1,$2,'running')`, []any{runID, fingerprint}},
		{`INSERT INTO ` + ident + `.article_ingestion_runs VALUES($1,$2,1,'running')`, []any{ingestionID, job}},
		{`INSERT INTO ` + ident + `.sources VALUES(9,'Synthetic','custom','https://source.example.test/news',true,'{"enable_ai_analysis":true}')`, nil},
		{`INSERT INTO ` + ident + `.editorial_digest_editions VALUES((clock_timestamp() AT TIME ZONE 'UTC')::date,'ru')`, nil},
		{`INSERT INTO ` + ident + `.articles(id,processing_attempts,url) VALUES(7,1,'https://example.test/story')`, nil},
		{`INSERT INTO ` + ident + `.extraction_patterns(domain,selector_pattern,extraction_strategy,success_count,failure_count,quality_score_avg,content_length_avg,discovered_by,is_stable,consecutive_successes,consecutive_failures,last_success_at) VALUES('example.test','main','readability',4,1,0.8,100,'manual',false,2,0,now())`, nil},
	}
	for _, s := range seed {
		if _, err = db.Exec(ctx, s.sql, s.args...); err != nil {
			t.Fatal(err)
		}
	}
	// The selected saved OpenAI profile has lost its key. The process must keep
	// the choice visible for repair without sending an AI request or falling back.
	if _, err = db.Exec(ctx, `INSERT INTO `+ident+`.settings(key,value) VALUES ('ai_settings_revision','1'),('ai_settings','{"active_provider":"openai","profiles":{"gemini":{"models":{"article":"fixture-model","digest":"fixture-model","text":"fixture-model"}},"openai":{"models":{"article":"openai-a","digest":"openai-d","text":"openai-t"}}}}')`); err != nil {
		t.Fatal(err)
	}
	var providerCalls atomic.Int32
	var mutateSource atomic.Bool
	var blockSelector atomic.Bool
	providerStarted := make(chan struct{}, 1)
	var selectorCalls atomic.Int32
	var digestMode atomic.Bool
	var mediaMode atomic.Bool
	var sourceMode atomic.Bool
	var backupMode atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/v1/job-executions/") {
			kind := "news_processing"
			if digestMode.Load() {
				kind = "news_digest"
			}
			payload := json.RawMessage(`{}`)
			if mediaMode.Load() {
				kind = "article_reprocess"
				payload = json.RawMessage(`{"article_ids":[7],"reextract_media":true}`)
			}
			if sourceMode.Load() {
				kind = "source_test"
				payload = json.RawMessage(`{"source_id":11}`)
			}
			if backupMode.Load() {
				kind = "backup"
				payload = json.RawMessage(`{}`)
			}
			_ = json.NewEncoder(w).Encode(authority.ExecutionContext{Version: 1, Envelope: env, ExecutorBackendPID: executorPID, Kind: kind, Payload: payload})
			return
		}
		if r.URL.Path == "/v1/sources/collect" {
			var request struct {
				RequestID string `json:"request_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&request)
			_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "build_version": "fixture", "request_id": request.RequestID, "results": []any{map[string]any{"source_id": 11, "type": "rss", "outcome": "success", "rss_articles": []any{}}}})
			return
		}
		providerCalls.Add(1)
		requestBody, _ := io.ReadAll(r.Body)
		if bytes.Contains(requestBody, []byte("Analyze this webpage")) {
			selectorCalls.Add(1)
			if blockSelector.Load() {
				providerStarted <- struct{}{}
				<-r.Context().Done()
				return
			}
			if mutateSource.Load() {
				if _, e := db.Exec(ctx, `UPDATE `+ident+`.sources SET config='{"enable_ai_analysis":false}' WHERE id=9`); e != nil {
					t.Error(e)
				}
			}
			_, _ = w.Write([]byte(`{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"{\"content_selectors\":[\"article\"],\"title_selectors\":[\"h2\"],\"date_selectors\":[\"time\"],\"page_type\":\"news\"}"}]}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"{\"summary\":\"ok\",\"categories\":[\"Business\"],\"is_advertisement\":false}"}]}}]}`))
	}))
	defer upstream.Close()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	listener.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDomainProcessStartupAndDrainPostgres$")
	cmd.Env = append(os.Environ(), "NEWS_DOMAIN_HELPER=1", "DATABASE_URL="+u.String(), "GEMINI_API_KEY=fixture-key", "OPENAI_API_KEY=", "AI_PROVIDER=gemini", "GEMINI_API_ENDPOINT="+upstream.URL+"/v1/models", "SUMMARIZATION_MODEL=fixture-model", "JOB_EXECUTOR_TOKEN=fixture-executor-token", "ARTICLE_AI_TOKEN=fixture-article-token", "WEB_BACKEND_TOKEN=fixture-web-token", "BACKGROUND_EXECUTION_MODE=worker", "EXTRACTION_SERVICE_URL="+upstream.URL, "EXTRACTION_SERVICE_TOKEN=fixture-extractor-token", "JOB_EXECUTOR_HOST=127.0.0.1", "JOB_EXECUTOR_PORT="+port, "EXECUTION_SERVICE_URL="+upstream.URL, "NEWS_FIXTURE_MODE=1", "NEWS_TEST_TELEGRAM_ENDPOINT="+upstream.URL)
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	client := &http.Client{Timeout: 5 * time.Second}
	var resp *http.Response
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		req, _ := http.NewRequest(http.MethodGet, "http://"+address+"/internal/v1/status", nil)
		req.Header.Set("Authorization", "Bearer fixture-executor-token")
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("domain startup failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("domain readiness status %d", resp.StatusCode)
	}
	var domainStatus struct {
		DomainInstanceID string `json:"domain_instance_id"`
		ActiveOperations int    `json:"active_operations"`
		Ready            bool   `json:"ready"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&domainStatus); err != nil || !domainStatus.Ready || domainStatus.ActiveOperations != 0 || len(domainStatus.DomainInstanceID) != 36 {
		t.Fatalf("invalid domain readiness payload: %#v, %v", domainStatus, err)
	}
	settingsURL := "http://" + address + "/internal/web/v1/ai/settings"
	settingsReq, _ := http.NewRequest(http.MethodGet, settingsURL, nil)
	settingsReq.Header.Set("Authorization", "Bearer fixture-web-token")
	settingsResp, e := client.Do(settingsReq)
	if e != nil {
		t.Fatal(e)
	}
	var settings map[string]any
	e = json.NewDecoder(settingsResp.Body).Decode(&settings)
	settingsResp.Body.Close()
	if e != nil || settingsResp.StatusCode != 200 || settings["active_provider"] != "openai" || settings["source"] != "override" || settings["revision"] != float64(1) {
		t.Fatalf("unavailable profile not visible: status=%d settings=%v err=%v", settingsResp.StatusCode, settings, e)
	}
	sourceFP := selectors.SourceFingerprint(9, "Synthetic", "custom", "https://source.example.test/news", true, `{"enable_ai_analysis": true}`)
	var unavailableRequest map[string]any
	if err = json.Unmarshal(envJSON, &unavailableRequest); err != nil {
		t.Fatal(err)
	}
	unavailableRequest["operation"] = "study"
	unavailableRequest["source_id"] = 9
	unavailableRequest["source_fingerprint"] = sourceFP
	unavailableRequest["study_html"] = `<article><h2>fixture</h2></article>`
	unavailableBody, _ := json.Marshal(unavailableRequest)
	unavailableReq, _ := http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/ingestion/selectors", bytes.NewReader(unavailableBody))
	unavailableReq.Header.Set("Authorization", "Bearer fixture-executor-token")
	unavailableResp, e := client.Do(unavailableReq)
	if e != nil {
		t.Fatal(e)
	}
	unavailableResp.Body.Close()
	if unavailableResp.StatusCode != http.StatusNoContent || providerCalls.Load() != 0 {
		t.Fatalf("invalid profile made provider calls or returned unexpected result: status=%d calls=%d", unavailableResp.StatusCode, providerCalls.Load())
	}
	resetPayload, _ := json.Marshal(map[string]any{"expected_revision": settings["revision"], "active_provider": settings["active_provider"], "profiles": settings["profiles"], "reset_to_env": true})
	resetReq, _ := http.NewRequest(http.MethodPut, settingsURL, bytes.NewReader(resetPayload))
	resetReq.Header.Set("Authorization", "Bearer fixture-web-token")
	resetResp, e := client.Do(resetReq)
	if e != nil {
		t.Fatal(e)
	}
	var reset map[string]any
	e = json.NewDecoder(resetResp.Body).Decode(&reset)
	resetResp.Body.Close()
	if e != nil || resetResp.StatusCode != 200 || reset["source"] != "env" || reset["active_provider"] != "gemini" || reset["revision"] != float64(2) {
		t.Fatalf("reset failed: status=%d result=%v err=%v", resetResp.StatusCode, reset, e)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("settings operation called provider")
	}
	for _, tc := range []struct {
		token string
		want  int
	}{{"wrong", 401}, {"fixture-web-token", 409}} {
		r, _ := http.NewRequest(http.MethodPost, "http://"+address+"/internal/web/v1/backups/restore", strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		v, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		v.Body.Close()
		if v.StatusCode != tc.want {
			t.Fatalf("admin worker restore status=%d want=%d", v.StatusCode, tc.want)
		}
	}
	payload, _ := json.Marshal(map[string]any{"version": 1, "run_id": runID, "envelope": env, "article_id": 7, "article_attempt": 1, "input_fingerprint": fingerprint, "operation": "analyze", "input": input})
	req, _ := http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/article-ai", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer fixture-article-token")
	articleResp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(articleResp.Body)
	articleResp.Body.Close()
	if articleResp.StatusCode != 200 || !strings.Contains(string(body), `"summary":"ok"`) {
		t.Fatalf("article callback status=%d body=%s", articleResp.StatusCode, body)
	}
	input = map[string]any{"url": "https://example.test/story"}
	payload, _ = json.Marshal(map[string]any{"version": 1, "run_id": runID, "envelope": env, "article_id": 7, "article_attempt": 1, "input_fingerprint": fingerprint, "operation": "extraction_context", "input": input})
	req, _ = http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/article-ai", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer fixture-article-token")
	articleResp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(articleResp.Body)
	articleResp.Body.Close()
	if articleResp.StatusCode != 200 || !strings.Contains(string(body), `"selector":"main"`) {
		t.Fatalf("context status=%d body=%s", articleResp.StatusCode, body)
	}
	input = map[string]any{"url": "https://example.test/story", "method": "readability", "success": false, "observations": []any{map[string]any{"selector": "main", "method": "readability", "success": false}}}
	payload, _ = json.Marshal(map[string]any{"version": 1, "run_id": runID, "envelope": env, "article_id": 7, "article_attempt": 1, "input_fingerprint": fingerprint, "operation": "extraction_feedback", "input": input})
	req, _ = http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/article-ai", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer fixture-article-token")
	articleResp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(articleResp.Body)
	articleResp.Body.Close()
	if articleResp.StatusCode != 200 || !strings.Contains(string(body), `"recorded":true`) {
		t.Fatalf("feedback status=%d body=%s", articleResp.StatusCode, body)
	}
	var failures int
	if err = db.QueryRow(ctx, `SELECT failure_count FROM `+ident+`.extraction_patterns WHERE selector_pattern='main'`).Scan(&failures); err != nil || failures != 2 {
		t.Fatalf("feedback failures=%d error=%v", failures, err)
	}
	var configText string
	if err = db.QueryRow(ctx, `SELECT config::text FROM `+ident+`.sources WHERE id=9`).Scan(&configText); err != nil {
		t.Fatal(err)
	}
	sourceFP = selectors.SourceFingerprint(9, "Synthetic", "custom", "https://source.example.test/news", true, configText)
	var selectorRequest map[string]any
	if err = json.Unmarshal(envJSON, &selectorRequest); err != nil {
		t.Fatal(err)
	}
	selectorRequest["operation"] = "study"
	selectorRequest["source_id"] = 9
	selectorRequest["source_fingerprint"] = sourceFP
	selectorRequest["study_html"] = `<article><h2>fixture</h2><time>today</time></article>`
	payload, _ = json.Marshal(selectorRequest)
	req, _ = http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/ingestion/selectors", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer fixture-executor-token")
	articleResp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(articleResp.Body)
	articleResp.Body.Close()
	if articleResp.StatusCode != 200 || !strings.Contains(string(body), `"learned_container_selectors":["article"]`) {
		t.Fatalf("selector study status=%d body=%s", articleResp.StatusCode, body)
	}
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.sources SET url='https://source.example.test/other' WHERE id=9`); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, `SELECT config::text FROM `+ident+`.sources WHERE id=9`).Scan(&configText); err != nil {
		t.Fatal(err)
	}
	selectorRequest["source_fingerprint"] = selectors.SourceFingerprint(9, "Synthetic", "custom", "https://source.example.test/other", true, configText)
	mutateSource.Store(true)
	payload, _ = json.Marshal(selectorRequest)
	req, _ = http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/ingestion/selectors", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer fixture-executor-token")
	articleResp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, articleResp.Body)
	articleResp.Body.Close()
	if articleResp.StatusCode != 409 {
		t.Fatalf("source change during study status=%d", articleResp.StatusCode)
	}
	mutateSource.Store(false)
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.sources SET config='{}' WHERE id=9`); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, `SELECT config::text FROM `+ident+`.sources WHERE id=9`).Scan(&configText); err != nil {
		t.Fatal(err)
	}
	selectorRequest["source_fingerprint"] = selectors.SourceFingerprint(9, "Synthetic", "custom", "https://source.example.test/other", true, configText)
	payload, _ = json.Marshal(selectorRequest)
	req, _ = http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/ingestion/selectors", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer fixture-executor-token")
	articleResp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, articleResp.Body)
	articleResp.Body.Close()
	if articleResp.StatusCode != 200 || selectorCalls.Load() != 3 {
		t.Fatalf("stale selector cached: status=%d provider_calls=%d", articleResp.StatusCode, selectorCalls.Load())
	}
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		t.Fatalf("domain shutdown: %v", err)
	}
	waited = true
	// Start a second process and revoke its dedicated advisory-lock session.
	// The process must stop rather than continuing to accept private work.
	lost := exec.Command(os.Args[0], "-test.run=^TestDomainProcessStartupAndDrainPostgres$")
	lost.Env = cmd.Env
	if err = lost.Start(); err != nil {
		t.Fatal(err)
	}
	lostWaited := false
	var doneLost chan error
	defer func() {
		if !lostWaited {
			_ = lost.Process.Kill()
			if doneLost != nil {
				<-doneLost
			} else {
				_ = lost.Wait()
			}
		}
	}()
	until = time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		r, _ := http.NewRequest(http.MethodGet, "http://"+address+"/internal/v1/status", nil)
		r.Header.Set("Authorization", "Bearer fixture-executor-token")
		v, e := client.Do(r)
		if e == nil {
			_ = v.Body.Close()
			if v.StatusCode == 200 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	blockSelector.Store(true)
	pendingDone := make(chan struct{}, 1)
	go func() {
		r, _ := http.NewRequest(http.MethodPost, "http://"+address+"/internal/v1/ingestion/selectors", bytes.NewReader(payload))
		r.Header.Set("Authorization", "Bearer fixture-executor-token")
		v, _ := client.Do(r)
		if v != nil {
			_ = v.Body.Close()
		}
		pendingDone <- struct{}{}
	}()
	select {
	case <-providerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("selector provider did not start")
	}
	var terminated bool
	err = db.QueryRow(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext($1) # $2::integer)::bigint+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`, schema, authority.ExecutorGuardXOR).Scan(&terminated)
	if err != nil || !terminated {
		t.Fatalf("fixture guard termination failed: %v", err)
	}
	doneLost = make(chan error, 1)
	go func() { doneLost <- lost.Wait() }()
	select {
	case e := <-doneLost:
		lostWaited = true
		if e == nil {
			t.Fatal("guard loss exited successfully")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("guard loss did not stop process")
	}
	select {
	case <-pendingDone:
	case <-time.After(5 * time.Second):
		t.Fatal("active callback did not drain after guard loss")
	}
	blockSelector.Store(false)
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	guard, err := authority.AcquireGuard(ctx, pool)
	if err != nil {
		t.Fatalf("guard not released after shutdown: %v", err)
	}
	defer guard.Close(context.Background())
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.background_jobs SET kind='news_digest' WHERE id=$1`, job); err != nil {
		t.Fatal(err)
	}
	digestMode.Store(true)
	authorityClient, err := authority.NewContextClient(upstream.URL, "fixture-executor-token")
	if err != nil {
		t.Fatal(err)
	}
	defer authorityClient.Close()
	runtime := airuntime.NewServer(airuntime.Config{APIKey: "fixture", Endpoint: upstream.URL + "/v1/models", Model: "fixture-model", RPS: 100}, pool)
	const domainInstance = "50000000-0000-4000-8000-000000000005"
	const operationID = "60000000-0000-4000-8000-000000000006"
	ops := domainops.New(ctx, "fixture-executor-token", domainInstance, guard, authorityClient, pool, runtime)
	digestHash := sha256.Sum256([]byte(`{}`))
	operation := domainops.Request{Version: 1, Operation: "summary_status", OperationID: operationID, DomainInstanceID: domainInstance, Envelope: env, InputFingerprint: hex.EncodeToString(digestHash[:]), Input: json.RawMessage(`{}`)}
	wire, _ := json.Marshal(operation)
	post := httptest.NewRequest(http.MethodPost, "/internal/v1/domain-operations", bytes.NewReader(wire))
	post.Header.Set("Authorization", "Bearer fixture-executor-token")
	post.Header.Set("X-Domain-Instance-Id", domainInstance)
	rec := httptest.NewRecorder()
	ops.Post(rec, post)
	if rec.Code != 202 {
		t.Fatalf("digest acceptance status=%d body=%s", rec.Code, rec.Body.String())
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/v1/domain-operations/{job}/{attempt}/{operation}/{id}", ops.Get)
	var state string
	until = time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		get := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/v1/domain-operations/%s/1/summary_status/%s", job, operationID), nil)
		get.Header.Set("Authorization", "Bearer fixture-executor-token")
		get.Header.Set("X-Domain-Instance-Id", domainInstance)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, get)
		var value map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &value)
		state, _ = value["state"].(string)
		if state == "succeeded" {
			if value["result"].(map[string]any)["summaries_count"] != float64(1) {
				t.Fatalf("digest result %v", value)
			}
			break
		}
		if state == "failed" {
			t.Fatalf("digest failed: %s", rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state != "succeeded" {
		t.Fatalf("digest not finished: %s", state)
	}
	statsInput := []byte(`{"stats":{"articles_processed":2,"duration_seconds":1.5,"errors_count":1}}`)
	statsHash := sha256.Sum256(statsInput)
	operation.Operation = "stats_publish"
	operation.OperationID = "70000000-0000-4000-8000-000000000007"
	operation.Input = statsInput
	operation.InputFingerprint = hex.EncodeToString(statsHash[:])
	wire, _ = json.Marshal(operation)
	post = httptest.NewRequest(http.MethodPost, "/internal/v1/domain-operations", bytes.NewReader(wire))
	post.Header.Set("Authorization", "Bearer fixture-executor-token")
	post.Header.Set("X-Domain-Instance-Id", domainInstance)
	rec = httptest.NewRecorder()
	ops.Post(rec, post)
	if rec.Code != 202 {
		t.Fatalf("stats acceptance status=%d body=%s", rec.Code, rec.Body.String())
	}
	state = ""
	until = time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		get := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/v1/domain-operations/%s/1/stats_publish/%s", job, operation.OperationID), nil)
		get.Header.Set("Authorization", "Bearer fixture-executor-token")
		get.Header.Set("X-Domain-Instance-Id", domainInstance)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, get)
		var value map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &value)
		state, _ = value["state"].(string)
		if state == "succeeded" {
			break
		}
		if state == "failed" {
			t.Fatalf("stats failed: %s", rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state != "succeeded" {
		t.Fatal("stats not finished")
	}
	var processed, duration, errorsCount int
	if err = db.QueryRow(ctx, `SELECT articles_processed,processing_time_seconds,errors_count FROM `+ident+`.processing_stats`).Scan(&processed, &duration, &errorsCount); err != nil || processed != 2 || duration != 1 || errorsCount != 1 {
		t.Fatalf("stats row %d %d %d %v", processed, duration, errorsCount, err)
	}
	if _, err = db.Exec(ctx, `INSERT INTO `+ident+`.sources VALUES(10,'Telegram fixture','telegram','https://t.me/fixture',false,'{}')`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.articles SET source_id=10,url='https://t.me/fixture/1',media_files='[]'::jsonb WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.background_jobs SET kind='article_reprocess',payload='{"article_ids":[7],"reextract_media":true}'::jsonb WHERE id=$1`, job); err != nil {
		t.Fatal(err)
	}
	mediaMode.Store(true)
	mediaService := media.New()
	mediaService.HTTP = &http.Client{Transport: fixtureTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://t.me/s/fixture/1" {
			t.Errorf("unexpected target %s", r.URL)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<div data-post="fixture/1"><a class="tgme_widget_message_photo_wrap" style="background-image:url(https://cdn.example/old.jpg)"></a></div>`))}, nil
	})}
	ops.Media = mediaService
	mediaInput := []byte(`{"article_id":7}`)
	mediaHash := sha256.Sum256(mediaInput)
	operation.Operation = "media_refresh"
	operation.OperationID = "90000000-0000-4000-8000-000000000009"
	operation.Input = mediaInput
	operation.InputFingerprint = hex.EncodeToString(mediaHash[:])
	wire, _ = json.Marshal(operation)
	post = httptest.NewRequest(http.MethodPost, "/internal/v1/domain-operations", bytes.NewReader(wire))
	post.Header.Set("Authorization", "Bearer fixture-executor-token")
	post.Header.Set("X-Domain-Instance-Id", domainInstance)
	rec = httptest.NewRecorder()
	ops.Post(rec, post)
	if rec.Code != 202 {
		t.Fatalf("media acceptance status=%d body=%s", rec.Code, rec.Body.String())
	}
	state = ""
	until = time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		get := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/v1/domain-operations/%s/1/media_refresh/%s", job, operation.OperationID), nil)
		get.Header.Set("Authorization", "Bearer fixture-executor-token")
		get.Header.Set("X-Domain-Instance-Id", domainInstance)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, get)
		var value map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &value)
		state, _ = value["state"].(string)
		if state == "succeeded" {
			if value["result"].(map[string]any)["refreshed"] != true {
				t.Fatalf("media result=%v", value)
			}
			break
		}
		if state == "failed" {
			t.Fatalf("media failed: %s", rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state != "succeeded" {
		t.Fatal("media not finished")
	}
	var imageURL string
	if err = db.QueryRow(ctx, `SELECT image_url FROM `+ident+`.articles WHERE id=7`).Scan(&imageURL); err != nil || imageURL != "https://cdn.example/old.jpg" {
		t.Fatalf("image=%q error=%v", imageURL, err)
	}
	mediaFence := authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "article_reprocess", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: workerPID, ExecutorBackendPID: executorPID, Deadline: deadline, Payload: json.RawMessage(`{"article_ids":[7],"reextract_media":true}`)}
	mediaService.HTTP = &http.Client{Transport: fixtureTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<div data-post="fixture/2">missing target</div>`))}, nil
	})}
	if refreshed, e := mediaService.Refresh(ctx, mediaFence, 7); e != nil || refreshed {
		t.Fatalf("missing post refreshed=%v error=%v", refreshed, e)
	}
	if err = db.QueryRow(ctx, `SELECT image_url FROM `+ident+`.articles WHERE id=7`).Scan(&imageURL); err != nil || imageURL != "https://cdn.example/old.jpg" {
		t.Fatalf("missing post changed image %q %v", imageURL, err)
	}
	mediaService.HTTP = &http.Client{Transport: fixtureTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<div data-post="fixture/1">empty media</div>`))}, nil
	})}
	if refreshed, e := mediaService.Refresh(ctx, mediaFence, 7); e != nil || !refreshed {
		t.Fatalf("empty post refreshed=%v error=%v", refreshed, e)
	}
	var stored json.RawMessage
	var image *string
	if err = db.QueryRow(ctx, `SELECT media_files,image_url FROM `+ident+`.articles WHERE id=7`).Scan(&stored, &image); err != nil || string(stored) != "[]" || image != nil {
		t.Fatalf("empty post media=%s image=%v error=%v", stored, image, err)
	}
	if _, err = db.Exec(ctx, `INSERT INTO `+ident+`.sources VALUES(11,'RSS fixture','rss','https://example.test/feed',false,'{}')`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.background_jobs SET kind='source_test',payload='{"source_id":11}'::jsonb WHERE id=$1`, job); err != nil {
		t.Fatal(err)
	}
	missingProbe := &sourceprobe.Probe{}
	sourceFence := authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "source_test", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: workerPID, ExecutorBackendPID: executorPID, Deadline: deadline, Payload: json.RawMessage(`{"source_id":11}`)}
	if ok, err := missingProbe.Test(ctx, sourceFence, 11); err != nil || ok {
		t.Fatalf("unconfigured source probe ok=%v err=%v", ok, err)
	}
	if _, err := missingProbe.Test(ctx, sourceFence, 99); err == nil {
		t.Fatal("unconfigured source probe accepted missing source")
	}
	sourceMode.Store(true)
	extractor, err := extractorclient.New(upstream.URL, "fixture-extractor-token")
	if err != nil {
		t.Fatal(err)
	}
	defer extractor.Close()
	ops.Source = &sourceprobe.Probe{Client: extractor}
	sourceInput := []byte(`{"source_id":11}`)
	sourceHash := sha256.Sum256(sourceInput)
	operation.Operation = "source_test"
	operation.OperationID = "a0000000-0000-4000-8000-00000000000a"
	operation.Input = sourceInput
	operation.InputFingerprint = hex.EncodeToString(sourceHash[:])
	wire, _ = json.Marshal(operation)
	post = httptest.NewRequest(http.MethodPost, "/internal/v1/domain-operations", bytes.NewReader(wire))
	post.Header.Set("Authorization", "Bearer fixture-executor-token")
	post.Header.Set("X-Domain-Instance-Id", domainInstance)
	rec = httptest.NewRecorder()
	ops.Post(rec, post)
	if rec.Code != 202 {
		t.Fatalf("source acceptance status=%d body=%s", rec.Code, rec.Body.String())
	}
	state = ""
	until = time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		get := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/v1/domain-operations/%s/1/source_test/%s", job, operation.OperationID), nil)
		get.Header.Set("Authorization", "Bearer fixture-executor-token")
		get.Header.Set("X-Domain-Instance-Id", domainInstance)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, get)
		var value map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &value)
		state, _ = value["state"].(string)
		if state == "succeeded" {
			if value["result"].(map[string]any)["success"] != true {
				t.Fatalf("source result=%v", value)
			}
			break
		}
		if state == "failed" {
			t.Fatalf("source failed: %s", rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state != "succeeded" {
		t.Fatal("source not finished")
	}
	if _, err = db.Exec(ctx, `UPDATE `+ident+`.background_jobs SET kind='backup',payload='{}'::jsonb WHERE id=$1`, job); err != nil {
		t.Fatal(err)
	}
	backupMode.Store(true)
	backupRoot := t.TempDir()
	if err = os.Mkdir(filepath.Join(backupRoot, "data"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(backupRoot, "data", "fixture.txt"), []byte("synthetic data"), 0600); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(backupRoot, "args.txt")
	t.Setenv("NEWS_BACKUP_TEST_ARGS", argsPath)
	fakeDump := filepath.Join(backupRoot, "fake-pg-dump")
	script := "#!/bin/sh\n[ \"$PGPASSWORD\" = 'fixture-only' ] || exit 9\nprintf '%s\\n' \"$@\" > \"$NEWS_BACKUP_TEST_ARGS\"\nprintf 'synthetic SQL dump'\n"
	if err = os.WriteFile(fakeDump, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	backupService := backup.New(backupRoot, u.String())
	backupService.DumpBinary = fakeDump
	ops.Backup = backupService
	operation.Operation = "backup"
	operation.OperationID = "b0000000-0000-4000-8000-00000000000b"
	operation.Input = json.RawMessage(`{}`)
	operation.InputFingerprint = hex.EncodeToString(digestHash[:])
	wire, _ = json.Marshal(operation)
	post = httptest.NewRequest(http.MethodPost, "/internal/v1/domain-operations", bytes.NewReader(wire))
	post.Header.Set("Authorization", "Bearer fixture-executor-token")
	post.Header.Set("X-Domain-Instance-Id", domainInstance)
	rec = httptest.NewRecorder()
	ops.Post(rec, post)
	if rec.Code != 202 {
		t.Fatalf("backup acceptance status=%d body=%s", rec.Code, rec.Body.String())
	}
	state = ""
	until = time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		get := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/v1/domain-operations/%s/1/backup/%s", job, operation.OperationID), nil)
		get.Header.Set("Authorization", "Bearer fixture-executor-token")
		get.Header.Set("X-Domain-Instance-Id", domainInstance)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, get)
		var value map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &value)
		state, _ = value["state"].(string)
		if state == "succeeded" {
			break
		}
		if state == "failed" {
			t.Fatalf("backup failed: %s", rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state != "succeeded" {
		t.Fatal("backup not finished")
	}
	args, err := os.ReadFile(argsPath)
	if err != nil || bytes.Contains(args, []byte("fixture-only")) {
		t.Fatal("backup password leaked into arguments")
	}
	files, err := os.ReadDir(filepath.Join(backupRoot, "backups"))
	if err != nil || len(files) != 1 {
		t.Fatalf("backup artifacts=%v error=%v", files, err)
	}
	archiveFile, err := os.Open(filepath.Join(backupRoot, "backups", files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(archiveFile)
	if err != nil {
		t.Fatal(err)
	}
	tarReader := tar.NewReader(gz)
	contents := map[string]string{}
	for {
		h, e := tarReader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if h.Typeflag == tar.TypeReg {
			data, e := io.ReadAll(tarReader)
			if e != nil {
				t.Fatal(e)
			}
			contents[filepath.Base(h.Name)] = string(data)
		}
	}
	gz.Close()
	archiveFile.Close()
	if contents["database.sql"] != "synthetic SQL dump" || contents["fixture.txt"] != "synthetic data" || contents["backup_info.json"] == "" {
		t.Fatalf("backup contents missing: %v", contents)
	}
	if err = ops.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err = guard.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
