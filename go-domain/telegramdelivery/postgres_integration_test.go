package telegramdelivery_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReceiptAndFencePostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("isolated PostgreSQL unavailable")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("loopback database required")
	}
	query, queryErr := url.ParseQuery(u.RawQuery)
	if queryErr != nil {
		t.Fatal("invalid fixture database URL")
	}
	for key := range query {
		switch strings.ToLower(key) {
		case "host", "hostaddr", "service", "servicefile":
			t.Fatal("fixture database routing override rejected")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("fixture database unavailable")
	}
	defer admin.Close(ctx)
	schema := os.Getenv("DIGEST_TEST_SCHEMA")
	if schema != "" {
		if !regexp.MustCompile(`^test_editorial_[a-f0-9]{32}$`).MatchString(schema) {
			t.Fatal("invalid pre-created fixture schema")
		}
	} else {
		schema = fmt.Sprintf("digest_%d", time.Now().UnixNano())
		if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatal("fixture schema creation failed")
		}
		defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("fixture configuration invalid")
	}
	cfg.MaxConns = 2
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("fixture pool unavailable")
	}
	defer pool.Close()
	ddl := `CREATE TABLE background_worker_status(id int primary key,owner_id uuid,generation int,status text,lock_backend_pid int,heartbeat_at timestamp);
 CREATE TABLE background_jobs(id uuid primary key,state text,owner_generation int,attempt_generation int,kind text,payload jsonb);
 CREATE TABLE background_job_executions(job_id uuid,attempt_generation int,executor_instance_id uuid,owner_id uuid,worker_generation int,deadline_at timestamp,state text,primary key(job_id,attempt_generation));
 CREATE TABLE IF NOT EXISTS sources(id int primary key,name text);
 CREATE TABLE IF NOT EXISTS articles(id int primary key,source_id int,title text,summary text,content text,url text,fetched_at timestamp,processing_state text,is_advertisement boolean);
 CREATE TABLE IF NOT EXISTS categories(id int primary key,name text,display_name text);
 CREATE TABLE IF NOT EXISTS article_categories(article_id int,category_id int,confidence double precision);
 CREATE TABLE settings(key text primary key,value json not null,updated_at timestamp);
 CREATE TABLE ai_usage_tracking(id serial primary key,domain text,analysis_type text,tokens_used integer,credits_cost numeric(10,4),analysis_result json,created_at timestamp);
 CREATE TABLE digest_usage_fixture (model text not null);
 CREATE TABLE IF NOT EXISTS editorial_digest_editions(id uuid primary key,edition_date date,locale text,policy_version text,snapshot_hash text,snapshot jsonb,edition jsonb,rendered jsonb,provider_usage jsonb,job_id uuid,attempt_generation int,created_at timestamp default timezone('UTC',now()),unique(job_id,attempt_generation,locale));
 CREATE TABLE IF NOT EXISTS digest_delivery_intents(id uuid primary key,edition_id uuid,destination_hash char(64),purpose text,render_hash char(64),job_id uuid,attempt_generation int,domain_instance_id uuid,state text,parts_sent int,rich_message bool,unique(edition_id,destination_hash,purpose));
 CREATE TABLE IF NOT EXISTS digest_delivery_parts(intent_id uuid,method text,part_index int,payload_hash char(64),job_id uuid,attempt_generation int,domain_instance_id uuid,state text,message_id bigint,status_code int,retry_after_seconds double precision,updated_at timestamp default now(),primary key(intent_id,method,part_index))`
	if _, err = pool.Exec(ctx, ddl); err != nil {
		t.Fatal("fixture schema setup failed")
	}
	connection := func() *pgx.Conn {
		c, e := pgx.Connect(ctx, dsn)
		if e != nil {
			t.Fatal("fixture lock connection unavailable")
		}
		if _, e = c.Exec(ctx, "SET search_path TO "+schema); e != nil {
			t.Fatal("fixture search path unavailable")
		}
		return c
	}
	worker := connection()
	defer worker.Close(ctx)
	executor := connection()
	defer executor.Close(ctx)
	var wp, ep int
	if err = worker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&wp); err != nil {
		t.Fatal("worker pid unavailable")
	}
	if err = executor.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&ep); err != nil {
		t.Fatal("executor pid unavailable")
	}
	if _, err = worker.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()))`); err != nil {
		t.Fatal("worker lock unavailable")
	}
	if _, err = executor.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # 1162757938)`); err != nil {
		t.Fatal("executor lock unavailable")
	}
	const job = "00000000-0000-0000-0000-000000000001"
	const owner = "00000000-0000-0000-0000-000000000002"
	const execID = "00000000-0000-0000-0000-000000000003"
	const intent = "00000000-0000-0000-0000-000000000004"
	edition := "00000000-0000-0000-0000-000000000005"
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	if _, err = pool.Exec(ctx, `INSERT INTO background_worker_status VALUES(1,$1,1,'running',$2,timezone('UTC',now()))`, owner, wp); err != nil {
		t.Fatal("fixture worker failed")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs VALUES($1,'running',1,1,'news_digest','{}')`, job); err != nil {
		t.Fatal("fixture job failed")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_job_executions VALUES($1,1,$2,$3,1,$4,'running')`, job, execID, owner, deadline); err != nil {
		t.Fatal("fixture receipt failed")
	}

	fence := authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: execID, Kind: "news_digest", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: wp, ExecutorBackendPID: ep, Deadline: deadline, Payload: []byte(`{}`)}
	if err = fence.Check(ctx); err != nil {
		t.Fatal("valid fence rejected")
	}
	if _, err = pool.Exec(ctx, `TRUNCATE article_categories,articles,categories,sources; INSERT INTO sources VALUES(1,'Fixture'); INSERT INTO categories VALUES(3,'Tech','Tech'),(4,'Business','Business');
       INSERT INTO articles(id,source_id,title,summary,content,url,fetched_at,processing_state,is_advertisement) VALUES(41,1,'Title','Summary','Content','https://source.test/41',timezone('UTC',now()),'completed',false), (42,1,'Ad','Summary','Content','https://source.test/42',timezone('UTC',now()),'completed',true), (43,1,'Pending','Summary','Content','https://source.test/43',timezone('UTC',now()),'pending',false), (44,1,'Retry','Summary','Content','https://source.test/44',timezone('UTC',now()),'retry_wait',false), (45,1,'Rejected','Summary','Content','https://source.test/45',timezone('UTC',now()),'rejected',false), (46,1,'Exhausted','Summary','Content','https://source.test/46',timezone('UTC',now()),'exhausted',false), (47,1,'Uncategorized','Summary','Content','https://source.test/47',timezone('UTC',now()),'completed',false);
       INSERT INTO article_categories VALUES(41,3,0.9),(41,4,0.9),(42,3,1.0),(43,3,1),(44,3,1),(45,3,1),(46,3,1)`); err != nil {
		t.Fatal("snapshot fixture failed")
	}
	wf := digestworkflow.Workflow{Pool: pool, Fence: fence, SiteBase: "https://news.fixture.test"}
	snapshot, _, err := wf.Snapshot(ctx, time.Now(), edition)
	if err != nil || len(snapshot.Articles) != 1 || snapshot.Articles[0].ID != "41" || snapshot.Articles[0].CategoryID != "3" || snapshot.Articles[0].CategoryName != "Технологии" {
		t.Fatal("snapshot parity failed")
	}
	var providerCalls atomic.Int32
	var telegramCalls atomic.Int32
	var lastDestination atomic.Value
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/sendRichMessage") || strings.HasSuffix(r.URL.Path, "/sendMessage") {
			telegramCalls.Add(1)
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			if destination, ok := payload["chat_id"].(string); ok {
				lastDestination.Store(destination)
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]int{"message_id": 123}})
			return
		}
		providerCalls.Add(1)
		var request struct {
			Contents []struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"contents"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Contents) == 0 || len(request.Contents[0].Parts) == 0 {
			w.WriteHeader(400)
			return
		}
		prompt := request.Contents[0].Parts[0].Text
		if strings.Contains(prompt, "Content") {
			t.Error("original content entered provider prompt")
		}
		pieces := strings.SplitN(prompt, "СНИМОК (недоверенные данные):\n", 2)
		if len(pieces) != 2 {
			w.WriteHeader(400)
			return
		}
		var snap struct {
			Articles []struct {
				ID    string `json:"id"`
				Title string `json:"title"`
				Spans []struct {
					ID string `json:"id"`
				} `json:"evidence_spans"`
			} `json:"articles"`
		}
		if json.Unmarshal([]byte(pieces[1]), &snap) != nil || len(snap.Articles) != 1 || len(snap.Articles[0].Spans) == 0 {
			w.WriteHeader(400)
			return
		}
		a := snap.Articles[0]
		draft, _ := json.Marshal(map[string]any{"main_stories": []any{map[string]any{"title": a.Title, "summary": "Summary", "primary_article_id": a.ID, "article_ids": []string{a.ID}}}, "brief_stories": []any{}, "article_stories": map[string]any{a.ID: map[string]string{"brief_summary": "Summary", "span_id": a.Spans[0].ID}}})
		json.NewEncoder(w).Encode(map[string]any{"candidates": []any{map[string]any{"finishReason": "STOP", "content": map[string]any{"parts": []any{map[string]string{"text": string(draft)}}}}}, "usageMetadata": map[string]int{"promptTokenCount": 1, "candidatesTokenCount": 1, "cachedContentTokenCount": 0, "thoughtsTokenCount": 0, "totalTokenCount": 2}})
	}))
	defer provider.Close()
	wf.RecordUsage = func(ctx context.Context, tx pgx.Tx, u editorial.ProviderUsage) error {
		_, err := tx.Exec(ctx, `INSERT INTO digest_usage_fixture(model) VALUES($1)`, u.Model)
		return err
	}
	wf.Provider = editorial.ProviderConfig{APIKey: "fixture", Model: "fixture-model", Endpoint: provider.URL, Client: provider.Client()}
	count, err := wf.Generate(ctx, job, 1, "00000000-0000-0000-0000-000000000006", time.Now())
	if err != nil || count != 1 || providerCalls.Load() != 1 {
		t.Fatalf("generation failed: count=%d err=%v", count, err)
	}
	count, err = wf.Generate(ctx, job, 1, "00000000-0000-0000-0000-000000000006", time.Now())
	if err != nil || count != 1 || providerCalls.Load() != 1 {
		t.Fatal("edition was regenerated")
	}
	var usageCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM digest_usage_fixture`).Scan(&usageCount); err != nil || usageCount != 1 {
		t.Fatalf("digest usage count=%d err=%v", usageCount, err)
	}
	var persisted bool
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM editorial_digest_editions WHERE job_id=$1 AND rendered->>'rich_html'<>'' AND edition->>'locale'='ru')`, job).Scan(&persisted); err != nil || !persisted {
		t.Fatal("edition not readable")
	}
	// Exercise authenticated native accept/status through real PostgreSQL and
	// a local fake Telegram provider, including repeat intent suppression.
	inputPrice, outputPrice, cachedPrice := 1.0, 2.0, 0.5
	runtime := airuntime.NewServer(airuntime.Config{Prices: map[string]airuntime.ModelPrice{"gemini/fixture-model": {Input: &inputPrice, Output: &outputPrice, CachedInput: &cachedPrice}}, Model: "fixture-model", Token: "01234567890123456789012345678901", RPS: 3, Endpoint: provider.URL + "/v1/models", APIKey: "fixture", DigestModel: "fixture-model", SiteBase: "https://news.fixture.test", TelegramToken: "fixture", TelegramMain: "fixture-main", TelegramService: "fixture-service", TelegramEndpoint: provider.URL + "/telegram", TelegramRich: true}, pool)
	call := func(operation, operationID string, input map[string]any) map[string]any {
		raw, _ := json.Marshal(input)
		sum := sha256.Sum256(raw)
		fingerprint := hex.EncodeToString(sum[:])
		instance := "00000000-0000-0000-0000-000000000007"
		body := map[string]any{"version": 1, "request_id": "fixture", "deadline_at": deadline.Format(time.RFC3339Nano), "input": map[string]any{"operation": operation, "operation_id": operationID, "domain_instance_id": instance, "envelope": map[string]any{"version": 1, "job_id": job, "attempt_generation": 1, "owner_id": owner, "generation": 1, "backend_pid": wp, "executor_instance_id": execID, "deadline_at": deadline.Format(time.RFC3339Nano)}, "input_fingerprint": fingerprint, "input": input, "kind": "news_digest", "payload": map[string]any{}, "executor_backend_pid": ep}}
		request, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/v2/digest/operations", bytes.NewReader(request))
		req.Header.Set("Authorization", "Bearer "+"01234567890123456789012345678901")
		out := httptest.NewRecorder()
		runtime.Handler().ServeHTTP(out, req)
		if out.Code != 200 {
			t.Fatalf("accept failed: %d", out.Code)
		}
		key := job + "/1/" + operation + "/" + operationID
		values := url.Values{"key": []string{key}, "domain_instance_id": []string{instance}}
		for i := 0; i < 200; i++ {
			request := httptest.NewRequest(http.MethodGet, "/v2/digest/operations?"+values.Encode(), nil)
			request.Header.Set("Authorization", "Bearer "+"01234567890123456789012345678901")
			rec := httptest.NewRecorder()
			runtime.Handler().ServeHTTP(rec, request)
			var response struct {
				Result struct {
					State  string         `json:"state"`
					Result map[string]any `json:"result"`
				} `json:"result"`
			}
			if json.Unmarshal(rec.Body.Bytes(), &response) != nil {
				t.Fatal("invalid operation receipt")
			}
			if response.Result.State == "succeeded" {
				return response.Result.Result
			}
			if response.Result.State == "failed" {
				t.Fatal("native operation failed")
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("native operation timed out")
		return nil
	}

	// Exercise the actual private runtime generation and accounting, using the
	// same deterministic edition identity after removing only the fixture row.
	if _, err = pool.Exec(ctx, `DELETE FROM editorial_digest_editions WHERE job_id=$1`, job); err != nil {
		t.Fatal(err)
	}
	generated := call("summaries_generate", "00000000-0000-0000-0000-000000000006", map[string]any{})
	if generated["summaries_generated"] != float64(1) || providerCalls.Load() != 2 {
		t.Fatalf("runtime generation=%v calls=%d", generated, providerCalls.Load())
	}
	var exactCost *float64
	var providerName, requested string
	var revision int
	var total int
	err = pool.QueryRow(ctx, `SELECT (analysis_result->>'cost_estimate')::float8,analysis_result->>'provider',analysis_result->>'requested_model',(analysis_result->>'profile_revision')::int,tokens_used FROM ai_usage_tracking WHERE analysis_type='digest'`).Scan(&exactCost, &providerName, &requested, &revision, &total)
	if err != nil || exactCost == nil || *exactCost != .000003 || providerName != "gemini" || requested != "fixture-model" || revision != 0 || total != 2 {
		t.Fatalf("digest accounting cost=%v provider=%q requested=%q revision=%d total=%d err=%v", exactCost, providerName, requested, revision, total, err)
	}
	var editionCost *float64
	var editionProvider, editionModel string
	if err = pool.QueryRow(ctx, `SELECT (provider_usage->>'cost')::float8,provider_usage->>'provider',provider_usage->>'requested_model' FROM editorial_digest_editions WHERE job_id=$1`, job).Scan(&editionCost, &editionProvider, &editionModel); err != nil || editionCost == nil || *editionCost != .000003 || editionProvider != "gemini" || editionModel != "fixture-model" {
		t.Fatalf("edition cost=%v provider=%q model=%q err=%v", editionCost, editionProvider, editionModel, err)
	}
	generated = call("summaries_generate", "00000000-0000-0000-0000-000000000012", map[string]any{})
	if generated["summaries_generated"] != float64(1) || providerCalls.Load() != 2 {
		t.Fatalf("runtime regenerated: %v calls=%d", generated, providerCalls.Load())
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ai_usage_tracking WHERE analysis_type='digest'`).Scan(&usageCount); err != nil || usageCount != 1 {
		t.Fatalf("runtime usage count=%d err=%v", usageCount, err)
	}
	prepared := call("digest_prepare", "00000000-0000-0000-0000-000000000008", map[string]any{})
	token, ok := prepared["prepared_token"].(string)
	if !ok {
		t.Fatal("missing prepared token")
	}
	published := call("digest_publish", "00000000-0000-0000-0000-000000000009", map[string]any{"prepared_token": token})
	if published["success"] != true || telegramCalls.Load() != 1 {
		t.Fatal("fake Telegram delivery failed")
	}
	prepared = call("digest_prepare", "00000000-0000-0000-0000-000000000010", map[string]any{})
	token = prepared["prepared_token"].(string)
	published = call("digest_publish", "00000000-0000-0000-0000-000000000011", map[string]any{"prepared_token": token})
	if published["success"] != true || telegramCalls.Load() != 1 {
		t.Fatal("confirmed delivery was sent twice")
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ai_usage_tracking WHERE analysis_type='digest'`).Scan(&usageCount); err != nil || usageCount != 1 || providerCalls.Load() != 2 {
		t.Fatalf("delivery repeated usage: count=%d calls=%d err=%v", usageCount, providerCalls.Load(), err)
	}
	const childJob = "00000000-0000-0000-0000-000000000015"
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs VALUES($1,'running',1,1,'news_digest','{}')`, childJob); err != nil {
		t.Fatal("child job fixture failed")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_job_executions VALUES($1,1,$2,$3,1,$4,'running')`, childJob, execID, owner, deadline); err != nil {
		t.Fatal("child receipt fixture failed")
	}

	edition = digestworkflow.EditionID(job, 1, "00000000-0000-0000-0000-000000000006")
	store := telegramdelivery.PostgresStore{Pool: pool, Fence: fence, JobID: job, AttemptGeneration: 1, DomainInstanceID: "00000000-0000-0000-0000-000000000007"}
	hash := strings.Repeat("a", 64)
	got, err := store.ResolveIntent(ctx, intent, edition, hash, hash)
	if err != nil || got.ID != intent {
		t.Fatal("intent resolution failed")
	}
	part := telegramdelivery.Part{Intent: intent, Method: "sendRichMessage", Index: 0, PayloadHash: hash}
	state, err := store.Reserve(ctx, part)
	if err != nil || state != telegramdelivery.New {
		t.Fatal("reservation failed")
	}
	state, err = store.Reserve(ctx, part)
	if err != nil || state != telegramdelivery.InFlight {
		t.Fatal("sending was replayable")
	}
	if err = store.Finish(ctx, part, telegramdelivery.Result{State: telegramdelivery.Sent, MessageID: 42, StatusCode: 200}); err != nil {
		t.Fatal("receipt finalization failed")
	}
	state, err = store.Reserve(ctx, part)
	if err != nil || state != telegramdelivery.AlreadySent {
		t.Fatal("sent receipt was replayable")
	}
	if err = store.Complete(ctx, intent, 1, true); err != nil {
		t.Fatal("intent completion failed")
	}
	secondFence := fence
	secondFence.JobID = childJob
	second := telegramdelivery.PostgresStore{Pool: pool, Fence: secondFence, JobID: childJob, AttemptGeneration: 1, DomainInstanceID: "00000000-0000-0000-0000-000000000008"}
	completed, err := second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000017", edition, hash, hash)
	if err != nil || !completed.Completed || completed.PartsSent != 1 || !completed.RichMessage {
		t.Fatal("completed intent was not reusable")
	}

	createIntent := func(id, destination string) telegramdelivery.Intent {
		value, err := store.ResolveIntent(ctx, id, edition, strings.Repeat(destination, 64), hash)
		if err != nil {
			t.Fatal("create takeover fixture failed")
		}
		return value
	}
	// An untouched intent can be claimed; the old, still-fenced owner may no
	// longer reserve, finish, or complete it.
	empty := createIntent("00000000-0000-0000-0000-000000000020", "c")
	if _, err = second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000021", edition, strings.Repeat("c", 64), hash); err != nil {
		t.Fatal("empty intent takeover failed")
	}
	emptyPart := telegramdelivery.Part{Intent: empty.ID, Method: "sendMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, emptyPart); !errors.Is(err, telegramdelivery.ErrUnknown) {
		t.Fatal("old owner reserved after takeover")
	}
	if err = store.Finish(ctx, emptyPart, telegramdelivery.Result{State: telegramdelivery.Sent}); !errors.Is(err, telegramdelivery.ErrUnknown) {
		t.Fatal("old owner finalized after takeover")
	}
	if err = store.Complete(ctx, empty.ID, 1, false); !errors.Is(err, telegramdelivery.ErrUnknown) {
		t.Fatal("old owner completed after takeover")
	}
	if state, err = second.Reserve(ctx, emptyPart); err != nil || state != telegramdelivery.New {
		t.Fatal("new owner cannot reserve empty intent")
	}

	// A child/domain restart with the same job attempt is a distinct owner.
	restarted := createIntent("00000000-0000-0000-0000-000000000026", "9")
	replacement := store
	replacement.DomainInstanceID = "00000000-0000-0000-0000-000000000027"
	if _, err = replacement.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000028", edition, strings.Repeat("9", 64), hash); err != nil {
		t.Fatal("new instance takeover failed")
	}
	restartedPart := telegramdelivery.Part{Intent: restarted.ID, Method: "sendMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, restartedPart); !errors.Is(err, telegramdelivery.ErrUnknown) {
		t.Fatal("old instance reserved after takeover")
	}
	if state, err = replacement.Reserve(ctx, restartedPart); err != nil || state != telegramdelivery.New {
		t.Fatal("replacement instance could not reserve")
	}

	// A 429 is terminal in the first call and until its saved cooldown has
	// elapsed. A later owner can then reserve once, without an automatic loop.
	limited := createIntent("00000000-0000-0000-0000-000000000022", "d")
	limitedPart := telegramdelivery.Part{Intent: limited.ID, Method: "sendRichMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, limitedPart); err != nil {
		t.Fatal("rate limit reservation failed")
	}
	delay := 60.0
	if err = store.Finish(ctx, limitedPart, telegramdelivery.Result{State: telegramdelivery.Rejected, StatusCode: 429, RetryAfterSeconds: &delay}); err != nil {
		t.Fatal("rate limit receipt failed")
	}
	if _, err = second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000023", edition, strings.Repeat("d", 64), hash); err != nil {
		t.Fatal("rejected intent takeover failed")
	}
	if state, err = second.Reserve(ctx, limitedPart); err != nil || state != telegramdelivery.RateLimited {
		t.Fatal("retry after was not respected")
	}
	if _, err = pool.Exec(ctx, `UPDATE digest_delivery_parts SET updated_at=timezone('UTC',now())-interval '61 seconds' WHERE intent_id=$1`, limited.ID); err != nil {
		t.Fatal("cooldown fixture failed")
	}
	if state, err = second.Reserve(ctx, limitedPart); err != nil || state != telegramdelivery.New {
		t.Fatal("later owner could not reserve 429")
	}
	if state, err = second.Reserve(ctx, limitedPart); err != nil || state != telegramdelivery.InFlight {
		t.Fatal("429 reservation was replayable")
	}
	if err = second.Finish(ctx, limitedPart, telegramdelivery.Result{State: telegramdelivery.Rejected, StatusCode: 429}); err != nil {
		t.Fatal("new owner rejection failed")
	}
	if state, err = second.Reserve(ctx, limitedPart); err != nil || state != telegramdelivery.RateLimited {
		t.Fatal("same owner retried rejected 429")
	}

	// A 429 protects every part, even if a later attempt changes the mode.
	for i, methods := range []struct{ previous, next string }{{"sendRichMessage", "sendMessage"}, {"sendMessage", "sendRichMessage"}} {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", 40+i)
		destination := strings.Repeat(string(rune('1'+i)), 64)
		value, err := store.ResolveIntent(ctx, id, edition, destination, hash)
		if err != nil {
			t.Fatal("mode cooldown intent failed")
		}
		oldPart := telegramdelivery.Part{Intent: value.ID, Method: methods.previous, Index: 0, PayloadHash: hash}
		if _, err = store.Reserve(ctx, oldPart); err != nil {
			t.Fatal("mode cooldown reservation failed")
		}
		delay := 60.0
		if err = store.Finish(ctx, oldPart, telegramdelivery.Result{State: telegramdelivery.Rejected, StatusCode: 429, RetryAfterSeconds: &delay}); err != nil {
			t.Fatal("mode cooldown receipt failed")
		}
		proposed := fmt.Sprintf("00000000-0000-0000-0000-%012d", 50+i)
		if _, err = second.ResolveIntent(ctx, proposed, edition, destination, hash); err != nil {
			t.Fatal("mode cooldown takeover failed")
		}
		nextPart := telegramdelivery.Part{Intent: value.ID, Method: methods.next, Index: 0, PayloadHash: hash, Payload: map[string]any{"chat_id": "fixture"}}
		delivery := telegramdelivery.Delivery{Store: second, Sender: telegramdelivery.Transport{Client: provider.Client(), Endpoint: provider.URL + "/telegram", Token: "fixture"}, Authority: secondFence}
		before := telegramCalls.Load()
		if _, err = delivery.SendPart(ctx, nextPart); !errors.Is(err, telegramdelivery.ErrRejected) || telegramCalls.Load() != before {
			t.Fatal("mode switch bypassed cooldown")
		}
		if _, err = pool.Exec(ctx, `UPDATE digest_delivery_parts SET updated_at=timezone('UTC',now())-interval '61 seconds' WHERE intent_id=$1`, value.ID); err != nil {
			t.Fatal("mode cooldown elapsed fixture failed")
		}
		if delivered, sendErr := delivery.SendPart(ctx, nextPart); sendErr != nil || delivered != telegramdelivery.Sent || telegramCalls.Load() != before+1 {
			t.Fatal("mode switch did not send after cooldown")
		}
	}

	huge := createIntent("00000000-0000-0000-0000-000000000060", "3")
	hugePart := telegramdelivery.Part{Intent: huge.ID, Method: "sendMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, hugePart); err != nil {
		t.Fatal("large cooldown fixture reservation failed")
	}
	maximum := math.MaxFloat64
	if err = store.Finish(ctx, hugePart, telegramdelivery.Result{State: telegramdelivery.Rejected, StatusCode: 429, RetryAfterSeconds: &maximum}); err != nil {
		t.Fatal("large cooldown fixture receipt failed")
	}
	if _, err = second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000061", edition, strings.Repeat("3", 64), hash); err != nil {
		t.Fatal("large cooldown takeover failed")
	}
	if state, err = second.Reserve(ctx, telegramdelivery.Part{Intent: huge.ID, Method: "sendRichMessage", Index: 0, PayloadHash: hash}); err != nil || state != telegramdelivery.RateLimited {
		t.Fatal("large finite cooldown was not safely enforced")
	}

	// Explicit rich rejection is retained for safe plain fallback after takeover.
	rejected := createIntent("00000000-0000-0000-0000-000000000024", "e")
	rejectedPart := telegramdelivery.Part{Intent: rejected.ID, Method: "sendRichMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, rejectedPart); err != nil {
		t.Fatal("rich fixture reservation failed")
	}
	if err = store.Finish(ctx, rejectedPart, telegramdelivery.Result{State: telegramdelivery.Rejected, StatusCode: 400}); err != nil {
		t.Fatal("rich fixture rejection failed")
	}
	if _, err = second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000025", edition, strings.Repeat("e", 64), hash); err != nil {
		t.Fatal("rejected rich takeover failed")
	}
	if state, err = second.Reserve(ctx, rejectedPart); err != nil || state != telegramdelivery.PreviouslyRejected {
		t.Fatal("rich rejection did not permit fallback")
	}
	if state, err = second.Reserve(ctx, telegramdelivery.Part{Intent: rejected.ID, Method: "sendMessage", Index: 0, PayloadHash: hash}); err != nil || state != telegramdelivery.New {
		t.Fatal("plain fallback reservation failed")
	}

	// Sending and explicitly unknown outcomes never permit a new owner.
	for i, kind := range []string{"sending", "unknown"} {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", 30+i)
		destination := strings.Repeat(string(rune('f'+i)), 64)
		// SHA-256 text must be lowercase hexadecimal.
		if kind == "unknown" {
			destination = strings.Repeat("0", 64)
		}
		pending, err := store.ResolveIntent(ctx, id, edition, destination, hash)
		if err != nil {
			t.Fatal("uncertain intent fixture failed")
		}
		pendingPart := telegramdelivery.Part{Intent: pending.ID, Method: "sendMessage", Index: 0, PayloadHash: hash}
		if _, err = store.Reserve(ctx, pendingPart); err != nil {
			t.Fatal("uncertain part fixture failed")
		}
		if kind == "unknown" {
			if err = store.Finish(ctx, pendingPart, telegramdelivery.Result{State: telegramdelivery.Unknown}); err != nil {
				t.Fatal("unknown receipt fixture failed")
			}
		}
		before := telegramCalls.Load()
		_, err = second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000032", edition, destination, hash)
		if !errors.Is(err, telegramdelivery.ErrUnknown) || telegramCalls.Load() != before {
			t.Fatal("uncertain intent takeover was allowed")
		}
	}
	partialID := "00000000-0000-0000-0000-000000000018"
	destinationHash := strings.Repeat("b", 64)
	partial, err := store.ResolveIntent(ctx, partialID, edition, destinationHash, hash)
	if err != nil {
		t.Fatal("partial intent failed")
	}
	rich := telegramdelivery.Part{Intent: partial.ID, Method: "sendRichMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, rich); err != nil {
		t.Fatal("rich reservation failed")
	}
	if err = store.Finish(ctx, rich, telegramdelivery.Result{State: telegramdelivery.Rejected, StatusCode: 400}); err != nil {
		t.Fatal("rich rejection failed")
	}
	first := telegramdelivery.Part{Intent: partial.ID, Method: "sendMessage", Index: 0, PayloadHash: hash}
	if _, err = store.Reserve(ctx, first); err != nil {
		t.Fatal("first part reservation failed")
	}
	if err = store.Finish(ctx, first, telegramdelivery.Result{State: telegramdelivery.Sent, MessageID: 43, StatusCode: 200}); err != nil {
		t.Fatal("first part receipt failed")
	}
	before := telegramCalls.Load()
	_, err = second.ResolveIntent(ctx, "00000000-0000-0000-0000-000000000019", edition, destinationHash, hash)
	if !errors.Is(err, telegramdelivery.ErrUnknown) || telegramCalls.Load() != before {
		t.Fatal("new attempt resumed incomplete delivery")
	}
	if _, err = pool.Exec(ctx, `DROP TABLE settings`); err != nil {
		t.Fatal("settings failure fixture failed")
	}
	alertBody, _ := json.Marshal(map[string]any{"version": 1, "request_id": "fixture-alert", "deadline_at": deadline.Format(time.RFC3339Nano), "channel": "service", "method": "sendMessage", "text": "synthetic alert"})
	alertReq := httptest.NewRequest(http.MethodPost, "/v1/telegram/send", bytes.NewReader(alertBody))
	alertReq.Header.Set("Authorization", "Bearer "+"01234567890123456789012345678901")
	alertOut := httptest.NewRecorder()
	beforeAlert := telegramCalls.Load()
	runtime.Handler().ServeHTTP(alertOut, alertReq)
	if alertOut.Code != 200 || telegramCalls.Load() != beforeAlert+1 || lastDestination.Load() != "fixture-service" {
		t.Fatal("alert fallback failed")
	}
	if _, err = pool.Exec(ctx, `UPDATE background_worker_status SET generation=2`); err != nil {
		t.Fatal("fixture stale setup failed")
	}
	if fence.Check(ctx) == nil {
		t.Fatal("stale fence accepted")
	}
}
