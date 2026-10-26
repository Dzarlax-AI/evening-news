package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pipelinePG struct {
	admin  *pgx.Conn
	pool   *pgxpool.Pool
	db     *store.Store
	schema string
	seq    int64
}

type shutdownTestRunner struct {
	started  chan struct{}
	returned chan struct{}
}

func (r *shutdownTestRunner) Run(ctx context.Context, _ store.Fence) (map[string]any, error) {
	close(r.started)
	<-ctx.Done()
	close(r.returned)
	return nil, ctx.Err()
}

func newPipelinePG(t *testing.T, omitTechCategory bool) *pipelinePG {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for Go article processing PostgreSQL integration tests")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("article processing PostgreSQL tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect synthetic PostgreSQL: %v", err)
	}
	schema := fmt.Sprintf("processing_pipeline_test_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		_ = admin.Close(context.Background())
		t.Fatalf("create isolated PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = admin.Exec(cleanup, "DROP SCHEMA "+q+" CASCADE")
		_ = admin.Close(cleanup)
	})
	statements := []string{
		`CREATE TABLE ` + q + `.article_processing_runs(
			id uuid PRIMARY KEY, job_id uuid NOT NULL, attempt_generation integer NOT NULL,
			state text NOT NULL, deadline_at timestamp NOT NULL, envelope_sha256 text NOT NULL,
			result jsonb NOT NULL DEFAULT '{}'::jsonb, error_code text, updated_at timestamp NOT NULL DEFAULT now(),
			finished_at timestamp)`,
		`CREATE TABLE ` + q + `.background_jobs(
			id uuid PRIMARY KEY, kind text NOT NULL, payload jsonb NOT NULL,
			state text NOT NULL, attempt_generation integer NOT NULL)`,
		`CREATE TABLE ` + q + `.sources(id bigint PRIMARY KEY,source_type text NOT NULL,name text NOT NULL)`,
		`CREATE TABLE ` + q + `.articles(
			id bigint PRIMARY KEY,title text NOT NULL,url text NOT NULL,content text,summary text,
			source_id bigint,published_at timestamp,summary_processed boolean NOT NULL DEFAULT false,
			category_processed boolean NOT NULL DEFAULT false,ad_processed boolean NOT NULL DEFAULT false,
			is_advertisement boolean,ad_confidence double precision,ad_type text,ad_reasoning text,
			processing_attempts integer NOT NULL DEFAULT 0,
			processing_state text NOT NULL DEFAULT 'pending',next_attempt_at timestamp,
			last_processing_error text,processed boolean NOT NULL DEFAULT false,image_url text,
			media_files jsonb NOT NULL DEFAULT '[]'::jsonb,hash_content text)`,
		`CREATE TABLE ` + q + `.article_processing_items(
			run_id uuid NOT NULL,article_id bigint NOT NULL,article_attempt integer NOT NULL,
			input_fingerprint text NOT NULL,claim_token uuid NOT NULL,state text NOT NULL,
			result jsonb NOT NULL,updated_at timestamp NOT NULL DEFAULT now(),
			PRIMARY KEY(run_id,article_id))`,
		`CREATE TABLE ` + q + `.categories(id serial PRIMARY KEY,name text NOT NULL UNIQUE)`,
		`CREATE TABLE ` + q + `.article_categories(
			article_id bigint NOT NULL,category_id integer NOT NULL,confidence double precision NOT NULL,
			ai_category text NOT NULL,created_at timestamp NOT NULL DEFAULT now(),
			UNIQUE(article_id,category_id))`,
		`CREATE TABLE ` + q + `.category_mapping(
			ai_category text NOT NULL,fixed_category text NOT NULL,is_active boolean NOT NULL DEFAULT true,
			usage_count integer NOT NULL DEFAULT 0,last_used timestamp)`,
		`CREATE FUNCTION ` + q + `.fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp)
			RETURNS TABLE(run_id uuid,run_state text) LANGUAGE sql AS $$
			SELECT r.id,r.state FROM ` + q + `.article_processing_runs r WHERE r.id=$1
			$$`,
	}
	if !omitTechCategory {
		statements = append(statements, `INSERT INTO `+q+`.categories(name) VALUES('Tech'),('AI'),('Other')`)
	} else {
		statements = append(statements, `INSERT INTO `+q+`.categories(name) VALUES('Other')`)
	}
	for _, statement := range statements {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatalf("prepare isolated processing schema: %v", err)
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns, cfg.MinConns = 8, 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &pipelinePG{admin: admin, pool: pool, db: &store.Store{Pool: pool}, schema: q}
}

func (pg *pipelinePG) seedArticle(t *testing.T, id int64) {
	t.Helper()
	_, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.articles
		(id,title,url,content,summary,processing_attempts,processing_state,summary_processed,
		 category_processed,ad_processed,is_advertisement,processed)
		VALUES($1,'Synthetic research report on local systems','https://example.test/article',
		 'Synthetic researchers collected measurements from several local systems over a controlled observation period. The report compares repeatable outcomes, documents the input conditions, and explains how the findings were independently reviewed. The observations describe changes across multiple synthetic regions without making claims about any real location or organization. Each section lists methods, results, and limitations so readers can understand the recorded evidence.',
		 '',0,'pending',false,false,false,false,false)`, id)
	if err != nil {
		t.Fatal(err)
	}
}

func (pg *pipelinePG) newRun(t *testing.T, articleID int64) store.Fence {
	return pg.newRunWithPayload(t, articleID, "news_processing", map[string]any{"article_ids": []int64{articleID}})
}

func (pg *pipelinePG) newRunWithPayload(t *testing.T, articleID int64, kind string, payloadValue map[string]any) store.Fence {
	t.Helper()
	pg.seq++
	jobID := fmt.Sprintf("20000000-0000-4000-8000-%012x", pg.seq)
	e := store.Envelope{
		Version: 1, JobID: jobID, AttemptGeneration: 1,
		OwnerID: "30000000-0000-4000-8000-000000000001", Generation: 1,
		BackendPID: 1, ExecutorInstanceID: "40000000-0000-4000-8000-000000000001",
		DeadlineAt: time.Now().Add(time.Minute).UTC(),
	}
	f := store.NewFence(e)
	payload, _ := json.Marshal(payloadValue)
	_, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.background_jobs(id,kind,payload,state,attempt_generation)
		VALUES($1::uuid,$2,$3::jsonb,'running',1)`, jobID, kind, payload)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.article_processing_runs
		(id,job_id,attempt_generation,state,deadline_at,envelope_sha256)
		VALUES($1::uuid,$2::uuid,1,'running',$3,$4)`, f.RunID, jobID, e.DeadlineAt, f.EnvelopeSHA256)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type aiEnvelope struct {
	RunID            string         `json:"run_id"`
	ArticleID        int64          `json:"article_id"`
	ArticleAttempt   int            `json:"article_attempt"`
	InputFingerprint string         `json:"input_fingerprint"`
	Operation        string         `json:"operation"`
	Input            map[string]any `json:"input"`
}

func analysisResponse(r aiEnvelope, result map[string]any) map[string]any {
	return map[string]any{"version": 1, "run_id": r.RunID, "article_id": r.ArticleID,
		"article_attempt": r.ArticleAttempt, "input_fingerprint": r.InputFingerprint,
		"operation": r.Operation, "result": result}
}

func answerFixtureAuxAI(t *testing.T, w http.ResponseWriter, request aiEnvelope) bool {
	t.Helper()
	switch request.Operation {
	case "extraction_context":
		if len(request.Input) != 1 || request.Input["url"] != "https://example.test/article" {
			http.Error(w, "extraction context input does not match the callback contract", http.StatusUnprocessableEntity)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, map[string]any{"selector": "article", "method": "source_content"}))
		return true
	case "extraction_feedback":
		allowed := map[string]bool{"url": true, "method": true, "success": true, "observations": true,
			"selector_used": true, "content_length": true, "extraction_time_ms": true, "quality_score": true, "error_code": true}
		for key := range request.Input {
			if !allowed[key] {
				http.Error(w, "extraction feedback input does not match the callback contract", http.StatusUnprocessableEntity)
				return true
			}
		}
		if request.Input["url"] != "https://example.test/article" || request.Input["method"] != "source_content" || request.Input["success"] != true {
			http.Error(w, "extraction feedback required fields are invalid", http.StatusUnprocessableEntity)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, map[string]any{"recorded": true}))
		return true
	case "analyze":
		if request.Input["url"] != "https://example.test/article" {
			http.Error(w, "analysis URL is invalid", http.StatusUnprocessableEntity)
			return true
		}
		return false
	default:
		http.Error(w, "unexpected synthetic AI operation", http.StatusUnprocessableEntity)
		return true
	}
}

const syntheticExtractedArticle = "Synthetic researchers measured a coordinated set of systems during a controlled observation period. The independent report records the methods, input conditions, and repeatable results collected by the fixture team. Separate sections describe how measurements changed under each synthetic configuration and explain the limits of the observation. Reviewers compared the recorded evidence with a second run and documented where the results agreed. The report makes no claims about real organizations, products, people, locations, or events. Its conclusion summarizes only the synthetic measurements and their stated uncertainty. Additional notes describe the instruments, sampling interval, validation steps, and data handling used in this fictional exercise."

func newFixtureExtractor(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/extract" || r.Header.Get("Authorization") != "Bearer fixture-extractor-token" {
			http.Error(w, "unexpected synthetic extractor request", http.StatusBadRequest)
			return
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid synthetic extractor JSON", http.StatusBadRequest)
			return
		}
		requestID, _ := request["request_id"].(string)
		rawURL, _ := request["url"].(string)
		if requestID == "" || rawURL != "https://example.test/article" || request["strategy"] != "auto" || request["source_type"] != "article" || request["use_browser"] != false {
			http.Error(w, "synthetic extractor contract mismatch", http.StatusBadRequest)
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": requestID, "original_url": rawURL,
			"final_url": rawURL, "outcome": "success",
			"result": map[string]any{
				"content": syntheticExtractedArticle, "title": "Synthetic fixture title",
				"publication_date": "2026-10-24", "method": "source_content", "method_used": "source_content",
				"selector_used": "article", "image_url": "https://example.test/synthetic.jpg",
				"media": []any{map[string]any{"type": "image", "url": "https://example.test/synthetic.jpg"}},
			},
			"observations": []any{map[string]any{"selector": "article", "method": "source_content", "success": true}},
		})
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

func newIntegrationProcessor(pg *pipelinePG, aiURL, extractorURL string) *Processor {
	return &Processor{DB: pg.db, AI: NewAIClient(aiURL, "fixture-ai-token"),
		Extractor:  NewExtractorClient(extractorURL, "fixture-extractor-token"),
		MaxWorkers: 1, MaxBatch: 10, MaxAttempts: 3, TextLimit: 10000,
		AttemptTimeout: 5 * time.Second, RetryInitial: time.Minute, RetryLater: 2 * time.Minute,
		Duplicates: NewDuplicateCache()}
}

func TestProcessorRunPostgresClaimBusyRefundPartialStageAndRetry(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(701)
	pg.seedArticle(t, articleID)
	extractor, extractionCalls := newFixtureExtractor(t)
	var calls atomic.Int32
	var claimVisible atomic.Bool
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		if request.Input["content"] != syntheticExtractedArticle && request.Input["content"] != "Synthetic summary saved before missing category retry." {
			http.Error(w, "analysis did not use fresh extracted content", http.StatusUnprocessableEntity)
			return
		}
		var attempt int
		var state string
		if err := pg.pool.QueryRow(r.Context(), `SELECT processing_attempts,processing_state FROM articles WHERE id=$1`, articleID).Scan(&attempt, &state); err != nil {
			http.Error(w, "article query failed", http.StatusInternalServerError)
			return
		}
		if attempt == request.ArticleAttempt && state == "retry_wait" {
			claimVisible.Store(true)
		}
		n := calls.Add(1)
		if n <= 2 {
			w.Header().Set("Retry-After", "bad-retry-hint")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		result := map[string]any{"is_advertisement": false}
		if n == 3 {
			result["summary"] = "Synthetic summary saved before missing category retry."
		} else {
			result["categories"] = []any{"technology"}
			result["category_confidences"] = []any{0.9}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	processor := newIntegrationProcessor(pg, ai.URL, extractor.URL)
	for run := 1; run <= 4; run++ {
		if run > 1 {
			_, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET next_attempt_at=now()-interval '1 second' WHERE id=$1`, articleID)
			if err != nil {
				t.Fatal(err)
			}
		}
		receipt, err := processor.Run(context.Background(), pg.newRun(t, articleID))
		if err != nil {
			t.Fatalf("processor run %d: %v", run, err)
		}
		_ = receipt
		var attempts int
		var state string
		var lastError *string
		var summary string
		var summaryProcessed, categoryProcessed, adProcessed bool
		if err := pg.admin.QueryRow(context.Background(), `SELECT processing_attempts,processing_state,last_processing_error,
			COALESCE(summary,''),summary_processed,category_processed,ad_processed FROM `+pg.schema+`.articles WHERE id=$1`, articleID).
			Scan(&attempts, &state, &lastError, &summary, &summaryProcessed, &categoryProcessed, &adProcessed); err != nil {
			t.Fatal(err)
		}
		switch run {
		case 1, 2:
			if attempts != 0 || state != "retry_wait" || lastError == nil || *lastError != "busy" || summaryProcessed {
				t.Fatalf("busy run %d was not refunded: attempts=%d state=%s error=%v summary_done=%v", run, attempts, state, lastError, summaryProcessed)
			}
		case 3:
			if attempts != 1 || state != "retry_wait" || lastError == nil || *lastError != "missing_categories" || !summaryProcessed || summary == "" || categoryProcessed {
				t.Fatalf("partial run did not preserve summary stage: attempts=%d state=%s error=%v summary_done=%v category_done=%v", attempts, state, lastError, summaryProcessed, categoryProcessed)
			}
		case 4:
			if attempts != 2 || state != "completed" || lastError != nil || !summaryProcessed || !categoryProcessed || !adProcessed {
				t.Fatalf("retry did not complete independent stages: attempts=%d state=%s error=%v summary_done=%v category_done=%v ad_done=%v", attempts, state, lastError, summaryProcessed, categoryProcessed, adProcessed)
			}
		}
	}
	if calls.Load() != 4 || extractionCalls.Load() != 3 || !claimVisible.Load() {
		t.Fatalf("expected four analysis calls and three ordinary extractions after committed claims (analysis=%d extraction=%d claim_visible=%v)", calls.Load(), extractionCalls.Load(), claimVisible.Load())
	}
	var category string
	if err := pg.admin.QueryRow(context.Background(), `SELECT c.name FROM `+pg.schema+`.article_categories ac JOIN `+pg.schema+`.categories c ON c.id=ac.category_id WHERE ac.article_id=$1`, articleID).Scan(&category); err != nil || category != "Tech" {
		t.Fatalf("category persistence: category=%q err=%v", category, err)
	}
}

func TestProcessorRunPostgresRejectsChangedArticleAndCancelledResponse(t *testing.T) {
	for _, mode := range []string{"title_changed", "summary_changed", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			pg := newPipelinePG(t, false)
			articleID := int64(710)
			pg.seedArticle(t, articleID)
			extractor, _ := newFixtureExtractor(t)
			f := pg.newRun(t, articleID)
			var changed atomic.Bool
			ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request aiEnvelope
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, "bad fixture request", http.StatusBadRequest)
					return
				}
				if answerFixtureAuxAI(t, w, request) {
					return
				}
				switch mode {
				case "title_changed":
					_, _ = pg.pool.Exec(r.Context(), `UPDATE articles SET title='Admin edited synthetic title' WHERE id=$1`, articleID)
				case "summary_changed":
					_, _ = pg.pool.Exec(r.Context(), `UPDATE articles SET summary='Admin edited synthetic summary' WHERE id=$1`, articleID)
				case "cancelled":
					_, _ = pg.pool.Exec(r.Context(), `UPDATE article_processing_runs SET state='cancelled' WHERE id=$1::uuid`, f.RunID)
				}
				changed.Store(true)
				result := map[string]any{"summary": "Stale synthetic provider summary.", "categories": []any{"technology"}, "is_advertisement": false}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
			}))
			defer ai.Close()
			_, runErr := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), f)
			if !changed.Load() {
				t.Fatal("provider response was not reached")
			}
			if runErr == nil {
				t.Fatal("stale or cancelled AI response unexpectedly persisted")
			}
			var title, summary string
			if err := pg.admin.QueryRow(context.Background(), `SELECT title,COALESCE(summary,'') FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&title, &summary); err != nil {
				t.Fatal(err)
			}
			if summary == "Stale synthetic provider summary." {
				t.Fatal("stale provider summary was persisted")
			}
			if mode == "title_changed" && title != "Admin edited synthetic title" {
				t.Fatalf("concurrent title edit lost: %q", title)
			}
			if mode == "summary_changed" && summary != "Admin edited synthetic summary" {
				t.Fatalf("concurrent summary edit lost: %q", summary)
			}
		})
	}
}

func TestProcessorRunPostgresCategoryFailureRollsBackItemAndContinuesSibling(t *testing.T) {
	pg := newPipelinePG(t, true)
	const articleID = int64(720)
	const siblingID = int64(721)
	pg.seedArticle(t, articleID)
	pg.seedArticle(t, siblingID)
	if _, err := pg.admin.Exec(context.Background(), `DELETE FROM `+pg.schema+`.categories WHERE lower(name)='other'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.categories(name) VALUES('AI')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET content=content || ' A second independent synthetic observation set distinguishes this sibling.' WHERE id=$1`, siblingID); err != nil {
		t.Fatal(err)
	}
	extractor, _ := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		category := "technology"
		if request.ArticleID == siblingID {
			category = "AI"
		}
		result := map[string]any{"summary": "Synthetic sibling summary persists.", "categories": []any{category}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	f := pg.newRunWithPayload(t, articleID, "news_processing", map[string]any{"article_ids": []int64{articleID, siblingID}})
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), f); err != nil {
		t.Fatalf("category target failure incorrectly cancelled the batch: %v", err)
	}
	var summary, state string
	var summaryProcessed bool
	var attempts int
	var deadline *time.Time
	var lastError *string
	if err := pg.admin.QueryRow(context.Background(), `SELECT COALESCE(summary,''),summary_processed,processing_state,processing_attempts,next_attempt_at,last_processing_error FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&summary, &summaryProcessed, &state, &attempts, &deadline, &lastError); err != nil {
		t.Fatal(err)
	}
	if summary != "" || summaryProcessed || state != "retry_wait" || attempts != 1 || deadline == nil || lastError == nil || *lastError != "category_mapping_failed" {
		t.Fatalf("category failure did not roll back into bounded retry: summary=%q summary_done=%v state=%s attempts=%d deadline=%v error=%v", summary, summaryProcessed, state, attempts, deadline, lastError)
	}
	var siblingState, siblingSummary string
	if err := pg.admin.QueryRow(context.Background(), `SELECT processing_state,COALESCE(summary,'') FROM `+pg.schema+`.articles WHERE id=$1`, siblingID).Scan(&siblingState, &siblingSummary); err != nil {
		t.Fatal(err)
	}
	if siblingState != "completed" || siblingSummary != "Synthetic sibling summary persists." {
		t.Fatalf("category failure cancelled valid sibling: state=%s summary=%q", siblingState, siblingSummary)
	}
}

func TestProcessorRunPostgresKnownFixedCategoryKeepsOriginalLabelAndFirstDuplicate(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(722)
	pg.seedArticle(t, articleID)
	if _, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.categories(name) VALUES('Business')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.category_mapping(ai_category,fixed_category,is_active) VALUES('Climate policy','Business',true)`); err != nil {
		t.Fatal(err)
	}
	extractor, _ := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		result := map[string]any{"summary": "Synthetic category mapping summary.", "categories": []any{
			map[string]any{"name": "Tech", "ai_category": "Climate policy", "confidence": 0.2},
			map[string]any{"name": "Tech", "ai_category": "science", "confidence": 0.9},
		}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), pg.newRun(t, articleID)); err != nil {
		t.Fatalf("known fixed category processing failed: %v", err)
	}
	var name, original string
	var confidence float64
	var usage int
	if err := pg.admin.QueryRow(context.Background(), `SELECT c.name,ac.ai_category,ac.confidence,cm.usage_count FROM `+pg.schema+`.article_categories ac JOIN `+pg.schema+`.categories c ON c.id=ac.category_id CROSS JOIN `+pg.schema+`.category_mapping cm WHERE ac.article_id=$1`, articleID).Scan(&name, &original, &confidence, &usage); err != nil {
		t.Fatal(err)
	}
	if name != "Tech" || original != "Climate policy" || confidence != 0.2 || usage != 0 {
		t.Fatalf("known fixed category did not bypass mapping / preserve first row: category=%s original=%s confidence=%v mapping_usage=%d", name, original, confidence, usage)
	}
}

func TestProcessorRunPostgresOrdinaryLongBodyStillExtractsAndUsesFreshInput(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(725)
	pg.seedArticle(t, articleID)
	var archivedBody string
	if err := pg.admin.QueryRow(context.Background(), `SELECT content FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&archivedBody); err != nil {
		t.Fatal(err)
	}
	extractor, extractionCalls := newFixtureExtractor(t)
	var analyzeCalls atomic.Int32
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		analyzeCalls.Add(1)
		if request.Input["content"] != syntheticExtractedArticle {
			http.Error(w, "ordinary analysis did not use extracted body", http.StatusUnprocessableEntity)
			return
		}
		if request.Input["original_context"] != archivedBody {
			http.Error(w, "ordinary extraction analysis lost original source context", http.StatusUnprocessableEntity)
			return
		}
		result := map[string]any{"summary": "Summary based on fresh synthetic extraction.", "categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	_, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), pg.newRun(t, articleID))
	if err != nil {
		t.Fatalf("ordinary long-body processing: %v", err)
	}
	if extractionCalls.Load() != 1 || analyzeCalls.Load() != 1 {
		t.Fatalf("ordinary long body must extract and analyze once: extraction=%d analysis=%d", extractionCalls.Load(), analyzeCalls.Load())
	}
	var content, summary string
	if err := pg.admin.QueryRow(context.Background(), `SELECT content,summary FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&content, &summary); err != nil {
		t.Fatal(err)
	}
	if content != syntheticExtractedArticle || summary != "Summary based on fresh synthetic extraction." {
		t.Fatalf("fresh extraction was not used and persisted as an improvement: content_match=%v summary=%q", content == syntheticExtractedArticle, summary)
	}
}

func TestProcessorRunPostgresOrdinaryFreshMissingSummaryGetsSingleFallbackAnalysis(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(730)
	pg.seedArticle(t, articleID)
	var archive string
	if err := pg.admin.QueryRow(context.Background(), `SELECT content FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&archive); err != nil {
		t.Fatal(err)
	}
	extractor, _ := newFixtureExtractor(t)
	var analyzeCalls atomic.Int32
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		call := analyzeCalls.Add(1)
		if request.Input["content"] != syntheticExtractedArticle {
			http.Error(w, "fallback did not retain accepted fresh content", http.StatusUnprocessableEntity)
			return
		}
		_, hasContext := request.Input["original_context"]
		if call == 1 && request.Input["original_context"] != archive {
			http.Error(w, "primary analysis lost archive context", http.StatusUnprocessableEntity)
			return
		}
		if call == 2 && hasContext {
			http.Error(w, "fallback analysis unexpectedly retained archive context", http.StatusUnprocessableEntity)
			return
		}
		result := map[string]any{"categories": []any{"technology"}, "is_advertisement": false}
		if call == 2 {
			result["summary"] = "Recovered synthetic summary."
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), pg.newRun(t, articleID)); err != nil {
		t.Fatalf("ordinary missing-summary fallback: %v", err)
	}
	if analyzeCalls.Load() != 2 {
		t.Fatalf("ordinary fresh-content fallback should analyze exactly twice, got %d", analyzeCalls.Load())
	}
	var summary string
	if err := pg.admin.QueryRow(context.Background(), `SELECT summary FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&summary); err != nil {
		t.Fatal(err)
	}
	if summary != "Recovered synthetic summary." {
		t.Fatalf("fallback summary was not persisted: %q", summary)
	}
}

func TestProcessorRunPostgresForcedTelegramKeepsBoundedArchiveAsContext(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(726)
	pg.seedArticle(t, articleID)
	if _, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.sources(id,source_type,name) VALUES(1,'telegram','Synthetic Telegram')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET source_id=1 WHERE id=$1`, articleID); err != nil {
		t.Fatal(err)
	}
	var archive string
	if err := pg.admin.QueryRow(context.Background(), `SELECT content FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&archive); err != nil {
		t.Fatal(err)
	}
	extractor, _ := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		if request.Input["content"] != syntheticExtractedArticle || request.Input["original_context"] != archive {
			http.Error(w, "forced Telegram analysis did not preserve bounded archive context", http.StatusUnprocessableEntity)
			return
		}
		result := map[string]any{"summary": "Synthetic Telegram summary.", "categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	f := pg.newRunWithPayload(t, articleID, "article_reprocess", map[string]any{"article_ids": []int64{articleID}, "force": true})
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), f); err != nil {
		t.Fatalf("forced Telegram reprocessing: %v", err)
	}
}

func TestProcessorRunPostgresDirectTelegramOmitsOriginalContext(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(729)
	pg.seedArticle(t, articleID)
	if _, err := pg.admin.Exec(context.Background(), `INSERT INTO `+pg.schema+`.sources(id,source_type,name) VALUES(1,'telegram','Synthetic Telegram')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET source_id=1,url='https://t.me/synthetic/729' WHERE id=$1`, articleID); err != nil {
		t.Fatal(err)
	}
	extractor, extractionCalls := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if request.Operation != "analyze" || request.Input["url"] != "https://t.me/synthetic/729" {
			http.Error(w, "direct Telegram analysis used the wrong callback", http.StatusUnprocessableEntity)
			return
		}
		if _, exists := request.Input["original_context"]; exists {
			http.Error(w, "direct Telegram analysis received original source context", http.StatusUnprocessableEntity)
			return
		}
		result := map[string]any{"summary": "Synthetic Telegram summary.", "categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), pg.newRun(t, articleID)); err != nil {
		t.Fatalf("direct Telegram processing: %v", err)
	}
	if extractionCalls.Load() != 0 {
		t.Fatalf("nonextractable Telegram URL unexpectedly used collector: %d", extractionCalls.Load())
	}
}

func TestProcessorRunPostgresSummaryClassificationOmitsOriginalContext(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(728)
	pg.seedArticle(t, articleID)
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET summary='Existing bounded summary for classification.',summary_processed=true WHERE id=$1`, articleID); err != nil {
		t.Fatal(err)
	}
	extractor, extractionCalls := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if request.Operation != "analyze" || request.Input["content"] != "Existing bounded summary for classification." {
			http.Error(w, "summary classification used the wrong input", http.StatusUnprocessableEntity)
			return
		}
		if _, exists := request.Input["original_context"]; exists {
			http.Error(w, "summary classification received original source context", http.StatusUnprocessableEntity)
			return
		}
		result := map[string]any{"categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), pg.newRun(t, articleID)); err != nil {
		t.Fatalf("summary-only classification: %v", err)
	}
	if extractionCalls.Load() != 0 {
		t.Fatalf("summary-only classification unexpectedly extracted source content: %d", extractionCalls.Load())
	}
}

func TestProcessorRunPostgresForceUsesFreshBodyButKeepsLongerArchive(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(726)
	pg.seedArticle(t, articleID)
	archive := syntheticExtractedArticle + "\n" + syntheticExtractedArticle
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET content=$2 WHERE id=$1`, articleID, archive); err != nil {
		t.Fatal(err)
	}
	extractor, extractionCalls := newFixtureExtractor(t)
	var analyzeCalls atomic.Int32
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		analyzeCalls.Add(1)
		if request.Input["content"] != syntheticExtractedArticle {
			http.Error(w, "forced analysis did not use freshly extracted body", http.StatusUnprocessableEntity)
			return
		}
		result := map[string]any{"summary": "Summary based on fresh forced extraction.", "categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	job := map[string]any{"article_ids": []int64{articleID}, "force": true, "reextract_media": false}
	f := pg.newRunWithPayload(t, articleID, "article_reprocess", job)
	if _, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), f); err != nil {
		t.Fatalf("forced processing: %v", err)
	}
	if extractionCalls.Load() != 1 || analyzeCalls.Load() != 1 {
		t.Fatalf("forced processing should use one fresh extraction: extraction=%d analysis=%d", extractionCalls.Load(), analyzeCalls.Load())
	}
	var stored string
	if err := pg.admin.QueryRow(context.Background(), `SELECT content FROM `+pg.schema+`.articles WHERE id=$1`, articleID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != archive {
		t.Fatal("forced reprocessing replaced the longer archived body with a shorter extraction")
	}
}

func TestProcessorRunPostgresAlreadyClassifiedAdPersistsTerminalStateWithoutRefund(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(727)
	pg.seedArticle(t, articleID)
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET processing_attempts=2,ad_processed=true,is_advertisement=true WHERE id=$1`, articleID); err != nil {
		t.Fatal(err)
	}
	var aiCalls atomic.Int32
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		aiCalls.Add(1)
		http.Error(w, "already-classified ad must not call AI", http.StatusInternalServerError)
	}))
	defer ai.Close()
	extractor, extractionCalls := newFixtureExtractor(t)
	receipt, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), pg.newRun(t, articleID))
	if err != nil {
		t.Fatalf("persist already-classified ad outcome: %v", err)
	}
	var attempts int
	var state, summary string
	var processed, adProcessed bool
	var isAd *bool
	if err := pg.admin.QueryRow(context.Background(), `SELECT processing_attempts,processing_state,processed,ad_processed,is_advertisement,COALESCE(summary,'') FROM `+pg.schema+`.articles WHERE id=$1`, articleID).
		Scan(&attempts, &state, &processed, &adProcessed, &isAd, &summary); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || state != "rejected" || !processed || !adProcessed || isAd == nil || !*isAd || summary != "" {
		t.Fatalf("already-classified ad state incorrect: attempts=%d state=%s processed=%v ad_processed=%v is_ad=%v summary=%q", attempts, state, processed, adProcessed, isAd, summary)
	}
	if aiCalls.Load() != 0 || extractionCalls.Load() != 0 {
		t.Fatalf("already-classified ad called a provider: ai=%d extractor=%d", aiCalls.Load(), extractionCalls.Load())
	}
	outcomes, ok := receipt["article_outcomes"].([]store.ArticleOutcome)
	if !ok || len(outcomes) != 1 || outcomes[0].State != "rejected" {
		t.Fatalf("already-classified ad receipt omitted terminal outcome: %#v", receipt["article_outcomes"])
	}
}

func TestProcessingHandlerPostgresCrossProcessReplayDoesNotRepeatAI(t *testing.T) {
	pg := newPipelinePG(t, false)
	const articleID = int64(730)
	pg.seedArticle(t, articleID)
	extractor, _ := newFixtureExtractor(t)
	f := pg.newRun(t, articleID)
	providerStarted := make(chan struct{}, 1)
	releaseProvider := make(chan struct{})
	var calls atomic.Int32
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		if request.Input["content"] != syntheticExtractedArticle {
			http.Error(w, "analysis did not use fresh extracted content", http.StatusUnprocessableEntity)
			return
		}
		calls.Add(1)
		providerStarted <- struct{}{}
		<-releaseProvider
		result := map[string]any{"summary": "One response for a replayed processing run.",
			"categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	processor := newIntegrationProcessor(pg, ai.URL, extractor.URL)
	first := httptest.NewServer(NewHandler(pg.db, processor, Config{Token: "fixture-processing-token"}).Routes())
	defer first.Close()
	second := httptest.NewServer(NewHandler(pg.db, processor, Config{Token: "fixture-processing-token"}).Routes())
	defer second.Close()
	body, err := json.Marshal(f.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	post := func(endpoint string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, endpoint+"/internal/v1/processing/runs", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer fixture-processing-token")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := post(first.URL); got != http.StatusOK {
		t.Fatalf("first run acceptance status=%d", got)
	}
	select {
	case <-providerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first processing instance did not reach the AI provider")
	}
	if got := post(second.URL); got != http.StatusOK {
		t.Fatalf("cross-process replay status=%d", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("same run acquired a second provider call while first held its lock: %d", got)
	}
	close(releaseProvider)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		if err := pg.pool.QueryRow(context.Background(), `SELECT state FROM article_processing_runs WHERE id=$1::uuid`, f.RunID).Scan(&state); err == nil && state == "succeeded" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var state string
	if err := pg.pool.QueryRow(context.Background(), `SELECT state FROM article_processing_runs WHERE id=$1::uuid`, f.RunID).Scan(&state); err != nil || state != "succeeded" {
		t.Fatalf("run did not finish successfully: state=%q err=%v", state, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("cross-process replay made %d provider calls", calls.Load())
	}
}

func TestHandlerCloseCancelsAndDrainsActiveRunBeforeStoreClose(t *testing.T) {
	pg := newPipelinePG(t, false)
	f := pg.newRun(t, 730)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	runner := &shutdownTestRunner{started: make(chan struct{}), returned: make(chan struct{})}
	handler := NewHandlerWithContext(parent, pg.db, runner, Config{Token: "fixture-processing-token"})
	if !handler.launch(f) {
		t.Fatal("handler refused the active run")
	}
	select {
	case <-runner.started:
	case <-time.After(3 * time.Second):
		t.Fatal("background runner did not start")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := handler.Close(closeCtx); err != nil {
		t.Fatalf("handler did not drain canceled run: %v", err)
	}
	select {
	case <-runner.returned:
	default:
		t.Fatal("handler returned before the run observed cancellation")
	}
	var state string
	if err := pg.admin.QueryRow(context.Background(), `SELECT state FROM `+pg.schema+`.article_processing_runs WHERE id=$1::uuid`, f.RunID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "failed" {
		t.Fatalf("canceled active run was not finalized safely: %s", state)
	}
}

func TestProcessorRunPostgresNormalReceiptIsBoundedForLargeCycle(t *testing.T) {
	pg := newPipelinePG(t, false)
	const firstID int64 = 800
	const articleCount = 205
	for i := int64(0); i < articleCount; i++ {
		pg.seedArticle(t, firstID+i)
	}
	extractor, _ := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		result := map[string]any{"summary": "Synthetic large-cycle summary.", "categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	processor := newIntegrationProcessor(pg, ai.URL, extractor.URL)
	processor.MaxWorkers = 4
	f := pg.newRunWithPayload(t, firstID, "news_processing", map[string]any{})
	receipt, err := processor.Run(context.Background(), f)
	if err != nil {
		t.Fatalf("large normal cycle: %v", err)
	}
	outcomes, ok := receipt["article_outcomes"].([]store.ArticleOutcome)
	if !ok || len(outcomes) != 20 || receipt["article_outcomes_total"] != articleCount || receipt["article_outcomes_omitted"] != articleCount-20 {
		t.Fatalf("normal receipt was not bounded accurately: outcomes=%d total=%v omitted=%v", len(outcomes), receipt["article_outcomes_total"], receipt["article_outcomes_omitted"])
	}
	wire, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) >= 16*1024 {
		t.Fatalf("bounded normal receipt exceeds callback cap: %d bytes", len(wire))
	}
}

func TestProcessorRunPostgresSelectedReceiptIncludesNotDueRowsButNoMissingFabrication(t *testing.T) {
	pg := newPipelinePG(t, false)
	const dueID int64 = 840
	const futureID int64 = 841
	const missingID int64 = 842
	pg.seedArticle(t, dueID)
	pg.seedArticle(t, futureID)
	if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles SET processing_attempts=1,processing_state='retry_wait',next_attempt_at=now()+interval '1 hour',last_processing_error='previous_retry' WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	extractor, _ := newFixtureExtractor(t)
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad fixture request", http.StatusBadRequest)
			return
		}
		if answerFixtureAuxAI(t, w, request) {
			return
		}
		result := map[string]any{"summary": "Synthetic selected summary.", "categories": []any{"technology"}, "is_advertisement": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, result))
	}))
	defer ai.Close()
	payload := map[string]any{"article_ids": []int64{dueID, futureID, missingID}, "force": true}
	f := pg.newRunWithPayload(t, dueID, "article_reprocess", payload)
	receipt, err := newIntegrationProcessor(pg, ai.URL, extractor.URL).Run(context.Background(), f)
	if err != nil {
		t.Fatalf("selected article processing: %v", err)
	}
	outcomes, ok := receipt["article_outcomes"].([]store.ArticleOutcome)
	if !ok || len(outcomes) != 2 || receipt["status"] != "incomplete" || receipt["settled"] != false || receipt["retryable"] != true || receipt["article_outcomes_total"] != 2 {
		t.Fatalf("selected receipt omitted not-due state or fabricated a missing row: %#v", receipt)
	}
	if outcomes[0].ArticleID != dueID || outcomes[0].State != "completed" || outcomes[1].ArticleID != futureID || outcomes[1].State != "retry_wait" {
		t.Fatalf("selected outcomes do not include due and not-due rows: %#v", outcomes)
	}
}

func TestSelectedRunGETSanitizesLegacyErrorsAndStaysBounded(t *testing.T) {
	pg := newPipelinePG(t, false)
	const firstID int64 = 1200
	const articleCount = 100
	requested := make([]int64, 0, articleCount)
	legacyError := strings.Repeat("private legacy detail ", 100)
	for i := int64(0); i < articleCount; i++ {
		id := firstID + i
		pg.seedArticle(t, id)
		requested = append(requested, id)
		if _, err := pg.admin.Exec(context.Background(), `UPDATE `+pg.schema+`.articles
			SET processing_state='retry_wait',processing_attempts=1,next_attempt_at=now()+interval '1 hour',last_processing_error=$2
			WHERE id=$1`, id, legacyError); err != nil {
			t.Fatal(err)
		}
	}
	f := pg.newRunWithPayload(t, firstID, "article_reprocess", map[string]any{"article_ids": requested})
	outcomes, err := pg.db.ArticleOutcomes(context.Background(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != articleCount || outcomes[0].Reason == nil || *outcomes[0].Reason != "other" {
		t.Fatalf("legacy error was not projected to a finite receipt code: count=%d reason=%v", len(outcomes), outcomes[0].Reason)
	}
	var stored string
	if err := pg.admin.QueryRow(context.Background(), `SELECT last_processing_error FROM `+pg.schema+`.articles WHERE id=$1`, firstID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != legacyError {
		t.Fatal("receipt projection changed the stored legacy error")
	}
	receipt := map[string]any{
		"status": "incomplete", "success": false, "settled": false, "retryable": true,
		"article_outcomes": outcomes, "article_outcomes_total": len(outcomes), "article_outcomes_omitted": 0,
	}
	if err := pg.db.FinishRun(context.Background(), f, receipt); err != nil {
		t.Fatalf("persist bounded selected receipt: %v", err)
	}
	handler := NewHandler(pg.db, nil, Config{Token: "fixture-token"})
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/processing/runs/"+f.RunID, nil)
	req.Header.Set("Authorization", "Bearer fixture-token")
	res := httptest.NewRecorder()
	handler.Routes().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("GET run returned %d: %s", res.Code, res.Body.String())
	}
	if res.Body.Len() >= 16*1024 {
		t.Fatalf("full GET run response exceeds callback limit: %d bytes", res.Body.Len())
	}
	if strings.Contains(res.Body.String(), "private legacy detail") {
		t.Fatal("full GET run response exposed a stored legacy error")
	}
}
