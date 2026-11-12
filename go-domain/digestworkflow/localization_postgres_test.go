package digestworkflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	ai "github.com/Dzarlax-AI/go-ai"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type localeTestFence struct{}

func (localeTestFence) Validate(context.Context, pgx.Tx) error { return nil }

type localeTestProvider func(context.Context, ai.Request) (ai.Response, error)

func (f localeTestProvider) Generate(c context.Context, r ai.Request) (ai.Response, error) {
	return f(c, r)
}
func localizedPG(t *testing.T) *pgxpool.Pool {
	pool, _ := localizedPGWithSchema(t)
	return pool
}

func localizedPGWithSchema(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	u, e := url.Parse(dsn)
	if e != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1") {
		t.Fatal("loopback required")
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := fmt.Sprintf("sr_editorial_%d", time.Now().UnixNano())
	admin, e := pgxpool.New(context.Background(), dsn)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(context.Background(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	result, e := migration.Run(context.Background(), pool, true, false)
	if e != nil || len(result.Errors) > 0 {
		t.Fatalf("migration: %v %v", e, result.Errors)
	}
	return pool, schema
}
func draftFor(ids []string, summaries map[string]string, serbian bool) map[string]any {
	items := map[string]any{}
	for _, id := range ids {
		summary := summaries[id]
		hash := sha256.Sum256([]byte(summary))
		items[id] = map[string]any{"brief_summary": summary, "span_id": fmt.Sprintf("span-%s-%x-0001", id, hash)}
	}
	title := "Русский выпуск"
	if serbian {
		title = "Srpski pregled"
	}
	return map[string]any{"main_stories": []any{map[string]any{"title": title, "summary": summaries[ids[0]], "primary_article_id": ids[0], "article_ids": ids}}, "brief_stories": []any{}, "article_stories": items}
}

func TestRussianGenerationUsesBoundedProviderBudget(t *testing.T) {
	pool := localizedPG(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Truncate(time.Microsecond)
	_, err := pool.Exec(ctx, `INSERT INTO sources(id,name,source_type,url) VALUES(90002,'Fixture','rss','https://source.fixture/feed'); INSERT INTO categories(id,name,display_name) VALUES(90002,'fixture-category','Категория'); INSERT INTO articles(id,title,url,summary,source_id,fetched_at,processing_state) VALUES(90002,'Русский заголовок','https://source.fixture/2','Город начал строительство моста.',90002,$1,'completed'); INSERT INTO article_categories(article_id,category_id,confidence) VALUES(90002,90002,1)`, pgx.QueryExecModeSimpleProtocol, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	provider := localeTestProvider(func(providerCtx context.Context, req ai.Request) (ai.Response, error) {
		calls++
		deadline, ok := providerCtx.Deadline()
		if !ok || time.Until(deadline) < 179*time.Second || time.Until(deadline) > 181*time.Second || req.Purpose != "digest" {
			t.Fatal("Russian generation did not receive the bounded digest budget")
		}
		raw, err := json.Marshal(draftFor([]string{"90002"}, map[string]string{"90002": "Город начал строительство моста."}, false))
		if err != nil {
			t.Fatal(err)
		}
		return ai.Response{Status: ai.Complete, Text: string(raw), Model: "fixture", Provider: "fixture"}, nil
	})
	wf := Workflow{Pool: pool, Fence: localeTestFence{}, SiteBase: "https://news.fixture", Provider: editorial.ProviderConfig{Provider: provider, Model: "fixture"}}
	n, err := wf.Generate(ctx, "10000000-0000-4000-8000-000000000002", 1, "20000000-0000-4000-8000-000000000001", cutoff)
	if err != nil || n != 1 || calls != 1 {
		t.Fatalf("Russian generation count=%d calls=%d error=%v", n, calls, err)
	}
}

func TestLocalizedEditionPartialFrozenRetryAndFreshJob(t *testing.T) {
	pool := localizedPG(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Truncate(time.Microsecond)
	_, err := pool.Exec(ctx, `INSERT INTO sources(id,name,source_type,url) VALUES(90001,'Fixture','rss','https://source.fixture/feed'); INSERT INTO categories(id,name,display_name) VALUES(90001,'fixture-category','Категория'); INSERT INTO category_localizations(category_id,locale,display_name) VALUES(90001,'sr-Latn','Vesti'); INSERT INTO articles(id,title,url,summary,source_id,fetched_at,processing_state) VALUES(90001,'Русский заголовок','https://source.fixture/1','Город начал строительство моста.',90001,$1,'completed'); INSERT INTO article_categories(article_id,category_id,confidence) VALUES(90001,90001,1); INSERT INTO article_localizations(article_id,locale,title,summary,input_fingerprint,prompt_version,model,status) VALUES(90001,'sr-Latn','Počinje izgradnja mosta','Grad je počeo izgradnju mosta.',repeat('a',64),'fixture','fixture','completed')`, pgx.QueryExecModeSimpleProtocol, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	ruSummaries := map[string]string{"90001": "Город начал строительство моста."}
	srSummaries := map[string]string{"90001": "Grad je počeo izgradnju mosta."}
	ids := []string{"90001"}
	calls := 0
	provider := localeTestProvider(func(providerCtx context.Context, r ai.Request) (ai.Response, error) {
		deadline, ok := providerCtx.Deadline()
		if !ok || time.Until(deadline) < 179*time.Second || time.Until(deadline) > 181*time.Second {
			t.Fatal("localized provider did not receive the full generation budget")
		}
		calls++
		var output any
		if calls == 1 {
			output = map[string]any{"ru": draftFor(ids, ruSummaries, false), "sr-Latn": map[string]any{}}
		} else if calls == 2 {
			return ai.Response{}, errors.New("synthetic provider failure")
		} else if calls == 3 {
			if !strings.Contains(r.Prompt, srSummaries["90001"]) || strings.Contains(r.Prompt, "PROMENJENI TEKST") {
				t.Fatal("retry reread mutated Serbian summary")
			}
			output = draftFor(ids, srSummaries, true)
		} else {
			output = map[string]any{"ru": draftFor(ids, ruSummaries, false), "sr-Latn": draftFor(ids, srSummaries, true)}
		}
		raw, _ := json.Marshal(output)
		return ai.Response{Status: ai.Complete, Text: string(raw), Model: "fixture", Provider: "fixture"}, nil
	})
	wf := Workflow{Pool: pool, Fence: localeTestFence{}, SiteBase: "https://news.fixture", Provider: editorial.ProviderConfig{Provider: provider, Model: "fixture"}, RetryPolicy: &SerbianRetryPolicy{}}
	job := "10000000-0000-4000-8000-000000000001"
	op := "20000000-0000-4000-8000-000000000001"
	n, err := wf.GenerateLocalized(ctx, job, 1, op, cutoff)
	if n != 1 || !errors.Is(err, ErrSerbianProviderFailed) {
		t.Fatalf("initial generation %d %v", n, err)
	}
	var ruID, hash string
	var srCount int
	if err = pool.QueryRow(ctx, `SELECT id::text,snapshot_hash FROM editorial_digest_editions WHERE locale='ru'`).Scan(&ruID, &hash); err != nil {
		t.Fatal(err)
	}
	if err = wf.BindSerbianPublication(ctx, ruID, SerbianRetryPolicy{DeliveryEnabled: true, Destination: "synthetic-sr-channel", PublicationPinned: true}); err != nil {
		t.Fatal(err)
	}
	if err = wf.BindSerbianPublication(ctx, ruID, SerbianRetryPolicy{DeliveryEnabled: true, Destination: "other-channel", PublicationPinned: true}); err != nil {
		t.Fatal(err)
	}
	if err = wf.MarkRussianPublicationAttempted(ctx, ruID); err != nil {
		t.Fatal(err)
	}
	legacyID := "30000000-0000-4000-8000-000000000003"
	if err = wf.BindSerbianPublication(ctx, legacyID, SerbianRetryPolicy{DeliveryEnabled: true, Destination: "synthetic-sr-channel"}); err != nil {
		t.Fatal(err)
	}
	var legacyExists bool
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM editorial_serbian_retry WHERE russian_edition_id=$1)`, legacyID).Scan(&legacyExists); err != nil || legacyExists {
		t.Fatal("legacy edition was backfilled")
	}
	disabledID := "40000000-0000-4000-8000-000000000004"
	if _, err = pool.Exec(ctx, `INSERT INTO editorial_serbian_retry(russian_edition_id) VALUES($1)`, disabledID); err != nil {
		t.Fatal(err)
	}
	if err = wf.BindSerbianPublication(ctx, disabledID, SerbianRetryPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err = wf.BindSerbianPublication(ctx, disabledID, SerbianRetryPolicy{DeliveryEnabled: true, Destination: "synthetic-sr-channel"}); err != nil {
		t.Fatal(err)
	}
	var disabled bool
	if err = pool.QueryRow(ctx, `SELECT delivery_enabled FROM editorial_serbian_retry WHERE russian_edition_id=$1`, disabledID).Scan(&disabled); err != nil || disabled {
		t.Fatal("disabled first publication was changed")
	}
	if _, err = pool.Exec(ctx, `UPDATE editorial_serbian_retry SET next_dispatch_at=now()-interval '1 second' WHERE russian_edition_id=$1`, ruID); err != nil {
		t.Fatal(err)
	}
	var admitted bool
	if err = pool.QueryRow(ctx, `SELECT enqueue_serbian_digest_retry()`).Scan(&admitted); err != nil || !admitted {
		t.Fatalf("retry admission: %v %v", admitted, err)
	}
	var retryKind, retryID, destination string
	var enabled bool
	if err = pool.QueryRow(ctx, `SELECT j.kind,j.payload->>'russian_edition_id',r.destination,r.delivery_enabled FROM editorial_serbian_retry r JOIN background_jobs j ON j.id=r.last_job_id WHERE r.russian_edition_id=$1`, ruID).Scan(&retryKind, &retryID, &destination, &enabled); err != nil || retryKind != "serbian_digest_retry" || retryID != ruID || destination != "synthetic-sr-channel" || !enabled {
		t.Fatal("retry identity or pinned policy changed")
	}
	if err = pool.QueryRow(ctx, `SELECT enqueue_serbian_digest_retry()`).Scan(&admitted); err != nil || admitted {
		t.Fatal("duplicate retry admitted")
	}
	if _, err = pool.Exec(ctx, `UPDATE editorial_serbian_retry SET destination='changed' WHERE russian_edition_id=$1`, ruID); err == nil {
		t.Fatal("pinned destination was mutable")
	}
	if _, err = pool.Exec(ctx, `UPDATE background_jobs SET state='delivery_unknown' WHERE id=(SELECT last_job_id FROM editorial_serbian_retry WHERE russian_edition_id=$1); UPDATE editorial_serbian_retry SET next_dispatch_at=now()-interval '1 second' WHERE russian_edition_id=$1`, pgx.QueryExecModeSimpleProtocol, ruID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT enqueue_serbian_digest_retry()`).Scan(&admitted); err != nil || admitted {
		t.Fatal("unknown delivery was requeued")
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM editorial_digest_editions WHERE locale='sr-Latn'`).Scan(&srCount); err != nil || srCount != 0 {
		t.Fatal("invalid SR persisted")
	}
	if _, err = pool.Exec(ctx, `UPDATE article_localizations SET summary='PROMENJENI TEKST' WHERE article_id=90001; UPDATE editorial_locale_inputs SET next_attempt_at=now() WHERE russian_edition_id=$1`, pgx.QueryExecModeSimpleProtocol, ruID); err != nil {
		t.Fatal(err)
	}
	// A durable retry may run after midnight; the exact edition ID remains authoritative.
	if _, err = pool.Exec(ctx, `UPDATE editorial_locale_inputs SET attempts=3,next_attempt_at=now()-interval '1 second' WHERE russian_edition_id=$1`, ruID); err != nil {
		t.Fatal(err)
	}
	if _, err = wf.RetrySerbian(ctx, job, 1, op, cutoff.Add(24*time.Hour), ruID); !errors.Is(err, ErrSerbianAttemptsExhausted) {
		t.Fatalf("exhausted retry error=%v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE editorial_locale_inputs SET attempts=1,next_attempt_at=now()-interval '1 second' WHERE russian_edition_id=$1`, ruID); err != nil {
		t.Fatal(err)
	}
	n, err = wf.RetrySerbian(ctx, job, 1, op, cutoff.Add(24*time.Hour), ruID)
	if !errors.Is(err, ErrSerbianProviderFailed) || n != 1 || calls != 2 {
		t.Fatalf("provider failure retry %d calls=%d err=%v", n, calls, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE editorial_locale_inputs SET next_attempt_at=now()-interval '1 second' WHERE russian_edition_id=$1`, ruID); err != nil {
		t.Fatal(err)
	}
	n, err = wf.RetrySerbian(ctx, job, 1, op, cutoff.Add(24*time.Hour), ruID)
	if err != nil || n != 1 || calls != 3 {
		t.Fatalf("retry %d calls=%d err=%v", n, calls, err)
	}
	var ready bool
	if ready, err = wf.HasSerbian(ctx, serbianEditionID(ruID)); err != nil || !ready {
		t.Fatalf("SR not ready: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE background_jobs SET state='failed' WHERE id=(SELECT last_job_id FROM editorial_serbian_retry WHERE russian_edition_id=$1); UPDATE editorial_serbian_retry SET next_dispatch_at=now()-interval '1 second' WHERE russian_edition_id=$1`, pgx.QueryExecModeSimpleProtocol, ruID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT enqueue_serbian_digest_retry()`).Scan(&admitted); err != nil || !admitted {
		t.Fatal("ready edition with no delivery receipt was not requeued")
	}
	if _, err = pool.Exec(ctx, `UPDATE background_jobs SET state='failed' WHERE id=(SELECT last_job_id FROM editorial_serbian_retry WHERE russian_edition_id=$1); UPDATE editorial_serbian_retry SET next_dispatch_at=now()-interval '1 second' WHERE russian_edition_id=$1; INSERT INTO digest_delivery_intents(id,edition_id,destination_hash,purpose,render_hash,job_id,attempt_generation,domain_instance_id,state,parts_sent,rich_message) VALUES(gen_random_uuid(),$2,repeat('a',64),'ru_digest',repeat('b',64),$3,1,$3,'completed',1,true)`, pgx.QueryExecModeSimpleProtocol, ruID, serbianEditionID(ruID), job); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT enqueue_serbian_digest_retry()`).Scan(&admitted); err != nil || admitted {
		t.Fatal("completed delivery was requeued")
	}
	var after string
	if err = pool.QueryRow(ctx, `SELECT snapshot_hash FROM editorial_digest_editions WHERE id=$1`, ruID).Scan(&after); err != nil || after != hash {
		t.Fatal("RU changed on SR retry")
	}
	if _, err = pool.Exec(ctx, `UPDATE article_localizations SET summary=$1 WHERE article_id=90001`, srSummaries["90001"]); err != nil {
		t.Fatal(err)
	}
	// A separate explicit generation job creates its own immutable edition.
	n, err = wf.GenerateLocalized(ctx, "10000000-0000-4000-8000-000000000002", 1, op, cutoff)
	if err != nil || n != 1 || calls != 4 {
		t.Fatalf("fresh job reused prior edition: calls=%d err=%v", calls, err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM editorial_digest_editions WHERE locale='ru'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("Russian editions=%d err=%v", count, err)
	}
}

func TestSourceRevisionInvalidatesOnlyExistingSerbianResult(t *testing.T) {
	pool := localizedPG(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO sources(id,name,source_type,url) VALUES(90001,'Fixture','rss','https://source.fixture/feed');INSERT INTO articles(id,title,url,summary,source_id,processing_state,processing_attempts)VALUES(90001,'Old title','https://source.fixture/1','Old summary',90001,'completed',2),(90002,'No translation','https://source.fixture/2','No translation',90001,'completed',2);INSERT INTO article_localizations(article_id,locale,title,summary,input_fingerprint,prompt_version,model,status)VALUES(90001,'ru','Old title','Old summary',repeat('a',64),'fixture','fixture','completed'),(90001,'sr-Latn','Stari naslov','Stari sažetak',repeat('a',64),'fixture','fixture','completed');UPDATE articles SET summary='New summary' WHERE id IN(90001,90002)`); err != nil {
		t.Fatal(err)
	}
	var state, ruState, articleState string
	var attempts, rows int
	if err := pool.QueryRow(ctx, `SELECT status FROM article_localizations WHERE article_id=90001 AND locale='sr-Latn'`).Scan(&state); err != nil || state != "retry_wait" {
		t.Fatalf("stale Serbian remains ready: %s %v", state, err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM article_localizations WHERE article_id=90001 AND locale='ru'`).Scan(&ruState); err != nil || ruState != "completed" {
		t.Fatal("Russian state reset")
	}
	if err := pool.QueryRow(ctx, `SELECT processing_state,processing_attempts FROM articles WHERE id=90001`).Scan(&articleState, &attempts); err != nil || articleState != "completed" || attempts != 2 {
		t.Fatal("article processing changed")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM article_localizations WHERE article_id=90002`).Scan(&rows); err != nil || rows != 0 {
		t.Fatal("historical backfill work created")
	}
}

func TestLocalizedSnapshotReportsBoundedReadinessFailures(t *testing.T) {
	pool := localizedPG(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO sources(id,name,source_type,url) VALUES(90011,'Fixture','rss','https://source.fixture/feed');
INSERT INTO categories(id,name,display_name) VALUES(90011,'nature','Nature'),(90012,'history','History'),(90013,'business','Business');
INSERT INTO category_localizations(category_id,locale,display_name) VALUES(90012,'sr-Latn','Istorija'),(90013,'sr-Latn','Poslovanje');
INSERT INTO articles(id,title,url,summary,source_id,processing_state) VALUES
(90011,'Nature title','https://source.fixture/nature','Nature summary',90011,'completed'),
(90012,'History title','https://source.fixture/history','History summary',90011,'completed'),
(90013,'Current title','https://source.fixture/business','Current summary',90011,'completed');
INSERT INTO article_localizations(article_id,locale,title,summary,input_fingerprint,prompt_version,model,status) VALUES
(90011,'sr-Latn','Naslov prirode','Sažetak prirode',repeat('a',64),'fixture','fixture','completed'),
(90013,'sr-Latn','Stari naslov','Stari sažetak',repeat('b',64),'fixture','fixture','completed');`); err != nil {
		t.Fatal(err)
	}
	wf := Workflow{Pool: pool, Fence: localeTestFence{}}
	for _, tc := range []struct {
		name, articleID, categoryID string
		want                        error
	}{
		{"category_missing", "90011", "90011", ErrCategoryTranslationMissing},
		{"article_missing", "90012", "90012", ErrArticleTranslationMissing},
		{"source_stale", "90013", "90013", ErrSerbianSourceStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := editorial.Snapshot{Date: "2026-11-11", SiteBaseURL: "https://news.fixture", Articles: []editorial.Article{{ID: tc.articleID, Title: map[string]string{"90011": "Nature title", "90012": "History title", "90013": "Original title"}[tc.articleID], Summary: map[string]string{"90011": "Nature summary", "90012": "History summary", "90013": "Original summary"}[tc.articleID], CategoryID: tc.categoryID}}}
			_, err := wf.localizedSnapshot(ctx, snapshot, "20000000-0000-4000-8000-000000000011")
			if !errors.Is(err, tc.want) {
				t.Fatalf("localized readiness error=%v want %v", err, tc.want)
			}
		})
	}
}

func TestMissingCategoryKeepsRussianEditionAndFailsRequiredSerbian(t *testing.T) {
	pool := localizedPG(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `INSERT INTO sources(id,name,source_type,url) VALUES(90014,'Fixture','rss','https://source.fixture/feed');
INSERT INTO categories(id,name,display_name) VALUES(90014,'nature','Nature');
INSERT INTO articles(id,title,url,summary,source_id,fetched_at,processing_state) VALUES(90014,'Русский заголовок','https://source.fixture/14','Город начал строительство моста.',90014,$1,'completed');
INSERT INTO article_categories(article_id,category_id,confidence) VALUES(90014,90014,1);
INSERT INTO article_localizations(article_id,locale,title,summary,input_fingerprint,prompt_version,model,status) VALUES(90014,'sr-Latn','Naslov','Sažetak',repeat('a',64),'fixture','fixture','completed')`, pgx.QueryExecModeSimpleProtocol, cutoff); err != nil {
		t.Fatal(err)
	}
	provider := localeTestProvider(func(_ context.Context, _ ai.Request) (ai.Response, error) {
		raw, _ := json.Marshal(draftFor([]string{"90014"}, map[string]string{"90014": "Город начал строительство моста."}, false))
		return ai.Response{Status: ai.Complete, Text: string(raw), Model: "fixture", Provider: "fixture"}, nil
	})
	wf := Workflow{Pool: pool, Fence: localeTestFence{}, SiteBase: "https://news.fixture", Provider: editorial.ProviderConfig{Provider: provider, Model: "fixture"}, RetryPolicy: &SerbianRetryPolicy{}}
	n, err := wf.GenerateLocalized(ctx, "10000000-0000-4000-8000-000000000014", 1, "20000000-0000-4000-8000-000000000014", cutoff)
	if n != 1 || !errors.Is(err, ErrCategoryTranslationMissing) {
		t.Fatalf("generation count=%d error=%v", n, err)
	}
	var ruCount, srCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE locale='ru'),count(*) FILTER (WHERE locale='sr-Latn') FROM editorial_digest_editions`).Scan(&ruCount, &srCount); err != nil || ruCount != 1 || srCount != 0 {
		t.Fatalf("RU count=%d SR count=%d error=%v", ruCount, srCount, err)
	}
}

func TestOneShotSerbianRecoverySQLIsIdempotentAndFenced(t *testing.T) {
	pairedEditionSQL := `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage,job_id,attempt_generation) SELECT '40000000-0000-4000-8000-000000000014','2026-11-11','sr-Latn','bilingual-v1',repeat('b',64),snapshot,jsonb_build_object('version','1','locale','sr-Latn','source_snapshot_hash',repeat('b',64),'stories','[]'::jsonb),'{"rich_html":"","plain_parts":[],"preview_text":""}','{}',job_id,attempt_generation FROM editorial_digest_editions WHERE id='6c215a61-23ad-58af-bab2-2db2df086401'`
	for _, tc := range []struct {
		name       string
		hazardSQL  string
		wantReject bool
	}{
		{name: "double_run"},
		{name: "unknown_last_job", hazardSQL: `UPDATE background_jobs SET state='delivery_unknown' WHERE id='10000000-0000-4000-8000-000000000011'`, wantReject: true},
		{name: "active_retry", hazardSQL: `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state) VALUES('10000000-0000-4000-8000-000000000012','serbian_digest_retry','{"russian_edition_id":"6c215a61-23ad-58af-bab2-2db2df086401"}','fixture-active-retry','running')`, wantReject: true},
		{name: "paired_serbian_edition", hazardSQL: pairedEditionSQL, wantReject: true},
		{name: "partial_unknown_receipt", hazardSQL: pairedEditionSQL + `;
INSERT INTO digest_delivery_intents(id,edition_id,destination_hash,purpose,render_hash,job_id,attempt_generation,domain_instance_id,state) VALUES('50000000-0000-4000-8000-000000000014','40000000-0000-4000-8000-000000000014',repeat('c',64),'ru_digest',repeat('d',64),'20000000-0000-4000-8000-000000000011',1,'30000000-0000-4000-8000-000000000011','active');
INSERT INTO digest_delivery_parts(intent_id,method,part_index,payload_hash,job_id,attempt_generation,domain_instance_id,state) VALUES('50000000-0000-4000-8000-000000000014','sendMessage',0,repeat('e',64),'20000000-0000-4000-8000-000000000011',1,'30000000-0000-4000-8000-000000000011','unknown')`, wantReject: true},
		{name: "completed_receipt", hazardSQL: pairedEditionSQL + `;
INSERT INTO digest_delivery_intents(id,edition_id,destination_hash,purpose,render_hash,job_id,attempt_generation,domain_instance_id,state,parts_sent,rich_message) VALUES('50000000-0000-4000-8000-000000000015','40000000-0000-4000-8000-000000000014',repeat('c',64),'ru_digest',repeat('d',64),'20000000-0000-4000-8000-000000000011',1,'30000000-0000-4000-8000-000000000011','completed',1,false)`, wantReject: true},
		{name: "unknown_category", hazardSQL: `INSERT INTO categories(id,name,display_name) VALUES(12,'economy','Economy')`, wantReject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, schema := localizedPGWithSchema(t)
			seedRecoveryIssue(t, pool)
			if tc.hazardSQL != "" {
				if _, err := pool.Exec(context.Background(), tc.hazardSQL); err != nil {
					t.Fatalf("seed hazard failed: %v", err)
				}
			}
			script := recoverySQL(t, schema)
			_, firstErr := pool.Exec(context.Background(), script)
			if tc.wantReject {
				if firstErr == nil {
					t.Fatal("hazardous recovery state was admitted")
				}
				var jobs int
				if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM background_jobs WHERE idempotency_key='serbian-recovery:6c215a61-23ad-58af-bab2-2db2df086401:2026-11-12-stage-a'`).Scan(&jobs); err != nil || jobs != 0 {
					t.Fatalf("hazard created recovery job count=%d err=%v", jobs, err)
				}
				var natureCount int
				if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM category_localizations WHERE locale='sr-Latn' AND display_name='Priroda i ekologija'`).Scan(&natureCount); err != nil || natureCount != 0 {
					t.Fatalf("hazard partially changed category labels count=%d err=%v", natureCount, err)
				}
				return
			}
			if firstErr != nil {
				t.Fatalf("first recovery: %v", firstErr)
			}
			var firstJob string
			if err := pool.QueryRow(context.Background(), `SELECT last_job_id::text FROM editorial_serbian_retry WHERE russian_edition_id='6c215a61-23ad-58af-bab2-2db2df086401'`).Scan(&firstJob); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(context.Background(), script); err != nil {
				t.Fatalf("idempotent second recovery: %v", err)
			}
			var secondJob string
			var dispatches, jobCount, natureCount, historyCount int
			if err := pool.QueryRow(context.Background(), `SELECT last_job_id::text,dispatches FROM editorial_serbian_retry WHERE russian_edition_id='6c215a61-23ad-58af-bab2-2db2df086401'`).Scan(&secondJob, &dispatches); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM background_jobs WHERE idempotency_key='serbian-recovery:6c215a61-23ad-58af-bab2-2db2df086401:2026-11-12-stage-a'`).Scan(&jobCount); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE display_name='Priroda i ekologija'),count(*) FILTER (WHERE display_name='Istorija') FROM category_localizations WHERE locale='sr-Latn'`).Scan(&natureCount, &historyCount); err != nil {
				t.Fatal(err)
			}
			if firstJob != secondJob || dispatches != 12 || jobCount != 1 || natureCount != 1 || historyCount != 1 {
				t.Fatalf("job changed on rerun (same=%v dispatches=%d jobs=%d nature=%d history=%d)", firstJob == secondJob, dispatches, jobCount, natureCount, historyCount)
			}
		})
	}
}

func recoverySQL(t *testing.T, schema string) string {
	t.Helper()
	path := "../../docs/reviews/2026-11-12-serbian-digest-recovery/recover_issue.sql"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(raw), "news", pgx.Identifier{schema}.Sanitize())
}

func seedRecoveryIssue(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
INSERT INTO categories(id,name,display_name) VALUES
(1,'Business','Business'),(2,'Tech','Tech'),(3,'Science','Science'),(4,'Nature','Nature'),(5,'Serbia','Serbia'),(6,'Marketing','Marketing'),(7,'Media','Media'),(8,'Other','Other'),(9,'AI','AI'),(10,'Politics','Politics'),(11,'History','History');
INSERT INTO category_localizations(category_id,locale,display_name) VALUES
(1,'sr-Latn','Poslovanje'),(2,'sr-Latn','Tehnologija'),(3,'sr-Latn','Nauka'),(5,'sr-Latn','Srbija'),(6,'sr-Latn','Marketing'),(7,'sr-Latn','Mediji'),(8,'sr-Latn','Ostalo'),(9,'sr-Latn','Veštačka inteligencija'),(10,'sr-Latn','Politika');
INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage,job_id,attempt_generation)
VALUES ('6c215a61-23ad-58af-bab2-2db2df086401','2026-11-11','ru','bilingual-v1',repeat('a',64),
'{"date":"2026-11-11","articles":[{}]}'::jsonb,
jsonb_build_object('version','1','locale','ru','source_snapshot_hash',repeat('a',64),'stories','[]'::jsonb),
'{"rich_html":"","plain_parts":[],"preview_text":""}'::jsonb,'{}',
'20000000-0000-4000-8000-000000000011',1);
INSERT INTO editorial_locale_inputs(russian_edition_id,russian_snapshot,serbian_snapshot,attempts,last_error)
SELECT id,snapshot,NULL,0,'serbian_category_translation_missing' FROM editorial_digest_editions WHERE id='6c215a61-23ad-58af-bab2-2db2df086401';
INSERT INTO background_jobs(id,kind,payload,idempotency_key,state) VALUES
('10000000-0000-4000-8000-000000000011','serbian_digest_retry','{"russian_edition_id":"6c215a61-23ad-58af-bab2-2db2df086401"}','fixture-old-retry','succeeded');
INSERT INTO editorial_serbian_retry(russian_edition_id,destination,delivery_enabled,publication_pinned,publication_attempted,dispatches,last_job_id)
VALUES ('6c215a61-23ad-58af-bab2-2db2df086401','synthetic-destination',true,true,true,12,'10000000-0000-4000-8000-000000000011');`)
	if err != nil {
		t.Fatal(err)
	}
}
