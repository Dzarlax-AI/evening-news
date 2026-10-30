package digestworkflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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
	return pool
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
	provider := localeTestProvider(func(_ context.Context, r ai.Request) (ai.Response, error) {
		calls++
		var output any
		if calls == 1 {
			output = map[string]any{"ru": draftFor(ids, ruSummaries, false), "sr-Latn": map[string]any{}}
		} else if calls == 2 {
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
	if err != nil || n != 1 {
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
	n, err = wf.RetrySerbian(ctx, job, 1, op, cutoff.Add(24*time.Hour), ruID)
	if err != nil || n != 1 || calls != 2 {
		t.Fatalf("retry %d calls=%d err=%v", n, calls, err)
	}
	var ready bool
	if ready, err = wf.hasSerbian(ctx, serbianEditionID(ruID)); err != nil || !ready {
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
	if err != nil || n != 1 || calls != 3 {
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
