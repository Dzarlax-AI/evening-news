package repositories

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This integration fixture deliberately uses an isolated schema and refuses
// non-loopback hosts. It exercises the public read SQL against PostgreSQL 17.
func TestPostgresPublicReaderMultiCategoryAndReadPaths(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" {
		t.Fatalf("refusing non-loopback TEST_DATABASE_URL host %q", parsed.Hostname())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "web_public_" + hex.EncodeToString(random[:])
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		_, _ = admin.Exec(dropCtx, `DROP SCHEMA `+schema+` CASCADE`)
	}()
	ddl := []string{
		`CREATE TABLE ` + schema + `.sources(id bigint PRIMARY KEY,name text NOT NULL,enabled boolean NOT NULL DEFAULT true)`,
		`CREATE TABLE ` + schema + `.categories(id bigint PRIMARY KEY,name text NOT NULL,display_name text NOT NULL,color text NOT NULL)`,
		`CREATE TABLE ` + schema + `.articles(id bigint PRIMARY KEY,source_id bigint,title text NOT NULL,summary text,content text,url text NOT NULL,image_url text,media_files jsonb,published_at timestamp,fetched_at timestamp,is_advertisement boolean,ad_confidence real,ad_type text,ad_reasoning text,ad_markers jsonb,processing_state text,ad_processed boolean)`,
		`CREATE TABLE ` + schema + `.article_categories(id bigint PRIMARY KEY,article_id bigint NOT NULL,category_id bigint NOT NULL,confidence real,ai_category text)`,
		`CREATE TABLE ` + schema + `.article_localizations(article_id bigint,locale text,title text,summary text,status text)`,
		`CREATE TABLE ` + schema + `.category_localizations(category_id bigint,locale text,display_name text)`,
		`CREATE TABLE ` + schema + `.daily_summaries(id bigint PRIMARY KEY,date date NOT NULL,category text NOT NULL,summary_text text NOT NULL,articles_count integer NOT NULL)`,
		`CREATE TABLE ` + schema + `.editorial_digest_editions(id uuid PRIMARY KEY,edition_date date NOT NULL,locale varchar(16) NOT NULL,policy_version varchar(32),snapshot_hash varchar(64),snapshot jsonb NOT NULL,edition jsonb NOT NULL,rendered jsonb NOT NULL,provider_usage jsonb NOT NULL DEFAULT '{}'::jsonb,job_id uuid,attempt_generation integer,created_at timestamp NOT NULL DEFAULT timezone('UTC',now()))`,
	}
	for _, statement := range ddl {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	seed := []string{
		`INSERT INTO ` + schema + `.sources(id,name,enabled) VALUES(7,'Fixture source',true)`,
		`INSERT INTO ` + schema + `.categories(id,name,display_name,color) VALUES(1,'tech','Технологии','#123456'),(2,'science','Наука','#abcdef')`,
		`INSERT INTO ` + schema + `.articles(id,source_id,title,summary,content,url,image_url,media_files,published_at,fetched_at,is_advertisement,ad_confidence,processing_state,ad_processed,ad_markers) VALUES(42,7,'Тестовый заголовок','Краткое содержание: Короткий пересказ','Полный исходный текст','https://example.test/story/42',NULL,'[]','2026-10-26 09:00:00','2026-10-26 09:05:00',false,0,'completed',false,'[]')`,
		// Python's DataService uses relationship iteration order for primary category.
		// The deliberately lower confidence on the first link catches accidental
		// reordering by confidence in the Go projection.
		`INSERT INTO ` + schema + `.article_categories(id,article_id,category_id,confidence,ai_category) VALUES(1,42,1,0.2,'Technology'),(2,42,2,0.95,'Science')`,
		`INSERT INTO ` + schema + `.daily_summaries(id,date,category,summary_text,articles_count) VALUES(1,'2026-10-26','Технологии','Новости дня',1)`,
	}
	for _, statement := range seed {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	readerCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	readerCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, readerCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	reader := &Postgres{pool: pool}
	if _, err := pool.Exec(ctx, `INSERT INTO article_localizations VALUES (42,'sr-Latn','Probni naslov','Kratak pregled','completed'); INSERT INTO category_localizations VALUES (1,'sr-Latn','Tehnologija')`); err != nil {
		t.Fatal(err)
	}
	sr := WithPublicLocale(ctx, "sr-Latn")
	localized, err := reader.Feed(sr, FeedQuery{Limit: 20, HideAds: true})
	if err != nil || len(localized.Articles) != 1 || localized.Articles[0]["title"] != "Probni naslov" {
		t.Fatalf("Serbian feed = %#v, %v", localized, err)
	}
	results, err := reader.Search(sr, SearchQuery{Query: "Probni", Limit: 20, HideAds: true})
	if err != nil || results["results_count"] != 1 {
		t.Fatalf("Serbian search = %#v, %v", results, err)
	}
	results, err = reader.Search(sr, SearchQuery{Query: "Тестовый", Limit: 20, HideAds: true})
	if err != nil || results["results_count"] != 0 {
		t.Fatalf("Russian search matched Serbian index = %#v, %v", results, err)
	}
	localizedAPIFeed, err := reader.APIFeed(sr, APIFeedQuery{Limit: 20})
	if err != nil || len(localizedAPIFeed.Items) != 1 || stringValue(localizedAPIFeed.Items[0]["title"]) != "Probni naslov" {
		t.Fatalf("Serbian API feed: %#v, %v", localizedAPIFeed, err)
	}
	localizedRSSArticles, err := reader.RSS(sr, 20)
	if err != nil || len(localizedRSSArticles) != 1 || localizedRSSArticles[0].Title == nil || *localizedRSSArticles[0].Title != "Probni naslov" || localizedRSSArticles[0].Summary == nil || *localizedRSSArticles[0].Summary != "Kratak pregled" {
		t.Fatalf("Serbian RSS: %#v, %v", localizedRSSArticles, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE article_localizations SET status='retry_wait' WHERE article_id=42`); err != nil {
		t.Fatal(err)
	}
	localized, err = reader.Feed(sr, FeedQuery{Limit: 20, HideAds: true})
	if err != nil || len(localized.Articles) != 0 {
		t.Fatalf("unready article included = %#v, %v", localized, err)
	}
	localizedAPIFeed, err = reader.APIFeed(sr, APIFeedQuery{Limit: 20})
	if err != nil || len(localizedAPIFeed.Items) != 0 {
		t.Fatalf("unready API feed: %#v, %v", localizedAPIFeed, err)
	}
	localizedRSSArticles, err = reader.RSS(sr, 20)
	if err != nil || len(localizedRSSArticles) != 0 {
		t.Fatalf("unready RSS: %#v, %v", localizedRSSArticles, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE article_localizations SET status='completed' WHERE article_id=42`); err != nil {
		t.Fatal(err)
	}
	snapshot := json.RawMessage(`{"date":"2026-10-26","cutoff_utc":"2026-10-26T09:00:00Z","site_base_url":"https://example.test","edition_url":"https://example.test/digest/2026-10-26","articles":[{"id":"42","title":"Frozen snapshot title","summary":"Frozen source summary","content":"Frozen source body","source_name":"Frozen source","url":"https://snapshot.example/story/42","category_id":"1","category_name":"Технологии"}]}`)
	editionJSON := json.RawMessage(`{"version":"1","locale":"ru","date":"2026-10-26","cutoff_utc":"2026-10-26T09:00:00Z","site_base_url":"https://example.test","edition_url":"https://example.test/digest/2026-10-26","source_snapshot_hash":"snapshot-hash","content_hash":"content-hash","stories":[{"id":"main-1","kind":"main","title":"Frozen editorial headline","summary":"Frozen editorial summary","primary_article_id":"42","article_ids":["42"],"evidence":[]}],"excluded":[]}`)
	const editionID = "8c93bb34-383d-449c-9e85-28aa9121a4fe"
	if _, err = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,rendered) VALUES($1,'2026-10-26','ru','snapshot-hash',$2,$3,'{}')`, editionID, snapshot, editionJSON); err != nil {
		t.Fatal(err)
	}
	const pairID = "42a8bb34-383d-449c-9e85-28aa9121a4fe"
	const pairJob = "d490bb34-383d-449c-9e85-28aa9121a4fe"
	if _, err = pool.Exec(ctx, `UPDATE editorial_digest_editions SET job_id=$2,attempt_generation=1 WHERE id=$1`, editionID, pairJob); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation) VALUES($1,'2026-10-26','sr-Latn','snapshot-hash',$2,$3,'{}',$4,1)`, pairID, snapshot, editionJSON, pairJob); err != nil {
		t.Fatal(err)
	}
	pair, err := reader.EditorialDigestCounterpart(ctx, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), editionID, "ru", "sr-Latn")
	if err != nil || pair != pairID {
		t.Fatalf("paired edition %q %v", pair, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation) VALUES('52a8bb34-383d-449c-9e85-28aa9121a4fe','2026-10-26','sr-Latn','snapshot-hash',$1,$2,'{}',$3,1)`, snapshot, editionJSON, pairJob); err != nil {
		t.Fatal(err)
	}
	pair, err = reader.EditorialDigestCounterpart(ctx, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), editionID, "ru", "sr-Latn")
	if err != nil || pair != "" {
		t.Fatalf("ambiguous pairing should fail closed: %q %v", pair, err)
	}
	feed, err := reader.Feed(ctx, FeedQuery{Limit: 20, HideAds: true})
	if err != nil {
		t.Fatal(err)
	}
	if feed.Total != 1 || len(feed.Articles) != 1 {
		t.Fatalf("unexpected feed: %+v", feed)
	}
	if got := feed.Articles[0]["category"]; got != "Технологии" {
		t.Fatalf("primary category order changed: %v", got)
	}
	categories, ok := feed.Articles[0]["categories"].([]any)
	if !ok || len(categories) != 2 {
		t.Fatalf("multi-category values lost: %#v", feed.Articles[0]["categories"])
	}
	article, err := reader.Article(ctx, 42, false)
	if err != nil {
		t.Fatal(err)
	}
	if article == nil || article["content"] != "Полный исходный текст" {
		t.Fatalf("article mapping differs: %#v", article)
	}
	search, err := reader.Search(ctx, SearchQuery{Query: "Тестовый", Limit: 20, Sort: "relevance", HideAds: true})
	if err != nil {
		t.Fatal(err)
	}
	if search["results_count"] != 1 {
		t.Fatalf("search result missing: %#v", search)
	}
	config, err := reader.CategoriesConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := config["categories"].(map[string]any)["tech"]; !ok {
		t.Fatalf("category config missing: %#v", config)
	}
	stats, err := reader.Categories(ctx, StatsQuery{HideAds: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats["total"] != 1 {
		t.Fatalf("category totals mismatch: %#v", stats)
	}
	sourceStats, err := reader.Sources(ctx, StatsQuery{HideAds: true})
	if err != nil {
		t.Fatal(err)
	}
	if sourceStats["total"] != 1 {
		t.Fatalf("source totals mismatch: %#v", sourceStats)
	}
	apiFeed, err := reader.APIFeed(ctx, APIFeedQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(apiFeed.Items) != 1 {
		t.Fatalf("API feed missing: %+v", apiFeed)
	}
	rss, err := reader.RSS(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(rss) != 1 {
		t.Fatalf("RSS result missing: %+v", rss)
	}
	digest, err := reader.Digest(ctx, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(digest.Summaries) != 1 || len(digest.Categories) != 2 {
		t.Fatalf("digest mapping mismatch: %+v", digest)
	}
	if _, err = pool.Exec(ctx, `UPDATE articles SET title='Mutated live title',url='https://mutated.example/story/42' WHERE id=42`); err != nil {
		t.Fatal(err)
	}
	editorial, err := reader.EditorialDigest(ctx, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), editionID, "ru")
	if err != nil {
		t.Fatal(err)
	}
	if editorial == nil || editorial.Snapshot.Articles[0].Title != "Frozen snapshot title" || editorial.Snapshot.Articles[0].URL != "https://snapshot.example/story/42" || editorial.Edition.Stories[0].Title != "Frozen editorial headline" {
		t.Fatalf("immutable editorial snapshot changed with current article rows: %#v", editorial)
	}
	latest, err := reader.EditorialDigest(ctx, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), "", "ru")
	if err != nil || latest == nil || latest.ID != editionID {
		t.Fatalf("latest editorial edition lookup failed: edition=%#v err=%v", latest, err)
	}
}

func TestPostgresEditorialDigestStatusDoesNotNeedLegacySummaries(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" {
		t.Fatalf("refusing non-loopback TEST_DATABASE_URL host %q", parsed.Hostname())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "web_editorial_status_" + hex.EncodeToString(random[:])
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) }()
	readerCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	readerCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, readerCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE editorial_digest_editions(id uuid PRIMARY KEY,edition_date date NOT NULL,locale varchar(16) NOT NULL,policy_version varchar(32),snapshot_hash varchar(64),snapshot jsonb NOT NULL,edition jsonb NOT NULL,rendered jsonb NOT NULL,provider_usage jsonb NOT NULL DEFAULT '{}'::jsonb,job_id uuid,attempt_generation integer,created_at timestamp NOT NULL DEFAULT timezone('UTC',now()))`); err != nil {
		t.Fatal(err)
	}
	snapshot := json.RawMessage(`{"date":"2026-10-27","cutoff_utc":"2026-10-27T09:00:00Z","site_base_url":"https://example.test","edition_url":"https://example.test/digest/2026-10-27","articles":[{"id":"42","title":"Frozen title","summary":"Frozen summary","content":"Private frozen source content","source_name":"Source","url":"https://source.example/42","category_id":"1","category_name":"Технологии"},{"id":"43","title":"Second title","source_name":"Source 2","url":"https://source.example/43","category_id":"2","category_name":"Мир"}]}`)
	issued := json.RawMessage(`{"version":"1","locale":"ru","date":"2026-10-27","cutoff_utc":"2026-10-27T09:00:00Z","site_base_url":"https://example.test","edition_url":"https://example.test/digest/2026-10-27","source_snapshot_hash":"snapshot-hash","content_hash":"content-hash","stories":[{"id":"main-1","kind":"main","title":"Main story","summary":"Main copy","primary_article_id":"42","article_ids":["42"],"evidence":[]},{"id":"brief-1","kind":"brief","title":"Short item","summary":"Brief copy","primary_article_id":"43","article_ids":["43"],"evidence":[]}],"excluded":[]}`)
	const editionID = "8c93bb34-383d-449c-9e85-28aa9121a4fe"
	if _, err := pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,rendered) VALUES($1,'2026-10-27','ru','snapshot-hash',$2,$3,'{}')`, editionID, snapshot, issued); err != nil {
		t.Fatal(err)
	}
	reader := &Postgres{pool: pool}
	status, err := reader.EditorialDigestStatus(ctx, time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), "ru")
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.EditionID != editionID || status.Date != "2026-10-27" || status.Locale != "ru" || status.MainCount != 1 || status.BriefCount != 1 || status.StoryCount != 2 || status.ArticleCount != 2 {
		t.Fatalf("editorial status mismatch with zero daily_summaries table: %+v", status)
	}
	missing, err := reader.EditorialDigestStatus(ctx, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), "ru")
	if err != nil || missing.Ready || missing.EditionID != "" || missing.ArticleCount != 0 {
		t.Fatalf("missing edition was presented as ready: status=%+v err=%v", missing, err)
	}
}
