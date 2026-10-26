package repositories

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/management"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresManagementSourceCategoryArticleAndSummaryContracts(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required for isolated PostgreSQL management tests")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	ip := net.ParseIP(cfg.ConnConfig.Host)
	if cfg.ConnConfig.Host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		t.Fatalf("TEST_DATABASE_URL must use loopback; got %q", cfg.ConnConfig.Host)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("test_management_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	appCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	appCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, appCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ddl := []string{
		`CREATE TABLE sources(id serial PRIMARY KEY,name varchar(255) NOT NULL,source_type varchar(50) NOT NULL,url text NOT NULL,enabled boolean DEFAULT true,config jsonb DEFAULT '{}'::jsonb,fetch_interval integer DEFAULT 1800,last_fetch timestamp,last_success timestamp,last_error text,error_count integer,created_at timestamp,updated_at timestamp)`,
		`CREATE TABLE categories(id serial PRIMARY KEY,name varchar(50) NOT NULL UNIQUE,display_name varchar(100) NOT NULL,description text,color varchar(7) DEFAULT '#6c757d',is_local boolean NOT NULL DEFAULT false,created_at timestamp)`,
		`CREATE TABLE articles(id serial PRIMARY KEY,source_id integer REFERENCES sources(id) ON DELETE CASCADE,title text NOT NULL,url text NOT NULL UNIQUE,content text,summary text,image_url text,media_files jsonb DEFAULT '[]'::jsonb,published_at timestamp,fetched_at timestamp DEFAULT timezone('UTC',now()),processed boolean DEFAULT false,summary_processed boolean DEFAULT false,category_processed boolean DEFAULT false,processing_state varchar(20) NOT NULL DEFAULT 'pending',processing_attempts integer NOT NULL DEFAULT 0,next_attempt_at timestamp,last_processing_error text,is_advertisement boolean DEFAULT false,ad_confidence double precision DEFAULT 0,ad_type varchar(50),ad_reasoning text,ad_markers jsonb DEFAULT '[]'::jsonb,ad_processed boolean DEFAULT false)`,
		`CREATE TABLE article_categories(id serial PRIMARY KEY,article_id integer NOT NULL REFERENCES articles(id) ON DELETE CASCADE,category_id integer NOT NULL REFERENCES categories(id) ON DELETE CASCADE,confidence double precision DEFAULT 1,ai_category varchar(100),created_at timestamp DEFAULT timezone('UTC',now()),UNIQUE(article_id,category_id))`,
		`CREATE TABLE category_mapping(id serial PRIMARY KEY,ai_category varchar(100) NOT NULL UNIQUE,fixed_category varchar(50) NOT NULL,confidence_threshold double precision,description text,created_by varchar(100) DEFAULT 'system',usage_count integer DEFAULT 0,last_used timestamp,is_active boolean DEFAULT true,created_at timestamp,updated_at timestamp)`,
		`CREATE TABLE daily_summaries(id serial PRIMARY KEY,date date NOT NULL,category varchar(100) NOT NULL,summary_text text NOT NULL,articles_count integer NOT NULL,created_at timestamp DEFAULT timezone('UTC',now()),updated_at timestamp)`,
		`CREATE TABLE background_jobs(id uuid PRIMARY KEY,kind varchar(40) NOT NULL,payload jsonb NOT NULL DEFAULT '{}'::jsonb,state varchar(24) NOT NULL DEFAULT 'queued',created_at timestamp DEFAULT timezone('UTC',now()),finished_at timestamp,result jsonb,error_code text,schedule_key varchar(128))`,
		`CREATE TABLE background_worker_status(id integer PRIMARY KEY,generation bigint NOT NULL,status varchar(16) NOT NULL,heartbeat_at timestamp)`,
		`CREATE TABLE processing_stats(id serial PRIMARY KEY,date date UNIQUE,articles_fetched integer DEFAULT 0,articles_processed integer DEFAULT 0,api_calls_made integer DEFAULT 0,errors_count integer DEFAULT 0,processing_time_seconds integer DEFAULT 0)`,
		`CREATE TABLE ai_usage_tracking(id serial PRIMARY KEY,domain varchar(255) NOT NULL,analysis_type varchar(50) NOT NULL,tokens_used integer,credits_cost numeric(10,4),patterns_discovered integer DEFAULT 0,patterns_successful integer DEFAULT 0,created_at timestamp)`,
		`CREATE TABLE domain_stability(domain varchar(255) PRIMARY KEY,is_stable boolean DEFAULT false,success_rate_7d numeric(5,2) DEFAULT 0,success_rate_30d numeric(5,2) DEFAULT 0,total_attempts integer DEFAULT 0,successful_attempts integer DEFAULT 0,last_successful_extraction timestamp,last_failed_extraction timestamp,consecutive_successes integer DEFAULT 0,consecutive_failures integer DEFAULT 0)`,
		`CREATE TABLE extraction_attempts(id serial PRIMARY KEY,article_url text NOT NULL,domain varchar(255) NOT NULL,extraction_strategy varchar(255) NOT NULL,success boolean NOT NULL,content_length integer,quality_score numeric(5,2),extraction_time_ms integer,created_at timestamp DEFAULT timezone('UTC',now()))`,
		`CREATE TABLE settings(key varchar(255) PRIMARY KEY,value jsonb NOT NULL,description text,updated_at timestamp)`,
		`INSERT INTO background_worker_status VALUES(1,4,'running',timezone('UTC',now()))`,
	}
	for _, q := range ddl {
		if _, err = pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	r := NewManagementRepository(pool)
	active := false
	created, err := r.CreateSource(ctx, management.SourceCreate{Name: "Fixture Source", SourceType: "rss", URL: "https://fixture.invalid/feed", IsActive: &active})
	if err != nil {
		t.Fatal(err)
	}
	sourceID := created["id"].(int64)
	var sourceDefaultsOK bool
	if err = pool.QueryRow(ctx, `SELECT error_count=0 AND created_at IS NOT NULL AND updated_at IS NOT NULL FROM sources WHERE id=$1`, sourceID).Scan(&sourceDefaultsOK); err != nil || !sourceDefaultsOK {
		t.Fatalf("source ORM client defaults were not persisted: ok=%v err=%v", sourceDefaultsOK, err)
	}
	if created["is_active"] != false {
		t.Fatalf("source active default mismatch: %#v", created)
	}
	if _, err = r.CreateSource(ctx, management.SourceCreate{Name: "Fixture Source 2", SourceType: "telegram", URL: "https://fixture.invalid/channel"}); err != nil {
		t.Fatal(err)
	}
	sources, err := r.Sources(ctx)
	if err != nil || len(sources["sources"].([]map[string]any)) != 2 {
		t.Fatalf("sources list: %#v %v", sources, err)
	}
	interval := 900
	updated, err := r.UpdateSource(ctx, sourceID, management.SourceUpdate{UpdateInterval: &interval})
	if err != nil || updated["update_interval"] != interval {
		t.Fatalf("source update: %#v %v", updated, err)
	}
	toggled, err := r.ToggleSource(ctx, sourceID)
	if err != nil || toggled["is_active"] != true {
		t.Fatalf("source toggle: %#v %v", toggled, err)
	}
	catA, err := r.CreateCategory(ctx, management.CategoryCreate{Name: "Tech", DisplayName: "Technology", Color: ptrString("#123456")})
	if err != nil {
		t.Fatal(err)
	}
	catAID := catA["id"].(int64)
	var categoryCreated bool
	if err = pool.QueryRow(ctx, `SELECT created_at IS NOT NULL FROM categories WHERE id=$1`, catAID).Scan(&categoryCreated); err != nil || !categoryCreated {
		t.Fatalf("category ORM created_at default was not persisted: ok=%v err=%v", categoryCreated, err)
	}
	catB, err := r.CreateCategory(ctx, management.CategoryCreate{Name: "Business", DisplayName: "Business", Color: ptrString("#654321")})
	if err != nil {
		t.Fatal(err)
	}
	catBID := catB["id"].(int64)
	local := true
	updatedCat, err := r.UpdateCategory(ctx, catBID, management.CategoryUpdate{DisplayName: "Companies", IsLocal: &local})
	if err != nil || updatedCat["is_local"] != true {
		t.Fatalf("category update: %#v %v", updatedCat, err)
	}
	var localCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM categories WHERE is_local`).Scan(&localCount); err != nil || localCount != 1 {
		t.Fatalf("local category uniqueness update failed: %d %v", localCount, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO articles(source_id,title,url,summary,content,media_files,published_at,fetched_at,processed,summary_processed,category_processed,processing_state,processing_attempts) VALUES($1,'One','https://fixture.invalid/one','brief','body','[{"type":"image","url":"https://fixture.invalid/a.png"}]',date_trunc('second',timezone('UTC',now())),timezone('UTC',now()),true,true,true,'completed',2)`, sourceID); err != nil {
		t.Fatal(err)
	}
	var articleID int64
	if err = pool.QueryRow(ctx, `SELECT id FROM articles LIMIT 1`).Scan(&articleID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO article_categories(article_id,category_id,confidence,ai_category) VALUES($1,$2,.92,'AI Engineering')`, articleID, catAID); err != nil {
		t.Fatal(err)
	}
	article, err := r.Article(ctx, articleID)
	if err != nil {
		t.Fatal(err)
	}
	source := article["source"].(map[string]any)
	sourceName := source["name"].(*string)
	if article["primary_category"] != "AI Engineering" || article["processing_state"] != "completed" || *sourceName != "Fixture Source" {
		t.Fatalf("article detail mismatch: %#v", article)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO articles(source_id,title,url,processing_state) VALUES($1,'Second','https://fixture.invalid/second','pending')`, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err = r.DeleteSource(ctx, sourceID, false); err == nil {
		t.Fatal("expected delete refusal while articles remain")
	} else {
		var api *management.APIError
		if !errors.As(err, &api) || api.Status != 400 {
			t.Fatalf("wrong delete refusal: %v", err)
		}
	}

	threshold := .82
	mapping, err := r.UpsertMapping(ctx, management.MappingRequest{AICategory: "AI Engineering", FixedCategory: "Tech", ConfidenceThreshold: &threshold, Description: ptrString("fixture")})
	if err != nil {
		t.Fatal(err)
	}
	mappingID := mapping["id"].(int64)
	var mappingTimestamps bool
	if err = pool.QueryRow(ctx, `SELECT created_at IS NOT NULL AND updated_at IS NOT NULL FROM category_mapping WHERE id=$1`, mappingID).Scan(&mappingTimestamps); err != nil || !mappingTimestamps {
		t.Fatalf("mapping ORM timestamps were not persisted: ok=%v err=%v", mappingTimestamps, err)
	}
	createdBy := mapping["created_by"].(*string)
	if *createdBy != "admin" {
		t.Fatalf("unexpected mapping: %#v", mapping)
	}
	threshold = .91
	updatedMapping, err := r.UpsertMapping(ctx, management.MappingRequest{AICategory: "AI Engineering", FixedCategory: "Business", ConfidenceThreshold: &threshold})
	if err != nil || updatedMapping["id"] != mappingID || updatedMapping["fixed_category"] != "Business" {
		t.Fatalf("upsert should update existing mapping: %#v %v", updatedMapping, err)
	}
	mappings, err := r.Mappings(ctx, true)
	if err != nil || len(mappings) != 1 {
		t.Fatalf("mapping list: %#v %v", mappings, err)
	}
	inactive, err := r.ToggleMapping(ctx, mappingID)
	if err != nil || inactive["is_active"] != false {
		t.Fatalf("mapping toggle: %#v %v", inactive, err)
	}
	if _, err = r.DeleteMapping(ctx, mappingID); err != nil {
		t.Fatal(err)
	}
	if _, err = r.ApproveAIMapping(ctx, management.MappingRequest{AICategory: "Approved AI", FixedCategory: "Tech"}); err != nil {
		t.Fatal(err)
	}
	var approvedDefaultsOK bool
	if err = pool.QueryRow(ctx, `SELECT created_at IS NOT NULL AND updated_at IS NOT NULL AND usage_count=0 FROM category_mapping WHERE ai_category='Approved AI'`).Scan(&approvedDefaultsOK); err != nil || !approvedDefaultsOK {
		t.Fatalf("approved mapping ORM defaults were not persisted: ok=%v err=%v", approvedDefaultsOK, err)
	}
	fixed, err := r.FixedCategories(ctx)
	if err != nil || len(fixed["fixed_categories"].([]string)) != 2 {
		t.Fatalf("fixed categories: %#v %v", fixed, err)
	}

	if _, err = pool.Exec(ctx, `INSERT INTO daily_summaries(date,category,summary_text,articles_count) VALUES('2026-10-26','Tech','summary',1),('2026-10-25','Business','older',2)`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO processing_stats(date,articles_fetched,articles_processed,api_calls_made,errors_count,processing_time_seconds) VALUES(timezone('UTC',now())::date,4,3,6,1,12); INSERT INTO ai_usage_tracking(domain,analysis_type,tokens_used,credits_cost,patterns_discovered,patterns_successful,created_at) VALUES('fixture.invalid','selector_discovery',120,.25,2,1,timezone('UTC',now())); INSERT INTO domain_stability(domain,is_stable,success_rate_7d,success_rate_30d,total_attempts,successful_attempts) VALUES('fixture.invalid',true,90,85,10,9); INSERT INTO extraction_attempts(article_url,domain,extraction_strategy,success,quality_score,extraction_time_ms) VALUES('https://fixture.invalid/1','fixture.invalid','rss',true,80,10),('https://fixture.invalid/2','fixture.invalid','rss',false,60,20)`); err != nil {
		t.Fatal(err)
	}
	dashboard, err := r.DashboardStats(ctx, 7)
	if err != nil || dashboard["total_articles"] != int64(2) {
		t.Fatalf("dashboard stats: %#v %v", dashboard, err)
	}
	usage, err := r.AIUsageStats(ctx, 30)
	if err != nil || usage["total"].(map[string]any)["tokens"] != int64(120) {
		t.Fatalf("AI usage stats: %#v %v", usage, err)
	}
	processing, err := r.ProcessingStats(ctx, 30)
	if err != nil || processing["totals"].(map[string]int64)["articles_processed"] != 3 {
		t.Fatalf("processing stats: %#v %v", processing, err)
	}
	extractor, err := r.ExtractorStats(ctx)
	if err != nil || len(extractor["domain_stats"].([]map[string]any)) != 1 {
		t.Fatalf("extractor stats: %#v %v", extractor, err)
	}
	chat, serviceChat := "-100fixture", "-100service"
	if err = r.UpdateTelegramSettings(ctx, management.TelegramSettings{ChatID: &chat, ServiceChatID: &serviceChat}); err != nil {
		t.Fatal(err)
	}
	var storedChat, storedService string
	if err = pool.QueryRow(ctx, `SELECT value #>> '{}' FROM settings WHERE key='telegram_chat_id'`).Scan(&storedChat); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT value #>> '{}' FROM settings WHERE key='telegram_service_chat_id'`).Scan(&storedService); err != nil {
		t.Fatal(err)
	}
	if storedChat != chat || storedService != serviceChat {
		t.Fatalf("telegram overrides mismatch: %q %q", storedChat, storedService)
	}
	var settingsTimestamp bool
	if err = pool.QueryRow(ctx, `SELECT bool_and(updated_at IS NOT NULL) FROM settings`).Scan(&settingsTimestamp); err != nil || !settingsTimestamp {
		t.Fatalf("settings ORM updated_at default was not persisted: ok=%v err=%v", settingsTimestamp, err)
	}
	date := "2026-10-26"
	daily, err := r.DailySummaries(ctx, management.SummaryQuery{Limit: 10, Date: &date})
	if err != nil || daily["total"] != 1 {
		t.Fatalf("daily summaries: %#v %v", daily, err)
	}
	cats, err := r.SummaryCategories(ctx)
	if err != nil || cats["total_categories"] != 2 {
		t.Fatalf("summary categories: %#v %v", cats, err)
	}
	var sumID int64
	if err = pool.QueryRow(ctx, `SELECT id FROM daily_summaries WHERE category='Tech'`).Scan(&sumID); err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(ctx, sumID)
	if err != nil || summary["summary_text"] != "summary" {
		t.Fatalf("summary: %#v %v", summary, err)
	}
	status, err := r.ProcessingStatus(ctx)
	if err != nil || status["worker_available"] != true || status["queued_jobs"] != int64(0) {
		t.Fatalf("processing status: %#v %v", status, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,state,created_at,finished_at,result,error_code,schedule_key) VALUES (gen_random_uuid(),'news_processing','{}','succeeded',timezone('UTC',now())-interval '2 hours',timezone('UTC',now())-interval '1 hour','{"status":"success","duration_seconds":10}',NULL,'news:fetch'),(gen_random_uuid(),'news_processing','{}','failed',timezone('UTC',now())-interval '1 minute',timezone('UTC',now()),NULL,'worker_error','news:fetch'),(gen_random_uuid(),'news_digest','{}','running',timezone('UTC',now()),NULL,NULL,NULL,'news:digest')`); err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewScheduleJobsSnapshot(pool).Snapshot(ctx, []string{"news:fetch", "news:digest"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Latest["news:fetch"].State != "failed" || snapshot.LastSuccess["news:fetch"].Result["status"] != "success" || snapshot.Running["news:digest"] != true {
		t.Fatalf("schedule job snapshot mismatch: %#v", snapshot)
	}
	deleted, err := r.DeleteSource(ctx, sourceID, true)
	if err != nil || deleted["deleted_articles"] != int64(2) {
		t.Fatalf("purged source: %#v %v", deleted, err)
	}
}
func ptrString(v string) *string { return &v }
