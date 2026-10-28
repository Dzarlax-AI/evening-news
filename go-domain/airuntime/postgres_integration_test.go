package airuntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Uses only an explicit loopback TEST_DATABASE_URL and an isolated random
// schema. Runtime SQL is exercised against synthetic tables, never production.
func TestPostgresMetadataAndUsageStores(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") == "true" {
			t.Fatal("TEST_DATABASE_URL is required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set; PostgreSQL integration is opt-in")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("TEST_DATABASE_URL must use loopback host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("loopback test database unavailable")
	}
	schema := fmt.Sprintf("ai_runtime_test_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal("create isolated test schema failed")
	}
	t.Cleanup(func() {
		clean, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_, _ = admin.Exec(clean, "DROP SCHEMA "+q+" CASCADE")
		_ = admin.Close(context.Background())
	})
	statements := []string{
		`CREATE TABLE ` + q + `.categories(name varchar(50) NOT NULL,display_name varchar(100) NOT NULL,is_local boolean DEFAULT false)`,
		`CREATE TABLE ` + q + `.category_mapping(ai_category varchar(100) NOT NULL,fixed_category varchar(50) NOT NULL,usage_count integer DEFAULT 0)`,
		`CREATE TABLE ` + q + `.ai_usage_tracking(id serial primary key,domain varchar(255) NOT NULL,analysis_type varchar(50) NOT NULL,tokens_used integer,credits_cost numeric(10,4),analysis_result jsonb,patterns_discovered integer DEFAULT 0,patterns_successful integer DEFAULT 0,cost_effectiveness numeric(5,2),created_at timestamp DEFAULT now())`,
		`INSERT INTO ` + q + `.categories(name,display_name,is_local) VALUES('Business','Business',false),('Local','Local',true),('Unknown','Unknown',NULL)`,
	}
	for _, statement := range statements {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatalf("create isolated runtime fixture: %v", err)
		}
	}
	for i := 0; i < 7; i++ {
		if _, err = admin.Exec(ctx, "INSERT INTO "+q+`.category_mapping(ai_category,fixed_category,usage_count) VALUES($1,'Business',$2)`, fmt.Sprintf("Example%d", i), i); err != nil {
			t.Fatal("populate mapping fixture failed")
		}
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse test database config failed")
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	poolConfig.MaxConns = 2
	poolConfig.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("create isolated native pool failed")
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		t.Fatal("isolated native pool ping failed")
	}
	if pool.Config().MaxConns > 2 {
		t.Fatalf("native test pool exceeds two: %d", pool.Config().MaxConns)
	}
	metadata := NewMetadataStore(pool)
	categories, local, err := metadata.loadDB(ctx)
	if err != nil {
		t.Fatalf("metadata SELECT failed: %v", err)
	}
	if local == nil || *local != "Local" || len(categories) != 3 {
		t.Fatalf("metadata categories=%#v local=%v", categories, local)
	}
	if len(categories[0].Examples) != 5 || categories[0].Examples[0] != "Example6" {
		t.Fatalf("mapping order/cap mismatch: %#v", categories[0].Examples)
	}
	metadata.snapshot = &metadataSnapshot{categories: categories, local: local, loaded: metadata.now()}
	cached, _ := metadata.Load(ctx)
	if len(cached[0].Examples) != 5 {
		t.Fatal("metadata cache lost loaded mappings")
	}
	metadata.Invalidate()
	if metadata.snapshot != nil {
		t.Fatal("metadata invalidation did not clear cache")
	}
	server := NewServer(Config{InputRate: .5, OutputRate: 1.5, CachedRate: .1}, pool)
	record := usageRecord{domain: "fixture.invalid", analysisType: "synthetic_test", usage: analysis.Usage{PromptTokens: 9, CompletionTokens: 3, CachedTokens: 2, TotalTokens: 14}}
	server.storeUsage(record)
	if server.usageFailed.Load() != 0 {
		t.Fatal("best-effort usage INSERT failed")
	}
	var tokens int
	var cost string
	var payload []byte
	if err = pool.QueryRow(ctx, "SELECT tokens_used,credits_cost::text,analysis_result FROM ai_usage_tracking WHERE domain=$1 AND analysis_type=$2", record.domain, record.analysisType).Scan(&tokens, &cost, &payload); err != nil {
		t.Fatal("usage INSERT was not durable")
	}
	if tokens != 14 || cost != "0.0000" {
		t.Fatalf("usage row tokens=%d cost=%s", tokens, cost)
	}
	var details map[string]int
	if err = json.Unmarshal(payload, &details); err != nil || details["cached_tokens"] != 2 {
		t.Fatalf("usage result=%v err=%v", details, err)
	}
	if _, err = OpenDatabase(ctx, Config{DatabaseURL: dsn, DBMaxConns: 3}); err == nil || !strings.Contains(err.Error(), "pool") {
		t.Fatalf("pool limit was not enforced: %v", err)
	}
}
