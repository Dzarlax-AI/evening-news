package repositories

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This test uses an isolated schema on a loopback PostgreSQL 16/17 instance.
// Its fixture contains the exact readiness contract but no application data;
// CheckSchema must only inspect it and must never repair it with DDL.
func TestCheckSchemaRequiresCompleteBootstrapWithoutDDL(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var version int
	if err := admin.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 160000 || version >= 180000 {
		t.Fatalf("schema readiness integration requires PostgreSQL 16 or 17, got server_version_num=%d", version)
	}

	tests := []struct {
		name      string
		missing   string
		wantReady bool
	}{
		{name: "complete schema", wantReady: true},
		{name: "missing table", missing: "article_processing_items"},
		{name: "missing column", missing: "articles.processing_attempts"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := readinessTestSchema(t, ctx, admin)
			if err := createReadinessFixture(ctx, admin, schema, tc.missing); err != nil {
				t.Fatal(err)
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
			before := readinessObjectCounts(t, ctx, pool)
			err = (&Postgres{pool: pool}).CheckSchema(ctx)
			if tc.wantReady && err != nil {
				t.Fatalf("complete migrated schema rejected: %v", err)
			}
			if !tc.wantReady && err == nil {
				t.Fatal("incomplete schema was reported ready")
			}
			after := readinessObjectCounts(t, ctx, pool)
			if after != before {
				t.Fatalf("CheckSchema changed schema object counts: before=%v after=%v", before, after)
			}
		})
	}
}

func readinessTestSchema(t *testing.T, ctx context.Context, admin *pgxpool.Pool) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	schema := "web_ready_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, `DROP SCHEMA `+schema+` CASCADE`)
	})
	return schema
}

func createReadinessFixture(ctx context.Context, admin *pgxpool.Pool, schema, missing string) error {
	tables := []string{
		"articles", "sources", "categories", "article_categories", "category_mapping",
		"daily_summaries", "settings", "ai_usage_tracking", "processing_stats", "extraction_attempts",
		"background_jobs", "background_job_idempotency_keys", "background_worker_status",
		"scheduler_occurrence_receipts", "background_job_executions", "article_ingestion_runs",
		"article_ingestion_source_runs", "article_processing_runs", "article_processing_items",
	}
	articleColumns := "processing_state text, processing_attempts integer, next_attempt_at timestamptz, summary_processed boolean, category_processed boolean, ad_processed boolean"
	jobColumns := "schedule_key text, schedule_revision integer, occurrence_id text, scheduled_for timestamptz, attempt_generation integer"
	for _, table := range tables {
		if missing == table {
			continue
		}
		columns := "id bigint"
		if table == "articles" {
			columns += ", " + articleColumns
			if missing == "articles.processing_attempts" {
				columns = "id bigint, processing_state text, next_attempt_at timestamptz, summary_processed boolean, category_processed boolean, ad_processed boolean"
			}
		}
		if table == "background_jobs" {
			columns += ", " + jobColumns
		}
		if _, err := admin.Exec(ctx, `CREATE TABLE `+schema+`.`+table+` (`+columns+`)`); err != nil {
			return err
		}
	}
	return nil
}

func readinessObjectCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) [2]int {
	t.Helper()
	var tables, columns int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema()`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema()`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	return [2]int{tables, columns}
}
