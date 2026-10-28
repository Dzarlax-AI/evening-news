package migration

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
)

//go:embed sql/012.sql
var ingestionSQL string

//go:embed sql/013.sql
var processingSQL string

//go:embed sql/014.sql
var editorialSQL string

//go:embed sql/015.sql
var deliverySQL string

func schemaSQL(ctx context.Context, tx pgx.Tx, sql string) error {
	var schema string
	if err := tx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return err
	}
	if schema == "" {
		return fmt.Errorf("could not resolve active database schema")
	}
	sql = strings.ReplaceAll(sql, "__SCHEMA_Q__", pgx.Identifier{schema}.Sanitize())
	sql = strings.ReplaceAll(sql, "__SCHEMA_LITERAL__", "'"+strings.ReplaceAll(schema, "'", "''")+"'")
	return execute(ctx, tx, sql)
}
func fencedMigration(id, description, sql string, tables []string, fn string, result map[string]any) migration {
	return migration{ID: id, Description: description, Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var count int
		err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=ANY($1)`, tables).Scan(&count)
		if err != nil {
			return false, err
		}
		var found bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND p.proname=$1)`, fn).Scan(&found)
		return count != len(tables) || !found, err
	}, Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) { return result, schemaSQL(ctx, tx, sql) }}
}
func ingestionMigration() migration {
	return fencedMigration("012_article_ingestion", "Add fenced, resumable Go article ingestion", ingestionSQL, []string{"article_ingestion_runs", "article_ingestion_source_runs", "source_collection_state"}, "fence_article_ingestion_attempt", map[string]any{"tables_created_or_confirmed": []string{"article_ingestion_runs", "article_ingestion_source_runs", "source_collection_state"}, "fence_function": "fence_article_ingestion_attempt"})
}
func processingMigration() migration {
	return fencedMigration("013_article_processing", "Add fenced Go article processing run and item receipts", processingSQL, []string{"article_processing_runs", "article_processing_items"}, "fence_article_processing_attempt", map[string]any{"tables_created": []string{"article_processing_runs", "article_processing_items"}, "fence_function": "fence_article_processing_attempt"})
}
func editorialMigration() migration {
	return migration{ID: "014_editorial_digest", Description: "Persist immutable Russian editorial editions", Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var table, index, dateIndex, trigger bool
		err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='editorial_digest_editions'), EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname='uq_editorial_digest_job_attempt_locale'), EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname='idx_editorial_digest_date_locale_created'), EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname='editorial_digest_editions' AND t.tgname='trg_editorial_digest_immutable' AND NOT t.tgisinternal)`).Scan(&table, &index, &dateIndex, &trigger)
		return !table || !index || !dateIndex || !trigger, err
	}, Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		return map[string]any{"tables_created_or_confirmed": []string{"editorial_digest_editions"}, "indexes_created_or_confirmed": []string{"uq_editorial_digest_job_attempt_locale", "idx_editorial_digest_date_locale_created"}}, schemaSQL(ctx, tx, editorialSQL)
	}}
}
func deliveryMigration() migration {
	return migration{ID: "015_digest_delivery", Description: "Persist digest delivery receipts", Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var exists bool
		err := tx.QueryRow(ctx, `SELECT to_regclass(current_schema() || '.digest_delivery_intents') IS NOT NULL AND to_regclass(current_schema() || '.digest_delivery_parts') IS NOT NULL`).Scan(&exists)
		return !exists, err
	}, Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		return map[string]any{"tables_created_or_confirmed": []string{"digest_delivery_intents", "digest_delivery_parts"}}, schemaSQL(ctx, tx, deliverySQL)
	}}
}
