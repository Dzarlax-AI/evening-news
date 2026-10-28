// Package migration contains the explicitly invoked database migration logic.
// It is not invoked during service startup.
package migration

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type migration struct {
	ID, Description string
	Needed          func(context.Context, pgx.Tx) (bool, error)
	Apply           func(context.Context, pgx.Tx) (map[string]any, error)
}

func execute(ctx context.Context, tx pgx.Tx, statements ...string) error {
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func hasColumns(ctx context.Context, tx pgx.Tx, table string, names []string) (bool, error) {
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=ANY($2)`, table, names).Scan(&count)
	return count == len(names), err
}

func earlyMigrations() []migration {
	schedulerNames := []string{"last_finished_at", "last_success_at", "last_status", "last_error", "last_duration_seconds"}
	articleNames := []string{"processing_state", "processing_attempts", "next_attempt_at", "last_processing_error"}
	return []migration{
		{"006_feed_performance_optimization", "Add performance indexes for feed API", func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var exists bool
			err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='idx_articles_fetched_at_desc')`).Scan(&exists)
			return !exists, err
		}, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			err := execute(ctx, tx,
				`CREATE INDEX IF NOT EXISTS idx_articles_fetched_at_desc ON articles(fetched_at DESC NULLS LAST)`,
				`CREATE INDEX IF NOT EXISTS idx_articles_is_ad_fetched_at ON articles(is_advertisement, fetched_at DESC)`,
				`CREATE INDEX IF NOT EXISTS idx_articles_published_fetched_at ON articles(published_at DESC, fetched_at DESC)`,
				`CREATE INDEX IF NOT EXISTS idx_articles_id_fetched_at ON articles(id, fetched_at DESC)`,
				`CREATE INDEX IF NOT EXISTS idx_articles_source_fetched_at ON articles(source_id, fetched_at DESC)`,
				`CREATE INDEX IF NOT EXISTS idx_articles_unprocessed ON articles(fetched_at DESC) WHERE NOT summary_processed`)
			return map[string]any{"indexes_created": 6, "errors": []string{}}, err
		}},
		{"007_scheduler_run_outcomes", "Persist scheduler success, failure, timeout, and error details", func(ctx context.Context, tx pgx.Tx) (bool, error) {
			yes, err := hasColumns(ctx, tx, "schedule_settings", schedulerNames)
			return !yes, err
		}, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			err := execute(ctx, tx,
				`ALTER TABLE schedule_settings ADD COLUMN IF NOT EXISTS last_finished_at TIMESTAMP`,
				`ALTER TABLE schedule_settings ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMP`,
				`ALTER TABLE schedule_settings ADD COLUMN IF NOT EXISTS last_status VARCHAR(20)`,
				`ALTER TABLE schedule_settings ADD COLUMN IF NOT EXISTS last_error TEXT`,
				`ALTER TABLE schedule_settings ADD COLUMN IF NOT EXISTS last_duration_seconds NUMERIC(12,3)`)
			return map[string]any{"columns_added_or_confirmed": schedulerNames}, err
		}},
		{"008_article_processing_state", "Persist bounded article processing outcomes and retries", func(ctx context.Context, tx pgx.Tx) (bool, error) {
			yes, err := hasColumns(ctx, tx, "articles", articleNames)
			if err != nil || !yes {
				return !yes, err
			}
			var pending bool
			err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM articles WHERE processing_state IS NULL)`).Scan(&pending)
			return pending, err
		}, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			err := execute(ctx, tx,
				`ALTER TABLE articles ADD COLUMN IF NOT EXISTS processing_state VARCHAR(20)`,
				`ALTER TABLE articles ADD COLUMN IF NOT EXISTS processing_attempts INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE articles ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP`,
				`ALTER TABLE articles ADD COLUMN IF NOT EXISTS last_processing_error TEXT`,
				`UPDATE articles SET processing_state=CASE WHEN processed IS TRUE AND summary_processed IS TRUE AND category_processed IS TRUE AND ad_processed IS TRUE THEN 'completed' ELSE 'pending' END WHERE processing_state IS NULL`,
				`ALTER TABLE articles ALTER COLUMN processing_state SET DEFAULT 'pending'`,
				`ALTER TABLE articles ALTER COLUMN processing_state SET NOT NULL`,
				`CREATE INDEX IF NOT EXISTS idx_articles_processing_due ON articles(processing_state,next_attempt_at,id) WHERE processing_state IN ('pending','retry_wait')`)
			return map[string]any{"columns_added_or_confirmed": articleNames}, err
		}},
	}
}
