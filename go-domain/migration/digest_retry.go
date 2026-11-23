package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/026.sql
var digestRetrySQL string

func digestRetryMigration() migration {
	return migration{
		ID:          "026_scheduled_digest_retry",
		Description: "Track guarded digest attempts and persisted retry times",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var columns int
			err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema()
				AND ((table_name='background_jobs' AND column_name='digest_retry_at')
				 OR (table_name='background_job_executions' AND column_name='digest_delivery_started'))`).Scan(&columns)
			return columns != 2, err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"scheduled_digest_retry": true}, schemaSQL(ctx, tx, digestRetrySQL)
		},
	}
}
