package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/022.sql
var articleDedupSQL string

func articleDedupMigration() migration {
	return migration{
		ID:          "022_article_dedup_trial",
		Description: "Add versioned article deduplication trial observations",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var exists bool
			err := tx.QueryRow(ctx, `SELECT to_regclass(current_schema() || '.article_dedup_observations') IS NOT NULL`).Scan(&exists)
			return !exists, err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"article_dedup_trial": true}, schemaSQL(ctx, tx, articleDedupSQL)
		},
	}
}
