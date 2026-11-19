package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/025.sql
var feedDuplicatesSQL string

func feedDuplicatesMigration() migration {
	return migration{
		ID:          "025_feed_duplicate_predecessors",
		Description: "Expose exact duplicate article IDs for feed visibility",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var exists bool
			err := tx.QueryRow(ctx, `SELECT to_regclass(format('%I.article_feed_duplicate_predecessors', current_schema())) IS NOT NULL`).Scan(&exists)
			return !exists, err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"view_created": "article_feed_duplicate_predecessors"}, schemaSQL(ctx, tx, feedDuplicatesSQL)
		},
	}
}
