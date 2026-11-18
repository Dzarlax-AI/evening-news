package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/023.sql
var telegramContextSQL string

func telegramContextMigration() migration {
	return migration{
		ID:          "023_telegram_source_context",
		Description: "Preserve Telegram source text and attachment/link metadata for safe article admission",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var exists bool
			err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='articles' AND column_name='telegram_context')`).Scan(&exists)
			return !exists, err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"column_created": "articles.telegram_context"}, schemaSQL(ctx, tx, telegramContextSQL)
		},
	}
}
