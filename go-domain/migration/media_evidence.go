package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/024.sql
var mediaEvidenceSQL string

func mediaEvidenceMigration() migration {
	return migration{
		ID:          "024_article_media_evidence",
		Description: "Preserve verified editorial cover evidence independently of article media",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var exists bool
			err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema=current_schema() AND table_name='articles' AND column_name='media_evidence')`).Scan(&exists)
			return !exists, err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"column_created": "articles.media_evidence"}, schemaSQL(ctx, tx, mediaEvidenceSQL)
		},
	}
}
