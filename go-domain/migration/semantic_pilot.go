package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/028.sql
var semanticPilotSQL string

func semanticPilotMigration() migration {
	return migration{
		ID:          "028_semantic_shadow_pilot",
		Description: "Persist the semantic topic index checkpoint and embedding budget",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var exists bool
			err := tx.QueryRow(ctx, `SELECT to_regclass(format('%I.topic_shadow_semantic_index',current_schema())) IS NOT NULL`).Scan(&exists)
			return !exists, err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"semantic_shadow_index": true}, schemaSQL(ctx, tx, semanticPilotSQL)
		},
	}
}
