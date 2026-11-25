package migration

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/027.sql
var topicShadowSQL string

func topicShadowMigration() migration {
	return migration{
		ID:          "027_topic_shadow",
		Description: "Persist isolated news topic shadow observations",
		Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
			names := []string{
				"topic_shadow_inputs", "topic_shadow_checkpoints", "topic_shadow_topics",
				"topic_shadow_decisions", "topic_shadow_events", "topic_shadow_editions",
				"idx_article_processing_items_topic_ready", "idx_topic_shadow_topics_activity",
				"idx_topic_shadow_decisions_pending", "idx_topic_shadow_events_cutoff",
			}
			var found int
			err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = current_schema() AND c.relname = ANY($1)`, names).Scan(&found)
			return found != len(names), err
		},
		Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			return map[string]any{"topic_shadow": true}, schemaSQL(ctx, tx, topicShadowSQL)
		},
	}
}
