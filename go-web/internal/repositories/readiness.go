package repositories

import (
	"context"
	"errors"
)

// CheckSchema verifies the existing bootstrap contract without applying DDL.
// A fresh or partially migrated database must never become a healthy web.
func (p *Postgres) CheckSchema(ctx context.Context) error {
	tables := []string{
		"articles", "sources", "categories", "article_categories", "category_mapping",
		"daily_summaries", "settings", "ai_usage_tracking", "processing_stats", "extraction_attempts",
		"background_jobs", "background_job_idempotency_keys", "background_worker_status",
		"scheduler_occurrence_receipts", "background_job_executions", "article_ingestion_runs",
		"article_ingestion_source_runs", "article_processing_runs", "article_processing_items",
	}
	var ready bool
	err := p.pool.QueryRow(ctx, `SELECT NOT EXISTS (
 SELECT 1 FROM unnest($1::text[]) AS needed(name)
 WHERE to_regclass(format('%I.%I',current_schema(),needed.name)) IS NULL
)`, tables).Scan(&ready)
	if err != nil {
		return errors.New("database schema could not be checked")
	}
	if !ready {
		return errors.New("database schema is not ready; run the migration bootstrap")
	}
	columns := []string{
		"articles.processing_state", "articles.processing_attempts", "articles.next_attempt_at",
		"articles.summary_processed", "articles.category_processed", "articles.ad_processed",
		"background_jobs.schedule_key", "background_jobs.schedule_revision", "background_jobs.occurrence_id",
		"background_jobs.scheduled_for", "background_jobs.attempt_generation",
	}
	err = p.pool.QueryRow(ctx, `SELECT count(*)=$1 FROM information_schema.columns
 WHERE table_schema=current_schema() AND table_name || '.' || column_name = ANY($2::text[])`, len(columns), columns).Scan(&ready)
	if err != nil {
		return errors.New("database schema could not be checked")
	}
	if !ready {
		return errors.New("database schema is not ready; run the migration bootstrap")
	}
	return nil
}
