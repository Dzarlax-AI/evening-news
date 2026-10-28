package migration

import (
	"context"
	"strings"

	_ "embed"
	"github.com/jackc/pgx/v5"
)

// These are the 009 statements from the legacy migration, including the
// catalog-conditional named constraints required for ORM-created tables.
//
//go:embed sql/009.sql
var backgroundSQL string

func backgroundMigration() migration {
	return migration{ID: "009_background_jobs", Description: "Create durable background jobs and worker ownership", Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var tables int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=ANY($1)`, []string{"background_jobs", "background_worker_status", "background_job_idempotency_keys"}).Scan(&tables); err != nil {
			return false, err
		}
		if tables != 3 {
			return true, nil
		}
		var columns, indexes, constraints, utc int
		var status, legacy bool
		var def *string
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='background_jobs' AND column_name=ANY($1)`, []string{"queue_position", "attempt_generation", "initialized_at"}).Scan(&columns); err != nil {
			return false, err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname=ANY($1)`, []string{"idx_background_jobs_queue", "idx_background_jobs_active_coalesce", "idx_background_jobs_status", "idx_background_jobs_queue_position", "idx_background_job_keys_job"}).Scan(&indexes); err != nil {
			return false, err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conname=ANY($1)`, []string{"ck_background_jobs_kind", "ck_background_jobs_state", "ck_background_jobs_error_code", "ck_background_jobs_payload_object", "ck_background_jobs_result_object", "ck_background_worker_singleton", "ck_background_worker_status"}).Scan(&constraints); err != nil {
			return false, err
		}
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM background_worker_status WHERE id=1), (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conname='ck_background_jobs_error_code'), EXISTS (SELECT 1 FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conname='background_jobs_error_code_check')`).Scan(&status, &def, &legacy); err != nil {
			return false, err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='background_jobs' AND column_name='created_at') OR (table_name='background_job_idempotency_keys' AND column_name='created_at')) AND column_default LIKE '%UTC%'`).Scan(&utc); err != nil {
			return false, err
		}
		return columns != 3 || indexes != 5 || constraints != 7 || !status || def == nil || !strings.Contains(*def, "worker_recovery_exhausted") || legacy || utc != 2, nil
	}, Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		err := execute(ctx, tx, backgroundSQL)
		return map[string]any{"tables_created_or_confirmed": []string{"background_jobs", "background_worker_status"}}, err
	}}
}
