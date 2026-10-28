package migration

import (
	"context"
	_ "embed"
	"github.com/jackc/pgx/v5"
	"strings"
)

//go:embed sql/010.sql
var schedulerSQL string

//go:embed sql/011.sql
var executionSQL string

func schedulerMigration() migration {
	return migration{ID: "010_scheduled_job_metadata", Description: "Add scheduler job metadata and durable occurrence receipts", Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
		columns, err := hasColumns(ctx, tx, "background_jobs", []string{"schedule_key", "schedule_revision", "occurrence_id", "scheduled_for"})
		if err != nil {
			return false, err
		}
		var receiptCount int
		err = tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='scheduler_occurrence_receipts'`).Scan(&receiptCount)
		if err != nil {
			return false, err
		}
		receipt, err := hasColumns(ctx, tx, "scheduler_occurrence_receipts", []string{"occurrence_id", "envelope_sha256", "job_id", "accepted_at"})
		if err != nil {
			return false, err
		}
		var index bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname='idx_background_jobs_occurrence')`).Scan(&index)
		return !columns || !receipt || receiptCount != 4 || !index, err
	}, Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		err := execute(ctx, tx, schedulerSQL)
		return map[string]any{"columns_added": 4, "tables_created_or_confirmed": []string{"scheduler_occurrence_receipts"}}, err
	}}
}
func executionMigration() migration {
	return migration{ID: "011_job_executor", Description: "Add durable Python executor receipts", Needed: func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var table, pid bool
		err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='background_job_executions'), EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='background_worker_status' AND column_name='lock_backend_pid')`).Scan(&table, &pid)
		if err != nil {
			return false, err
		}
		if !table {
			return true, nil
		}
		cols, err := hasColumns(ctx, tx, "background_job_executions", []string{"legacy_completion_applied_at", "legacy_alert_state", "owner_id", "worker_generation", "deadline_at"})
		if err != nil {
			return false, err
		}
		var errorDef, attemptDef *string
		err = tx.QueryRow(ctx, `SELECT (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid='background_job_executions'::regclass AND conname='ck_background_execution_error_code'), (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid='background_job_executions'::regclass AND conname='ck_background_execution_attempt_generation')`).Scan(&errorDef, &attemptDef)
		if err != nil {
			return false, err
		}
		return !pid || !cols || errorDef == nil || !strings.Contains(*errorDef, "worker_recovery_exhausted") || attemptDef == nil || !strings.Contains(*attemptDef, ">= 0"), nil
	}, Apply: func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		err := execute(ctx, tx, executionSQL)
		return map[string]any{"tables_created_or_confirmed": []string{"background_job_executions"}, "columns_added": []string{"background_worker_status.lock_backend_pid"}}, err
	}}
}
