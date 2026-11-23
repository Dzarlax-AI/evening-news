package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var ErrDigestRetryScheduled = errors.New("scheduled digest will be retried")

func digestRetryAvailable(ctx context.Context, tx pgx.Tx) (bool, error) {
	var available bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_attribute
		WHERE attrelid='background_jobs'::regclass AND attname='digest_retry_at' AND NOT attisdropped)`).Scan(&available)
	return available, err
}

func retryableDigestFailure(state string, result map[string]any, recovered bool) bool {
	if state == "delivery_unknown" {
		// Positive database evidence of a guarded, pre-publish attempt is
		// required independently of this coarse recovery disposition.
		return recovered
	}
	if state != "failed" {
		return false
	}
	switch result["failure_reason"] {
	case "digest_processing_failed", "digest_delivery_guard_failed", "digest_domain_status_failed", "digest_summary_status_failed",
		"digest_summaries_generate_failed", "editorial_provider_timeout", "editorial_provider_unavailable":
		return true
	default:
		return false
	}
}

func scheduleDigestRetry(ctx context.Context, tx pgx.Tx, job Job, info LeaseInfo, state string, result map[string]any, body []byte, code *string, recovered bool) (bool, error) {
	if (job.Kind != "news_digest" && job.Kind != "telegram_digest") ||
		job.AttemptGeneration < 1 || job.AttemptGeneration >= 3 || !retryableDigestFailure(state, result, recovered) {
		return false, nil
	}
	if available, err := digestRetryAvailable(ctx, tx); err != nil || !available {
		return false, err
	}
	tag, err := tx.Exec(ctx, `WITH prior AS (
		SELECT COALESCE(first.legacy_completion_applied_at, first.finished_at) AS first_failure FROM background_job_executions first
		WHERE first.job_id=$1 AND first.attempt_generation=1
	), next_retry AS (
		SELECT CASE WHEN $2=1 THEN timezone('UTC', statement_timestamp())+interval '5 minutes'
			ELSE GREATEST((SELECT first_failure FROM prior)+interval '15 minutes',
				timezone('UTC', statement_timestamp())) END AS retry_at
	)
	UPDATE background_jobs j SET state='failed', result=$3::jsonb, error_code=$4,
		finished_at=timezone('UTC', statement_timestamp()), owner_generation=$5, digest_retry_at=n.retry_at
	FROM next_retry n WHERE j.id=$1 AND j.occurrence_id IS NOT NULL
		AND n.retry_at < j.scheduled_for+interval '30 minutes'
		AND EXISTS (SELECT 1 FROM background_job_executions e WHERE e.job_id=j.id
			AND e.attempt_generation=$2 AND e.digest_delivery_started IS FALSE)
		AND NOT EXISTS (SELECT 1 FROM background_job_executions e WHERE e.job_id=j.id
			AND e.attempt_generation<$2 AND e.digest_delivery_started IS DISTINCT FROM FALSE)`,
		job.ID, job.AttemptGeneration, string(body), code, info.Generation)
	if err != nil || tag.RowsAffected() != 1 {
		return false, err
	}
	// Preserve the attempt receipt and prevent its intermediate outcome from
	// being replayed as a final schedule alert after the next claim.
	_, err = tx.Exec(ctx, `UPDATE background_job_executions
		SET legacy_completion_applied_at=timezone('UTC', statement_timestamp()), legacy_alert_state='not_needed'
		WHERE job_id=$1 AND attempt_generation=$2`, job.ID, job.AttemptGeneration)
	return err == nil, err
}
