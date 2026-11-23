ALTER TABLE __SCHEMA_Q__.background_jobs
  ADD COLUMN IF NOT EXISTS digest_retry_at timestamp;
ALTER TABLE __SCHEMA_Q__.background_job_executions
  ADD COLUMN IF NOT EXISTS digest_delivery_started boolean;

CREATE INDEX IF NOT EXISTS idx_background_jobs_digest_retry
  ON __SCHEMA_Q__.background_jobs (digest_retry_at) WHERE digest_retry_at IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='news_worker_runtime') THEN
    GRANT UPDATE (legacy_completion_applied_at, legacy_alert_state)
      ON __SCHEMA_Q__.background_job_executions TO news_worker_runtime;
  END IF;
END
$$;
