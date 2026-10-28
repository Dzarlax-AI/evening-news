ALTER TABLE background_worker_status ADD COLUMN IF NOT EXISTS lock_backend_pid INTEGER;

CREATE TABLE IF NOT EXISTS background_job_executions (
                job_id UUID NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
                attempt_generation INTEGER NOT NULL,
                envelope_sha256 VARCHAR(64) NOT NULL CHECK (length(envelope_sha256) = 64),
                executor_instance_id UUID NOT NULL,
                owner_id UUID,
                worker_generation INTEGER CHECK (worker_generation IS NULL OR worker_generation > 0),
                deadline_at TIMESTAMP,
                state VARCHAR(24) NOT NULL DEFAULT 'accepted'
                    CHECK (state IN ('accepted', 'running', 'succeeded', 'failed', 'interrupted', 'delivery_unknown')),
                accepted_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                result JSONB,
                error_code VARCHAR(40),
                legacy_completion_applied_at TIMESTAMP,
                legacy_alert_state VARCHAR(16)
                    CHECK (legacy_alert_state IS NULL OR legacy_alert_state IN ('pending', 'attempting', 'delivered', 'unknown', 'not_needed')),
                CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
                PRIMARY KEY (job_id, attempt_generation)
            );

ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS legacy_completion_applied_at TIMESTAMP;

ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS legacy_alert_state VARCHAR(16);

ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS owner_id UUID;

ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS worker_generation INTEGER;

ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMP;

ALTER TABLE background_job_executions DROP CONSTRAINT IF EXISTS background_job_executions_attempt_generation_check;

ALTER TABLE background_job_executions DROP CONSTRAINT IF EXISTS ck_background_execution_attempt_generation;

ALTER TABLE background_job_executions
            ADD CONSTRAINT ck_background_execution_attempt_generation CHECK (attempt_generation >= 0);

ALTER TABLE background_job_executions DROP CONSTRAINT IF EXISTS ck_background_execution_error_code;

ALTER TABLE background_job_executions
            ADD CONSTRAINT ck_background_execution_error_code
            CHECK (error_code IS NULL OR error_code IN
                ('worker_error', 'worker_interrupted', 'worker_recovery_exhausted',
                 'external_outcome_unknown', 'invalid_job', 'resource_limit', 'database_unavailable'));

DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint
                               WHERE conname = 'ck_background_execution_alert_state'
                                 AND connamespace = current_schema()::regnamespace) THEN
                    ALTER TABLE background_job_executions
                    ADD CONSTRAINT ck_background_execution_alert_state
                    CHECK (legacy_alert_state IS NULL OR legacy_alert_state IN
                        ('pending', 'attempting', 'delivered', 'unknown', 'not_needed'));
                END IF;
            END $$;
