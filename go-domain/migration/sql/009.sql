CREATE TABLE IF NOT EXISTS background_jobs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    queue_position BIGSERIAL NOT NULL UNIQUE,
                    kind VARCHAR(40) NOT NULL CHECK (kind IN (
                        'news_processing', 'news_digest', 'telegram_digest', 'backup',
                        'reprocess_failed', 'article_reprocess', 'summaries_generate', 'source_test')),
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    idempotency_key VARCHAR(200) NOT NULL UNIQUE,
                    coalesce_key VARCHAR(64),
                    state VARCHAR(24) NOT NULL DEFAULT 'queued' CHECK (state IN (
                        'queued', 'running', 'succeeded', 'failed', 'interrupted', 'delivery_unknown')),
                    created_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    owner_generation INTEGER,
                    attempt_generation INTEGER NOT NULL DEFAULT 0,
                    result JSONB,
                    error_code VARCHAR(40),
                    CHECK (error_code IS NULL OR error_code IN (
                        'worker_error', 'worker_interrupted', 'worker_recovery_exhausted',
                        'external_outcome_unknown',
                        'invalid_job', 'resource_limit', 'database_unavailable')),
                    CHECK (jsonb_typeof(payload) = 'object'),
                    CHECK (result IS NULL OR jsonb_typeof(result) = 'object')
                );

CREATE INDEX IF NOT EXISTS idx_background_jobs_queue
                ON background_jobs (created_at, id) WHERE state = 'queued';

CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_active_coalesce
                ON background_jobs (coalesce_key)
                WHERE state IN ('queued', 'running', 'interrupted') AND coalesce_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_background_jobs_status
                ON background_jobs (state, created_at);

ALTER TABLE background_jobs
                ADD COLUMN IF NOT EXISTS attempt_generation INTEGER NOT NULL DEFAULT 0;

ALTER TABLE background_jobs
                ADD COLUMN IF NOT EXISTS queue_position BIGSERIAL;

ALTER TABLE background_jobs
                ADD COLUMN IF NOT EXISTS initialized_at TIMESTAMP;

CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_queue_position
                ON background_jobs (queue_position);

CREATE TABLE IF NOT EXISTS background_worker_status (
                    id SMALLINT PRIMARY KEY CHECK (id = 1),
                    generation INTEGER NOT NULL DEFAULT 0,
                    owner_id UUID,
                    heartbeat_at TIMESTAMP,
                    status VARCHAR(16) NOT NULL DEFAULT 'stopped'
                        CHECK (status IN ('stopped', 'running'))
                );

CREATE TABLE IF NOT EXISTS background_job_idempotency_keys (
                    key VARCHAR(200) PRIMARY KEY,
                    job_id UUID NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
                    created_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                );

CREATE INDEX IF NOT EXISTS idx_background_job_keys_job
                ON background_job_idempotency_keys (job_id);

ALTER TABLE background_jobs
                ALTER COLUMN created_at SET DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

ALTER TABLE background_job_idempotency_keys
                ALTER COLUMN created_at SET DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

ALTER TABLE background_jobs DROP CONSTRAINT IF EXISTS ck_background_jobs_error_code;

ALTER TABLE background_jobs DROP CONSTRAINT IF EXISTS background_jobs_error_code_check;

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_jobs_kind' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_jobs ADD CONSTRAINT ck_background_jobs_kind CHECK (kind IN ('news_processing', 'news_digest', 'telegram_digest', 'backup', 'reprocess_failed', 'article_reprocess', 'summaries_generate', 'source_test')); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_jobs_state' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_jobs ADD CONSTRAINT ck_background_jobs_state CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'interrupted', 'delivery_unknown')); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_jobs_error_code' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_jobs ADD CONSTRAINT ck_background_jobs_error_code CHECK (error_code IS NULL OR error_code IN ('worker_error', 'worker_interrupted', 'worker_recovery_exhausted', 'external_outcome_unknown', 'invalid_job', 'resource_limit', 'database_unavailable')); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_jobs_payload_object' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_jobs ADD CONSTRAINT ck_background_jobs_payload_object CHECK (jsonb_typeof(payload) = 'object'); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_jobs_result_object' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_jobs ADD CONSTRAINT ck_background_jobs_result_object CHECK (result IS NULL OR jsonb_typeof(result) = 'object'); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_worker_singleton' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_worker_status ADD CONSTRAINT ck_background_worker_singleton CHECK (id = 1); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_background_worker_status' AND connamespace = current_schema()::regnamespace) THEN ALTER TABLE background_worker_status ADD CONSTRAINT ck_background_worker_status CHECK (status IN ('stopped', 'running')); END IF; END $$;

INSERT INTO background_worker_status (id) VALUES (1)
                ON CONFLICT (id) DO NOTHING;
