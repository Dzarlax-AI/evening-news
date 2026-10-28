ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS schedule_key VARCHAR(128);

ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS schedule_revision INTEGER;

ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS occurrence_id UUID;

ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMP;

CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_occurrence
            ON background_jobs (occurrence_id) WHERE occurrence_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS scheduler_occurrence_receipts (
                occurrence_id UUID PRIMARY KEY,
                envelope_sha256 VARCHAR(64) NOT NULL,
                job_id UUID NOT NULL,
                accepted_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                CONSTRAINT ck_scheduler_receipt_hash CHECK (length(envelope_sha256) = 64)
            );

DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint
                               WHERE conname = 'ck_scheduler_receipt_hash'
                                 AND connamespace = current_schema()::regnamespace) THEN
                    ALTER TABLE scheduler_occurrence_receipts
                    ADD CONSTRAINT ck_scheduler_receipt_hash CHECK (length(envelope_sha256) = 64);
                END IF;
            END $$;
