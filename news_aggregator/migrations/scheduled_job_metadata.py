"""Add scheduler-origin metadata and durable delivery receipts."""

from sqlalchemy import text

from .base_migration import BaseMigration

REQUIRED_MIGRATION_ID = "010_scheduled_job_metadata"


def ensure_scheduled_job_metadata_migration(results):
    errors = [str(error) for error in results.get("errors", [])]
    failures = [error for error in errors if REQUIRED_MIGRATION_ID in error
                or error.startswith("Migration system error:")]
    if failures:
        raise RuntimeError("Required scheduler metadata migration failed: " + "; ".join(failures))


class ScheduledJobMetadataMigration(BaseMigration):
    def __init__(self):
        super().__init__(REQUIRED_MIGRATION_ID,
                         "Add scheduler job metadata and durable occurrence receipts", "1.0.0")

    async def check_needed(self, db):
        columns = (await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'background_jobs'
              AND column_name IN ('schedule_key', 'schedule_revision', 'occurrence_id', 'scheduled_for')
        """))).scalars().all()
        receipt_columns = (await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'scheduler_occurrence_receipts'
        """))).scalars().all()
        index_exists = await db.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM pg_indexes
                           WHERE schemaname = current_schema()
                             AND indexname = 'idx_background_jobs_occurrence')
        """))
        return (set(columns) != {"schedule_key", "schedule_revision", "occurrence_id", "scheduled_for"}
                or set(receipt_columns) != {"occurrence_id", "envelope_sha256", "job_id", "accepted_at"}
                or not index_exists)

    async def execute(self, db):
        await db.execute(text("ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS schedule_key VARCHAR(128)"))
        await db.execute(text("ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS schedule_revision INTEGER"))
        await db.execute(text("ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS occurrence_id UUID"))
        await db.execute(text("ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMP"))
        await db.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_occurrence
            ON background_jobs (occurrence_id) WHERE occurrence_id IS NOT NULL
        """))
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS scheduler_occurrence_receipts (
                occurrence_id UUID PRIMARY KEY,
                envelope_sha256 VARCHAR(64) NOT NULL,
                job_id UUID NOT NULL,
                accepted_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                CONSTRAINT ck_scheduler_receipt_hash CHECK (length(envelope_sha256) = 64)
            )
        """))
        await db.execute(text("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint
                               WHERE conname = 'ck_scheduler_receipt_hash'
                                 AND connamespace = current_schema()::regnamespace) THEN
                    ALTER TABLE scheduler_occurrence_receipts
                    ADD CONSTRAINT ck_scheduler_receipt_hash CHECK (length(envelope_sha256) = 64);
                END IF;
            END $$
        """))
        await db.commit()
        return {"columns_added": 4, "tables_created_or_confirmed": ["scheduler_occurrence_receipts"]}

    async def rollback(self, db):
        raise NotImplementedError("Scheduler occurrence receipts protect replay safety and require retention review")
