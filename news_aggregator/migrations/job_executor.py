"""Install durable receipts for the private Python job executor."""

from typing import Any, Dict

from sqlalchemy import text

from .base_migration import BaseMigration


def ensure_job_executor_migration(results: Dict[str, Any]) -> None:
    errors = [str(error) for error in results.get("errors", [])]
    failures = [error for error in errors if "011_job_executor" in error
                or error.startswith("Migration system error:")]
    if failures:
        raise RuntimeError("Required job executor migration failed: " + "; ".join(failures))


class JobExecutorMigration(BaseMigration):
    def __init__(self):
        super().__init__("011_job_executor", "Add durable Python executor receipts", "2.4.0")

    async def check_needed(self, db):
        table = await db.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM information_schema.tables
                           WHERE table_schema = current_schema()
                             AND table_name = 'background_job_executions')
        """))
        pid_column = await db.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_schema = current_schema()
                             AND table_name = 'background_worker_status'
                             AND column_name = 'lock_backend_pid')
        """))
        receipt_columns = await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'background_job_executions'
              AND column_name IN ('legacy_completion_applied_at', 'legacy_alert_state', 'owner_id',
                                  'worker_generation', 'deadline_at')
        """))
        error_constraint = await db.scalar(text("""
            SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE connamespace=current_schema()::regnamespace
              AND conrelid='background_job_executions'::regclass
              AND conname='ck_background_execution_error_code'
        """)) if table else None
        attempt_constraint = await db.scalar(text("""
            SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE connamespace=current_schema()::regnamespace
              AND conrelid='background_job_executions'::regclass
              AND conname='ck_background_execution_attempt_generation'
        """)) if table else None
        return (not table or not pid_column
                or set(receipt_columns.scalars().all()) != {
                    "legacy_completion_applied_at", "legacy_alert_state", "owner_id",
                    "worker_generation", "deadline_at"}
                or not error_constraint or "worker_recovery_exhausted" not in error_constraint
                or not attempt_constraint or ">= 0" not in attempt_constraint)

    async def execute(self, db) -> Dict[str, Any]:
        await db.execute(text("ALTER TABLE background_worker_status ADD COLUMN IF NOT EXISTS lock_backend_pid INTEGER"))
        await db.execute(text("""
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
            )
        """))
        await db.execute(text("ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS legacy_completion_applied_at TIMESTAMP"))
        await db.execute(text("ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS legacy_alert_state VARCHAR(16)"))
        await db.execute(text("ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS owner_id UUID"))
        await db.execute(text("ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS worker_generation INTEGER"))
        await db.execute(text("ALTER TABLE background_job_executions ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMP"))
        await db.execute(text("ALTER TABLE background_job_executions DROP CONSTRAINT IF EXISTS background_job_executions_attempt_generation_check"))
        await db.execute(text("ALTER TABLE background_job_executions DROP CONSTRAINT IF EXISTS ck_background_execution_attempt_generation"))
        await db.execute(text("""
            ALTER TABLE background_job_executions
            ADD CONSTRAINT ck_background_execution_attempt_generation CHECK (attempt_generation >= 0)
        """))
        await db.execute(text("ALTER TABLE background_job_executions DROP CONSTRAINT IF EXISTS ck_background_execution_error_code"))
        await db.execute(text("""
            ALTER TABLE background_job_executions
            ADD CONSTRAINT ck_background_execution_error_code
            CHECK (error_code IS NULL OR error_code IN
                ('worker_error', 'worker_interrupted', 'worker_recovery_exhausted',
                 'external_outcome_unknown', 'invalid_job', 'resource_limit', 'database_unavailable'))
        """))
        await db.execute(text("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint
                               WHERE conname = 'ck_background_execution_alert_state'
                                 AND connamespace = current_schema()::regnamespace) THEN
                    ALTER TABLE background_job_executions
                    ADD CONSTRAINT ck_background_execution_alert_state
                    CHECK (legacy_alert_state IS NULL OR legacy_alert_state IN
                        ('pending', 'attempting', 'delivered', 'unknown', 'not_needed'));
                END IF;
            END $$
        """))
        await db.commit()
        return {"tables_created_or_confirmed": ["background_job_executions"],
                "columns_added": ["background_worker_status.lock_backend_pid"]}

    async def rollback(self, db):
        raise NotImplementedError("Executor receipts record accepted work and require an explicit retention decision")
