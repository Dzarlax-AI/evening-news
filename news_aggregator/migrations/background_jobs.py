"""Create the durable background job queue and singleton worker lease."""

from typing import Any, Dict

from sqlalchemy import text

from .base_migration import BaseMigration

REQUIRED_MIGRATION_ID = "009_background_jobs"


def ensure_background_jobs_migration(results: Dict[str, Any]) -> None:
    errors = [str(error) for error in results.get("errors", [])]
    failures = [error for error in errors if REQUIRED_MIGRATION_ID in error
                or error.startswith("Migration system error:")]
    if failures:
        raise RuntimeError("Required background jobs migration failed: " + "; ".join(failures))


class BackgroundJobsMigration(BaseMigration):
    """Idempotently install only the new worker-owned tables and indexes."""

    def __init__(self):
        super().__init__(REQUIRED_MIGRATION_ID,
                         "Create durable background jobs and worker ownership", "2.3.0")

    async def check_needed(self, db):
        result = await db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name IN ('background_jobs', 'background_worker_status',
                                 'background_job_idempotency_keys')
        """))
        if set(result.scalars().all()) != {
                "background_jobs", "background_worker_status", "background_job_idempotency_keys"}:
            return True
        columns = await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'background_jobs'
              AND column_name IN ('queue_position', 'attempt_generation', 'initialized_at')
        """))
        indexes = await db.execute(text("""
            SELECT indexname FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname IN ('idx_background_jobs_queue',
                                'idx_background_jobs_active_coalesce',
                                'idx_background_jobs_status',
                                'idx_background_jobs_queue_position',
                                'idx_background_job_keys_job')
        """))
        constraints = await db.execute(text("""
            SELECT conname FROM pg_constraint
            WHERE connamespace = current_schema()::regnamespace
              AND conname IN ('ck_background_jobs_kind', 'ck_background_jobs_state',
                              'ck_background_jobs_error_code',
                              'ck_background_jobs_payload_object', 'ck_background_jobs_result_object',
                              'ck_background_worker_singleton', 'ck_background_worker_status')
        """))
        status_row = await db.scalar(text("SELECT EXISTS (SELECT 1 FROM background_worker_status WHERE id = 1)"))
        return (set(columns.scalars().all()) != {
                    "queue_position", "attempt_generation", "initialized_at"}
                or set(indexes.scalars().all()) != {
                    "idx_background_jobs_queue", "idx_background_jobs_active_coalesce",
                    "idx_background_jobs_status", "idx_background_jobs_queue_position",
                    "idx_background_job_keys_job"}
                or set(constraints.scalars().all()) != {
                    "ck_background_jobs_kind", "ck_background_jobs_state",
                    "ck_background_jobs_error_code",
                    "ck_background_jobs_payload_object", "ck_background_jobs_result_object",
                    "ck_background_worker_singleton", "ck_background_worker_status"}
                or not status_row)

    async def execute(self, db):
        try:
            await db.execute(text("""
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
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    owner_generation INTEGER,
                    attempt_generation INTEGER NOT NULL DEFAULT 0,
                    result JSONB,
                    error_code VARCHAR(40),
                    CHECK (error_code IS NULL OR error_code IN (
                        'worker_error', 'worker_interrupted', 'external_outcome_unknown',
                        'invalid_job', 'resource_limit', 'database_unavailable')),
                    CHECK (jsonb_typeof(payload) = 'object'),
                    CHECK (result IS NULL OR jsonb_typeof(result) = 'object')
                )
            """))
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_background_jobs_queue
                ON background_jobs (created_at, id) WHERE state = 'queued'
            """))
            await db.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_active_coalesce
                ON background_jobs (coalesce_key)
                WHERE state IN ('queued', 'running', 'interrupted') AND coalesce_key IS NOT NULL
            """))
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_background_jobs_status
                ON background_jobs (state, created_at)
            """))
            await db.execute(text("""
                ALTER TABLE background_jobs
                ADD COLUMN IF NOT EXISTS attempt_generation INTEGER NOT NULL DEFAULT 0
            """))
            await db.execute(text("""
                ALTER TABLE background_jobs
                ADD COLUMN IF NOT EXISTS queue_position BIGSERIAL
            """))
            await db.execute(text("""
                ALTER TABLE background_jobs
                ADD COLUMN IF NOT EXISTS initialized_at TIMESTAMP
            """))
            await db.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_queue_position
                ON background_jobs (queue_position)
            """))
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS background_worker_status (
                    id SMALLINT PRIMARY KEY CHECK (id = 1),
                    generation INTEGER NOT NULL DEFAULT 0,
                    owner_id UUID,
                    heartbeat_at TIMESTAMP,
                    status VARCHAR(16) NOT NULL DEFAULT 'stopped'
                        CHECK (status IN ('stopped', 'running'))
                )
            """))
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS background_job_idempotency_keys (
                    key VARCHAR(200) PRIMARY KEY,
                    job_id UUID NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_background_job_keys_job
                ON background_job_idempotency_keys (job_id)
            """))
            # create_all may have created the ORM tables before this migration.
            # Add the same named constraints in that case, using catalog checks
            # so execution remains safe on every startup.
            for table, name, expression in (
                ("background_jobs", "ck_background_jobs_kind", "kind IN ('news_processing', 'news_digest', 'telegram_digest', 'backup', 'reprocess_failed', 'article_reprocess', 'summaries_generate', 'source_test')"),
                ("background_jobs", "ck_background_jobs_state", "state IN ('queued', 'running', 'succeeded', 'failed', 'interrupted', 'delivery_unknown')"),
                ("background_jobs", "ck_background_jobs_error_code", "error_code IS NULL OR error_code IN ('worker_error', 'worker_interrupted', 'external_outcome_unknown', 'invalid_job', 'resource_limit', 'database_unavailable')"),
                ("background_jobs", "ck_background_jobs_payload_object", "jsonb_typeof(payload) = 'object'"),
                ("background_jobs", "ck_background_jobs_result_object", "result IS NULL OR jsonb_typeof(result) = 'object'"),
                ("background_worker_status", "ck_background_worker_singleton", "id = 1"),
                ("background_worker_status", "ck_background_worker_status", "status IN ('stopped', 'running')"),
            ):
                await db.execute(text(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint
                                       WHERE conname = '{name}'
                                         AND connamespace = current_schema()::regnamespace) THEN
                            ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression});
                        END IF;
                    END $$
                """))
            await db.execute(text("""
                INSERT INTO background_worker_status (id) VALUES (1)
                ON CONFLICT (id) DO NOTHING
            """))
            await db.commit()
            return {"tables_created_or_confirmed": [
                "background_jobs", "background_worker_status"]}
        except Exception:
            await db.rollback()
            raise

    async def rollback(self, db):
        # Queue tables contain accepted work; automatic rollback would discard it.
        raise NotImplementedError("Background job tables require an explicit data-retention decision")
