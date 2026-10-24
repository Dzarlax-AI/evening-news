"""Additive migration contract for durable execution receipts."""

import pytest
from sqlalchemy import text

from news_aggregator.migrations.job_executor import JobExecutorMigration


@pytest.mark.asyncio
async def test_executor_migration_is_idempotent_and_cascades_with_job(executor_pg):
    _, factory, _ = executor_pg
    async with factory() as session:
        migration = JobExecutorMigration()
        assert not await migration.check_needed(session)
        await migration.execute(session)
        assert not await migration.check_needed(session)
        columns = set((await session.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema=current_schema() AND table_name='background_job_executions'
        """))).scalars().all())
        assert {"job_id", "attempt_generation", "envelope_sha256", "owner_id",
                "worker_generation", "deadline_at", "legacy_completion_applied_at"} <= columns
        fk = await session.scalar(text("""
            SELECT confdeltype FROM pg_constraint
            WHERE connamespace=current_schema()::regnamespace
              AND conrelid='background_job_executions'::regclass AND contype='f'
        """))
        assert fk == b"c"
        attempt_check = await session.scalar(text("""
            SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE connamespace=current_schema()::regnamespace
              AND conrelid='background_job_executions'::regclass
              AND conname='ck_background_execution_attempt_generation'
        """))
        assert "attempt_generation >= 0" in attempt_check
