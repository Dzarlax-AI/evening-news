"""Add durable scheduler run outcome fields."""

from typing import Any, Dict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .base_migration import BaseMigration


class SchedulerRunOutcomesMigration(BaseMigration):
    """Idempotently add fields required to distinguish success from failure."""

    _columns = {
        "last_finished_at": "TIMESTAMP",
        "last_success_at": "TIMESTAMP",
        "last_status": "VARCHAR(20)",
        "last_error": "TEXT",
        "last_duration_seconds": "NUMERIC(12, 3)",
    }

    def __init__(self):
        super().__init__(
            migration_id="007_scheduler_run_outcomes",
            description="Persist scheduler success, failure, timeout, and error details",
            version="2.1.0",
        )

    async def check_needed(self, db: AsyncSession) -> bool:
        result = await db.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'schedule_settings'
                  AND column_name IN (
                      'last_finished_at',
                      'last_success_at',
                      'last_status',
                      'last_error',
                      'last_duration_seconds'
                  )
                """
            )
        )
        existing = set(result.scalars().all())
        return existing != set(self._columns)

    async def execute(self, db: AsyncSession) -> Dict[str, Any]:
        for name, sql_type in self._columns.items():
            await db.execute(
                text(
                    f"ALTER TABLE schedule_settings "
                    f"ADD COLUMN IF NOT EXISTS {name} {sql_type}"
                )
            )
        await db.commit()
        return {"columns_added_or_confirmed": list(self._columns)}
