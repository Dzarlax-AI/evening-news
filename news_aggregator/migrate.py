"""One-shot database migration bootstrap; does not import the web application."""

import asyncio

from .database import AsyncSessionLocal, init_db
from .migrations.universal_migration_manager import create_migration_manager
from .migrations.feed_performance_optimization import FeedPerformanceOptimization
from .migrations.scheduler_run_outcomes import SchedulerRunOutcomesMigration, ensure_scheduler_outcome_migration
from .migrations.article_processing_state import ArticleProcessingStateMigration, ensure_article_processing_migration
from .migrations.background_jobs import BackgroundJobsMigration, ensure_background_jobs_migration
from .migrations.scheduled_job_metadata import ScheduledJobMetadataMigration, ensure_scheduled_job_metadata_migration
from .migrations.job_executor import JobExecutorMigration, ensure_job_executor_migration
from .migrations.article_ingestion import ArticleIngestionMigration, ensure_article_ingestion_migration
from .migrations.article_processing import ArticleProcessingMigration, ensure_article_processing_service_migration
from .migrations.editorial_digest import EditorialDigestMigration, ensure_editorial_digest_migration


def build_migration_manager():
    manager = create_migration_manager(AsyncSessionLocal, "Evening News v2")
    for migration in (
        FeedPerformanceOptimization(), SchedulerRunOutcomesMigration(),
        ArticleProcessingStateMigration(), BackgroundJobsMigration(),
        ScheduledJobMetadataMigration(), JobExecutorMigration(),
        ArticleIngestionMigration(), ArticleProcessingMigration(), EditorialDigestMigration(),
    ):
        manager.register_migration(migration)
    return manager


async def run_migrations() -> dict:
    await init_db()
    results = await build_migration_manager().check_and_run_migrations()
    for ensure in (
        ensure_scheduler_outcome_migration, ensure_article_processing_migration,
        ensure_background_jobs_migration, ensure_scheduled_job_metadata_migration,
        ensure_job_executor_migration, ensure_article_ingestion_migration,
        ensure_article_processing_service_migration,
        ensure_editorial_digest_migration,
    ):
        ensure(results)
    return results


def main() -> None:
    async def run_and_close():
        try:
            return await run_migrations()
        finally:
            from .database import close_db_engine
            await close_db_engine()

    results = asyncio.run(run_and_close())
    print(f"migrations_run={len(results.get('migrations_run', []))} errors={len(results.get('errors', []))}")
    if results.get("errors"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
