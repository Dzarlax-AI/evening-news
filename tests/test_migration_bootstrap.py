import asyncio


def test_migration_bootstrap_registers_same_migrations_without_importing_main(monkeypatch):
    from news_aggregator import migrate
    manager = migrate.build_migration_manager()
    assert list(manager.migrations) == [
        "006_feed_performance_optimization", "007_scheduler_run_outcomes",
        "008_article_processing_state", "009_background_jobs",
        "010_scheduled_job_metadata", "011_job_executor", "012_article_ingestion",
        "013_article_processing", "014_editorial_digest", "015_digest_delivery",
    ]


def test_run_migrations_calls_init_check_and_required_ensures(monkeypatch):
    from news_aggregator import migrate
    events = []

    class Manager:
        async def check_and_run_migrations(self):
            events.append("run")
            return {"migrations_run": [], "errors": []}

    monkeypatch.setattr(migrate, "init_db", lambda: _async_record(events, "init"))
    monkeypatch.setattr(migrate, "build_migration_manager", lambda: Manager())
    result = asyncio.run(migrate.run_migrations())
    assert result == {"migrations_run": [], "errors": []}
    assert events == ["init", "run"]


async def _async_record(events, event):
    events.append(event)
