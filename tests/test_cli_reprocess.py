"""CLI compatibility tests for the current bounded reprocessing result shape."""

import importlib
from unittest.mock import AsyncMock

import pytest
@pytest.mark.asyncio
async def test_reprocess_cli_queues_durable_go_job_without_orchestrator(monkeypatch):
    cli_module = importlib.import_module("news_aggregator.cli")
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "embedded")
    enqueue = AsyncMock(return_value=type("Job", (), {"id": "fixture-job", "state": "queued"})())
    jobs_module = importlib.import_module("news_aggregator.services.background_jobs")
    monkeypatch.setattr(jobs_module, "enqueue_job", enqueue)

    command = cli_module.cli.commands["reprocess-failed"]
    await command.callback(limit=10, dry_run=False)
    enqueue.assert_awaited_once()
    args, kwargs = enqueue.await_args
    assert args[0] == "reprocess_failed"
    assert args[1] == {"limit": 10, "dry_run": False}


@pytest.mark.asyncio
async def test_worker_add_source_skips_connection_test(monkeypatch):
    cli_module = importlib.import_module("news_aggregator.cli")
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    calls = {}

    class SessionContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            return None

    class SourceManagerStub:
        async def create_source(self, db, name, source_type, url, **kwargs):
            calls.update(kwargs)
            return type("Source", (), {"id": 3, "enabled": True})()

    monkeypatch.setattr(cli_module, "AsyncSessionLocal", SessionContext)
    monkeypatch.setattr(cli_module, "SourceManager", SourceManagerStub)
    command = cli_module.cli.commands["add-source"]
    await command.callback(name="Fixture", source_type="rss", url="https://fixture.invalid/feed")
    assert calls == {"skip_connection_test": True}
