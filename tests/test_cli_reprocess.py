"""CLI compatibility tests for the current bounded reprocessing result shape."""

import importlib
from io import StringIO
from unittest.mock import AsyncMock

import pytest
from rich.console import Console


@pytest.mark.asyncio
async def test_reprocess_cli_displays_current_pipeline_result_without_legacy_keys(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "embedded")
    cli_module = importlib.import_module("news_aggregator.cli")
    output = StringIO()
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=output, width=120, force_terminal=False),
    )
    orchestrator = type("OrchestratorStub", (), {})()
    orchestrator.start = AsyncMock()
    orchestrator.stop = AsyncMock()
    orchestrator.reprocess_failed_extractions = AsyncMock(return_value={
        "status": "ok",
        "articles_processed": 4,
        "articles_summarized": 3,
        "processed": 4,
        "improved": 3,
        "errors": [],
    })
    monkeypatch.setattr(cli_module, "NewsOrchestrator", lambda: orchestrator)

    command = cli_module.cli.commands["reprocess-failed"]
    await command.callback(limit=10, dry_run=False)

    rendered = output.getvalue()
    assert "Completed" in rendered
    assert "4" in rendered
    assert "Summaries generated" in rendered
    assert "3" in rendered
    assert "Errors" in rendered
    assert "Success rate" not in rendered
    assert "Improvement rate" not in rendered
    orchestrator.start.assert_awaited_once()
    orchestrator.stop.assert_awaited_once()
    orchestrator.reprocess_failed_extractions.assert_awaited_once_with(
        limit=10, dry_run=False
    )


@pytest.mark.asyncio
async def test_worker_reprocess_cli_queues_dry_run_without_orchestrator(monkeypatch):
    cli_module = importlib.import_module("news_aggregator.cli")
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    enqueue = AsyncMock(return_value=type("Job", (), {"id": "fixture-job", "state": "queued"})())
    jobs_module = importlib.import_module("news_aggregator.services.background_jobs")
    monkeypatch.setattr(jobs_module, "enqueue_job", enqueue)
    monkeypatch.setattr(cli_module, "NewsOrchestrator", lambda: (_ for _ in ()).throw(
        AssertionError("worker CLI must not construct an orchestrator")
    ))

    command = cli_module.cli.commands["reprocess-failed"]
    await command.callback(limit=7, dry_run=True)
    enqueue.assert_awaited_once()
    args, kwargs = enqueue.await_args
    assert args[0] == "reprocess_failed"
    assert args[1] == {"limit": 7, "dry_run": True}


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
