"""CLI compatibility tests for the current bounded reprocessing result shape."""

import importlib
from io import StringIO
from unittest.mock import AsyncMock

import pytest
from rich.console import Console


@pytest.mark.asyncio
async def test_reprocess_cli_displays_current_pipeline_result_without_legacy_keys(monkeypatch):
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
