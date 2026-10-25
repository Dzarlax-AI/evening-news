"""Go-mode selection and failure behavior for the full processing cycle."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from news_aggregator import orchestrator as orchestrator_module
from news_aggregator.orchestrator import NewsOrchestrator
from news_aggregator.services import article_ingestion_client as client_module


def orchestrator():
    instance = NewsOrchestrator.__new__(NewsOrchestrator)
    instance.db_queue_manager = MagicMock()
    instance.db_queue_manager.execute_write = AsyncMock(return_value=None)
    instance._process_unprocessed_articles = AsyncMock(return_value={
        "articles_processed": 0, "articles_summarized": 0, "articles_categorized": 0,
    })
    return instance


@pytest.mark.asyncio
async def test_full_cycle_uses_go_ingestion_then_existing_ai_processing(monkeypatch):
    monkeypatch.setattr(orchestrator_module, "settings", SimpleNamespace(article_ingestion_mode="go"))
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 2, "articles_fetched": 3, "errors": [], "fatal_error": None,
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    instance = orchestrator()

    result = await instance._run_full_cycle()

    client.collect_and_save.assert_awaited_once()
    instance._process_unprocessed_articles.assert_awaited_once()
    assert result["sources_synced"] == 2
    assert result["articles_fetched"] == 3
    assert result["fatal_error"] is None


@pytest.mark.asyncio
async def test_full_cycle_does_not_start_ai_after_unconfirmed_ingestion(monkeypatch):
    monkeypatch.setattr(orchestrator_module, "settings", SimpleNamespace(article_ingestion_mode="go"))
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 0, "articles_fetched": 0, "errors": [], "fatal_error": "cancel_unconfirmed",
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    instance = orchestrator()

    result = await instance._run_full_cycle()

    instance._process_unprocessed_articles.assert_not_awaited()
    assert result["fatal_error"]
