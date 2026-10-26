"""Focused lifecycle tests for the News-owned CDP client session."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_aggregator.orchestrator import NewsOrchestrator, processing_cycle_busy


def make_orchestrator() -> NewsOrchestrator:
    orchestrator = NewsOrchestrator.__new__(NewsOrchestrator)
    orchestrator.db_queue_manager = MagicMock()
    orchestrator.db_queue_manager.execute_write = AsyncMock(side_effect=[{}, None])
    return orchestrator


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_after_success(monkeypatch):
    from news_aggregator.services.article_ingestion_client import ArticleIngestionClient
    from news_aggregator.services import article_processing_client
    monkeypatch.setattr(ArticleIngestionClient, 'collect_and_save', AsyncMock(return_value={
        'sources_synced': 0, 'articles_fetched': 0, 'errors': [], 'errors_count': 0,
    }))
    processor = type('Processor', (), {'process_current_job': AsyncMock(return_value={
        'status': 'success', 'articles_processed': 0, 'articles_summarized': 0,
        'articles_categorized': 0, 'errors': [],
    })})()
    monkeypatch.setattr(article_processing_client, 'ArticleProcessingClient', lambda: processor)
    orchestrator = make_orchestrator()

    result = await orchestrator.run_full_cycle()

    assert result["fatal_error"] is None
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_after_warning_result(monkeypatch):
    from news_aggregator.services.article_ingestion_client import ArticleIngestionClient
    from news_aggregator.services import article_processing_client
    monkeypatch.setattr(ArticleIngestionClient, 'collect_and_save', AsyncMock(return_value={
        'sources_synced': 0, 'articles_fetched': 0, 'errors': [], 'errors_count': 0,
    }))
    processor = type('Processor', (), {'process_current_job': AsyncMock(return_value={
        'status': 'warning', 'articles_processed': 0, 'errors': ['one article failed'],
    })})()
    monkeypatch.setattr(article_processing_client, 'ArticleProcessingClient', lambda: processor)
    orchestrator = make_orchestrator()

    result = await orchestrator.run_full_cycle()

    assert result["errors"] == ["one article failed"]
    assert result["fatal_error"] is None
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_after_fatal_result(monkeypatch):
    from news_aggregator.services.article_ingestion_client import ArticleIngestionClient
    monkeypatch.setattr(ArticleIngestionClient, 'collect_and_save', AsyncMock(
        side_effect=RuntimeError("ingestion unavailable")))
    orchestrator = make_orchestrator()

    result = await orchestrator.run_full_cycle()

    assert "ingestion unavailable" in result["fatal_error"]
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_before_propagating_cancellation():
    from news_aggregator.services.article_ingestion_client import ArticleIngestionClient
    orchestrator = make_orchestrator()
    entered = asyncio.Event()
    async def hang(*_args, **_kwargs):
        entered.set()
        await asyncio.Event().wait()

    with patch.object(ArticleIngestionClient, 'collect_and_save', side_effect=hang):
        task = asyncio.create_task(orchestrator.run_full_cycle())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_overlapping_full_cycle_returns_busy_without_starting_work(monkeypatch):
    from news_aggregator.services.article_ingestion_client import ArticleIngestionClient
    first = make_orchestrator()
    second = make_orchestrator()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold_first(*_args, **_kwargs):
        entered.set()
        await release.wait()

    monkeypatch.setattr(ArticleIngestionClient, 'collect_and_save', hold_first)
    first_task = asyncio.create_task(first.run_full_cycle())
    await entered.wait()
    try:
        result = await second.run_full_cycle()
        assert result['status'] == 'busy'
    finally:
        release.set()
        await first_task


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_replace_fatal_cycle_result(monkeypatch):
    from news_aggregator.services.article_ingestion_client import ArticleIngestionClient
    monkeypatch.setattr(ArticleIngestionClient, 'collect_and_save', AsyncMock(
        side_effect=RuntimeError("primary")))
    orchestrator = make_orchestrator()

    result = await orchestrator.run_full_cycle()

    assert "primary" in result["fatal_error"]
    assert not processing_cycle_busy()
