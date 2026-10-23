"""Focused lifecycle tests for the News-owned CDP client session."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_aggregator.orchestrator import NewsOrchestrator, processing_cycle_busy


def make_orchestrator() -> NewsOrchestrator:
    orchestrator = NewsOrchestrator.__new__(NewsOrchestrator)
    orchestrator.source_manager = MagicMock()
    orchestrator.source_manager.get_sources_from_db = AsyncMock(return_value=[])
    orchestrator.source_manager.fetch_from_all_sources_no_db = AsyncMock(return_value={})
    orchestrator.db_queue_manager = MagicMock()
    orchestrator.db_queue_manager.execute_write = AsyncMock(side_effect=[{}, None])
    orchestrator._process_unprocessed_articles = AsyncMock(
        return_value={
            "articles_processed": 0,
            "articles_summarized": 0,
            "articles_categorized": 0,
        }
    )
    return orchestrator


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_after_success():
    orchestrator = make_orchestrator()

    result = await orchestrator.run_full_cycle()

    assert result["fatal_error"] is None
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_after_warning_result():
    orchestrator = make_orchestrator()
    orchestrator._process_unprocessed_articles.return_value = {
        "articles_processed": 0,
        "articles_summarized": 0,
        "articles_categorized": 0,
        "errors": ["one article failed"],
    }

    result = await orchestrator.run_full_cycle()

    assert result["errors"] == ["one article failed"]
    assert result["fatal_error"] is None
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_after_fatal_result():
    orchestrator = make_orchestrator()
    orchestrator.source_manager.get_sources_from_db.side_effect = RuntimeError("db down")

    result = await orchestrator.run_full_cycle()

    assert "db down" in result["fatal_error"]
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_full_cycle_releases_reservation_before_propagating_cancellation():
    orchestrator = make_orchestrator()
    entered = asyncio.Event()
    async def hang():
        entered.set()
        await asyncio.Event().wait()

    orchestrator.source_manager.get_sources_from_db.side_effect = hang
    task = asyncio.create_task(orchestrator.run_full_cycle())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not processing_cycle_busy()


@pytest.mark.asyncio
async def test_overlapping_full_cycle_returns_busy_without_starting_work():
    first = make_orchestrator()
    second = make_orchestrator()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold_first():
        entered.set()
        await release.wait()
        return []

    first.source_manager.get_sources_from_db.side_effect = hold_first
    first_task = asyncio.create_task(first.run_full_cycle())
    await entered.wait()
    try:
        result = await second.run_full_cycle()
        assert result['status'] == 'busy'
        second.source_manager.get_sources_from_db.assert_not_awaited()
    finally:
        release.set()
        await first_task


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_replace_fatal_cycle_result():
    orchestrator = make_orchestrator()
    orchestrator.source_manager.get_sources_from_db.side_effect = RuntimeError("primary")

    result = await orchestrator.run_full_cycle()

    assert "primary" in result["fatal_error"]
    assert not processing_cycle_busy()
