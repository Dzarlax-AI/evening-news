"""Focused lifecycle tests for the News-owned CDP client session."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import news_aggregator.core.browser_pool as bp
from news_aggregator.orchestrator import NewsOrchestrator


@pytest.fixture(autouse=True)
def reset_browser_pool_state():
    original = {
        "browser": bp._browser,
        "generation": bp._browser_generation,
        "cycle_lock": bp._cycle_lock,
        "cycle_leases": bp._active_cycle_leases,
        "tab_leases": bp._active_tab_leases,
        "close_when_idle": bp._close_when_idle,
    }
    bp._browser = None
    bp._browser_generation = 0
    bp._cycle_lock = asyncio.Lock()
    bp._active_cycle_leases = set()
    bp._active_tab_leases = set()
    bp._close_when_idle = False
    yield
    bp._browser = original["browser"]
    bp._browser_generation = original["generation"]
    bp._cycle_lock = original["cycle_lock"]
    bp._active_cycle_leases = original["cycle_leases"]
    bp._active_tab_leases = original["tab_leases"]
    bp._close_when_idle = original["close_when_idle"]


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
async def test_full_cycle_closes_browser_session_on_success():
    orchestrator = make_orchestrator()

    with patch(
        "news_aggregator.core.browser_pool.close_browser", AsyncMock()
    ) as close_browser:
        result = await orchestrator.run_full_cycle()

    assert result["fatal_error"] is None
    close_browser.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_cycle_closes_browser_session_on_warning_result():
    orchestrator = make_orchestrator()
    orchestrator._process_unprocessed_articles.return_value = {
        "articles_processed": 0,
        "articles_summarized": 0,
        "articles_categorized": 0,
        "errors": ["one article failed"],
    }

    with patch(
        "news_aggregator.core.browser_pool.close_browser", AsyncMock()
    ) as close_browser:
        result = await orchestrator.run_full_cycle()

    assert result["errors"] == ["one article failed"]
    assert result["fatal_error"] is None
    close_browser.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_cycle_closes_browser_session_on_fatal_result():
    orchestrator = make_orchestrator()
    orchestrator.source_manager.get_sources_from_db.side_effect = RuntimeError("db down")

    with patch(
        "news_aggregator.core.browser_pool.close_browser", AsyncMock()
    ) as close_browser:
        result = await orchestrator.run_full_cycle()

    assert "db down" in result["fatal_error"]
    close_browser.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_cycle_closes_browser_session_before_propagating_cancellation():
    orchestrator = make_orchestrator()
    entered = asyncio.Event()
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def hang():
        entered.set()
        await asyncio.Event().wait()

    orchestrator.source_manager.get_sources_from_db.side_effect = hang

    async def delayed_close():
        close_started.set()
        await allow_close.wait()

    with patch(
        "news_aggregator.core.browser_pool.close_browser",
        AsyncMock(side_effect=delayed_close),
    ) as close_browser:
        task = asyncio.create_task(orchestrator.run_full_cycle())
        await entered.wait()
        task.cancel()
        await close_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        allow_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    close_browser.assert_awaited_once()


@pytest.mark.asyncio
async def test_overlapping_full_cycles_keep_session_until_both_exit():
    first = make_orchestrator()
    second = make_orchestrator()
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    release_first = asyncio.Event()
    release_second = asyncio.Event()

    async def hold_first():
        first_entered.set()
        await release_first.wait()
        return []

    async def hold_second():
        second_entered.set()
        await release_second.wait()
        return []

    first.source_manager.get_sources_from_db.side_effect = hold_first
    second.source_manager.get_sources_from_db.side_effect = hold_second
    browser = MagicMock()
    browser.aclose = AsyncMock()
    browser._process = None
    bp._browser = browser
    bp._active_cycle_leases = set()
    bp._cycle_lock = asyncio.Lock()

    first_task = asyncio.create_task(first.run_full_cycle())
    second_task = asyncio.create_task(second.run_full_cycle())
    await first_entered.wait()
    await second_entered.wait()

    release_first.set()
    await first_task
    assert bp._browser is browser
    browser.aclose.assert_not_awaited()

    release_second.set()
    await second_task
    assert bp._browser is None
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_replace_fatal_cycle_result():
    orchestrator = make_orchestrator()
    orchestrator.source_manager.get_sources_from_db.side_effect = RuntimeError("primary")

    with patch(
        "news_aggregator.core.browser_pool.close_browser",
        AsyncMock(side_effect=RuntimeError("cleanup")),
    ):
        result = await orchestrator.run_full_cycle()

    assert "primary" in result["fatal_error"]
