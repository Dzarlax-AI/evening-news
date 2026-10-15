"""Tests for browser-based extraction (nodriver)."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Pre-import so patch() can resolve the dotted path
import news_aggregator.extraction.extraction_strategies  # noqa: F401


@pytest.fixture
def mock_browser():
    browser = MagicMock()
    browser.connection = MagicMock()
    browser.connection.closed = False
    return browser


@pytest.fixture
def mock_tab():
    tab = AsyncMock()
    tab.get_content = AsyncMock(return_value="<html><body><article><p>Test article content that is long enough to pass quality checks and extraction validation.</p></article></body></html>")
    tab.evaluate = AsyncMock(side_effect=lambda expression, **kwargs:
        json.dumps({"html": tab.get_content.return_value}) if "JSON.stringify" in expression else 600)
    tab.send = AsyncMock()
    tab.get = AsyncMock()
    tab.close = AsyncMock()
    return tab


def _make_strategies(browser):
    """Create ExtractionStrategies with a mock browser."""
    # Import inside function to avoid module-level import issues
    from news_aggregator.extraction.extraction_strategies import ExtractionStrategies
    from news_aggregator.extraction.extraction_utils import ExtractionUtils
    from news_aggregator.extraction.html_processor import HTMLProcessor
    from news_aggregator.extraction.date_extractor import DateExtractor
    from news_aggregator.extraction.metadata_extractor import MetadataExtractor

    utils = ExtractionUtils()
    strategies = ExtractionStrategies(
        utils, HTMLProcessor(utils), DateExtractor(utils), MetadataExtractor(utils)
    )
    strategies.browser = browser
    return strategies


@pytest.mark.asyncio
async def test_extract_with_browser_opens_tab(mock_browser, mock_tab):
    """Browser extraction should open a new tab and close it after."""
    mock_browser.get = AsyncMock(return_value=mock_tab)

    with patch("news_aggregator.extraction.extraction_strategies.get_stability_tracker") as mock_tracker_fn, \
         patch("news_aggregator.core.browser_pool.get_browser", new_callable=AsyncMock, return_value=mock_browser):
        tracker = AsyncMock()
        tracker.get_method_timeout = MagicMock(return_value=25000)
        mock_tracker_fn.return_value = tracker

        strategies = _make_strategies(mock_browser)
        content, selector, html = await strategies._extract_with_browser_and_html(
            "https://example.com/article"
        )

        mock_browser.get.assert_awaited_once()
        mock_tab.close.assert_awaited()
        assert html is not None


@pytest.mark.asyncio
async def test_extract_with_browser_blocks_resources(mock_browser, mock_tab):
    """Browser extraction should block images/fonts/media via CDP."""
    mock_browser.get = AsyncMock(return_value=mock_tab)

    with patch("news_aggregator.extraction.extraction_strategies.get_stability_tracker") as mock_tracker_fn, \
         patch("news_aggregator.core.browser_pool.get_browser", new_callable=AsyncMock, return_value=mock_browser):
        tracker = AsyncMock()
        tracker.get_method_timeout = MagicMock(return_value=25000)
        mock_tracker_fn.return_value = tracker

        strategies = _make_strategies(mock_browser)
        await strategies._extract_with_browser_and_html("https://example.com")

        # Should have sent CDP commands: set_user_agent_override + set_blocked_ur_ls
        assert mock_tab.send.call_count >= 2


@pytest.mark.asyncio
async def test_extract_with_browser_closes_tab_on_error(mock_browser):
    """Tab should be closed even if extraction fails."""
    failing_tab = AsyncMock()
    failing_tab.send = AsyncMock(side_effect=Exception("CDP error"))
    failing_tab.close = AsyncMock()
    mock_browser.get = AsyncMock(return_value=failing_tab)

    with patch("news_aggregator.extraction.extraction_strategies.get_stability_tracker") as mock_tracker_fn, \
         patch("news_aggregator.core.browser_pool.get_browser", new_callable=AsyncMock, return_value=mock_browser):
        tracker = AsyncMock()
        tracker.get_method_timeout = MagicMock(return_value=25000)
        mock_tracker_fn.return_value = tracker

        strategies = _make_strategies(mock_browser)

        from news_aggregator.core.exceptions import ContentExtractionError
        with pytest.raises(ContentExtractionError):
            await strategies._extract_with_browser_and_html("https://example.com")

        failing_tab.close.assert_awaited()
