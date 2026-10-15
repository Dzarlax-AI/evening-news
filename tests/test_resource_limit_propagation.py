"""Resource exhaustion must stop extraction instead of looking like bad content."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from news_aggregator.core.resource_limits import ResourceLimitError
from news_aggregator.extraction.extraction_strategies import ExtractionStrategies
from news_aggregator.services.ai_extraction_optimizer import AIExtractionOptimizer
from news_aggregator.sources.page_monitor_source import PageMonitorConfig, PageMonitorSource
from news_aggregator.sources.base import SourceInfo, SourceType
from news_aggregator.telegram.telegram_source import TelegramSource


@pytest.mark.asyncio
async def test_oversized_extracted_paragraph_is_rejected_before_smart_filter(monkeypatch):
    """A real paragraph over the configured cap must not reach quality checks."""
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "extraction_text_max_chars", 48)
    smart_filter = Mock()
    smart_filter.should_process_with_ai = AsyncMock(return_value=(True, "ok"))

    with patch(
        "news_aggregator.services.smart_filter.get_smart_filter",
        return_value=smart_filter,
    ):
        with pytest.raises(ResourceLimitError):
            await ExtractionStrategies._is_high_quality_content(
                object(), "A valid paragraph. " * 8, title="A useful article"
            )

    smart_filter.should_process_with_ai.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_limit_does_not_record_success_or_continue_to_fallback(monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "extraction_text_max_chars", 48)
    paragraph = "A valid paragraph. " * 8
    memory = Mock()
    memory.get_successful_pattern = AsyncMock(return_value=None)

    strategies = object.__new__(ExtractionStrategies)
    strategies.fetch_html_content = AsyncMock(return_value="<html><body>fixture</body></html>")
    strategies.fetch_html_content_fallback = AsyncMock()
    strategies._extract_reddit_json = AsyncMock()
    strategies._extract_with_browser_and_html = AsyncMock()
    strategies.record_extraction_success = AsyncMock()
    strategies.record_extraction_failure = AsyncMock()
    strategies.utils = Mock()
    strategies.utils.is_good_content.return_value = True
    strategies.html_processor = Mock()
    strategies.html_processor.extract_by_enhanced_selectors.return_value = (
        paragraph,
        "article p",
    )
    strategies.html_processor.clean_text.side_effect = lambda value: value

    with patch(
        "news_aggregator.extraction.extraction_strategies.get_extraction_memory",
        new=AsyncMock(return_value=memory),
    ):
        with pytest.raises(ResourceLimitError):
            await strategies.attempt_extraction_with_metadata(
                "https://example.test/story", "example.test", 1
            )

    strategies.record_extraction_success.assert_not_awaited()
    strategies.fetch_html_content_fallback.assert_not_awaited()
    strategies._extract_with_browser_and_html.assert_not_awaited()
    strategies.record_extraction_failure.assert_not_awaited()


@pytest.mark.asyncio
async def test_fallback_limit_is_checked_before_success_recording(monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "extraction_text_max_chars", 48)
    paragraph = "A valid paragraph. " * 8
    memory = Mock()
    memory.get_successful_pattern = AsyncMock(return_value=None)

    strategies = object.__new__(ExtractionStrategies)
    strategies.fetch_html_content = AsyncMock(return_value="<html></html>")
    strategies.fetch_html_content_fallback = AsyncMock()
    strategies._extract_reddit_json = AsyncMock(return_value=None)
    strategies._extract_with_browser_and_html = AsyncMock(return_value=(None, None, None))
    strategies.record_extraction_success = AsyncMock()
    strategies.record_extraction_failure = AsyncMock()
    strategies.utils = Mock()
    strategies.utils.is_good_content.return_value = True
    strategies.html_processor = Mock()
    strategies.html_processor.extract_by_enhanced_selectors.return_value = (None, None)
    strategies.html_processor.extract_by_enhanced_heuristics.return_value = None
    strategies.html_processor.clean_text.side_effect = lambda value: value
    strategies.metadata_extractor = Mock()
    strategies.metadata_extractor.extract_from_json_ld.return_value = (paragraph, "json-ld")

    with (
        patch(
            "news_aggregator.extraction.extraction_strategies.get_extraction_memory",
            new=AsyncMock(return_value=memory),
        ),
        patch("news_aggregator.extraction.extraction_strategies.Document") as document,
    ):
        document.return_value.summary.return_value = ""
        with pytest.raises(ResourceLimitError):
            await strategies.attempt_extraction_with_metadata(
                "https://example.test/story", "example.test", 1
            )

    strategies.record_extraction_success.assert_not_awaited()
    strategies.record_extraction_failure.assert_not_awaited()


@pytest.mark.asyncio
async def test_reddit_json_limit_is_checked_before_return_or_fallback(monkeypatch):
    import json

    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "extraction_text_max_chars", 48)
    post_data = [
        {"data": {"children": [{"data": {
            "title": "A useful title",
            "selftext": "A valid paragraph. " * 8,
            "is_self": True,
            "post_hint": "self",
        }}]}},
        {"data": {"children": []}},
    ]
    response = Mock(status=200)
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=None)
    client = Mock()
    client.get = AsyncMock(return_value=response)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    strategies = object.__new__(ExtractionStrategies)
    strategies.utils = Mock()
    strategies.utils.get_headers.return_value = {}
    strategies.fetch_html_content = AsyncMock()
    strategies.fetch_html_content_fallback = AsyncMock()
    strategies.record_extraction_success = AsyncMock()
    strategies.record_extraction_failure = AsyncMock()

    with (
        patch(
            "news_aggregator.extraction.extraction_strategies.get_http_client",
            return_value=client,
        ),
        patch(
            "news_aggregator.extraction.extraction_strategies.read_page_bytes",
            new=AsyncMock(return_value=json.dumps(post_data).encode()),
        ),
    ):
        with pytest.raises(ResourceLimitError):
            await strategies.attempt_extraction_with_metadata(
                "https://www.reddit.com/r/news/comments/abc/story",
                "www.reddit.com",
                1,
            )

    strategies.record_extraction_success.assert_not_awaited()
    strategies.fetch_html_content.assert_not_awaited()
    strategies.fetch_html_content_fallback.assert_not_awaited()


@pytest.mark.asyncio
async def test_optimizer_sample_fetch_propagates_resource_limit_without_ai_or_learning():
    optimizer = AIExtractionOptimizer()
    stability = Mock()
    stability.should_use_ai_optimization.return_value = (True, "test")
    memory = Mock()
    memory.get_best_patterns_for_domain = AsyncMock(return_value=[])
    memory.record_ai_pattern_discovery = AsyncMock()

    with (
        patch(
            "news_aggregator.services.ai_extraction_optimizer.get_stability_tracker",
            new=AsyncMock(return_value=stability),
        ),
        patch(
            "news_aggregator.services.ai_extraction_optimizer.get_extraction_memory",
            new=AsyncMock(return_value=memory),
        ) as get_memory,
        patch("news_aggregator.services.ai_client.get_ai_client") as get_ai,
        patch.object(
            optimizer,
            "_fetch_sample_html",
            new=AsyncMock(side_effect=ResourceLimitError("page too large")),
        ),
    ):
        with pytest.raises(ResourceLimitError):
            await optimizer.analyze_domain_extraction(
                "example.test", ["https://example.test/article"]
            )

    get_memory.assert_not_awaited()
    get_ai.assert_not_called()
    memory.record_ai_pattern_discovery.assert_not_awaited()


@pytest.mark.asyncio
async def test_optimizer_sample_fetch_does_not_swallow_http_body_limit():
    optimizer = AIExtractionOptimizer()
    response = Mock(status=200)
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=None)
    client = Mock()
    client.get = AsyncMock(return_value=response)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    with (
        patch(
            "news_aggregator.services.ai_extraction_optimizer.get_http_client",
            return_value=client,
        ),
        patch(
            "news_aggregator.core.resource_limits.read_page_text",
            new=AsyncMock(side_effect=ResourceLimitError("page too large")),
        ),
    ):
        with pytest.raises(ResourceLimitError):
            await optimizer._fetch_sample_html("https://example.test/article")


@pytest.mark.asyncio
async def test_page_monitor_limit_does_not_count_as_failure_or_trigger_learning():
    source = PageMonitorSource(
        PageMonitorConfig(
            url="https://example.test/updates",
            name="fixture",
            use_browser=False,
            enable_ai_analysis=True,
            reanalyze_after_failures=1,
        )
    )
    source._load_learned_structure = AsyncMock()
    source._take_http_snapshot = AsyncMock(
        side_effect=ResourceLimitError("page too large")
    )
    source._trigger_ai_analysis = AsyncMock()

    with pytest.raises(ResourceLimitError):
        await source.fetch_articles()

    assert source.failure_count == 0
    source._trigger_ai_analysis.assert_not_awaited()
    source._take_http_snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_telegram_browser_limit_does_not_fall_back_to_http(monkeypatch):
    source = TelegramSource(
        SourceInfo(
            name="fixture",
            source_type=SourceType.TELEGRAM,
            url="https://t.me/s/fixture",
            description="test fixture",
        )
    )
    monkeypatch.setattr("news_aggregator.telegram.telegram_source.BROWSER_AVAILABLE", True)

    async def browser_limit():
        raise ResourceLimitError("browser page too large")
        yield  # pragma: no cover - keep this an async generator

    source._fetch_with_browser = browser_limit
    source._fetch_with_http = AsyncMock()

    with pytest.raises(ResourceLimitError):
        async for _ in source.fetch_articles():
            pass

    source._fetch_with_http.assert_not_awaited()


@pytest.mark.asyncio
async def test_telegram_http_limit_does_not_try_an_alternative_url(monkeypatch):
    import aiohttp

    source = TelegramSource(
        SourceInfo(
            name="fixture",
            source_type=SourceType.TELEGRAM,
            url="https://t.me/s/fixture",
            description="test fixture",
        )
    )
    requested_urls = []

    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def get(self, url, **kwargs):
            requested_urls.append(url)
            return FakeResponse()

    monkeypatch.setattr(aiohttp, "ClientSession", FakeSession)
    with patch(
        "news_aggregator.telegram.telegram_source.read_page_text",
        new=AsyncMock(side_effect=ResourceLimitError("HTTP page too large")),
    ):
        with pytest.raises(ResourceLimitError):
            async for _ in source._fetch_with_http():
                pass

    assert requested_urls == [source.access_urls[0]]
