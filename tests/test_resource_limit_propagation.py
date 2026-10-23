"""Resource exhaustion must stop Go extraction before quality checks or learning."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pydantic import SecretStr

from news_aggregator.core.resource_limits import ResourceLimitError
from news_aggregator.extraction.core_extractor import CoreExtractor


@pytest.mark.asyncio
async def test_oversized_go_result_stops_before_quality_or_learning(monkeypatch):
    from news_aggregator.config import get_settings

    url = "https://example.test/article"
    settings = SimpleNamespace(
        extraction_service_url="http://extractor:8080",
        extraction_service_token=SecretStr("synthetic-token"),
        extraction_service_total_budget_seconds=2,
        extraction_service_timeout_seconds=1,
        extraction_service_native_browser=False,
        extraction_http_max_bytes=1024,
        extraction_browser_max_bytes=1024,
        extraction_text_max_chars=48,
    )
    from news_aggregator.config import settings as app_settings
    monkeypatch.setattr(app_settings, "extraction_text_max_chars", 48)
    utils = Mock()
    utils.clean_url.return_value = url
    utils.extract_domain.return_value = "example.test"
    utils.is_good_content.return_value = True
    extractor = CoreExtractor(utils, Mock(), Mock())
    memory = SimpleNamespace(
        get_successful_pattern=AsyncMock(return_value=None),
        record_extraction_attempt=AsyncMock(), degrade_pattern=AsyncMock(),
    )
    response = {
        "outcome": "success", "original_url": url, "final_url": url,
        "result": {"content": "x" * 64, "method_used": "readability", "media": []},
        "observations": [],
    }
    with patch("news_aggregator.config.get_settings", return_value=settings), \
            patch("news_aggregator.extraction.core_extractor.get_extraction_memory",
                  AsyncMock(return_value=memory)), \
            patch("news_aggregator.extraction.service_client.ExtractionServiceClient.extract",
                  AsyncMock(return_value=response)):
        with pytest.raises(ResourceLimitError):
            await extractor.extract_article_content_with_metadata(url)

    utils.is_good_content.assert_not_called()
    memory.record_extraction_attempt.assert_not_awaited()
