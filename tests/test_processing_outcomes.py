"""Article failure classification and finite retry schedule regressions."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from news_aggregator.processing.ai_processor import AIProcessor
from news_aggregator.processing.processing_outcome import (
    ProcessingOutcome, is_challenge_text, outcome_fields,
)
from news_aggregator.core.resource_limits import ResourceLimitError


def article(**changes):
    return {"id": 1, "url": "https://example.org/article", "title": "Cloudflare launches a service",
            "content": "An informative article about Cloudflare and browser security. " * 5,
            "summary": None, "published_at": None, "source_type": "rss",
            "summary_processed": False, "category_processed": False, "ad_processed": False,
            **changes}


def processor(result):
    value = AIProcessor.__new__(AIProcessor)
    value.get_summary_by_source_type = AsyncMock(return_value=result)
    return value


@pytest.mark.parametrize("text", [
    "Cloudflare expands its network and introduces new developer tools.",
    "Researchers investigated bot detection and security verification approaches.",
    "Сервис временно недоступен в России: компания рассказала о причинах сбоя.",
])
def test_legitimate_news_is_not_a_challenge(text):
    assert not is_challenge_text(text)


@pytest.mark.parametrize("text", ["Just a moment...", "Verify you are human to continue", "Checking your browser before accessing the site"])
def test_actual_challenge_text_is_detected(text):
    assert is_challenge_text(text)


def test_retry_backoff_and_exhaustion_honor_retry_after():
    now = datetime(2026, 10, 15)
    outcome = ProcessingOutcome("retry_wait", error="rate_limited", retry_after_seconds=9000)
    assert outcome_fields(outcome, 1, now)["next_attempt_at"] == now + timedelta(seconds=9000)
    assert outcome_fields(outcome, 2, now)["next_attempt_at"] == now + timedelta(hours=6)
    assert outcome_fields(outcome, 3, now)["processing_state"] == "exhausted"
    assert outcome_fields(outcome, 3, now)["next_attempt_at"] is None


@pytest.mark.asyncio
async def test_missing_summary_is_retryable_and_not_advertising():
    result = await processor({"summary": None}).process_article_outcome(article(), {})
    assert result.state == "retry_wait"
    assert result.error == "missing_summary"
    assert "is_advertisement" not in result.fields


@pytest.mark.asyncio
async def test_successful_summary_survives_later_category_failure():
    result = await processor({"summary": "A valid Cloudflare product announcement."}).process_article_outcome(article(), {})
    assert result.state == "retry_wait"
    assert result.fields["summary_processed"] is True
    assert "category_processed" not in result.fields


@pytest.mark.asyncio
async def test_completed_summary_is_preserved_when_later_analysis_fails():
    value = processor({})
    value.ai_client = type("Client", (), {"analyze_article_complete": AsyncMock(return_value={"_processing_error": "api_down"})})()
    result = await value.process_article_outcome(article(summary="Existing valid summary", summary_processed=True), {})
    assert result.state == "retry_wait"
    assert "summary" not in result.fields
    value.get_summary_by_source_type.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_limit_is_terminal_and_does_not_become_an_ad():
    value = processor({})
    value.get_summary_by_source_type.side_effect = ResourceLimitError("too large")
    result = await value.process_article_outcome(article(), {})
    assert result.state == "rejected"
    assert result.error == "resource_limit"
    assert "is_advertisement" not in result.fields


@pytest.mark.asyncio
async def test_completed_news_accepts_cloudflare_in_title_and_summary():
    result = await processor({"summary": "Cloudflare introduced a new service.",
                              "optimized_title": "Cloudflare service launch",
                              "categories": ["Tech"], "is_advertisement": False}).process_article_outcome(article(), {})
    assert result.state == "completed"
    assert result.fields["title"] == "Cloudflare service launch"


@pytest.mark.asyncio
async def test_transport_retry_after_reaches_outcome():
    value = processor({"_processing_error": "429", "_retry_after_seconds": 7200})
    result = await value.process_article_outcome(article(), {})
    assert result.retry_after_seconds == 7200


@pytest.mark.asyncio
async def test_confirmed_advertisement_is_terminal_rejection():
    result = await processor({"summary": "A product promotion", "categories": ["Tech"],
                              "is_advertisement": True}).process_article_outcome(article(), {})
    assert result.state == "rejected"
    assert result.fields["is_advertisement"] is True
    assert result.error == "advertisement"


@pytest.mark.asyncio
async def test_failed_primary_analysis_does_not_make_immediate_rss_ai_retry():
    from types import SimpleNamespace
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(
        get_article_summary_with_metadata=AsyncMock(return_value={
            '_processing_error': 'APIError', '_retry_after_seconds': 7200}),
        analyze_article_complete=AsyncMock())
    result = await value.get_summary_by_source_type(SimpleNamespace(**article()), 'rss', {})
    assert result['_retry_after_seconds'] == 7200
    value.ai_client.analyze_article_complete.assert_not_awaited()


def test_provider_retry_after_disables_immediate_transport_retries():
    from news_aggregator.core.exceptions import APIError
    from news_aggregator.services.ai_client import is_retryable_api_error
    error = APIError('rate limited', status_code=429)
    assert is_retryable_api_error(error)
    error.retry_after_seconds = 7200
    assert not is_retryable_api_error(error)


@pytest.mark.asyncio
@pytest.mark.parametrize('ad_value', [None, 'false', 'true', 0, 1, {}, []])
async def test_normalizer_keeps_missing_or_malformed_ad_classification_retryable(ad_value):
    from news_aggregator.services.ai_client import AIClient
    client = AIClient.__new__(AIClient)
    raw = {'summary': 'A valid article summary.', 'categories': ['Tech']}
    if ad_value is not None:
        raw['is_advertisement'] = ad_value
    normalized = client._validate_analysis_result(raw, valid_categories=['Tech'])
    assert normalized['is_advertisement'] is None
    result = await processor(normalized).process_article_outcome(article(), {})
    assert result.state == 'retry_wait'
    assert result.error == 'missing_ad_classification'
    assert 'is_advertisement' not in result.fields
