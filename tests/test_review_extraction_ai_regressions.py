"""Shared content-quality and provider regressions.

HTML candidate selection is covered by Go extractor quality_regression_test.go
and quality_cleanup_regression_test.go, its sole runtime implementation.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from news_aggregator.core.exceptions import APIError
from news_aggregator.extraction.extraction_utils import ExtractionUtils
from news_aggregator.processing.ai_processor import AIProcessor
from news_aggregator.services.ai_client import AIClient


MEMBERSHIP_CHOOSER = (
    "Independent analysis membership. Choose your edge. Signal costs $69 / year billed annually. "
    "Weekly reports and your own cloud computer are included. Insider costs $199 / year billed annually "
    "with additional research tools. Ultimate costs $299 / year and offers priority support for research "
    "projects. Cancel anytime. AI token usage is billed separately. Continue with free."
)


def test_membership_plan_chooser_signature_rejected_but_editorial_discussion_preserved():
    utils = ExtractionUtils()
    discussion = (
        "Researchers compared independent journalism memberships and the costs of sustaining local reporting. "
        "The analysis describes a $69 / year plan billed annually and a $199 / year plan billed annually as "
        "examples of common subscription structures. It notes that some services say cancel anytime, while "
        "reporting argues that subscribers need transparent funding, independent editorial standards, and "
        "durable public access. Readers are urged to examine each publication's actual newsroom practices "
        "before subscribing."
    )
    assert not utils.is_good_content(MEMBERSHIP_CHOOSER)
    assert utils.is_good_content(discussion)


@pytest.mark.asyncio
@pytest.mark.parametrize('status,terminal', [(400, True), (401, False), (403, False), (404, False), (413, True), (422, True)])
async def test_provider_configuration_errors_are_retryable_not_article_rejections(status, terminal):
    client = AIClient.__new__(AIClient)
    client.supports_structured_output = True
    client.summarization_model = 'test-model'
    client._build_combined_analysis_prompt_enhanced = AsyncMock(return_value='test prompt')
    client._make_structured_ai_request = AsyncMock(side_effect=APIError('test failure', status_code=status))
    result = await client.analyze_article_complete('Article', 'A useful source article with sufficient content.', 'https://example.test/article')
    assert result['_processing_terminal'] is terminal
    processor = AIProcessor.__new__(AIProcessor)
    processor.get_summary_by_source_type = AsyncMock(return_value=result)
    outcome = await processor.process_article_outcome({
        'title': 'Article', 'url': 'https://example.test/article', 'content': 'Saved article content.',
        'published_at': None, 'source_type': 'rss'}, {})
    assert outcome.state == ('rejected' if terminal else 'retry_wait')
    assert 'summary' not in outcome.fields


@pytest.mark.asyncio
@pytest.mark.parametrize('stub', ['Short teaser', 'x'])
async def test_short_extracted_stub_allows_original_source_content_analysis(stub):
    client = AIClient.__new__(AIClient)
    client.supports_structured_output = True
    client.summarization_model = 'test-model'
    client._build_combined_analysis_prompt_enhanced = AsyncMock(return_value='test prompt')
    client._make_structured_ai_request = AsyncMock(return_value={'result': {
        'summary': 'Valid summary from the full source content.', 'categories': ['Tech'], 'is_advertisement': False}})
    extractor_context = AsyncMock()
    extractor_context.__aenter__.return_value = SimpleNamespace(
        extract_article_content_with_metadata=AsyncMock(return_value={'content': stub}))
    cache = SimpleNamespace(get_categories=AsyncMock(return_value=['Tech']))
    processor = AIProcessor.__new__(AIProcessor)
    processor.ai_client = client
    article = SimpleNamespace(id=1, title='Original RSS article title', url='https://example.test/article',
                              content='Original RSS article with enough information to produce a useful summary.', published_at=None)
    with patch('news_aggregator.extraction.ContentExtractor', return_value=extractor_context), patch(
            'news_aggregator.services.category_cache.get_category_cache', return_value=cache):
        result = await processor.get_summary_by_source_type(article, 'rss', {})
    assert result['summary'] == 'Valid summary from the full source content.'
    client._make_structured_ai_request.assert_awaited_once()
    assert client._build_combined_analysis_prompt_enhanced.await_args.args[1] == article.content
