"""Article failure classification and finite retry schedule regressions."""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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


@pytest.mark.parametrize(('code', 'attempts'), [
    ('busy', 1), ('busy', 2), ('busy', 3),
    ('native_browser_busy', 1), ('native_browser_busy', 2), ('native_browser_busy', 3),
])
def test_local_admission_busy_refunds_claim_and_never_exhausts(code, attempts):
    now = datetime(2026, 10, 15)
    outcome = ProcessingOutcome('retry_wait', {'summary': 'Saved summary'}, code)

    fields = outcome_fields(outcome, attempts, now, max_attempts=3,
                            initial_seconds=30, later_seconds=120)

    assert fields['processing_attempts'] == attempts - 1
    assert fields['processing_state'] == 'retry_wait'
    assert fields['processed'] is False
    assert fields['next_attempt_at'] == now + timedelta(seconds=30)
    assert fields['summary'] == 'Saved summary'


@pytest.mark.parametrize('retry_after', [None, float('nan'), float('inf'), float('-inf'), -1, 'invalid'])
def test_invalid_retry_after_does_not_break_durable_busy_schedule(retry_after):
    now = datetime(2026, 10, 15)
    fields = outcome_fields(ProcessingOutcome('retry_wait', error='busy', retry_after_seconds=retry_after),
                            1, now, initial_seconds=30, later_seconds=120)

    assert fields['processing_state'] == 'retry_wait'
    assert fields['processing_attempts'] == 0
    assert fields['next_attempt_at'] == now + timedelta(seconds=30)


def test_extreme_finite_retry_after_stays_within_datetime_range():
    fields = outcome_fields(ProcessingOutcome('retry_wait', error='busy', retry_after_seconds=1e300),
                            1, datetime(2026, 10, 15), initial_seconds=30)

    assert fields['next_attempt_at'] < datetime.max


def test_non_busy_retry_still_exhausts_at_attempt_limit():
    fields = outcome_fields(ProcessingOutcome('retry_wait', error='APIError'), 3, datetime(2026, 10, 15))

    assert fields['processing_state'] == 'exhausted'
    assert fields['processing_attempts'] == 3
    assert fields['processed'] is True
    assert fields['next_attempt_at'] is None


def test_busy_with_zero_claim_count_never_goes_negative():
    fields = outcome_fields(ProcessingOutcome('retry_wait', error='busy'), 0, datetime(2026, 10, 15),
                            initial_seconds=30)

    assert fields['processing_attempts'] == 0
    assert fields['processing_state'] == 'retry_wait'
    assert fields['processed'] is False


@pytest.mark.parametrize('state', ['completed', 'rejected'])
def test_busy_error_on_terminal_outcome_does_not_refund_attempts(state):
    fields = outcome_fields(ProcessingOutcome(state, error='busy'), 2, datetime(2026, 10, 15))

    assert fields['processing_attempts'] == 2
    assert fields['processing_state'] == state


@pytest.mark.parametrize('code', ['upstream_rate_limited', 'unavailable', 'native_browser_timeout'])
def test_other_retryable_codes_do_not_refund_claim(code):
    fields = outcome_fields(ProcessingOutcome('retry_wait', error=code), 2, datetime(2026, 10, 15),
                            max_attempts=3)

    assert fields['processing_attempts'] == 2
    assert fields['processing_state'] == 'retry_wait'
    assert fields['processed'] is False


def test_near_limit_genuine_attempt_is_preserved_with_infinite_retry_after():
    fields = outcome_fields(ProcessingOutcome('retry_wait', error='native_browser_timeout',
                                               retry_after_seconds=float('inf')),
                            2, datetime(2026, 10, 15), max_attempts=3)

    assert fields['processing_attempts'] == 2
    assert fields['processing_state'] == 'retry_wait'
    assert fields['next_attempt_at'] == datetime(2026, 10, 15) + timedelta(hours=6)


@pytest.mark.asyncio
@pytest.mark.parametrize(('code', 'level'), [('busy', 'WARNING'), ('native_browser_busy', 'WARNING'),
                                             ('unexpected', 'ERROR')])
async def test_summary_extraction_logs_local_busy_as_waiting_but_keeps_other_errors(caplog, code, level):
    from news_aggregator.extraction.service_client import ExtractionServiceError
    from news_aggregator.services.ai_client import AIClient

    extractor_context = AsyncMock()
    extractor_context.__aenter__.side_effect = ExtractionServiceError(code, True)
    caplog.set_level('WARNING', logger='news_aggregator.services.ai_client')
    with patch('news_aggregator.extraction.ContentExtractor', return_value=extractor_context):
        result = await AIClient.__new__(AIClient).get_article_summary_with_metadata('https://example.org/article')

    records = [record for record in caplog.records if record.name == 'news_aggregator.services.ai_client']
    assert any(record.levelname == level for record in records)
    if code in {'busy', 'native_browser_busy'}:
        assert not any(record.levelname == 'ERROR' for record in records)
        assert 'rescheduled' in records[-1].message
    else:
        assert any(record.levelname == 'ERROR' for record in records)
    assert result['_processing_error'] == code


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
async def test_completed_short_summary_supports_remaining_stages_with_oversized_archival_body(monkeypatch):
    from news_aggregator.config import settings
    from types import SimpleNamespace
    monkeypatch.setattr(settings, 'extraction_text_max_chars', 100)
    value = processor({})
    value.ai_client = SimpleNamespace(analyze_article_complete=AsyncMock(return_value={
        'categories': ['Tech'], 'is_advertisement': False}))
    result = await value.process_article_outcome(article(
        content='x' * 101, summary='Existing valid short summary.', summary_processed=True), {})
    assert result.state == 'completed'
    assert result.fields['category_processed'] is True
    assert result.fields['ad_processed'] is True
    assert 'summary' not in result.fields
    assert value.ai_client.analyze_article_complete.await_args.args[1] == 'Existing valid short summary.'
    value.get_summary_by_source_type.assert_not_awaited()


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


@pytest.mark.asyncio
@pytest.mark.parametrize('summary,categories', [(None, ['Tech']), ('A valid summary.', [])])
async def test_validated_ad_is_terminal_despite_incomplete_other_stages(summary, categories):
    from news_aggregator.services.ai_client import AIClient
    normalized = AIClient.__new__(AIClient)._validate_analysis_result({
        'summary': summary, 'categories': categories, 'is_advertisement': True,
    }, valid_categories=['Tech'])
    result = await processor(normalized).process_article_outcome(article(), {})
    assert result.state == 'rejected'
    assert result.error == 'advertisement'
    assert result.fields['is_advertisement'] is True
    assert result.fields['ad_processed'] is True
    assert bool(result.fields.get('summary_processed')) is bool(summary)
    assert bool(result.fields.get('category_processed')) is bool(categories)


@pytest.mark.asyncio
async def test_primary_ad_verdict_without_summary_does_not_fall_back_to_another_ai_call():
    from types import SimpleNamespace
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(
        get_article_summary_with_metadata=AsyncMock(return_value={
            'summary': None, 'categories': [], 'is_advertisement': True}),
        analyze_article_complete=AsyncMock())
    result = await value.get_summary_by_source_type(SimpleNamespace(**article()), 'rss', {})
    assert result['is_advertisement'] is True
    value.ai_client.analyze_article_complete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('is_advertisement', [False, True])
async def test_category_normalization_preserves_original_indices_labels_and_dictionary_confidence(is_advertisement):
    result = await processor({
        'summary': 'A valid article summary.', 'is_advertisement': is_advertisement,
        'categories': ['', 'Tech', {'name': ' Science ', 'confidence': 0.94, 'ai_category': 'Raw science'}],
        'category_confidences': [0.99, 0.14, 0.31], 'category_confidence': 0.8,
        'original_categories': ['Skipped label', 'Raw technology', 'Ignored dictionary fallback'],
    }).process_article_outcome(article(), {})
    assert result.state == ('rejected' if is_advertisement else 'completed')
    assert result.fields['categories'] == [
        {'name': 'Tech', 'confidence': 0.14, 'ai_category': 'Raw technology'},
        {'name': 'Science', 'confidence': 0.94, 'ai_category': 'Raw science'},
    ]


@pytest.mark.parametrize('malformed', [None, True, '0.2', float('nan'), float('inf'), -float('inf'), {}, [], 10 ** 400])
def test_malformed_per_category_confidence_uses_validated_fallbacks(malformed):
    from news_aggregator.processing.ai_processor import normalize_analysis_categories
    result = normalize_analysis_categories({
        'categories': ['Tech', {'name': 'Science', 'confidence': malformed}],
        'category_confidences': [malformed, 0.72], 'category_confidence': 0.61,
    })
    assert [category['confidence'] for category in result] == [0.61, 0.72]


def test_confidence_normalization_clamps_finite_numbers_and_validates_scalar_default():
    from news_aggregator.processing.ai_processor import normalize_analysis_categories
    result = normalize_analysis_categories({
        'categories': ['Tech', 'Science', 'Business'],
        'category_confidences': [-0.3, 1.4], 'category_confidence': float('nan'),
    })
    assert [category['confidence'] for category in result] == [0.0, 1.0, 0.8]


@pytest.mark.asyncio
@pytest.mark.parametrize('malformed', [True, float('nan'), float('inf'), -float('inf'), 10 ** 400])
async def test_real_ai_normalizer_does_not_turn_malformed_scalar_into_high_confidence(malformed):
    from news_aggregator.services.ai_client import AIClient
    analysis = AIClient.__new__(AIClient)._validate_analysis_result({
        'summary': 'A valid article summary.', 'is_advertisement': False,
        'categories': ['Tech'], 'category_confidence': malformed,
    }, valid_categories=['Tech'])
    result = await processor(analysis).process_article_outcome(article(), {})
    assert result.state == 'completed'
    assert result.fields['categories'][0]['confidence'] == 0.8


@pytest.fixture
def fresh_smart_filter(monkeypatch):
    from news_aggregator.services.smart_filter import SmartFilter
    smart_filter = SmartFilter()
    monkeypatch.setattr('news_aggregator.services.smart_filter.get_smart_filter', lambda: smart_filter)
    return smart_filter


def valid_analysis(**changes):
    return {'summary': 'A valid summary based on the full article.', 'categories': ['Tech'],
            'is_advertisement': False, **changes}


@pytest.mark.asyncio
@pytest.mark.parametrize('source_type', ['rss', 'custom'])
@pytest.mark.parametrize('existing_image', [None, 'https://example.org/existing.jpg'])
async def test_ordinary_extraction_retains_improved_body_missing_image_and_source_title(
        fresh_smart_filter, source_type, existing_image):
    from news_aggregator.services.ai_client import AIClient
    content = ('Researchers published detailed observations and explained their methods. '
               'Their results identify new opportunities for scientific collaboration. ') * 4
    client = AIClient.__new__(AIClient)
    client.analyze_article_complete = AsyncMock(return_value=valid_analysis())
    extractor = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={
        'content': content, 'image_url': 'https://example.org/new.jpg', 'publication_date': '2026-10-15'}))
    context = AsyncMock()
    context.__aenter__.return_value = extractor
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = client
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context):
        result = await value.process_article_outcome(article(source_type=source_type, image_url=existing_image), {})
    assert result.state == 'completed'
    assert result.fields['content'] == content
    assert result.fields.get('image_url') == (None if existing_image else 'https://example.org/new.jpg')
    assert result.fields['published_at'] == datetime(2026, 10, 15)
    extractor.extract_article_content_with_metadata.assert_awaited_once()
    assert client.analyze_article_complete.await_args.args[0] == 'Cloudflare launches a service'
    assert client.analyze_article_complete.await_args.args[1] == content


@pytest.mark.asyncio
async def test_ordinary_metadata_improvements_survive_later_ai_failure(fresh_smart_filter):
    content = 'An independently validated full article with substantially more information. ' * 8
    value = processor({'content': content, 'image_url': 'https://example.org/new.jpg',
                       '_processing_error': 'APIError'})
    result = await value.process_article_outcome(article(image_url=None), {})
    assert result.state == 'retry_wait'
    assert result.fields['content'] == content
    assert result.fields['image_url'] == 'https://example.org/new.jpg'
    assert 'summary' not in result.fields


@pytest.mark.asyncio
async def test_smart_filter_skip_is_terminal_without_ai_or_fake_summary_and_retains_media(fresh_smart_filter):
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(get_article_summary_with_metadata=AsyncMock(), analyze_article_complete=AsyncMock())
    content = ('Buy now and claim this promotional offer. ' * 6
               + '<img src="/campaign.jpg"><a href="/details.pdf">Details</a>')
    result = await value.process_article_outcome(article(content=content), {})
    assert result.state == 'rejected'
    assert result.error == 'smart_filter_rejected'
    assert 'summary' not in result.fields
    assert 'is_advertisement' not in result.fields
    assert {item['type'] for item in result.fields['media_files']} == {'image', 'document'}
    value.ai_client.get_article_summary_with_metadata.assert_not_awaited()
    value.ai_client.analyze_article_complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_needs_extraction_enriches_then_filters_full_body_before_ai(fresh_smart_filter):
    from news_aggregator.services.ai_client import AIClient
    client = AIClient.__new__(AIClient)
    client.analyze_article_complete = AsyncMock()
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={
        'content': 'Buy now and claim this promotional offer. ' * 8}))
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = client
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context):
        result = await value.process_article_outcome(article(content='5 min read'), {})
    assert result.state == 'rejected'
    assert result.error == 'smart_filter_rejected'
    assert 'content' not in result.fields and 'image_url' not in result.fields
    client.analyze_article_complete.assert_not_awaited()
    context.__aenter__.assert_awaited_once()


@pytest.mark.asyncio
async def test_existing_media_and_completed_summary_classification_are_preserved(fresh_smart_filter):
    fresh_smart_filter.should_process_with_ai = AsyncMock(side_effect=AssertionError('completed summary admission'))
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(analyze_article_complete=AsyncMock(return_value={
        'categories': ['Tech'], 'is_advertisement': False, 'optimized_title': 'Improved category-stage title'}))
    result = await value.process_article_outcome(article(
        summary='An existing completed summary with adequate analysis context.', summary_processed=True,
        media_files=[{'url': 'https://example.org/photo.jpg', 'type': 'image'}]), {})
    assert result.state == 'completed'
    assert result.fields['title'] == 'Improved category-stage title'
    assert 'summary' not in result.fields and 'media_files' not in result.fields
    fresh_smart_filter.should_process_with_ai.assert_not_awaited()


@pytest.mark.asyncio
async def test_short_completed_summary_does_not_fabricate_missing_classification():
    from news_aggregator.services.ai_client import AIClient
    client = AIClient.__new__(AIClient)
    client._make_structured_ai_request = AsyncMock()
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = client
    result = await value.process_article_outcome(article(summary='A short saved summary.', summary_processed=True), {})
    assert result.state == 'retry_wait'
    assert result.error == 'missing_categories'
    assert 'category_processed' not in result.fields and 'ad_processed' not in result.fields
    client._make_structured_ai_request.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('url', ['https://maps.app.goo.gl/route', 'https://www.google.com/maps/place/test',
                               'https://yandex.ru/maps/1/', 'https://yandex.com.tr/maps/',
                               'https://play.google.com/store/apps/details', 'https://apps.apple.com/app/test'])
@pytest.mark.parametrize('force', [False, True])
async def test_non_article_telegram_urls_analyze_message_without_extraction(fresh_smart_filter, url, force):
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(get_article_summary_with_metadata=AsyncMock(),
                                      analyze_article_complete=AsyncMock(return_value=valid_analysis()))
    if force:
        fresh_smart_filter.should_process_with_ai = AsyncMock(side_effect=AssertionError('forced admission'))
    with patch('news_aggregator.extraction.ContentExtractor') as extractor:
        result = await value.process_article_outcome(article(source_type='telegram', url=url), {}, force_refresh=force)
    assert result.state == 'completed'
    extractor.assert_not_called()
    value.ai_client.get_article_summary_with_metadata.assert_not_awaited()
    assert value.ai_client.analyze_article_complete.await_args.kwargs['content'] == article()['content']
    if force:
        fresh_smart_filter.should_process_with_ai.assert_not_awaited()


@pytest.mark.asyncio
async def test_scientific_title_analysis_survives_unavailable_extraction(fresh_smart_filter):
    from news_aggregator.services.ai_client import AIClient
    client = AIClient.__new__(AIClient)
    client.supports_structured_output = True
    client.summarization_model = 'test-model'
    client._build_combined_analysis_prompt_enhanced = AsyncMock(return_value='test prompt')
    client._make_structured_ai_request = AsyncMock(return_value={'result': valid_analysis()})
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        extract_article_content_with_metadata=AsyncMock(return_value={'content': None}),
        extract_article_content=AsyncMock(return_value=None))
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = client
    cache = SimpleNamespace(get_categories=AsyncMock(return_value=['Tech']))
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context), patch(
            'news_aggregator.services.category_cache.get_category_cache', return_value=cache):
        result = await value.process_article_outcome(article(
            title='Quantum research result', content='', url='https://arxiv.org/abs/test'), {})
    assert result.state == 'completed'
    assert client._build_combined_analysis_prompt_enhanced.await_args.args[1] == 'Quantum research result'
    client._make_structured_ai_request.assert_awaited_once()


@pytest.mark.asyncio
async def test_forced_telegram_external_article_keeps_original_message_context(fresh_smart_filter):
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(analyze_article_complete=AsyncMock(return_value=valid_analysis()))
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={
        'content': 'A detailed article extracted from the external source. ' * 12}))
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context):
        result = await value.process_article_outcome(article(source_type='telegram'), {}, force_refresh=True)
    assert result.state == 'completed'
    assert value.ai_client.analyze_article_complete.await_args.kwargs['original_context'] == article()['content']


@pytest.mark.asyncio
@pytest.mark.parametrize('bad_page', ['Subscribe now. ' * 30, 'Buy now and claim this offer. ' * 12])
async def test_unusable_extracted_page_falls_back_to_valid_original_without_saving_junk(fresh_smart_filter, bad_page):
    from news_aggregator.services.ai_client import AIClient
    client = AIClient.__new__(AIClient)
    client.analyze_article_complete = AsyncMock(return_value=valid_analysis())
    context = AsyncMock()
    extractor = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={
        'content': bad_page, 'image_url': 'https://example.org/junk.jpg'}))
    context.__aenter__.return_value = extractor
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = client
    original = article()
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context):
        result = await value.process_article_outcome(original, {})
    assert result.state == 'completed'
    assert 'content' not in result.fields and 'image_url' not in result.fields
    client.analyze_article_complete.assert_awaited_once()
    assert client.analyze_article_complete.await_args.kwargs['content'] == original['content']
    extractor.extract_article_content_with_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_forced_analysis_uses_shorter_fresh_body_without_replacing_longer_archival_body(fresh_smart_filter):
    original = article(content='An older archival article with extensive historical context. ' * 20)
    refreshed = 'Fresh corrected facts replace the outdated account of the event. ' * 2
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(analyze_article_complete=AsyncMock(return_value=valid_analysis()))
    context = AsyncMock()
    extractor = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={
        'content': refreshed, 'image_url': 'https://example.org/new.jpg'}))
    context.__aenter__.return_value = extractor
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context):
        result = await value.process_article_outcome(original, {}, force_refresh=True)
    assert result.state == 'completed'
    assert value.ai_client.analyze_article_complete.await_args.args[1] == refreshed
    assert 'content' not in result.fields
    assert 'image_url' not in result.fields
    assert result.fields['summary'] == valid_analysis()['summary']
    extractor.extract_article_content_with_metadata.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('source_type', ['rss', 'telegram'])
async def test_forced_refresh_recovers_from_oversized_archival_body_without_sending_or_parsing_it(
        fresh_smart_filter, monkeypatch, source_type):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'extraction_text_max_chars', 100)
    original = article(content='x' * 101, source_type=source_type)
    refreshed = 'Fresh corrected article content with sufficient factual detail.'
    value = AIProcessor.__new__(AIProcessor)
    value.ai_client = SimpleNamespace(analyze_article_complete=AsyncMock(return_value=valid_analysis()))
    context = AsyncMock()
    extractor = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={'content': refreshed}))
    context.__aenter__.return_value = extractor
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context), patch(
            'news_aggregator.processing.media_extractor.get_media_extractor') as media:
        media.return_value.extract_media_files.return_value = []
        result = await value.process_article_outcome(original, {}, force_refresh=True)
    assert result.state == 'completed'
    assert value.ai_client.analyze_article_complete.await_args.args[1] == refreshed
    assert 'original_context' not in value.ai_client.analyze_article_complete.await_args.kwargs
    media.assert_not_called()
    assert 'content' not in result.fields
    extractor.extract_article_content_with_metadata.assert_awaited_once()
