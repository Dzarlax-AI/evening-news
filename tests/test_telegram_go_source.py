from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from news_aggregator.config import Settings
from pydantic import SecretStr
from news_aggregator.core.exceptions import SourceError
from news_aggregator.sources.base import SourceInfo, SourceType
from news_aggregator.telegram import telegram_source as module
from news_aggregator.telegram.telegram_source import TelegramSource


CHANNEL = 'https://t.me/s/example'


def go_article(**changes):
    value = {
        'title': 'Short title', 'url': 'https://example.org/story', 'content': 'Short post',
        'summary': None, 'published_at': '2026-10-22T08:15:00Z',
        'source_type': 'telegram', 'source_name': 'Example', 'image_url': None,
        'media_files': [{'url': 'https://cdn.example/video.mp4', 'type': 'video', 'thumbnail': None}],
        'raw_data': {'channel': 'example', 'original_link': 'https://example.org/story',
                     'content_length': 10, 'hashtags': [], 'telegram_url': 'https://t.me/example/1'},
    }
    value.update(changes)
    return value


def source():
    return TelegramSource(SourceInfo(
        name='Example', source_type=SourceType.TELEGRAM, url=CHANNEL, description=''))


def settings(**changes):
    defaults = dict(
        telegram_source_mode='go', extraction_service_url='http://extractor:8080',
        extraction_service_token=SecretStr('token'), extraction_service_total_budget_seconds=120,
        extraction_service_native_browser=False, extraction_http_max_bytes=5 * 1024 * 1024,
        extraction_text_max_chars=100_000)
    defaults.update(changes)
    return SimpleNamespace(**defaults)


@pytest.mark.asyncio
async def test_go_mode_avoids_python_browser_and_parser_and_preserves_article(monkeypatch):
    monkeypatch.setattr(module, 'BROWSER_AVAILABLE', True)
    monkeypatch.setattr(module, 'get_http_client', lambda: pytest.fail('unexpected Python HTTP path'), raising=False)
    monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings())

    class Client:
        def __init__(self, endpoint, token):
            assert endpoint == 'http://extractor:8080'
            assert token == 'token'

        async def fetch(self, channel_url, name, *, limit, timeout_seconds):
            assert (channel_url, name, limit) == (CHANNEL, 'Example', 100)
            assert 0 < timeout_seconds <= 30
            return {'outcome': 'success', 'articles': [go_article()]}

    monkeypatch.setattr('news_aggregator.telegram.service_client.TelegramServiceClient', Client)
    monkeypatch.setattr(TelegramSource, '_fetch_with_browser', lambda *args: pytest.fail('Python browser called'))
    monkeypatch.setattr(TelegramSource, '_fetch_with_http', lambda *args: pytest.fail('Python HTTP called'))
    result = [item async for item in source().fetch_articles()]
    assert len(result) == 1
    item = result[0]
    assert item.url == 'https://example.org/story'
    assert item.published_at == datetime(2026, 10, 22, 8, 15, tzinfo=timezone.utc)
    assert item.image_url is None
    assert item.media_files[0]['thumbnail'] is None
    assert item.raw_data['telegram_url'] == 'https://t.me/example/1'


@pytest.mark.asyncio
async def test_partial_go_response_yields_prefix_without_raising(monkeypatch):
    monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings())

    class Client:
        def __init__(self, *args):
            pass

        async def fetch(self, *args, **kwargs):
            return {'outcome': 'partial', 'error': {'code': 'resource_limit'},
                    'articles': [go_article(raw_data={'channel': 'example', 'content_length': 10})]}

    monkeypatch.setattr('news_aggregator.telegram.service_client.TelegramServiceClient', Client)
    result = [item async for item in source().fetch_articles(limit=1)]
    assert len(result) == 1
    assert result[0].content == 'Short post'


@pytest.mark.asyncio
async def test_go_enrichment_failure_keeps_post_and_never_uses_python_extraction(monkeypatch):
    monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings())

    class Client:
        def __init__(self, *args):
            pass

        async def fetch(self, *args, **kwargs):
            return {'outcome': 'success', 'articles': [go_article()]}

    async def fail_extract(*args, **kwargs):
        from news_aggregator.extraction.service_client import ExtractionServiceError
        raise ExtractionServiceError('transport_error', True)

    monkeypatch.setattr('news_aggregator.telegram.service_client.TelegramServiceClient', Client)
    monkeypatch.setattr('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', fail_extract)
    result = [item async for item in source().fetch_articles(limit=1)]
    assert result[0].content == 'Short post'
    assert result[0].raw_data['content_length'] == 10


@pytest.mark.asyncio
async def test_go_enrichment_replaces_only_when_more_than_twice_as_long(monkeypatch):
    monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings())

    class TelegramClient:
        def __init__(self, *args):
            pass

        async def fetch(self, *args, **kwargs):
            return {'outcome': 'success', 'articles': [go_article()]}

    async def extract(*args, **kwargs):
        assert kwargs['source_type'] == 'article'
        content = 'expanded #NEWS ' + ('x' * 30)
        return {'outcome': 'success', 'result': {'content': content, 'title': 'Expanded title'}}

    monkeypatch.setattr('news_aggregator.telegram.service_client.TelegramServiceClient', TelegramClient)
    monkeypatch.setattr('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', extract)
    result = [item async for item in source().fetch_articles(limit=1)]
    assert result[0].content.startswith('expanded #NEWS')
    assert result[0].title == 'expanded #NEWS ' + ('x' * 30)
    assert result[0].raw_data['hashtags'] == ['news']
    assert result[0].raw_data['content_length'] == len(result[0].content)
    assert result[0].raw_data['original_link'] == 'https://example.org/story'


@pytest.mark.asyncio
async def test_go_enrichment_uses_one_native_article_fallback_only_when_enabled(monkeypatch):
    monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(
        extraction_service_native_browser=True))

    class TelegramClient:
        def __init__(self, *args):
            pass

        async def fetch(self, *args, **kwargs):
            return {'outcome': 'success', 'articles': [go_article()]}

    calls = []

    async def extract(self, url, **kwargs):
        calls.append((url, kwargs))
        assert kwargs['source_type'] == 'article'
        if not kwargs.get('use_browser'):
            return {'outcome': 'needs_selector_study', 'result': {},
                    'error': {'code': 'needs_selector_study', 'retryable': False}}
        content = 'expanded ' + ('x' * 30)
        return {'outcome': 'success', 'result': {'content': content, 'title': 'Expanded'}}

    monkeypatch.setattr('news_aggregator.telegram.service_client.TelegramServiceClient', TelegramClient)
    monkeypatch.setattr('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', extract)
    result = [item async for item in source().fetch_articles(limit=1)]
    assert len(calls) == 2
    assert [call[1].get('use_browser', False) for call in calls] == [False, True]
    assert result[0].title == 'expanded ' + ('x' * 30)


@pytest.mark.asyncio
async def test_enrichment_deadline_reserves_time_and_timeout_keeps_all_posts(monkeypatch):
    monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(
        extraction_service_total_budget_seconds=0.1))

    class TelegramClient:
        def __init__(self, *args):
            pass

        async def fetch(self, *args, **kwargs):
            assert kwargs['timeout_seconds'] <= 0.1
            return {'outcome': 'success', 'articles': [
                go_article(url='https://t.me/example/1'),
                go_article(url='https://t.me/example/2',
                           raw_data={'channel': 'example', 'original_link': 'https://example.org/2',
                                     'content_length': 10}),
            ]}

    attempts = []

    async def slow_enrichment(self, article, current_settings, timeout_seconds):
        attempts.append(timeout_seconds)
        await asyncio.sleep(timeout_seconds)
        raise asyncio.TimeoutError()

    import asyncio
    monkeypatch.setattr('news_aggregator.telegram.service_client.TelegramServiceClient', TelegramClient)
    monkeypatch.setattr(TelegramSource, '_enrich_go_article', slow_enrichment)
    result = [item async for item in source().fetch_articles(limit=2)]
    assert len(result) == 2
    assert len(attempts) == 1
    assert result[0].content == result[1].content == 'Short post'


def test_telegram_mode_config_defaults_to_python_and_validates_go_requirements():
    base = Settings(_env_file=None)
    assert base.telegram_source_mode == 'python'
    with pytest.raises(ValueError, match='TELEGRAM_SOURCE_MODE=go requires'):
        Settings(_env_file=None, telegram_source_mode='go')
    configured = Settings(_env_file=None, telegram_source_mode='go',
                          extraction_service_url='http://extractor:8080',
                          extraction_service_token='token')
    assert configured.telegram_source_mode == 'go'
