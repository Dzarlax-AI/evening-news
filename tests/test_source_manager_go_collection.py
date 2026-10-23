import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from news_aggregator.config import settings
from news_aggregator.services.source_manager import SourceManager
from news_aggregator.sources.base import Article, SourceInfo, SourceType
from news_aggregator.sources.page_monitor_adapter import PageMonitorAdapter
from news_aggregator.sources.rss_source import RSSSource


def source(source_id, name, source_type, **kwargs):
    return SimpleNamespace(id=source_id, name=name, source_type=source_type,
                           url=f'https://example.test/{source_id}', enabled=True,
                           config=kwargs.get('config') or {})


def rss_item(title, url, *, content=None, summary=None):
    return {'title': title, 'url': url, 'content': content, 'summary': summary,
            'image_url': None, 'published_at': '2026-10-23T10:00:00Z',
            'source_type': 'rss', 'source_name': 'Same name',
            'raw_data': {'guid': 'guid-' + title, 'author': None, 'tags': []}}


def go_settings(monkeypatch):
    monkeypatch.setattr(settings, 'source_manager_mode', 'go')
    monkeypatch.setattr(settings, 'telegram_source_mode', 'go')
    monkeypatch.setattr(settings, 'page_monitor_source_mode', 'go')
    monkeypatch.setattr(settings, 'extraction_service_url', 'http://extractor:8080')
    monkeypatch.setattr(settings, 'extraction_service_token', SecretStr('private-token'))
    monkeypatch.setattr(settings, 'extraction_service_total_budget_seconds', 120.0)


def test_rss_dto_preserves_null_fields_raw_data_and_utc_naive_date():
    article = RSSSource.article_from_go(rss_item('Story', 'https://example.test/story'))
    assert article.content is None and article.summary is None and article.image_url is None
    assert article.published_at == datetime(2026, 10, 23, 10, 0)
    assert article.published_at.tzinfo is None
    assert article.raw_data == {'guid': 'guid-Story', 'author': None, 'tags': []}


def test_go_empty_rss_success_remains_distinct_from_missing_or_failed_source(monkeypatch):
    go_settings(monkeypatch)
    item = source(30, 'Empty feed', 'rss')

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            return {'results': [{'source_id': 30, 'type': 'rss', 'outcome': 'success',
                                 'rss_articles': []}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    result = asyncio.run(SourceManager().fetch_from_all_sources_no_db([item]))
    assert result == {30: []}


def test_go_descriptor_uses_caller_per_source_timeout_budget(monkeypatch):
    go_settings(monkeypatch)
    received = []

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            received.extend(descriptors)
            return {'results': [{'source_id': 301, 'type': 'rss', 'outcome': 'success',
                                 'rss_articles': []}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    result = asyncio.run(SourceManager().fetch_from_all_sources_no_db(
        [source(301, 'Feed', 'rss')], per_source_timeout=0.25))
    assert received[0]['per_source_budget_ms'] == 250
    assert result == {301: []}


def test_go_telegram_limit_above_python_cap_is_clamped_before_adapter_validation(monkeypatch):
    go_settings(monkeypatch)
    item = source(302, 'Channel', 'telegram', config={'limit': 101})
    received = []

    class TelegramAdapter:
        @staticmethod
        def go_collection_config(limit=None):
            from news_aggregator.telegram.telegram_source import TelegramSource
            return TelegramSource.go_collection_config(limit)

        async def consume_go_collection(self, payload, **kwargs):
            yield Article('Channel story', 'https://example.test/tg')

    async def get_instance(_source):
        return TelegramAdapter()

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            received.extend(descriptors)
            return {'results': [{'source_id': 302, 'type': 'telegram', 'outcome': 'success',
                                 'telegram': {'outcome': 'success', 'articles': []}}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    result = asyncio.run(manager.fetch_from_all_sources_no_db([item]))
    assert received[0]['telegram'] == {'limit': 100}
    assert received[0]['limit'] == 100
    assert [article.title for article in result[302]] == ['Channel story']


@pytest.mark.parametrize('limit', [True, 0])
def test_go_telegram_invalid_limits_are_rejected_during_preparation(monkeypatch, limit):
    go_settings(monkeypatch)
    item = source(303, 'Invalid channel', 'telegram', config={'limit': limit})

    class TelegramAdapter:
        @staticmethod
        def go_collection_config(value=None):
            from news_aggregator.telegram.telegram_source import TelegramSource
            return TelegramSource.go_collection_config(value)

    async def get_instance(_source):
        return TelegramAdapter()

    class ForbiddenClient:
        def __init__(self, *args):
            pass

        async def collect(self, *args, **kwargs):
            raise AssertionError('invalid Telegram config must not reach Go collection')

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', ForbiddenClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    result = asyncio.run(manager.fetch_from_all_sources_no_db([item]))
    assert result == {303: []}


def test_go_collection_retains_duplicate_names_by_source_id_without_python_fetch(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(31, 'Same name', SourceType.RSS), source(32, 'Same name', 'rss')]
    received = []

    class FakeClient:
        def __init__(self, endpoint, token):
            assert endpoint == 'http://extractor:8080'
            assert token == 'private-token'

        async def collect(self, descriptors, *, timeout_seconds):
            received.extend(descriptors)
            return {'results': [
                {'source_id': item['source_id'], 'type': 'rss', 'outcome': 'success',
                 'rss_articles': [rss_item(f"Story {item['source_id']}",
                                            f"https://example.test/article-{item['source_id']}")]}
                for item in descriptors]}

    async def forbidden_instance(_source):
        raise AssertionError('Go RSS must not instantiate or fetch the Python source')

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, 'get_source_instance', forbidden_instance)
    result = asyncio.run(manager.fetch_from_all_sources_no_db(sources))
    assert set(result) == {31, 32}
    assert [result[source_id][0].title for source_id in (31, 32)] == ['Story 31', 'Story 32']
    assert [item['source_id'] for item in received] == [31, 32]
    assert all(item['type'] == 'rss' and item['source_name'] == 'Same name' for item in received)


def test_deferred_sources_repeat_only_their_ids_and_keep_partial_prefix(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(41, 'first', 'rss'), source(42, 'second', 'rss')]
    calls = []

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            ids = [item['source_id'] for item in descriptors]
            calls.append(ids)
            if len(calls) == 1:
                return {'results': [
                    {'source_id': 41, 'type': 'rss', 'outcome': 'partial',
                     'rss_articles': [rss_item('Prefix', 'https://example.test/prefix')]},
                    {'source_id': 42, 'type': 'rss', 'outcome': 'deferred'},
                ]}
            return {'results': [{'source_id': 42, 'type': 'rss', 'outcome': 'success',
                                 'rss_articles': [rss_item('Second', 'https://example.test/second')]}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    manager = SourceManager()
    result = asyncio.run(manager.fetch_from_all_sources_no_db(sources))
    assert calls == [[41, 42], [42]]
    assert [article.title for article in result[41]] == ['Prefix']
    assert [article.title for article in result[42]] == ['Second']


def test_permanently_deferred_source_has_a_bounded_retry_count(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(43, 'Deferred', 'rss')]
    attempts = []

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            attempts.append([item['source_id'] for item in descriptors])
            if len(attempts) <= 8:
                return {'results': [{'source_id': 43, 'type': 'rss', 'outcome': 'deferred',
                                     'error': {'code': 'response_budget_exhausted', 'retryable': True,
                                               'retry_after_seconds': 1}}]}
            return {'results': [{'source_id': 43, 'type': 'rss', 'outcome': 'success',
                                 'rss_articles': [rss_item('Recovered',
                                                           'https://example.test/recovered')]}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr('news_aggregator.services.source_manager.asyncio.sleep', no_wait)
    result = asyncio.run(SourceManager().fetch_from_all_sources_no_db(sources, per_source_timeout=120))
    assert [item.title for item in result[43]] == ['Recovered']
    assert attempts == [[43]] * 9


def test_large_rss_limit_is_not_silently_clamped(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(47, 'Large feed', 'rss', config={'limit': 1001})]
    received = []

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            received.extend(descriptors)
            return {'results': [{'source_id': 47, 'type': 'rss', 'outcome': 'rejected',
                                 'error': {'code': 'invalid_input', 'retryable': False}}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    result = asyncio.run(SourceManager().fetch_from_all_sources_no_db(sources))
    assert received[0]['limit'] == 1001
    assert result == {47: []}


def test_preparation_failure_keeps_empty_source_identity_and_does_not_block_rss(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(44, 'Broken monitor', 'custom'), source(45, 'Feed', 'rss')]
    calls = []

    class BrokenPageMonitor:
        async def prepare_go_collection(self):
            raise RuntimeError('details are deliberately not logged')

    async def get_instance(item):
        if item.id == 44:
            return BrokenPageMonitor()
        raise AssertionError('RSS must not instantiate the Python source')

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            calls.append([item['source_id'] for item in descriptors])
            return {'results': [{'source_id': 45, 'type': 'rss', 'outcome': 'success',
                                 'rss_articles': [rss_item('Story', 'https://example.test/story')]}]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    result = asyncio.run(manager.fetch_from_all_sources_no_db(sources))
    assert calls == [[45]]
    assert set(result) == {44, 45}
    assert result[44] == []
    assert [item.title for item in result[45]] == ['Story']


def test_real_page_monitor_adapter_forwards_go_finalize_deadline(monkeypatch):
    go_settings(monkeypatch)
    page_source = source(48, 'Page monitor', 'custom')
    adapter = PageMonitorAdapter(SourceInfo(
        name=page_source.name, source_type=SourceType.CUSTOM, url=page_source.url,
        description='test adapter', config={'use_browser': False},
    ))
    observed = {}

    async def prepare():
        return {'limit': 20, 'use_browser': False, 'wait_for_js': False}

    async def finalize(result, *, limit=None, deadline=None):
        observed.update(result=result, limit=limit, deadline=deadline)
        return [Article('Captured PM item', 'https://example.test/pm')]

    monkeypatch.setattr(adapter.monitor, 'prepare_go_collection', prepare)
    monkeypatch.setattr(adapter.monitor, 'finalize_go_collection', finalize)

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            captured = {'outcome': 'success', 'items': [], 'marker': 'captured'}
            return {'results': [{'source_id': 48, 'type': 'custom', 'outcome': 'success',
                                 'page_monitor': captured}]}

    async def get_instance(_source):
        return adapter

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    result = asyncio.run(manager.fetch_from_all_sources_no_db([page_source]))
    assert result[48][0].title == 'Captured PM item'
    assert observed['result']['marker'] == 'captured'
    assert observed['limit'] is None
    assert isinstance(observed['deadline'], float)


def test_go_collection_rejects_duplicate_or_invalid_source_ids_before_mapping(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(46, 'First', 'rss'), source(46, 'Second', 'rss')]

    class ForbiddenClient:
        def __init__(self, *args):
            raise AssertionError('invalid source IDs must be rejected before client setup')

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', ForbiddenClient)
    try:
        asyncio.run(SourceManager().fetch_from_all_sources_no_db(sources))
    except Exception as exc:
        assert getattr(exc, 'code', None) == 'invalid_request'
    else:
        raise AssertionError('duplicate source IDs were silently accepted')


def test_mixed_batch_normalizes_typed_results_and_keeps_other_sources_after_failure(monkeypatch):
    go_settings(monkeypatch)
    sources = [source(51, 'Feed', 'rss'), source(52, 'Channel', 'telegram'),
               source(53, 'Monitor', 'custom'), source(54, 'Bad', 'rss')]
    fetched = []

    class TelegramAdapter:
        def go_collection_config(self, limit=None):
            return {'limit': limit} if limit else {}

        async def consume_go_collection(self, payload, **kwargs):
            fetched.append(('telegram', payload['marker']))
            yield Article('Channel story', 'https://example.test/tg')

    class PageMonitorAdapter:
        async def prepare_go_collection(self):
            return {'limit': 20, 'use_browser': True, 'wait_for_js': True}

        async def finalize_go_collection(self, payload, **kwargs):
            fetched.append(('custom', payload['marker']))
            return [Article('Monitor story', 'https://example.test/pm')]

    instances = {52: TelegramAdapter(), 53: PageMonitorAdapter()}

    async def get_instance(item):
        return instances[item.id]

    class FakeClient:
        def __init__(self, *args):
            pass

        async def collect(self, descriptors, *, timeout_seconds):
            assert [item['source_id'] for item in descriptors] == [51, 52, 53, 54]
            return {'results': [
                {'source_id': 51, 'type': 'rss', 'outcome': 'success', 'rss_articles': [
                    rss_item('Feed story', 'https://example.test/feed')]},
                {'source_id': 52, 'type': 'telegram', 'outcome': 'success',
                 'telegram': {'outcome': 'success', 'articles': [], 'marker': 'captured'}},
                {'source_id': 53, 'type': 'custom', 'outcome': 'success',
                 'page_monitor': {'outcome': 'success', 'marker': 'captured'}},
                {'source_id': 54, 'type': 'rss', 'outcome': 'failed',
                 'error': {'code': 'unavailable', 'retryable': True}},
            ]}

    monkeypatch.setattr('news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient', FakeClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    result = asyncio.run(manager.fetch_from_all_sources_no_db(sources))
    assert set(result) == {51, 52, 53, 54}
    assert [result[51][0].title, result[52][0].title, result[53][0].title] == [
        'Feed story', 'Channel story', 'Monitor story']
    assert fetched == [('telegram', 'captured'), ('custom', 'captured')]
    assert result[54] == []
