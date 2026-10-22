import asyncio
import time
from datetime import datetime
from unittest.mock import AsyncMock

from pydantic import SecretStr
import pytest

from news_aggregator.config import settings
from news_aggregator.sources.page_monitor_service_client import PageMonitorServiceError
from news_aggregator.sources.page_monitor_source import PageMonitorConfig, PageMonitorSource
from news_aggregator.core.resource_limits import ResourceLimitError


def response(config, titles, outcome='success'):
    return {'outcome': outcome, 'error': None, 'snapshot_at': '2026-10-22T08:15:00Z',
            'content_hash': 'a' * 32, 'selectors_used': ['article'],
            'needs_selector_study': False, 'study_html': '',
            'items': [{'title': title, 'link': f'https://example.test/{title.lower().replace(" ", "-")}',
                       'description': f'Description of {title}', 'published': '2026-10-22T08:00:00Z',
                       'content_type': 'general', 'id': f'{i:032x}', 'source': config.name,
                       'source_url': config.url} for i, title in enumerate(titles, 1)]}


def install_go_mode(monkeypatch):
    monkeypatch.setattr(settings, 'page_monitor_source_mode', 'go')
    monkeypatch.setattr(settings, 'extraction_service_url', 'http://extractor:8080')
    monkeypatch.setattr(settings, 'extraction_service_token', SecretStr('private-token'))
    monkeypatch.setattr(settings, 'extraction_service_total_budget_seconds', 120.0)
    monkeypatch.setattr(settings, 'extraction_service_timeout_seconds', 30.0)


def test_delivery_limit_acknowledges_only_yielded_items_and_recovers_reappearing(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=True,
                               max_articles_per_check=20)
    monitor = PageMonitorSource(config)
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    calls = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            assert limit == 20  # caller delivery limit must not truncate the scan
            calls.append(limit)
            titles = ['Story one', 'Story two', 'Story three', 'Story four', 'Story five']
            if len(calls) == 5:
                titles = []
            elif len(calls) == 6:
                titles = ['Story one', 'Story two', 'Story three', 'Story four', 'Story five']
            return response(config, titles)

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', FakeClient)
    async def run():
        assert [a.title for a in await monitor.fetch_articles(limit=2)] == ['Story one', 'Story two']
        assert [a.title for a in await monitor.fetch_articles(limit=2)] == ['Story three', 'Story four']
        assert [a.title for a in await monitor.fetch_articles(limit=2)] == ['Story five']
        assert await monitor.fetch_articles(limit=2) == []
        assert await monitor.fetch_articles(limit=2) == []
        assert [a.title for a in await monitor.fetch_articles(limit=2)] == ['Story one', 'Story two']
    asyncio.run(run())


def test_failed_service_call_preserves_snapshot_and_does_not_open_python_browser(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=True)
    monitor = PageMonitorSource(config)
    prior = object()
    monitor.last_snapshot = prior
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))

    class FailedClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, *args, **kwargs):
            raise PageMonitorServiceError('unavailable', True)

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', FailedClient)
    async def forbidden_browser():
        raise AssertionError('Go mode must not acquire the Python browser')
    monkeypatch.setattr('news_aggregator.core.browser_pool.get_browser', forbidden_browser)
    async def run():
        await monitor.__aenter__()
        assert monitor.browser is None
        assert await monitor.fetch_articles(limit=1) == []
        assert await monitor.test_connection() is False
    asyncio.run(run())
    assert monitor.last_snapshot is prior
    assert monitor.failure_count == 1


def test_go_service_timeouts_respect_configured_wait_timeout_and_preserve_flags(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=False,
                               wait_for_js=False, wait_timeout_ms=1500)
    monitor = PageMonitorSource(config)
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    requests = []

    class CapturingClient:
        def __init__(self, *args, **kwargs):
            pass

        async def fetch(self, sent_config, *, limit, timeout_seconds):
            requests.append((sent_config.use_browser, sent_config.wait_for_js, timeout_seconds))
            return response(config, ['Story one'])

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient',
                        CapturingClient)

    async def run():
        assert [article.title for article in await monitor.fetch_articles(limit=1)] == ['Story one']
        assert await monitor.test_connection() is True

    asyncio.run(run())
    assert len(requests) == 2
    for use_browser, wait_for_js, timeout_seconds in requests:
        assert (use_browser, wait_for_js) == (False, False)
        assert 0 < timeout_seconds <= 1.5


def test_partial_prefix_delivers_without_losing_remaining_items(monkeypatch, caplog):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=False,
                               max_articles_per_check=20)
    monitor = PageMonitorSource(config)
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    class PartialClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            result = response(config, ['Story one', 'Story two', 'Story three'], 'partial')
            result['error'] = {'code': 'resource_limit', 'retryable': False}
            return result
    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', PartialClient)
    async def run():
        assert [a.title for a in await monitor.fetch_articles(limit=1)] == ['Story one']
        assert [a.title for a in await monitor.fetch_articles(limit=2)] == ['Story two', 'Story three']
    asyncio.run(run())
    assert 'partial prefix: resource_limit' in caplog.text


def test_partial_resource_limit_prefix_skips_selector_learning(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', enable_ai_analysis=True)
    monitor = PageMonitorSource(config)
    monitor._study_source_structure = AsyncMock()
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    class StudyNeededPartialClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            result = response(config, ['Story one'], 'partial')
            result.update(error={'code': 'resource_limit', 'retryable': False},
                          needs_selector_study=True, study_html='<article>fallback</article>')
            return result
    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', StudyNeededPartialClient)
    assert [a.title for a in asyncio.run(monitor.fetch_articles(limit=1))] == ['Story one']
    monitor._study_source_structure.assert_not_awaited()


def test_changed_article_hash_is_delivered_again(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', max_articles_per_check=20)
    monitor = PageMonitorSource(config)
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    counter = {'calls': 0}
    class ChangingClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            counter['calls'] += 1
            result = response(config, ['Stable story'])
            if counter['calls'] == 2:
                result['items'][0]['description'] = 'Changed summary'
            return result
    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', ChangingClient)
    async def run():
        first = await monitor.fetch_articles(limit=1)
        changed = await monitor.fetch_articles(limit=1)
        assert first[0].title == changed[0].title == 'Stable story'
        assert changed[0].content == 'Changed summary'
    asyncio.run(run())


def test_ai_study_reparses_the_same_html_and_failure_keeps_original_prefix(monkeypatch, caplog):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=True,
                               max_articles_per_check=20, enable_ai_analysis=True)
    monitor = PageMonitorSource(config)
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    monkeypatch.setattr(monitor, '_study_source_structure', lambda html: asyncio.sleep(0, result=True))
    captured = {}

    class StudyClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            result = response(config, ['Story one', 'Story two'])
            result.update(needs_selector_study=True, study_html='<article>captured once</article>')
            return result
        async def parse(self, sent_config, html, snapshot_at, *, limit, timeout_seconds):
            captured.update(html=html, snapshot_at=snapshot_at, limit=limit)
            raise PageMonitorServiceError('unsupported_selector')

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', StudyClient)
    async def run():
        delivered = await monitor.fetch_articles(limit=1)
        assert [article.title for article in delivered] == ['Story one']
    asyncio.run(run())
    assert captured == {'html': '<article>captured once</article>',
                        'snapshot_at': datetime(2026, 10, 22, 8, 15), 'limit': 20}
    assert 'keeping extracted prefix' in caplog.text
    assert len(monitor.last_snapshot.article_hashes) == 1


@pytest.mark.parametrize('hanging_phase', ['study', 'reparse'])
def test_short_deadline_hanging_optional_phase_keeps_prefix_and_ack(monkeypatch, hanging_phase):
    install_go_mode(monkeypatch)
    monkeypatch.setattr(settings, 'extraction_service_total_budget_seconds', 0.2)
    config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=True,
                               max_articles_per_check=20, enable_ai_analysis=True)
    monitor = PageMonitorSource(config)
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))

    async def hanging_study(_html):
        if hanging_phase == 'study':
            await asyncio.Future()
        return True
    monkeypatch.setattr(monitor, '_study_source_structure', hanging_study)

    class HangingOptionalClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            result = response(config, ['Story one'])
            result.update(needs_selector_study=True, study_html='<article>captured</article>')
            return result
        async def parse(self, *args, **kwargs):
            if hanging_phase == 'reparse':
                await asyncio.Future()
            raise AssertionError('study-only timeout must skip parse')

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', HangingOptionalClient)
    async def run():
        started = time.monotonic()
        articles = await monitor.fetch_articles(limit=1)
        elapsed = time.monotonic() - started
        assert [article.title for article in articles] == ['Story one']
        assert elapsed < 0.35
    asyncio.run(run())
    assert len(monitor.last_snapshot.article_hashes) == 1
    assert monitor.failure_count == 0


def test_empty_resource_limit_partial_preserves_ack_and_propagates_without_learning(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example', enable_ai_analysis=True)
    monitor = PageMonitorSource(config)
    prior = object()
    monitor.last_snapshot = prior
    monitor.failure_count = 0
    monitor._study_source_structure = AsyncMock()
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))

    class EmptyLimitClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, sent_config, *, limit, timeout_seconds):
            result = response(config, [], 'partial')
            result['error'] = {'code': 'resource_limit', 'retryable': False}
            return result

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', EmptyLimitClient)
    with pytest.raises(ResourceLimitError, match='resource_limit'):
        asyncio.run(monitor.fetch_articles(limit=1))
    assert monitor.last_snapshot is prior
    assert monitor.failure_count == 0
    monitor._study_source_structure.assert_not_awaited()


def test_external_cancellation_propagates_and_preserves_acknowledged_snapshot(monkeypatch):
    install_go_mode(monkeypatch)
    config = PageMonitorConfig('https://example.test/updates', 'Example')
    monitor = PageMonitorSource(config)
    prior = object()
    monitor.last_snapshot = prior
    monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
    started = asyncio.Event()

    class BlockingClient:
        def __init__(self, *args, **kwargs):
            pass
        async def fetch(self, *args, **kwargs):
            started.set()
            await asyncio.Future()

    monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', BlockingClient)
    async def run():
        task = asyncio.create_task(monitor.fetch_articles(limit=1))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(run())
    assert monitor.last_snapshot is prior
    assert monitor.failure_count == 0
