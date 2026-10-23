import asyncio
import json
from pathlib import Path

import pytest

from news_aggregator.sources.source_manager_service_client import (
    MAX_SOURCES,
    SourceManagerServiceClient,
    SourceManagerServiceError,
)
from news_aggregator.config import Settings
from pydantic import SecretStr


def payload(request_id='request-1', results=None):
    return {'contract_version': '1', 'build_version': 'test-build', 'request_id': request_id,
            'results': results if results is not None else [
                {'source_id': 10, 'type': 'rss', 'outcome': 'success', 'rss_articles': []}]}


def test_validate_correlates_each_result_to_exact_request_source_ids():
    sources = [{'source_id': 10, 'type': 'rss'}, {'source_id': 11, 'type': 'rss'}]
    results = [
        {'source_id': 11, 'type': 'rss', 'outcome': 'partial', 'rss_articles': []},
        {'source_id': 10, 'type': 'rss', 'outcome': 'success', 'rss_articles': []},
    ]
    assert SourceManagerServiceClient._validate(payload(results=results), 'request-1', sources)['results'] == results


def test_actual_go_rss_wire_failure_mixed_with_empty_title_success():
    response = json.loads((Path(__file__).parent / 'fixtures/go-source-manager/rss-failed-mixed-response.json')
                          .read_text())
    sources = [
        {'source_id': 10, 'type': 'rss', 'source_name': 'Failed feed'},
        {'source_id': 11, 'type': 'rss', 'source_name': 'Empty-title feed'},
    ]
    validated = SourceManagerServiceClient._validate(response, 'batch-actual-wire', sources)
    assert validated['results'][0]['rss_articles'] is None
    assert validated['results'][0]['error']['code'] == 'upstream_timeout'
    assert validated['results'][1]['rss_articles'][0]['title'] == ''


def test_successful_rss_rejects_null_articles_payload():
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(
            payload(results=[{'source_id': 10, 'type': 'rss', 'outcome': 'success',
                              'rss_articles': None}]), 'request-1',
            [{'source_id': 10, 'type': 'rss', 'source_name': 'Feed'}])


@pytest.mark.parametrize('unsafe_url', [
    'javascript:alert(1)',
    '//example.test/story',
    'https://user:secret@example.test/story',
    'https://example.test/story" onmouseover="alert(1)',
    "https://example.test/story' onclick='alert(1)",
    'https://example.test/<svg>',
    'https://example.test/story`x`',
    'https://example.test/story\nnext',
    'https://example.test/' + ('x' * 8200),
])
def test_mixed_go_rss_wire_rejects_unsafe_article_urls(unsafe_url):
    response = json.loads((Path(__file__).parent / 'fixtures/go-source-manager/rss-failed-mixed-response.json')
                          .read_text())
    response['results'][1]['rss_articles'][0]['url'] = unsafe_url
    sources = [
        {'source_id': 10, 'type': 'rss', 'source_name': 'Failed feed'},
        {'source_id': 11, 'type': 'rss', 'source_name': 'Empty-title feed'},
    ]
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(response, 'batch-actual-wire', sources)


@pytest.mark.parametrize('unsafe_url', [
    'javascript:alert(1)',
    'https://user:secret@example.test/image.png',
    'https://example.test/" onerror="alert(1)',
    'https://example.test/<svg>',
    'https://example.test/image.png`x`',
    'https://example.test/image.png\x00',
])
def test_mixed_go_rss_wire_rejects_unsafe_nonnull_image_urls(unsafe_url):
    response = json.loads((Path(__file__).parent / 'fixtures/go-source-manager/rss-failed-mixed-response.json')
                          .read_text())
    response['results'][1]['rss_articles'][0]['image_url'] = unsafe_url
    sources = [
        {'source_id': 10, 'type': 'rss', 'source_name': 'Failed feed'},
        {'source_id': 11, 'type': 'rss', 'source_name': 'Empty-title feed'},
    ]
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(response, 'batch-actual-wire', sources)


@pytest.mark.parametrize('results', [
    [],
    [{'source_id': 10, 'type': 'rss', 'outcome': 'success', 'rss_articles': []},
     {'source_id': 10, 'type': 'rss', 'outcome': 'success', 'rss_articles': []}],
    [{'source_id': 99, 'type': 'rss', 'outcome': 'success', 'rss_articles': []}],
    [{'source_id': 10, 'type': 'telegram', 'outcome': 'success', 'telegram': {'articles': []}}],
    [{'source_id': 10, 'type': 'rss', 'outcome': 'success'}],
])
def test_validate_rejects_missing_duplicate_foreign_or_mismatched_results(results):
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(payload(results=results), 'request-1',
                                             [{'source_id': 10, 'type': 'rss'}])


def test_validate_rejects_wrong_contract_version_or_request_id():
    data = payload()
    data['request_id'] = 'different'
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(data, 'request-1', [{'source_id': 10, 'type': 'rss'}])


def test_validate_checks_nested_telegram_and_page_monitor_request_ids():
    telegram = {
        'contract_version': '1', 'build_version': 'build', 'request_id': 'wrong',
        'original_url': 'https://t.me/s/example', 'final_url': 'https://t.me/s/example',
        'outcome': 'success', 'articles': [],
    }
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(
            payload(results=[{'source_id': 10, 'type': 'telegram', 'outcome': 'success',
                              'telegram': telegram}]), 'request-1',
            [{'source_id': 10, 'type': 'telegram', 'url': telegram['original_url'],
              'source_name': 'Example'}])

    page_monitor = {
        'contract_version': '1', 'build_version': 'build', 'request_id': 'wrong',
        'original_url': 'https://example.test/list', 'final_url': 'https://example.test/list',
        'outcome': 'success', 'selectors_used': [], 'items': [], 'needs_selector_study': False,
    }
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(
            payload(results=[{'source_id': 10, 'type': 'custom', 'outcome': 'success',
                              'page_monitor': page_monitor}]), 'request-1',
            [{'source_id': 10, 'type': 'custom', 'url': page_monitor['original_url'],
              'source_name': 'Example', 'page_monitor': {}}])
    data = payload()
    data['contract_version'] = '2'
    with pytest.raises(SourceManagerServiceError, match='invalid_payload'):
        SourceManagerServiceClient._validate(data, 'request-1', [{'source_id': 10, 'type': 'rss'}])


def test_collect_rejects_duplicate_ids_and_batches_over_eight_before_network():
    client = SourceManagerServiceClient('http://source-manager:8080', 'private-token')
    with pytest.raises(SourceManagerServiceError, match='invalid_request'):
        asyncio.run(client.collect([{'source_id': 1, 'type': 'rss'}, {'source_id': 1, 'type': 'rss'}]))
    with pytest.raises(SourceManagerServiceError, match='invalid_request'):
        asyncio.run(client.collect([{'source_id': index + 1, 'type': 'rss'} for index in range(MAX_SOURCES + 1)]))


def test_collect_sends_private_bearer_and_bounded_contract_payload():
    observed = {}
    response_body = json.dumps(payload()).encode()

    class Content:
        async def iter_chunked(self, _size):
            yield response_body

    class Response:
        status = 200
        headers = {'Content-Encoding': 'identity'}
        content = Content()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class Session:
        def __init__(self, **kwargs):
            observed['session'] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def post(self, url, **kwargs):
            observed.update(url=url, **kwargs)
            return Response()

    client = SourceManagerServiceClient('https://source-manager.example', 'bearer-secret',
                                        session_factory=Session)
    result = asyncio.run(client.collect([{'source_id': 10, 'type': 'rss'}], request_id='request-1'))
    assert result['request_id'] == 'request-1'
    assert observed['url'] == 'https://source-manager.example/v1/sources/collect'
    assert observed['headers']['Authorization'] == 'Bearer bearer-secret'
    sent = json.loads(observed['data'])
    assert sent == {'contract_version': '1', 'request_id': 'request-1',
                    'batch_budget_ms': 115000, 'sources': [{'source_id': 10, 'type': 'rss'}]}
    assert observed['allow_redirects'] is False


def test_collect_cancellation_propagates_without_retrying_or_falling_back():
    started = asyncio.Event()

    class Content:
        async def iter_chunked(self, _size):
            started.set()
            await asyncio.Future()
            yield b''

    class Response:
        status = 200
        headers = {'Content-Encoding': 'identity'}
        content = Content()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class Session:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def post(self, *args, **kwargs):
            return Response()

    client = SourceManagerServiceClient('http://source-manager:8080', 'token', session_factory=Session)

    async def run():
        task = asyncio.create_task(client.collect([{'source_id': 10, 'type': 'rss'}]))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())


def test_go_modes_are_defaults_and_runtime_config_is_validated_at_startup():
    from news_aggregator.config import Settings
    from pydantic import ValidationError

    defaults = Settings(_env_file=None)
    assert defaults.source_manager_mode == 'go'
    assert defaults.telegram_source_mode == 'go'
    assert defaults.page_monitor_source_mode == 'go'
    with pytest.raises(ValidationError):
        Settings(_env_file=None, source_manager_mode='python')
    configured = Settings(
        _env_file=None, EXTRACTION_SERVICE_URL='http://extractor:8080',
        EXTRACTION_SERVICE_TOKEN='private-token-value',
        SCHEDULER_API_URL='https://scheduler.example.test',
        SCHEDULER_API_TOKEN='scheduler-token-value',
        SCHEDULED_JOBS_TOKEN='receiver-token-value')
    assert configured.validate_runtime_services() is True


@pytest.mark.asyncio
async def test_source_test_failure_and_preparation_failure_are_not_empty_feed_success(monkeypatch):
    from types import SimpleNamespace

    import news_aggregator.config as config
    from news_aggregator.services.source_manager import SourceManager

    settings = Settings(_env_file=None,
                        EXTRACTION_SERVICE_URL="http://extractor:8080",
                        EXTRACTION_SERVICE_TOKEN="private-token-value")
    monkeypatch.setattr(config, "get_settings", lambda: settings)

    class FailedClient:
        def __init__(self, *_args):
            pass

        async def collect(self, *_args, **_kwargs):
            raise SourceManagerServiceError("unavailable")

    monkeypatch.setattr(
        "news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient",
        FailedClient)
    manager = SourceManager()
    rss = SimpleNamespace(id=10, name="Feed", source_type="rss", url="https://feed.example/rss",
                          enabled=True, config={})
    assert await manager._fetch_from_go_source_manager([rss], 1, source_test=True) is False
    assert await manager._fetch_from_go_source_manager(
        [rss], 1, source_test=True, return_source_test_result=True) is None

    telegram = SimpleNamespace(id=11, name="Channel", source_type="telegram", url="https://t.me/example",
                               enabled=True, config={})

    async def cannot_prepare(_source):
        raise RuntimeError("fixture preparation failure")

    monkeypatch.setattr(manager, "get_source_instance", cannot_prepare)
    assert await manager._fetch_from_go_source_manager([telegram], 1, source_test=True) is False


@pytest.mark.asyncio
async def test_successful_empty_rss_collection_is_a_healthy_source_test(monkeypatch):
    from types import SimpleNamespace

    import news_aggregator.config as config
    from news_aggregator.services.source_manager import SourceManager

    settings = Settings(_env_file=None,
                        EXTRACTION_SERVICE_URL="http://extractor:8080",
                        EXTRACTION_SERVICE_TOKEN="private-token-value")
    monkeypatch.setattr(config, "get_settings", lambda: settings)

    class EmptyFeedClient:
        def __init__(self, *_args):
            pass

        async def collect(self, *_args, **_kwargs):
            return {"results": [{"source_id": 10, "type": "rss", "outcome": "success",
                                  "rss_articles": []}]}

    monkeypatch.setattr(
        "news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient",
        EmptyFeedClient)
    source = SimpleNamespace(id=10, name="Empty feed", source_type="rss",
                             url="https://feed.example/rss", enabled=True, config={})
    assert await SourceManager()._fetch_from_go_source_manager([source], 1, source_test=True) is True


@pytest.mark.asyncio
async def test_inline_connection_test_probes_disabled_source_without_enabling_it(monkeypatch):
    from types import SimpleNamespace

    import news_aggregator.config as config
    from news_aggregator.services.source_manager import SourceManager

    settings = Settings(_env_file=None,
                        EXTRACTION_SERVICE_URL="http://extractor:8080",
                        EXTRACTION_SERVICE_TOKEN="private-token-value")
    monkeypatch.setattr(config, "get_settings", lambda: settings)
    source = SimpleNamespace(
        id=13, name="Disabled feed", source_type="rss", url="https://feed.example/rss",
        enabled=False, config={}, last_success=None, error_count=3, last_error="old failure",
    )
    observed = {}

    class SuccessfulClient:
        def __init__(self, *_args):
            pass

        async def collect(self, sources, **_kwargs):
            observed.setdefault("enabled", []).append(sources[0]["enabled"])
            return {"results": [{"source_id": 13, "type": "rss", "outcome": "success",
                                  "rss_articles": []}]}

    monkeypatch.setattr(
        "news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient",
        SuccessfulClient)
    manager = SourceManager()
    monkeypatch.setattr(manager, "get_source_by_id", lambda _db, _source_id: _source())

    async def _source():
        return source

    class FakeSession:
        async def commit(self):
            pass

    assert await manager.test_source_connection(FakeSession(), source.id) is True
    assert await manager._fetch_from_go_source_manager([source], 1) == {13: []}
    assert observed["enabled"] == [True, False]
    assert source.enabled is False
    assert source.error_count == 0
    assert source.last_error is None


@pytest.mark.asyncio
async def test_page_monitor_source_test_uses_adapter_config_without_learning_or_state_updates(monkeypatch):
    import news_aggregator.config as config
    from news_aggregator.models import Source
    from news_aggregator.services.source_manager import SourceManager

    settings = Settings(_env_file=None,
                        EXTRACTION_SERVICE_URL="http://extractor:8080",
                        EXTRACTION_SERVICE_TOKEN="private-token-value")
    monkeypatch.setattr(config, "get_settings", lambda: settings)
    observed = {}

    class SuccessfulClient:
        def __init__(self, *_args):
            pass

        async def collect(self, sources, **_kwargs):
            observed["sources"] = sources
            return {"results": [{"source_id": 12, "type": "custom", "outcome": "success",
                                  "page_monitor": {}}]}

    monkeypatch.setattr(
        "news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient",
        SuccessfulClient)
    manager = SourceManager()
    source = Source(id=12, name="Fixture monitor", source_type="custom",
                    url="https://fixture.example/list", enabled=True, config={})
    adapter = await manager.get_source_instance(source)
    monitor = adapter.monitor
    before = (monitor.last_snapshot, monitor.failure_count, dict(monitor.learned_selectors),
              monitor._go_offset, dict(monitor._go_previous_segments))

    async def forbidden_learning_preparation():
        raise AssertionError("source-test must not run selector-learning preparation")

    monkeypatch.setattr(adapter, "prepare_go_collection", forbidden_learning_preparation)
    assert await manager._fetch_from_go_source_manager([source], 1, source_test=True) is True
    after = (monitor.last_snapshot, monitor.failure_count, dict(monitor.learned_selectors),
             monitor._go_offset, dict(monitor._go_previous_segments))
    assert after == before
    assert observed["sources"][0]["page_monitor"]["use_browser"] is True
    assert observed["sources"][0]["page_monitor"]["article_selectors"]
