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


def test_go_manager_mode_requires_both_typed_source_modes_and_private_service_config():
    with pytest.raises(ValueError, match='SOURCE_MANAGER_MODE=go requires TELEGRAM_SOURCE_MODE=go'):
        Settings(_env_file=None, source_manager_mode='go')
    with pytest.raises(ValueError, match='TELEGRAM_SOURCE_MODE=go requires EXTRACTION_SERVICE_URL'):
        Settings(_env_file=None, source_manager_mode='go', telegram_source_mode='go',
                 page_monitor_source_mode='go')
    configured = Settings(
        _env_file=None, source_manager_mode='go', telegram_source_mode='go',
        page_monitor_source_mode='go', extraction_service_url='http://extractor:8080',
        extraction_service_token=SecretStr('private-token'))
    assert configured.source_manager_mode == 'go'
