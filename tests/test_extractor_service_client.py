"""Exercise real bounded HTTP plus routing without external services or state."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import aiohttp
from aiohttp import web
from pydantic import SecretStr

from news_aggregator.extraction.core_extractor import CoreExtractor
from news_aggregator.extraction.admission import endpoint_admission
from news_aggregator.extraction.service_client import ExtractionServiceClient, ExtractionServiceError
from news_aggregator.sources.page_monitor_service_client import PageMonitorServiceClient
from news_aggregator.sources.source_manager_service_client import SourceManagerServiceClient
from news_aggregator.telegram.service_client import TelegramServiceClient


URL = 'https://example.org/article'
TOKEN = 'isolated-test-token'


def response_for(request, **changes):
    return {'contract_version': '1', 'build_version': 'test',
            'request_id': request['request_id'], 'original_url': request['url'],
            'final_url': request['url'], 'outcome': 'success', 'observations': [],
            'result': {'content': 'Useful article content.', 'title': 'Title',
                       'publication_date': '2026-10-15', 'image_url': 'https://example.org/a.jpg',
                       'method_used': 'readability', 'selector_used': 'readability',
                       'media': [{'type': 'image', 'url': 'https://example.org/a.jpg'}]}, **changes}


@asynccontextmanager
async def server(handler):
    app = web.Application()
    app.router.add_post('/v1/extract', handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    try:
        yield f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_wire_metadata_media_and_learned_identity():
    async def handler(request):
        assert request.headers['Authorization'] == 'Bearer ' + TOKEN
        data = await request.json()
        assert data['learned_selector'] == '.story'
        assert data['learned_method'] == 'browser_rendering'
        assert 0 < data['budget_ms'] <= 1000
        assert 'cancel_at' not in data
        return web.json_response(response_for(data))
    async with server(handler) as endpoint:
        result = await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1,
            learned_pattern={'selector': '.story', 'method': 'browser_rendering'})
    assert result['result']['publication_date'] == '2026-10-15'
    assert result['result']['media'][0]['type'] == 'image'


@pytest.mark.asyncio
async def test_three_distinct_clients_share_single_slot_and_finish_in_one_pass():
    inflight = 0
    maximum = 0
    busy_responses = 0

    async def single_slot(request, response_factory):
        nonlocal inflight, maximum, busy_responses
        if inflight:
            busy_responses += 1
            return web.Response(status=429, headers={'Retry-After': '0'})
        inflight += 1
        maximum = max(maximum, inflight)
        try:
            data = await request.json()
            await asyncio.sleep(0.02)
            return response_factory(data)
        finally:
            inflight -= 1

    async def article_handler(request):
        return await single_slot(request, lambda data: web.json_response(response_for(data)))

    async def telegram_handler(request):
        def response(data):
            post = {
                'title': 'A post', 'url': 'https://example.org/story', 'content': 'Short post',
                'summary': None, 'published_at': '2026-10-22T08:15:00+02:00',
                'source_type': 'telegram', 'source_name': 'Example', 'image_url': None,
                'media_files': [],
                'raw_data': {'channel': 'example', 'access_method': 'https://t.me/s/example',
                    'content_length': 10, 'media_info': {}, 'has_media': False, 'media_count': 0,
                    'external_links': [], 'hashtags': [], 'original_link': None, 'forwarded_from': None,
                    'message_id': '123', 'telegram_url': 'https://t.me/example/123'},
            }
            return web.json_response({'contract_version': '1', 'build_version': 'test',
                'request_id': data['request_id'], 'original_url': data['channel_url'],
                'final_url': 'https://t.me/s/example', 'outcome': 'success',
                'fetch_method': 'http', 'articles': [post]})
        return await single_slot(request, response)

    app = web.Application()
    app.router.add_post('/v1/extract', article_handler)
    app.router.add_post('/v1/telegram/fetch', telegram_handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    endpoint = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    try:
        article_clients = [ExtractionServiceClient(endpoint, TOKEN) for _ in range(3)]
        telegram = TelegramServiceClient(endpoint, TOKEN)
        requests = [asyncio.create_task(client.extract(URL, timeout_seconds=1))
                    for client in article_clients]
        requests.append(asyncio.create_task(telegram.fetch(
            'https://t.me/s/example', 'Example', limit=1, timeout_seconds=1)))
        ai_progress = asyncio.create_task(asyncio.sleep(0.001, result='AI completed'))
        assert await ai_progress == 'AI completed'
        assert not all(request.done() for request in requests)
        results = await asyncio.gather(*requests)
    finally:
        await runner.cleanup()

    assert len(results) == 4
    assert maximum == 1
    assert busy_responses == 0


@pytest.mark.asyncio
async def test_all_four_clients_share_one_prefixed_service_base():
    inflight = 0
    maximum = 0
    overlap_rejections = 0

    async def unavailable(request):
        nonlocal inflight, maximum, overlap_rejections
        if inflight:
            overlap_rejections += 1
            return web.Response(status=429, headers={'Retry-After': '0'})
        inflight += 1
        maximum = max(maximum, inflight)
        try:
            await asyncio.sleep(0.01)
            return web.Response(status=404)
        finally:
            inflight -= 1

    app = web.Application()
    for route in ('/private/v1/extract', '/private/v1/telegram/fetch',
                  '/private/v1/page-monitor/extract', '/private/v1/sources/collect'):
        app.router.add_post(route, unavailable)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/private"
    config = SimpleNamespace(
        url=URL, name='Example', min_title_length=10, article_selectors=[], title_selectors=[],
        date_selectors=[], link_selectors=[], learned_container_selectors=[],
        learned_title_selectors=[], learned_link_selectors=[], learned_date_selectors=[],
        learned_selectors=[], use_browser=False, wait_for_js=False,
    )
    clients = [
        ExtractionServiceClient(base, TOKEN),
        TelegramServiceClient(base, TOKEN),
        PageMonitorServiceClient(base, TOKEN),
        SourceManagerServiceClient(base, TOKEN),
    ]
    try:
        outcomes = await asyncio.gather(
            clients[0].extract(URL, timeout_seconds=1),
            clients[1].fetch('https://t.me/s/example', 'Example', limit=1, timeout_seconds=1),
            clients[2].fetch(config, limit=1, timeout_seconds=1),
            clients[3].collect([{'source_id': 1, 'type': 'rss'}], timeout_seconds=1),
            return_exceptions=True,
        )
    finally:
        await runner.cleanup()

    assert all(isinstance(outcome, Exception) for outcome in outcomes)
    assert maximum == 1
    assert overlap_rejections == 0


@pytest.mark.asyncio
async def test_admission_wait_has_separate_deadline_from_http_timeout():
    started = asyncio.Event()
    release = asyncio.Event()
    requests = 0

    async def handler(request):
        nonlocal requests
        requests += 1
        data = await request.json()
        if requests == 1:
            started.set()
            await release.wait()
        return web.json_response(response_for(data))

    async with server(handler) as endpoint:
        first = asyncio.create_task(ExtractionServiceClient(endpoint, TOKEN).extract(
            URL, timeout_seconds=1, admission_timeout_seconds=1))
        await started.wait()
        second = asyncio.create_task(ExtractionServiceClient(endpoint, TOKEN).extract(
            URL, timeout_seconds=0.05, admission_timeout_seconds=0.5))
        await asyncio.sleep(0.08)
        release.set()
        await first
        await second

    assert requests == 2


@pytest.mark.asyncio
async def test_admission_timeout_is_busy_and_http_timeout_stays_deadline_exceeded():
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(request):
        data = await request.json()
        started.set()
        await release.wait()
        return web.json_response(response_for(data))

    async with server(handler) as endpoint:
        first = asyncio.create_task(ExtractionServiceClient(endpoint, TOKEN).extract(
            URL, timeout_seconds=1, admission_timeout_seconds=1))
        await started.wait()
        with pytest.raises(ExtractionServiceError, match='busy'):
            await ExtractionServiceClient(endpoint, TOKEN).extract(
                URL, timeout_seconds=0.2, admission_timeout_seconds=0.03)
        release.set()
        await first

    async def slow_handler(request):
        await asyncio.sleep(0.08)
        return web.json_response(response_for(await request.json()))

    async with server(slow_handler) as endpoint:
        with pytest.raises(ExtractionServiceError, match='deadline_exceeded'):
            await ExtractionServiceClient(endpoint, TOKEN).extract(
                URL, timeout_seconds=0.02, admission_timeout_seconds=0.2)


@pytest.mark.asyncio
async def test_explicit_busy_retries_only_within_retry_after_budget():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=429, headers={'Retry-After': '0'})
        return web.json_response(response_for(await request.json()))

    async with server(handler) as endpoint:
        result = await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1)

    assert result['outcome'] == 'success'
    assert calls == 2


@pytest.mark.asyncio
async def test_explicit_busy_retry_count_is_bounded():
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return web.Response(status=429, headers={'Retry-After': '0'})

    async with server(handler) as endpoint:
        with pytest.raises(ExtractionServiceError, match='busy'):
            await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1)

    assert calls == 9


@pytest.mark.asyncio
async def test_json_encoding_and_session_creation_wait_for_admission():
    sessions = []

    async def handler(request):
        return web.json_response(response_for(await request.json()))

    async with server(handler) as endpoint:
        def factory(**kwargs):
            session = aiohttp.ClientSession(**kwargs)
            sessions.append(session)
            return session

        client = ExtractionServiceClient(endpoint, TOKEN, session_factory=factory)
        with patch('news_aggregator.extraction.service_client.json.dumps', wraps=json.dumps) as encode:
            async with endpoint_admission(endpoint + '/v1/extract', 1):
                waiting = asyncio.create_task(client.extract(
                    URL, timeout_seconds=1, html='x' * 1_000_000))
                await asyncio.sleep(0.01)
                encode.assert_not_called()
                assert sessions == []
            await waiting
        assert sessions and all(session.closed for session in sessions)


@pytest.mark.asyncio
async def test_core_outer_deadline_during_admission_is_busy_and_manual_cancel_propagates():
    async def handler(request):
        return web.json_response(response_for(await request.json()))

    async with server(handler) as endpoint:
        settings, core, memory = routing(budget=0.05)
        settings.extraction_service_url = endpoint
        settings.extraction_service_timeout_seconds = 0.02
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory',
                      AsyncMock(return_value=memory)), \
                patch('news_aggregator.extraction.core_extractor.get_stability_tracker',
                      AsyncMock(return_value=Mock())):
            async with endpoint_admission(endpoint + '/v1/extract', 1):
                with pytest.raises(ExtractionServiceError, match='busy'):
                    await core.extract_article_content_with_metadata(URL)

            active = asyncio.Event()

            async def hold_permit():
                async with endpoint_admission(endpoint + '/v1/extract', 2):
                    active.set()
                    await asyncio.Future()

            holder = asyncio.create_task(hold_permit())
            await active.wait()
            client_task = asyncio.create_task(ExtractionServiceClient(endpoint, TOKEN).extract(
                URL, timeout_seconds=1, admission_timeout_seconds=2))
            await asyncio.sleep(0)
            client_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await client_task
            holder.cancel()
            with pytest.raises(asyncio.CancelledError):
                await holder


@pytest.mark.asyncio
async def test_outer_timeout_before_client_admission_deadline_stays_a_timeout():
    async def handler(request):
        return web.json_response(response_for(await request.json()))

    async with server(handler) as endpoint:
        async with endpoint_admission(endpoint + '/v1/extract', 1):
            client = ExtractionServiceClient(endpoint, TOKEN)
            phase = {'value': None}
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.02):
                    await client.extract(URL, timeout_seconds=1,
                        admission_timeout_seconds=2, admission_phase=phase)
            assert phase['value'] == 'waiting'


@pytest.mark.asyncio
async def test_transport_failure_is_not_replayed():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        request.transport.close()
        return web.Response()

    async with server(handler) as endpoint:
        with pytest.raises(ExtractionServiceError, match='transport_error'):
            await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1)

    assert calls == 1


@pytest.mark.asyncio
async def test_explicit_service_client_use_browser_remains_direct_native_request():
    observed = []

    async def handler(request):
        data = await request.json()
        observed.append(data['use_browser'])
        return web.json_response(response_for(data))

    async with server(handler) as endpoint:
        await ExtractionServiceClient(endpoint, TOKEN).extract(
            URL, timeout_seconds=1, use_browser=True)

    assert observed == [True]


@pytest.mark.parametrize('status,code,retryable', [
    (401, 'unauthorized', False), (429, 'busy', True), (503, 'unavailable', True),
    (413, 'request_too_large', False), (302, 'http_error', False)])
@pytest.mark.asyncio
async def test_error_classification_and_no_redirect(status, code, retryable):
    async def handler(request):
        return web.Response(status=status, headers={'Retry-After': '12', 'Location': '/other'},
                            text='sensitive article body')
    async with server(handler) as endpoint:
        with pytest.raises(ExtractionServiceError) as error:
            await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1)
    assert error.value.code == code
    assert error.value.retryable is retryable
    assert error.value.retry_after_seconds == 12
    assert TOKEN not in str(error.value)
    assert 'sensitive' not in str(error.value)


@pytest.mark.parametrize('change', [
    {'request_id': 'wrong'}, {'contract_version': '2'}, {'result': {'content': []}},
    {'result': {'content': 'yes', 'media': [{}]}}, {'observations': [{'success': 'yes'}]},
    {'outcome': 'failed', 'error': 'bad'}, {'outcome': 'success', 'result': {'content': ''}}])
@pytest.mark.asyncio
async def test_invalid_payloads_rejected(change):
    async def handler(request):
        return web.json_response(response_for(await request.json(), **change))
    async with server(handler) as endpoint:
        with pytest.raises(ExtractionServiceError, match='invalid_payload'):
            await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1)


@pytest.mark.asyncio
async def test_streamed_response_size_cap():
    async def handler(request):
        return web.Response(body=b' ' * 2000)
    async with server(handler) as endpoint:
        with pytest.raises(ExtractionServiceError, match='response_too_large'):
            await ExtractionServiceClient(endpoint, TOKEN, max_bytes=1000).extract(URL, timeout_seconds=1)


@pytest.mark.asyncio
async def test_malformed_response_closes_permit_for_next_request():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        data = await request.json()
        if calls == 1:
            return web.Response(text='{')
        return web.json_response(response_for(data))

    async with server(handler) as endpoint:
        client = ExtractionServiceClient(endpoint, TOKEN)
        with pytest.raises(ExtractionServiceError, match='invalid_payload'):
            await client.extract(URL, timeout_seconds=1)
        result = await client.extract(URL, timeout_seconds=1)

    assert result['outcome'] == 'success'
    assert calls == 2


@pytest.mark.parametrize('cancel', [False, True])
@pytest.mark.asyncio
async def test_deadline_and_cancellation_close_owned_session(cancel):
    sessions = []
    entered = asyncio.Event()
    release = asyncio.Event()
    import aiohttp
    def factory(**kwargs):
        session = aiohttp.ClientSession(**kwargs)
        sessions.append(session)
        return session
    async def handler(request):
        entered.set()
        await release.wait()
        return web.json_response(response_for(await request.json()))
    async with server(handler) as endpoint:
        task = asyncio.create_task(ExtractionServiceClient(endpoint, TOKEN, session_factory=factory).extract(
            URL, timeout_seconds=1 if cancel else 0.03))
        await entered.wait()
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else ExtractionServiceError):
            await task
        assert all(session.closed for session in sessions)
        release.set()


def routing(budget=1, native_browser=False):
    settings = SimpleNamespace(extraction_service_url='http://extractor:8080',
        extraction_service_token=SecretStr(TOKEN), extraction_service_timeout_seconds=.1,
        extraction_service_total_budget_seconds=budget,
        extraction_service_native_browser=native_browser,
        extraction_http_max_bytes=5242880, extraction_browser_max_bytes=5242880,
        extraction_text_max_chars=100000)
    utils = Mock()
    utils.clean_url.side_effect = lambda value: value
    utils.extract_domain.return_value = 'example.org'
    utils.is_good_content.return_value = True
    core = CoreExtractor(utils, Mock(), Mock())
    core._extract_article_content_python = AsyncMock(return_value={'content': 'Python authoritative'})
    memory = SimpleNamespace(get_successful_pattern=AsyncMock(return_value=None),
        record_extraction_attempt=AsyncMock(), degrade_pattern=AsyncMock())
    return settings, core, memory


@pytest.mark.asyncio
async def test_go_success_records_once_and_maps_metadata_media():
    settings, core, memory = routing()
    payload = response_for({'url': URL, 'request_id': 'unused'}, final_url='https://example.org/full')
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.core_extractor.get_stability_tracker', AsyncMock(return_value=Mock())), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(return_value=payload)):
        result = await core.extract_article_content_with_metadata(URL, 1)
    assert result['media_files'][0]['type'] == 'image'
    assert result['full_article_url'] == 'https://example.org/full'
    assert result['publication_date'] == '2026-10-15'
    memory.record_extraction_attempt.assert_awaited_once()
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_native_browser_go_route_opts_in_wire_and_preserves_learned_metadata(caplog):
    caplog.set_level('INFO', logger='extraction.core')
    settings, core, memory = routing(native_browser=True)
    memory.get_successful_pattern.return_value = {
        'selector': '.learned-story', 'method': 'browser_rendering'}
    captured = []

    async def handler(request):
        data = await request.json()
        captured.append(data)
        if not data['use_browser']:
            assert 'learned_selector' not in data and 'learned_method' not in data
            assert data['max_bytes'] == settings.extraction_http_max_bytes
            return web.json_response(response_for(data, outcome='failed',
                error={'code': 'browser_required', 'retryable': False}))
        assert data['learned_selector'] == '.learned-story'
        assert data['learned_method'] == 'browser_rendering'
        assert data['max_bytes'] == 5242880
        return web.json_response(response_for(data, result={
            'content': 'Native browser extracted the article.', 'title': 'Native title',
            'publication_date': '2026-10-22', 'author': 'Author', 'description': 'Summary',
            'image_url': 'https://example.org/native.jpg', 'method_used': 'browser_rendering',
            'selector_used': '.learned-story',
            'media': [{'type': 'image', 'url': 'https://example.org/native.jpg'}]}))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
                patch('news_aggregator.extraction.core_extractor.get_stability_tracker',
                      AsyncMock(return_value=Mock())):
            result = await core.extract_article_content_with_metadata(URL)

    assert [item['use_browser'] for item in captured] == [False, True]
    assert result['content'] == 'Native browser extracted the article.'
    assert result['title'] == 'Native title'
    assert result['publication_date'] == '2026-10-22'
    assert result['media_files'][0]['url'] == 'https://example.org/native.jpg'
    memory.get_successful_pattern.assert_awaited_once_with('example.org')
    memory.record_extraction_attempt.assert_awaited_once()
    core._extract_article_content_python.assert_not_called()
    retry_logs = [record for record in caplog.records
                  if getattr(record, 'event', None) == 'extractor_native_browser_retry']
    assert len(retry_logs) == 1
    assert retry_logs[0].code == 'browser_required'
    assert retry_logs[0].getMessage() == 'Extractor native browser retry reason=browser_required'


@pytest.mark.asyncio
async def test_native_browser_mode_http_success_uses_one_request_without_cdp_fallback():
    settings, core, memory = routing(native_browser=True)
    memory.get_successful_pattern.return_value = {
        'selector': '.learned-story', 'method': 'browser_rendering'}
    calls = []

    async def handler(request):
        data = await request.json()
        calls.append(data)
        assert data['use_browser'] is False
        assert 'learned_selector' not in data and 'learned_method' not in data
        return web.json_response(response_for(data, result={
            'content': 'The HTTP extractor returned a validated article.',
            'method_used': 'readability', 'media': []}))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
                patch('news_aggregator.extraction.core_extractor.get_stability_tracker',
                      AsyncMock(return_value=Mock())):
            result = await core.extract_article_content_with_metadata(URL)

    assert result['content'].startswith('The HTTP extractor')
    assert len(calls) == 1
    memory.get_successful_pattern.assert_awaited_once_with('example.org')
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_native_browser_http_probe_preserves_nonbrowser_learned_pattern():
    settings, core, memory = routing(native_browser=True)
    memory.get_successful_pattern.return_value = {
        'selector': '.learned-story', 'method': 'readability'}

    async def handler(request):
        data = await request.json()
        assert data['use_browser'] is False
        assert data['learned_selector'] == '.learned-story'
        assert data['learned_method'] == 'readability'
        return web.json_response(response_for(data))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            await core.extract_article_content_with_metadata(URL)

    memory.get_successful_pattern.assert_awaited_once_with('example.org')


@pytest.mark.parametrize('method', [
    'browser_rendering', 'browser_heuristics',
    'learned_pattern_browser_rendering', 'learned_pattern_browser_heuristics',
])
def test_browser_only_learned_methods_are_not_sent_on_http_probe(method):
    from news_aggregator.extraction.core_extractor import CoreExtractor

    pattern = {'selector': '.learned-story', 'method': method}
    assert CoreExtractor._request_learned_pattern(pattern, use_browser=False) is None
    assert CoreExtractor._request_learned_pattern(pattern, use_browser=True) is pattern


def test_native_browser_http_probe_preserves_http_learned_methods():
    from news_aggregator.extraction.core_extractor import CoreExtractor

    pattern = {'selector': '.learned-story', 'method': 'readability'}
    assert CoreExtractor._request_learned_pattern(pattern, use_browser=False) is pattern


@pytest.mark.parametrize('cancel', [False, True])
@pytest.mark.asyncio
async def test_native_browser_retry_uses_remaining_budget_and_propagates_cancellation(cancel):
    settings, core, memory = routing(native_browser=True, budget=.2)
    settings.extraction_service_timeout_seconds = 1
    browser_entered, browser_closed, release_browser = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def handler(request):
        data = await request.json()
        calls.append(data['use_browser'])
        if not data['use_browser']:
            await asyncio.sleep(.04)
            return web.json_response(response_for(data, outcome='failed',
                error={'code': 'browser_required', 'retryable': False}))
        browser_entered.set()
        try:
            await release_browser.wait()
        finally:
            browser_closed.set()

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            task = asyncio.create_task(core.extract_article_content_with_metadata(URL))
            await browser_entered.wait()
            if cancel:
                task.cancel()
            with pytest.raises(asyncio.CancelledError if cancel else ExtractionServiceError):
                await task
            release_browser.set()
            await asyncio.wait_for(browser_closed.wait(), timeout=1)

    assert calls == [False, True]
    assert browser_closed.is_set()
    core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()


@pytest.mark.asyncio
async def test_native_browser_uses_minimum_byte_cap_and_never_falls_back():
    settings, core, memory = routing(native_browser=True)
    settings.extraction_http_max_bytes = 5000
    settings.extraction_browser_max_bytes = 1024
    observed = []

    async def handler(request):
        data = await request.json()
        observed.append((data['use_browser'], data['max_bytes']))
        if not data['use_browser']:
            return web.json_response(response_for(data, outcome='failed',
                error={'code': 'browser_required', 'retryable': False}))
        return web.json_response(response_for(data, result={
            'content': 'x' * 3000, 'title': 'large', 'method_used': 'native_browser',
            'media': []}))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError, match='response_too_large'):
                await core.extract_article_content_with_metadata(URL)

    assert observed == [(False, 5000), (True, 1024)]
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_native_browser_failure_preserves_retryability_without_python_fallback():
    settings, core, memory = routing(native_browser=True)
    memory.get_successful_pattern.return_value = {
        'selector': '.learned-story', 'method': 'browser_rendering'}
    observed = []

    async def handler(request):
        data = await request.json()
        observed.append(data)
        if not data['use_browser']:
            return web.json_response(response_for(data, outcome='failed',
                error={'code': 'no_content', 'retryable': False}))
        return web.json_response(response_for(data, outcome='failed',
            error={'code': 'native_browser_timeout', 'retryable': True,
                   'retry_after_seconds': 12}, observations=[
                {'selector': '.learned-story', 'method': 'browser_rendering', 'success': False}]))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError) as failure:
                await core.extract_article_content_with_metadata(URL)

    assert failure.value.code == 'native_browser_timeout'
    assert failure.value.retryable is True
    assert failure.value.retry_after_seconds == 12
    assert [item['use_browser'] for item in observed] == [False, True]
    memory.degrade_pattern.assert_awaited_once_with(
        'example.org', '.learned-story', 'browser_rendering')
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_native_browser_strips_fragment_without_changing_article_identity_or_redirect():
    settings, core, memory = routing(native_browser=True)
    article_url = 'https://example.org/article#comments'
    settings.extraction_service_python_fallback = True
    captured = []

    async def handler(request):
        data = await request.json()
        captured.append(data)
        if not data['use_browser']:
            return web.json_response(response_for(data, outcome='failed',
                error={'code': 'browser_required', 'retryable': False}))
        return web.json_response(response_for(data))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
                patch('news_aggregator.extraction.core_extractor.get_stability_tracker',
                      AsyncMock(return_value=Mock())):
            result = await core.extract_article_content_with_metadata(article_url)

    assert captured[0]['url'] == 'https://example.org/article'
    assert [item['use_browser'] for item in captured] == [False, True]
    assert 'full_article_url' not in result
    assert memory.record_extraction_attempt.await_args.args[0].article_url == article_url
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('article_url', [
    'http://example.org/article#comments',
    'https://example.org:8443/article#comments',
])
async def test_native_mode_keeps_non_native_urls_on_go_http_path_without_python_fallback(article_url):
    settings, core, memory = routing(native_browser=True)
    settings.extraction_http_max_bytes = 4096
    settings.extraction_browser_max_bytes = 1024
    captured = []

    async def handler(request):
        data = await request.json()
        captured.append(data)
        return web.json_response(response_for(data, outcome='failed', error={
            'code': 'upstream_unavailable', 'retryable': True, 'retry_after_seconds': 12}))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError) as failure:
                await core.extract_article_content_with_metadata(article_url)

    assert len(captured) == 1
    assert captured[0]['url'] == article_url.split('#', 1)[0]
    assert captured[0]['use_browser'] is False
    assert captured[0]['max_bytes'] == 4096
    assert failure.value.code == 'upstream_unavailable'
    assert failure.value.retryable is True
    assert failure.value.retry_after_seconds == 12
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_native_redirect_sets_full_article_url_only_for_a_different_document():
    settings, core, memory = routing(native_browser=True)
    original = 'https://example.org/article#comments'

    async def handler(request):
        data = await request.json()
        return web.json_response(response_for(data, final_url='https://example.org/story#top'))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
                patch('news_aggregator.extraction.core_extractor.get_stability_tracker',
                      AsyncMock(return_value=Mock())):
            result = await core.extract_article_content_with_metadata(original)

    assert result['full_article_url'] == 'https://example.org/story#top'
    assert memory.record_extraction_attempt.await_args.args[0].article_url == original
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_go_success_records_result_and_applies_matching_observation():
    settings, core, memory = routing()
    memory.get_successful_pattern.return_value = {'selector': '.story', 'method': 'readability'}
    payload = response_for({'url': URL, 'request_id': 'unused'}, observations=[
        {'selector': '.story', 'method': 'readability', 'success': False}])
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.core_extractor.get_stability_tracker', AsyncMock(return_value=Mock())), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(return_value=payload)):
        result = await core.extract_article_content_with_metadata(URL)
    assert result['content'] == 'Useful article content.'
    memory.record_extraction_attempt.assert_awaited_once()
    memory.degrade_pattern.assert_awaited_once_with('example.org', '.story', 'readability')
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_service_failure_never_falls_back_to_python():
    settings, core, memory = routing()
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(side_effect=ExtractionServiceError('unavailable', True))):
        with pytest.raises(ExtractionServiceError, match='unavailable'):
            await core.extract_article_content_with_metadata(URL)
        core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()




@pytest.mark.asyncio
async def test_failed_metadata_content_fallback_does_not_restart_service_budget():
    settings, core, memory = routing()
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(
                side_effect=ExtractionServiceError('unavailable', True))) as remote:
        with pytest.raises(ExtractionServiceError):
            await core.extract_article_content_with_metadata(URL)
        with pytest.raises(ExtractionServiceError):
            await core.extract_article_content(URL)
    remote.assert_awaited_once()


@pytest.mark.asyncio
async def test_slow_learned_pattern_lookup_consumes_extraction_deadline():
    settings, core, memory = routing(budget=.03)
    async def slow_pattern(_domain):
        await asyncio.sleep(.2)
    memory.get_successful_pattern.side_effect = slow_pattern
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract',
                  new_callable=AsyncMock) as remote:
        with pytest.raises(ExtractionServiceError, match='deadline_exceeded'):
            await core.extract_article_content_with_metadata(URL)
    remote.assert_not_awaited()


@pytest.mark.asyncio
async def test_pattern_lookup_and_success_recording_share_one_deadline():
    settings, core, memory = routing(budget=.06)
    async def lookup(_domain):
        await asyncio.sleep(.035)
        return None
    async def record(_attempt):
        await asyncio.sleep(.2)
    memory.get_successful_pattern.side_effect = lookup
    memory.record_extraction_attempt.side_effect = record
    payload = response_for({'url': URL, 'request_id': 'unused'})
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.core_extractor.get_stability_tracker', AsyncMock(return_value=Mock())), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract',
                  AsyncMock(return_value=payload)):
        started = asyncio.get_running_loop().time()
        with pytest.raises(ExtractionServiceError, match='deadline_exceeded'):
            await core.extract_article_content_with_metadata(URL)
        assert asyncio.get_running_loop().time() - started < .15


def test_remote_error_labels_are_bounded():
    from news_aggregator.extraction.core_extractor import CoreExtractor
    assert CoreExtractor._service_error_label('secret body token') == 'other'


@pytest.mark.asyncio
async def test_malformed_port_is_reported_as_invalid_input():
    settings, core, memory = routing()
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
        with pytest.raises(ExtractionServiceError, match='invalid_input'):
            await core.extract_article_content_with_metadata('https://example.org:bad/article')


@pytest.mark.asyncio
async def test_learning_degrades_only_supplied_identity_once_after_accepted_success():
    settings, core, memory = routing()
    memory.get_successful_pattern.return_value = {'selector': '.learned', 'method': 'browser_rendering'}
    observation = {'selector': '.learned', 'method': 'browser_rendering', 'success': False}
    payload = response_for({'url': URL, 'request_id': 'unused'}, observations=[observation, observation,
        {'selector': '.unrequested', 'method': 'readability', 'success': False}])
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.core_extractor.get_stability_tracker', AsyncMock(return_value=Mock())), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(return_value=payload)):
        await core.extract_article_content_with_metadata(URL)
    memory.get_successful_pattern.assert_awaited_once()
    memory.degrade_pattern.assert_awaited_once_with('example.org', '.learned', 'browser_rendering')
    memory.record_extraction_attempt.assert_awaited_once()


@pytest.mark.asyncio
async def test_study_outcome_without_opted_in_fallback_never_calls_llm_or_python():
    settings, core, memory = routing()
    payload = response_for({'url': URL, 'request_id': 'unused'}, outcome='needs_selector_study',
                           study_html='<main>Bounded captured HTML</main>')
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(return_value=payload)):
        with pytest.raises(ExtractionServiceError, match='needs_selector_study'):
            await core.extract_article_content_with_metadata(URL)
    core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()


@pytest.mark.parametrize('native_browser', [False, True])
@pytest.mark.asyncio
async def test_needs_study_without_wire_html_preserves_service_error(native_browser):
    settings, core, memory = routing(native_browser=native_browser)

    async def handler(request):
        data = await request.json()
        response = response_for(data, outcome='needs_selector_study',
            needs_selector_study=True,
            error={'code': 'needs_selector_study', 'retryable': False})
        response.pop('study_html', None)
        return web.json_response(response)

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
                patch('news_aggregator.extraction.core_extractor.get_stability_tracker', AsyncMock(return_value=Mock())):
            with pytest.raises(ExtractionServiceError, match='needs_selector_study'):
                await core.extract_article_content_with_metadata(URL)

    core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()


@pytest.mark.parametrize('outcome,terminal', [
    ('failed', False), ('rejected', False), ('needs_selector_study', False), ('failed', True)])
@pytest.mark.asyncio
async def test_failed_learning_observations_degrade_only_matching_supplied_pattern(outcome, terminal):
    settings, core, memory = routing()
    memory.get_successful_pattern.return_value = {'selector': '.learned', 'method': 'browser_rendering'}
    observation = {'selector': '.learned', 'method': 'browser_rendering', 'success': False}
    payload = response_for({'url': URL, 'request_id': 'unused'}, outcome=outcome,
        error={'code': 'resource_limit' if terminal else 'no_content' if outcome == 'rejected'
               else 'needs_selector_study' if outcome == 'needs_selector_study' else 'transport_failed', 'retryable': not terminal},
        observations=[observation, observation,
            {'selector': '.unrequested', 'method': 'readability', 'success': False}])
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)), \
            patch('news_aggregator.extraction.service_client.ExtractionServiceClient.extract', AsyncMock(return_value=payload)):
        with pytest.raises(ExtractionServiceError) as error:
            await core.extract_article_content_with_metadata(URL)
        assert error.value.terminal is terminal
    memory.degrade_pattern.assert_awaited_once_with('example.org', '.learned', 'browser_rendering')
    core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()








@pytest.mark.parametrize('code,allowed', [
    ('browser_required', True), ('upstream_forbidden', True), ('no_content', True),
    ('needs_selector_study', True), ('challenge', True), ('invalid_content', True),
    ('transport_failed', False), ('upstream_unavailable', False),
    ('public_url_denied', False), ('resource_limit', False), ('invalid_input', False),
    ('upstream_auth_required', False), ('upstream_rate_limited', False), ('cancelled', False),
    ('admission_unavailable', False), ('native_browser_policy_denied', False),
    ('native_browser_unsupported', False), ('native_browser_challenge', False),
    ('native_browser_http_rejected', False), ('native_browser_no_content', False),
    ('native_browser_unavailable', False), ('native_browser_timeout', False),
    ('native_browser_busy', False), ('new_unrecognized_failure', False)])
@pytest.mark.asyncio
async def test_native_browser_retry_uses_go_failure_allowlist(code, allowed):
    settings, core, memory = routing(native_browser=True)
    requests = []
    async def handler(request):
        data = await request.json()
        requests.append(data)
        if data['use_browser']:
            return web.json_response(response_for(data, result={
                'content': 'Go native browser returned validated content.',
                'method_used': 'browser_rendering', 'media': []}))
        result = response_for(data, outcome='needs_selector_study' if code == 'needs_selector_study' else 'failed',
            error={'code': code, 'retryable': False})
        return web.json_response(result)
    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            if allowed:
                result = await core.extract_article_content_with_metadata(URL)
                assert result['content'] == 'Go native browser returned validated content.'
                assert [request['use_browser'] for request in requests] == [False, True]
                memory.record_extraction_attempt.assert_awaited_once()
            else:
                with pytest.raises(ExtractionServiceError) as failure:
                    await core.extract_article_content_with_metadata(URL)
                assert failure.value.code == code
                assert [request['use_browser'] for request in requests] == [False]
                core._extract_article_content_python.assert_not_called()
    core._extract_article_content_python.assert_not_called()


@pytest.mark.parametrize('status,code', [(401, 'unauthorized'), (403, 'unauthorized'),
                                         (429, 'busy'), (503, 'unavailable')])
@pytest.mark.asyncio
async def test_service_auth_and_admission429_never_native_fallback(status, code):
    settings, core, memory = routing(native_browser=True)
    requests = 0

    async def handler(request):
        nonlocal requests
        requests += 1
        return web.Response(status=status, headers={'Retry-After': '39'})
    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError) as failure:
                await core.extract_article_content_with_metadata(URL)
    assert failure.value.code == code
    assert failure.value.retry_after_seconds == 39
    assert requests == 1
    core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()


@pytest.mark.parametrize('code', [
    'resource_limit', 'invalid_input', 'invalid_payload', 'public_url_denied',
    'upstream_auth_required', 'upstream_rate_limited', 'admission_unavailable',
    'native_browser_policy_denied', 'native_browser_unsupported', 'new_unrecognized_failure',
])
@pytest.mark.asyncio
async def test_native_browser_retry_does_not_expand_terminal_or_unknown_outcomes(code):
    settings, core, memory = routing(native_browser=True)
    requests = []

    async def handler(request):
        data = await request.json()
        requests.append(data)
        return web.json_response(response_for(data, outcome='failed',
            error={'code': code, 'retryable': False}))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError) as failure:
                await core.extract_article_content_with_metadata(URL)

    assert failure.value.code == code
    assert len(requests) == 1
    assert requests[0]['use_browser'] is False
    core._extract_article_content_python.assert_not_called()


@pytest.mark.parametrize('endpoint,token', [(None, SecretStr(TOKEN)), ('invalid-endpoint', SecretStr(TOKEN)),
                                         ('http://extractor:8080', None)])
@pytest.mark.asyncio
async def test_invalid_or_missing_configuration_never_native_fallback(endpoint, token):
    settings, core, memory = routing()
    settings.extraction_service_url, settings.extraction_service_token = endpoint, token
    with patch('news_aggregator.config.get_settings', return_value=settings), \
            patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
        with pytest.raises(ExtractionServiceError):
            await core.extract_article_content_with_metadata(URL)
    core._extract_article_content_python.assert_not_called()


@pytest.mark.asyncio
async def test_low_quality_http_success_is_rejected_without_learning_go_success():
    settings, core, memory = routing()
    core.utils.is_good_content.return_value = False
    async def handler(request):
        return web.json_response(response_for(await request.json()))
    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError, match='invalid_content'):
                await core.extract_article_content_with_metadata(URL)
    core._extract_article_content_python.assert_not_called()
    memory.record_extraction_attempt.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('retry_after', [1.5, -1, 'soon', True])
async def test_runtime_retry_after_rejects_invalid_wire_values(retry_after):
    async def handler(request):
        return web.json_response(response_for(await request.json(), outcome='failed',
            error={'code': 'upstream_rate_limited', 'retryable': True, 'retry_after_seconds': retry_after}))
    async with server(handler) as endpoint:
        with pytest.raises(ExtractionServiceError, match='invalid_payload'):
            await ExtractionServiceClient(endpoint, TOKEN).extract(URL, timeout_seconds=1)






def test_owned_page_crash_is_durably_retryable_without_immediate_python_fallback():
    from news_aggregator.extraction.core_extractor import CoreExtractor
    crash = ExtractionServiceError('native_browser_target_crashed', False)
    assert not crash.terminal
    assert not CoreExtractor._native_browser_retry_allowed(crash, None)
    assert not ExtractionServiceError('native_browser_unavailable', True).terminal


@pytest.mark.asyncio
async def test_owned_page_crash_does_not_use_configured_python_fallback():
    settings, core, memory = routing()
    requests = 0

    async def handler(request):
        nonlocal requests
        requests += 1
        return web.json_response(response_for(await request.json(), outcome='failed',
            error={'code': 'native_browser_target_crashed', 'retryable': False}))

    async with server(handler) as endpoint:
        settings.extraction_service_url = endpoint
        with patch('news_aggregator.config.get_settings', return_value=settings), \
                patch('news_aggregator.extraction.core_extractor.get_extraction_memory', AsyncMock(return_value=memory)):
            with pytest.raises(ExtractionServiceError) as error:
                await core.extract_article_content_with_metadata(URL)
    assert error.value.code == 'native_browser_target_crashed'
    assert not error.value.retryable
    assert not error.value.terminal
    assert requests == 1
    core._extract_article_content_python.assert_not_called()
