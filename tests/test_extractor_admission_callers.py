"""Caller-level queue budget regressions using only local fake HTTP endpoints."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from aiohttp import web
from pydantic import SecretStr

from news_aggregator.extraction.admission import endpoint_admission
from news_aggregator.services.source_manager import SourceManager
from news_aggregator.sources.base import SourceInfo, SourceType
from news_aggregator.sources.page_monitor_source import PageMonitorConfig, PageMonitorSource
from news_aggregator.telegram.telegram_source import TelegramSource


TOKEN = 'local-test-token'


@asynccontextmanager
async def local_server(routes):
    app = web.Application()
    for path, handler in routes.items():
        app.router.add_post(path, handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    try:
        yield f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    finally:
        await runner.cleanup()


def settings(endpoint, **changes):
    values = dict(
        extraction_service_url=endpoint,
        extraction_service_token=SecretStr(TOKEN),
        extraction_service_total_budget_seconds=120.0,
        extraction_service_timeout_seconds=30.0,
        extraction_service_native_browser=False,
        extraction_http_max_bytes=5 * 1024 * 1024,
        extraction_text_max_chars=100_000,
        source_manager_mode='go',
        telegram_source_mode='go',
        page_monitor_source_mode='go',
    )
    values.update(changes)
    return SimpleNamespace(**values)


def post_article(request_id, name='Example'):
    return {
        'title': 'Short post', 'url': 'https://t.me/example/1', 'content': 'Short post',
        'summary': None, 'published_at': '2026-10-22T08:15:00Z', 'source_type': 'telegram',
        'source_name': name, 'image_url': None, 'media_files': [],
        'raw_data': {'channel': 'example', 'access_method': 'https://t.me/s/example',
                     'content_length': 10, 'media_info': {}, 'has_media': False, 'media_count': 0,
                     'external_links': [], 'hashtags': [], 'original_link': None,
                     'forwarded_from': None, 'message_id': request_id,
                     'telegram_url': 'https://t.me/example/1'},
    }


async def collect_telegram(source):
    return [article async for article in source._fetch_with_go(limit=1)]


@pytest.mark.asyncio
async def test_telegram_source_queued_fetch_keeps_full_rpc_budget(monkeypatch):
    observed_budget = []

    async def telegram_handler(request):
        data = await request.json()
        observed_budget.append(data['budget_ms'])
        return web.json_response({
            'contract_version': '1', 'build_version': 'test', 'request_id': data['request_id'],
            'original_url': data['channel_url'], 'final_url': 'https://t.me/s/example',
            'outcome': 'success', 'fetch_method': 'http',
            'articles': [post_article(data['request_id'], data['source_name'])],
        })

    async with local_server({'/v1/telegram/fetch': telegram_handler}) as endpoint:
        monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(endpoint))
        # Isolate the caller's outer budget without waiting for a real 115 seconds.
        import news_aggregator.telegram.telegram_source as telegram_module
        monkeypatch.setattr(telegram_module, 'time', SimpleNamespace(monotonic=lambda: 100.0))
        source = TelegramSource(SourceInfo('Example', SourceType.TELEGRAM,
                                           'https://t.me/s/example', ''))
        async with endpoint_admission(endpoint + '/v1/telegram/fetch', 2):
            task = asyncio.create_task(collect_telegram(source))
            await asyncio.sleep(0.08)
        articles = await task

    assert len(articles) == 1
    assert observed_budget and observed_budget[0] >= 29_990


@pytest.mark.asyncio
async def test_telegram_enrichment_queued_request_keeps_full_rpc_budget(monkeypatch):
    observed_budget = []

    async def extraction_handler(request):
        data = await request.json()
        observed_budget.append(data['budget_ms'])
        return web.json_response({
            'contract_version': '1', 'build_version': 'test', 'request_id': data['request_id'],
            'original_url': data['url'], 'final_url': data['url'], 'outcome': 'success',
            'observations': [],
            'result': {'content': 'Expanded article content with enough detail to replace the post.',
                       'title': 'Expanded article', 'publication_date': None, 'image_url': None,
                       'method_used': 'readability', 'selector_used': 'readability', 'media': []},
        })

    async with local_server({'/v1/extract': extraction_handler}) as endpoint:
        source = TelegramSource(SourceInfo('Example', SourceType.TELEGRAM,
                                           'https://t.me/s/example', ''))
        post = post_article('123')
        post['raw_data']['original_link'] = 'https://example.test/article'
        monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(endpoint))

        async def consume():
            return [article async for article in source.consume_go_collection(
                {'outcome': 'success', 'articles': [post]}, settings=settings(endpoint),
                deadline=asyncio.get_running_loop().time() + 60)]

        async with endpoint_admission(endpoint + '/v1/extract', 2):
            task = asyncio.create_task(consume())
            await asyncio.sleep(0.08)
        articles = await task

    assert len(articles) == 1
    assert articles[0].content.startswith('Expanded article content')
    assert observed_budget and observed_budget[0] >= 29_990


@pytest.mark.asyncio
async def test_page_monitor_queued_fetch_keeps_configured_wait_timeout(monkeypatch):
    observed_budget = []

    async def page_handler(request):
        data = await request.json()
        observed_budget.append(data['budget_ms'])
        return web.json_response({
            'contract_version': '1', 'build_version': 'test', 'request_id': data['request_id'],
            'outcome': 'success', 'original_url': data['url'], 'final_url': data['url'],
            'fetch_method': 'http', 'snapshot_at': '2026-10-23T10:00:00Z',
            'content_hash': 'a' * 32, 'selectors_used': [], 'items': [],
            'needs_selector_study': False,
        })

    async with local_server({'/v1/page-monitor/extract': page_handler}) as endpoint:
        monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(endpoint))
        config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=False,
                                   wait_for_js=False, wait_timeout_ms=120)
        monitor = PageMonitorSource(config)
        monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))
        async with endpoint_admission(endpoint + '/v1/page-monitor/extract', 2):
            task = asyncio.create_task(monitor.fetch_articles(limit=1))
            await asyncio.sleep(0.08)
        assert await task == []

    assert observed_budget and observed_budget[0] >= 110


@pytest.mark.asyncio
async def test_page_monitor_queued_same_snapshot_parse_keeps_full_rpc_budget(monkeypatch):
    parse_budget = []
    holder_acquired = asyncio.Event()
    release_holder = asyncio.Event()
    holder_tasks = []

    async def page_handler(request):
        data = await request.json()
        if data['operation'] == 'parse':
            parse_budget.append(data['budget_ms'])
        return web.json_response({
            'contract_version': '1', 'build_version': 'test', 'request_id': data['request_id'],
            'outcome': 'success', 'original_url': data['url'], 'final_url': data['url'],
            'fetch_method': 'provided_html' if data['operation'] == 'parse' else 'http',
            'snapshot_at': '2026-10-23T10:00:00Z', 'content_hash': 'b' * 32,
            'selectors_used': [], 'items': [],
            'needs_selector_study': data['operation'] == 'fetch',
            'study_html': '<article>captured</article>' if data['operation'] == 'fetch' else '',
        })

    async with local_server({'/v1/page-monitor/extract': page_handler}) as endpoint:
        monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(endpoint))
        config = PageMonitorConfig('https://example.test/updates', 'Example', use_browser=False,
                                   enable_ai_analysis=True)
        monitor = PageMonitorSource(config)
        monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))

        async def study(_html):
            async def hold_permit():
                async with endpoint_admission(endpoint + '/v1/page-monitor/extract', 2):
                    holder_acquired.set()
                    await release_holder.wait()
            holder_tasks.append(asyncio.create_task(hold_permit()))
            await holder_acquired.wait()
            return True

        monkeypatch.setattr(monitor, '_study_source_structure', study)
        task = asyncio.create_task(monitor.fetch_articles(limit=1))
        await holder_acquired.wait()
        await asyncio.sleep(0.08)
        release_holder.set()
        assert await task == []
        await asyncio.gather(*holder_tasks)

    assert parse_budget and parse_budget[0] >= 29_990


@pytest.mark.asyncio
async def test_source_manager_queued_batch_keeps_per_source_wire_budget(monkeypatch):
    observed_budget = []

    async def source_handler(request):
        data = await request.json()
        observed_budget.append(data['batch_budget_ms'])
        await asyncio.sleep(0.25)
        source = data['sources'][0]
        return web.json_response({
            'contract_version': '1', 'build_version': 'test', 'request_id': data['request_id'],
            'results': [{'source_id': source['source_id'], 'type': source['type'],
                         'outcome': 'success', 'rss_articles': []}],
        })

    async with local_server({'/v1/sources/collect': source_handler}) as endpoint:
        monkeypatch.setattr('news_aggregator.config.get_settings', lambda: settings(endpoint))
        source = SimpleNamespace(id=401, name='Feed', source_type='rss',
                                 url='https://example.test/feed', enabled=True, config={})
        manager = SourceManager()
        async with endpoint_admission(endpoint + '/v1/sources/collect', 2):
            task = asyncio.create_task(manager.fetch_from_all_sources_no_db(
                [source], per_source_timeout=1))
            await asyncio.sleep(0.8)
        assert await task == {401: []}

    assert observed_budget and observed_budget[0] >= 890
