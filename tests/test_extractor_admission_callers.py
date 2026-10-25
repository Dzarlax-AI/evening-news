"""Caller-level admission budget regression for the retained source probe."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from aiohttp import web
from pydantic import SecretStr

from news_aggregator.extraction.admission import endpoint_admission
from news_aggregator.services.source_manager import SourceManager


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


def settings(endpoint):
    return SimpleNamespace(
        extraction_service_url=endpoint,
        extraction_service_token=SecretStr('local-test-token'),
    )


@pytest.mark.asyncio
async def test_source_probe_queued_request_keeps_wire_budget(monkeypatch):
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
            task = asyncio.create_task(manager._probe_source(source))
            await asyncio.sleep(0.8)
        assert await task is True

    assert observed_budget and observed_budget[0] >= 25_000
