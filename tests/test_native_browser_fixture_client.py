"""The native browser fixture client is independent of the retired News app."""
import asyncio
import builtins
import json
import sys

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest

from scripts import native_browser_fixture_client as fixture


@pytest.mark.asyncio
async def test_fixture_client_uses_only_direct_native_request_and_no_app_import(monkeypatch):
    before = {name for name in sys.modules if name == 'news_aggregator' or name.startswith('news_aggregator.')}
    original = builtins.__import__
    def deny_application(name, *args, **kwargs):
        if name == 'news_aggregator' or name.startswith('news_aggregator.'):
            raise AssertionError('retired application import')
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', deny_application)
    calls = []
    async def extract(request):
        assert request.headers['Authorization'] == 'Bearer fixture-token'
        assert request.headers['Accept-Encoding'] == 'identity'
        payload = await request.json()
        calls.append(payload)
        assert payload['strategy'] == 'browser_rendering'
        assert payload['source_type'] == 'article'
        assert payload['use_browser'] is True
        assert payload['max_bytes'] == 5 * 1024 * 1024
        assert payload['max_chars'] == 100000
        assert 1 <= payload['budget_ms'] <= 30000
        assert not ({'html', 'content', 'title', 'learned_selector'} & set(payload))
        return web.json_response({'contract_version': '1', 'request_id': payload['request_id'],
            'original_url': payload['url'], 'outcome': 'success',
            'result': {'content': 'synthetic article', 'title': 'Synthetic title'}, 'observations': []})
    app = web.Application()
    app.router.add_post('/v1/extract', extract)
    async with TestServer(app) as server:
        result = await fixture.ExtractionServiceClient(str(server.make_url('/')).rstrip('/'), 'fixture-token').extract(
            'https://native-fixture.test/article', timeout_seconds=30,
            strategy='browser_rendering', source_type='article', use_browser=True)
    assert result['result']['content'] == 'synthetic article'
    assert len(calls) == 1
    assert {name for name in sys.modules if name == 'news_aggregator' or name.startswith('news_aggregator.')} == before


@pytest.mark.asyncio
async def test_fixture_client_rejects_other_strategies_before_network():
    client = fixture.ExtractionServiceClient('http://127.0.0.1:1', 'fixture-token')
    with pytest.raises(fixture.ExtractionServiceError, match='invalid_fixture_request'):
        await client.extract('https://native-fixture.test/article', timeout_seconds=1,
                             strategy='auto', source_type='article', use_browser=False)


@pytest.mark.asyncio
async def test_fixture_client_rejects_bad_identity_and_large_response():
    async def extract(request):
        payload = await request.json()
        if large:
            return web.Response(body=b'x' * (5 * 1024 * 1024 + 1))
        return web.json_response({'contract_version': '1', 'request_id': 'wrong',
            'original_url': payload['url'], 'outcome': 'success', 'result': {'content': 'synthetic'}})
    for large, code in ((False, 'invalid_payload'), (True, 'response_too_large')):
        app = web.Application()
        app.router.add_post('/v1/extract', extract)
        async with TestServer(app) as server:
            client = fixture.ExtractionServiceClient(str(server.make_url('/')).rstrip('/'), 'fixture-token')
            with pytest.raises(fixture.ExtractionServiceError, match=code):
                await client.extract('https://native-fixture.test/article', timeout_seconds=5,
                                     strategy='browser_rendering', source_type='article', use_browser=True)


def test_fresh_interpreter_imports_harness_with_application_blocked():
    import subprocess
    source = """import builtins,sys
old=builtins.__import__
def deny(name,*args,**kwargs):
 if name=='news_aggregator' or name.startswith('news_aggregator.'):
  raise AssertionError('retired app imported')
 return old(name,*args,**kwargs)
builtins.__import__=deny
from scripts import native_browser_fixture_client, benchmark_native_cloak, run_native_shadow
assert not any(name=='news_aggregator' or name.startswith('news_aggregator.') for name in sys.modules)
"""
    result = subprocess.run([sys.executable, '-c', source], capture_output=True, text=True,
                            env={'PATH': '/usr/bin:/bin', 'PYTHONDONTWRITEBYTECODE': '1'}, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_fixture_admission_retries_only_explicit_429_and_keeps_one_request_id():
    requests = []
    async def extract(request):
        payload = await request.json()
        requests.append(payload['request_id'])
        if len(requests) == 1:
            return web.Response(status=429, headers={'Retry-After': '0'})
        return web.json_response({'contract_version': '1', 'request_id': payload['request_id'],
            'original_url': payload['url'], 'outcome': 'success',
            'result': {'content': 'synthetic article'}, 'observations': []})
    app = web.Application()
    app.router.add_post('/v1/extract', extract)
    async with TestServer(app) as server:
        client = fixture.ExtractionServiceClient(str(server.make_url('/')).rstrip('/'), 'fixture-token')
        result = await client.extract('https://native-fixture.test/article', timeout_seconds=5,
            strategy='browser_rendering', source_type='article', use_browser=True)
    assert result['outcome'] == 'success'
    assert len(requests) == 2 and requests[0] == requests[1]


@pytest.mark.asyncio
async def test_fixture_timeout_is_finite_and_never_retries():
    calls = []
    async def extract(request):
        calls.append(True)
        await asyncio.sleep(.15)
        return web.json_response({})
    app = web.Application()
    app.router.add_post('/v1/extract', extract)
    async with TestServer(app) as server:
        client = fixture.ExtractionServiceClient(str(server.make_url('/')).rstrip('/'), 'fixture-token')
        with pytest.raises(fixture.ExtractionServiceError, match='deadline_exceeded'):
            await client.extract('https://native-fixture.test/article', timeout_seconds=.03,
                strategy='browser_rendering', source_type='article', use_browser=True)
    assert len(calls) == 1
