"""Proxy context acceptance against installed nodriver and loopback CDP only."""

import asyncio
import json
import weakref

import pytest
import websockets
from nodriver.core.config import Config
from pydantic import SecretStr

from news_aggregator.config import Settings
from news_aggregator.core import browser_pool as bp, browser_proxy
from news_aggregator.core.exceptions import BrowserUnavailableError


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['initial', 'deferred', 'navigation_error', 'publisher_auth', 'monitor_ok', 'monitor_error', 'next_job'])
async def test_proxy_context_real_nodriver_protocol(monkeypatch, mode):
    commands = []
    sockets = set()
    pending_navigation = {}
    contexts = []
    targets = []
    dummy_token = 'loopback-only-proxy-credential'
    proxy_url = 'http://gateway.invalid:8081'

    async def fake_cdp(ws):
        sockets.add(ws)
        try:
            async for raw in ws:
                command = json.loads(raw)
                commands.append(command)
                method, params = command['method'], command.get('params', {})
                result = {}
                if method == 'Target.createBrowserContext':
                    contexts.append(f'owned-context-{len(contexts) + 1}')
                    result = {'browserContextId': contexts[-1]}
                elif method == 'Target.createTarget':
                    targets.append(f'owned-target-{len(targets) + 1}')
                    result = {'targetId': targets[-1]}
                elif method in ('Target.attachToBrowserTarget', 'Target.attachToTarget'):
                    result = {'sessionId': 'loopback-session'}
                elif method == 'Target.getTargetInfo':
                    result = {'targetInfo': {
                        'targetId': params['targetId'], 'type': 'page', 'title': 'fixture',
                        'url': 'about:blank', 'attached': True, 'canAccessOpener': False,
                    }}
                elif method == 'Page.navigate':
                    if mode in ('navigation_error', 'monitor_error'):
                        result = {'frameId': 'frame', 'errorText': 'net::ERR_PROXY_CONNECTION_FAILED'}
                    else:
                        pending_navigation[ws] = command['id']
                        await ws.send(json.dumps({'method': 'Fetch.authRequired', 'params': {
                            'requestId': 'auth-request', 'frameId': 'frame', 'resourceType': 'Document',
                            'request': {'url': params['url'], 'method': 'GET', 'headers': {},
                                        'initialPriority': 'High', 'referrerPolicy': 'no-referrer'},
                            'authChallenge': {'source': 'Server' if mode == 'publisher_auth' else 'Proxy',
                                              'origin': proxy_url, 'scheme': 'basic', 'realm': 'fixture'},
                        }}))
                        continue
                elif method == 'Fetch.continueWithAuth':
                    response = params['authChallengeResponse']
                    if mode == 'publisher_auth':
                        assert response == {'response': 'CancelAuth'}
                    else:
                        assert response == {'response': 'ProvideCredentials',
                                            'username': 'extractor', 'password': dummy_token}
                    await ws.send(json.dumps({'id': pending_navigation.pop(ws), 'result': {'frameId': 'frame'}}))
                elif method == 'Browser.close':
                    pytest.fail('Shared browser must never be closed')
                elif method == 'Target.closeTarget':
                    pytest.fail('Context cleanup must not close a foreign or default target')
                await ws.send(json.dumps({'id': command['id'], 'result': result}))
        finally:
            sockets.discard(ws)

    for name, value in (
        ('_browser', None), ('_lock', asyncio.Lock()), ('_cycle_lock', asyncio.Lock()),
        ('_tab_semaphore', asyncio.Semaphore(1)), ('_active_cycle_leases', set()),
        ('_active_tab_leases', set()), ('_close_when_idle', False),
        ('_detached_operation_tasks', set()), ('_managed_connections', weakref.WeakValueDictionary()),
    ):
        monkeypatch.setattr(bp, name, value)
    monkeypatch.setattr(browser_proxy, '_ownership_unknown', False)
    settings = Settings(_env_file=None, BROWSER_PROXY_ENABLED=True,
                        BROWSER_PROXY_URL=proxy_url, BROWSER_PROXY_TOKEN=SecretStr(dummy_token),
                        BROWSER_TAB_CREATE_TIMEOUT_SECONDS=2, BROWSER_TAB_CLOSE_TIMEOUT_SECONDS=1)
    monkeypatch.setattr('news_aggregator.config.settings', settings)
    async with websockets.serve(fake_cdp, '127.0.0.1', 0) as server:
        port = server.sockets[0].getsockname()[1]
        browser = bp.Browser(Config(host='127.0.0.1', port=port,
                                    browser_executable_path='loopback-only'))
        browser.websocket_url = f'ws://127.0.0.1:{port}'
        await browser.aopen()
        bp._browser = browser
        try:
            async def operation():
                initial = 'about:blank' if mode == 'deferred' else 'https://example.com/article'
                async with bp.browser_tab(initial) as tab:
                    if mode == 'deferred':
                        await bp.run_browser_operation(tab.get('https://example.com/article'),
                                                       'fixture navigation', browser_session=tab)
                    assert tab.__dict__['_browser_pool_context_id'] == contexts[-1]
            if mode == 'next_job':
                await asyncio.wait_for(operation(), 3)
                await asyncio.wait_for(operation(), 3)
                assert len(set(contexts)) == 2 and len(set(targets)) == 2
            elif mode in ('monitor_ok', 'monitor_error'):
                from types import SimpleNamespace
                from news_aggregator.sources.page_monitor_source import PageMonitorSource
                source = object.__new__(PageMonitorSource)
                source.config = SimpleNamespace(url='https://example.com/monitor', use_browser=True)
                assert await asyncio.wait_for(source.test_connection(), 3) is (mode == 'monitor_ok')
            elif mode in ('navigation_error', 'publisher_auth'):
                with pytest.raises(BrowserUnavailableError):
                    await asyncio.wait_for(operation(), 3)
            else:
                await asyncio.wait_for(operation(), 3)
            methods = [c['method'] for c in commands]
            assert methods.count('Target.disposeBrowserContext') == (2 if mode == 'next_job' else 1)
            assert methods.index('Fetch.enable') < methods.index('Page.navigate')
            context = next(c['params'] for c in commands if c['method'] == 'Target.createBrowserContext')
            assert context['proxyServer'] == proxy_url
            assert context['proxyBypassList'] == '<-loopback>' and context['disposeOnDetach'] is True
            assert not browser.__dict__.get('_browser_pool_tabs')
            assert not bp._active_tab_leases
            assert not bp._tab_semaphore.locked()
            for command in commands:
                if command['method'] != 'Fetch.continueWithAuth':
                    assert dummy_token not in json.dumps(command)
        finally:
            await bp.close_browser()
        await asyncio.sleep(0)
        assert not sockets
