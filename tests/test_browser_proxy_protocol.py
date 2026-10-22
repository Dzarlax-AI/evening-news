"""Proxy context acceptance against installed nodriver and loopback CDP only."""

import asyncio
import json
import weakref

import pytest
import websockets
from nodriver import cdp
from nodriver.core.config import Config
from pydantic import SecretStr

from news_aggregator.config import Settings
from news_aggregator.core import browser_pool as bp, browser_proxy
from news_aggregator.core.exceptions import BrowserOperationTimeoutError, BrowserUnavailableError


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['initial', 'deferred', 'navigation_error', 'navigation_proxy_error',
    'navigation_proxy_tunnel',
    'delayed_load', 'load_cancel', 'publisher_auth', 'monitor_ok', 'monitor_error', 'next_job'])
async def test_proxy_context_real_nodriver_protocol(monkeypatch, mode):
    commands = []
    sockets = set()
    pending_navigation = {}
    load_events = []
    attachment_sessions = []
    target_attachment_sessions = []
    fetch_enabled_sessions = set()
    continued_request_sessions = []
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
                    session_id = f'loopback-session-{len(attachment_sessions) + 1}'
                    attachment_sessions.append(session_id)
                    if method == 'Target.attachToTarget':
                        target_attachment_sessions.append(session_id)
                    result = {'sessionId': session_id}
                elif method == 'Target.getTargetInfo':
                    result = {'targetInfo': {
                        'targetId': params['targetId'], 'type': 'page', 'title': 'fixture',
                        'url': 'about:blank', 'attached': True, 'canAccessOpener': False,
                    }}
                elif method == 'Page.navigate':
                    if mode in ('navigation_error', 'navigation_proxy_error', 'navigation_proxy_tunnel', 'monitor_error'):
                        if mode == 'navigation_proxy_error':
                            await ws.send(json.dumps({'method': 'Fetch.authRequired', 'params': {
                                'requestId': 'auth-request', 'frameId': 'frame', 'resourceType': 'Document',
                                'request': {'url': params['url'], 'method': 'GET', 'headers': {},
                                            'initialPriority': 'High', 'referrerPolicy': 'no-referrer'},
                                'authChallenge': {'source': 'Server', 'origin': 'https://publisher.invalid',
                                                 'scheme': 'basic', 'realm': 'fixture'},
                            }}))
                        if mode == 'navigation_proxy_tunnel':
                            session=command.get('sessionId')
                            await ws.send(json.dumps({'method': 'Fetch.requestPaused', 'params': {
                                'requestId': 'tunnel-request', 'frameId': 'frame', 'resourceType': 'Document',
                                'request': {'url': params['url'], 'method': 'GET', 'headers': {},
                                            'initialPriority': 'High', 'referrerPolicy': 'no-referrer'},
                                'networkId': 'tunnel-network', 'redirectedRequestId': None,
                            }, 'sessionId': session}))
                            await ws.send(json.dumps({'method': 'Fetch.authRequired', 'params': {
                                'requestId': 'tunnel-auth', 'frameId': 'frame', 'resourceType': 'Document',
                                'request': {'url': params['url'], 'method': 'GET', 'headers': {},
                                            'initialPriority': 'High', 'referrerPolicy': 'no-referrer'},
                                'authChallenge': {'source': 'Proxy', 'origin': proxy_url,
                                                 'scheme': 'basic', 'realm': 'fixture'},
                            }, 'sessionId': session}))
                            result = {'frameId': 'frame', 'errorText': 'net::ERR_TUNNEL_CONNECTION_FAILED'}
                        else:
                            result = {'frameId': 'frame', 'errorText': 'net::ERR_PROXY_CONNECTION_FAILED'}
                    elif mode in ('delayed_load', 'load_cancel'):
                        result = {'frameId': 'frame', 'loaderId': 'current-loader'}
                        await ws.send(json.dumps({'method': 'Page.lifecycleEvent', 'params': {
                            'frameId': 'frame', 'loaderId': 'previous-loader',
                            'name': 'DOMContentLoaded', 'timestamp': 1.0,
                        }, 'sessionId': command.get('sessionId')}))

                        if mode == 'delayed_load':
                            async def send_current_load():
                                await asyncio.sleep(0.025)
                                await ws.send(json.dumps({'method': 'Fetch.requestPaused', 'params': {
                                    'requestId': 'late-script', 'frameId': 'frame',
                                    'resourceType': 'Script',
                                    'request': {'url': 'https://example.com/app.js', 'method': 'GET',
                                                'headers': {}, 'initialPriority': 'High',
                                                'referrerPolicy': 'no-referrer'},
                                    'networkId': 'late-script-network', 'redirectedRequestId': None,
                                }, 'sessionId': command.get('sessionId')}))
                                await asyncio.sleep(0.025)
                                load_events.append('current-loader')
                                await ws.send(json.dumps({'method': 'Page.lifecycleEvent', 'params': {
                                    'frameId': 'frame', 'loaderId': 'current-loader',
                                    'name': 'DOMContentLoaded', 'timestamp': 2.0,
                                }, 'sessionId': command.get('sessionId')}))

                            asyncio.create_task(send_current_load())
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
                    if mode in ('publisher_auth', 'navigation_proxy_error'):
                        assert response == {'response': 'CancelAuth'}
                    else:
                        assert response == {'response': 'ProvideCredentials',
                                            'username': 'extractor', 'password': dummy_token}
                    if mode in ('navigation_proxy_error', 'navigation_proxy_tunnel'):
                        await ws.send(json.dumps({'id': command['id'], 'result': {}}))
                    else:
                        await ws.send(json.dumps({'id': pending_navigation.pop(ws), 'result': {'frameId': 'frame'}}))
                elif method == 'Fetch.continueRequest':
                    continued_request_sessions.append(command.get('sessionId'))
                    assert command.get('sessionId') in fetch_enabled_sessions
                elif method == 'Fetch.enable':
                    fetch_enabled_sessions.add(command.get('sessionId'))
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
    registered_tabs = []
    register_tab = bp._register_tab

    def track_tab(browser, tab, target_id=None):
        register_tab(browser, tab, target_id)
        registered_tabs.append(tab)

    monkeypatch.setattr(bp, '_register_tab', track_tab)
    settings = Settings(_env_file=None, BROWSER_PROXY_ENABLED=True,
                        BROWSER_PROXY_URL=proxy_url, BROWSER_PROXY_TOKEN=SecretStr(dummy_token),
                        BROWSER_TAB_CREATE_TIMEOUT_SECONDS=1 if mode == 'load_cancel' else 2,
                        BROWSER_TAB_CLOSE_TIMEOUT_SECONDS=1)
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
                    if mode == 'delayed_load':
                        assert load_events == ['current-loader']
                        assert len(target_attachment_sessions) == 1
                        assert continued_request_sessions == list(fetch_enabled_sessions)
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
            elif mode in ('navigation_error', 'navigation_proxy_error', 'navigation_proxy_tunnel', 'publisher_auth'):
                with pytest.raises(BrowserUnavailableError) as caught:
                    await asyncio.wait_for(operation(), 3)
                if mode == 'navigation_error':
                    assert caught.value.navigation_error_code == 'net::ERR_PROXY_CONNECTION_FAILED'
                if mode == 'navigation_proxy_error':
                    assert str(caught.value) == 'Proxy authentication challenge rejected'
                if mode == 'navigation_proxy_tunnel':
                    assert caught.value.navigation_error_code == 'net::ERR_TUNNEL_CONNECTION_FAILED'
                    assert caught.value.proxy_diagnostics == {
                        'request_paused': 1, 'request_continued': 1, 'request_blocked': 0,
                        'auth_required': 1, 'auth_proxy': 1, 'auth_server': 0, 'auth_other': 0,
                        'auth_provided': 1, 'auth_cancelled': 0, 'auth_continued': 1, 'handler_failed': 0,
                    }
            elif mode == 'load_cancel':
                with pytest.raises(BrowserOperationTimeoutError):
                    await asyncio.wait_for(operation(), 3)
            else:
                await asyncio.wait_for(operation(), 3)
            methods = [c['method'] for c in commands]
            assert methods.count('Target.disposeBrowserContext') == (2 if mode == 'next_job' else 1)
            assert methods.index('Fetch.enable') < methods.index('Page.navigate')
            if mode == 'delayed_load':
                assert len(fetch_enabled_sessions) == 1
                assert len(target_attachment_sessions) == 1
                assert continued_request_sessions == list(fetch_enabled_sessions)
            context = next(c['params'] for c in commands if c['method'] == 'Target.createBrowserContext')
            assert context['proxyServer'] == proxy_url
            assert context['proxyBypassList'] == '<-loopback>' and context['disposeOnDetach'] is True
            assert not browser.__dict__.get('_browser_pool_tabs'), [
                {'tab_id': id(tab), 'context': tab.__dict__.get('_browser_pool_context_id'),
                 'cleanup_done': getattr(tab.__dict__.get('_browser_pool_tab_cleanup'), 'done', lambda: None)(),
                 'handlers': len(tab.handlers)} for tab in browser.__dict__.get('_browser_pool_tabs', {}).values()]
            assert all(cdp.page.LifecycleEvent not in tab.handlers for tab in registered_tabs)
            assert not bp._active_tab_leases
            assert not bp._tab_semaphore.locked()
            for command in commands:
                if command['method'] != 'Fetch.continueWithAuth':
                    assert dummy_token not in json.dumps(command)
        finally:
            await bp.close_browser()
        await asyncio.sleep(0)
        assert not sockets
