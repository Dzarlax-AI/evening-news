"""Controlled loopback-only CDP diagnostic; never navigates or closes the browser."""

import argparse
import asyncio
import json
from urllib.request import urlopen
from urllib.parse import urlsplit

import websockets


async def probe(args):
    endpoint = args.endpoint.rstrip('/')
    if urlsplit(endpoint).hostname not in {'127.0.0.1', 'localhost', '::1'}:
        raise ValueError('diagnostic_requires_isolated_loopback_endpoint')
    records = []
    for cycle in range(args.cycles):
        step = 'discovery'
        try:
            def discover():
                with urlopen(endpoint + '/json/version', timeout=3) as response:
                    return json.loads(response.read(65536))['webSocketDebuggerUrl']
            ws_url = await asyncio.to_thread(discover)
            if urlsplit(ws_url).hostname not in {'127.0.0.1', 'localhost', '::1'}:
                raise ValueError('unexpected_discovery_authority')
            async with websockets.connect(ws_url, max_size=1024*1024, ping_interval=None) as ws:
                seq = 0
                async def call(method, params=None, session=None):
                    nonlocal seq, step
                    step = method
                    seq += 1
                    payload = {'id': seq, 'method': method, 'params': params or {}}
                    if session:
                        payload['sessionId'] = session
                    await ws.send(json.dumps(payload))
                    async with asyncio.timeout(3):
                        while True:
                            reply = json.loads(await ws.recv())
                            if reply.get('id') == seq:
                                if 'error' in reply:
                                    raise RuntimeError('cdp_command_rejected')
                                return reply.get('result', {})
                await call('Browser.getVersion')
                if args.mode != 'connection':
                    config = {'disposeOnDetach': not args.no_dispose_on_detach}
                    if args.proxy:
                        config.update(proxyServer='http://127.0.0.1:9', proxyBypassList='<-loopback>')
                    context = (await call('Target.createBrowserContext', config))['browserContextId']
                    if args.mode == 'target':
                        target = (await call('Target.createTarget', {'url': 'about:blank', 'browserContextId': context}))['targetId']
                        session = (await call('Target.attachToTarget', {'targetId': target, 'flatten': True}))['sessionId']
                        await call('Page.enable', session=session)
                        await call('Page.stopLoading', session=session)
                        if not args.context_cleanup_only:
                            await call('Target.detachFromTarget', {'sessionId': session})
                            await call('Target.closeTarget', {'targetId': target})
                    await call('Target.disposeBrowserContext', {'browserContextId': context})
                await call('Target.getBrowserContexts')
            records.append({'cycle': cycle+1, 'passed': True})
        except Exception as exc:
            records.append({'cycle': cycle+1, 'passed': False, 'step': step, 'error': type(exc).__name__})
            break
    print(json.dumps({'mode': args.mode, 'proxy': args.proxy,
                      'dispose_on_detach': not args.no_dispose_on_detach,
                      'context_cleanup_only': args.context_cleanup_only, 'records': records}))
    return int(len(records) != args.cycles or not all(r['passed'] for r in records))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default='http://127.0.0.1:9222')
    parser.add_argument('--mode', choices=['connection', 'context', 'target'], required=True)
    parser.add_argument('--cycles', type=int, default=6, choices=range(1, 13))
    parser.add_argument('--proxy', action='store_true')
    parser.add_argument('--no-dispose-on-detach', action='store_true')
    parser.add_argument('--context-cleanup-only', action='store_true')
    raise SystemExit(asyncio.run(probe(parser.parse_args())))
