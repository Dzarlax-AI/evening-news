"""Exercise an isolated real Go service without fetching any publisher page."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from news_aggregator.extraction.service_client import ExtractionServiceClient


async def smoke(endpoint, token, expected_build):
    if not token:
        raise ValueError('EXTRACTOR_TEST_TOKEN is required')
    async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), trust_env=False) as session:
        async with session.get(endpoint + '/health/ready', allow_redirects=False) as response:
            assert response.status == 200, 'service is not ready'
            assert (await response.json())['capabilities']['http'] is True
        async with session.get(endpoint + '/version', allow_redirects=False) as response:
            assert response.status == 200
            version = await response.json()
            assert version['contract_version'] == '1'
            assert version['build_version'] == expected_build, 'wrong image/build selected'
        async with session.post(endpoint + '/v1/extract', json={},
                                allow_redirects=False) as response:
            assert response.status == 401, 'unauthenticated extraction was admitted'
        async with session.post(endpoint + '/v1/extract', json={
                'request_id': 'smoke-injection', 'url': 'https://example.com/story',
                'cancel_at': 'browser_html_capture'}, headers={
                    'Authorization': 'Bearer ' + token}, allow_redirects=False) as response:
            assert response.status == 400, 'replay-only field admitted over HTTP'

    manifest = json.loads((ROOT / 'tests/fixtures/go-parity/manifest.json').read_text('utf-8'))
    case = next(case for case in manifest['cases']
                if case['input'].get('strategy') == 'readability'
                and case['input'].get('html')
                and case['expected'].get('state') == 'extracted'
                and case['expected'].get('contains'))
    client = ExtractionServiceClient(endpoint, token)
    result = await client.extract(
        case['input']['url'], timeout_seconds=10, html=case['input']['html'],
        strategy='readability', source_type='article', use_browser=False)
    assert result['build_version'] == expected_build
    assert result['outcome'] == 'success', result.get('error')
    for anchor in case['expected']['contains']:
        assert anchor in result['result']['content'], 'source anchor lost'
    for noise in case['expected'].get('not_contains', []):
        assert noise not in result['result']['content'], 'source boilerplate retained'

    # The destination must be rejected before any request to this loopback port.
    denied = await client.extract('http://127.0.0.1:9/private', timeout_seconds=5)
    assert denied['outcome'] in {'rejected', 'failed'}
    assert denied.get('error', {}).get('code') == 'public_url_denied', denied.get('error')
    print(json.dumps({'api': '1', 'build': expected_build,
                      'fixture': case['id'], 'status': 'passed'}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--expected-build', required=True)
    args = parser.parse_args()
    asyncio.run(smoke(args.endpoint.rstrip('/'), os.environ.get('EXTRACTOR_TEST_TOKEN'),
                      args.expected_build))


if __name__ == '__main__':
    main()
