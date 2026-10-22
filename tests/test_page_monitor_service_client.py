import asyncio
import hashlib
import json

import pytest

from news_aggregator.sources.page_monitor_service_client import (
    MAX_HTML_BYTES, MAX_RESPONSE_BYTES, PageMonitorServiceClient, PageMonitorServiceError,
)


class FakeContent:
    def __init__(self, body):
        self.body = body

    async def iter_chunked(self, size):
        for offset in range(0, len(self.body), size):
            yield self.body[offset:offset + size]


class FakeResponse:
    def __init__(self, body, status=200, headers=None):
        self.status = status
        self.headers = headers or {'Content-Encoding': 'identity'}
        self.content = FakeContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class FakeSession:
    def __init__(self, responder, state, **kwargs):
        state['session_options'] = kwargs
        self.responder, self.state = responder, state

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def post(self, url, *, data, headers, allow_redirects):
        self.state.update(url=url, data=data, headers=headers, allow_redirects=allow_redirects)
        return self.responder(json.loads(data))


class Config:
    url = 'https://example.test/updates'
    name = 'Example'
    article_selectors = ['article']
    title_selectors = ['h2']
    date_selectors = ['time']
    link_selectors = ['a[href]']
    learned_container_selectors = ['.learned']
    learned_title_selectors = ['h3']
    learned_link_selectors = ['a.story']
    learned_date_selectors = ['time[datetime]']
    learned_selectors = [('.general', 0.9)]
    min_title_length = 10
    use_browser = False
    wait_for_js = False


def item(**changes):
    title, link = 'A sufficiently long story', 'https://example.test/story'
    value = {'title': title, 'link': link,
             'description': 'Summary', 'published': '2026-10-22T08:15:00Z',
             'content_type': 'general', 'id': hashlib.md5((title + link).encode()).hexdigest(),
             'source': 'Example', 'source_url': Config.url}
    value.update(changes)
    return value


def envelope(request, **changes):
    value = {'contract_version': '1', 'build_version': 'test', 'request_id': request['request_id'],
             'outcome': 'success', 'original_url': Config.url, 'final_url': Config.url,
             'fetch_method': 'http' if request['operation'] == 'fetch' else 'provided_html',
             'snapshot_at': '2026-10-22T08:15:00Z', 'content_hash': 'a' * 32,
             'selectors_used': ['article'], 'items': [item()], 'needs_selector_study': False}
    value.update(changes)
    return value


def client_for(response_builder, state=None, max_bytes=MAX_RESPONSE_BYTES):
    state = state if state is not None else {}
    def responder(request):
        return response_builder(request)
    def factory(**kwargs):
        return FakeSession(responder, state, **kwargs)
    return PageMonitorServiceClient('http://extractor:8080', 'private-token',
                                    session_factory=factory, max_bytes=max_bytes), state


def test_fetch_request_is_authenticated_bounded_and_preserves_false_flags():
    client, state = client_for(lambda req: FakeResponse(json.dumps(envelope(req)).encode()))
    result = asyncio.run(client.fetch(Config, limit=4, timeout_seconds=2))
    request = json.loads(state['data'])
    assert request['contract_version'] == '1' and request['operation'] == 'fetch'
    assert request['use_browser'] is False and request['wait_for_js'] is False
    assert request['learned_general_selectors'] == [{'selector': '.general', 'confidence': 0.9}]
    assert state['headers']['Authorization'] == 'Bearer private-token'
    assert state['headers']['Accept-Encoding'] == 'identity' and state['allow_redirects'] is False
    assert state['session_options']['trust_env'] is False
    assert result['items'][0]['title'] == item()['title']


def test_parse_sends_same_snapshot_without_browser_flags_and_bounds_html():
    client, state = client_for(lambda req: FakeResponse(json.dumps(envelope(req)).encode()))
    asyncio.run(client.parse(Config, '<article>same</article>',
                             __import__('datetime').datetime(2026, 10, 22, 8, 15),
                             limit=10, timeout_seconds=2))
    request = json.loads(state['data'])
    assert request['operation'] == 'parse' and request['provided_html'] == '<article>same</article>'
    assert request['snapshot_at'] == '2026-10-22T08:15:00Z'
    assert 'use_browser' not in request and 'wait_for_js' not in request
    client_small, _ = client_for(lambda req: None)
    with pytest.raises(PageMonitorServiceError, match='request_too_large'):
        asyncio.run(client_small.parse(Config, 'x' * (MAX_HTML_BYTES + 1),
                                       __import__('datetime').datetime.now(), limit=1, timeout_seconds=1))


@pytest.mark.parametrize('mutate', [
    lambda data: data.update(contract_version='2'),
    lambda data: data.update(request_id='wrong'),
    lambda data: data.update(original_url='https://evil.test/'),
    lambda data: data.update(unexpected='value'),
    lambda data: data.update(snapshot_at='not-a-date'),
    lambda data: data['items'][0].update(extra='value'),
    lambda data: data['items'][0].update(source='other'),
    lambda data: data.update(items=[item(), item()]),
    lambda data: data.update(error={'code': 'invented', 'retryable': False}, outcome='partial'),
])
def test_rejects_malformed_response(mutate):
    def response(req):
        data = envelope(req)
        mutate(data)
        return FakeResponse(json.dumps(data).encode())
    client, _ = client_for(response)
    with pytest.raises(PageMonitorServiceError, match='invalid_payload'):
        asyncio.run(client.fetch(Config, limit=1, timeout_seconds=2))


def test_partial_prefix_is_returned_but_failure_and_auth_are_finite_errors():
    def partial(req):
        return FakeResponse(json.dumps(envelope(req, outcome='partial',
            error={'code': 'resource_limit', 'retryable': False})).encode())
    client, _ = client_for(partial)
    assert asyncio.run(client.fetch(Config, limit=1, timeout_seconds=2))['outcome'] == 'partial'
    for code in ('deadline_exceeded', 'cancelled'):
        client_with_prefix, _ = client_for(lambda req, code=code: FakeResponse(json.dumps(
            envelope(req, outcome='partial', error={'code': code, 'retryable': code == 'deadline_exceeded'})).encode()))
        assert asyncio.run(client_with_prefix.fetch(Config, limit=1, timeout_seconds=2))['items']
    failed, _ = client_for(lambda req: FakeResponse(json.dumps(envelope(req, outcome='failed', items=[],
        error={'code': 'deadline_exceeded', 'retryable': True})).encode()))
    with pytest.raises(PageMonitorServiceError, match='deadline_exceeded'):
        asyncio.run(failed.fetch(Config, limit=1, timeout_seconds=2))
    no_method, _ = client_for(lambda req: FakeResponse(json.dumps(envelope(
        req, outcome='failed', fetch_method=None, snapshot_at=None, content_hash=None, items=[],
        error={'code': 'transport_failed', 'retryable': True})).encode()))
    with pytest.raises(PageMonitorServiceError, match='transport_failed'):
        asyncio.run(no_method.fetch(Config, limit=1, timeout_seconds=2))
    unauthorized, _ = client_for(lambda req: FakeResponse(b'', status=401))
    with pytest.raises(PageMonitorServiceError, match='unauthorized'):
        asyncio.run(unauthorized.fetch(Config, limit=1, timeout_seconds=2))


def test_response_byte_cap_and_invalid_limit():
    client, _ = client_for(lambda req: FakeResponse(b' ' * 1025), max_bytes=1024)
    with pytest.raises(PageMonitorServiceError, match='response_too_large'):
        asyncio.run(client.fetch(Config, limit=1, timeout_seconds=2))
    with pytest.raises(ValueError, match='invalid_page_monitor_limit'):
        asyncio.run(client.fetch(Config, limit=True, timeout_seconds=1))


def test_long_selected_title_and_large_minimum_title_length_match_go_bounds():
    long_title = 'A' * 5000
    long_item = item(title=long_title)
    long_item['id'] = hashlib.md5((long_item['title'] + long_item['link']).encode()).hexdigest()
    long_config = type('LongConfig', (Config,), {'min_title_length': 100000})
    client, state = client_for(lambda req: FakeResponse(json.dumps(
        envelope(req, items=[long_item])).encode()))
    result = asyncio.run(client.fetch(long_config, limit=1, timeout_seconds=2))
    assert len(result['items'][0]['title']) == 5000
    assert json.loads(state['data'])['min_title_length'] == 100000
