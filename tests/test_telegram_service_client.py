import asyncio
import json
import pytest

# Load the existing source registry first; it owns TelegramSource's compatibility facade.
from news_aggregator.sources.base import Article  # noqa: F401
from news_aggregator.telegram.service_client import (
    MAX_RESPONSE_BYTES, TelegramServiceClient, TelegramServiceError,
)


CHANNEL = 'https://t.me/s/example'


def article(**changes):
    value = {
        'title': 'A post', 'url': 'https://example.org/story', 'content': 'Short post',
        'summary': None, 'published_at': '2026-10-22T08:15:00+02:00',
        'source_type': 'telegram', 'source_name': 'Example', 'image_url': None,
        'media_files': [{'url': 'https://cdn.example/video.mp4', 'type': 'video', 'thumbnail': None}],
        'raw_data': {'channel': 'example', 'access_method': CHANNEL, 'content_length': 10,
                     'media_info': {}, 'has_media': True, 'media_count': 1,
                     'external_links': ['https://example.org/story'], 'hashtags': [],
                     'original_link': 'https://example.org/story', 'forwarded_from': None,
                     'message_id': '123', 'telegram_url': 'https://t.me/example/123'},
    }
    value.update(changes)
    return value


def envelope(request_id, **changes):
    value = {
        'contract_version': '1', 'build_version': 'test', 'request_id': request_id,
        'original_url': CHANNEL, 'final_url': CHANNEL, 'outcome': 'success',
        'fetch_method': 'http', 'articles': [article()],
    }
    value.update(changes)
    return value


class FakeContent:
    def __init__(self, body):
        self.body = body

    async def iter_chunked(self, size):
        for offset in range(0, len(self.body), size):
            yield self.body[offset:offset + size]


class FakeResponse:
    def __init__(self, body, *, status=200, headers=None):
        self.status = status
        self.headers = headers or {'Content-Encoding': 'identity'}
        self.content = FakeContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class FakeSession:
    def __init__(self, response_factory, call_record, **kwargs):
        call_record['session_options'] = kwargs
        self.response_factory = response_factory
        self.call_record = call_record

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def post(self, url, *, data, headers, allow_redirects):
        self.call_record.update(url=url, data=data, headers=headers, allow_redirects=allow_redirects)
        return self.response_factory(data)


def test_client_sends_bounded_authenticated_request_and_preserves_contract():
    seen = {}

    def response(body):
        request = json.loads(body)
        return FakeResponse(json.dumps(envelope(request['request_id'])).encode())

    def session_factory(**kwargs):
        return FakeSession(response, seen, **kwargs)

    client = TelegramServiceClient('http://extractor', 'local-token', session_factory=session_factory)
    result = asyncio.run(client.fetch(CHANNEL, 'Example', limit=3, timeout_seconds=2))
    assert seen['headers']['Authorization'] == 'Bearer local-token'
    assert seen['headers']['Accept-Encoding'] == 'identity'
    assert seen['allow_redirects'] is False
    assert seen['session_options']['trust_env'] is False
    body = json.loads(seen['data'])
    assert body['contract_version'] == '1'
    assert body['request_id'] == result['request_id']
    assert body['channel_url'] == CHANNEL
    assert body['source_name'] == 'Example'
    assert body['limit'] == 3
    assert 1 <= body['budget_ms'] <= 2000
    assert result['articles'][0]['media_files'][0]['thumbnail'] is None


@pytest.mark.parametrize('mutate', [
    lambda data: data.update(contract_version='2'),
    lambda data: data.update(request_id='wrong'),
    lambda data: data.update(original_url='https://t.me/s/other'),
    lambda data: data.update(final_url='https://evil.example/s/example'),
    lambda data: data.update(final_url='https://t.me/s/example?redirect=1'),
    lambda data: data.update(unexpected=True),
    lambda data: data['articles'][0].update(extra=True),
    lambda data: data['articles'][0].update(published_at=123),
    lambda data: data['articles'][0].update(media_files='bad'),
    lambda data: data['articles'][0]['raw_data'].update(message_id=123),
    lambda data: data['articles'][0].update(source_name='wrong'),
    lambda data: data['articles'][0].update(published_at='2026-10-22T08:15:00'),
])
def test_client_rejects_invalid_envelope_and_article_types(mutate):
    def response(body):
        request = json.loads(body)
        data = envelope(request['request_id'])
        mutate(data)
        return FakeResponse(json.dumps(data).encode())

    def session_factory(**kwargs):
        return FakeSession(response, {}, **kwargs)

    client = TelegramServiceClient('http://extractor', 'token', session_factory=session_factory)
    with pytest.raises(TelegramServiceError, match='invalid_payload'):
        asyncio.run(client.fetch(CHANNEL, 'Example', limit=3, timeout_seconds=2))


def test_client_rejects_server_articles_above_requested_limit():
    def response(body):
        request = json.loads(body)
        return FakeResponse(json.dumps(envelope(
            request['request_id'], articles=[article(), article()])).encode())

    def session_factory(**kwargs):
        return FakeSession(response, {}, **kwargs)

    client = TelegramServiceClient('http://extractor', 'token', session_factory=session_factory)
    with pytest.raises(TelegramServiceError, match='invalid_payload'):
        asyncio.run(client.fetch(CHANNEL, 'Example', limit=1, timeout_seconds=2))


def test_client_bounds_response_and_handles_auth_and_invalid_json():
    def client_for(make_response, max_bytes=MAX_RESPONSE_BYTES):
        def response(body):
            return make_response(json.loads(body))
        def session_factory(**kwargs):
            return FakeSession(response, {}, **kwargs)
        return TelegramServiceClient('http://extractor', 'token', max_bytes=max_bytes,
                                     session_factory=session_factory)

    large = client_for(lambda _: FakeResponse(b' ' * 1025), max_bytes=1024)
    with pytest.raises(TelegramServiceError, match='response_too_large'):
        asyncio.run(large.fetch(CHANNEL, 'Example', limit=1, timeout_seconds=2))
    unauthorized = client_for(lambda _: FakeResponse(b'secret body', status=401))
    with pytest.raises(TelegramServiceError, match='unauthorized'):
        asyncio.run(unauthorized.fetch(CHANNEL, 'Example', limit=1, timeout_seconds=2))
    malformed = client_for(lambda _: FakeResponse(b'{'))
    with pytest.raises(TelegramServiceError, match='invalid_payload'):
        asyncio.run(malformed.fetch(CHANNEL, 'Example', limit=1, timeout_seconds=2))


def test_client_rejects_boolean_and_out_of_range_limits():
    client = TelegramServiceClient('http://extractor', 'token', session_factory=lambda **kwargs: None)
    for limit in (True, False, 0, -1, 101, 1.0):
        with pytest.raises(ValueError, match='invalid_telegram_limit'):
            asyncio.run(client.fetch(CHANNEL, 'Example', limit=limit, timeout_seconds=1))


def test_client_cancellation_closes_its_session():
    class BlockingResponse(FakeResponse):
        async def __aenter__(self):
            started.set()
            await asyncio.Future()

    started = asyncio.Event()
    state = {'closed': False}

    class BlockingSession(FakeSession):
        async def __aexit__(self, *args):
            state['closed'] = True
            return False

        def post(self, *args, **kwargs):
            return BlockingResponse(b'{}')

    def session_factory(**kwargs):
        return BlockingSession(lambda _: None, {}, **kwargs)

    async def run():
        client = TelegramServiceClient('http://extractor', 'token', session_factory=session_factory)
        task = asyncio.create_task(client.fetch(CHANNEL, 'Example', limit=1, timeout_seconds=20))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert state['closed'] is True
