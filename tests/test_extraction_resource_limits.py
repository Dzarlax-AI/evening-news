"""Resource limits apply before parsing, including compressed and browser data."""

import asyncio
import gzip
import zlib
from unittest.mock import Mock, patch

import brotli
import pytest
from aiohttp import ClientSession, web

from news_aggregator.core.resource_limits import (
    ResourceLimitError, enforce_text_limit, read_page_bytes,
)


@pytest.mark.parametrize("encoding,compress", [
    ("identity", lambda b: b), ("gzip", gzip.compress),
    ("deflate", zlib.compress), ("br", brotli.compress),
])
@pytest.mark.asyncio
async def test_chunked_compressed_body_limit_and_exact_boundary(encoding, compress):
    """Exercise real HTTP and decoders; encoded Content-Length is misleading."""
    payload = "Привет, мир!".encode() * 100
    encoded = compress(payload)

    async def handler(request):
        response = web.StreamResponse(headers={"Content-Encoding": encoding})
        await response.prepare(request)
        for index in range(0, len(encoded), 19):
            await response.write(encoded[index:index + 19])
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/page", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with ClientSession(auto_decompress=False) as session:
            async with session.get(f"http://127.0.0.1:{port}/page") as response:
                assert await read_page_bytes(response, len(payload)) == payload
            async with session.get(f"http://127.0.0.1:{port}/page") as response:
                with pytest.raises(ResourceLimitError):
                    await read_page_bytes(response, len(payload) - 1)
                assert response.closed
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_gzip_expansion_stops_at_output_budget():
    compressed = gzip.compress(b"x" * (10 * 1024 * 1024))
    response = Mock(headers={"Content-Encoding": "gzip"})

    async def chunks(size):
        yield compressed

    response.content.iter_chunked = chunks
    with pytest.raises(ResourceLimitError, match="http_limit"):
        await read_page_bytes(response, 64 * 1024)
    response.close.assert_called_once()


@pytest.mark.asyncio
async def test_unknown_encoding_fails_closed():
    response = Mock(headers={"Content-Encoding": "gzip, br"})
    with pytest.raises(ResourceLimitError, match="encoding"):
        await read_page_bytes(response, 10)
    response.close.assert_called_once()


def test_text_limit_enforced_without_silent_truncation():
    from news_aggregator.config import settings
    with patch.object(settings, "extraction_text_max_chars", 3):
        assert enforce_text_limit("abc") == "abc"
        with pytest.raises(ResourceLimitError):
            enforce_text_limit("abcd")


@pytest.mark.asyncio
async def test_legacy_post_obtains_a_native_permit_before_network_attempt(monkeypatch):
    from news_aggregator.core.http_client import AsyncHTTPClient
    permits = []

    class Runtime:
        async def permit(self, timeout):
            permits.append(timeout)

    class Response:
        pass

    class Session:
        closed = False

        async def post(self, *args, **kwargs):
            return Response()

    monkeypatch.setattr("news_aggregator.services.ai_runtime.get_ai_runtime", lambda: Runtime())
    client = AsyncHTTPClient()
    client.session = Session()
    response = await client.post("https://example.test/telegram", json={"text": "fixture"})
    assert isinstance(response, Response)
    assert permits == [60]
