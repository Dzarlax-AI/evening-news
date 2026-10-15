"""Resource limits apply before parsing, including compressed and browser data."""

import asyncio
import gzip
import json
import zlib
from unittest.mock import AsyncMock, Mock, patch

import brotli
import pytest
from aiohttp import ClientSession, web

from news_aggregator.core.resource_limits import (
    ResourceLimitError, enforce_text_limit, read_browser_html, read_page_bytes,
)


async def run_operation(coro, *args, **kwargs):
    return await coro


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


@pytest.mark.asyncio
async def test_browser_uses_bounded_js_value_and_preserves_html():
    html = '<html><head><title>Title</title></head><body><article class="custom">Text</article></body></html>'
    tab = Mock(evaluate=AsyncMock(return_value=json.dumps({"html": html})), get_content=AsyncMock())
    with patch("news_aggregator.core.browser_pool.run_browser_operation", side_effect=run_operation):
        assert await read_browser_html(tab) == html
    tab.get_content.assert_not_called()
    expression = tab.evaluate.call_args.args[0]
    assert "TextEncoder" in expression
    assert "JSON.stringify({error:" in expression
    assert tab.evaluate.call_args.kwargs["return_by_value"] is True


@pytest.mark.asyncio
async def test_browser_limit_is_terminal_and_multibyte_aware():
    from news_aggregator.config import settings
    tab = Mock(evaluate=AsyncMock(return_value=json.dumps({"html": "я" * 6})))
    with (
        patch.object(settings, "extraction_browser_max_bytes", 10),
        patch("news_aggregator.core.browser_pool.run_browser_operation", side_effect=run_operation),
        pytest.raises(ResourceLimitError, match="browser_limit"),
    ):
        await read_browser_html(tab)


@pytest.mark.asyncio
async def test_resource_limit_does_not_retry_or_train_bad_selector():
    from news_aggregator.extraction import ContentExtractor
    extractor = ContentExtractor()
    strategies = Mock(attempt_extraction_with_metadata=AsyncMock(side_effect=ResourceLimitError("budget")))
    extractor.core_extractor.strategies = strategies
    with (
        patch("news_aggregator.extraction.core_extractor.get_extraction_memory", AsyncMock()) as memory,
        pytest.raises(ResourceLimitError),
    ):
        await extractor.extract_article_content_with_metadata("https://example.com/article")
    strategies.attempt_extraction_with_metadata.assert_awaited_once()
    memory.assert_not_called()


def test_text_limit_enforced_without_silent_truncation():
    from news_aggregator.config import settings
    with patch.object(settings, "extraction_text_max_chars", 3):
        assert enforce_text_limit("abc") == "abc"
        with pytest.raises(ResourceLimitError):
            enforce_text_limit("abcd")


@pytest.mark.asyncio
async def test_parallel_requests_do_not_deadlock_at_rate_limit():
    from news_aggregator.core.http_client import RateLimiter
    limiter = RateLimiter(max_calls=1, time_window=0.01)
    await asyncio.wait_for(asyncio.gather(*(limiter.acquire() for _ in range(3))), timeout=1)
