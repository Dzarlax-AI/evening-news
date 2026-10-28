import asyncio
import json
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from news_aggregator.services import article_analysis as adapter
from news_aggregator.services import ai_client


class FakeWriter:
    def __init__(self):
        self.payload = bytearray()
        self.closed = False

    def write(self, payload):
        self.payload.extend(payload)

    async def drain(self):
        return None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class FakeProcess:
    def __init__(self, output=b"", returncode=0, *, wait_forever=False):
        self.stdin = FakeWriter()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode = returncode
        self.killed = False
        self.waited = False
        self._finished = asyncio.Event()
        if not wait_forever:
            self.stdout.feed_data(output)
            self.stdout.feed_eof()
            self.stderr.feed_eof()
            self._finished.set()

    async def wait(self):
        self.waited = True
        await self._finished.wait()
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self._finished.set()


def _adapter_stubs(monkeypatch, create_process):
    monkeypatch.delenv("NEWS_ARTICLE_ANALYSIS_BINARY", raising=False)
    limiter = SimpleNamespace(acquire=AsyncMock())

    @asynccontextmanager
    async def client_context():
        yield SimpleNamespace(rate_limiter=limiter)

    async def breaker_call(fn):
        return await fn()

    monkeypatch.setattr(adapter, "get_http_client", client_context)
    monkeypatch.setattr(adapter.ai_service_breaker, "call", breaker_call)
    monkeypatch.setattr(adapter, "_category_metadata", AsyncMock(return_value=(
        [{"name": "Tech", "display_name": "Технологии", "examples": ["AI"]}], "Tech")))
    monkeypatch.setattr(adapter.settings, "news_article_analysis_binary", "/usr/local/bin/fake-analysis")
    monkeypatch.setattr(adapter.settings, "local_category", "Tech")
    monkeypatch.setattr(adapter.asyncio, "create_subprocess_exec", create_process)
    return limiter


def _success_envelope():
    return {"version": 1, "ok": True,
            "result": {"summary": "synthetic summary", "categories": ["Tech"]},
            "usage": {"model": "gemini-test", "prompt_tokens": 7, "completion_tokens": 3,
                      "cached_tokens": 1, "total_tokens": 11}}


def _error_envelope(**error):
    return {"version": 1, "ok": False, "error": error}


@pytest.mark.asyncio
async def test_article_analysis_cli_success_records_usage_once_and_uses_narrow_environment(monkeypatch):
    process = FakeProcess(json.dumps(_success_envelope()).encode())
    calls = []

    async def create_process(*args, **kwargs):
        calls.append((args, kwargs))
        return process

    limiter = _adapter_stubs(monkeypatch, create_process)
    usages = []
    result = await adapter.analyze_article(
        "Synthetic title", "Synthetic content long enough for analysis", "https://example.test/story", None,
        api_key="synthetic-secret", model="gemini-test", endpoint="https://provider.test/v1/models",
        max_text_chars=12345, deadline=datetime.now(timezone.utc) + timedelta(seconds=10),
        on_usage=usages.append,
    )
    assert result["summary"] == "synthetic summary"
    assert usages == [{"model": "gemini-test", "prompt_tokens": 7, "completion_tokens": 3,
                       "cached_tokens": 1, "total_tokens": 11}]
    limiter.acquire.assert_awaited_once()
    args, kwargs = calls[0]
    assert args == ("/usr/local/bin/fake-analysis",)
    assert kwargs["env"] == {
        "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "GEMINI_API_KEY": "synthetic-secret", "SUMMARIZATION_MODEL": "gemini-test",
        "GEMINI_ENDPOINT": "https://provider.test/v1/models",
    }
    assert json.loads(bytes(process.stdin.payload))["max_text_chars"] == 12345
    assert process.waited and not process.killed


@pytest.mark.asyncio
async def test_article_analysis_retries_only_preheader_transport_errors(monkeypatch):
    outputs = [
        (json.dumps(_error_envelope(code="provider_transport_error", retryable_transport=True)).encode(), 1),
        (json.dumps(_error_envelope(code="provider_timeout", retryable_transport=True)).encode(), 1),
        (json.dumps({"version": 1, "ok": True, "result": {"summary": "ok"}}).encode(), 0),
    ]
    processes = []

    async def create_process(*_args, **_kwargs):
        output, code = outputs.pop(0)
        process = FakeProcess(output, code)
        processes.append(process)
        return process

    limiter = _adapter_stubs(monkeypatch, create_process)
    sleep = AsyncMock()
    monkeypatch.setattr(adapter.asyncio, "sleep", sleep)
    result = await adapter.analyze_article(
        "Title", "Body", "https://example.test", None, api_key="key", model="model",
        endpoint="https://provider.test/v1/models", max_text_chars=100000,
        deadline=datetime.now(timezone.utc) + timedelta(seconds=20), on_usage=lambda _usage: None,
    )
    assert result["summary"] == "ok"
    assert not outputs
    assert limiter.acquire.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [4, 4]
    assert all(p.waited for p in processes)


@pytest.mark.asyncio
async def test_provider_status_error_is_durable_without_immediate_retry(monkeypatch):
    process = FakeProcess(json.dumps(_error_envelope(
        code="provider_http_error", status_code=429, retry_after_seconds=17,
    )).encode(), 1)

    async def create_process(*_args, **_kwargs):
        return process

    limiter = _adapter_stubs(monkeypatch, create_process)
    sleep = AsyncMock()
    monkeypatch.setattr(adapter.asyncio, "sleep", sleep)
    with pytest.raises(adapter.ArticleAnalysisError) as caught:
        await adapter.analyze_article(
            "Title", "Body", "https://example.test", None, api_key="key", model="model",
            endpoint="https://provider.test/v1/models", max_text_chars=100000,
            deadline=datetime.now(timezone.utc) + timedelta(seconds=20), on_usage=lambda _usage: None,
        )
    assert caught.value.status_code == 429
    assert caught.value.retry_after_seconds == 17
    assert not caught.value.retryable_transport
    limiter.acquire.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_deadline_cancels_and_reaps_analysis_child(monkeypatch):
    process = FakeProcess(wait_forever=True)

    async def create_process(*_args, **_kwargs):
        return process

    _adapter_stubs(monkeypatch, create_process)
    with pytest.raises(adapter.ArticleAnalysisError) as caught:
        await adapter.analyze_article(
            "Title", "Body", "https://example.test", None, api_key="key", model="model",
            endpoint="https://provider.test/v1/models", max_text_chars=100000,
            deadline=datetime.now(timezone.utc) + timedelta(milliseconds=100), on_usage=lambda _usage: None,
        )
    assert caught.value.code == "provider_timeout"
    assert process.killed and process.waited


@pytest.mark.asyncio
async def test_ai_client_delegates_to_go_and_tracks_provider_usage_once(monkeypatch):
    monkeypatch.setattr(ai_client.settings, "gemini_api_key", "synthetic-secret")
    monkeypatch.setattr(ai_client.settings, "summarization_model", "gemini-test")
    monkeypatch.setattr(ai_client.settings, "gemini_api_endpoint", "https://provider.test/v1/models/")
    monkeypatch.setattr(ai_client.settings, "extraction_text_max_chars", 23456)
    usage_callback = Mock()
    expected = {"summary": "synthetic summary", "categories": ["Tech"]}
    async def delegated_analysis(*_args, **kwargs):
        kwargs["on_usage"]({
            "model": "gemini-test", "prompt_tokens": 3, "completion_tokens": 2,
            "cached_tokens": 1, "total_tokens": 6,
        })
        return expected

    delegate = AsyncMock(side_effect=delegated_analysis)
    monkeypatch.setattr(adapter, "analyze_article", delegate)
    client = ai_client.AIClient()
    client._track_ai_usage = usage_callback
    deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
    result = await client.analyze_article_complete(
        "Synthetic title", "Synthetic content with enough characters to pass validation.",
        "https://example.test/story", original_context="Original synthetic context", deadline=deadline,
    )
    assert result == expected
    delegate.assert_awaited_once()
    assert delegate.await_args.kwargs["api_key"] == "synthetic-secret"
    assert delegate.await_args.kwargs["model"] == "gemini-test"
    assert delegate.await_args.kwargs["endpoint"] == "https://provider.test/v1/models"
    assert delegate.await_args.kwargs["max_text_chars"] == 23456
    assert delegate.await_args.kwargs["deadline"] == deadline
    usage_callback.assert_called_once_with(
        {"usage": {"model": "gemini-test", "prompt_tokens": 3, "completion_tokens": 2,
                   "cached_tokens": 1, "total_tokens": 6}}, "combined_analysis", "example.test")


@pytest.mark.asyncio
async def test_ai_client_preserves_terminal_and_retry_after_error_metadata(monkeypatch):
    monkeypatch.setattr(ai_client.settings, "gemini_api_key", "synthetic-secret")
    monkeypatch.setattr(ai_client.settings, "summarization_model", "gemini-test")
    monkeypatch.setattr(ai_client.settings, "gemini_api_endpoint", "https://provider.test/v1/models")
    monkeypatch.setattr(ai_client.settings, "extraction_text_max_chars", 100000)
    delegate = AsyncMock(side_effect=adapter.ArticleAnalysisError(
        "provider_http_error", status_code=422, retry_after_seconds=9,
    ))
    monkeypatch.setattr(adapter, "analyze_article", delegate)
    client = ai_client.AIClient()
    result = await client.analyze_article_complete(
        "Title", "Synthetic content with enough characters to pass validation.", "https://example.test/story")
    assert result["_processing_error"] == "APIError"
    assert result["_processing_terminal"] is True
    assert result["_retry_after_seconds"] == 9


@pytest.mark.asyncio
async def test_existing_category_invalidation_refreshes_article_analysis_metadata(monkeypatch):
    monkeypatch.setattr(adapter, "_category_cache", None)
    monkeypatch.setattr(adapter, "_category_cache_at", 0.0)
    old = ([{"name": "Tech", "display_name": "Old", "examples": []}], "Tech")
    new = ([{"name": "Tech", "display_name": "New", "examples": ["synthetic"]}], "Tech")
    load = AsyncMock(side_effect=[old, new])
    monkeypatch.setattr(adapter, "_load_category_metadata", load)
    assert await adapter._category_metadata() == old

    category_cache = SimpleNamespace(invalidate=AsyncMock())
    from news_aggregator.services import category_cache as category_cache_module
    monkeypatch.setattr(category_cache_module, "get_category_cache", lambda: category_cache)
    from news_aggregator.services.web_backend import invalidate_category_cache
    await invalidate_category_cache()

    assert category_cache.invalidate.await_count == 1
    assert await adapter._category_metadata() == new
    assert load.await_count == 2


@pytest.mark.asyncio
async def test_actual_go_cli_against_loopback_gemini_provider(monkeypatch):
    binary = os.environ.get("NEWS_ARTICLE_ANALYSIS_BINARY") or adapter.settings.news_article_analysis_binary
    required = os.environ.get("NEWS_ARTICLE_ANALYSIS_INTEGRATION_REQUIRED") == "1"
    if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        if required:
            pytest.fail("NEWS_ARTICLE_ANALYSIS_INTEGRATION_REQUIRED=1 but the Go CLI binary is unavailable")
        pytest.skip("Go article-analysis binary is not available")

    class GeminiHandler(BaseHTTPRequestHandler):
        requests = 0

        def log_message(self, *_args):
            return

        def do_POST(self):
            type(self).requests += 1
            self.rfile.read(int(self.headers["Content-Length"]))
            if type(self).requests == 1:
                structured = json.dumps({
                    "optimized_title": "Синтетическая новость",
                    "summary": "Synthetic integration summary",
                    "categories": ["Tech"], "category_confidences": [0.91],
                    "category_confidence": 0.91, "summary_confidence": 0.88,
                    "is_advertisement": False, "ad_type": "news_article",
                    "ad_confidence": 0.04, "ad_reasoning": "Synthetic news",
                    "publication_date": "2026-01-02", "content_quality": 0.93,
                    "confidence": 0.89,
                }, ensure_ascii=False)
                provider_text = "Structured result follows:\n```json\n" + structured + "\n```"
                body = {
                    "candidates": [{"content": {"parts": [{"text": provider_text}]}}],
                    "usageMetadata": {"promptTokenCount": 12, "candidatesTokenCount": 8,
                                      "cachedContentTokenCount": 2, "totalTokenCount": 22},
                }
                encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
            else:
                self.send_response(429)
                self.send_header("Retry-After", "13")
                encoded = b'{"error":"synthetic rate limit"}'
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    GeminiHandler.requests = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), GeminiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    breaker = adapter.ai_service_breaker
    breaker.reset()
    try:
        monkeypatch.setattr(ai_client.settings, "gemini_api_key", "synthetic-loopback-key")
        monkeypatch.setattr(ai_client.settings, "summarization_model", "gemini-2.5-flash")
        monkeypatch.setattr(ai_client.settings, "gemini_api_endpoint",
                            f"http://127.0.0.1:{server.server_port}/v1/models")
        monkeypatch.setattr(ai_client.settings, "extraction_text_max_chars", 100000)
        monkeypatch.setattr(adapter.settings, "local_category", "Tech")
        monkeypatch.setattr(adapter, "_category_metadata", AsyncMock(return_value=(
            [{"name": "Tech", "display_name": "Технологии", "examples": ["synthetic mapping"]}], "Tech")))
        limiter = SimpleNamespace(acquire=AsyncMock())

        @asynccontextmanager
        async def admission():
            yield SimpleNamespace(rate_limiter=limiter)

        monkeypatch.setattr(adapter, "get_http_client", admission)
        usage = Mock()
        client = ai_client.AIClient()
        client._track_ai_usage = usage
        deadline = datetime.now(timezone.utc) + timedelta(seconds=30)
        result = await client.analyze_article_complete(
            "Synthetic integration title",
            "Synthetic article content provides enough characters for analysis.",
            "https://example.test/synthetic", deadline=deadline,
        )
        assert result["summary"] == "Synthetic integration summary"
        assert result["optimized_title"] == "Синтетическая новость"
        assert result["categories_parsed"] == [{"name": "Tech", "confidence": 1.0}]
        usage.assert_called_once()
        assert usage.call_args.args[0]["usage"]["total_tokens"] == 22

        rate_limited = await client.analyze_article_complete(
            "Synthetic rate-limit title",
            "Synthetic article content provides enough characters for analysis.",
            "https://example.test/rate-limit", deadline=deadline,
        )
        assert rate_limited["_processing_error"] == "APIError"
        assert rate_limited["_processing_terminal"] is False
        assert rate_limited["_retry_after_seconds"] == 13
        usage.assert_called_once()
        assert GeminiHandler.requests == 2
        assert limiter.acquire.await_count == 2
    finally:
        breaker.reset()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_parent_cancellation_kills_and_reaps_actual_go_child(monkeypatch):
    binary = os.environ.get("NEWS_ARTICLE_ANALYSIS_BINARY") or adapter.settings.news_article_analysis_binary
    required = os.environ.get("NEWS_ARTICLE_ANALYSIS_INTEGRATION_REQUIRED") == "1"
    if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        if required:
            pytest.fail("NEWS_ARTICLE_ANALYSIS_INTEGRATION_REQUIRED=1 but the Go CLI binary is unavailable")
        pytest.skip("Go article-analysis binary is not available")

    request_seen = threading.Event()
    release_provider = threading.Event()

    class BlockingGeminiHandler(BaseHTTPRequestHandler):
        requests = 0

        def log_message(self, *_args):
            return

        def do_POST(self):
            type(self).requests += 1
            self.rfile.read(int(self.headers["Content-Length"]))
            request_seen.set()
            release_provider.wait(timeout=2)
            body = b'{"candidates":[]}'
            try:
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    BlockingGeminiHandler.requests = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), BlockingGeminiHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    breaker = adapter.ai_service_breaker
    breaker.reset()
    processes = []
    original_create_process = asyncio.create_subprocess_exec

    async def capture_process(*args, **kwargs):
        process = await original_create_process(*args, **kwargs)
        processes.append(process)
        return process

    async def breaker_call(fn):
        return await fn()

    limiter = SimpleNamespace(acquire=AsyncMock())

    @asynccontextmanager
    async def admission():
        yield SimpleNamespace(rate_limiter=limiter)

    try:
        monkeypatch.setattr(adapter, "get_http_client", admission)
        monkeypatch.setattr(adapter, "_category_metadata", AsyncMock(return_value=(
            [{"name": "Tech", "display_name": "Технологии", "examples": []}], "Tech")))
        monkeypatch.setattr(adapter.ai_service_breaker, "call", breaker_call)
        monkeypatch.setattr(adapter.asyncio, "create_subprocess_exec", capture_process)
        monkeypatch.setattr(ai_client.settings, "gemini_api_key", "synthetic-cancel-key")
        monkeypatch.setattr(ai_client.settings, "summarization_model", "gemini-2.5-flash")
        monkeypatch.setattr(ai_client.settings, "gemini_api_endpoint",
                            f"http://127.0.0.1:{server.server_port}/v1/models")
        monkeypatch.setattr(ai_client.settings, "extraction_text_max_chars", 100000)
        monkeypatch.setattr(adapter.settings, "local_category", "Tech")
        client = ai_client.AIClient()
        usage = Mock()
        client._track_ai_usage = usage
        analysis_task = asyncio.create_task(client.analyze_article_complete(
            "Synthetic cancellation title",
            "Synthetic content long enough to reach the provider before cancellation.",
            "https://example.test/cancellation",
            deadline=datetime.now(timezone.utc) + timedelta(seconds=20),
        ))

        assert await asyncio.to_thread(request_seen.wait, 1.5), "Go child did not reach the fake provider"
        analysis_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await analysis_task

        assert len(processes) == 1, "parent cancellation must not trigger an immediate child retry"
        process = processes[0]
        assert process.returncode is not None
        await process.wait()
        assert process.returncode is not None, "cancelled child was not reaped"
        limiter.acquire.assert_awaited_once()
        usage.assert_not_called()
        assert BlockingGeminiHandler.requests == 1
    finally:
        release_provider.set()
        breaker.reset()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=1)
