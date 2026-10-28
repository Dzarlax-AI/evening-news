"""Bounded Python adapter for the private Go article-analysis engine."""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlsplit

from sqlalchemy import select

from ..config import settings
from ..core.circuit_breaker import CircuitBreakerError, ai_service_breaker
from ..core.http_client import get_http_client
from ..models import Category, CategoryMapping

MAX_REQUEST_BYTES = 5 * 1024 * 1024
MAX_STDOUT_BYTES = 1024 * 1024
MAX_STDERR_BYTES = 16 * 1024
# Let the Go client's 40s request deadline return its structured timeout envelope.
MAX_PROVIDER_SECONDS = 42.0
MAX_ATTEMPTS = 3
_SAFE_ERROR_CODES = {
    "invalid_input", "resource_limit", "insufficient_content", "provider_not_configured",
    "provider_invalid_configuration", "provider_request_too_large", "provider_timeout",
    "provider_cancelled", "provider_transport_error", "provider_response_read_error",
    "provider_response_too_large", "provider_invalid_response", "provider_empty_response",
    "provider_invalid_json", "provider_http_error", "analysis_failed",
}
_CATEGORY_CACHE_TTL_SECONDS = 300.0
_category_cache: tuple[list[dict[str, Any]], str | None] | None = None
_category_cache_at = 0.0
_category_cache_lock = asyncio.Lock()


class ArticleAnalysisError(RuntimeError):
    """Finite analysis failure; never includes prompts, article text, or provider body."""

    def __init__(self, code: str, *, status_code: int | None = None,
                 retry_after_seconds: float | None = None, retryable_transport: bool = False):
        self.code = code if code in _SAFE_ERROR_CODES else "analysis_failed"
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.retryable_transport = retryable_transport
        super().__init__(self.code)


async def _category_metadata() -> tuple[list[dict[str, Any]], str | None]:
    """Read the same ordered category and mapping data used by the legacy prompt."""
    global _category_cache, _category_cache_at
    if _category_cache is not None and time.monotonic() - _category_cache_at < _CATEGORY_CACHE_TTL_SECONDS:
        return _category_cache
    async with _category_cache_lock:
        if _category_cache is not None and time.monotonic() - _category_cache_at < _CATEGORY_CACHE_TTL_SECONDS:
            return _category_cache
        result = await _load_category_metadata()
        if result is not None:
            _category_cache = result
            _category_cache_at = time.monotonic()
            return result
    return _fallback_categories()


async def invalidate_category_metadata_cache() -> None:
    """Expire category prompt metadata after the existing category-cache invalidation event."""
    global _category_cache, _category_cache_at
    async with _category_cache_lock:
        _category_cache = None
        _category_cache_at = 0.0


async def _load_category_metadata() -> tuple[list[dict[str, Any]], str | None] | None:
    try:
        from .database_queue import get_db_queue_manager

        async def load(session):
            category_result = await session.execute(
                select(Category.name, Category.display_name, Category.is_local).order_by(Category.name)
            )
            rows = category_result.all()
            map_result = await session.execute(
                select(CategoryMapping.ai_category, CategoryMapping.fixed_category)
                .order_by(CategoryMapping.fixed_category, CategoryMapping.usage_count.desc())
            )
            examples: dict[str, list[str]] = {}
            for ai_category, fixed_category in map_result.all():
                bucket = examples.setdefault(fixed_category, [])
                if len(bucket) < 5:
                    bucket.append(ai_category)
            categories = [{
                "name": str(name),
                "display_name": str(display_name),
                "examples": [str(value) for value in examples.get(name, [])],
            } for name, display_name, _is_local in rows]
            local = next((name for name, _display, is_local in rows if is_local), None)
            return categories, local

        return await get_db_queue_manager().execute_read(load)
    except Exception:
        return None


def _fallback_categories() -> tuple[list[dict[str, Any]], str | None]:
    return ([
            {"name": "Business", "display_name": "Бизнес", "examples": []},
            {"name": "Tech", "display_name": "Технологии", "examples": []},
            {"name": "Science", "display_name": "Наука", "examples": []},
            {"name": "Serbia", "display_name": "Сербия", "examples": []},
            {"name": "Other", "display_name": "Прочее", "examples": []},
        ], None)


def _child_environment(api_key: str, model: str, endpoint: str) -> dict[str, str]:
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    if api_key:
        env["GEMINI_API_KEY"] = api_key
    env["SUMMARIZATION_MODEL"] = model
    env["GEMINI_ENDPOINT"] = endpoint
    return env


async def _read_bounded(stream: asyncio.StreamReader, maximum: int) -> bytes:
    collected = bytearray()
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            return bytes(collected)
        if len(collected) + len(chunk) > maximum:
            raise ArticleAnalysisError("resource_limit")
        collected.extend(chunk)


async def _write_and_close(stream: asyncio.StreamWriter, payload: bytes) -> None:
    stream.write(payload)
    await stream.drain()
    stream.close()
    await stream.wait_closed()


def _kill_child(process: asyncio.subprocess.Process) -> None:
    try:
        process.kill()
    except ProcessLookupError:
        pass


def _parse_error(stdout: bytes) -> ArticleAnalysisError:
    try:
        envelope = json.loads(stdout.decode("utf-8"), parse_constant=lambda _v: (_ for _ in ()).throw(ValueError()))
        detail = envelope.get("error") if isinstance(envelope, dict) else None
        if not isinstance(detail, dict) or envelope.get("version") != 1 or envelope.get("ok") is not False:
            raise ValueError("invalid error envelope")
        code = detail.get("code")
        status = detail.get("status_code")
        retry_after = detail.get("retry_after_seconds")
        retryable = detail.get("retryable_transport", False)
        if status is not None and (type(status) is not int or not 100 <= status <= 599):
            status = None
        if retry_after is not None and (isinstance(retry_after, bool) or not isinstance(retry_after, (int, float))
                                        or not math.isfinite(float(retry_after)) or retry_after < 0):
            retry_after = None
        if type(retryable) is not bool:
            retryable = False
        return ArticleAnalysisError(code if isinstance(code, str) else "analysis_failed",
                                    status_code=status,
                                    retry_after_seconds=float(retry_after) if retry_after is not None else None,
                                    retryable_transport=retryable)
    except (UnicodeError, ValueError, TypeError, AttributeError):
        return ArticleAnalysisError("analysis_failed")


async def _run_cli_once(request: dict[str, Any], *, api_key: str, model: str, endpoint: str,
                        deadline: datetime) -> tuple[dict[str, Any], dict[str, Any] | None]:
    try:
        payload = json.dumps(request, ensure_ascii=False, allow_nan=False,
                             separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ArticleAnalysisError("invalid_input") from None
    if len(payload) > MAX_REQUEST_BYTES:
        raise ArticleAnalysisError("resource_limit")
    binary = os.environ.get("NEWS_ARTICLE_ANALYSIS_BINARY") or settings.news_article_analysis_binary
    if not isinstance(binary, str) or not binary.startswith("/"):
        raise ArticleAnalysisError("analysis_failed")
    remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        raise ArticleAnalysisError("provider_timeout", status_code=504)
    try:
        process = await asyncio.create_subprocess_exec(
            binary, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=_child_environment(api_key, model, endpoint),
        )
    except (OSError, ValueError):
        raise ArticleAnalysisError("analysis_failed") from None
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    tasks = [
        asyncio.create_task(_write_and_close(process.stdin, payload)),
        asyncio.create_task(_read_bounded(process.stdout, MAX_STDOUT_BYTES)),
        asyncio.create_task(_read_bounded(process.stderr, MAX_STDERR_BYTES)),
        asyncio.create_task(process.wait()),
    ]
    try:
        _write_task, stdout, _stderr, returncode = await asyncio.wait_for(
            asyncio.gather(*tasks), min(remaining, MAX_PROVIDER_SECONDS))
    except asyncio.TimeoutError:
        _kill_child(process)
        await process.wait()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise ArticleAnalysisError("provider_timeout", status_code=504) from None
    except asyncio.CancelledError:
        _kill_child(process)
        try:
            await asyncio.shield(process.wait())
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except ArticleAnalysisError:
        _kill_child(process)
        await process.wait()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except (OSError, BrokenPipeError):
        _kill_child(process)
        await process.wait()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise ArticleAnalysisError("analysis_failed") from None

    if returncode != 0:
        raise _parse_error(stdout)
    try:
        envelope = json.loads(stdout.decode("utf-8"), parse_constant=lambda _v: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, TypeError):
        raise ArticleAnalysisError("analysis_failed") from None
    if (not isinstance(envelope, dict) or envelope.get("version") != 1 or envelope.get("ok") is not True
            or set(envelope) - {"version", "ok", "result", "usage"}):
        raise ArticleAnalysisError("analysis_failed")
    result = envelope.get("result")
    usage = envelope.get("usage")
    if not isinstance(result, dict):
        raise ArticleAnalysisError("analysis_failed")
    if usage is not None:
        fields = {"model", "prompt_tokens", "completion_tokens", "cached_tokens", "total_tokens"}
        if (not isinstance(usage, dict) or set(usage) != fields or not isinstance(usage["model"], str)
                or not usage["model"] or any(type(usage[key]) is not int or not 0 <= usage[key] <= 100_000_000
                                              for key in fields - {"model"})):
            raise ArticleAnalysisError("analysis_failed")
    return result, usage


async def analyze_article(title: str, content: str, url: str, original_context: str | None,
                          *, api_key: str, model: str, endpoint: str, max_text_chars: int,
                          deadline: datetime, on_usage: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    """Invoke one analysis under the shared breaker and per-attempt rate gate."""
    categories, local_category = await _category_metadata()
    request = {
        "version": 1, "title": title, "content": content, "url": url,
        "original_context": original_context, "categories": categories,
        "local_category": local_category, "configured_local_category": settings.local_category,
        "model": model, "endpoint": endpoint, "max_text_chars": max_text_chars,
    }

    async def attempt_series():
        for attempt in range(MAX_ATTEMPTS):
            try:
                async with get_http_client() as client:
                    # The legacy AsyncHTTPClient.post acquires this limiter for each request attempt.
                    await client.rate_limiter.acquire()
                result, usage = await _run_cli_once(request, api_key=api_key, model=model,
                                                    endpoint=endpoint, deadline=deadline)
                if usage is not None:
                    on_usage(usage)
                return result
            except ArticleAnalysisError as exc:
                if not exc.retryable_transport or attempt + 1 == MAX_ATTEMPTS:
                    raise
                # Match tenacity wait_exponential(multiplier=1, min=4, max=10).
                await asyncio.sleep(min(max(2 ** attempt, 4), 10))
        raise ArticleAnalysisError("analysis_failed")

    try:
        return await ai_service_breaker.call(attempt_series)
    except CircuitBreakerError:
        raise ArticleAnalysisError("provider_http_error", status_code=503) from None
