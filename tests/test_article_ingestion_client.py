"""Private Go ingestion client protocol and recovery tests."""

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from news_aggregator.services import article_ingestion_client as client_module
from news_aggregator.services.article_ingestion_client import (
    ArticleIngestionClient,
    ArticleIngestionError,
    _bounded_json,
    _run_id,
)
from news_aggregator.services.job_fence import current_job_fence


class Content:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, _size):
        for chunk in self.chunks:
            yield chunk


class Response:
    def __init__(self, status, payload=None, headers=None, error=None):
        self.status = status
        self.headers = headers or {}
        self.error = error
        data = json.dumps(payload).encode() if payload is not None else b""
        self.content = Content([data[i:i + 4096] for i in range(0, len(data), 4096)])

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        return False


class ResponseContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        self.response = await self.response
        return await self.response.__aenter__()

    async def __aexit__(self, *args):
        return await self.response.__aexit__(*args)


class ScriptedSession:
    def __init__(self, handler):
        self.handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def post(self, url, *, json=None, headers=None, allow_redirects=False):
        return ResponseContext(self.handler("POST", url, json, headers))

    def get(self, url, *, headers=None, allow_redirects=False):
        return ResponseContext(self.handler("GET", url, None, headers))


def active_fence():
    job_id = uuid.uuid4()
    attempt = 4
    return SimpleNamespace(
        active=True,
        job_id=job_id,
        attempt_generation=attempt,
        executor_instance_id=uuid.uuid4(),
        lease=SimpleNamespace(
            owner_id=uuid.uuid4(), generation=8, backend_pid=1234,
            deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
        ),
    )


def run_receipt(active, state, *, result=None):
    result = dict(result or {})
    result.setdefault("status", "success")
    result.setdefault("errors_count", 0)
    return {
        "run_id": _run_id(active.job_id, active.attempt_generation),
        "job_id": str(active.job_id),
        "attempt_generation": active.attempt_generation,
        "state": state,
        "result": result,
    }


@pytest.mark.asyncio
async def test_response_size_is_enforced_while_streaming():
    with pytest.raises(ArticleIngestionError, match="exceeded its limit"):
        await _bounded_json(SimpleNamespace(content=Content([b'{"a":', b'"x"' * 5000, b'}'])))


@pytest.mark.asyncio
async def test_busy_submit_retries_the_identical_fenced_envelope(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)
    submitted = []
    gets = 0

    async def transport(method, url, body, headers):
        nonlocal gets
        if method == "POST":
            submitted.append(body)
            if len(submitted) == 1:
                return Response(429, headers={"Retry-After": "0.1"})
            return Response(202, run_receipt(active, "running"))
        gets += 1
        return Response(200, run_receipt(active, "succeeded", result={
            "sources_completed": 2, "articles_fetched": 3,
        }))

    monkeypatch.setattr(client_module, "settings", SimpleNamespace(
        article_ingestion_url="http://127.0.0.1:8089", article_ingestion_token=SecretStr("fixture"),
    ))
    real_sleep = asyncio.sleep
    monkeypatch.setattr(client_module.asyncio, "sleep", lambda _: real_sleep(0))
    factory = lambda **kwargs: ScriptedSession(transport)
    try:
        result = await ArticleIngestionClient(client_factory=factory).collect_and_save()
    finally:
        current_job_fence.reset(token)
    assert len(submitted) == 2
    assert submitted[0] == submitted[1]
    assert result["sources_synced"] == 2
    assert result["articles_fetched"] == 3
    assert gets == 1


@pytest.mark.asyncio
async def test_lost_submit_ack_reconciles_then_retries_same_envelope(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)
    submitted = []
    gets = 0

    async def transport(method, url, body, headers):
        nonlocal gets
        if method == "POST":
            submitted.append(body)
            if len(submitted) == 1:
                return Response(0, error=asyncio.TimeoutError("fixture acknowledgement dropped"))
            return Response(202, run_receipt(active, "running"))
        gets += 1
        if gets == 1:
            return Response(404, {"error": "not_found"})
        return Response(200, run_receipt(active, "succeeded", result={
            "sources_completed": 1, "articles_fetched": 0,
        }))

    monkeypatch.setattr(client_module, "settings", SimpleNamespace(
        article_ingestion_url="http://127.0.0.1:8089", article_ingestion_token=SecretStr("fixture"),
    ))
    real_sleep = asyncio.sleep
    monkeypatch.setattr(client_module.asyncio, "sleep", lambda _: real_sleep(0))
    factory = lambda **kwargs: ScriptedSession(transport)
    try:
        result = await ArticleIngestionClient(client_factory=factory).collect_and_save()
    finally:
        current_job_fence.reset(token)
    assert len(submitted) == 2 and submitted[0] == submitted[1]
    assert gets == 2
    assert result["articles_fetched"] == 0


@pytest.mark.asyncio
async def test_ambiguous_cancellation_requires_confirmed_terminal_receipt(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)

    async def transport(method, url, body, headers):
        if method == "POST" and url.endswith("/cancel"):
            return Response(503)
        if method == "POST":
            return Response(202, run_receipt(active, "running"))
        if method == "GET":
            return Response(200, run_receipt(active, "running"))
        raise AssertionError(method)

    async def cancel_during_poll(_):
        active.active = False

    monkeypatch.setattr(client_module, "settings", SimpleNamespace(
        article_ingestion_url="http://127.0.0.1:8089", article_ingestion_token=SecretStr("fixture"),
    ))
    monkeypatch.setattr(client_module.asyncio, "sleep", cancel_during_poll)
    factory = lambda **kwargs: ScriptedSession(transport)
    try:
        with pytest.raises(ArticleIngestionError, match="cancellation was not confirmed"):
            await ArticleIngestionClient(client_factory=factory).collect_and_save()
    finally:
        current_job_fence.reset(token)


@pytest.mark.asyncio
async def test_confirmed_partial_source_warning_and_error_count_are_preserved(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)

    async def transport(method, url, body, headers):
        if method == "POST":
            return Response(202, run_receipt(active, "running"))
        return Response(200, run_receipt(active, "succeeded", result={
            "status": "warning", "sources_completed": 1, "articles_fetched": 2,
            "source_outcomes": {"resource_limit": 1, "upstream_unavailable": 2},
            "errors_count": 3,
        }))

    monkeypatch.setattr(client_module, "settings", SimpleNamespace(
        article_ingestion_url="http://127.0.0.1:8089", article_ingestion_token=SecretStr("fixture"),
    ))
    real_sleep = asyncio.sleep
    monkeypatch.setattr(client_module.asyncio, "sleep", lambda _: real_sleep(0))
    try:
        result = await ArticleIngestionClient(client_factory=lambda **kwargs: ScriptedSession(transport)).collect_and_save()
    finally:
        current_job_fence.reset(token)
    assert result["errors_count"] == 3
    assert result["errors"] == [
        "Go article ingestion source warning: resource_limit",
        "Go article ingestion source warning: upstream_unavailable",
        "Go article ingestion source warning: upstream_unavailable",
    ]
