"""Narrow executor callback identity and replay admission tests."""

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import SecretStr

from news_aggregator import executor
from news_aggregator.services.article_processing_client import _run_id


def install_go_authority(monkeypatch, envelope, kind="news_processing", payload=None):
    """Supply the Go context and a transaction-fence double for callback tests."""
    from news_aggregator.services import domain_fence

    parsed = domain_fence.parse_envelope(envelope)
    context = domain_fence.ExecutionContext(parsed, 7890, kind, payload or {})

    async def get_context(job_id, attempt):
        assert (job_id, attempt) == (parsed["job_uuid"], parsed["attempt_generation"])
        return context

    class Fence:
        def __init__(self, current, **_kwargs):
            self.job_id = current.job_id
            self.attempt_generation = current.attempt_generation
            self.owner_id = current.envelope["owner_uuid"]
            self.generation = current.envelope["generation"]
            self.worker_backend_pid = current.envelope["backend_pid"]
            self.executor_instance_id = current.envelope["executor_uuid"]
            self.executor_backend_pid = current.executor_backend_pid
            self.deadline_at = current.envelope["deadline_db"]
            self.active = True

        async def validate(self, _session):
            return None

        @asynccontextmanager
        async def transaction_fence(self, _session, job_id, attempt):
            assert (job_id, attempt) == (self.job_id, self.attempt_generation)
            yield

    monkeypatch.setattr(domain_fence, "get_execution_context", get_context)
    monkeypatch.setattr(domain_fence, "RemoteDomainFence", Fence)


def test_callback_fingerprint_matches_go_compact_utf8_encoding():
    value = {
        "title": "café <news> \u2028 \u2029 & \U0001f680",
        "content": "line one\nline two / \\",
        "url": "https://example.test/a?x=1&y=2",
        "original_context": "context > body",
    }
    canonical = {
        "content": value["content"],
        "original_context": value["original_context"],
        "title": value["title"],
        "url": value["url"],
    }
    expected_json = json.dumps(canonical, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":"))
    expected_json = expected_json.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    expected = hashlib.sha256(expected_json.encode("utf-8")).hexdigest()

    assert executor._article_ai_input_fingerprint(value) == expected


@pytest.mark.asyncio
async def test_duplicate_inflight_callback_is_rejected_and_completed_replay_is_cached():
    key = ("run", 1, 2, "f" * 64, "analyze", "input-hash")
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE", executor.OrderedDict())
    monkeypatch.setattr(executor, "_ARTICLE_AI_ACTIVE", set())
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE_BYTES", 0)
    try:
        assert await executor._begin_article_ai_callback(key) is None
        with pytest.raises(HTTPException) as duplicate:
            await executor._begin_article_ai_callback(key)
        assert duplicate.value.status_code == 429

        response = {"version": 1, "result": {"summary": "synthetic"}}
        await executor._finish_article_ai_callback(key, response)
        assert await executor._begin_article_ai_callback(key) == response
        assert not executor._ARTICLE_AI_ACTIVE
    finally:
        monkeypatch.undo()


@pytest.mark.asyncio
async def test_article_ai_admission_rejects_new_work_after_bounded_active_limit(monkeypatch):
    monkeypatch.setattr(executor, "MAX_ARTICLE_AI_ACTIVE", 2)
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE", executor.OrderedDict())
    monkeypatch.setattr(executor, "_ARTICLE_AI_ACTIVE", set())
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE_BYTES", 0)

    first = ("run", 1, 2, "1" * 64, "analyze", "one")
    second = ("run", 2, 2, "2" * 64, "analyze", "two")
    third = ("run", 3, 2, "3" * 64, "analyze", "three")
    assert await executor._begin_article_ai_callback(first) is None
    assert await executor._begin_article_ai_callback(second) is None
    with pytest.raises(HTTPException) as full:
        await executor._begin_article_ai_callback(third)
    assert full.value.status_code == 429
    await executor._finish_article_ai_callback(first, None)
    await executor._finish_article_ai_callback(second, None)


class MappingResult:
    def __init__(self, value):
        self.value = value

    def mappings(self):
        return self

    def one_or_none(self):
        return self.value


class CallbackSession:
    def __init__(self, article_url, article_attempt, fingerprint, run_id):
        self.article_url = article_url
        self.article_attempt = article_attempt
        self.fingerprint = fingerprint
        self.run_id = run_id

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, statement, params):
        sql = str(statement)
        if "fence_article_processing_attempt" in sql:
            return MappingResult({"run_id": self.run_id, "run_state": "running"})
        if "article_processing_items" in sql:
            return MappingResult({"article_attempt": self.article_attempt,
                                  "input_fingerprint": self.fingerprint, "state": "running"})
        if "FROM articles" in sql:
            return MappingResult({"processing_attempts": self.article_attempt, "url": self.article_url})
        raise AssertionError("unexpected callback query")

    async def rollback(self):
        return None


def callback_body(job_id, owner_id, executor_id, deadline, run_id, fingerprint):
    return {
        "version": 1,
        "run_id": str(run_id),
        "envelope": {
            "version": 1, "job_id": str(job_id), "attempt_generation": 1,
            "owner_id": str(owner_id), "generation": 7, "backend_pid": 3456,
            "executor_instance_id": str(executor_id),
            "deadline_at": deadline.isoformat().replace("+00:00", "Z"),
        },
        "article_id": 72,
        "article_attempt": 2,
        "input_fingerprint": fingerprint,
        "operation": "analyze",
        "input": {"title": "Synthetic story", "content": "Synthetic body",
                  "url": "https://example.test/story"},
    }


def request_for_payload(payload, authorization="Bearer fixture-ai-token"):
    from starlette.requests import Request

    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request({
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "POST", "scheme": "http", "path": "/internal/v1/article-ai",
        "raw_path": b"/internal/v1/article-ai", "query_string": b"",
        "headers": [(b"content-type", b"application/json"),
                    (b"content-length", str(len(raw)).encode("ascii")),
                    (b"authorization", authorization.encode("ascii"))],
        "server": ("fixture", 80), "client": ("fixture", 1234),
    }, receive)


@pytest.mark.asyncio
async def test_article_ai_callback_fences_claim_and_caches_provider_replay(monkeypatch):
    job_id, owner_id = uuid.uuid4(), uuid.uuid4()
    executor_id = uuid.uuid4()
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    body_input = {"title": "Synthetic story", "content": "Synthetic body",
                  "url": "https://example.test/story"}
    fingerprint = executor._article_ai_input_fingerprint(body_input)
    run_id = uuid.UUID(_run_id(job_id, 1))
    body = callback_body(job_id, owner_id, executor_id, deadline, run_id, fingerprint)
    install_go_authority(monkeypatch, body["envelope"])
    ai = SimpleNamespace(analyze_article_complete=AsyncMock(return_value={
        "summary": "Synthetic summary", "categories": [{"name": "Tech"}],
    }))
    from news_aggregator.services import ai_client
    monkeypatch.setattr(ai_client, "get_ai_client", lambda: ai)
    monkeypatch.setattr(executor, "_article_ai_auth", lambda _request: None)
    monkeypatch.setattr(executor, "_require_executor_guard", AsyncMock())
    monkeypatch.setattr(executor, "AsyncSessionLocal", lambda: CallbackSession(
        body_input["url"], 2, fingerprint, run_id))
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE", executor.OrderedDict())
    monkeypatch.setattr(executor, "_ARTICLE_AI_ACTIVE", set())
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE_BYTES", 0)

    from starlette.requests import Request

    def request_for(payload):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": raw, "more_body": False}

        return Request({
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "POST", "scheme": "http", "path": "/internal/v1/article-ai",
            "raw_path": b"/internal/v1/article-ai", "query_string": b"",
            "headers": [(b"content-type", b"application/json"),
                        (b"content-length", str(len(raw)).encode("ascii"))],
            "server": ("fixture", 80), "client": ("fixture", 1234),
        }, receive)

    first = await executor.article_ai_callback(request_for(body))
    replay = await executor.article_ai_callback(request_for(body))

    assert first == replay
    assert first["result"]["summary"] == "Synthetic summary"
    ai.analyze_article_complete.assert_awaited_once_with(
        "Synthetic story", "Synthetic body", "https://example.test/story", original_context=None)


def test_article_ai_auth_uses_its_separate_token(monkeypatch):
    from starlette.requests import Request

    monkeypatch.setattr(executor, "settings", SimpleNamespace(article_ai_token=SecretStr("ai-only-token")))
    request = Request({"type": "http", "headers": [(b"authorization", b"Bearer wrong-token")]})
    with pytest.raises(HTTPException) as denied:
        executor._article_ai_auth(request)
    assert denied.value.status_code == 401


@pytest.mark.asyncio
async def test_context_then_feedback_preserves_observations_and_learned_pattern(monkeypatch):
    job_id, owner_id = uuid.uuid4(), uuid.uuid4()
    executor_id = uuid.uuid4()
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    article_input = {"title": "Synthetic story", "content": "Synthetic body",
                     "url": "https://example.test/story"}
    fingerprint = executor._article_ai_input_fingerprint(article_input)
    run_id = uuid.UUID(_run_id(job_id, 1))
    monkeypatch.setattr(executor, "_article_ai_auth", lambda _request: None)
    monkeypatch.setattr(executor, "_require_executor_guard", AsyncMock())
    monkeypatch.setattr(executor, "AsyncSessionLocal", lambda: CallbackSession(
        article_input["url"], 2, fingerprint, run_id))
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE", executor.OrderedDict())
    monkeypatch.setattr(executor, "_ARTICLE_AI_ACTIVE", set())
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE_BYTES", 0)
    monkeypatch.setattr(executor, "_ARTICLE_AI_CONTEXT_PATTERNS", executor.OrderedDict())

    from news_aggregator.services import extraction_memory
    memory = SimpleNamespace(get_successful_pattern=AsyncMock(return_value={
        "selector": ".learned", "method": "readability",
    }))
    async def get_memory():
        return memory

    monkeypatch.setattr(extraction_memory, "get_extraction_memory", get_memory)
    recorded = {}

    async def record_feedback(lease_arg, job_arg, attempt_arg, fence_arg, data, **kwargs):
        recorded.update(data=data, learned=kwargs)

    monkeypatch.setattr(executor, "_record_article_extraction_feedback", record_feedback)
    context_body = callback_body(job_id, owner_id, executor_id, deadline, run_id, fingerprint)
    install_go_authority(monkeypatch, context_body["envelope"])
    context_body.update(operation="extraction_context", input={"url": article_input["url"]})
    context_result = await executor.article_ai_callback(request_for_payload(context_body))
    assert context_result["result"] == {"selector": ".learned", "method": "readability"}

    feedback = {
        "url": article_input["url"], "method": "readability", "success": True,
        "selector_used": ".fallback", "content_length": 240, "extraction_time_ms": 731,
        "quality_score": 0.9,
        "observations": [
            {"selector": ".learned", "method": "readability", "success": False},
            {"selector": ".learned", "method": "readability", "success": False},
            {"selector": ".fallback", "method": "readability", "success": True},
        ],
    }
    feedback_body = callback_body(job_id, owner_id, executor_id, deadline, run_id, fingerprint)
    feedback_body.update(operation="extraction_feedback", input=feedback)
    feedback_result = await executor.article_ai_callback(request_for_payload(feedback_body))

    assert feedback_result["result"] == {"recorded": True}
    assert recorded["data"] == feedback
    assert recorded["learned"] == {"learned_selector": ".learned", "learned_method": "readability"}
    assert not executor._ARTICLE_AI_CONTEXT_PATTERNS


@pytest.mark.asyncio
async def test_feedback_rejects_malformed_observations_and_stale_article_url(monkeypatch):
    job_id, owner_id = uuid.uuid4(), uuid.uuid4()
    executor_id = uuid.uuid4()
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    article_input = {"title": "Synthetic story", "content": "Synthetic body",
                     "url": "https://example.test/story"}
    fingerprint = executor._article_ai_input_fingerprint(article_input)
    run_id = uuid.UUID(_run_id(job_id, 1))
    monkeypatch.setattr(executor, "_article_ai_auth", lambda _request: None)
    monkeypatch.setattr(executor, "_require_executor_guard", AsyncMock())
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE", executor.OrderedDict())
    monkeypatch.setattr(executor, "_ARTICLE_AI_ACTIVE", set())
    monkeypatch.setattr(executor, "_ARTICLE_AI_CACHE_BYTES", 0)
    monkeypatch.setattr(executor, "_ARTICLE_AI_CONTEXT_PATTERNS", executor.OrderedDict())
    body = callback_body(job_id, owner_id, executor_id, deadline, run_id, fingerprint)
    install_go_authority(monkeypatch, body["envelope"])
    body.update(operation="extraction_feedback", input={
        "url": article_input["url"], "method": "readability", "success": False,
        "observations": [{"selector": ".learned", "method": "readability", "success": False}],
    })
    malformed = {**body, "input": {**body["input"], "observations": [
        {"selector": ".learned", "method": "arbitrary_method", "success": False},
    ]}}
    with pytest.raises(HTTPException) as invalid:
        await executor.article_ai_callback(request_for_payload(malformed))
    assert invalid.value.status_code == 422

    monkeypatch.setattr(executor, "AsyncSessionLocal", lambda: CallbackSession(
        "https://example.test/changed", 2, fingerprint, run_id))
    with pytest.raises(HTTPException) as stale:
        await executor.article_ai_callback(request_for_payload(body))
    assert stale.value.status_code == 409


@pytest.mark.asyncio
async def test_article_ai_callback_endpoint_rejects_wrong_token(monkeypatch):
    monkeypatch.setattr(executor, "settings", SimpleNamespace(article_ai_token=SecretStr("fixture-ai-token")))
    with pytest.raises(HTTPException) as denied:
        await executor.article_ai_callback(request_for_payload({}, authorization="Bearer wrong"))
    assert denied.value.status_code == 401
