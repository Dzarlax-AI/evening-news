"""Private Go article processor client protocol tests."""

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from news_aggregator.services import article_processing_client as client_module
from news_aggregator.services.article_processing_client import (
    ALLOWED_PROCESSING_STATUSES, ArticleProcessingClient, ArticleProcessingError,
    _bounded_result, _run_id,
)
from news_aggregator.services.job_fence import current_job_fence


class Content:
    def __init__(self, data):
        self.data = data

    async def iter_chunked(self, size):
        for offset in range(0, len(self.data), size):
            yield self.data[offset:offset + size]


class Response:
    def __init__(self, status, payload=None, headers=None, error=None):
        self.status = status
        self.headers = headers or {}
        self.error = error
        self.content = Content(json.dumps(payload or {}).encode())

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        return False


class Context:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        self.response = await self.response
        return await self.response.__aenter__()

    async def __aexit__(self, *args):
        return await self.response.__aexit__(*args)


class Session:
    def __init__(self, handler):
        self.handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def post(self, url, *, json=None, headers=None, allow_redirects=False):
        return Context(self.handler("POST", url, json, headers))

    def get(self, url, *, headers=None, allow_redirects=False):
        return Context(self.handler("GET", url, None, headers))


def active_fence():
    return SimpleNamespace(
        active=True, job_id=uuid.uuid4(), attempt_generation=3,
        executor_instance_id=uuid.uuid4(),
        lease=SimpleNamespace(owner_id=uuid.uuid4(), generation=7, backend_pid=2345,
                              deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2)),
    )


def receipt(active, state, result=None, *, run_id=None):
    return {
        "id": run_id or _run_id(active.job_id, active.attempt_generation),
        "job_id": str(active.job_id), "attempt_generation": active.attempt_generation,
        "state": state, "result": result or {"status": "success", "articles_processed": 2},
    }


def settings(monkeypatch):
    monkeypatch.setattr(client_module, "settings", SimpleNamespace(
        article_processing_url="http://127.0.0.1:8094",
        article_processing_token=SecretStr("synthetic-token-value"),
    ))


@pytest.mark.parametrize("status", [
    "completed", "rejected", "exhausted", "retry_wait", "settled", "incomplete",
])
def test_public_processing_outcomes_survive_bounded_receipt_mapping(status):
    mapped = _bounded_result({
        "status": status, "success": False, "settled": status in {"completed", "rejected"},
        "retryable": status == "retry_wait", "processed": 4, "improved": 3,
        "article_outcomes": [{"article_id": 8, "state": status if status in {
            "completed", "rejected", "exhausted", "retry_wait"} else "pending",
            "reason": "APIError"}],
        "private_payload": "must not escape",
    })
    assert status in ALLOWED_PROCESSING_STATUSES
    assert mapped["status"] == status
    assert mapped["success"] is False
    assert mapped["settled"] is (status in {"completed", "rejected"})
    assert mapped["retryable"] is (status == "retry_wait")
    assert mapped["processed"] == 4 and mapped["improved"] == 3
    assert mapped["article_outcomes"][0]["reason"] == "other"
    assert mapped["article_outcomes_total"] == 1
    assert mapped["article_outcomes_omitted"] == 0
    assert "private_payload" not in mapped


def test_article_outcomes_sample_reports_omitted_items():
    mapped = _bounded_result({
        "status": "incomplete",
        "article_outcomes": [
            {"article_id": article_id, "state": "retry_wait", "reason": "missing_summary"}
            for article_id in range(1, 31)
        ],
    })
    assert len(mapped["article_outcomes"]) == 20
    assert mapped["article_outcomes_total"] == 30
    assert mapped["article_outcomes_omitted"] == 10


def test_unknown_processing_status_is_not_accepted():
    assert "unknown" not in ALLOWED_PROCESSING_STATUSES


@pytest.mark.asyncio
async def test_busy_submission_retries_same_fence_and_returns_bounded_receipt(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)
    settings(monkeypatch)
    posts, gets = [], 0

    async def transport(method, url, body, headers):
        nonlocal gets
        assert headers["Authorization"] == "Bearer synthetic-token-value"
        if method == "POST":
            posts.append((url, body))
            if len(posts) == 1:
                return Response(429, headers={"Retry-After": "0.1"})
            return Response(202, receipt(active, "running"))
        gets += 1
        return Response(200, receipt(active, "succeeded", {
            "status": "success", "articles_processed": 2,
            "article_outcomes": [{"article_id": 44, "state": "completed", "reason": "ignored"}],
            "unbounded": "excluded",
        }))

    real_sleep = asyncio.sleep
    monkeypatch.setattr(client_module.asyncio, "sleep", lambda _: real_sleep(0))
    try:
        result = await ArticleProcessingClient(client_factory=lambda **_: Session(transport)).process_current_job()
    finally:
        current_job_fence.reset(token)
    assert len(posts) == 2 and posts[0] == posts[1]
    assert posts[0][0].endswith("/internal/v1/processing/runs")
    assert result == {"status": "success", "articles_processed": 2,
                      "article_outcomes": [{"article_id": 44, "state": "completed", "reason": "other"}],
                      "article_outcomes_total": 1, "article_outcomes_omitted": 0}
    assert gets == 1


@pytest.mark.asyncio
async def test_lost_submit_ack_reconciles_before_exact_retry(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)
    settings(monkeypatch)
    posts, gets = [], 0

    async def transport(method, url, body, headers):
        nonlocal gets
        if method == "POST":
            posts.append(body)
            if len(posts) == 1:
                return Response(0, error=asyncio.TimeoutError("ack dropped"))
            return Response(202, receipt(active, "running"))
        gets += 1
        if gets == 1:
            return Response(404, {"error": "not_found"})
        return Response(200, receipt(active, "succeeded"))

    real_sleep = asyncio.sleep
    monkeypatch.setattr(client_module.asyncio, "sleep", lambda _: real_sleep(0))
    try:
        await ArticleProcessingClient(client_factory=lambda **_: Session(transport)).process_current_job()
    finally:
        current_job_fence.reset(token)
    assert posts[0] == posts[1]
    assert gets == 2


@pytest.mark.parametrize("lost_ack", [False, True], ids=["poll", "lost-ack-reconcile"])
@pytest.mark.asyncio
async def test_task_cancellation_confirms_go_run_terminal_before_reraising(monkeypatch, lost_ack):
    active = active_fence()
    token = current_job_fence.set(active)
    settings(monkeypatch)
    waiting = asyncio.Event()
    cancelled_runs = []

    async def transport(method, url, body, headers):
        if method == "POST" and url.endswith("/cancel"):
            cancelled_runs.append(url)
            return Response(200, receipt(active, "cancelled"))
        if method == "POST":
            if lost_ack:
                return Response(0, error=asyncio.TimeoutError("ack dropped"))
            return Response(202, receipt(active, "running"))
        waiting.set()
        await asyncio.Future()

    task = asyncio.create_task(
        ArticleProcessingClient(client_factory=lambda **_: Session(transport)).process_current_job()
    )
    try:
        await asyncio.wait_for(waiting.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        current_job_fence.reset(token)
    assert len(cancelled_runs) == 1
    assert cancelled_runs[0].endswith(f"/processing/runs/{_run_id(active.job_id, active.attempt_generation)}/cancel")


@pytest.mark.asyncio
async def test_protocol_identity_failure_requires_confirmed_matching_terminal_cancel(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)
    settings(monkeypatch)
    calls = []

    async def transport(method, url, body, headers):
        calls.append((method, url))
        if method == "POST" and url.endswith("/cancel"):
            return Response(200, receipt(active, "cancelled", run_id=str(uuid.uuid4())))
        if method == "POST":
            return Response(202, receipt(active, "running", run_id=str(uuid.uuid4())))
        return Response(200, receipt(active, "running", run_id=str(uuid.uuid4())))

    try:
        with pytest.raises(ArticleProcessingError, match="cancellation was not confirmed"):
            await ArticleProcessingClient(client_factory=lambda **_: Session(transport)).process_current_job()
    finally:
        current_job_fence.reset(token)
    assert any(method == "POST" and url.endswith("/cancel") for method, url in calls)


@pytest.mark.asyncio
async def test_mismatched_run_identity_is_rejected(monkeypatch):
    active = active_fence()
    token = current_job_fence.set(active)
    settings(monkeypatch)

    async def transport(method, url, body, headers):
        return Response(202, receipt(active, "running", run_id=str(uuid.uuid4())))

    try:
        with pytest.raises(ArticleProcessingError, match="cancellation was not confirmed"):
            await ArticleProcessingClient(client_factory=lambda **_: Session(transport)).process_current_job()
    finally:
        current_job_fence.reset(token)


@pytest.mark.asyncio
async def test_processing_requires_an_active_executor_fence(monkeypatch):
    settings(monkeypatch)
    with pytest.raises(ArticleProcessingError, match="active executor fence"):
        await ArticleProcessingClient().process_current_job()
