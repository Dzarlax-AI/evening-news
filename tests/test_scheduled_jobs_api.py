"""Private scheduler receiver auth, replay, and worker availability checks."""

import asyncio
import json
import os
import uuid
from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

# Some imported database modules instantiate settings during collection. Set
# this test-only value before importing any project package so collection does
# not depend on suite ordering or a developer's local credentials.
os.environ.setdefault("ADMIN_PASSWORD", "fixture-only")

from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
from news_aggregator.migrations.job_executor import JobExecutorMigration
from news_aggregator.models import (
    BackgroundJob, BackgroundWorkerStatus, ScheduleSettings, SchedulerOccurrenceReceipt,
)


def _test_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    return url


@pytest.fixture
async def scheduled_db():
    schema = "test_scheduler_ingress_" + uuid.uuid4().hex
    admin = create_async_engine(_test_url())
    engine = create_async_engine(
        _test_url(), connect_args={"server_settings": {"search_path": schema}}
    )
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
            await ScheduledJobMetadataMigration().execute(session)
            await JobExecutorMigration().execute(session)
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync: ScheduleSettings.__table__.create(sync, checkfirst=True))
        async with factory() as session:
            status = await session.get(BackgroundWorkerStatus, 1)
            status.status = "running"
            status.generation = 1
            status.owner_id = uuid.uuid4()
            status.heartbeat_at = datetime.utcnow()
            await session.commit()
        yield engine, factory
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


def _request(envelope, token="target-secret"):
    raw = json.dumps(envelope, separators=(",", ":")).encode()
    async def receive():
        return {"type": "http.request", "body": raw, "more_body": False}
    return Request({
        "type": "http", "method": "POST", "path": "/api/v1/internal/scheduled-jobs",
        "query_string": b"", "headers": [
            (b"authorization", ("Bearer " + token).encode()),
            (b"content-type", b"application/json"),
            (b"idempotency-key", envelope["occurrence_id"].encode()),
            (b"content-length", str(len(raw)).encode()),
        ],
    }, receive)


def _envelope(occurrence_id=None, **changes):
    value = {
        "version": 1,
        "occurrence_id": str(occurrence_id or uuid.uuid4()),
        "schedule_key": "news:42",
        "schedule_revision": 3,
        "scheduled_for": "2026-10-23T17:30:19.728869Z",
        "operation": "news_digest",
        "payload": {"run_processing": True, "send_telegram": False},
    }
    value.update(changes)
    return value


async def _asgi_request(app, method, path, headers=(), body=b""):
    sent = []
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    await app({
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": method, "scheme": "http",
        "path": path, "raw_path": path.encode(), "query_string": b"",
        "root_path": "", "headers": list(headers),
        "client": ("127.0.0.1", 45678), "server": ("testserver", 80),
    }, receive, send)
    start = next(message for message in sent if message["type"] == "http.response.start")
    payload = b"".join(
        message.get("body", b"") for message in sent
        if message["type"] == "http.response.body"
    )
    return start["status"], payload


class _PostgresQueue:
    def __init__(self, factory):
        self.factory = factory

    async def execute_write(self, operation):
        async with self.factory() as session:
            try:
                result = await operation(session)
                await session.commit()
                return result
            except BaseException:
                await session.rollback()
                raise


@pytest.mark.asyncio
async def test_occurrence_receipt_survives_job_retention_and_conflicts_on_full_envelope(
        scheduled_db, monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module

    _, factory = scheduled_db
    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "get_database_queue", lambda: _PostgresQueue(factory))
    envelope = _envelope()
    response = await module.receive_scheduled_job(_request(envelope))
    assert response.status_code == 202
    receipt = json.loads(response.body)
    assert receipt["status"] == "accepted"

    async with factory() as session:
        job = await session.get(BackgroundJob, uuid.UUID(receipt["job_id"]))
        assert job.schedule_key == envelope["schedule_key"]
        assert job.schedule_revision == envelope["schedule_revision"]
        assert job.occurrence_id == uuid.UUID(envelope["occurrence_id"])
        assert job.scheduled_for == datetime(2026, 10, 23, 17, 30, 19, 728869)
        await session.delete(job)
        status = await session.get(BackgroundWorkerStatus, 1)
        status.status = "stopped"
        await session.commit()

    replay = await module.receive_scheduled_job(_request(envelope))
    assert replay.status_code == 200
    assert json.loads(replay.body) == receipt

    changed = _envelope(
        occurrence_id=envelope["occurrence_id"],
        schedule_revision=envelope["schedule_revision"] + 1,
    )
    with pytest.raises(HTTPException) as caught:
        await module.receive_scheduled_job(_request(changed))
    assert caught.value.status_code == 409
    async with factory() as session:
        assert await session.scalar(select(BackgroundJob.id)) is None
        assert await session.scalar(select(SchedulerOccurrenceReceipt.occurrence_id)) is not None


@pytest.mark.asyncio
async def test_worker_down_rejects_new_occurrence_without_receipt_or_job(scheduled_db, monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module

    _, factory = scheduled_db
    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "get_database_queue", lambda: _PostgresQueue(factory))
    async with factory() as session:
        status = await session.get(BackgroundWorkerStatus, 1)
        status.status = "stopped"
        await session.commit()

    with pytest.raises(HTTPException) as caught:
        await module.receive_scheduled_job(_request(_envelope()))
    assert caught.value.status_code == 503
    async with factory() as session:
        assert await session.scalar(select(BackgroundJob.id)) is None
        assert await session.scalar(select(SchedulerOccurrenceReceipt.occurrence_id)) is None


@pytest.mark.asyncio
async def test_concurrent_identical_deliveries_create_one_durable_job(scheduled_db, monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module

    _, factory = scheduled_db
    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "get_database_queue", lambda: _PostgresQueue(factory))
    envelope = _envelope()

    first, second = await asyncio.gather(
        module.receive_scheduled_job(_request(envelope)),
        module.receive_scheduled_job(_request(envelope)),
    )
    assert sorted([first.status_code, second.status_code]) == [200, 202]
    assert json.loads(first.body) == json.loads(second.body)
    async with factory() as session:
        assert await session.scalar(select(BackgroundJob.id)) is not None
        assert await session.scalar(select(SchedulerOccurrenceReceipt.occurrence_id)) == uuid.UUID(envelope["occurrence_id"])
        assert await session.scalar(select(BackgroundJob.id).where(
            BackgroundJob.occurrence_id == uuid.UUID(envelope["occurrence_id"])
        )) is not None


@pytest.mark.asyncio
async def test_malformed_operation_type_returns_422_without_database_access(monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module

    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    called = False

    def forbidden_db():
        nonlocal called
        called = True
        raise AssertionError("malformed envelope must be rejected before database access")

    monkeypatch.setattr(module, "get_database_queue", forbidden_db)
    malformed = _envelope(operation=["news_digest"])
    with pytest.raises(HTTPException) as caught:
        await module.receive_scheduled_job(_request(malformed))
    assert caught.value.status_code == 422
    assert called is False


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_field,value", [
    ("schedule_id", 42),
    ("scheduled_for", "2026-10-23T17:30:19.728869"),
])
async def test_service_envelope_rejects_legacy_schedule_payload_identity(
        monkeypatch, legacy_field, value):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module

    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    envelope = _envelope(payload={legacy_field: value})
    with pytest.raises(HTTPException) as caught:
        await module.receive_scheduled_job(_request(envelope))
    assert caught.value.status_code == 422


@pytest.mark.asyncio
async def test_valid_token_in_python_dispatch_mode_does_not_enqueue(scheduled_db, monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module

    _, factory = scheduled_db
    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "python")
    monkeypatch.setattr(module, "get_database_queue", lambda: _PostgresQueue(factory))

    with pytest.raises(HTTPException) as caught:
        await module.receive_scheduled_job(_request(_envelope()))
    assert caught.value.status_code == 503
    async with factory() as session:
        assert await session.scalar(select(BackgroundJob.id)) is None
        assert await session.scalar(select(SchedulerOccurrenceReceipt.occurrence_id)) is None


def test_internal_receiver_uses_scoped_bearer_outside_admin_dependency(monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import create_api_router
    from news_aggregator.api import scheduled_jobs_router as module

    monkeypatch.setattr(settings, "admin_password", "fixture-admin")
    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    app = FastAPI()
    app.include_router(create_api_router(), prefix="/api/v1")
    app.include_router(module.router, prefix="/api/v1")
    calls = {"db": 0}

    async def unavailable(*args, **kwargs):
        calls["db"] += 1
        raise AssertionError("invalid auth must be rejected before database access")

    monkeypatch.setattr(module, "get_database_queue", lambda: SimpleNamespace(execute_write=unavailable))
    envelope = _envelope()
    body = json.dumps(envelope, separators=(",", ":")).encode()
    status, _ = asyncio.run(_asgi_request(app, "POST", "/api/v1/internal/scheduled-jobs", [
        (b"authorization", b"Bearer wrong"), (b"content-type", b"application/json"),
        (b"idempotency-key", envelope["occurrence_id"].encode()),
        (b"content-length", str(len(body)).encode()),
    ], body))
    assert status == 401
    status, _ = asyncio.run(_asgi_request(app, "GET", "/api/v1/schedule/settings"))
    assert status == 401
    assert calls["db"] == 0

    class FakeSession:
        async def get(self, *_args, **_kwargs):
            return None

        async def scalar(self, *_args, **_kwargs):
            return 0

        def add(self, _row):
            pass

        async def flush(self):
            pass

    async def execute_write(operation):
        calls["db"] += 1
        return await operation(FakeSession())

    async def lock(_session):
        pass

    async def available(_session):
        return True

    async def enqueue(*_args, **_kwargs):
        return SimpleNamespace(id=uuid.uuid4()), True

    monkeypatch.setattr(module, "get_database_queue", lambda: SimpleNamespace(execute_write=execute_write))
    monkeypatch.setattr(module, "lock_job_producers", lock)
    monkeypatch.setattr(module, "is_worker_available", available)
    monkeypatch.setattr(module, "enqueue_job_in_session", enqueue)
    status, response = asyncio.run(_asgi_request(app, "POST", "/api/v1/internal/scheduled-jobs", [
        (b"authorization", b"Bearer target-secret"), (b"content-type", b"application/json"),
        (b"idempotency-key", envelope["occurrence_id"].encode()),
        (b"content-length", str(len(body)).encode()),
    ], body))
    assert status == 202
    assert json.loads(response)["status"] == "accepted"
    assert calls["db"] == 1


@pytest.mark.asyncio
async def test_non_ascii_bearer_token_is_rejected_before_body_or_database(monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.api import scheduled_jobs_router as module
    from starlette.requests import Request

    monkeypatch.setattr(settings, "scheduled_jobs_token", SecretStr("target-secret"))
    reads = {"body": 0, "database": 0}

    async def receive():
        reads["body"] += 1
        return {"type": "http.request", "body": b"{}", "more_body": False}

    request = Request({
        "type": "http", "method": "POST", "path": "/api/v1/internal/scheduled-jobs",
        "query_string": b"", "headers": [
            (b"authorization", "Bearer café".encode("latin-1")),
            (b"content-type", b"application/json"),
        ],
    }, receive)

    def forbidden_db():
        reads["database"] += 1
        raise AssertionError("unauthorized request must not access the database")

    monkeypatch.setattr(module, "get_database_queue", forbidden_db)
    with pytest.raises(HTTPException) as caught:
        await module.receive_scheduled_job(request)
    assert caught.value.status_code == 401
    assert reads == {"body": 0, "database": 0}
