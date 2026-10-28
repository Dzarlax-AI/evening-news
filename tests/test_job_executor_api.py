"""Strict input and authentication checks for the private executor API."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import uuid
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from news_aggregator.executor import _auth
from tests.frozen_execution_contract import InvalidEnvelope, parse_envelope


def _body(**overrides):
    value = {
        "version": 1,
        "job_id": str(uuid.uuid4()),
        "owner_id": str(uuid.uuid4()),
        "generation": 2,
        "attempt_generation": 4,
        "deadline_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    }
    value.update(overrides)
    return value


def test_envelope_is_strict_and_hashes_immutable_values():
    body = _body()
    parsed, digest = parse_envelope(json.dumps(body).encode())
    assert parsed["job_id"] == body["job_id"]
    assert parsed["attempt_generation"] == 4
    assert len(digest) == 64
    assert parse_envelope(json.dumps(body, sort_keys=True).encode())[1] == digest

    changed = {**body, "generation": 3}
    assert parse_envelope(json.dumps(changed).encode())[1] != digest


@pytest.mark.parametrize("raw", [
    b'{"version":1,"version":1}',
    b'{"version":1,"job_id":"x","extra":true}',
    b'{"version":1,"job_id":"x","owner_id":"x","generation":true,"attempt_generation":1,"deadline_at":"2026-10-24T10:00:00Z"}',
])
def test_envelope_rejects_duplicate_extra_and_boolean_integer_fields(raw):
    with pytest.raises(InvalidEnvelope):
        parse_envelope(raw)


def test_envelope_rejects_naive_deadline_and_oversized_body():
    with pytest.raises(InvalidEnvelope):
        parse_envelope(json.dumps(_body(deadline_at="2026-10-24T10:00:00")).encode())
    with pytest.raises(InvalidEnvelope):
        parse_envelope(b" " * 8193)


def test_auth_requires_exact_bearer_token(monkeypatch):
    from news_aggregator.executor import settings

    monkeypatch.setattr(settings, "job_executor_token", SimpleNamespace(get_secret_value=lambda: "test-token"))
    _auth(SimpleNamespace(headers={"authorization": "Bearer test-token"}))
    with pytest.raises(HTTPException) as error:
        _auth(SimpleNamespace(headers={"authorization": "bearer test-token"}))
    assert error.value.status_code == 401


def test_executor_readiness_requires_ai_credential_without_calling_provider(monkeypatch):
    from news_aggregator import executor as executor_module

    settings = executor_module.settings
    monkeypatch.setattr(settings, "job_executor_token", SimpleNamespace(get_secret_value=lambda: "fixture-token-123456"))
    monkeypatch.setattr(type(settings), "validate_runtime_services", lambda self: True)
    monkeypatch.setattr(settings, "gemini_api_key", None)
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        executor_module._validate_executor_runtime()

    monkeypatch.setattr(settings, "gemini_api_key", "fixture-gemini-key")
    assert executor_module._validate_executor_runtime() is None
    with pytest.raises(HTTPException) as error:
        _auth(SimpleNamespace(headers={"authorization": "Bearer test-tokén"}))
    assert error.value.status_code == 401


@pytest.mark.parametrize("url", [
    None,
    "",
    "ftp://background:8101",
    "http://user:secret@background:8101",
    "http://background:8101/path",
    "http://background:8101?",
    "http://background:8101#",
    "http://background:8101 bad",
    "http://background:70000",
    "http:///missing-host",
])
def test_executor_readiness_rejects_missing_or_invalid_execution_origin(monkeypatch, url):
    from news_aggregator import executor as executor_module

    settings = executor_module.settings
    monkeypatch.setattr(settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "fixture-token-123456"))
    monkeypatch.setattr(settings, "gemini_api_key", "fixture-gemini-key")
    monkeypatch.setattr(settings, "execution_service_url", url)
    monkeypatch.setattr(type(settings), "validate_runtime_services", lambda self: True)

    with pytest.raises(RuntimeError, match="EXECUTION_SERVICE_URL"):
        executor_module._validate_executor_runtime()


@pytest.mark.parametrize("url", [
    "http://background:8101",
    "https://background.internal:8101/",
])
def test_executor_readiness_accepts_http_origin_without_probing_go(monkeypatch, url):
    from news_aggregator import executor as executor_module

    settings = executor_module.settings
    runtime_validation = []
    monkeypatch.setattr(settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "fixture-token-123456"))
    monkeypatch.setattr(settings, "gemini_api_key", "fixture-gemini-key")
    monkeypatch.setattr(settings, "execution_service_url", url)
    monkeypatch.setattr(type(settings), "validate_runtime_services",
                        lambda self: runtime_validation.append("validated"))

    assert executor_module._validate_executor_runtime() is None
    assert runtime_validation == ["validated"]


@pytest.mark.asyncio
async def test_executor_shutdown_drains_tasks_before_services_and_database(monkeypatch):
    from news_aggregator import executor as executor_module
    from news_aggregator.services import domain_operations

    events = []
    started = asyncio.Event()

    async def active_work():
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            events.append("task-cleanup")
            raise

    class Engine:
        async def dispose(self):
            events.append("engine-dispose")

    task = asyncio.create_task(active_work())
    key = (uuid.uuid4(), 3)
    monkeypatch.setattr(domain_operations, "_ACTIVE", {key: task})
    monkeypatch.setattr(executor_module, "engine", Engine())
    await started.wait()

    await executor_module._shutdown_executor(drain_seconds=1)

    assert task.done()
    assert events == ["task-cleanup", "engine-dispose"]


@pytest.mark.asyncio
async def test_cancelled_runtime_start_releases_child_guard_and_engine(monkeypatch):
    from news_aggregator import executor as executor_module
    from news_aggregator.services import ai_runtime as runtime_module

    events = []
    starting = asyncio.Event()

    class Runtime:
        async def start(self, **_kwargs):
            events.append("runtime-start")
            starting.set()
            await asyncio.Event().wait()

        async def stop(self, **_kwargs):
            events.append("runtime-stop")

    class Engine:
        async def dispose(self):
            events.append("engine-dispose")

    runtime = Runtime()
    monkeypatch.setattr(executor_module, "engine", Engine())
    monkeypatch.setattr(executor_module, "_validate_executor_runtime", lambda: None)

    async def acquire_guard():
        events.append("guard-acquire")

    async def release_guard():
        events.append("guard-release")

    monkeypatch.setattr(executor_module, "_acquire_executor_guard", acquire_guard)
    monkeypatch.setattr(executor_module, "_release_executor_guard", release_guard)
    monkeypatch.setattr(runtime_module, "get_ai_runtime", lambda: runtime)

    task = asyncio.create_task(executor_module._serve())
    await starting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert events == ["guard-acquire", "runtime-start", "runtime-stop",
                      "guard-release", "engine-dispose"]


@pytest.mark.asyncio
async def test_executor_database_guard_rejects_second_instance(executor_pg, monkeypatch):
    from news_aggregator import executor as executor_module

    engine, _, _ = executor_pg
    monkeypatch.setattr(executor_module, "engine", engine)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_CONNECTION", None)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_BACKEND_PID", None)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_HEALTHY", False)
    await executor_module._acquire_executor_guard()
    try:
        assert await executor_module._executor_guard_is_held()
        with pytest.raises(RuntimeError, match="another Python job executor"):
            await executor_module._acquire_executor_guard()
    finally:
        await executor_module._release_executor_guard()


@pytest.mark.asyncio
async def test_executor_guard_connection_loss_revokes_active_work(executor_pg, monkeypatch):
    from sqlalchemy import text
    from news_aggregator import executor as executor_module
    from news_aggregator.services import domain_operations

    engine, _, _ = executor_pg
    monkeypatch.setattr(executor_module, "engine", engine)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_CONNECTION", None)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_BACKEND_PID", None)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_HEALTHY", False)
    monkeypatch.setattr(executor_module, "_EXECUTOR_SERVER", None)
    monkeypatch.setattr(domain_operations, "_ACTIVE", {})
    await executor_module._acquire_executor_guard()
    server = SimpleNamespace(should_exit=False)
    monkeypatch.setattr(executor_module, "_EXECUTOR_SERVER", server)
    task = asyncio.create_task(asyncio.sleep(60))
    key = (uuid.uuid4(), 1)
    domain_operations._ACTIVE[key] = task
    try:
        async with engine.begin() as connection:
            terminated = await connection.scalar(text("SELECT pg_terminate_backend(:pid)"),
                                                {"pid": executor_module._EXECUTOR_GUARD_BACKEND_PID})
        assert terminated
        # The authenticated request path is the first detector and has no
        # direct Uvicorn server argument; it must still stop the process.
        assert not await executor_module._check_executor_guard()
        assert server.should_exit
        assert task.cancelled()
        assert not await executor_module._check_executor_guard(server)
        assert server.should_exit
    finally:
        await executor_module._release_executor_guard()
        monkeypatch.setattr(executor_module, "_EXECUTOR_SERVER", None)
    assert domain_operations._ACTIVE == {}


@pytest.mark.asyncio
async def test_executor_guard_query_timeout_invalidates_connection(monkeypatch):
    from news_aggregator import executor as executor_module

    class HangingConnection:
        invalidated = False
        closed = False

        async def execute(self, *_args, **_kwargs):
            await asyncio.Event().wait()

        async def invalidate(self):
            self.invalidated = True

        async def close(self):
            self.closed = True

    connection = HangingConnection()
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_CONNECTION", connection)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_BACKEND_PID", 123)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_HEALTHY", True)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_QUERY_TIMEOUT", 0.01)

    assert not await executor_module._executor_guard_is_held()
    assert connection.invalidated and connection.closed
    assert executor_module._EXECUTOR_GUARD_CONNECTION is None
    assert not executor_module._EXECUTOR_GUARD_HEALTHY


@pytest.mark.asyncio
async def test_executor_guard_cancelled_acquire_discards_connection(monkeypatch):
    from news_aggregator import executor as executor_module

    class HangingConnection:
        def __init__(self):
            self.started = asyncio.Event()
            self.invalidated = False
            self.closed = False

        async def execute(self, *_args, **_kwargs):
            self.started.set()
            await asyncio.Event().wait()

        async def invalidate(self):
            self.invalidated = True

        async def close(self):
            self.closed = True

    connection = HangingConnection()
    monkeypatch.setattr(executor_module, "engine", SimpleNamespace(connect=lambda: _return(connection)))
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_CONNECTION", None)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_BACKEND_PID", None)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_HEALTHY", False)
    task = asyncio.create_task(executor_module._acquire_executor_guard())
    await connection.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert connection.invalidated and connection.closed
    assert executor_module._EXECUTOR_GUARD_CONNECTION is None


async def _return(value):
    return value


@pytest.mark.asyncio
async def test_cancelled_guard_probe_revokes_executor_and_stops_server(monkeypatch):
    from news_aggregator import executor as executor_module
    from news_aggregator.services import domain_operations

    class HangingConnection:
        def __init__(self):
            self.started = asyncio.Event()
            self.invalidated = False
            self.closed = False

        async def execute(self, *_args, **_kwargs):
            self.started.set()
            await asyncio.Event().wait()

        async def invalidate(self):
            self.invalidated = True

        async def close(self):
            self.closed = True

    connection = HangingConnection()
    server = SimpleNamespace(should_exit=False)
    task = asyncio.create_task(asyncio.sleep(60))
    key = (uuid.uuid4(), 1)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_CONNECTION", connection)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_BACKEND_PID", 321)
    monkeypatch.setattr(executor_module, "_EXECUTOR_GUARD_HEALTHY", True)
    monkeypatch.setattr(executor_module, "_EXECUTOR_SERVER", server)
    monkeypatch.setattr(domain_operations, "_ACTIVE", {key: task})

    probe = asyncio.create_task(executor_module._check_executor_guard())
    await connection.started.wait()
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe
    assert connection.invalidated and connection.closed
    assert server.should_exit
    assert task.cancelled()
