"""The legacy News schedule API acts as a thin Go-service facade."""

import asyncio
import base64
from importlib import import_module
import os
import uuid
from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Security validates configuration while the API module is imported. Import it
# under a temporary test-only credential so collection is independent of a
# developer's .env or CI variables, then restore the caller's environment.
_previous_admin_password = os.environ.get("ADMIN_PASSWORD")
os.environ["ADMIN_PASSWORD"] = "scheduler-facade-test-only"
_settings_previous_admin_password = None
_settings_admin_password_was_patched = False
try:
    from news_aggregator.config import settings

    if not (settings.admin_password or settings.jwt_secret):
        _settings_previous_admin_password = settings.admin_password
        settings.admin_password = "scheduler-facade-test-only"
        _settings_admin_password_was_patched = True
    try:
        module = import_module("news_aggregator.api.scheduler_router")
    finally:
        if _settings_admin_password_was_patched:
            settings.admin_password = _settings_previous_admin_password
finally:
    if _previous_admin_password is None:
        os.environ.pop("ADMIN_PASSWORD", None)
    else:
        os.environ["ADMIN_PASSWORD"] = _previous_admin_password
assert os.environ.get("ADMIN_PASSWORD") == _previous_admin_password


async def _asgi_get(app, path, authorization):
    messages = []
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await app({
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": "GET", "scheme": "http",
        "path": path, "raw_path": path.encode(), "query_string": b"",
        "root_path": "", "headers": [(b"authorization", authorization.encode())],
        "client": ("127.0.0.1", 45678), "server": ("testserver", 80),
    }, receive, send)
    start = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages
                     if message["type"] == "http.response.body")
    return start["status"], body


def test_service_job_completion_never_writes_legacy_schedule_row():
    from news_aggregator.services.background_execution import DurableBackgroundWorker

    worker = DurableBackgroundWorker()

    class SessionThatMustNotBeRead:
        async def get(self, *_args, **_kwargs):
            raise AssertionError("service-owned schedule must not be updated locally")

    job = SimpleNamespace(schedule_key="news:42", payload={"schedule_id": 42})
    asyncio.run(worker._finish_schedule_in_session(SessionThatMustNotBeRead(), job, "succeeded", {"status": "success"}))


def test_service_job_alert_key_uses_stable_schedule_identity():
    from news_aggregator.services.background_execution import DurableBackgroundWorker

    class Alerts:
        def __init__(self):
            self.calls = []

        async def failure(self, key, *_args):
            self.calls.append(("failure", key))

        async def recovery(self, key, *_args):
            self.calls.append(("recovery", key))

    alerts = Alerts()
    worker = DurableBackgroundWorker()
    worker.alerts = alerts
    job = SimpleNamespace(schedule_key="news:42", kind="news_digest", payload={})
    asyncio.run(worker._notify_schedule(job, "failed", None))
    assert alerts.calls == [("failure", "scheduler-warning-news:42")]


class FakeQueue:
    async def execute_read(self, operation):
        return {"latest": {}, "running": set(), "last_success": {}}


class FakeScheduler:
    def __init__(self):
        self.resources = []
        self.put_calls = []
        self.deleted = []
        self.list_calls = 0

    async def list_schedules(self):
        self.list_calls += 1
        return list(self.resources)

    async def put_schedule(self, key, body):
        self.put_calls.append((key, body))
        old = next((row for row in self.resources if row["schedule_key"] == key), None)
        revision = 1 if old is None else old["revision"] + 1
        resource = {
            "schedule_key": key, "revision": revision,
            "enabled": body["enabled"], "calendar": body["calendar"],
            "target_alias": body["target_alias"], "operation": body["operation"],
            "payload": body["payload"],
            "next_due_at": "2026-10-23T17:30:19.728869Z" if body["enabled"] else None,
            "created_at": "2026-10-23T10:00:00.000001Z",
            "updated_at": "2026-10-23T10:00:00.000002Z",
        }
        if old is not None:
            self.resources.remove(old)
        self.resources.append(resource)
        return resource

    async def delete_schedule(self, key, expected_revision):
        self.deleted.append((key, expected_revision))
        self.resources = [row for row in self.resources if row["schedule_key"] != key]


def test_settings_get_route_works_in_python_and_service_modes(monkeypatch):
    from news_aggregator.api import create_api_router
    from news_aggregator.database import get_db

    monkeypatch.setattr(settings, "admin_username", "fixture-admin")
    monkeypatch.setattr(settings, "admin_password", "fixture-only")
    auth = "Basic " + base64.b64encode(b"fixture-admin:fixture-only").decode()

    class EmptyDB:
        async def execute(self, _statement):
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))

    async def db_dependency():
        yield EmptyDB()

    async def run():
        app = FastAPI()
        app.include_router(create_api_router(), prefix="/api/v1")
        app.dependency_overrides[get_db] = db_dependency

        monkeypatch.setattr(settings, "schedule_dispatch_mode", "python")
        status, body = await _asgi_get(app, "/api/v1/schedule/settings", auth)
        assert status == 200
        assert __import__("json").loads(body)["settings"] == []

        monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
        monkeypatch.setattr(module, "_scheduler_client", lambda: FakeScheduler())
        status, body = await _asgi_get(app, "/api/v1/schedule/settings", auth)
        assert status == 200
        assert __import__("json").loads(body)["settings"] == []

    asyncio.run(run())


def test_service_facade_reports_missing_client_configuration_as_503(monkeypatch):
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(settings, "scheduler_api_url", None)
    monkeypatch.setattr(settings, "scheduler_api_token", None)

    async def run():
        with pytest.raises(HTTPException) as caught:
            await module.get_schedule_settings(None)
        assert caught.value.status_code == 503
        assert caught.value.detail == "Scheduler service is not configured"

    asyncio.run(run())


@pytest.mark.parametrize("timestamp_field", ["next_due_at", "created_at", "updated_at"])
def test_service_facade_maps_malformed_service_timestamps_to_502(monkeypatch, timestamp_field):
    from news_aggregator.services.scheduler_service_client import SchedulerServiceClient

    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    resource = {
        "schedule_key": "news:news_digest", "revision": 1, "enabled": True,
        "calendar": {"type": "daily", "timezone": "UTC", "hour": 18, "minute": 30},
        "target_alias": "news-worker", "operation": "news_digest", "payload": {},
        "next_due_at": None, "created_at": "2026-10-23T10:00:00Z",
        "updated_at": "2026-10-23T10:00:00Z",
    }
    resource[timestamp_field] = "not-an-utc-timestamp"
    client = SchedulerServiceClient("https://scheduler.example", "fixture-token")

    async def response(_method, _path):
        return {"schedules": [resource], "next_cursor": None}

    monkeypatch.setattr(client, "request", response)
    monkeypatch.setattr(module, "_scheduler_client", lambda: client)

    async def run():
        with pytest.raises(HTTPException) as caught:
            await module.get_schedule_settings(None)
        assert caught.value.status_code == 502
        assert caught.value.detail == "Scheduler service returned an invalid response"

    asyncio.run(run())


def test_service_status_reports_older_running_job_when_newer_job_is_queued(monkeypatch):
    from news_aggregator.api import create_api_router
    from news_aggregator.database import get_db
    from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
    from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
    from news_aggregator.models import BackgroundJob, ScheduleSettings
    from sqlalchemy.engine import make_url

    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(database_url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("This test requires a loopback-only PostgreSQL test DB")

    schema = "test_scheduler_status_" + uuid.uuid4().hex
    admin = create_async_engine(database_url)
    engine = create_async_engine(database_url, connect_args={"server_settings": {"search_path": schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    class Queue:
        async def execute_read(self, operation):
            async with factory() as session:
                return await operation(session)

    class Service:
        async def list_schedules(self):
            return [{
                "schedule_key": "news:news_digest", "revision": 4,
                "enabled": True, "calendar": {"type": "daily", "timezone": "UTC", "hour": 18, "minute": 30},
                "target_alias": "news-worker", "operation": "news_digest",
                "payload": {"send_telegram": True}, "next_due_at": None,
                "created_at": "2026-10-23T10:00:00Z", "updated_at": "2026-10-23T10:00:00Z",
            }]

        async def get_status(self):
            return {"paused": False, "pending_outbox": 0, "control_revision": 1}

    monkeypatch.setattr(settings, "admin_username", "fixture-admin")
    monkeypatch.setattr(settings, "admin_password", "fixture-only")
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "get_database_queue", lambda: Queue())
    monkeypatch.setattr(module, "_scheduler_client", lambda: Service())
    auth = "Basic " + base64.b64encode(b"fixture-admin:fixture-only").decode()

    async def db_dependency():
        yield None

    async def run():
        try:
            async with admin.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            async with factory() as session:
                await BackgroundJobsMigration().execute(session)
                await ScheduledJobMetadataMigration().execute(session)
            async with engine.begin() as conn:
                await conn.run_sync(lambda sync: ScheduleSettings.__table__.create(sync, checkfirst=True))

            async with factory() as session:
                session.add_all([
                    BackgroundJob(
                        kind="news_digest", payload={}, idempotency_key="status:success",
                        state="succeeded", created_at=datetime(2026, 10, 23, 8),
                        finished_at=datetime(2026, 10, 23, 8, 1), schedule_key="news:news_digest",
                        result={"status": "success"},
                    ),
                    BackgroundJob(
                        kind="news_digest", payload={}, idempotency_key="status:running",
                        state="running", created_at=datetime(2026, 10, 23, 9),
                        started_at=datetime(2026, 10, 23, 9), schedule_key="news:news_digest",
                    ),
                    BackgroundJob(
                        kind="news_digest", payload={}, idempotency_key="status:queued",
                        state="queued", created_at=datetime(2026, 10, 23, 10),
                        schedule_key="news:news_digest",
                    ),
                ])
                await session.commit()

            app = FastAPI()
            app.include_router(create_api_router(), prefix="/api/v1")
            app.dependency_overrides[get_db] = db_dependency
            status, body = await _asgi_get(app, "/api/v1/schedule/status", auth)
            assert status == 200
            response = __import__("json").loads(body)
            assert response["running_tasks"] == 1
            task = response["tasks"][0]
            assert task["is_running"] is True
            assert task["last_status"] == "queued"
            assert task["last_run"] == "2026-10-23T10:00:00"
            assert task["last_success_at"] == "2026-10-23T08:01:00"
        finally:
            await engine.dispose()
            async with admin.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await admin.dispose()

    asyncio.run(run())


def test_legacy_admin_crud_facade_uses_service_config_without_schedule_settings_read(monkeypatch):
    from scripts.scheduler_bridge import legacy_rollback_record

    client = FakeScheduler()
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "_scheduler_client", lambda: client)
    monkeypatch.setattr(module, "get_database_queue", lambda: FakeQueue())

    async def run():
        created = await module.create_schedule_setting(
            module.ScheduleCreateRequest(
                task_name="news_digest", schedule_type="daily", hour=18, minute=30,
                weekdays=[1, 2, 3, 4, 5], timezone="Europe/Belgrade",
                task_config={"run_processing": True, "send_telegram": False},
            ), None,
        )
        assert created["success"] is True
        assert created["setting"]["schedule_key"] == "news:news_digest"
        assert created["setting"]["last_status"] is None
        assert client.put_calls[0][1]["expected_revision"] == 0
        assert client.put_calls[0][1]["target_alias"] == "news-worker"

        # A config created through the legacy admin facade must remain
        # representable in the offline rollback snapshot. Named stable keys
        # must not be dropped or rejected as import-only numeric IDs.
        rollback = legacy_rollback_record(client.resources[0])
        assert rollback["id"] == "news:news_digest"
        assert rollback["task_name"] == "news_digest"
        assert rollback["task_config"] == {"run_processing": True, "send_telegram": False}
        assert rollback["next_run"] == "2026-10-23T17:30:19.728869Z"

        updated = await module.update_schedule_setting(
            "news_digest",
            module.ScheduleUpdateRequest(enabled=False, task_config={"send_telegram": True}),
            None,
        )
        assert updated["setting"]["schedule_revision"] == 2
        assert updated["setting"]["next_run"] is None
        assert client.put_calls[1][1]["expected_revision"] == 1
        assert client.put_calls[1][1]["payload"] == {"send_telegram": True}

        removed = await module.delete_schedule_setting("news_digest", None)
        assert removed["success"] is True
        assert client.deleted == [("news:news_digest", 2)]
        with pytest.raises(HTTPException) as caught:
            await module.create_schedule_setting(
                module.ScheduleCreateRequest(task_name="backup", task_config={"keep_days": 14}),
                None,
            )
        assert caught.value.status_code == 422
        assert len(client.put_calls) == 2

    asyncio.run(run())


def test_service_create_success_does_not_depend_on_post_create_status_read(monkeypatch):
    client = FakeScheduler()
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "_scheduler_client", lambda: client)

    async def status_read_would_fail(*_args, **_kwargs):
        raise HTTPException(status_code=503, detail="News status store unavailable")

    monkeypatch.setattr(module, "_service_settings_snapshot", status_read_would_fail)

    async def run():
        result = await module.create_schedule_setting(
            module.ScheduleCreateRequest(
                task_name="news_digest", task_config={"run_processing": True},
            ), None,
        )
        assert result["success"] is True
        assert result["setting"]["schedule_key"] == "news:news_digest"
        assert len(client.put_calls) == 1

    asyncio.run(run())


def test_service_update_success_does_not_depend_on_post_update_status_read(monkeypatch):
    client = FakeScheduler()
    asyncio.run(client.put_schedule("news:news_digest", {
        "expected_revision": 0,
        "enabled": True,
        "calendar": {"type": "daily", "timezone": "Europe/Belgrade", "hour": 18, "minute": 30},
        "target_alias": "news-worker",
        "operation": "news_digest",
        "payload": {"send_telegram": True},
    }))
    client.put_calls.clear()
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "_scheduler_client", lambda: client)

    class StatusStoreUnavailable:
        def __init__(self):
            self.calls = 0

        async def execute_read(self, _operation):
            self.calls += 1
            raise RuntimeError("fixture status-store failure")

    status_store = StatusStoreUnavailable()
    monkeypatch.setattr(module, "get_database_queue", lambda: status_store)

    async def run():
        result = await module.update_schedule_setting(
            "news_digest",
            module.ScheduleUpdateRequest(enabled=False, task_config={"send_telegram": False}),
            None,
        )
        assert result["success"] is True
        assert result["setting"]["schedule_revision"] == 2
        assert result["setting"]["enabled"] is False
        assert result["setting"]["next_run"] is None
        assert client.put_calls == [("news:news_digest", {
            "expected_revision": 1,
            "enabled": False,
            "calendar": {"type": "daily", "timezone": "Europe/Belgrade", "hour": 18, "minute": 30},
            "target_alias": "news-worker",
            "operation": "news_digest",
            "payload": {"send_telegram": False},
        })]
        assert client.list_calls == 1  # Pre-update lookup only; no post-PUT listing.
        assert status_store.calls == 0  # The remote PUT response is the update result.
        assert client.resources[0]["revision"] == 2  # PUT committed despite the unavailable status store.

    asyncio.run(run())


def test_same_operation_for_another_consumer_is_hidden_and_not_mutated(monkeypatch):
    client = FakeScheduler()
    foreign = {
        "schedule_key": "other:backup", "revision": 7,
        "enabled": True, "calendar": {"type": "daily", "timezone": "UTC", "hour": 2, "minute": 0},
        "target_alias": "archive-worker", "operation": "backup", "payload": {},
        "next_due_at": "2026-10-24T02:00:00Z",
        "created_at": "2026-10-23T10:00:00Z", "updated_at": "2026-10-23T10:00:00Z",
    }
    client.resources.append(foreign.copy())
    monkeypatch.setattr(settings, "schedule_dispatch_mode", "service")
    monkeypatch.setattr(module, "_scheduler_client", lambda: client)
    monkeypatch.setattr(module, "get_database_queue", lambda: FakeQueue())

    async def run():
        created = await module.create_schedule_setting(
            module.ScheduleCreateRequest(task_name="backup", task_config={}), None,
        )
        assert created["setting"]["schedule_key"] == "news:backup"
        assert created["setting"]["task_name"] == "backup"
        assert client.resources[0] == foreign
        assert client.put_calls[0][0] == "news:backup"

        updated = await module.update_schedule_setting(
            "backup", module.ScheduleUpdateRequest(enabled=False), None,
        )
        assert updated["setting"]["schedule_key"] == "news:backup"
        assert client.put_calls[-1][0] == "news:backup"
        assert client.resources[0] == foreign

    asyncio.run(run())
