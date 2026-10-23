"""Offline scheduler bridge transformations never include runtime secrets."""

import asyncio
import os
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from scripts.scheduler_bridge import (
    is_news_owned_occurrence, is_news_owned_schedule, legacy_item, legacy_rollback_record, legacy_run_history,
    rollback_inventory, safe_payload, utc_timestamp, write_artifact,
)


def test_legacy_schedule_import_preserves_due_time_and_identity():
    row = SimpleNamespace(
        id=42, task_name="news_digest", enabled=True, schedule_type="interval",
        timezone="Europe/Belgrade", hour=9, minute=0, weekdays=[],
        task_config={"interval_minutes": 45, "run_processing": False,
                     "send_telegram": True, "timeout_seconds": 120},
        next_run=datetime(2026, 10, 23, 17, 30, 19, 728869),
    )
    item = legacy_item(row)
    assert item["schedule_key"] == "news:42"
    assert item["calendar"]["interval_minutes"] == 45
    assert item["payload"] == {
        "run_processing": False, "send_telegram": True, "timeout_seconds": 120,
    }
    assert item["next_due_at"] == "2026-10-23T17:30:19.728869Z"


def test_bridge_refuses_unrecognized_configuration_fields():
    with pytest.raises(ValueError, match="sensitive task_config field"):
        safe_payload("backup", {"keep_days": 30, "api_token": "must-not-export"})


def test_timestamp_serialization_keeps_microseconds_and_treats_legacy_naive_as_utc():
    assert utc_timestamp(datetime(2026, 10, 23, 17, 30, 19, 7)) == "2026-10-23T17:30:19.000007Z"


def test_rollback_snapshot_maps_current_service_config_and_retains_successful_run():
    schedule = {
        "schedule_key": "news:42", "revision": 7, "enabled": True,
        "calendar": {"type": "interval", "timezone": "Europe/Belgrade", "interval_minutes": 45},
        "target_alias": "news-worker", "operation": "news_digest",
        "payload": {"run_processing": False, "send_telegram": True},
        "next_due_at": "2026-10-23T17:30:19.728869Z",
        "created_at": "2026-10-22T10:00:00.000001Z",
        "updated_at": "2026-10-23T10:00:00.000002Z",
    }
    job = {"state": "succeeded", "result_status": "success",
           "created_at": "2026-10-23T16:00:00.000001Z",
           "finished_at": "2026-10-23T16:02:00.000001Z"}
    record = legacy_rollback_record(schedule, job)
    assert record["task_name"] == "news_digest"
    assert record["task_config"] == {
        "run_processing": False, "send_telegram": True, "interval_minutes": 45,
    }
    assert record["next_run"] == schedule["next_due_at"]
    assert record["last_status"] == "success"
    assert record["last_success_at"] == job["finished_at"]


def test_rollback_inventory_ownership_requires_news_key_and_target_alias():
    news = {
        "schedule_key": "news:backup", "target_alias": "news-worker",
        "operation": "backup",
    }
    assert is_news_owned_schedule(news)
    assert not is_news_owned_schedule({**news, "target_alias": "archive-worker"})
    assert not is_news_owned_schedule({**news, "schedule_key": "archive:backup"})
    assert is_news_owned_occurrence({
        "schedule_key": "news:42", "target_alias": "news-worker",
    })
    assert is_news_owned_occurrence({
        "schedule_key": "fixture:queue-capacity", "target_alias": "news-worker",
    })
    assert not is_news_owned_occurrence({
        "schedule_key": "news:42", "target_alias": "other-worker",
    })


def _fake_inventory_session(monkeypatch, responses, calls, *, db_session=None):
    import scripts.scheduler_bridge as bridge

    class ClientSession:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    async def request_json(_session, method, url, token, body=None):
        calls.append((method, url))
        return responses(url)

    monkeypatch.setattr(bridge, "service_config", lambda _args: ("http://scheduler", "fixture-token"))
    monkeypatch.setattr(bridge.aiohttp, "ClientSession", ClientSession)
    monkeypatch.setattr(bridge, "request_json", request_json)
    if db_session is not None:
        class Engine:
            async def dispose(self):
                pass

        monkeypatch.setattr(bridge, "snapshot_database_url", lambda: "postgresql+asyncpg://fixture")
        monkeypatch.setattr(bridge, "create_async_engine", lambda _url: Engine())
        monkeypatch.setattr(bridge, "async_sessionmaker", lambda *_args, **_kwargs: lambda: db_session())
    return bridge


def test_rollback_inventory_rejects_unpaused_scheduler_before_listing(monkeypatch):
    calls = []
    import scripts.scheduler_bridge as bridge

    def response(url):
        return {"paused": False, "control_revision": 4, "pending_outbox": 0}

    bridge = _fake_inventory_session(monkeypatch, response, calls)
    with pytest.raises(ValueError, match="valid paused status"):
        asyncio.run(bridge.rollback_inventory(SimpleNamespace(timeout=1, output="unused.json")))
    assert calls == [("GET", "http://scheduler/v1/status")]


def test_rollback_inventory_succeeds_with_stable_paused_state(monkeypatch):
    calls = []
    artifacts = []
    status = {"paused": True, "control_revision": 4, "pending_outbox": 0}
    responses = iter([
        status,
        {"schedules": [], "next_cursor": None},
        {"occurrences": [], "next_cursor": None},
        status,
    ])

    def response(_url):
        return next(responses)

    class EmptyDBSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    bridge = _fake_inventory_session(
        monkeypatch, response, calls, db_session=EmptyDBSession,
    )
    monkeypatch.setattr(bridge, "write_artifact", lambda _path, artifact: artifacts.append(artifact))
    asyncio.run(bridge.rollback_inventory(SimpleNamespace(timeout=1, output="unused.json")))
    assert [url.rsplit("/", 1)[-1].split("?", 1)[0] for _, url in calls] == [
        "status", "schedules", "occurrences", "status",
    ]
    assert len(artifacts) == 1
    assert artifacts[0]["scheduler_status"] == status


def test_rollback_inventory_rejects_leased_dispatch(monkeypatch):
    calls = []
    responses = iter([
        {"paused": True, "control_revision": 4, "pending_outbox": 1},
        {"schedules": [], "next_cursor": None},
        {"occurrences": [_occurrence("news:42", status="leased", lease_active=True)],
         "next_cursor": None},
    ])

    def response(_url):
        return next(responses)

    import scripts.scheduler_bridge as bridge
    bridge = _fake_inventory_session(monkeypatch, response, calls)
    with pytest.raises(ValueError, match="in-flight leased dispatch"):
        asyncio.run(bridge.rollback_inventory(SimpleNamespace(timeout=1, output="unused.json")))
    assert [url.rsplit("/", 1)[-1].split("?", 1)[0] for _, url in calls] == [
        "status", "schedules", "occurrences",
    ]


@pytest.mark.parametrize("remove_field", ["target_alias", "lease_active", "lease_expires_at"])
def test_rollback_inventory_fails_closed_on_missing_occurrence_ownership_or_lease_metadata(
        monkeypatch, remove_field):
    calls = []
    row = _occurrence("news:77")
    row.pop(remove_field)
    responses = iter([
        {"paused": True, "control_revision": 4, "pending_outbox": 1},
        {"schedules": [], "next_cursor": None},
        {"occurrences": [row], "next_cursor": None},
    ])
    import scripts.scheduler_bridge as bridge
    bridge = _fake_inventory_session(monkeypatch, lambda _url: next(responses), calls)
    with pytest.raises(ValueError, match="occurrence inventory row has an invalid field set"):
        asyncio.run(bridge.rollback_inventory(SimpleNamespace(timeout=1, output="unused.json")))
    assert calls[-1][1].endswith("/v1/occurrences?limit=100")


def _occurrence(schedule_key, *, status="pending", alias="news-worker", lease_active=False):
    return {
        "occurrence_id": str(uuid4()), "schedule_key": schedule_key,
        "schedule_revision": 1, "scheduled_for": "2026-10-23T00:00:00.000001Z",
        "status": status, "attempts": 0,
        "created_at": "2026-10-23T00:00:00Z", "updated_at": "2026-10-23T00:00:00Z",
        "job_id": None, "target_alias": alias,
        "lease_expires_at": "2026-10-22T00:00:00Z" if status == "leased" else None,
        "lease_active": lease_active,
    }


def test_rollback_inventory_keeps_news_orphan_occurrences_and_jobs_but_excludes_foreign_alias(
        monkeypatch):
    calls, artifacts, queries = [], [], []
    status = {"paused": True, "control_revision": 4, "pending_outbox": 2}
    schedule = {
        "schedule_key": "news:news_digest", "revision": 1, "enabled": True,
        "calendar": {"type": "daily", "timezone": "UTC", "hour": 1, "minute": 2},
        "target_alias": "news-worker", "operation": "news_digest", "payload": {},
        "next_due_at": None, "created_at": "2026-10-22T00:00:00Z",
        "updated_at": "2026-10-23T00:00:00Z",
    }
    rows = [
        _occurrence("news:77", status="pending"),
        _occurrence("news:77", status="blocked"),
        _occurrence("news:77", status="accepted"),
        _occurrence("news:88", status="leased", lease_active=False),
        _occurrence("news:independent-dispatch", status="pending"),
        _occurrence("fixture:queue-capacity", status="accepted"),
        _occurrence("news:99", status="accepted", alias="other-worker"),
        _occurrence("news:77", status="accepted", alias="other-worker"),
    ]
    responses = iter([
        status,
        {"schedules": [schedule], "next_cursor": None},
        {"occurrences": rows, "next_cursor": None},
        status,
    ])

    class DBSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, statement):
            queries.append(str(statement.compile().params))
            jobs = [
                SimpleNamespace(
                    id=uuid4(), schedule_key=key, occurrence_id=uuid4(), state="succeeded",
                    result={"status": "success"}, created_at=datetime(2026, 10, 23),
                    finished_at=datetime(2026, 10, 23, 0, 1),
                ) for key in (
                    "news:news_digest", "news:77", "news:88",
                    "news:independent-dispatch", "fixture:queue-capacity",
                )
            ]
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: jobs))

    bridge = _fake_inventory_session(
        monkeypatch, lambda _url: next(responses), calls, db_session=DBSession,
    )
    monkeypatch.setattr(bridge, "write_artifact", lambda _path, artifact: artifacts.append(artifact))
    asyncio.run(bridge.rollback_inventory(SimpleNamespace(timeout=1, output="unused.json")))

    artifact = artifacts[0]
    assert artifact["orphaned_news_schedule_keys"] == [
        "fixture:queue-capacity", "news:77", "news:88", "news:independent-dispatch",
    ]
    assert {row["schedule_key"] for row in artifact["scheduler_occurrences"]} == {
        "news:77", "news:88", "news:independent-dispatch", "fixture:queue-capacity",
    }
    assert {row["status"] for row in artifact["scheduler_occurrences"]} == {
        "pending", "blocked", "accepted", "leased",
    }
    assert {job["schedule_key"] for job in artifact["news_execution_inventory"]} == {
        "news:news_digest", "news:77", "news:88",
        "news:independent-dispatch", "fixture:queue-capacity",
    }
    assert "news:99" not in repr(artifact)
    assert all(key in queries[0] for key in (
        "news:news_digest", "news:77", "news:88",
        "news:independent-dispatch", "fixture:queue-capacity",
    ))


def test_rollback_inventory_rejects_control_change_during_capture(monkeypatch):
    calls = []
    responses = iter([
        {"paused": True, "control_revision": 4, "pending_outbox": 0},
        {"schedules": [], "next_cursor": None},
        {"occurrences": [], "next_cursor": None},
        {"paused": True, "control_revision": 5, "pending_outbox": 0},
    ])

    def response(_url):
        return next(responses)

    import scripts.scheduler_bridge as bridge
    bridge = _fake_inventory_session(monkeypatch, response, calls)
    with pytest.raises(ValueError, match="state changed during rollback inventory"):
        asyncio.run(bridge.rollback_inventory(SimpleNamespace(timeout=1, output="unused.json")))
    assert calls[-1] == ("GET", "http://scheduler/v1/status")


def test_export_fails_closed_for_backup_config_not_supported_by_worker_jobs():
    with pytest.raises(ValueError, match="sensitive task_config field"):
        safe_payload("backup", {"keep_days": 14})


def test_export_preserves_legacy_success_history_without_unbounded_error_text():
    row = SimpleNamespace(
        id=9, task_name="news_digest", last_run=datetime(2026, 10, 23, 16),
        last_finished_at=datetime(2026, 10, 23, 16, 2),
        last_success_at=datetime(2026, 10, 23, 16, 2), last_status="success",
        last_duration_seconds=120.5,
    )
    history = legacy_run_history(row)
    assert history["schedule_key"] == "news:9"
    assert history["last_status"] == "success"
    assert history["last_success_at"] == "2026-10-23T16:02:00.000000Z"
    assert "last_error" not in history


def test_artifact_writer_uses_private_permissions_and_never_overwrites(tmp_path):
    path = tmp_path / "snapshot.json"
    write_artifact(path, {"ok": True})
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        write_artifact(path, {"ok": False})
    assert path.read_text(encoding="utf-8") == '{\n  "ok": true\n}\n'


def test_request_json_accumulates_a_streamed_http_response():
    import scripts.scheduler_bridge as bridge

    async def run():
        app = web.Application()

        async def handler(_request):
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(_request)
            for chunk in (b'{"schedule', b's":[],"next_', b'cursor":null}'):
                await response.write(chunk)
                await asyncio.sleep(0.01)
            await response.write_eof()
            return response

        app.router.add_get("/stream", handler)
        async with TestServer(app) as server:
            async with aiohttp.ClientSession() as session:
                return await bridge.request_json(
                    session, "GET", str(server.make_url("/stream")), "fixture-token"
                )

    assert asyncio.run(run()) == {"schedules": [], "next_cursor": None}


def test_request_json_rejects_a_streamed_response_over_the_limit():
    import scripts.scheduler_bridge as bridge

    async def run():
        app = web.Application()

        async def handler(request):
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(request)
            remaining = bridge.MAX_RESPONSE_BYTES + 1
            try:
                while remaining:
                    chunk = b"x" * min(32 * 1024, remaining)
                    await response.write(chunk)
                    remaining -= len(chunk)
                    await asyncio.sleep(0.001)
                await response.write_eof()
            except ConnectionResetError:
                # The client intentionally closes once the bounded reader has
                # consumed the first byte beyond its limit.
                pass
            return response

        app.router.add_get("/oversized", handler)
        async with TestServer(app) as server:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ValueError, match="bounded response limit"):
                    await bridge.request_json(
                        session, "GET", str(server.make_url("/oversized")), "fixture-token"
                    )

    asyncio.run(run())


def _loopback_test_url():
    value = os.environ.get("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(value).host not in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    return value


@pytest.fixture
async def scheduler_bridge_pg(monkeypatch):
    """Use a real loopback PostgreSQL DB with a per-test schema for bridge IO."""
    import scripts.scheduler_bridge as bridge
    from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
    from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
    from news_aggregator.models import BackgroundJob, ScheduleSettings

    url = _loopback_test_url()
    schema = "test_scheduler_bridge_" + uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(
        url, connect_args={"server_settings": {"search_path": schema}}
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
            await ScheduledJobMetadataMigration().execute(session)
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync: ScheduleSettings.__table__.create(sync, checkfirst=True)
            )

        create_engine = bridge.create_async_engine

        def create_scoped_engine(database_url):
            # The bridge still uses a real PostgreSQL connection; this wrapper
            # sets only the unique schema used by this test.
            return create_engine(
                database_url, connect_args={"server_settings": {"search_path": schema}}
            )

        monkeypatch.setattr(bridge, "create_async_engine", create_scoped_engine)
        monkeypatch.setattr(bridge, "snapshot_database_url", lambda: url)
        yield factory, BackgroundJob, ScheduleSettings
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_export_and_rollback_inventory_use_real_isolated_postgres(
        scheduler_bridge_pg, tmp_path, monkeypatch):
    import json
    import scripts.scheduler_bridge as bridge

    factory, BackgroundJob, ScheduleSettings = scheduler_bridge_pg
    due = datetime(2026, 10, 23, 17, 30, 19, 728869)
    async with factory() as session:
        session.add(ScheduleSettings(
            id=42, task_name="news_digest", enabled=True, schedule_type="interval",
            timezone="Europe/Belgrade", task_config={"interval_minutes": 45},
            next_run=due,
        ))
        session.add(BackgroundJob(
            kind="news_digest", payload={"send_telegram": True},
            idempotency_key="scheduler-bridge-integration", state="succeeded",
            created_at=datetime(2026, 10, 23, 16), finished_at=datetime(2026, 10, 23, 16, 2),
            schedule_key="news:42", schedule_revision=3, occurrence_id=uuid4(),
            scheduled_for=due, result={"status": "success"},
        ))
        await session.commit()

    export_path = tmp_path / "snapshot.json"
    await bridge.export_snapshot(export_path)
    snapshot = json.loads(export_path.read_text(encoding="utf-8"))
    assert export_path.stat().st_mode & 0o777 == 0o600
    assert snapshot["format"] == "news-scheduler-snapshot-v1"
    assert snapshot["schedules"][0]["schedule_key"] == "news:42"
    assert snapshot["schedules"][0]["next_due_at"] == "2026-10-23T17:30:19.728869Z"
    assert snapshot["news_job_inventory"][0]["scheduled_for"] == "2026-10-23T17:30:19.728869Z"

    schedule = {
        **snapshot["schedules"][0], "revision": 3,
        "created_at": "2026-10-22T00:00:00.000001Z",
        "updated_at": "2026-10-23T10:00:00.000002Z",
    }
    status = {"paused": True, "control_revision": 9, "pending_outbox": 0}
    responses = {
        "/v1/status": status,
        "/v1/schedules?limit=2": {"schedules": [schedule], "next_cursor": None},
        "/v1/occurrences?limit=100": {"occurrences": [], "next_cursor": None},
    }

    class FakeClientSession:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    async def request_json(_session, _method, url, _token, body=None):
        if url.endswith("/v1/status"):
            return status
        for suffix, response in responses.items():
            if url.endswith(suffix):
                return response
        raise AssertionError("unexpected scheduler API request")

    monkeypatch.setattr(bridge.aiohttp, "ClientSession", FakeClientSession)
    monkeypatch.setattr(bridge, "request_json", request_json)
    monkeypatch.setenv("SCHEDULER_API_TOKEN", "fixture-token")
    inventory_path = tmp_path / "rollback.json"
    await bridge.rollback_inventory(SimpleNamespace(
        timeout=1, output=str(inventory_path), api_url="http://scheduler",
        allow_http=True,
    ))
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    assert inventory["legacy_schedule_snapshot"]["schedules"][0]["next_run"] == (
        "2026-10-23T17:30:19.728869Z"
    )
    assert inventory["news_execution_inventory"][0]["schedule_key"] == "news:42"
    assert inventory["news_execution_inventory"][0]["state"] == "succeeded"
