"""Receipt idempotency and real PostgreSQL lease-fence checks."""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import uuid
from types import SimpleNamespace
import asyncio
import json

import pytest
from fastapi import HTTPException
from sqlalchemy import text, update

import news_aggregator.executor as executor_module
from news_aggregator.executor import PythonJobExecutor, RemoteJobFence, _parse_envelope
from news_aggregator.models import (
    BackgroundJob, BackgroundJobExecution, BackgroundWorkerStatus, ScheduleSettings,
)
from news_aggregator.services.background_jobs import StaleWorker


async def _leased_job(factory, engine, *, job_id=None):
    job_id = job_id or uuid.uuid4()
    owner_id = uuid.uuid4()
    lease_conn = await engine.connect()
    acquired = await lease_conn.scalar(text(
        "SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"
    ))
    assert acquired
    backend_pid = await lease_conn.scalar(text("SELECT pg_backend_pid()"))
    async with factory() as session:
        await session.execute(text("""
            UPDATE background_worker_status SET owner_id=:owner, generation=4,
                lock_backend_pid=:pid, heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC', status='running'
            WHERE id=1
        """), {"owner": owner_id, "pid": backend_pid})
        session.add(BackgroundJob(
            id=job_id, kind="news_processing", payload={}, idempotency_key=f"executor:{job_id}",
            state="running", owner_generation=4, attempt_generation=2,
        ))
        await session.commit()
    return lease_conn, job_id, owner_id, backend_pid


@pytest.fixture(autouse=True)
def direct_endpoint_calls_assume_owned_process(monkeypatch):
    async def require_guard():
        return None

    monkeypatch.setattr(executor_module, "_require_executor_guard", require_guard)


@pytest.mark.asyncio
async def test_remote_fence_requires_actual_advisory_lease_backend(executor_pg):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, backend_pid = await _leased_job(factory, engine)
    try:
        fence = RemoteJobFence(job_id=job_id, owner_id=owner_id, generation=4,
                               attempt_generation=2, backend_pid=backend_pid)
        async with factory() as session:
            async with fence.transaction_fence(session, job_id, 2):
                await session.execute(text("SELECT 1"))
                await session.commit()

        async with factory() as session:
            await session.execute(text("UPDATE background_worker_status SET lock_backend_pid=:pid WHERE id=1"),
                                  {"pid": backend_pid + 1})
            await session.commit()
        async with factory() as session:
            with pytest.raises(StaleWorker):
                async with fence.transaction_fence(session, job_id, 2):
                    pass
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_finalized_event_creates_idempotent_attempt_zero_legacy_receipt(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn = await engine.connect()
    assert await lease_conn.scalar(text(
        "SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"
    ))
    backend_pid = await lease_conn.scalar(text("SELECT pg_backend_pid()"))
    owner_id, job_id = uuid.uuid4(), uuid.uuid4()
    async with factory() as session:
        await session.execute(text("""
            UPDATE background_worker_status SET owner_id=:owner, generation=4,
                lock_backend_pid=:pid, heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC', status='running'
            WHERE id=1
        """), {"owner": owner_id, "pid": backend_pid})
        session.add(BackgroundJob(
            id=job_id, kind="news_processing", payload={}, idempotency_key=f"legacy-zero:{job_id}",
            state="delivery_unknown", owner_generation=4, attempt_generation=0,
            error_code="external_outcome_unknown", result={"status": "warning"},
        ))
        await session.commit()
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    worker = PythonJobExecutor()
    try:
        first = await worker.apply_legacy_completion(job_id, 0)
        second = await worker.apply_legacy_completion(job_id, 0)
        assert first["legacy_completion_applied"] is True
        assert second["legacy_completion_applied"] is True
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, 0))
            assert receipt.state == "delivery_unknown"
            assert receipt.legacy_completion_applied_at is not None
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_acceptance_receipt_is_idempotent_and_keeps_original_deadline(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    executor = PythonJobExecutor()
    envelope = {
        "version": 1, "job_id": str(job_id), "owner_id": str(owner_id),
        "generation": 4, "attempt_generation": 2,
        "deadline_at": (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat(),
    }
    parsed, digest = _parse_envelope(__import__("json").dumps(envelope).encode())
    try:
        first, created = await executor.accept(parsed, digest)
        assert created
        replay, created = await executor.accept(parsed, digest)
        assert not created
        assert replay["deadline_at"] is not None
        assert replay["owner_id"] == str(owner_id)
        assert replay["generation"] == 4
        changed, changed_digest = _parse_envelope(
            __import__("json").dumps({**envelope, "generation": 5}).encode()
        )
        with pytest.raises(HTTPException) as error:
            await executor.accept(changed, changed_digest)
        assert error.value.status_code == 409
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_two_executor_instances_concurrently_accept_one_receipt(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module, "_ACTIVE", {})
    envelope = {
        "version": 1, "job_id": str(job_id), "owner_id": str(owner_id),
        "generation": 4, "attempt_generation": 2,
        "deadline_at": (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat(),
    }
    parsed, digest = _parse_envelope(json.dumps(envelope).encode())
    first_executor, second_executor = PythonJobExecutor(), PythonJobExecutor()
    try:
        outcomes = await asyncio.gather(
            first_executor.accept(parsed, digest),
            second_executor.accept(parsed, digest),
        )
        assert sorted(created for _, created in outcomes) == [False, True]
        assert all(response["job_id"] == str(job_id) for response, _ in outcomes)
        async with factory() as session:
            rows = (await session.execute(text(
                "SELECT count(*) FROM background_job_executions WHERE job_id=:job_id AND attempt_generation=2"
            ), {"job_id": job_id})).scalar_one()
            assert rows == 1
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,payload", [
    ("news_processing", {}),
    ("news_digest", {}),
    ("telegram_digest", {}),
    ("backup", {}),
    ("reprocess_failed", {}),
    ("article_reprocess", {"article_ids": [1], "force": True, "reextract_media": False}),
    ("summaries_generate", {}),
    ("source_test", {"source_id": 5}),
])
async def test_executor_runs_all_job_kinds_with_fenced_terminal_receipt(
        executor_pg, monkeypatch, kind, payload):
    engine, factory, schema = executor_pg
    lease_conn = await engine.connect()
    assert await lease_conn.scalar(text(
        "SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"
    ))
    backend_pid = await lease_conn.scalar(text("SELECT pg_backend_pid()"))
    owner_id, job_id = uuid.uuid4(), uuid.uuid4()
    async with factory() as session:
        await session.execute(text("""
            UPDATE background_worker_status SET owner_id=:owner, generation=4,
                lock_backend_pid=:pid, heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC', status='running'
            WHERE id=1
        """), {"owner": owner_id, "pid": backend_pid})
        job = BackgroundJob(
            id=job_id, kind=kind, payload=payload, idempotency_key=f"executor-run:{job_id}",
            state="running", owner_generation=4, attempt_generation=2,
        )
        session.add(job)
        session.add(BackgroundJobExecution(
            job_id=job_id, attempt_generation=2, envelope_sha256="a" * 64,
            executor_instance_id=uuid.uuid4(), owner_id=owner_id, worker_generation=4,
            deadline_at=(datetime.now(timezone.utc) + timedelta(minutes=3)).replace(tzinfo=None),
            state="accepted",
        ))
        await session.commit()

    if kind == "article_reprocess":
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE articles (
                    id INTEGER PRIMARY KEY, processing_state VARCHAR(20), processing_attempts INTEGER,
                    next_attempt_at TIMESTAMP, last_processing_error TEXT, processed BOOLEAN,
                    summary_processed BOOLEAN, category_processed BOOLEAN, ad_processed BOOLEAN
                )
            """))
            await conn.execute(text("""
                INSERT INTO articles VALUES (1, 'failed', 3, CURRENT_TIMESTAMP, 'old', true, true, true, true)
            """))

    async def run_success(job, orchestrator, reservation):
        return {"success": True}

    async def start():
        return None

    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module, "execute_job", run_success)
    import news_aggregator.orchestrator as orchestrator_module
    monkeypatch.setattr(orchestrator_module, "reserve_processing_cycle", lambda: object())
    monkeypatch.setattr(orchestrator_module, "release_processing_cycle", lambda _: None)
    from news_aggregator.services.background_execution import DurableBackgroundWorker
    async def timeout(_self, _job):
        return 60
    monkeypatch.setattr(DurableBackgroundWorker, "_job_timeout_seconds", timeout)
    worker = PythonJobExecutor()
    worker.orchestrator = SimpleNamespace(start=start)
    try:
        await worker._run({
            "job_id": str(job_id), "attempt_generation": 2, "owner_id": str(owner_id),
            "generation": 4, "backend_pid": backend_pid,
            "deadline_at": (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat(),
            "kind": kind, "payload": payload,
        })
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, 2))
            stored_job = await session.get(BackgroundJob, job_id)
            assert receipt.state == "succeeded"
            assert isinstance(receipt.result.get("duration_seconds"), (int, float))
            assert 0 <= receipt.result["duration_seconds"] <= 86400
            if kind == "article_reprocess":
                assert stored_job.initialized_at is not None
                article = (await session.execute(text("SELECT processing_state, processed FROM articles WHERE id=1"))).one()
                assert article == ("pending", False)
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_swallowed_deadline_cancellation_denies_late_commit_and_finishes_receipt(
        executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, backend_pid = await _leased_job(factory, engine)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=0.12)
    async with factory() as session:
        session.add(BackgroundJobExecution(
            job_id=job_id, attempt_generation=2, envelope_sha256="b" * 64,
            executor_instance_id=uuid.uuid4(), owner_id=owner_id, worker_generation=4,
            deadline_at=deadline.replace(tzinfo=None), state="accepted",
        ))
        await session.commit()

    async def swallow_timeout_then_write(job, _orchestrator, _reservation):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            await asyncio.sleep(0.05)
            async with factory() as session:
                row = await session.get(BackgroundJob, job.id)
                row.result = {"late_write": True}
                await session.commit()
            return {"success": True}

    async def start():
        return None

    async def timeout(_self, _job):
        return 0.12

    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module, "execute_job", swallow_timeout_then_write)
    import news_aggregator.orchestrator as orchestrator_module
    monkeypatch.setattr(orchestrator_module, "reserve_processing_cycle", lambda: object())
    monkeypatch.setattr(orchestrator_module, "release_processing_cycle", lambda _: None)
    from news_aggregator.services.background_execution import DurableBackgroundWorker
    monkeypatch.setattr(DurableBackgroundWorker, "_job_timeout_seconds", timeout)
    worker = PythonJobExecutor()
    worker.orchestrator = SimpleNamespace(start=start)
    try:
        await worker._run({
            "job_id": str(job_id), "attempt_generation": 2, "owner_id": str(owner_id),
            "generation": 4, "backend_pid": backend_pid,
            "deadline_at": deadline.isoformat(), "kind": "news_processing", "payload": {},
        })
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, 2))
            stored_job = await session.get(BackgroundJob, job_id)
            assert receipt.state == "failed"
            assert receipt.error_code == "worker_error"
            assert isinstance(receipt.result.get("duration_seconds"), (int, float))
            assert 0 <= receipt.result["duration_seconds"] <= 86400
            assert stored_job.result is None
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_cache_publication_io_failure_does_not_fail_news_job(executor_pg, monkeypatch, tmp_path):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, backend_pid = await _leased_job(factory, engine)
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    async with factory() as session:
        session.add(BackgroundJobExecution(
            job_id=job_id, attempt_generation=2, envelope_sha256="d" * 64,
            executor_instance_id=uuid.uuid4(), owner_id=owner_id, worker_generation=4,
            deadline_at=deadline.replace(tzinfo=None), state="accepted",
        ))
        await session.commit()

    from news_aggregator.services.domain_stability_tracker import DomainStabilityTracker
    from news_aggregator.services.job_fence import current_job_fence
    tracker = DomainStabilityTracker(str(tmp_path / "cache.json"))
    target = tmp_path / "cache.json"
    target.mkdir()
    async def success_with_pending_cache(_job, _orchestrator, _reservation):
        fence = current_job_fence.get()
        assert fence is not None and fence.active
        fence.pending_state_publish = True
        return {"success": True}

    async def start():
        return None

    async def timeout(_self, _job):
        return 30

    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    import news_aggregator.database as database_module
    monkeypatch.setattr(database_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module, "execute_job", success_with_pending_cache)
    import news_aggregator.orchestrator as orchestrator_module
    monkeypatch.setattr(orchestrator_module, "reserve_processing_cycle", lambda: object())
    monkeypatch.setattr(orchestrator_module, "release_processing_cycle", lambda _: None)
    from news_aggregator.services.background_execution import DurableBackgroundWorker
    monkeypatch.setattr(DurableBackgroundWorker, "_job_timeout_seconds", timeout)
    import news_aggregator.services.domain_stability_tracker as tracker_module
    async def get_tracker():
        return tracker
    monkeypatch.setattr(tracker_module, "get_stability_tracker", get_tracker)
    replace_calls = []
    real_replace = os.replace
    def track_replace(source, destination):
        replace_calls.append((source, destination))
        return real_replace(source, destination)
    monkeypatch.setattr(os, "replace", track_replace)
    worker = PythonJobExecutor()
    worker.orchestrator = SimpleNamespace(start=start)
    try:
        await worker._run({
            "job_id": str(job_id), "attempt_generation": 2, "owner_id": str(owner_id),
            "generation": 4, "backend_pid": backend_pid,
            "deadline_at": deadline.isoformat(), "kind": "news_processing", "payload": {},
        })
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, 2))
            assert receipt.state == "succeeded"
        assert len(replace_calls) == 1
        assert Path(replace_calls[0][0]).name.endswith(".working")
        assert Path(replace_calls[0][1]) == target
        assert target.is_dir()
        assert not list(tmp_path.glob(".*.working"))
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_remote_fence_rejects_preloaded_attempt_after_takeover(executor_pg):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, backend_pid = await _leased_job(factory, engine)
    try:
        fence = RemoteJobFence(job_id=job_id, owner_id=owner_id, generation=4,
                               attempt_generation=2, backend_pid=backend_pid)
        async with factory() as stale_session:
            loaded = await stale_session.get(BackgroundJob, job_id)
            async with factory() as takeover:
                await takeover.execute(update(BackgroundJob).where(BackgroundJob.id == job_id).values(
                    attempt_generation=3, state="running"))
                await takeover.commit()
            with pytest.raises(StaleWorker):
                async with fence.transaction_fence(stale_session, job_id, 2):
                    pass
            assert loaded.attempt_generation == 2
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_inactive_child_fence_cannot_enter_publication_context(executor_pg):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, backend_pid = await _leased_job(factory, engine)
    fence = RemoteJobFence(job_id=job_id, owner_id=owner_id, generation=4,
                           attempt_generation=2, backend_pid=backend_pid)
    execution_context = executor_module.JobExecutionFence(fence, job_id, 2, active=False)
    token = executor_module.current_job_fence.set(execution_context)
    published = False
    try:
        async with factory() as session:
            with pytest.raises(StaleWorker):
                async with fence.publication_fence(session):
                    published = True
        assert not published
    finally:
        executor_module.current_job_fence.reset(token)
        await lease_conn.close()


@pytest.mark.asyncio
async def test_inactive_local_worker_child_cannot_enter_publication_context(executor_pg):
    engine, factory, _ = executor_pg
    from news_aggregator.services.background_jobs import BackgroundWorkerLease

    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    job_id = uuid.uuid4()
    async with factory() as session:
        session.add(BackgroundJob(
            id=job_id, kind="news_processing", payload={}, idempotency_key=f"local-publication:{job_id}",
            state="running", owner_generation=lease.generation, attempt_generation=2,
        ))
        await session.commit()
    execution_context = executor_module.JobExecutionFence(lease, job_id, 2, active=False)
    token = executor_module.current_job_fence.set(execution_context)
    published = False
    try:
        async with factory() as session:
            with pytest.raises(StaleWorker):
                async with lease.publication_fence(session):
                    published = True
        assert not published
    finally:
        executor_module.current_job_fence.reset(token)
        await lease.release()


@pytest.mark.asyncio
async def test_legacy_completion_records_recovery_exhausted_error(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    try:
        async with factory() as session:
            await session.execute(update(BackgroundJob).where(BackgroundJob.id == job_id).values(
                state="failed", error_code="worker_recovery_exhausted", finished_at=datetime.utcnow()))
            await session.commit()
        await PythonJobExecutor().apply_legacy_completion(job_id, 2)
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, 2))
            assert receipt.error_code == "worker_recovery_exhausted"
            assert receipt.legacy_completion_applied_at is not None
    finally:
        await lease_conn.close()


async def _invoke_asgi(app, method, path, body=b"", headers=()):
    sent = []
    consumed = False

    async def receive():
        nonlocal consumed
        if consumed:
            await asyncio.Future()
        consumed = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": method, "scheme": "http",
        "path": path, "raw_path": path.encode(), "query_string": b"",
        "root_path": "", "headers": list(headers), "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 8100),
    }
    await app(scope, receive, send)
    start = next(item for item in sent if item["type"] == "http.response.start")
    payload = next(item for item in sent if item["type"] == "http.response.body").get("body", b"")
    return start["status"], json.loads(payload or b"{}")


@pytest.mark.asyncio
async def test_private_http_post_get_and_concurrent_lost_ack_replay(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    worker = PythonJobExecutor()
    launches = []
    async def launch(accepted):
        launches.append(accepted["job_id"])
    worker.launch = launch
    monkeypatch.setattr(executor_module, "executor", worker)
    monkeypatch.setattr(executor_module.settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "executor-test-token-123"))
    envelope = {
        "version": 1, "job_id": str(job_id), "owner_id": str(owner_id),
        "generation": 4, "attempt_generation": 2,
        "deadline_at": (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat(),
    }
    body = json.dumps(envelope).encode()
    headers = ((b"authorization", b"Bearer executor-test-token-123"),
               (b"content-type", b"application/json"))
    path = "/internal/v1/job-executions"
    try:
        # Treat the first successful admission response as lost, then replay it.
        status, first = await _invoke_asgi(executor_module.app, "POST", path, body, headers)
        assert status == 202 and first["duplicate"] is False
        replay_status, replay = await _invoke_asgi(executor_module.app, "POST", path, body, headers)
        assert replay_status == 202 and replay["duplicate"] is True
        reads = await asyncio.gather(*[
            _invoke_asgi(executor_module.app, "GET",
                         f"/internal/v1/job-executions/{job_id}/2", headers=headers)
            for _ in range(3)
        ])
        assert all(item[0] == 200 for item in reads)
        assert len({item[1]["deadline_at"] for item in reads}) == 1
        assert reads[0][1]["deadline_at"] is not None
        assert launches == [str(job_id)]
        changed = json.dumps({**envelope, "generation": 5}).encode()
        conflict_status, _ = await _invoke_asgi(executor_module.app, "POST", path, changed, headers)
        assert conflict_status == 409
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_event_endpoints_reject_invalid_utf8_and_non_string_owner(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, _owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module.settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "executor-test-token-123"))
    headers = ((b"authorization", b"Bearer executor-test-token-123"),
               (b"content-type", b"application/json"))
    try:
        event_status, _ = await _invoke_asgi(
            executor_module.app, "POST",
            f"/internal/v1/job-executions/{job_id}/2/events", b"\xff", headers,
        )
        worker_status, _ = await _invoke_asgi(
            executor_module.app, "POST", "/internal/v1/worker-events",
            b'{"event":"recovered","owner_id":12,"generation":4}', headers,
        )
        assert event_status == 422
        assert worker_status == 422
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_finalized_event_rejects_resumable_interrupted_job(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, _owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module.settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "executor-test-token-123"))
    async with factory() as session:
        schedule = ScheduleSettings(task_name="news_processing", is_running=True, enabled=True)
        session.add(schedule)
        await session.flush()
        await session.execute(update(BackgroundJob).where(BackgroundJob.id == job_id).values(
            state="interrupted", payload={"schedule_id": schedule.id},
        ))
        await session.commit()
        schedule_id = schedule.id
    headers = ((b"authorization", b"Bearer executor-test-token-123"),
               (b"content-type", b"application/json"))
    try:
        status, _ = await _invoke_asgi(
            executor_module.app, "POST",
            f"/internal/v1/job-executions/{job_id}/2/events",
            b'{"event":"finalized"}', headers,
        )
        assert status == 409
        async with factory() as session:
            schedule = await session.get(ScheduleSettings, schedule_id)
            receipt = await session.get(BackgroundJobExecution, (job_id, 2))
            assert schedule.is_running is True
            assert receipt is None
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_cancel_endpoint_revokes_fence_before_cancelling_task(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, _ = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(executor_module.settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "executor-test-token-123"))
    async with factory() as session:
        session.add(BackgroundJobExecution(
            job_id=job_id, attempt_generation=2, envelope_sha256="c" * 64,
            executor_instance_id=uuid.uuid4(), owner_id=owner_id, worker_generation=4,
            deadline_at=(datetime.now(timezone.utc) + timedelta(minutes=1)).replace(tzinfo=None),
            state="running",
        ))
        await session.commit()

    key = (job_id, 2)
    fence_context = executor_module.JobExecutionFence(
        RemoteJobFence(job_id=job_id, owner_id=owner_id, generation=4,
                       attempt_generation=2, backend_pid=0), job_id, 2,
    )
    started = asyncio.Event()
    observed_active = []

    async def active_task():
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            observed_active.append(fence_context.active)
            raise

    task = asyncio.create_task(active_task())
    monkeypatch.setattr(executor_module, "_ACTIVE", {key: task})
    monkeypatch.setattr(executor_module, "_ACTIVE_FENCES", {key: fence_context})
    await started.wait()
    request = SimpleNamespace(headers={"authorization": "Bearer executor-test-token-123"})
    try:
        receipt = await executor_module.cancel_execution(job_id, 2, request)
        assert receipt["state"] == "running"
        assert observed_active == [False]
        assert task.done()
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_completion_event_and_stale_receipt_finish_do_not_deadlock(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn, job_id, owner_id, backend_pid = await _leased_job(factory, engine)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    async with factory() as session:
        await session.execute(update(BackgroundJob).where(BackgroundJob.id == job_id).values(
            state="failed", error_code="worker_error", finished_at=datetime.utcnow()))
        await session.commit()
    worker = PythonJobExecutor()
    async def stale_finish():
        fence_token = executor_module.current_job_fence.set(
            executor_module.JobExecutionFence(
            RemoteJobFence(job_id=job_id, owner_id=owner_id, generation=4,
                           attempt_generation=2, backend_pid=backend_pid), job_id, 2,
            )
        )
        try:
            with pytest.raises(StaleWorker):
                await worker._set_receipt_state(job_id, 2, "failed", error_code="worker_error", finished=True)
        finally:
            executor_module.current_job_fence.reset(fence_token)
    try:
        await asyncio.wait_for(asyncio.gather(
            worker.apply_legacy_completion(job_id, 2), stale_finish(),
        ), timeout=2)
    finally:
        await lease_conn.close()
