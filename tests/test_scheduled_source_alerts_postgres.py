"""Scheduled source warning hysteresis with isolated PostgreSQL and fake delivery."""

import asyncio
from datetime import datetime, timedelta
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy import text

import news_aggregator.executor as executor_module
from news_aggregator.executor import PythonJobExecutor
from news_aggregator.models import (
    ArticleIngestionRun, ArticleIngestionSourceRun, BackgroundJob,
    BackgroundJobExecution, BackgroundWorkerStatus, Setting,
)
from news_aggregator.services import scheduled_source_alerts
from news_aggregator.services.background_execution import _bounded_result

_CONFIG = "a" * 64


async def _lease(executor_pg, monkeypatch):
    engine, factory, _ = executor_pg
    lease_conn = await engine.connect()
    assert await lease_conn.scalar(select(BackgroundWorkerStatus.id).where(
        BackgroundWorkerStatus.id == 1)) == 1
    acquired = await lease_conn.scalar(text(
        "SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"
    ))
    assert acquired
    backend_pid = await lease_conn.scalar(text("SELECT pg_backend_pid()"))
    owner_id = uuid.uuid4()
    async with factory() as session:
        await session.execute(text("""
            UPDATE background_worker_status SET owner_id=:owner, generation=4,
                lock_backend_pid=:pid, heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC', status='running'
            WHERE id=1
        """), {"owner": owner_id, "pid": backend_pid})
        await session.commit()
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", factory)
    return lease_conn, factory, owner_id, backend_pid


async def _job(factory, owner_id, backend_pid, *, schedule_key="news:1", warning=None,
               source_id=10, source_type="rss", config=_CONFIG, state="succeeded",
               ai_errors=0, go_run=True, kind="news_processing", source_rows=None, schedule_id=None):
    job_id = uuid.uuid4()
    attempt = 2
    if source_rows is None:
        source_rows = [{"id": source_id, "type": source_type,
                        "config": config, "warning": warning}]
    warning_count = sum(item.get("warning") is not None for item in source_rows)
    job_status = "warning" if warning_count + ai_errors else "success"
    job_result = {"status": job_status, "errors_count": warning_count + ai_errors}
    async with factory() as session:
        job = BackgroundJob(
            id=job_id, kind=kind, payload={"schedule_id": schedule_id} if schedule_id is not None else {},
            idempotency_key=f"source-alert:{job_id}",
            state=state, owner_generation=4, attempt_generation=attempt, schedule_key=schedule_key,
            result=job_result,
        )
        session.add(job)
        await session.flush()
        if go_run:
            run_id = uuid.uuid4()
            go_result = {"status": "warning" if warning_count else "success",
                         "errors_count": warning_count,
                         "sources_total": len(source_rows), "sources_completed": len(source_rows)}
            if warning_count == 0:
                outcomes = {}
            else:
                outcomes = {}
                for source in source_rows:
                    code = source.get("warning")
                    if code:
                        outcomes[code] = outcomes.get(code, 0) + 1
            if warning_count:
                go_result["source_outcomes"] = outcomes
            session.add(ArticleIngestionRun(
                id=run_id, job_id=job_id, attempt_generation=attempt,
                envelope_sha256="b" * 64, owner_id=owner_id, worker_generation=4,
                backend_pid=backend_pid, executor_instance_id=uuid.uuid4(),
                deadline_at=datetime.utcnow() + timedelta(minutes=5), state="succeeded",
                result=go_result,
            ))
            for source in source_rows:
                source_warning = source.get("warning")
                session.add(ArticleIngestionSourceRun(
                    run_id=run_id, source_id=source["id"], source_type=source["type"],
                    config_sha256=source["config"], state="partial" if source_warning else "succeeded",
                    error_code=source_warning,
                ))
        await session.commit()
    return job_id, attempt


def _executor(sender):
    instance = PythonJobExecutor()
    instance.orchestrator = SimpleNamespace(send_operational_alert=sender)
    return instance


@pytest.mark.asyncio
async def test_two_same_source_warnings_alert_and_recovery_need_two_successes(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        first, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        assert (await _executor(sender).apply_legacy_completion(first, attempt))["alert_state"] == "not_needed"
        second, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        results = await __import__("asyncio").gather(
            _executor(sender).apply_legacy_completion(second, attempt),
            _executor(sender).apply_legacy_completion(second, attempt),
        )
        assert results[0]["alert_state"] == "delivered"
        assert sender.await_count == 1
        assert "Source #10 (RSS)" in sender.await_args.args[1]
        assert "transport_failed" in sender.await_args.args[1]
        assert "https://" not in sender.await_args.args[1]

        healthy, attempt = await _job(factory, owner_id, backend_pid)
        assert (await _executor(sender).apply_legacy_completion(healthy, attempt))["alert_state"] == "not_needed"
        assert sender.await_count == 1
        healthy_again, attempt = await _job(factory, owner_id, backend_pid)
        result = await _executor(sender).apply_legacy_completion(healthy_again, attempt)
        assert result["alert_state"] == "delivered"
        assert sender.await_count == 2
        assert "Source #10 (RSS) recovered" in sender.await_args.args[1]
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_different_sources_do_not_combine_and_config_change_resets_streak(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", source_id=10)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", source_id=11)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 0

        # Restart the same source streak on news:1, then verify news:2 cannot
        # contribute its observation to that counter.
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", source_id=10)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 0
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", source_id=10,
                                     schedule_key="news:2")
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 0
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", source_id=10)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 1

        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", source_id=10,
                                     config="c" * 64)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 1
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_schedule_key_and_schedule_id_namespaces_do_not_collide(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        key_job, attempt = await _job(factory, owner_id, backend_pid, schedule_key="1",
                                      warning="transport_failed")
        await _executor(sender).apply_legacy_completion(key_job, attempt)
        id_job, attempt = await _job(factory, owner_id, backend_pid, schedule_key=None, schedule_id=1,
                                     warning="transport_failed")
        await _executor(sender).apply_legacy_completion(id_job, attempt)
        assert sender.await_count == 0
        key_job, attempt = await _job(factory, owner_id, backend_pid, schedule_key="1",
                                      warning="transport_failed")
        await _executor(sender).apply_legacy_completion(key_job, attempt)
        assert sender.await_count == 1
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_unapplied_scheduled_job_breaks_consecutive_streak(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        first_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(first_bad, attempt)

        # This clean scheduled job is finalized and gets a queue position, but
        # its callback has not been applied before the next bad job arrives.
        await _job(factory, owner_id, backend_pid)
        second_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(second_bad, attempt)
        assert sender.await_count == 0

        third_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(third_bad, attempt)
        assert sender.await_count == 1
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_crashed_failure_delivery_becomes_unknown_without_replay(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        first_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(first_bad, attempt)

        sender.side_effect = asyncio.CancelledError()
        crashed_job, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        with pytest.raises(asyncio.CancelledError):
            await _executor(sender).apply_legacy_completion(crashed_job, attempt)
        sender.side_effect = None
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (crashed_job, attempt))
            state = await session.scalar(select(Setting).where(
                Setting.key.like("scheduled_source_alerts:v1:%"),
            ))
            assert receipt.legacy_completion_applied_at is not None
            assert receipt.legacy_alert_state == "attempting"
            assert state.value["sources"]["10"]["incident"] == "attempting"

        # A fresh executor observes the orphan but never resends the old event.
        next_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(next_bad, attempt)
        assert sender.await_count == 1
        for _ in range(2):
            healthy, attempt = await _job(factory, owner_id, backend_pid)
            await _executor(sender).apply_legacy_completion(healthy, attempt)
        for _ in range(2):
            bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
            await _executor(sender).apply_legacy_completion(bad, attempt)
        assert sender.await_count == 2
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_crashed_recovery_delivery_never_resends_or_claims_recovery(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        for _ in range(2):
            bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
            await _executor(sender).apply_legacy_completion(bad, attempt)
        healthy, attempt = await _job(factory, owner_id, backend_pid)
        await _executor(sender).apply_legacy_completion(healthy, attempt)

        sender.side_effect = asyncio.CancelledError()
        recovery_job, attempt = await _job(factory, owner_id, backend_pid)
        with pytest.raises(asyncio.CancelledError):
            await _executor(sender).apply_legacy_completion(recovery_job, attempt)
        sender.side_effect = None
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (recovery_job, attempt))
            state = await session.scalar(select(Setting).where(
                Setting.key.like("scheduled_source_alerts:v1:%"),
            ))
            assert receipt.legacy_alert_state == "attempting"
            assert state.value["sources"]["10"]["incident"] == "recovering"

        clean, attempt = await _job(factory, owner_id, backend_pid)
        await _executor(sender).apply_legacy_completion(clean, attempt)
        assert sender.await_count == 2
        for _ in range(2):
            bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
            await _executor(sender).apply_legacy_completion(bad, attempt)
        assert sender.await_count == 3
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_reconciliation_preserves_marker_for_a_locally_inflight_sender(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    started = asyncio.Event()
    release = asyncio.Event()
    sends = []

    async def sender(title, message):
        sends.append((title, message))
        started.set()
        await release.wait()
        return True

    try:
        first_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(first_bad, attempt)
        inflight, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        task = asyncio.create_task(_executor(sender).apply_legacy_completion(inflight, attempt))
        await asyncio.wait_for(started.wait(), timeout=5)

        later_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(later_bad, attempt)
        async with factory() as session:
            state = await session.scalar(select(Setting).where(
                Setting.key.like("scheduled_source_alerts:v1:%"),
            ))
            assert state.value["sources"]["10"]["incident"] == "attempting"
            assert state.value["sources"]["10"]["event_job"] == str(inflight)

        release.set()
        await task
        assert len(sends) == 1
        async with factory() as session:
            state = await session.scalar(select(Setting).where(
                Setting.key.like("scheduled_source_alerts:v1:%"),
            ))
            assert state.value["sources"]["10"]["incident"] == "active"
    finally:
        release.set()
        await lease_conn.close()


@pytest.mark.asyncio
async def test_mixed_recovery_and_new_source_incident_are_aggregated(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        both = lambda bad_id, include_bad: [
            {"id": 10, "type": "rss", "config": _CONFIG,
             "warning": "transport_failed" if include_bad and bad_id == 10 else None},
            {"id": 11, "type": "custom", "config": "c" * 64,
             "warning": "transport_failed" if include_bad and bad_id == 11 else None},
        ]
        job_id, attempt = await _job(factory, owner_id, backend_pid,
                                     source_rows=both(10, True))
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        job_id, attempt = await _job(factory, owner_id, backend_pid,
                                     source_rows=both(10, True))
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 1

        job_id, attempt = await _job(factory, owner_id, backend_pid,
                                     source_rows=both(11, True))
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        job_id, attempt = await _job(factory, owner_id, backend_pid,
                                     source_rows=both(11, True))
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 2
        final_message = sender.await_args.args[1]
        assert "Source #10 (RSS) recovered" in final_message
        assert "Source #11 (Page Monitor) reported transport_failed" in final_message
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_replayed_completion_and_unknown_send_are_never_retried(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 0
        sender.side_effect = RuntimeError("delivery outcome unknown")
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        result = await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert result["alert_state"] == "unknown"
        sender.side_effect = None
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 1
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, attempt))
            assert receipt.legacy_alert_state == "unknown"
        for _ in range(2):
            healthy, attempt = await _job(factory, owner_id, backend_pid)
            await _executor(sender).apply_legacy_completion(healthy, attempt)
        for _ in range(2):
            bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
            await _executor(sender).apply_legacy_completion(bad, attempt)
        assert sender.await_count == 2
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_missing_source_does_not_count_as_recovery_and_manual_job_does_not_break_streak(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(bad, attempt)
        manual, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed",
                                     schedule_key=None, source_id=10)
        await _executor(sender).apply_legacy_completion(manual, attempt)
        bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(bad, attempt)
        assert sender.await_count == 1

        # An active incident is not recovered by a scheduled observation where
        # that source is absent, nor by the first later healthy observation.
        missing, attempt = await _job(factory, owner_id, backend_pid, source_rows=[])
        await _executor(sender).apply_legacy_completion(missing, attempt)
        healthy, attempt = await _job(factory, owner_id, backend_pid)
        await _executor(sender).apply_legacy_completion(healthy, attempt)
        assert sender.await_count == 1
        healthy, attempt = await _job(factory, owner_id, backend_pid)
        await _executor(sender).apply_legacy_completion(healthy, attempt)
        assert sender.await_count == 2
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_unknown_observation_and_real_job_failure_alert_immediately(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="unknown_warning")
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 1
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", go_run=False)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 2
        job_id, attempt = await _job(factory, owner_id, backend_pid, warning=None, state="failed", go_run=False)
        await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert sender.await_count == 3
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_clean_source_observation_recovers_existing_generic_incident(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    executor = _executor(sender)
    try:
        fallback, attempt = await _job(factory, owner_id, backend_pid, go_run=False)
        result = await executor.apply_legacy_completion(fallback, attempt)
        assert result["alert_state"] == "not_needed"
        assert sender.await_count == 0
        failed, attempt = await _job(factory, owner_id, backend_pid, state="failed", go_run=False)
        await executor.apply_legacy_completion(failed, attempt)
        assert sender.await_count == 1
        clean, attempt = await _job(factory, owner_id, backend_pid)
        result = await executor.apply_legacy_completion(clean, attempt)
        assert result["alert_state"] == "delivered"
        assert sender.await_count == 2
        assert "recovered" in sender.await_args.args[0]

        warning, attempt = await _job(factory, owner_id, backend_pid, warning="unknown_warning")
        await executor.apply_legacy_completion(warning, attempt)
        assert sender.await_count == 3
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_unknown_observation_breaks_streak_and_corrupt_state_falls_back(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    try:
        first, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(first, attempt)
        invalid, attempt = await _job(factory, owner_id, backend_pid, warning="unknown_warning")
        await _executor(sender).apply_legacy_completion(invalid, attempt)
        assert sender.await_count == 1
        next_bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        await _executor(sender).apply_legacy_completion(next_bad, attempt)
        assert sender.await_count == 1

        mixed, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed", ai_errors=1)
        await _executor(sender).apply_legacy_completion(mixed, attempt)
        assert sender.await_count == 2

        async with factory() as session:
            row = await session.scalar(select(Setting).where(Setting.key.like("scheduled_source_alerts:v1:%")))
            row.value = {"corrupt": True}
            await session.commit()
        bad, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")
        result = await _executor(sender).apply_legacy_completion(bad, attempt)
        assert result["alert_state"] == "delivered"
        assert sender.await_count == 3
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (bad, attempt))
            assert receipt.legacy_completion_applied_at is not None
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_policy_sql_failure_rolls_back_savepoint_then_alerts_generically(executor_pg, monkeypatch):
    lease_conn, factory, owner_id, backend_pid = await _lease(executor_pg, monkeypatch)
    sender = AsyncMock(return_value=True)
    job_id, attempt = await _job(factory, owner_id, backend_pid, warning="transport_failed")

    async def fail_policy(session, *args, **kwargs):
        await session.execute(text("SELECT 1 / 0"))

    monkeypatch.setattr(scheduled_source_alerts, "prepare_alert", fail_policy)
    try:
        result = await _executor(sender).apply_legacy_completion(job_id, attempt)
        assert result["alert_state"] == "delivered"
        assert sender.await_count == 1
        async with factory() as session:
            receipt = await session.get(BackgroundJobExecution, (job_id, attempt))
            assert receipt.legacy_completion_applied_at is not None
    finally:
        await lease_conn.close()


def test_error_metrics_do_not_mask_ai_errors_behind_source_warnings():
    result = _bounded_result("news_processing", {
        "status": "warning", "errors": ["source warning", "category mapping warning"],
        "errors_count": 1,
    })
    assert result["errors_count"] == 2
