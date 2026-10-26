"""PostgreSQL concurrency and fencing checks for the durable job queue."""

import asyncio
import hashlib
import math
import os
import uuid
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
from news_aggregator.migrations.job_executor import JobExecutorMigration
from news_aggregator.models import (
    Article, BackgroundJob, BackgroundJobIdempotencyKey, BackgroundWorkerStatus, ScheduleSettings, Source,
)
from news_aggregator.database import Base
from news_aggregator.services.background_jobs import (
    BackgroundWorkerLease,
    IdempotencyConflict,
    QueueFull,
    StaleWorker,
    WorkerLeaseUnavailable,
    claim_next_job,
    enqueue_job_in_session,
    finish_job,
    validate_payload,
)
from news_aggregator.services.background_execution import _bounded_result


def _test_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    return url


@pytest.fixture
async def pg_jobs():
    url = _test_url()
    schema = "test_bg_jobs_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        migration = BackgroundJobsMigration()
        async with factory() as session:
            assert await migration.check_needed(session)
            await migration.execute(session)
            assert not await migration.check_needed(session)
            await migration.execute(session)
            # 010 remains a distinct additive migration so an existing 009
            # installation receives scheduler metadata without rewriting 009.
            scheduler_metadata = ScheduledJobMetadataMigration()
            assert await scheduler_metadata.check_needed(session)
            await scheduler_metadata.execute(session)
            assert not await scheduler_metadata.check_needed(session)
            executor_migration = JobExecutorMigration()
            assert await executor_migration.check_needed(session)
            await executor_migration.execute(session)
            assert not await executor_migration.check_needed(session)
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync: ScheduleSettings.__table__.create(sync, checkfirst=True))
        yield engine, factory
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_payload_validation_bounds_and_rejects_bool_as_id():
    with pytest.raises(ValueError):
        validate_payload("unknown", {})
    with pytest.raises(ValueError):
        validate_payload("article_reprocess", {"article_ids": [True]})
    with pytest.raises(ValueError):
        validate_payload("article_reprocess", {"article_ids": list(range(1, 102))})
    with pytest.raises(ValueError):
        validate_payload("article_reprocess", {"article_ids": [2, 2]})
    with pytest.raises(ValueError):
        validate_payload("news_processing", {"url": "https://example.test"})
    with pytest.raises(ValueError):
        validate_payload("article_reprocess", {"article_ids": [1], "force": 1})
    with pytest.raises(ValueError):
        validate_payload("news_processing", {"schedule_id": True, "scheduled_for": "2026-10-23T09:00:00"})
    with pytest.raises(ValueError):
        validate_payload("reprocess_failed", {"dry_run": "false"})
    with pytest.raises(ValueError):
        validate_payload("source_test", {"source_id": True})
    assert validate_payload("news_processing", {
        "schedule_id": 7, "scheduled_for": "2026-10-23T09:00:00+02:00"}) == {
            "schedule_id": 7, "scheduled_for": "2026-10-23T09:00:00+02:00"}
    assert validate_payload("article_reprocess", {"article_ids": [2, 1]}) == {
        "article_ids": [1, 2], "force": True, "reextract_media": False}
    assert validate_payload("summaries_generate", {}) == {}
    assert validate_payload("source_test", {"source_id": 5}) == {"source_id": 5}
    assert validate_payload("news_digest", {"run_processing": False, "send_telegram": True}) == {
        "run_processing": False, "send_telegram": True}
    with pytest.raises(ValueError):
        from news_aggregator.services.background_jobs import _safe_result
        _safe_result({"error": "https://example.test"})
    with pytest.raises(ValueError):
        _safe_result({"duration_seconds": math.inf})
    assert _safe_result({"connection_ok": True}) == {"connection_ok": True}


@pytest.mark.asyncio
async def test_concurrent_enqueue_respects_pending_limit_and_idempotency(pg_jobs):
    _, factory = pg_jobs

    async def enqueue(i):
        async with factory() as session:
            try:
                job = await enqueue_job_in_session(
                    session, "article_reprocess", {"article_ids": [i + 1]},
                    f"test-cap:{i}", coalesce_active=False)
                await session.commit()
                return job.id
            except QueueFull:
                await session.rollback()
                return None

    results = await asyncio.gather(*(enqueue(i) for i in range(40)))
    assert sum(item is not None for item in results) == 32
    async with factory() as session:
        await session.execute(update(BackgroundJob).values(state="succeeded"))
        await session.commit()

    async with factory() as session:
        job = await enqueue_job_in_session(session, "news_processing", {}, "same-key")
        await session.commit()
        assert job.state == "queued"
        assert job.idempotency_key != "same-key"
    async with factory() as session:
        same = await enqueue_job_in_session(session, "news_processing", {}, "same-key")
        assert same.id == job.id
        await session.commit()
    async with factory() as session:
        with pytest.raises(IdempotencyConflict):
            await enqueue_job_in_session(session, "backup", {}, "same-key")
        await session.rollback()


@pytest.mark.asyncio
async def test_identical_manual_active_work_coalesces_but_payloads_do_not(pg_jobs):
    _, factory = pg_jobs
    async with factory() as session:
        first = await enqueue_job_in_session(session, "article_reprocess", {"article_ids": [10]}, "manual:a")
        await session.commit()
    async with factory() as session:
        same = await enqueue_job_in_session(session, "article_reprocess", {"article_ids": [10]}, "manual:b")
        different = await enqueue_job_in_session(session, "article_reprocess", {"article_ids": [11]}, "manual:c")
        first_db = await session.get(BackgroundJob, first.id)
        first_db.state = "interrupted"
        interrupted_same = await enqueue_job_in_session(
            session, "article_reprocess", {"article_ids": [10]}, "manual:d")
        await session.commit()
        assert same.id == first_db.id
        assert interrupted_same.id == first.id
        assert different.id != first.id
    async with factory() as session:
        first_db = await session.get(BackgroundJob, first.id)
        first_db.state = "succeeded"
        await session.commit()
    async with factory() as session:
        replayed_alias = await enqueue_job_in_session(
            session, "article_reprocess", {"article_ids": [10]}, "manual:b")
        assert replayed_alias.id == first.id
        await session.commit()
    async with factory() as session:
        with pytest.raises(IdempotencyConflict):
            await enqueue_job_in_session(
                session, "article_reprocess", {"article_ids": [99]}, "manual:b")
        await session.rollback()


@pytest.mark.asyncio
async def test_selected_article_outcome_survives_the_durable_job_receipt(pg_jobs, monkeypatch):
    engine, factory = pg_jobs
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: Base.metadata.create_all(
            sync, tables=[Source.__table__, Article.__table__]))
    async with factory() as session:
        session.add(Article(id=17, title="Selected result fixture",
                            url="https://example.test/selected-result", content="Synthetic content"))
        await session.commit()
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    try:
        async with factory() as session:
            queued = await enqueue_job_in_session(
                session, "article_reprocess", {"article_ids": [17], "force": True},
                "selected-result:17", coalesce_active=False)
            await session.commit()
        async with factory() as session:
            claimed = await claim_next_job(session, lease)
            await session.commit()
            assert claimed.id == queued.id

        result = _bounded_result("article_reprocess", {
            "status": "retry_wait", "success": False, "settled": False,
            "retryable": True, "processed": 0, "improved": 0,
            "article_outcomes": [{
                "article_id": 17, "state": "retry_wait", "reason": "missing_summary",
            }],
        })
        async with factory() as session:
            await finish_job(session, lease, queued.id, "succeeded",
                             attempt_generation=claimed.attempt_generation, result=result)
            await session.commit()
        async with factory() as session:
            persisted = await session.get(BackgroundJob, queued.id)
            assert persisted.state == "succeeded"
            assert persisted.result == result
            assert persisted.result["status"] == "retry_wait"
            assert persisted.result["settled"] is False
            assert persisted.result["retryable"] is True
            assert persisted.result["article_outcomes"] == [{
                "article_id": 17, "state": "retry_wait", "reason": "missing_summary",
            }]

        # Exercise the public job-status response against the persisted receipt,
        # as clients observe it after the selected reprocess request is queued.
        from fastapi import Response
        from importlib import import_module
        jobs_api = import_module("news_aggregator.api.background_jobs_router")
        monkeypatch.setattr(jobs_api, "AsyncSessionLocal", factory)
        monkeypatch.setattr(jobs_api, "read_worker_status", AsyncMock(return_value={"healthy": True}))
        public_receipt = await jobs_api.get_job(queued.id, Response())
        assert public_receipt["status"] == "succeeded"
        assert public_receipt["result"] == result
        assert public_receipt["result"]["article_outcomes"][0]["state"] == "retry_wait"
    finally:
        await lease.release()


@pytest.mark.asyncio
async def test_singleton_lease_claim_recovery_and_stale_generation_fence(pg_jobs):
    engine, factory = pg_jobs
    first, second = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    assert await first.acquire()
    assert not await second.acquire()
    async with factory() as session:
        processing = await enqueue_job_in_session(session, "news_processing", {}, "process:one")
        await session.commit()
    async with factory() as session:
        claimed_processing = await claim_next_job(session, first)
        await session.commit()
        assert claimed_processing.id == processing.id
        assert await claim_next_job(session, first) is None
    # Simulate a process crash: PostgreSQL drops its session advisory lock.
    lost_connection = first.connection
    await lost_connection.invalidate()
    await lost_connection.close()
    first.connection = first.owner_id = first.generation = None
    assert await second.acquire()
    async with factory() as session:
        recovered = await second.recover_jobs(session)
        await session.commit()
        assert recovered == {"interrupted": 1, "delivery_unknown": 0, "recovery_exhausted": 0}
        proc = await session.get(BackgroundJob, processing.id)
        assert proc.state == "interrupted"
        with pytest.raises(StaleWorker):
            await first.assert_generation(session, lock_row=True)
        await session.rollback()
    async with factory() as session:
        resumed = await claim_next_job(session, second)
        await session.commit()
        assert resumed.id == processing.id
        assert resumed.owner_generation == second.generation
    await second.release()


@pytest.mark.asyncio
async def test_crashed_telegram_job_becomes_delivery_unknown_and_is_not_replayed(pg_jobs):
    engine, factory = pg_jobs
    first, second = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    assert await first.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(session, "telegram_digest", {}, "delivery:crash")
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, first)
        await session.commit()
        assert claimed.id == job.id
    old_connection = first.connection
    await old_connection.invalidate()
    await old_connection.close()
    first.connection = first.owner_id = first.generation = None
    assert await second.acquire()
    async with factory() as session:
        assert await second.recover_jobs(session) == {"interrupted": 0, "delivery_unknown": 1, "recovery_exhausted": 0}
        await session.commit()
    async with factory() as session:
        unknown = await session.get(BackgroundJob, job.id)
        assert unknown.state == "delivery_unknown"
        assert await claim_next_job(session, second) is None
        await session.rollback()
    await second.release()


@pytest.mark.asyncio
async def test_job_and_alias_server_timestamps_are_utc_with_local_session_timezone(pg_jobs):
    _, factory = pg_jobs
    before = datetime.utcnow()
    async with factory() as session:
        await session.execute(text("SET TIME ZONE 'Europe/Belgrade'"))
        job = await enqueue_job_in_session(session, "news_processing", {}, "utc:created")
        await session.commit()
        created = await session.get(BackgroundJob, job.id)
        alias = await session.get(
            BackgroundJobIdempotencyKey,
            hashlib.sha256(b"utc:created").hexdigest(),
        )
        assert abs((created.created_at - before).total_seconds()) < 5
        assert abs((alias.created_at - before).total_seconds()) < 5


@pytest.mark.asyncio
async def test_recovering_scheduled_digest_clears_running_schedule_atomically(pg_jobs):
    engine, factory = pg_jobs
    future_due = datetime.utcnow() + timedelta(days=1)
    first, second = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    assert await first.acquire()
    async with factory() as session:
        schedule = ScheduleSettings(
            task_name="telegram_digest", enabled=True, is_running=True,
            next_run=future_due, last_status="running",
        )
        session.add(schedule)
        await session.flush()
        job = await enqueue_job_in_session(session, "telegram_digest", {
            "schedule_id": schedule.id,
            "scheduled_for": "2026-10-23T07:00:00",
        }, "schedule:telegram:crash", coalesce_active=False)
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, first)
        await session.commit()
        assert claimed.id == job.id
    connection = first.connection
    await connection.invalidate()
    await connection.close()
    first.connection = first.owner_id = first.generation = None
    assert await second.acquire()

    async with factory() as session:
        assert await second.recover_jobs(session) == {"interrupted": 0, "delivery_unknown": 1, "recovery_exhausted": 0}
        await session.commit()
    async with factory() as session:
        recovered_schedule = await session.get(ScheduleSettings, schedule.id)
        recovered_job = await session.get(BackgroundJob, job.id)
        assert recovered_job.state == "delivery_unknown"
        assert recovered_schedule.is_running is False
        assert recovered_schedule.last_status == "delivery_unknown"
        assert recovered_schedule.last_error == "External delivery outcome is unknown"
        assert recovered_schedule.next_run == future_due
    await second.release()


@pytest.mark.asyncio
async def test_recovered_scheduled_processing_finishes_schedule_successfully(pg_jobs):
    from news_aggregator.services.background_execution import DurableBackgroundWorker

    engine, factory = pg_jobs
    first, second = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    assert await first.acquire()
    async with factory() as session:
        schedule = ScheduleSettings(
            task_name="news_processing", enabled=True, is_running=True,
            next_run=datetime(2026, 10, 24, 7, 0), last_status="running",
        )
        session.add(schedule)
        await session.flush()
        job = await enqueue_job_in_session(session, "news_processing", {
            "schedule_id": schedule.id,
            "scheduled_for": "2026-10-23T07:00:00",
        }, "schedule:processing:crash", coalesce_active=False)
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, first)
        await session.commit()
        assert claimed.id == job.id
    connection = first.connection
    await connection.invalidate()
    await connection.close()
    first.connection = first.owner_id = first.generation = None
    assert await second.acquire()
    async with factory() as session:
        assert await second.recover_jobs(session) == {"interrupted": 1, "delivery_unknown": 0, "recovery_exhausted": 0}
        await session.commit()
    async with factory() as session:
        recovering_schedule = await session.get(ScheduleSettings, schedule.id)
        assert recovering_schedule.is_running is True
        resumed = await claim_next_job(session, second)
        await session.commit()
        assert resumed.id == job.id
    worker = DurableBackgroundWorker(db_engine=engine, session_factory=factory)
    worker.lease = second
    result = {"status": "success", "duration_seconds": 1.0}
    async with factory() as session:
        await finish_job(session, second, job.id, "succeeded",
                         attempt_generation=resumed.attempt_generation, result=result)
        await worker._finish_schedule_in_session(session, resumed, "succeeded", result)
        await session.commit()
    async with factory() as session:
        finished_schedule = await session.get(ScheduleSettings, schedule.id)
        assert finished_schedule.is_running is False
        assert finished_schedule.last_status == "success"
        assert finished_schedule.last_success_at is not None
    await second.release()


@pytest.mark.asyncio
async def test_failed_acquire_releases_advisory_lock(pg_jobs):
    engine, factory = pg_jobs
    async with factory() as session:
        await session.execute(delete(BackgroundWorkerStatus).where(BackgroundWorkerStatus.id == 1))
        await session.commit()
    failed, next_owner = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    with pytest.raises(WorkerLeaseUnavailable, match="worker status row is missing"):
        await failed.acquire()
    async with factory() as session:
        session.add(BackgroundWorkerStatus(id=1, generation=0, status="stopped"))
        await session.commit()
    assert await next_owner.acquire()
    await next_owner.release()


@pytest.mark.asyncio
async def test_heartbeat_failure_releases_live_session_lock(pg_jobs):
    engine, factory = pg_jobs
    first, next_owner = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    assert await first.acquire()
    async with factory() as session:
        row = await session.get(BackgroundWorkerStatus, 1)
        row.owner_id = None
        row.status = "stopped"
        await session.commit()
    with pytest.raises(StaleWorker):
        await first.heartbeat()
    assert first.connection is None
    assert await next_owner.acquire()
    await next_owner.release()


@pytest.mark.asyncio
async def test_old_attempt_cannot_write_after_same_worker_reclaims_job(pg_jobs):
    engine, factory = pg_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(session, "news_processing", {}, "attempt:one")
        await session.commit()
    async with factory() as session:
        first_attempt = await claim_next_job(session, lease)
        await session.commit()
    async with factory() as session:
        row = await session.get(BackgroundJob, job.id)
        row.state = "interrupted"
        await session.commit()
    async with factory() as session:
        second_attempt = await claim_next_job(session, lease)
        await session.commit()
    assert second_attempt.attempt_generation == first_attempt.attempt_generation + 1
    async with factory() as session:
        with pytest.raises(StaleWorker):
            async with lease.transaction_fence(session, job.id, first_attempt.attempt_generation):
                pass
        await session.rollback()
    async with factory() as session:
        with pytest.raises(StaleWorker):
            await finish_job(session, lease, job.id, "succeeded",
                             attempt_generation=first_attempt.attempt_generation)
        await session.rollback()
    async with factory() as session:
        await finish_job(session, lease, job.id, "succeeded",
                         attempt_generation=second_attempt.attempt_generation,
                         result={"articles_processed": 0})
        await session.commit()
    await lease.release()


@pytest.mark.asyncio
async def test_fenced_finish_is_generation_checked_and_delivery_unknown_not_replayed(pg_jobs):
    engine, factory = pg_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(session, "news_digest", {}, "digest:one")
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()
        assert claimed.id == job.id
    async with factory() as session:
        await finish_job(session, lease, job.id, "delivery_unknown",
                         attempt_generation=claimed.attempt_generation,
                         error_code="external_outcome_unknown")
        await session.commit()
    async with factory() as session:
        assert await claim_next_job(session, lease) is None
    await lease.release()


@pytest.mark.asyncio
async def test_scheduler_enqueue_is_atomic_with_caller_transaction(pg_jobs):
    _, factory = pg_jobs
    async with factory() as session:
        job = await enqueue_job_in_session(
            session, "news_processing",
            {"schedule_id": 7, "scheduled_for": "2026-10-23T09:00:00+02:00"},
            "schedule:7:2026-10-23T09:00:00+02:00",
            coalesce_active=False)
        job_id = job.id
        await session.rollback()
    async with factory() as session:
        assert await session.get(BackgroundJob, job_id) is None
        job = await enqueue_job_in_session(
            session, "news_processing",
            {"schedule_id": 7, "scheduled_for": "2026-10-23T09:00:00+02:00"},
            "schedule:7:2026-10-23T09:00:00+02:00",
            coalesce_active=False)
        await session.commit()
        assert job.state == "queued"
