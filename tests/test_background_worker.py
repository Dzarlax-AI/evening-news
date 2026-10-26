"""Runtime fencing checks for the separately deployed background worker."""

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import make_url

from news_aggregator.database import FencedAsyncSession
from news_aggregator.models import Article, BackgroundJob, Source
from news_aggregator.services.background_execution import (
    DurableBackgroundWorker, JobExecutionFence, _bounded_result, job_execution,
    worker_healthcheck,
)
from news_aggregator.services.background_jobs import (
    BackgroundWorkerLease, BackgroundWorkerStatus, StaleWorker,
    claim_next_job, enqueue_job_in_session, read_worker_status,
)
from news_aggregator.services.background_jobs import _safe_result
from news_aggregator.models import ScheduleSettings


@pytest.mark.asyncio
async def test_worker_retention_loop_runs_pruning_and_stops_cleanly():
    worker = DurableBackgroundWorker()
    worker.stop_event = asyncio.Event()

    async def prune_once():
        worker.stop_event.set()

    worker._prune_terminal_jobs = AsyncMock(side_effect=prune_once)
    await worker._retention_loop()
    worker._prune_terminal_jobs.assert_awaited_once()


@pytest.fixture
async def pg_worker_jobs():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    from sqlalchemy.ext.asyncio import create_async_engine
    from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
    from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
    from news_aggregator.migrations.job_executor import JobExecutorMigration
    schema = "test_worker_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
            await ScheduledJobMetadataMigration().execute(session)
            await JobExecutorMigration().execute(session)
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: ScheduleSettings.__table__.create(sync, checkfirst=True))
        yield engine, factory, admin, schema
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_worker_schema_readiness_fails_closed_without_migration(pg_worker_jobs, monkeypatch):
    engine, _, admin, _ = pg_worker_jobs
    missing_schema = "test_worker_missing_" + uuid.uuid4().hex
    # Use a new engine with an empty isolated schema, then skip backoff delays.
    from sqlalchemy.ext.asyncio import create_async_engine
    # Preserve the parsed URL object so SQLAlchemy does not redact credentials
    # when constructing the test engine.
    source_url = engine.url
    empty = create_async_engine(source_url, connect_args={
        "server_settings": {"search_path": missing_schema}
    })
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{missing_schema}"'))

    async def no_sleep(_):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    worker = DurableBackgroundWorker(db_engine=empty)
    with pytest.raises(RuntimeError, match="apply database migrations"):
        await worker.check_schema()
    await empty.dispose()
    async with admin.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA "{missing_schema}" CASCADE'))


@pytest.mark.asyncio
async def test_worker_refuses_embedded_mode(monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "background_execution_mode", "embedded")
    monkeypatch.setattr(type(settings), "validate_runtime_services", lambda self: True)
    worker = DurableBackgroundWorker(db_engine=object())
    worker.lease.acquire = AsyncMock()
    with pytest.raises(RuntimeError, match="BACKGROUND_EXECUTION_MODE=worker"):
        await worker.run()
    worker.lease.acquire.assert_not_awaited()


def test_large_selected_result_is_bounded_without_losing_aggregate():
    domain = {
        "status": "incomplete",
        "article_outcomes": [
            {"article_id": article_id, "state": "completed", "reason": "transient detail"}
            for article_id in range(1, 100)
        ],
    }
    domain["article_outcomes"].append(
        {"article_id": 100, "state": "rejected", "reason": "resource_limit"}
    )
    result = _bounded_result("article_reprocess", domain)
    assert result["article_outcomes_total"] == 100
    assert result["article_outcomes_omitted"] == 80
    assert len(result["article_outcomes"]) == 20
    assert result["status"] == "incomplete"
    assert _safe_result(result) == result
    persisted = _bounded_result("article_reprocess", {
        **result, "duration_seconds": 1.25,
    })
    assert persisted["article_outcomes_total"] == 100
    assert persisted["article_outcomes_omitted"] == 80
    assert persisted["status"] == "incomplete"
    assert persisted["duration_seconds"] == 1.25
    backup = _bounded_result("backup", {"status": "success", "backup_bytes": 4096})
    assert backup["backup_bytes"] == 4096
    assert _safe_result(backup) == backup
    single = _bounded_result("article_reprocess", {
        "status": "rejected",
        "article_outcomes": [{"article_id": 1, "state": "rejected", "reason": "advertisement"}],
    })
    assert single["article_outcomes"] == [
        {"article_id": 1, "state": "rejected", "reason": "advertisement"}
    ]


@pytest.mark.asyncio
async def test_worker_releases_lease_if_queue_startup_fails(monkeypatch):
    import importlib
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(type(settings), "validate_runtime_services", lambda self: True)
    worker = DurableBackgroundWorker(db_engine=object())
    worker.check_schema = AsyncMock()
    lease_state = {"released": False}

    class FakeLease:
        async def acquire(self):
            return True

        async def release(self):
            lease_state["released"] = True

    class FakeQueue:
        async def start(self):
            raise RuntimeError("queue failed to start")

        async def stop(self):
            lease_state["queue_stopped"] = True

    worker.lease = FakeLease()
    queue_module = importlib.import_module("news_aggregator.services.database_queue")
    monkeypatch.setattr(queue_module, "get_database_queue", lambda: FakeQueue())
    with pytest.raises(RuntimeError, match="queue failed to start"):
        await worker.run()
    assert lease_state == {"released": True, "queue_stopped": True}


@pytest.mark.asyncio
async def test_worker_healthcheck_uses_fresh_persisted_heartbeat(pg_worker_jobs, monkeypatch):
    from datetime import datetime
    from news_aggregator.config import settings

    _, factory, _, _ = pg_worker_jobs
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    async with factory() as session:
        status = await session.get(BackgroundWorkerStatus, 1)
        status.status = "running"
        status.heartbeat_at = datetime.utcnow()
        await session.commit()
    assert await worker_healthcheck(factory) == 0

    async with factory() as session:
        status = await session.get(BackgroundWorkerStatus, 1)
        status.status = "stopped"
        await session.commit()
    assert await worker_healthcheck(factory) == 1


@pytest.mark.asyncio
async def test_ordinary_digest_exception_is_delivery_unknown_and_not_replayed(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(session, "telegram_digest", {}, "digest:exception")
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()

    class FailingDigestOrchestrator:
        async def start(self):
            return None

        async def send_telegram_digest(self):
            raise RuntimeError("delivery response was lost")

    worker = DurableBackgroundWorker(db_engine=engine, session_factory=factory)
    worker.lease = lease
    worker.orchestrator = FailingDigestOrchestrator()
    await worker._run_claim(claimed)

    async with factory() as session:
        saved = await session.get(BackgroundJob, job.id)
        assert saved.state == "delivery_unknown"
        assert saved.error_code == "external_outcome_unknown"
        assert await claim_next_job(session, lease) is None
        await session.rollback()
    await lease.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "timeout", "cancel"])
async def test_news_digest_without_delivery_failures_are_not_delivery_unknown(pg_worker_jobs, failure):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(
            session, "news_digest", {"run_processing": True, "send_telegram": False},
            "digest:no-delivery",
        )
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()

    class FailingProcessingOrchestrator:
        async def start(self):
            return None

        async def run_full_cycle(self, reservation=None):
            if failure == "timeout":
                await asyncio.sleep(1)
            if failure == "cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("processing failed before any delivery was enabled")

    worker = DurableBackgroundWorker(db_engine=engine, session_factory=factory)
    worker.lease = lease
    worker.orchestrator = FailingProcessingOrchestrator()
    if failure == "timeout":
        async def short_timeout(_job):
            return 0.001
        worker._job_timeout_seconds = short_timeout
    if failure == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await worker._run_claim(claimed)
    else:
        await worker._run_claim(claimed)

    async with factory() as session:
        saved = await session.get(BackgroundJob, job.id)
        expected = ("interrupted", "worker_interrupted") if failure == "cancel" else ("failed", "worker_error")
        assert (saved.state, saved.error_code) == expected
        if failure == "cancel":
            resumed = await claim_next_job(session, lease)
            assert resumed.id == job.id
        else:
            assert await claim_next_job(session, lease) is None
        await session.rollback()
    await lease.release()


@pytest.mark.asyncio
async def test_recovery_news_digest_without_delivery_is_resumable(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(
            session, "news_digest", {"run_processing": True, "send_telegram": False},
            "digest:recover-no-delivery",
        )
        job.state = "running"
        job.attempt_generation = 1
        job.owner_generation = lease.generation
        await session.commit()

    async with factory() as session:
        recovered = await lease.recover_jobs(session)
        await session.commit()
    assert recovered == {"interrupted": 1, "delivery_unknown": 0, "recovery_exhausted": 0}

    async with factory() as session:
        saved = await session.get(BackgroundJob, job.id)
        assert saved.state == "interrupted"
        assert saved.error_code == "worker_interrupted"
        resumed = await claim_next_job(session, lease)
        await session.commit()
        assert resumed.id == job.id
    await lease.release()


@pytest.mark.asyncio
async def test_article_reprocess_media_failure_is_best_effort(monkeypatch):
    from types import SimpleNamespace
    from news_aggregator.services import article_media_reextract
    from news_aggregator.services.background_execution import execute_job

    attempted = []
    async def refresh(article_id):
        attempted.append(article_id)
        if article_id == 1:
            raise RuntimeError("media page unavailable")
        return True

    monkeypatch.setattr(article_media_reextract, "reextract_article_media", refresh)

    class SelectedOrchestrator:
        async def process_selected_articles(self, article_ids, **kwargs):
            return {"success": True, "articles_processed": len(article_ids)}

    job = SimpleNamespace(kind="article_reprocess", payload={
        "article_ids": [1, 2], "force": True, "reextract_media": True,
    })
    result = await execute_job(job, SelectedOrchestrator())
    assert attempted == [1, 2]
    assert result == {"success": True, "articles_processed": 2}


@pytest.mark.asyncio
async def test_article_reprocess_media_fence_failure_is_not_swallowed(monkeypatch):
    from types import SimpleNamespace
    from news_aggregator.services import article_media_reextract
    from news_aggregator.services.background_execution import execute_job

    async def stale_refresh(article_id):
        raise StaleWorker("worker attempt is stale")

    monkeypatch.setattr(article_media_reextract, "reextract_article_media", stale_refresh)
    job = SimpleNamespace(kind="article_reprocess", payload={
        "article_ids": [1], "force": True, "reextract_media": True,
    })
    with pytest.raises(StaleWorker):
        await execute_job(job, object())


@pytest.mark.asyncio
async def test_article_reprocess_media_cancellation_is_not_swallowed(monkeypatch):
    from types import SimpleNamespace
    from news_aggregator.services import article_media_reextract
    from news_aggregator.services.background_execution import execute_job

    async def cancelled_refresh(article_id):
        raise asyncio.CancelledError()

    monkeypatch.setattr(article_media_reextract, "reextract_article_media", cancelled_refresh)
    job = SimpleNamespace(kind="article_reprocess", payload={
        "article_ids": [1], "force": True, "reextract_media": True,
    })
    with pytest.raises(asyncio.CancelledError):
        await execute_job(job, object())


@pytest.mark.asyncio
async def test_source_test_success_survives_completion_persistence(pg_worker_jobs, monkeypatch):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(
            session, "source_test", {"source_id": 3}, "source-test:stored-result"
        )
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()

    async def successful_test_executor(*args, **kwargs):
        return {"success": True}

    import news_aggregator.services.background_execution as module
    monkeypatch.setattr(module, "execute_job", successful_test_executor)

    class FakeOrchestrator:
        async def start(self):
            return None

    worker = DurableBackgroundWorker(db_engine=engine, session_factory=factory)
    worker.lease = lease
    worker.orchestrator = FakeOrchestrator()
    await worker._run_claim(claimed)

    async with factory() as session:
        saved = await session.get(BackgroundJob, job.id)
        assert saved.state == "succeeded"
        assert saved.result["connection_ok"] is True
        assert saved.result["status"] == "success"
        assert saved.result["duration_seconds"] >= 0
    await lease.release()


@pytest.mark.asyncio
async def test_selected_article_initialization_is_not_repeated_after_reclaim(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    async with engine.begin() as connection:
        await connection.execute(text("""
            CREATE TABLE articles (
                id INTEGER PRIMARY KEY,
                processing_state VARCHAR(20) NOT NULL,
                processing_attempts INTEGER NOT NULL,
                next_attempt_at TIMESTAMP,
                last_processing_error TEXT,
                processed BOOLEAN NOT NULL,
                summary_processed BOOLEAN NOT NULL,
                category_processed BOOLEAN NOT NULL,
                ad_processed BOOLEAN NOT NULL
            )
        """))
        await connection.execute(text("""
            INSERT INTO articles VALUES (7, 'retry_wait', 3, NULL, 'temporary',
                                         TRUE, TRUE, TRUE, TRUE)
        """))
    first, second = BackgroundWorkerLease(engine), BackgroundWorkerLease(engine)
    assert await first.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(
            session, "article_reprocess", {"article_ids": [7], "force": True},
            "selected:resume", coalesce_active=False,
        )
        await session.commit()
    async with factory() as session:
        initial = await claim_next_job(session, first)
        await session.commit()
        assert initial.initialized_at is not None
    async with factory() as session:
        article = (await session.execute(text("SELECT * FROM articles WHERE id=7"))).one()
        assert article.processing_state == "pending"
        assert article.processing_attempts == 0
        # Simulate work making progress before the worker process crashes.
        await session.execute(text("""
            UPDATE articles SET processing_state='retry_wait', processing_attempts=2,
                summary_processed=TRUE, category_processed=TRUE WHERE id=7
        """))
        await session.commit()
    old_connection = first.connection
    await old_connection.invalidate()
    await old_connection.close()
    first.connection = first.owner_id = first.generation = None
    assert await second.acquire()
    async with factory() as session:
        assert await second.recover_jobs(session) == {"interrupted": 1, "delivery_unknown": 0, "recovery_exhausted": 0}
        await session.commit()
    async with factory() as session:
        resumed = await claim_next_job(session, second)
        await session.commit()
        assert resumed.id == job.id
        assert resumed.initialized_at == initial.initialized_at
    async with factory() as session:
        article = (await session.execute(text("SELECT * FROM articles WHERE id=7"))).one()
        assert article.processing_state == "retry_wait"
        assert article.processing_attempts == 2
        assert article.summary_processed is True
        assert article.category_processed is True
    await second.release()


@pytest.mark.asyncio
async def test_graceful_interruption_keeps_schedule_occurrence_for_resume(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        schedule = ScheduleSettings(task_name="news_processing", enabled=True,
                                    is_running=True, last_status="running")
        session.add(schedule)
        await session.flush()
        job = await enqueue_job_in_session(session, "news_processing", {
            "schedule_id": schedule.id, "scheduled_for": "2026-10-23T07:00:00",
        }, "schedule:graceful-interrupt", coalesce_active=False)
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()

    started = asyncio.Event()
    release = asyncio.Event()

    class PausedOrchestrator:
        async def start(self):
            return None

        async def run_full_cycle(self, reservation=None):
            started.set()
            await release.wait()
            return {"success": True}

    worker = DurableBackgroundWorker(db_engine=engine, session_factory=factory)
    worker.lease = lease
    worker.orchestrator = PausedOrchestrator()
    task = asyncio.create_task(worker._run_claim(claimed))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with factory() as session:
        saved_job = await session.get(BackgroundJob, job.id)
        saved_schedule = await session.get(ScheduleSettings, schedule.id)
        assert saved_job.state == "interrupted"
        assert saved_schedule.is_running is True
        resumed = await claim_next_job(session, lease)
        await session.commit()
        assert resumed.id == job.id

    class SuccessfulOrchestrator:
        async def start(self):
            return None

        async def run_full_cycle(self, reservation=None):
            return {"success": True, "articles_processed": 1}

    worker.orchestrator = SuccessfulOrchestrator()
    await worker._run_claim(resumed)
    async with factory() as session:
        saved_schedule = await session.get(ScheduleSettings, schedule.id)
        assert saved_schedule.is_running is False
        assert saved_schedule.last_status == "success"
        assert saved_schedule.last_success_at is not None
    await lease.release()


@pytest.mark.asyncio
async def test_recovery_reconciles_orphan_schedules_and_preserves_live_or_edited_rows(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    now = datetime.utcnow()
    old = now - timedelta(hours=2)
    due = now - timedelta(minutes=5)
    async with factory() as session:
        processing_orphan = ScheduleSettings(
            task_name="orphan_processing", enabled=True, is_running=False,
            next_run=due, last_run=None,
            last_status="running",
        )
        no_next_run_orphan = ScheduleSettings(
            task_name="orphan_without_next_run", enabled=True, is_running=True,
            next_run=None, last_run=old, updated_at=old - timedelta(minutes=1),
            last_status="running",
        )
        digest_orphan = ScheduleSettings(
            task_name="news_digest", enabled=True, schedule_type="interval",
            task_config={"interval_minutes": 45}, timezone="Europe/Belgrade",
            is_running=True, next_run=due, last_run=old,
            updated_at=old - timedelta(minutes=1), last_status="running",
        )
        queued_schedule = ScheduleSettings(
            task_name="queued_processing", enabled=True, is_running=True,
            next_run=due, last_run=old, updated_at=old - timedelta(minutes=1),
            last_status="running",
        )
        interrupted_schedule = ScheduleSettings(
            task_name="interrupted_processing", enabled=True, is_running=True,
            next_run=due, last_run=old, updated_at=old - timedelta(minutes=1),
            last_status="running",
        )
        running_schedule = ScheduleSettings(
            task_name="running_backup", enabled=True, is_running=True,
            next_run=due, last_run=old, updated_at=old - timedelta(minutes=1),
            last_status="running",
        )
        edited_schedule = ScheduleSettings(
            task_name="edited_backup", enabled=True, is_running=True,
            next_run=now + timedelta(hours=2), task_config={"admin": "preserve"},
            updated_at=now, last_status="admin_edit",
        )
        session.add_all([
            processing_orphan, no_next_run_orphan, digest_orphan, queued_schedule,
            interrupted_schedule, running_schedule, edited_schedule,
        ])
        await session.flush()
        # Reproduce the legacy scheduler UPDATE: updated_at is newer than last_run.
        await session.execute(update(ScheduleSettings).where(
            ScheduleSettings.id == processing_orphan.id
        ).values(is_running=True, last_run=old))
        await session.flush()
        await session.refresh(processing_orphan)
        assert processing_orphan.updated_at > processing_orphan.last_run

        async def scheduled_job(setting, kind, key):
            return await enqueue_job_in_session(session, kind, {
                "schedule_id": setting.id, "scheduled_for": due.isoformat(),
            }, key, coalesce_active=False)

        queued_job = await scheduled_job(queued_schedule, "news_processing", "recover:queued")
        interrupted_job = await scheduled_job(interrupted_schedule, "news_processing", "recover:interrupted")
        running_job = await scheduled_job(running_schedule, "backup", "recover:running")
        interrupted_job.state = "interrupted"
        running_job.state = "running"
        running_job.owner_generation = lease.generation
        running_job.attempt_generation = 1
        await session.commit()

    async with factory() as session:
        result = await lease.recover_jobs(session)
        await session.commit()
    assert result == {"interrupted": 2, "delivery_unknown": 0, "recovery_exhausted": 0}

    async with factory() as session:
        processing_saved = await session.get(ScheduleSettings, processing_orphan.id)
        no_next_run_saved = await session.get(ScheduleSettings, no_next_run_orphan.id)
        digest_saved = await session.get(ScheduleSettings, digest_orphan.id)
        queued_saved = await session.get(ScheduleSettings, queued_schedule.id)
        interrupted_saved = await session.get(ScheduleSettings, interrupted_schedule.id)
        running_saved = await session.get(ScheduleSettings, running_schedule.id)
        edited_saved = await session.get(ScheduleSettings, edited_schedule.id)
        jobs = {
            job.id: job for job in (
                await session.execute(select(BackgroundJob))
            ).scalars().all()
        }

        assert processing_saved.is_running is False
        assert processing_saved.last_status == "interrupted"
        assert processing_saved.next_run == due
        assert no_next_run_saved.is_running is False
        assert no_next_run_saved.next_run > datetime.utcnow()
        assert digest_saved.is_running is False
        assert digest_saved.last_status == "delivery_unknown"
        assert digest_saved.last_error == "External delivery outcome is unknown after service restart"
        assert digest_saved.next_run > datetime.utcnow()
        for saved in (queued_saved, interrupted_saved, running_saved):
            assert saved.is_running is True
        assert jobs[queued_job.id].state == "queued"
        assert jobs[interrupted_job.id].state == "interrupted"
        assert jobs[running_job.id].state == "interrupted"
        assert edited_saved.is_running is False
        assert edited_saved.enabled is True
        assert edited_saved.task_config == {"admin": "preserve"}
        assert edited_saved.next_run > datetime.utcnow()
    await lease.release()


@pytest.mark.asyncio
async def test_old_digest_recovery_does_not_overwrite_newer_schedule_occurrence(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    now = datetime.utcnow()
    schedule_edited_at = now - timedelta(minutes=2)
    async with factory() as session:
        schedule = ScheduleSettings(
            task_name="news_digest", enabled=True, schedule_type="interval",
            task_config={"interval_minutes": 60}, is_running=True,
            last_run=schedule_edited_at, next_run=now + timedelta(hours=3),
            last_status="running", last_error=None, updated_at=schedule_edited_at,
        )
        session.add(schedule)
        await session.flush()
        old_job = await enqueue_job_in_session(
            session, "news_digest", {
                "schedule_id": schedule.id,
                "scheduled_for": (now - timedelta(hours=2)).isoformat(),
            }, "recover:old-digest",
            coalesce_active=False,
        )
        new_job = await enqueue_job_in_session(
            session, "news_digest", {
                "schedule_id": schedule.id,
                "scheduled_for": (now - timedelta(minutes=1)).isoformat(),
            }, "recover:new-digest",
            coalesce_active=False,
        )
        old_job.state = "running"
        old_job.created_at = now - timedelta(minutes=5)
        old_job.started_at = now - timedelta(seconds=1)
        old_job.attempt_generation = 1
        old_job.owner_generation = lease.generation
        new_job.created_at = now - timedelta(minutes=1)
        await session.commit()

    async with factory() as session:
        recovered = await lease.recover_jobs(session)
        await session.commit()
    assert recovered == {"interrupted": 0, "delivery_unknown": 1, "recovery_exhausted": 0}

    async with factory() as session:
        saved_schedule = await session.get(ScheduleSettings, schedule.id)
        saved_old = await session.get(BackgroundJob, old_job.id)
        saved_new = await session.get(BackgroundJob, new_job.id)
        assert saved_old.state == "delivery_unknown"
        assert saved_new.state == "queued"
        assert saved_schedule.is_running is True
        assert saved_schedule.enabled is True
        assert saved_schedule.task_config == {"interval_minutes": 60}
        assert saved_schedule.next_run == now + timedelta(hours=3)
        assert saved_schedule.last_status == "running"
        assert saved_schedule.last_run == schedule_edited_at
    await lease.release()


@pytest.mark.asyncio
async def test_recovery_attempt_limit_fails_poison_job_and_claims_next_without_resetting_article(pg_worker_jobs, monkeypatch):
    engine, factory, _, _ = pg_worker_jobs
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: Source.__table__.create(sync, checkfirst=True))
        await connection.run_sync(lambda sync: Article.__table__.create(sync, checkfirst=True))
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "worker_max_job_attempts", 2)

    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    now = datetime.utcnow()
    async with factory() as session:
        source = Source(id=7, name="Fixture", source_type="rss", url="https://fixture.invalid/feed")
        article = Article(
            id=7, source_id=7, title="Fixture article", url="https://fixture.invalid/article",
            processing_state="retry_wait", processing_attempts=4,
            summary_processed=True, category_processed=True, ad_processed=True,
        )
        schedule = ScheduleSettings(
            task_name="article_reprocess", enabled=True, is_running=True,
            next_run=now + timedelta(hours=1), last_status="running",
        )
        session.add_all([source, article, schedule])
        await session.flush()
        poison = await enqueue_job_in_session(session, "news_processing", {
            "schedule_id": schedule.id,
            "scheduled_for": now.isoformat(),
        }, "recovery:poison", coalesce_active=False)
        next_job = await enqueue_job_in_session(
            session, "news_processing", {}, "recovery:next", coalesce_active=False
        )
        poison.state = "running"
        poison.attempt_generation = 2
        poison.owner_generation = 1
        poison.started_at = now + timedelta(seconds=1)
        await session.commit()

    async with factory() as session:
        recovered = await lease.recover_jobs(session)
        await session.commit()
    assert recovered == {"interrupted": 0, "delivery_unknown": 0, "recovery_exhausted": 1}

    async with factory() as session:
        failed = await session.get(BackgroundJob, poison.id)
        saved_schedule = await session.get(ScheduleSettings, schedule.id)
        saved_article = await session.get(Article, 7)
        assert failed.state == "failed"
        assert failed.error_code == "worker_recovery_exhausted"
        assert failed.result == {"status": "incomplete"}
        assert saved_schedule.is_running is False
        assert saved_schedule.last_status == "failed"
        assert saved_schedule.last_error == "Worker restart limit reached"
        assert saved_schedule.next_run > datetime.utcnow()
        assert saved_article.processing_state == "retry_wait"
        assert saved_article.processing_attempts == 4
        assert saved_article.summary_processed is True
        assert saved_article.category_processed is True
        assert saved_article.ad_processed is True
        claimed = await claim_next_job(session, lease)
        await session.commit()
        assert claimed.id == next_job.id
    await lease.release()


@pytest.mark.asyncio
async def test_local_database_timezone_keeps_worker_heartbeat_utc(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    await lease.connection.execute(text("SET TIME ZONE 'Europe/Belgrade'"))
    await lease.connection.commit()
    await lease.heartbeat()
    async with factory() as session:
        status = await session.get(BackgroundWorkerStatus, 1)
        assert status.heartbeat_at is not None
        assert abs((datetime.utcnow() - status.heartbeat_at).total_seconds()) < 5
        assert (await read_worker_status(session))["healthy"] is True
    await lease.release()


@pytest.mark.asyncio
async def test_direct_commit_is_fenced_after_worker_attempt_changes(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    fenced_factory = sessionmaker(engine, class_=FencedAsyncSession, expire_on_commit=False)
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(session, "news_processing", {}, "worker-fence:one")
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()

    stale = JobExecutionFence(lease, job.id, claimed.attempt_generation)
    async with factory() as session:
        row = await session.get(BackgroundJob, job.id)
        row.state = "interrupted"
        await session.commit()
    async with factory() as session:
        resumed = await claim_next_job(session, lease)
        await session.commit()
        assert resumed.attempt_generation == claimed.attempt_generation + 1

    token = __import__("news_aggregator.services.job_fence", fromlist=["current_job_fence"]).current_job_fence.set(stale)
    try:
        async with fenced_factory() as session:
            row = await session.get(BackgroundJob, job.id)
            row.result = {"status": "success"}
            with pytest.raises(StaleWorker):
                await session.commit()
            await session.rollback()
    finally:
        from news_aggregator.services.job_fence import current_job_fence
        current_job_fence.reset(token)
    await lease.release()


@pytest.mark.asyncio
async def test_inherited_detached_task_cannot_commit_after_job_finishes(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    fenced_factory = sessionmaker(engine, class_=FencedAsyncSession, expire_on_commit=False)
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    async with factory() as session:
        job = await enqueue_job_in_session(session, "news_processing", {}, "worker-fence:late")
        await session.commit()
    async with factory() as session:
        claimed = await claim_next_job(session, lease)
        await session.commit()

    ready = asyncio.Event()
    allow_commit = asyncio.Event()
    outcome = []

    async def delayed_write():
        ready.set()
        await allow_commit.wait()
        async with fenced_factory() as session:
            await session.execute(update(BackgroundJob).where(
                BackgroundJob.id == job.id
            ).values(result={"status": "success"}))
            from news_aggregator.services.job_fence import current_job_fence
            try:
                await session.commit()
            except RuntimeError as exc:
                if "no longer active" not in str(exc):
                    raise
                outcome.append("blocked")
                return
        if current_job_fence.get() is None:
            outcome.append("blocked")
            return
        outcome.append("unexpected")

    async with job_execution(JobExecutionFence(lease, job.id, claimed.attempt_generation)):
        child = asyncio.create_task(delayed_write())
        await ready.wait()
    allow_commit.set()
    await child
    assert outcome == ["blocked"]
    await lease.release()
