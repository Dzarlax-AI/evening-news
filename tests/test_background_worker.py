"""Runtime fencing checks for the separately deployed background worker."""

import asyncio
import hashlib
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
from news_aggregator.models import BackgroundJob
from news_aggregator.services.background_execution import (
    DurableBackgroundWorker, JobExecutionFence, _bounded_result, job_execution,
    worker_healthcheck,
)
from news_aggregator.services.background_jobs import (
    BackgroundWorkerLease, BackgroundWorkerStatus, StaleWorker,
    claim_next_job, enqueue_job_in_session,
)
from news_aggregator.services.background_jobs import _safe_result
from news_aggregator.models import ScheduleSettings


@pytest.fixture
async def pg_worker_jobs():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    from sqlalchemy.ext.asyncio import create_async_engine
    from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
    schema = "test_worker_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
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
    assert result["status"] == "warning"
    assert _safe_result(result) == result
    persisted = _bounded_result("article_reprocess", {
        **result, "duration_seconds": 1.25,
    })
    assert persisted["article_outcomes_total"] == 100
    assert persisted["article_outcomes_omitted"] == 80
    assert persisted["status"] == "warning"
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
        assert await second.recover_jobs(session) == {"interrupted": 1, "delivery_unknown": 0}
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
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: ScheduleSettings.__table__.create(sync))
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
async def test_worker_due_schedule_enqueue_is_unique_and_finishes_atomically(pg_worker_jobs):
    engine, factory, _, _ = pg_worker_jobs
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: ScheduleSettings.__table__.create(sync))
    lease = BackgroundWorkerLease(engine)
    assert await lease.acquire()
    due = datetime.utcnow() - timedelta(minutes=3)
    async with factory() as session:
        schedule = ScheduleSettings(
            task_name="news_processing", enabled=True, schedule_type="interval",
            timezone="Europe/Belgrade", task_config={"interval_minutes": 60},
            next_run=due, last_status="success",
        )
        session.add(schedule)
        await session.commit()

    worker = DurableBackgroundWorker(db_engine=engine, session_factory=factory)
    worker.lease = lease
    await worker._enqueue_due_schedules()
    await worker._enqueue_due_schedules()
    async with factory() as session:
        jobs = (await session.execute(select(BackgroundJob))).scalars().all()
        saved_schedule = await session.get(ScheduleSettings, schedule.id)
        assert len(jobs) == 1
        assert jobs[0].payload["schedule_id"] == schedule.id
        assert jobs[0].payload["scheduled_for"] == due.isoformat()
        schedule_key = f"schedule:{schedule.id}:{due.isoformat()}"
        assert jobs[0].idempotency_key == hashlib.sha256(schedule_key.encode()).hexdigest()
        assert saved_schedule.is_running is True
        assert saved_schedule.last_run is not None
        assert saved_schedule.next_run > datetime.utcnow()
        assert saved_schedule.task_config == {"interval_minutes": 60}
        claimed = await claim_next_job(session, lease)
        await session.commit()
        assert claimed.id == jobs[0].id

    class SuccessfulOrchestrator:
        async def start(self):
            return None

        async def run_full_cycle(self, reservation=None):
            return {"success": True, "articles_processed": 3}

    worker.orchestrator = SuccessfulOrchestrator()
    await worker._run_claim(claimed)
    async with factory() as session:
        saved_job = await session.get(BackgroundJob, claimed.id)
        saved_schedule = await session.get(ScheduleSettings, schedule.id)
        assert saved_job.state == "succeeded"
        assert saved_schedule.is_running is False
        assert saved_schedule.last_status == "success"
        assert saved_schedule.last_success_at is not None
        assert saved_schedule.last_finished_at is not None
        assert saved_schedule.next_run > saved_schedule.last_finished_at
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
