"""PostgreSQL checks for fenced article extraction learning only."""

from datetime import datetime, timedelta, timezone
import os
import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.database import Base
from news_aggregator.executor import ArticleProcessingCallbackFence, _record_article_extraction_feedback
from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
from news_aggregator.migrations.job_executor import JobExecutorMigration
from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
from news_aggregator.models import (
    Article, BackgroundJob, BackgroundJobExecution, BackgroundWorkerStatus,
    ExtractionAttempt, ExtractionPattern, Source,
)
from news_aggregator.services.extraction_memory import ExtractionMemoryEntry
from news_aggregator.services.domain_fence import (
    ExecutionContext, GO_EXECUTION_GUARD_XOR, RemoteDomainFence, parse_envelope,
)


def _database_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    return parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


@pytest.fixture
async def learning_db():
    schema = "test_article_ai_learning_" + uuid.uuid4().hex
    admin = create_async_engine(_database_url())
    engine = create_async_engine(_database_url(), connect_args={
        "server_settings": {"search_path": schema},
    })
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    tables = [Source.__table__, Article.__table__, ExtractionPattern.__table__, ExtractionAttempt.__table__]
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
            await ScheduledJobMetadataMigration().execute(session)
            await JobExecutorMigration().execute(session)
        yield engine, factory
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


class _Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


class _GuardedSession:
    """Use real job/domain tables and stub only the Go run/item claim rows."""

    def __init__(self, factory, run_id, article_id, attempt, fingerprint):
        self.factory = factory
        self.run_id = run_id
        self.article_id = article_id
        self.attempt = attempt
        self.fingerprint = fingerprint

    async def __aenter__(self):
        self.session = self.factory()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self.session.__aexit__(*args)

    @property
    def no_autoflush(self):
        return self.session.no_autoflush

    async def scalar(self, statement, params=None):
        return await self.session.scalar(statement, params or {})

    async def execute(self, statement, params=None):
        sql = str(statement)
        if "fence_article_processing_attempt" in sql:
            return _Result({"run_id": self.run_id, "run_state": "running"})
        if "article_processing_items" in sql:
            return _Result({"article_attempt": self.attempt,
                            "input_fingerprint": self.fingerprint, "state": "running"})
        return await self.session.execute(statement, params or {})

    async def commit(self):
        await self.session.commit()


@pytest.mark.asyncio
async def test_feedback_records_observations_and_selector_learning_without_article_writes(
        learning_db, monkeypatch):
    engine, factory = learning_db
    article_id = 72
    run_id = uuid.uuid4()
    fingerprint = "f" * 64
    async with factory() as session:
        session.add(Article(id=article_id, title="Synthetic title", url="https://example.test/story",
                            content="Original synthetic content", summary="Original synthetic summary",
                            processing_attempts=2, processing_state="pending"))
        session.add(ExtractionPattern(
            domain="example.test", selector_pattern=".learned", extraction_strategy="readability",
            success_count=3, failure_count=0, quality_score_avg=0.8, content_length_avg=180,
            is_stable=True, consecutive_successes=3, consecutive_failures=0,
        ))
        await session.commit()

    import news_aggregator.executor as executor_module
    from news_aggregator.services import domain_stability_tracker, extraction_memory

    job_id, owner_id, executor_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    worker_generation, attempt_generation = 7, 1
    deadline = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=2)
    worker_lock = await engine.connect()
    execution_lock = await engine.connect()
    await worker_lock.execute(text(
        "SELECT pg_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"))
    await worker_lock.commit()
    await execution_lock.execute(text(
        "SELECT pg_advisory_lock(hashtext(current_database()), "
        "hashtext(current_schema()) # :xor)"), {"xor": GO_EXECUTION_GUARD_XOR})
    await execution_lock.commit()
    worker_pid = await worker_lock.scalar(text("SELECT pg_backend_pid()"))
    executor_pid = await execution_lock.scalar(text("SELECT pg_backend_pid()"))

    async with factory() as session:
        status = await session.get(BackgroundWorkerStatus, 1)
        status.owner_id = owner_id
        status.generation = worker_generation
        status.status = "running"
        status.lock_backend_pid = worker_pid
        status.heartbeat_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.add(BackgroundJob(
            id=job_id, kind="news_processing", payload={}, idempotency_key="feedback-fixture",
            state="running", owner_generation=worker_generation,
            attempt_generation=attempt_generation,
        ))
        session.add(BackgroundJobExecution(
            job_id=job_id, attempt_generation=attempt_generation, envelope_sha256="a" * 64,
            executor_instance_id=executor_id, owner_id=owner_id,
            worker_generation=worker_generation, deadline_at=deadline, state="running",
        ))
        await session.commit()

    envelope = {
        "version": 1, "job_id": str(job_id), "attempt_generation": attempt_generation,
        "owner_id": str(owner_id), "generation": worker_generation, "backend_pid": worker_pid,
        "executor_instance_id": str(executor_id),
        "deadline_at": deadline.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    context = ExecutionContext(parse_envelope(envelope), executor_pid, "news_processing", {})
    lease = RemoteDomainFence(context)
    fence = ArticleProcessingCallbackFence(
        lease, run_id=run_id, envelope_digest="a" * 64, article_id=article_id,
        article_attempt=2, input_fingerprint=fingerprint)
    monkeypatch.setattr(executor_module, "AsyncSessionLocal", lambda: _GuardedSession(
        factory, run_id, article_id, 2, fingerprint))

    learned = ExtractionMemoryEntry(
        "example.test", ".learned", "readability", success_count=3, failure_count=0,
        success_rate=100.0, quality_score_avg=0.8, content_length_avg=180,
        is_stable=True, consecutive_successes=3,
    )
    memory = type("Memory", (), {
        "_patterns": {"example.test": [learned]}, "_ensure_initialized": AsyncMock()
    })()
    async def get_memory():
        return memory

    monkeypatch.setattr(extraction_memory, "get_extraction_memory", get_memory)
    tracker = type("Tracker", (), {"update_domain_stats": lambda self, **kwargs: None})()
    async def get_tracker():
        return tracker
    monkeypatch.setattr(domain_stability_tracker, "get_stability_tracker", get_tracker)

    metadata = {
        "url": "https://example.test/story", "method": "readability", "success": True,
        "selector_used": ".fallback", "content_length": 240, "quality_score": 0.9,
        "observations": [
            {"selector": ".learned", "method": "readability", "success": False},
            {"selector": ".learned", "method": "readability", "success": False},
            {"selector": ".fallback", "method": "readability", "success": True},
        ],
    }
    await _record_article_extraction_feedback(
        lease, job_id, attempt_generation, fence, metadata,
        learned_selector=".learned", learned_method="readability")

    async with factory() as session:
        rows = (await session.execute(text("""
            SELECT selector_used,success FROM extraction_attempts ORDER BY id
        """))).all()
        assert rows == [(".fallback", True), (".learned", False),
                        (".learned", False), (".fallback", True)]
        pattern_rows = (await session.execute(text("""
            SELECT selector_pattern,success_count,failure_count,is_stable,
                   consecutive_successes,consecutive_failures
            FROM extraction_patterns ORDER BY selector_pattern
        """))).all()
        assert pattern_rows == [
            (".fallback", 1, 0, False, 1, 0),
            (".learned", 3, 1, False, 0, 1),
        ]
        article = (await session.execute(text("""
            SELECT title,content,summary,processing_attempts,processing_state
            FROM articles WHERE id=:article_id
        """), {"article_id": article_id})).one()
        assert article == ("Synthetic title", "Original synthetic content",
                           "Original synthetic summary", 2, "pending")
    assert learned.failure_count == 1 and learned.is_stable is False
    await worker_lock.close()
    await execution_lock.close()
