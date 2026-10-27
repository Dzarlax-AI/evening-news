"""Database-backed tests for the actual article ingestion migration fence."""

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

os.environ.setdefault("ADMIN_PASSWORD", "fixture-only")

from news_aggregator.migrations.article_ingestion import ArticleIngestionMigration
from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
from news_aggregator.migrations.job_executor import JobExecutorMigration
from news_aggregator.models import ArticleIngestionRun, SourceCollectionState
from news_aggregator.executor import ArticleIngestionCallbackFence, _source_fingerprint
from news_aggregator.services.job_fence import JobExecutionFence, current_job_fence
from news_aggregator.services.background_jobs import validate_payload
from news_aggregator.services.domain_fence import ExecutionContext, RemoteDomainFence, parse_envelope
from news_aggregator.services import extraction_memory as memory_module
from news_aggregator.services.extraction_memory import ExtractionMemoryEntry, ExtractionMemoryService
from news_aggregator.sources.ai_page_analyzer import PageAnalysis


def _url():
    value = os.environ.get("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    parsed = make_url(value)
    if parsed.host not in {"localhost", "127.0.0.1"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    return parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


@pytest.fixture
async def ingestion_schema():
    schema = "test_article_ingestion_" + uuid.uuid4().hex
    role = "ingest_runtime_" + uuid.uuid4().hex[:16]
    admin = create_async_engine(_url())
    app = create_async_engine(
        _url(), connect_args={"server_settings": {"search_path": schema}}
    )
    factory = sessionmaker(app, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            await conn.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
        async with app.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE sources (
                    id SERIAL PRIMARY KEY, name TEXT NOT NULL, source_type TEXT NOT NULL,
                    url TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    config JSONB NOT NULL DEFAULT '{}'::jsonb, last_fetch TIMESTAMP
                )
            """))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
            await JobExecutorMigration().execute(session)
            await ArticleIngestionMigration().execute(session)
        async with app.begin() as conn:
            await conn.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
            await conn.execute(text(f"""
                GRANT EXECUTE ON FUNCTION "{schema}".fence_article_ingestion_attempt(
                    UUID,UUID,INTEGER,TEXT,UUID,INTEGER,INTEGER,UUID,TIMESTAMP) TO "{role}"
            """))
        yield schema, role, app
    finally:
        await app.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await conn.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        await admin.dispose()


async def _seed_fence(conn, kind="news_processing", payload="{}", ttl_seconds=90):
    job, owner, executor = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    attempt, generation = 3, 7
    deadline = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=ttl_seconds)
    pid = await conn.scalar(text("SELECT pg_backend_pid()"))
    await conn.execute(text("""
        UPDATE background_worker_status
        SET status='running',generation=:generation,owner_id=:owner,
            lock_backend_pid=:pid,heartbeat_at=(clock_timestamp() AT TIME ZONE 'UTC')
        WHERE id=1
    """), {"generation": generation, "owner": owner, "pid": pid})
    await conn.execute(text("""
        INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation)
            VALUES(:job,:kind,CAST(:payload AS jsonb),'fence-' || gen_random_uuid()::text,'running',:generation,:attempt)
    """), {"job": job, "kind": kind, "payload": payload,
           "generation": generation, "attempt": attempt})
    await conn.execute(text("""
        INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,
            executor_instance_id,owner_id,worker_generation,deadline_at,state)
        VALUES(:job,:attempt,:hash,:executor,:owner,:generation,:deadline,'running')
    """), {"job": job, "attempt": attempt, "hash": "a" * 64,
           "executor": executor, "owner": owner, "generation": generation,
           "deadline": deadline})
    await conn.execute(text(
        "SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()))"
    ))
    await conn.execute(text(
        "SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # 1162757938)"
    ))
    return {
        "run": uuid.uuid4(), "job": job, "attempt": attempt, "hash": "b" * 64,
        "owner": owner, "generation": generation, "pid": pid,
        "executor": executor, "deadline": deadline, "kind": kind,
        "payload": json.loads(payload),
    }


async def _call_fence(conn, data):
    return await conn.execute(text("""
        SELECT * FROM fence_article_ingestion_attempt(
          :run,:job,:attempt,:hash,:owner,:generation,:pid,:executor,:deadline)
    """), data)


def _callback_domain_fence(params):
    envelope = {
        "version": 1, "job_id": str(params["job"]), "attempt_generation": params["attempt"],
        "owner_id": str(params["owner"]), "generation": params["generation"],
        "backend_pid": params["pid"], "executor_instance_id": str(params["executor"]),
        "deadline_at": params["deadline"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    context = ExecutionContext(
        envelope=parse_envelope(envelope), executor_backend_pid=params["pid"],
        kind=params["kind"], payload=validate_payload(params["kind"], params["payload"]),
    )
    parent = RemoteDomainFence(context)
    active = JobExecutionFence(
        parent, params["job"], params["attempt"], executor_instance_id=params["executor"],
    )
    return parent, active


async def _prepare_callback_memory(engine):
    from news_aggregator.services.article_ingestion_client import _run_id

    lease_conn = await engine.connect()
    async with lease_conn.begin():
        params = await _seed_fence(lease_conn)
    run_id = uuid.UUID(_run_id(params["job"], params["attempt"]))
    async with engine.begin() as conn:
        await conn.execute(text("""
            INSERT INTO article_ingestion_runs(
                id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,
                backend_pid,executor_instance_id,deadline_at,state
            ) VALUES(:run,:job,:attempt,:hash,:owner,:generation,:pid,:executor,:deadline,'running')
        """), {**params, "run": run_id})
        await conn.execute(text("""
            CREATE TABLE extraction_patterns(
                domain TEXT, selector_pattern TEXT, extraction_strategy TEXT,
                quality_score_avg DOUBLE PRECISION, discovered_by TEXT,
                success_count INTEGER DEFAULT 0, failure_count INTEGER DEFAULT 0, updated_at TIMESTAMP,
                UNIQUE(domain,selector_pattern,extraction_strategy)
            )
        """))
        await conn.execute(text("""
            INSERT INTO sources(id,name,source_type,url,enabled,config)
            VALUES(7,'Fixture monitor','custom','https://fixture.invalid/list',true,'{"selector": "original"}')
        """))
        source = (await conn.execute(text("""
            SELECT name,source_type,url,enabled,config::text AS config_text FROM sources WHERE id=7
        """))).mappings().one()
    fingerprint = _source_fingerprint(7, source["name"], source["source_type"], source["url"],
                                      source["enabled"], source["config_text"])
    parent, context = _callback_domain_fence(params)
    callback_fence = ArticleIngestionCallbackFence(
        parent, run_id=run_id, executor_instance_id=params["executor"], context=context,
        source_id=7, source_fingerprint=fingerprint,
    )
    context.lease = callback_fence
    return lease_conn, params, run_id, context, fingerprint


@pytest.mark.asyncio
async def test_actual_migration_fence_locks_and_runtime_role_is_narrow(ingestion_schema):
    schema, role, engine = ingestion_schema
    async with engine.connect() as conn:
        async with conn.begin():
            params = await _seed_fence(conn)
            acl = await conn.scalar(text("""
                SELECT EXISTS (
                  SELECT 1 FROM pg_proc p, LATERAL aclexplode(
                    COALESCE(p.proacl,acldefault('f',p.proowner))) acl
                  WHERE p.proname='fence_article_ingestion_attempt' AND acl.grantee=0
                    AND acl.privilege_type='EXECUTE'
                    AND p.pronamespace=current_schema()::regnamespace
                )
            """))
            assert acl is False
            await conn.execute(text(f'SET LOCAL ROLE "{role}"'))
            with pytest.raises(Exception):
                async with conn.begin_nested():
                    await conn.execute(text("UPDATE background_jobs SET state='failed' WHERE id=:job"), params)
            row = (await _call_fence(conn, params)).one()
            assert str(row.run_id) == str(params["run"])


@pytest.mark.asyncio
async def test_actual_migration_fence_rejects_digest_processing_disabled(ingestion_schema):
    _, _, engine = ingestion_schema
    async with engine.connect() as conn:
        async with conn.begin():
            params = await _seed_fence(conn, "news_digest", '{"run_processing":false}')
            with pytest.raises(Exception):
                await _call_fence(conn, params)


@pytest.mark.asyncio
async def test_actual_migration_fence_rechecks_deadline_after_row_lock_wait(ingestion_schema):
    schema, _, engine = ingestion_schema
    blocker = await engine.connect()
    caller = await engine.connect()
    try:
        async with blocker.begin():
            params = await _seed_fence(blocker, ttl_seconds=1)
            # Hold the worker lease row while the function begins; its deadline
            # expires during this wait, so a pre-lock clock sample would be unsafe.
            await blocker.execute(text("SELECT id FROM background_worker_status WHERE id=1 FOR UPDATE"))
            task = asyncio.create_task(_call_fence(caller, params))
            await asyncio.sleep(1.2)
        with pytest.raises(Exception):
            await task
    finally:
        await caller.close()
        await blocker.close()


@pytest.mark.asyncio
async def test_cancelled_ingestion_callback_cannot_publish_page_patterns(ingestion_schema, monkeypatch):
    schema, _, engine = ingestion_schema
    lease_conn = await engine.connect()
    try:
        async with lease_conn.begin():
            params = await _seed_fence(lease_conn)
        from news_aggregator.services.article_ingestion_client import _run_id
        run_id = uuid.UUID(_run_id(params["job"], params["attempt"]))
        async with engine.begin() as conn:
            await conn.execute(text("""
                INSERT INTO article_ingestion_runs(
                    id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,
                    backend_pid,executor_instance_id,deadline_at,state
                ) VALUES(:run,:job,:attempt,:hash,:owner,:generation,:pid,:executor,:deadline,'running')
            """), {**params, "run": run_id})
            await conn.execute(text("""
                CREATE TABLE extraction_patterns(
                    domain TEXT, selector_pattern TEXT, extraction_strategy TEXT,
                    quality_score_avg DOUBLE PRECISION, discovered_by TEXT,
                    success_count INTEGER DEFAULT 0, failure_count INTEGER DEFAULT 0, updated_at TIMESTAMP,
                    UNIQUE(domain,selector_pattern,extraction_strategy)
                )
            """))
            await conn.execute(text("""
                INSERT INTO sources(id,name,source_type,url,enabled,config)
                VALUES(7,'Fixture monitor','custom','https://fixture.invalid/list',true,'{"selector": "original"}')
            """))
            source = (await conn.execute(text("""
                SELECT name,source_type,url,enabled,config::text AS config_text FROM sources WHERE id=7
            """))).mappings().one()
            fingerprint = _source_fingerprint(7, source["name"], source["source_type"], source["url"],
                                              source["enabled"], source["config_text"])

        async with engine.begin() as conn:
            await conn.execute(text("UPDATE article_ingestion_runs SET state='cancelled' WHERE id=:run"),
                               {"run": run_id})

        parent, context = _callback_domain_fence(params)
        callback_fence = ArticleIngestionCallbackFence(
            parent, run_id=run_id, executor_instance_id=params["executor"], context=context,
            source_id=7, source_fingerprint=fingerprint,
        )
        context.lease = callback_fence
        token = current_job_fence.set(context)
        service = ExtractionMemoryService()
        service._initialized = True
        kept = ExtractionMemoryEntry(
            domain="kept.test", selector_pattern=".known", extraction_strategy="source_title",
            success_count=0, failure_count=0, success_rate=70.0, quality_score_avg=0.7,
            discovered_by="ai",
        )
        service._patterns = {"kept.test": [kept]}
        service._ensure_initialized = AsyncMock()
        factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        monkeypatch.setattr(memory_module, "AsyncSessionLocal", factory)
        try:
            recorded = await service._record_fenced_page_structure(
                "new.test", PageAnalysis(["article"], ["h1"], [], {}, "news", "fixture", {}), context,
            )
        finally:
            current_job_fence.reset(token)
        assert recorded is False
        assert service._patterns == {"kept.test": [kept]}
        async with engine.connect() as conn:
            count = await conn.scalar(text("SELECT count(*) FROM extraction_patterns"))
        assert count == 0
    finally:
        await lease_conn.close()


@pytest.mark.parametrize("change", ["config", "url", "disabled", "deleted"])
@pytest.mark.asyncio
async def test_callback_rechecks_source_after_study_before_pattern_write(ingestion_schema, monkeypatch, change):
    _, _, engine = ingestion_schema
    lease_conn, _, _, context, _ = await _prepare_callback_memory(engine)
    try:
        # This mutation stands in for a source edit while the selector analyzer
        # was awaited; the publication fence must re-read after that await.
        async with engine.begin() as conn:
            if change == "config":
                await conn.execute(text("UPDATE sources SET config='{" + '"selector":"edited"' + "}'::jsonb WHERE id=7"))
            elif change == "url":
                await conn.execute(text("UPDATE sources SET url='https://fixture.invalid/edited' WHERE id=7"))
            elif change == "disabled":
                await conn.execute(text("UPDATE sources SET enabled=false WHERE id=7"))
            elif change == "deleted":
                await conn.execute(text("DELETE FROM sources WHERE id=7"))

        service = ExtractionMemoryService()
        service._initialized = True
        service._ensure_initialized = AsyncMock()
        original = {"kept.test": [ExtractionMemoryEntry(
            domain="kept.test", selector_pattern=".known", extraction_strategy="source_title",
            success_rate=70.0, quality_score_avg=0.7, discovered_by="ai",
        )]}
        service._patterns = original
        factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        monkeypatch.setattr(memory_module, "AsyncSessionLocal", factory)
        token = current_job_fence.set(context)
        try:
            recorded = await service._record_fenced_page_structure(
                "new.test", PageAnalysis(["article"], ["h1"], [], {}, "news", "fixture", {}), context,
            )
        finally:
            current_job_fence.reset(token)
        assert recorded is False
        assert service._patterns == original
        async with engine.connect() as conn:
            count = await conn.scalar(text("SELECT count(*) FROM extraction_patterns"))
        assert count == 0
    finally:
        await lease_conn.close()


@pytest.mark.asyncio
async def test_fenced_selector_cache_waits_for_database_commit(ingestion_schema, monkeypatch):
    _, _, engine = ingestion_schema
    lease_conn, _, _, context, _ = await _prepare_callback_memory(engine)
    commit_entered, allow_commit = asyncio.Event(), asyncio.Event()
    try:
        original_save = ExtractionMemoryService._save_ai_discovery_in_session

        async def blocked_save(session, entry):
            await original_save(session, entry)
            commit_entered.set()
            await allow_commit.wait()

        monkeypatch.setattr(ExtractionMemoryService, "_save_ai_discovery_in_session", staticmethod(blocked_save))

        class BlockedCommitSession(AsyncSession):
            async def commit(self):
                commit_entered.set()
                await allow_commit.wait()
                await super().commit()

        factory = sessionmaker(engine, class_=BlockedCommitSession, expire_on_commit=False)
        monkeypatch.setattr(memory_module, "AsyncSessionLocal", factory)
        service = ExtractionMemoryService()
        service._initialized = True
        service._ensure_initialized = AsyncMock()
        service._patterns = {"kept.test": [ExtractionMemoryEntry(
            domain="kept.test", selector_pattern=".known", extraction_strategy="source_title",
            success_rate=70.0, quality_score_avg=0.7, discovered_by="ai",
        )]}
        before = service._patterns
        token = current_job_fence.set(context)
        task = asyncio.create_task(service._record_fenced_page_structure(
            "new.test", PageAnalysis(["article"], ["h1"], [], {}, "news", "fixture", {}), context,
        ))
        try:
            await asyncio.wait_for(commit_entered.wait(), timeout=2)
            assert service._patterns is before
            async with engine.connect() as conn:
                count = await conn.scalar(text("SELECT count(*) FROM extraction_patterns"))
            assert count == 0, "uncommitted selector rows leaked to another connection"
        finally:
            allow_commit.set()
            current_job_fence.reset(token)
        assert await task is True
        assert "new.test" in service._patterns
    finally:
        allow_commit.set()
        await lease_conn.close()


@pytest.mark.asyncio
async def test_cancel_between_pattern_commit_and_cache_publication_keeps_cache_old(ingestion_schema, monkeypatch):
    _, _, engine = ingestion_schema
    lease_conn, _, run_id, context, _ = await _prepare_callback_memory(engine)
    publication_fence_entered, continue_publication = asyncio.Event(), asyncio.Event()
    try:
        original_fence = context.lease.transaction_fence
        calls = 0

        @asynccontextmanager
        async def pause_before_second_fence(session, job_id, attempt_generation):
            nonlocal calls
            calls += 1
            if calls == 2:
                publication_fence_entered.set()
                await continue_publication.wait()
            async with original_fence(session, job_id, attempt_generation):
                yield

        context.lease.transaction_fence = pause_before_second_fence
        service = ExtractionMemoryService()
        service._initialized = True
        service._ensure_initialized = AsyncMock()
        original = {"kept.test": [ExtractionMemoryEntry(
            domain="kept.test", selector_pattern=".known", extraction_strategy="source_title",
            success_rate=70.0, quality_score_avg=0.7, discovered_by="ai",
        )]}
        service._patterns = original
        factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        monkeypatch.setattr(memory_module, "AsyncSessionLocal", factory)
        token = current_job_fence.set(context)
        task = asyncio.create_task(service._record_fenced_page_structure(
            "new.test", PageAnalysis(["article"], ["h1"], [], {}, "news", "fixture", {}), context,
        ))
        try:
            await asyncio.wait_for(publication_fence_entered.wait(), timeout=2)
            async with engine.begin() as conn:
                await conn.execute(text("UPDATE article_ingestion_runs SET state='cancelled' WHERE id=:run"),
                                   {"run": run_id})
            continue_publication.set()
            assert await task is False
        finally:
            continue_publication.set()
            current_job_fence.reset(token)

        assert service._patterns is original
        async with engine.connect() as conn:
            count = await conn.scalar(text("SELECT count(*) FROM extraction_patterns"))
        # The pattern transaction linearized before cancellation; the second
        # receipt fence must still prevent a stale cache publication.
        assert count > 0
    finally:
        continue_publication.set()
        await lease_conn.close()


@pytest.mark.asyncio
async def test_create_all_article_ingestion_timestamp_defaults_are_utc_for_non_utc_session():
    schema = "test_article_ingestion_defaults_" + uuid.uuid4().hex
    admin = create_async_engine(_url())
    engine = create_async_engine(
        _url(), connect_args={
            "server_settings": {
                "search_path": schema,
                "timezone": "America/Los_Angeles",
            }
        }
    )
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE background_jobs (id UUID PRIMARY KEY)
            """))
            await conn.execute(text("""
                CREATE TABLE sources (id SERIAL PRIMARY KEY)
            """))
            await conn.run_sync(lambda sync: ArticleIngestionRun.__table__.create(sync))
            await conn.run_sync(lambda sync: SourceCollectionState.__table__.create(sync))
            job_id, run_id = uuid.uuid4(), uuid.uuid4()
            await conn.execute(text("INSERT INTO background_jobs(id) VALUES(:id)"), {"id": job_id})
            await conn.execute(text("INSERT INTO sources(id) VALUES(1)"))
            run_times = (await conn.execute(text("""
                INSERT INTO article_ingestion_runs
                    (id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,
                     backend_pid,executor_instance_id,deadline_at)
                VALUES (:id,:job,1,:digest,:owner,1,1,:executor,:deadline)
                RETURNING started_at,updated_at
            """), {
                "id": run_id, "job": job_id, "digest": "a" * 64,
                "owner": uuid.uuid4(), "executor": uuid.uuid4(),
                "deadline": datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=2),
            })).one()
            state_updated = await conn.scalar(text("""
                INSERT INTO source_collection_state(source_id,config_sha256)
                VALUES(1,:digest) RETURNING updated_at
            """), {"digest": "b" * 64})
            utc_now = await conn.scalar(text("SELECT timezone('UTC', now())"))
        for value in (*run_times, state_updated):
            assert abs((value - utc_now).total_seconds()) < 5
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()
