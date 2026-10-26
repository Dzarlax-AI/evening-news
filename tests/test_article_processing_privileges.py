"""Exercise the reviewed processor role against an isolated synthetic schema."""
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.database import Base
from news_aggregator import models  # Register the real application tables.
from news_aggregator.migrations.article_processing import ArticleProcessingMigration

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
async def processing_privileges():
    value = os.environ.get("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL checks")
    parsed = make_url(value)
    if parsed.host not in {"localhost", "127.0.0.1"}:
        raise RuntimeError("Processor privilege tests require loopback PostgreSQL")
    url = parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    suffix = uuid.uuid4().hex
    schema, runtime, owner = "test_processing_role_" + suffix, "proc_rt_" + suffix, "proc_own_" + suffix
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with AsyncSession(engine) as session:
            await ArticleProcessingMigration().execute(session)
        sql = (ROOT / "deploy/news-processing/provision-runtime-role.sql").read_text()
        sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("\\"))
        sql = sql.replace(' :"news_schema"', f' "{schema}"')
        sql = sql.replace("news_processing_fence_owner", owner).replace("news_processing_runtime", runtime)
        async with engine.begin() as conn:
            for statement in sql.split(";"):
                if statement.strip():
                    await conn.execute(text(statement))
            await conn.execute(text("INSERT INTO articles(id,title,url,content) VALUES(1,'Synthetic story','https://fixture.invalid/story','Synthetic text')"))
            await conn.execute(text("INSERT INTO categories(id,name,display_name) VALUES(1,'Tech','Tech')"))
        yield engine, schema, runtime, owner
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await conn.execute(text(f'DROP ROLE IF EXISTS "{runtime}"'))
            await conn.execute(text(f'DROP ROLE IF EXISTS "{owner}"'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_reviewed_role_can_save_processing_and_replace_categories(processing_privileges):
    engine, _, runtime, _ = processing_privileges
    async with engine.begin() as conn:
        await conn.execute(text(f'SET LOCAL ROLE "{runtime}"'))
        await conn.execute(text("SELECT id,kind,payload,state,attempt_generation FROM background_jobs LIMIT 1"))
        await conn.execute(text("UPDATE articles SET summary='Synthetic summary',summary_processed=true WHERE id=1"))
        await conn.execute(text("INSERT INTO article_categories(article_id,category_id,confidence,ai_category) VALUES(1,1,0.9,'synthetic category')"))
        await conn.execute(text("DELETE FROM article_categories WHERE article_id=1"))
        assert await conn.scalar(text("SELECT summary_processed FROM articles WHERE id=1")) is True


@pytest.mark.asyncio
async def test_reviewed_role_has_no_queue_ownership_or_article_deletion_rights(processing_privileges):
    engine, schema, runtime, owner = processing_privileges
    async with engine.connect() as conn:
        flags = (await conn.execute(text("SELECT rolsuper,rolcreaterole,rolcreatedb,rolbypassrls,rolinherit FROM pg_roles WHERE rolname=:name"), {"name": runtime})).one()
        assert not any(flags)
        for table, privilege in [("articles", "DELETE"), ("background_jobs", "UPDATE"),
                                 ("background_worker_status", "UPDATE"),
                                 ("background_job_executions", "UPDATE"), ("daily_summaries", "INSERT")]:
            assert not await conn.scalar(text("SELECT has_table_privilege(:role,:table,:privilege)"),
                                         {"role": runtime, "table": schema + "." + table, "privilege": privilege})
        assert not await conn.scalar(text("SELECT has_column_privilege(:role,:table,'fetched_at','UPDATE')"),
                                     {"role": runtime, "table": schema + ".articles"})
        assert not await conn.scalar(text("SELECT pg_has_role(:runtime,:owner,'MEMBER')"),
                                     {"runtime": runtime, "owner": owner})
        function = schema + ".fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp without time zone)"
        assert await conn.scalar(text("SELECT has_function_privilege(:role,:fn,'EXECUTE')"), {"role": runtime,"fn":function})
        public = await conn.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_proc p, LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE p.oid=CAST(:fn AS regprocedure) AND a.grantee=0 AND a.privilege_type='EXECUTE')"), {"fn": function})
        assert public is False


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,payload,allowed", [
    ("news_processing", "{}", True),
    ("news_digest", '{"run_processing":true}', True),
    ("article_reprocess", '{"article_ids":[1],"force":true}', True),
    ("reprocess_failed", '{"limit":20,"dry_run":true}', True),
    ("news_digest", '{"run_processing":false}', False),
    ("telegram_digest", "{}", False),
])
async def test_runtime_fence_authorizes_only_processing_job_kinds(processing_privileges, kind, payload, allowed):
    from datetime import datetime, timedelta, timezone
    from sqlalchemy.exc import DBAPIError

    engine, _, runtime, _ = processing_privileges
    job, owner, executor_id, run = (uuid.uuid4() for _ in range(4))
    deadline = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=2)
    async with engine.connect() as lease:
        pid = await lease.scalar(text("SELECT pg_backend_pid()"))
        await lease.execute(text("SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()))"))
        await lease.execute(text("INSERT INTO background_worker_status(id,generation,owner_id,lock_backend_pid,heartbeat_at,status) VALUES(1,7,:owner,:pid,clock_timestamp() AT TIME ZONE 'UTC','running')"), {"owner":owner,"pid":pid})
        await lease.execute(text("INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES(:job,:kind,CAST(:payload AS jsonb),:key,'running',7,3)"), {"job":job,"kind":kind,"payload":payload,"key":str(job)})
        await lease.execute(text("INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES(:job,3,:hash,:executor,:owner,7,:deadline,'running')"), {"job":job,"hash":"a"*64,"executor":executor_id,"owner":owner,"deadline":deadline})
        await lease.commit()
        try:
            async with engine.connect() as conn:
                await conn.execute(text(f'SET LOCAL ROLE "{runtime}"'))
                query = text("SELECT * FROM fence_article_processing_attempt(:run,:job,3,:hash,:owner,7,:pid,:executor,:deadline)")
                values = {"run":run,"job":job,"hash":"b"*64,"owner":owner,"pid":pid,"executor":executor_id,"deadline":deadline}
                if allowed:
                    row = (await conn.execute(query, values)).one()
                    assert row.run_id == run and row.run_state == "running"
                    await conn.commit()
                    for field, stale_value in (
                        ("owner", uuid.uuid4()), ("pid", pid + 100000),
                        ("executor", uuid.uuid4()), ("deadline", deadline + timedelta(seconds=1)),
                    ):
                        stale = dict(values, **{field: stale_value})
                        await conn.execute(text(f'SET LOCAL ROLE "{runtime}"'))
                        with pytest.raises(DBAPIError):
                            await conn.execute(query, stale)
                        await conn.rollback()
                else:
                    with pytest.raises(DBAPIError, match="stale_processing_job"):
                        await conn.execute(query, values)
                    await conn.rollback()
        finally:
            await lease.execute(text("SELECT pg_advisory_unlock_all()"))
            await lease.commit()
