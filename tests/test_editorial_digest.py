"""Synthetic native editorial fixture and migration acceptance."""
import importlib.util
import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from news_aggregator.migrations.editorial_digest import EditorialDigestMigration

def test_consolidated_fixture_accepts_editorial_schema_and_projects_its_counter():
    fixture_path = Path(__file__).parent / "fixtures/go-scheduler/fake_services.py"
    spec = importlib.util.spec_from_file_location("go_scheduler_fake_services", fixture_path)
    assert spec is not None and spec.loader is not None
    fake = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fake)
    with fake.LOCK:
        before = fake.COUNTERS["editorial_gemini"]
    server = ThreadingHTTPServer(("127.0.0.1", 0), fake.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    snapshot = {"articles": [{
        "id": "1", "title": "Короткая новость",
        "evidence_spans": [{"id": "span-1-fixture-0001", "text": "Точный фрагмент источника."}],
    }]}
    payload = {
        "generationConfig": {"responseSchema": {"properties": {"article_stories": {}}}},
        "contents": [{"parts": [{"text": "СНИМОК (недоверенные данные):\n"
                                     + json.dumps(snapshot, ensure_ascii=False)}]}],
    }
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        request = Request(base + "/v1/models/fixture-model:generateContent?key=fixture",
                          data=json.dumps(payload, ensure_ascii=False).encode(),
                          headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=5) as response:
            body = json.load(response)
        draft = json.loads(body["candidates"][0]["content"]["parts"][0]["text"])
        assert body["candidates"][0]["finishReason"] == "STOP"
        assert draft["article_stories"]["1"]["span_id"] == "span-1-fixture-0001"
        assert draft["brief_stories"] == []
        with urlopen(base + "/__gate/counters", timeout=5) as response:
            counters = json.load(response)
        assert counters["providers"]["editorial_gemini"] == before + 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)



def _direct_loopback_database(url):
    parsed = make_url(url)
    return (parsed.host in {"127.0.0.1", "localhost", "::1"}
            and not any(key.lower() in {"host", "hostaddr", "service", "servicefile"}
                        for key in parsed.query))


def test_editorial_fixture_rejects_database_routing_overrides():
    assert _direct_loopback_database("postgresql+asyncpg://fixture@127.0.0.1/db")
    for query in ("host=remote", "HOSTADDR=203.0.113.10", "service=remote", "servicefile=/tmp/pg_service.conf", "host=127.0.0.1&host=remote"):
        assert not _direct_loopback_database("postgresql+asyncpg://fixture@127.0.0.1/db?" + query)


@pytest.fixture
async def editorial_pg():
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        if os.environ.get("NEWS_EDITORIAL_INTEGRATION_REQUIRED") == "1":
            raise RuntimeError("required editorial integration needs TEST_DATABASE_URL")
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if not _direct_loopback_database(database_url):
        raise RuntimeError("Editorial persistence tests require direct loopback-only PostgreSQL")
    schema = "test_editorial_" + uuid.uuid4().hex
    admin = create_async_engine(database_url)
    engine = create_async_engine(database_url, connect_args={"server_settings": {"search_path": schema}})
    factory = sessionmaker(engine, class_=__import__("sqlalchemy.ext.asyncio", fromlist=["AsyncSession"]).AsyncSession,
                          expire_on_commit=False)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with factory() as session:
            migration = EditorialDigestMigration()
            await migration.execute(session)
            assert not await migration.check_needed(session)
            await session.execute(text("""
                CREATE TABLE sources (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL)
            """))
            await session.execute(text("""
                CREATE TABLE articles (
                    id INTEGER PRIMARY KEY, source_id INTEGER, title TEXT NOT NULL, url TEXT NOT NULL,
                    summary TEXT, content TEXT, fetched_at TIMESTAMP NOT NULL,
                    processing_state VARCHAR(20) NOT NULL, is_advertisement BOOLEAN
                )
            """))
            await session.execute(text("""
                CREATE TABLE categories (
                    id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL, display_name VARCHAR(100) NOT NULL
                )
            """))
            await session.execute(text("""
                CREATE TABLE article_categories (
                    article_id INTEGER NOT NULL, category_id INTEGER NOT NULL, confidence DOUBLE PRECISION
                )
            """))
            today = datetime.now(timezone.utc).date()
            await session.execute(text("INSERT INTO sources(id,name) VALUES (1,'Fixture News')"))
            await session.execute(text("INSERT INTO categories(id,name,display_name) VALUES (4,'Tech','Tech'),(8,'Business','Business')"))
            await session.execute(text("""
                INSERT INTO articles(id,source_id,title,url,summary,content,fetched_at,processing_state,is_advertisement)
                VALUES
                  (101,1,'Мост построят','https://source.example/101#details','Иной прежний пересказ.',
                   'Город сообщил о начале строительства моста через реку.',:fetched,'completed',FALSE),
                  (102,1,'Новый проект','https://source.example/102','Другой текст, не являющийся источником.',
                   'Компания объявила о запуске независимого проекта.',:fetched,'completed',FALSE),
                  (103,1,'Реклама','https://source.example/103','',
                   'Рекламный материал.',:fetched,'completed',TRUE),
                  (104,1,'В обработке','https://source.example/104','',
                   'Ещё не готово.',:fetched,'pending',FALSE),
                  (105,1,'Без категории','https://source.example/105','',
                   'Завершённый материал без категории.',:fetched,'completed',FALSE)
            """), {"fetched": datetime.combine(today, datetime.min.time()) + timedelta(hours=12)})
            await session.execute(text("""
                INSERT INTO article_categories(article_id,category_id,confidence) VALUES
                  (101,4,0.81),(101,8,0.94),(102,8,0.94),(102,4,0.94)
            """))
            await session.commit()
        yield engine, factory, schema
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_digest_delivery_migration_with_native_receipts(editorial_pg):
    """Go exercises the exact migration tables, not a parallel test schema."""
    import subprocess
    import sys
    from news_aggregator.migrations.digest_delivery import DigestDeliveryMigration

    _engine, factory, schema = editorial_pg
    async with factory() as session:
        migration = DigestDeliveryMigration()
        assert await migration.check_needed(session)
        await migration.execute(session)
        assert not await migration.check_needed(session)
        await migration.execute(session)
        assert not await migration.check_needed(session)

    env = dict(os.environ)
    env["DIGEST_TEST_SCHEMA"] = schema
    env["DIGEST_PROBE_PYTHON"] = sys.executable
    env["TEST_DATABASE_URL"] = env["TEST_DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://", 1)
    env["GOTOOLCHAIN"] = "go1.25.6"
    completed = subprocess.run(
        ["go", "test", "./telegramdelivery", "-run", "^TestReceiptAndFencePostgres$", "-count=1"],
        cwd=Path(__file__).resolve().parents[1] / "go-domain", env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90, check=False,
    )
    assert completed.returncode == 0, "native receipt test failed on the migrated fixture schema"
