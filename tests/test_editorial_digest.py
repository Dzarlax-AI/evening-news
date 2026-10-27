"""Private editorial adapter, immutable persistence, and fake-provider acceptance."""

import asyncio
import importlib.util
import json
import os
import sys
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.migrations.editorial_digest import (
    EditorialDigestMigration, ensure_editorial_digest_migration,
)
from news_aggregator.services import editorial_digest as editorial


def _article(article_id="101", *, content="Источник сообщает точный факт.", category_id="4"):
    return {
        "id": article_id, "title": "Синтетическая новость", "summary": "",
        "content": content, "source_name": "Синтетический источник",
        "url": "https://source.example/story/101", "category_id": category_id,
        "category_name": "Технологии",
    }


def _snapshot(articles=None):
    return {
        "date": "2026-10-27", "cutoff_utc": "2026-10-27T18:00:00.000000Z",
        "site_base_url": "https://news.example",
        "edition_url": "https://news.example/digest/2026-10-27?edition=11111111-1111-5111-8111-111111111111",
        "articles": articles if articles is not None else [_article()],
    }


def test_snapshot_preflight_rejects_overflow_without_truncating_and_maps_only_known_categories():
    value = _snapshot([_article(content="x" * 30001)])
    with pytest.raises(editorial.EditorialOperationError) as exc:
        editorial._validate_snapshot(value)
    assert exc.value.code == "editorial_resource_limit"

    many = _snapshot([_article(str(i + 1)) for i in range(editorial.MAX_ARTICLES + 1)])
    with pytest.raises(editorial.EditorialOperationError) as exc:
        editorial._validate_snapshot(many)
    assert exc.value.code == "editorial_resource_limit"

    assert editorial._category_label("Serbia", "Serbia") == "Сербия"
    assert editorial._category_label("AI", "Artificial Intelligence") == "Искусственный интеллект"
    assert editorial._category_label("Local Reports", "Местные репортажи") == "Местные репортажи"


def test_gemini_endpoint_adapts_models_base_and_rejects_credential_or_path_redirects():
    assert editorial._gemini_endpoint(
        "https://generativelanguage.googleapis.com/v1/models", "gemini-test-v1",
    ) == "https://generativelanguage.googleapis.com/v1/models/gemini-test-v1:generateContent"
    assert editorial._gemini_endpoint(
        "http://127.0.0.1:8123/v1/models", "fake-model",
    ) == "http://127.0.0.1:8123/v1/models/fake-model:generateContent"
    assert editorial._gemini_endpoint(
        "http://fake:8090/v1/models", "fake-model",
    ) == "http://fake:8090/v1/models/fake-model:generateContent"
    for endpoint in (
        "https://user:secret@example.com/v1/models",
        "https://example.com/v1/models?key=embedded",
        "https://example.com/not-models",
        "ftp://example.com/v1/models",
    ):
        with pytest.raises(editorial.EditorialOperationError):
            editorial._gemini_endpoint(endpoint, "gemini-test-v1")


def test_editorial_provider_endpoint_comes_only_from_trusted_settings(monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(settings, "digest_model", "fixture-model")
    monkeypatch.setattr(settings, "gemini_api_endpoint", "http://fake:8090/v1/models")
    env = editorial._child_environment()
    untrusted_request = {"operation": "generate", "locale": "ru", "endpoint": "https://untrusted.invalid"}
    assert env["GEMINI_ENDPOINT"] == "http://fake:8090/v1/models/fixture-model:generateContent"
    assert env["GEMINI_ENDPOINT"] != untrusted_request["endpoint"]


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
        "generationConfig": {"responseSchema": {"properties": {"main_stories": {}}}},
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
        assert draft["main_stories"][0]["evidence"][0] == {
            "article_id": "1", "span_id": "span-1-fixture-0001",
        }
        with urlopen(base + "/__gate/counters", timeout=5) as response:
            counters = json.load(response)
        assert counters["providers"]["editorial_gemini"] == before + 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_cli_runner_uses_finite_error_envelope_and_kills_on_deadline(tmp_path, monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(settings, "digest_model", "fake-model")
    monkeypatch.setattr(settings, "gemini_api_endpoint", "https://example.invalid/v1/models")
    failure = tmp_path / "fake-editorial-failure"
    failure.write_text(
        "#!/bin/sh\n"
        f"exec '{sys.executable}' -c \"import json,sys; sys.stdin.buffer.read(); print(json.dumps({{'ok':False,'error_code':'provider_timeout'}})); sys.exit(1)\"\n",
        encoding="utf-8",
    )
    failure.chmod(0o755)
    with pytest.raises(editorial.EditorialOperationError) as exc:
        await editorial.run_editorial_cli(
            {"operation": "generate", "locale": "ru", "snapshot": _snapshot()},
            datetime.now(timezone.utc) + timedelta(seconds=5), binary=str(failure),
        )
    assert exc.value.code == "editorial_provider_timeout"

    slow = tmp_path / "fake-editorial-slow"
    slow.write_text(
        "#!/bin/sh\n"
        f"exec '{sys.executable}' -c \"import sys,time; sys.stdin.buffer.read(); time.sleep(2)\"\n",
        encoding="utf-8",
    )
    slow.chmod(0o755)
    with pytest.raises(editorial.EditorialOperationError) as exc:
        await editorial.run_editorial_cli(
            {"operation": "generate", "locale": "ru", "snapshot": _snapshot()},
            datetime.now(timezone.utc) + timedelta(milliseconds=100), binary=str(slow),
        )
    assert exc.value.code == "editorial_provider_timeout"


@pytest.fixture
async def editorial_pg():
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        if os.environ.get("NEWS_EDITORIAL_INTEGRATION_REQUIRED") == "1":
            raise RuntimeError("required editorial integration needs TEST_DATABASE_URL")
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(database_url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("Editorial persistence tests require loopback-only PostgreSQL")
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


def _fake_gemini_response(snapshot):
    main_stories = []
    brief_stories = []
    for index, article in enumerate(snapshot["articles"]):
        if index < 2:
            main_stories.append({
                "title": article["title"],
                "summary": "Синтетический материал подтверждает событие.",
                "primary_article_id": article["id"], "article_ids": [article["id"]],
                "evidence": [{"article_id": article["id"], "span_id": article["evidence_spans"][0]["id"]}],
            })
        else:
            brief_stories.append({
                "title": article["title"],
                "summary": "Синтетический материал подтверждает событие.",
                "article_id": article["id"], "span_id": article["evidence_spans"][0]["id"],
            })
    return json.dumps({"main_stories": main_stories, "brief_stories": brief_stories, "excluded": []}, ensure_ascii=False)


def _editorial_binary_or_skip():
    binary = os.environ.get("NEWS_EDITORIAL_BINARY")
    if not binary or not Path(binary).is_file():
        if os.environ.get("NEWS_EDITORIAL_INTEGRATION_REQUIRED") == "1":
            raise RuntimeError("required editorial integration needs NEWS_EDITORIAL_BINARY")
        pytest.skip("NEWS_EDITORIAL_BINARY must point to the locally built editorial CLI")
    return binary


def test_required_editorial_integration_fails_without_cli(monkeypatch):
    monkeypatch.setenv("NEWS_EDITORIAL_BINARY", "/missing/news-editorial")
    monkeypatch.setenv("NEWS_EDITORIAL_INTEGRATION_REQUIRED", "1")
    with pytest.raises(RuntimeError, match="required editorial integration needs NEWS_EDITORIAL_BINARY"):
        _editorial_binary_or_skip()


def test_optional_editorial_integration_skips_without_cli(monkeypatch):
    monkeypatch.setenv("NEWS_EDITORIAL_BINARY", "/missing/news-editorial")
    monkeypatch.delenv("NEWS_EDITORIAL_INTEGRATION_REQUIRED", raising=False)
    with pytest.raises(pytest.skip.Exception):
        _editorial_binary_or_skip()


@pytest.mark.asyncio
async def test_actual_go_cli_persists_once_and_reads_best_category_with_fake_gemini(editorial_pg, monkeypatch):
    binary = _editorial_binary_or_skip()
    from news_aggregator.config import settings
    engine, factory, _schema = editorial_pg
    monkeypatch.setattr(editorial, "AsyncSessionLocal", factory)
    monkeypatch.setattr(settings, "editorial_binary", binary)
    monkeypatch.setattr(settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(settings, "digest_model", "fake-model")
    server_state = {"requests": 0}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_POST(self):
            server_state["requests"] += 1
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length))
            prompt = request["contents"][0]["parts"][0]["text"]
            snapshot = json.loads(prompt.split("СНИМОК (недоверенные данные):\n", 1)[1])
            result = _fake_gemini_response(snapshot)
            body = json.dumps({
                "candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": result}]}}],
                "usageMetadata": {"promptTokenCount": 70, "candidatesTokenCount": 40, "totalTokenCount": 110},
            }, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(settings, "gemini_api_endpoint", f"http://127.0.0.1:{server.server_port}/v1/models")

    class Fence:
        job_id = uuid.uuid4()
        attempt_generation = 3

        @asynccontextmanager
        async def transaction_fence(self, session, job_id, attempt_generation):
            assert job_id == self.job_id and attempt_generation == self.attempt_generation
            yield

    fence = Fence()
    record = type("Record", (), {
        "job_id": str(fence.job_id), "attempt_generation": fence.attempt_generation,
        "operation_id": str(uuid.uuid4()), "deadline": datetime.now(timezone.utc) + timedelta(minutes=1),
    })()
    try:
        generated = await editorial.generate_and_persist(record, None, fence, revalidate=lambda: _async_none())
        assert generated == 1 and server_state["requests"] == 1
        async with factory() as session:
            count = await editorial.count_ready_editions(session, datetime.now(timezone.utc).date())
            row = await editorial.latest_ready_edition(session, datetime.now(timezone.utc).date())
            assert count == 1
            assert row["edition"]["locale"] == "ru"
            assert row["provider_usage"]["total_token_count"] == 110
            assert row["edition"]["edition_url"].endswith(f"?edition={row['id']}")
            articles = row["snapshot"]["articles"]
            assert [(a["id"], a["category_id"], a["category_name"]) for a in articles] == [
                ("101", "8", "Бизнес"), ("102", "4", "Технологии"),
            ]
            assert articles[0]["url"] == "https://source.example/101#details"
            assert [story["primary_article_id"] for story in row["edition"]["stories"]] == ["101", "102"]
            quotes = {
                evidence["article_id"]: evidence["quote"]
                for story in row["edition"]["stories"]
                for evidence in story["evidence"]
            }
            assert quotes == {
                "101": "Город сообщил о начале строительства моста через реку.",
                "102": "Компания объявила о запуске независимого проекта.",
            }
            assert all("пересказ" not in quote and "не являющийся" not in quote for quote in quotes.values())
        async def must_not_regenerate(*_args, **_kwargs):
            raise AssertionError("an immutable replay must not call the CLI")
        monkeypatch.setattr(editorial, "run_editorial_cli", must_not_regenerate)
        assert await editorial.generate_and_persist(record, None, fence, revalidate=lambda: _async_none()) == 1
        assert server_state["requests"] == 1
        async with factory() as session:
            with pytest.raises(Exception):
                await session.execute(text("UPDATE editorial_digest_editions SET policy_version='changed'"))
                await session.commit()
            await session.rollback()
            fk_count = await session.scalar(text("""
                SELECT count(*) FROM pg_constraint WHERE conrelid='editorial_digest_editions'::regclass
                  AND contype='f'
            """))
            assert fk_count == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


async def _async_none():
    return None


@pytest.mark.asyncio
async def test_editorial_publish_sends_prebuilt_parts_without_appending_or_real_transport(monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.services import domain_operations as domain

    class FakeService:
        def __init__(self):
            self.sent = []
            self.rich = []

        async def send_message(self, value):
            self.sent.append(value)
            return type("Result", (), {"status_code": 200, "description": None, "__bool__": lambda _self: True})()

        async def send_editorial_message(self, value):
            self.sent.append(value)
            return type("Result", (), {"status_code": 200, "description": None, "__bool__": lambda _self: True})()

        async def send_rich_message(self, value):
            self.rich.append(value)
            return type("Result", (), {"status_code": 200, "description": None, "__bool__": lambda _self: True})()

    service = FakeService()
    token = str(uuid.uuid4())
    record = type("Record", (), {
        "job_id": str(uuid.uuid4()), "attempt_generation": 3,
        "operation_id": str(uuid.uuid4()), "domain_instance_id": str(domain.DOMAIN_INSTANCE_ID),
        "input": {"prepared_token": token}, "deadline": datetime.now(timezone.utc) + timedelta(minutes=1),
    })()
    parts = ['<b><a href="https://news.example/article/101">Заголовок</a></b>',
             '<a href="https://news.example/digest/2026-10-27?edition=fixture">Полный выпуск</a>']
    domain._PREPARED[token] = domain.PreparedDigest(
        record.job_id, record.attempt_generation, record.domain_instance_id, record.deadline,
        {"editorial": True, "rich_html": "<h2>Rich</h2>", "plain_parts": parts,
         "digest_url": "https://news.example/digest/2026-10-27?edition=fixture"}, 100,
    )
    monkeypatch.setattr(domain, "_revalidate_context", lambda *_args: _async_none())
    monkeypatch.setattr(domain, "_telegram_service_with_db_overrides", lambda: _async_value(service))
    monkeypatch.setattr(settings, "telegram_rich_messages_enabled", False)
    result = await domain._publish_digest(record, type("Fence", (), {"context": None})())
    assert result == {"success": True, "parts_sent": 2, "parts_total": 2, "rich_message": False}
    assert service.sent == parts
    assert service.rich == []


@pytest.mark.asyncio
async def test_oversize_local_editorial_rejection_does_not_become_unknown_delivery(monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.services import domain_operations as domain
    from news_aggregator.services.telegram_service import TelegramService

    service = TelegramService.__new__(TelegramService)
    service.api_url = "https://api.telegram.org/bottoken"
    service.chat_id = "@news"
    http_attempts = []

    class UnusedClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, *_args, **_kwargs):
            http_attempts.append(True)
            raise AssertionError("local validation must reject before HTTP")

    monkeypatch.setattr("news_aggregator.services.telegram_service.get_http_client", lambda: UnusedClient())
    token = str(uuid.uuid4())
    record = type("Record", (), {
        "job_id": str(uuid.uuid4()), "attempt_generation": 3,
        "operation_id": str(uuid.uuid4()), "domain_instance_id": str(domain.DOMAIN_INSTANCE_ID),
        "input": {"prepared_token": token}, "deadline": datetime.now(timezone.utc) + timedelta(minutes=1),
    })()
    domain._PREPARED[token] = domain.PreparedDigest(
        record.job_id, record.attempt_generation, record.domain_instance_id, record.deadline,
        {"editorial": True, "plain_parts": ["<b>" + ("я" * 4097) + "</b>"]}, 100,
    )
    monkeypatch.setattr(domain, "_revalidate_context", lambda *_args: _async_none())
    monkeypatch.setattr(domain, "_telegram_service_with_db_overrides", lambda: _async_value(service))
    monkeypatch.setattr(settings, "telegram_rich_messages_enabled", False)
    try:
        result = await domain._publish_digest(record, type("Fence", (), {"context": None})())
        assert result == {"success": False, "parts_sent": 0, "parts_total": 1, "rich_message": False}
        assert http_attempts == []
    finally:
        domain._PREPARED.pop(token, None)


async def _async_value(value):
    return value
