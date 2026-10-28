"""Real native child with loopback PostgreSQL and a local fake Gemini endpoint."""

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from aiohttp import web
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from news_aggregator.services.ai_runtime import AIRuntime


_ROUTING_DSN_QUERY_KEYS = {"host", "hostaddr", "service", "servicefile"}


def _validate_loopback_test_database_url(database_url):
    parsed = make_url(database_url)
    if parsed.host not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("test database URL must use loopback host")
    if any(key.lower() in _ROUTING_DSN_QUERY_KEYS for key in parsed.query):
        raise ValueError("test database URL cannot override routing through query parameters")
    return parsed


def _integration_settings():
    required = os.environ.get("NEWS_AI_RUNTIME_INTEGRATION_REQUIRED") == "1"
    binary = os.environ.get("NEWS_AI_RUNTIME_BINARY")
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not binary:
        if required:
            pytest.fail("NEWS_AI_RUNTIME_INTEGRATION_REQUIRED=1 but NEWS_AI_RUNTIME_BINARY is unavailable")
        pytest.skip("NEWS_AI_RUNTIME_BINARY is not configured")
    if not database_url:
        if required or os.environ.get("CI") == "true":
            pytest.fail("native AI runtime integration requires a loopback TEST_DATABASE_URL")
        pytest.skip("TEST_DATABASE_URL is not configured")
    try:
        _validate_loopback_test_database_url(database_url)
    except ValueError:
        pytest.fail("native AI runtime integration accepts only direct loopback TEST_DATABASE_URL")
    return binary, database_url


@pytest.mark.parametrize("database_url", [
    "postgresql://fixture:fixture@localhost:5432/testdb?host=remote.example",
    "postgresql://fixture:fixture@127.0.0.1:5432/testdb?host=127.0.0.1&host=remote.example",
    "postgresql://fixture:fixture@localhost:5432/testdb?hostaddr=203.0.113.10",
    "postgresql://fixture:fixture@localhost:5432/testdb?service=remote",
    "postgresql://fixture:fixture@localhost:5432/testdb?servicefile=%2Ftmp%2Fpg_service.conf",
])
def test_python_postgres_fixture_rejects_query_routing_overrides(database_url):
    with pytest.raises(ValueError, match="routing"):
        _validate_loopback_test_database_url(database_url)


def test_python_postgres_fixture_accepts_direct_ipv6_loopback():
    parsed = _validate_loopback_test_database_url(
        "postgresql://fixture:fixture@[::1]:5432/testdb?application_name=fixture"
    )
    assert parsed.host == "::1"


@pytest.mark.asyncio
async def test_native_runtime_metadata_cache_and_usage_store(monkeypatch):
    binary, test_database_url = _integration_settings()
    from news_aggregator.services import ai_runtime as runtime_module

    provider_payloads = []

    async def fake_gemini(request):
        assert request.query.get("key") == "fixture-only-key"
        provider_payloads.append(await request.json())
        result = {
            "optimized_title": "Synthetic title", "summary": "Synthetic summary.",
            "categories": ["Tech"], "category_confidence": 0.72,
            "is_advertisement": False,
        }
        return web.json_response({
            "candidates": [{"content": {"parts": [{"text": json.dumps(result)}]}}],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 2,
                              "cachedContentTokenCount": 1, "totalTokenCount": 8},
        })

    app = web.Application()
    app.router.add_post("/v1/models/{model}:generateContent", fake_gemini)
    provider = web.AppRunner(app)
    await provider.setup()
    site = web.TCPSite(provider, "127.0.0.1", 0)
    await site.start()
    provider_port = site._server.sockets[0].getsockname()[1]

    schema = "ai_runtime_py_" + uuid.uuid4().hex
    admin = create_async_engine(test_database_url)
    schema_url = make_url(test_database_url)
    schema_engine = create_async_engine(
        schema_url,
        connect_args={"server_settings": {"search_path": schema}},
    )
    runtime = None
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with schema_engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE categories (
                    name varchar(50) NOT NULL,
                    display_name varchar(100) NOT NULL,
                    is_local boolean NULL
                )
            """))
            await conn.execute(text("""
                CREATE TABLE category_mapping (
                    ai_category varchar(100) NOT NULL,
                    fixed_category varchar(50) NOT NULL,
                    usage_count integer NULL
                )
            """))
            await conn.execute(text("""
                CREATE TABLE ai_usage_tracking (
                    id bigserial PRIMARY KEY,
                    domain varchar(255) NOT NULL,
                    analysis_type varchar(50) NOT NULL,
                    tokens_used integer,
                    credits_cost numeric(10,4),
                    analysis_result jsonb,
                    created_at timestamp DEFAULT now()
                )
            """))
            await conn.execute(text("""
                INSERT INTO categories(name,display_name,is_local) VALUES
                    ('Business','Business',NULL), ('Local','Local',true), ('Tech','Technology',false)
            """))
            await conn.execute(text("""
                INSERT INTO category_mapping(ai_category,fixed_category,usage_count)
                VALUES ('Synthetic mapping example','Business',50)
            """))

        monkeypatch.setattr(runtime_module.settings, "database_url", schema_url.set(
            drivername="postgresql", query={**schema_url.query, "search_path": schema}
        ).render_as_string(hide_password=False))
        monkeypatch.setattr(runtime_module.settings, "gemini_api_key", "fixture-only-key")
        monkeypatch.setattr(runtime_module.settings, "gemini_api_endpoint",
                            f"http://127.0.0.1:{provider_port}/v1/models")
        monkeypatch.setattr(runtime_module.settings, "summarization_model", "fixture-model")
        monkeypatch.setattr(runtime_module.settings, "api_rate_limit", 3)
        monkeypatch.setattr(runtime_module.settings, "local_category", None)
        monkeypatch.setattr(runtime_module.settings, "ai_input_cost_per_1m", 0.5)
        monkeypatch.setattr(runtime_module.settings, "ai_output_cost_per_1m", 1.5)
        monkeypatch.setattr(runtime_module.settings, "ai_cached_input_cost_per_1m", 0.1)

        runtime = AIRuntime(binary)
        await runtime.start()
        article_url = "https://example.test:8443/story"
        first = await runtime.analyze(
            "Synthetic report", "Synthetic article content long enough for native integration analysis.",
            article_url, None, 100000, "fixture-model", datetime.now(timezone.utc) + timedelta(seconds=60),
        )
        assert first["summary"] == "Synthetic summary."
        assert "Business (Business) — e.g.: Synthetic mapping example" in provider_payloads[0]["contents"][0]["parts"][0]["text"]
        assert "LOCAL CATEGORY RULE" in provider_payloads[0]["contents"][0]["parts"][0]["text"]

        async with schema_engine.begin() as conn:
            await conn.execute(text("INSERT INTO categories(name,display_name,is_local) VALUES ('New','New',false)"))
        await runtime.invalidate_categories()
        second = await runtime.analyze(
            "Synthetic report", "Synthetic article content long enough for native integration analysis.",
            article_url, None, 100000, "fixture-model", datetime.now(timezone.utc) + timedelta(seconds=60),
        )
        assert second["summary"] == "Synthetic summary."
        assert "New" in provider_payloads[1]["contents"][0]["parts"][0]["text"]

        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            async with schema_engine.connect() as conn:
                row = (await conn.execute(text("""
                    SELECT domain, analysis_type, tokens_used
                    FROM ai_usage_tracking ORDER BY id
                """))).all()
            if len(row) >= 2:
                break
            await asyncio.sleep(0.05)
        assert [(item[0], item[1], item[2]) for item in row] == [
            ("example.test:8443", "combined_analysis", 8),
            ("example.test:8443", "combined_analysis", 8),
        ]

        long_url = "https://example.test/" + "x" * 400
        long_result = await runtime.generate(
            mode="text", prompt="fixture prompt", model="fixture-model", schema=None,
            temperature=0.1, max_tokens=20, analysis_type="publication_date",
            domain=long_url, timeout=30,
        )
        assert long_result["choices"][0]["message"]["content"]
        assert len(provider_payloads) == 3
        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            status = await runtime.status()
            if status["usage_failed"] == 1:
                break
            await asyncio.sleep(0.05)
        assert status["usage_failed"] == 1
        async with schema_engine.connect() as conn:
            rows_after_long_url = (await conn.execute(text(
                "SELECT domain FROM ai_usage_tracking ORDER BY id"
            ))).scalars().all()
        assert len(rows_after_long_url) == 2
        assert long_url not in rows_after_long_url

        unicode_domain = "é" * 200  # 200 PostgreSQL characters, 400 UTF-8 bytes.
        unicode_result = await runtime.generate(
            mode="text", prompt="fixture prompt", model="fixture-model", schema=None,
            temperature=0.1, max_tokens=20, analysis_type="publication_date",
            domain=unicode_domain, timeout=30,
        )
        assert unicode_result["choices"][0]["message"]["content"]
        assert len(provider_payloads) == 4
        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            async with schema_engine.connect() as conn:
                usage_rows = (await conn.execute(text(
                    "SELECT domain, analysis_type FROM ai_usage_tracking ORDER BY id"
                ))).all()
            if len(usage_rows) >= 3:
                break
            await asyncio.sleep(0.05)
        assert usage_rows[-1] == (unicode_domain, "publication_date")
    finally:
        if runtime is not None:
            await runtime.stop()
        await schema_engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()
        await provider.cleanup()
