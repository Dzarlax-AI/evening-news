"""API boundaries that must stay lightweight when a worker owns execution."""

from datetime import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException


@pytest.mark.asyncio
async def test_worker_mode_source_creation_skips_network_connection_test(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "admin_password", "fixture-admin-password")
    sources_module = importlib.import_module("news_aggregator.api.sources_router")
    CreateSourceRequest, create_source = sources_module.CreateSourceRequest, sources_module.create_source
    calls = {}

    class FakeSourceManager:
        async def create_source(self, db, **kwargs):
            calls.update(kwargs)
            return SimpleNamespace(
                id=8, name=kwargs["name"], source_type=kwargs["source_type"],
                url=kwargs["url"], enabled=True, created_at=datetime(2026, 1, 1),
            )

    monkeypatch.setattr(sources_module, "SourceManager", FakeSourceManager)
    db = SimpleNamespace(commit=AsyncMock())
    result = await create_source(CreateSourceRequest(
        name="Fixture source", source_type="rss", url="https://fixture.invalid/feed",
    ), db)

    assert calls["skip_connection_test"] is True
    assert result["id"] == 8


@pytest.mark.asyncio
async def test_worker_mode_source_test_preserves_not_found_http_status(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "admin_password", "fixture-admin-password")
    test_source = importlib.import_module("news_aggregator.api.sources_router").test_source
    db = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: None)))

    with pytest.raises(HTTPException) as caught:
        await test_source(42, None, db)
    assert caught.value.status_code == 404


@pytest.mark.asyncio
async def test_worker_mode_source_test_preserves_queue_http_errors(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "admin_password", "fixture-admin-password")
    test_source = importlib.import_module("news_aggregator.api.sources_router").test_source
    source = SimpleNamespace(id=42)
    db = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: source)))

    async def reject(*args, **kwargs):
        raise HTTPException(status_code=503, detail="queue full")

    jobs_module = importlib.import_module("news_aggregator.api.background_jobs_router")
    monkeypatch.setattr(jobs_module, "enqueue_from_request", reject)
    with pytest.raises(HTTPException) as caught:
        await test_source(42, None, db)
    assert caught.value.status_code == 503


@pytest.mark.asyncio
async def test_worker_mode_ai_category_analysis_is_rejected_before_db_or_ai(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "admin_password", "fixture-admin-password")
    categories = importlib.import_module("news_aggregator.api.categories_router")
    monkeypatch.setattr(categories, "app_settings", settings)
    monkeypatch.setattr(
        importlib.import_module("news_aggregator.services.ai_client"),
        "get_ai_client",
        lambda: pytest.fail("worker-mode web must not initialize an AI client"),
    )
    db = SimpleNamespace(execute=AsyncMock())
    request = categories.AICategoryAnalysisRequest()
    with pytest.raises(HTTPException) as caught:
        await categories.analyze_categories_with_ai(request, db)
    assert caught.value.status_code == 409
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_mode_keeps_explicit_telegram_connection_diagnostic(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(settings, "admin_password", "fixture-admin-password")
    telegram = importlib.import_module("news_aggregator.api.telegram_router")
    monkeypatch.setattr(telegram, "app_settings", settings)
    service = SimpleNamespace(test_connection=AsyncMock(return_value=True))
    monkeypatch.setattr(
        telegram, "_get_telegram_service_with_db_overrides", AsyncMock(return_value=service)
    )
    result = await telegram.test_telegram_connection(SimpleNamespace())
    assert result["success"] is True
    service.test_connection.assert_awaited_once()


@pytest.mark.asyncio
async def test_generated_idempotency_key_uses_alphanumeric_prefix(monkeypatch):
    import importlib
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    jobs_module = importlib.import_module("news_aggregator.api.background_jobs_router")
    received = {}

    class FakeRequest:
        headers = {}

    async def accept(kind, payload, key):
        received.update(kind=kind, payload=payload, key=key)
        return SimpleNamespace(id="fixture-job", state="queued")

    monkeypatch.setattr(jobs_module, "enqueue_job", accept)
    import secrets
    monkeypatch.setattr(secrets, "token_hex", lambda size: "a" * (size * 2))
    result = await jobs_module.enqueue_from_request("news_processing", {}, FakeRequest())
    assert received["key"].startswith(tuple("0123456789abcdef"))
    assert len(received["key"]) == 48
    assert result["status"] == "queued"
