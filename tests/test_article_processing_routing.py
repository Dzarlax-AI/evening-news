"""Python entry points queue durable Go-owned article processing jobs."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_processing_run_endpoint_enqueues_worker_job():
    from importlib import import_module
    from news_aggregator.api.processing_router import trigger_processing

    jobs = import_module("news_aggregator.api.background_jobs_router")
    accepted = SimpleNamespace(status="queued", job_id="fixture-job")
    enqueue = AsyncMock(return_value=accepted)
    response_value = SimpleNamespace(status_code=202, accepted=True)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(jobs, "enqueue_from_request", enqueue)
    monkeypatch.setattr(jobs, "accepted_response", lambda result: response_value)
    request = object()
    try:
        assert await trigger_processing(request) is response_value
        enqueue.assert_awaited_once_with("news_processing", {}, request)
    finally:
        monkeypatch.undo()


@pytest.mark.asyncio
async def test_article_reprocess_enqueues_identity_and_force_options(monkeypatch):
    from importlib import import_module
    from news_aggregator.api.articles_router import ReprocessRequest, reprocess_article

    jobs = import_module("news_aggregator.api.background_jobs_router")
    accepted = SimpleNamespace(status="queued", job_id="fixture-job")
    enqueue = AsyncMock(return_value=accepted)
    response_value = SimpleNamespace(status_code=202)
    monkeypatch.setattr(jobs, "enqueue_from_request", enqueue)
    monkeypatch.setattr(jobs, "accepted_response", lambda result: response_value)
    request = object()
    db = SimpleNamespace(scalar=AsyncMock(return_value=51))

    result = await reprocess_article(
        51, ReprocessRequest(force=False, reextract_media=True), db, request
    )

    assert result is response_value
    enqueue.assert_awaited_once_with("article_reprocess", {
        "article_ids": [51], "force": False, "reextract_media": True,
    }, request)


@pytest.mark.asyncio
async def test_selected_retry_preserves_completed_stages_and_queues_ids(monkeypatch):
    from importlib import import_module
    from news_aggregator.api.processing_router import SelectedRetryRequest, retry_selected_articles

    jobs = import_module("news_aggregator.api.background_jobs_router")
    accepted = SimpleNamespace(status="queued", job_id="fixture-job")
    enqueue = AsyncMock(return_value=accepted)
    response_value = SimpleNamespace(status_code=202)
    monkeypatch.setattr(jobs, "enqueue_from_request", enqueue)
    monkeypatch.setattr(jobs, "accepted_response", lambda result: response_value)
    request = object()

    result = await retry_selected_articles(SelectedRetryRequest(article_ids=[12, 13]), request)

    assert result is response_value
    enqueue.assert_awaited_once_with("article_reprocess", {
        "article_ids": [12, 13], "force": False, "reextract_media": False,
    }, request)
