"""Go-mode selection and failure behavior for the full processing cycle."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from news_aggregator import orchestrator as orchestrator_module
from news_aggregator.orchestrator import NewsOrchestrator
from news_aggregator.services import article_ingestion_client as client_module
from news_aggregator.services import article_processing_client as processing_module


def orchestrator():
    instance = NewsOrchestrator.__new__(NewsOrchestrator)
    instance.db_queue_manager = MagicMock()
    instance.db_queue_manager.execute_write = AsyncMock(return_value=None)
    return instance


@pytest.mark.asyncio
async def test_full_cycle_uses_go_ingestion_then_go_article_processing(monkeypatch):
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 2, "articles_fetched": 3, "errors": [], "fatal_error": None,
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    processor = SimpleNamespace(process_current_job=AsyncMock(return_value={
        "status": "success", "articles_processed": 2, "articles_summarized": 2,
        "articles_categorized": 2,
    }))
    monkeypatch.setattr(processing_module, "ArticleProcessingClient", lambda: processor)
    instance = orchestrator()

    result = await instance._run_full_cycle()

    client.collect_and_save.assert_awaited_once()
    processor.process_current_job.assert_awaited_once()
    assert result["sources_synced"] == 2
    assert result["articles_fetched"] == 3
    assert result["fatal_error"] is None


@pytest.mark.parametrize("processor_result,expected_counter", [
    ({
        "status": "settled", "success": True, "settled": True, "retryable": False,
        "articles_processed": 2, "articles_rejected": 1,
        "article_outcomes": [{"article_id": 72, "state": "rejected", "reason": "advertisement"}],
    }, "articles_rejected"),
    ({
        "status": "incomplete", "success": False, "settled": False, "retryable": True,
        "articles_processed": 0, "articles_retrying": 1,
        "article_outcomes": [{"article_id": 72, "state": "retry_wait", "reason": "provider_timeout"}],
    }, "articles_retrying"),
])
@pytest.mark.asyncio
async def test_full_cycle_keeps_aggregate_counters_without_selected_receipt_statuses(
        monkeypatch, processor_result, expected_counter):
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 1, "articles_fetched": 1, "errors": [], "fatal_error": None,
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    processor = SimpleNamespace(process_current_job=AsyncMock(return_value=processor_result))
    monkeypatch.setattr(processing_module, "ArticleProcessingClient", lambda: processor)

    result = await orchestrator()._run_full_cycle()

    assert result[expected_counter] == 1
    assert result["articles_processed"] == processor_result["articles_processed"]
    assert not ({"success", "settled", "retryable", "article_outcomes",
                 "article_outcomes_total", "article_outcomes_omitted"} & result.keys())
    # A selected item status must not make a full cycle look settled or incomplete.
    assert result.get("status") is None


@pytest.mark.asyncio
async def test_full_cycle_preserves_ingestion_warning_when_processor_rejects_an_ad(monkeypatch):
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 1, "articles_fetched": 1, "errors": ["source_fetch_warning"],
        "errors_count": 1, "fatal_error": None,
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    processor = SimpleNamespace(process_current_job=AsyncMock(return_value={
        "status": "rejected", "success": False, "settled": True, "retryable": False,
        "articles_processed": 0, "articles_rejected": 1,
        "article_outcomes": [{"article_id": 72, "state": "rejected", "reason": "advertisement"}],
    }))
    monkeypatch.setattr(processing_module, "ArticleProcessingClient", lambda: processor)

    result = await orchestrator()._run_full_cycle()

    assert result["errors"] == ["source_fetch_warning"]
    assert result["errors_count"] == 1
    assert result["articles_rejected"] == 1
    assert "article_outcomes" not in result


@pytest.mark.asyncio
async def test_full_cycle_preserves_ingestion_warning_through_processor_success(monkeypatch):
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 1, "articles_fetched": 1, "errors": ["source_fetch_warning"],
        "errors_count": 3, "fatal_error": None,
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    processor = SimpleNamespace(process_current_job=AsyncMock(return_value={
        "status": "success", "success": True, "settled": True, "retryable": False,
        "articles_processed": 1, "errors_count": 0,
    }))
    monkeypatch.setattr(processing_module, "ArticleProcessingClient", lambda: processor)

    result = await orchestrator()._run_full_cycle()

    assert result["status"] == "success"
    assert result["errors"] == ["source_fetch_warning"]
    assert result["errors_count"] == 3
    from news_aggregator.services.background_execution import _bounded_result
    durable = _bounded_result("news_processing", result)
    assert durable["status"] == "warning"
    assert durable["errors_count"] == 3


@pytest.mark.asyncio
async def test_full_cycle_keeps_go_category_mapping_warning_and_sums_error_counts(monkeypatch):
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 1, "articles_fetched": 1, "errors": ["source_fetch_warning"],
        "errors_count": 3, "fatal_error": None,
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    processor = SimpleNamespace(process_current_job=AsyncMock(return_value={
        "status": "warning", "success": False, "settled": True, "retryable": False,
        "articles_processed": 1, "errors_count": 2,
    }))
    monkeypatch.setattr(processing_module, "ArticleProcessingClient", lambda: processor)

    result = await orchestrator()._run_full_cycle()

    assert result["status"] == "warning"
    assert result["errors"] == ["source_fetch_warning"]
    assert result["errors_count"] == 5


@pytest.mark.asyncio
async def test_full_cycle_does_not_start_ai_after_unconfirmed_ingestion(monkeypatch):
    client = SimpleNamespace(collect_and_save=AsyncMock(return_value={
        "sources_synced": 0, "articles_fetched": 0, "errors": [], "fatal_error": "cancel_unconfirmed",
    }))
    monkeypatch.setattr(client_module, "ArticleIngestionClient", lambda: client)
    processor = SimpleNamespace(process_current_job=AsyncMock())
    monkeypatch.setattr(processing_module, "ArticleProcessingClient", lambda: processor)
    instance = orchestrator()

    result = await instance._run_full_cycle()

    processor.process_current_job.assert_not_awaited()
    assert result["fatal_error"]
