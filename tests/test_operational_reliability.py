"""Regression tests for truthful scheduler outcomes and service-chat alerts."""

import asyncio
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_aggregator.services.operational_alerts import OperationalAlertManager
from news_aggregator.services.process_monitor import ProcessMonitor
from news_aggregator.services.scheduler import TaskScheduler
from news_aggregator.services.telegram_service import TelegramSendResult


@pytest.mark.asyncio
async def test_operational_alerts_deduplicate_and_send_one_recovery():
    sender = AsyncMock(return_value=True)
    alerts = OperationalAlertManager(sender, repeat_interval_seconds=3600)

    assert await alerts.failure("chrome", "Chrome down", "first") is True
    assert await alerts.failure("chrome", "Chrome down", "second") is False
    assert sender.await_count == 1

    assert await alerts.recovery("chrome", "Chrome up", "recovered") is True
    assert sender.await_count == 2
    assert "Подавлено повторов: 1" in sender.await_args_list[-1].args[1]
    assert await alerts.recovery("chrome", "Chrome up", "again") is False


@pytest.mark.asyncio
async def test_process_monitor_is_passive_and_alerts_on_failure_and_recovery():
    alerts = MagicMock()
    alerts.failure = AsyncMock(return_value=True)
    alerts.recovery = AsyncMock(return_value=True)
    monitor = ProcessMonitor(alerts=alerts)

    unavailable = SimpleNamespace(
        connected=False,
        last_error="ConnectionError: refused",
        cooldown_remaining_seconds=42.0,
    )
    healthy = SimpleNamespace(
        connected=True,
        last_error=None,
        cooldown_remaining_seconds=0.0,
    )

    with patch(
        "news_aggregator.core.browser_pool.get_browser_pool_status",
        side_effect=[unavailable, healthy],
    ):
        await monitor._check_browser_health()
        await monitor._check_browser_health()

    alerts.failure.assert_awaited_once()
    alerts.recovery.assert_awaited_once()


def make_scheduler_for_unit_test() -> TaskScheduler:
    scheduler = TaskScheduler.__new__(TaskScheduler)
    scheduler.orchestrator = MagicMock()
    scheduler.alerts = MagicMock()
    scheduler.alerts.failure = AsyncMock(return_value=True)
    scheduler.alerts.recovery = AsyncMock(return_value=True)
    scheduler._task_timeout_seconds = 30
    scheduler._task_handles = {}
    scheduler._tasks = {}
    scheduler._task_semaphore = MagicMock()
    scheduler._update_task_schedule = AsyncMock()
    return scheduler


@pytest.mark.asyncio
async def test_digest_delivery_failure_is_not_reported_as_success():
    scheduler = make_scheduler_for_unit_test()
    scheduler.orchestrator.send_telegram_digest = AsyncMock(
        return_value={
            "success": False,
            "parts_sent": 0,
            "error": "HTTP 400: Bad Request: chat not found",
        }
    )

    with pytest.raises(RuntimeError, match="chat not found"):
        await scheduler._run_telegram_digest({})


@pytest.mark.asyncio
async def test_scheduler_persists_failed_run_and_sends_alert():
    scheduler = make_scheduler_for_unit_test()
    scheduler._run_telegram_digest = AsyncMock(
        side_effect=RuntimeError("Telegram digest was not delivered: chat not found")
    )

    await scheduler._run_task("telegram_digest", {}, setting_id=7)

    scheduler.alerts.failure.assert_awaited_once()
    kwargs = scheduler._update_task_schedule.await_args.kwargs
    assert kwargs["status"] == "failed"
    assert "chat not found" in kwargs["error"]
    assert kwargs["duration_seconds"] >= 0


@pytest.mark.asyncio
async def test_scheduler_aggregates_processing_errors_as_one_warning():
    scheduler = make_scheduler_for_unit_test()
    scheduler._run_news_processing = AsyncMock(
        return_value={"status": "warning", "error": "Ошибок источников/обработки: 3"}
    )

    await scheduler._run_task("news_processing", {}, setting_id=8)

    scheduler.alerts.failure.assert_awaited_once()
    kwargs = scheduler._update_task_schedule.await_args.kwargs
    assert kwargs["status"] == "warning"
    assert kwargs["error"] == "Ошибок источников/обработки: 3"


@pytest.mark.asyncio
async def test_manual_digest_endpoint_returns_non_2xx_on_delivery_failure(monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "admin_password", "test-only-password")
    orchestrator = MagicMock()
    orchestrator.start = AsyncMock()
    orchestrator.stop = AsyncMock()
    orchestrator.send_telegram_digest = AsyncMock(
        return_value={"success": False, "error": "HTTP 400: chat not found"}
    )

    with patch(
        "news_aggregator.api.telegram_router.NewsOrchestrator",
        return_value=orchestrator,
    ):
        from news_aggregator.api.telegram_router import send_telegram_digest

        response = await send_telegram_digest()

    assert response.status_code == 502
    body = json.loads(response.body)
    assert body["success"] is False
    assert body["result"]["error"] == "HTTP 400: chat not found"
    orchestrator.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_run_preserves_previous_success_timestamp():
    scheduler = make_scheduler_for_unit_test()
    previous_success = datetime(2026, 7, 24, 19, 6)
    setting = SimpleNamespace(
        enabled=False,
        is_running=True,
        task_name="telegram_digest",
        next_run=None,
        last_finished_at=None,
        last_success_at=previous_success,
        last_status=None,
        last_error=None,
        last_duration_seconds=None,
    )
    result = MagicMock()
    result.scalar_one_or_none.return_value = setting
    db = MagicMock()
    db.execute = AsyncMock(return_value=result)

    async def execute(operation):
        return await operation(db)

    with patch(
        "news_aggregator.services.scheduler.execute_custom_write",
        side_effect=execute,
    ):
        await TaskScheduler._update_task_schedule(
            scheduler,
            7,
            status="failed",
            error="chat not found",
            duration_seconds=1.25,
        )

    assert setting.last_status == "failed"
    assert setting.last_success_at == previous_success
    assert setting.last_finished_at is not None


@pytest.mark.asyncio
async def test_partial_split_digest_remains_failed_with_counts():
    from news_aggregator.config import settings
    from news_aggregator.orchestrator import NewsOrchestrator

    orchestrator = NewsOrchestrator.__new__(NewsOrchestrator)
    orchestrator.db_queue_manager = MagicMock()
    orchestrator.db_queue_manager.execute_read = AsyncMock(
        side_effect=[
            1,
            {"split": True, "digest_parts": ["part one", "part two"], "rich_digest": None},
        ]
    )
    service = MagicMock()
    service.send_message = AsyncMock(
        side_effect=[
            TelegramSendResult(True, "sendMessage", "@news", status_code=200),
            TelegramSendResult(
                False,
                "sendMessage",
                "@news",
                status_code=400,
                error_code=400,
                description="Bad Request: chat not found",
            ),
        ]
    )
    orchestrator._get_telegram_service_with_db_overrides = AsyncMock(return_value=service)

    original_rich_setting = settings.telegram_rich_messages_enabled
    settings.telegram_rich_messages_enabled = False
    try:
        result = await NewsOrchestrator.send_telegram_digest(orchestrator)
    finally:
        settings.telegram_rich_messages_enabled = original_rich_setting

    assert result["success"] is False
    assert result["parts_sent"] == 1
    assert result["parts_total"] == 2
    assert "chat not found" in result["error"]


@pytest.mark.asyncio
async def test_scheduler_check_failure_reaches_service_alert():
    scheduler = make_scheduler_for_unit_test()

    with patch(
        "news_aggregator.services.scheduler.fetch_all",
        AsyncMock(side_effect=RuntimeError("database queue unavailable")),
    ):
        await scheduler._check_and_run_tasks()

    scheduler.alerts.failure.assert_awaited_once()
    assert scheduler.alerts.failure.await_args.args[0] == "scheduler-task-check"
    assert "database queue unavailable" in scheduler.alerts.failure.await_args.args[2]
