"""Regression tests for truthful scheduler outcomes and service-chat alerts."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_aggregator.services.operational_alerts import OperationalAlertManager
from news_aggregator.services.process_monitor import ProcessMonitor


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
async def test_failed_alert_delivery_is_retried_and_has_no_phantom_recovery():
    sender = AsyncMock(side_effect=[False, True])
    alerts = OperationalAlertManager(sender, repeat_interval_seconds=3600)

    assert await alerts.failure("database", "Database down", "first") is False
    assert alerts.is_active("database") is False
    assert await alerts.recovery("database", "Database up", "never announced") is False

    assert await alerts.failure("database", "Database down", "retry") is True
    assert sender.await_count == 2
    assert alerts.is_active("database") is True


@pytest.mark.asyncio
async def test_process_monitor_reports_remote_browser_state_as_unknown():
    with patch.object(OperationalAlertManager, "failure", new_callable=AsyncMock) as failure, patch.object(
            OperationalAlertManager, "recovery", new_callable=AsyncMock) as recovery:
        assert await ProcessMonitor().get_status_snapshot() == {
            "owner": "go-collector", "browser_status": "unknown"
        }
    failure.assert_not_awaited()
    recovery.assert_not_awaited()


@pytest.mark.asyncio
async def test_operational_alert_falls_back_to_environment_when_db_is_down():
    from news_aggregator.orchestrator import NewsOrchestrator

    orchestrator = NewsOrchestrator.__new__(NewsOrchestrator)
    orchestrator.db_queue_manager = MagicMock()
    orchestrator.db_queue_manager.execute_read = AsyncMock(
        side_effect=RuntimeError("database unavailable")
    )
    fallback_service = MagicMock()
    fallback_service.send_alert = AsyncMock(return_value=True)

    with patch(
        "news_aggregator.orchestrator.get_telegram_service",
        return_value=fallback_service,
    ):
        result = await NewsOrchestrator.send_operational_alert(
            orchestrator, "Database error", "queue unavailable"
        )

    assert result is True
    fallback_service.send_alert.assert_awaited_once_with(
        "Database error", "queue unavailable"
    )


@pytest.mark.asyncio
async def test_manual_digest_endpoint_returns_non_2xx_on_delivery_failure(monkeypatch):
    from news_aggregator.config import settings

    monkeypatch.setattr(settings, "admin_password", "test-only-password")
    monkeypatch.setattr(settings, "background_execution_mode", "embedded")
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
async def test_manual_digest_endpoint_refuses_active_processing_without_constructing_sender(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'test-only-password')
    monkeypatch.setattr(settings, 'background_execution_mode', 'embedded')
    from news_aggregator.api.telegram_router import send_telegram_digest
    from news_aggregator.orchestrator import reserve_processing_cycle, release_processing_cycle
    reservation = reserve_processing_cycle()
    assert reservation is not None
    try:
        with patch('news_aggregator.api.telegram_router.NewsOrchestrator') as factory:
            response = await send_telegram_digest()
        assert response.status_code == 409
        assert json.loads(response.body)['status'] == 'busy'
        factory.assert_not_called()
    finally:
        release_processing_cycle(reservation)


@pytest.mark.asyncio
async def test_manual_digest_reservation_lasts_through_cleanup_and_releases_on_failure(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'test-only-password')
    monkeypatch.setattr(settings, 'background_execution_mode', 'embedded')
    from news_aggregator.api.telegram_router import send_telegram_digest
    from news_aggregator.orchestrator import processing_cycle_busy
    async def send():
        assert processing_cycle_busy()
        return {'success': True}
    async def stop():
        assert processing_cycle_busy()
        raise RuntimeError('cleanup failed')
    orchestrator = SimpleNamespace(start=AsyncMock(), send_telegram_digest=send, stop=stop)
    with patch('news_aggregator.api.telegram_router.NewsOrchestrator', return_value=orchestrator):
        with pytest.raises(RuntimeError, match='cleanup failed'):
            await send_telegram_digest()
    assert not processing_cycle_busy()
