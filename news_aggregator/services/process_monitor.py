"""Process monitoring — lightweight health checker for the browser connection."""

import asyncio
import logging
from typing import Optional
from datetime import datetime

from .operational_alerts import OperationalAlertManager

logger = logging.getLogger(__name__)


async def _send_service_alert(title: str, message: str):
    from ..orchestrator import NewsOrchestrator

    return await NewsOrchestrator().send_operational_alert(title, message)


class ProcessMonitor:
    """Observe browser pool health without opening replacement CDP sessions."""

    def __init__(
        self,
        check_interval: int = 300,
        alerts: Optional[OperationalAlertManager] = None,
    ):  # 5 minutes
        self.check_interval = check_interval
        self.cleanup_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.alerts = alerts or OperationalAlertManager(_send_service_alert)

    async def start(self):
        """Start periodic health monitoring."""
        if self.is_running:
            logger.warning("Process monitor already running")
            return

        self.is_running = True
        self.cleanup_task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Process monitor started with {self.check_interval}s interval")

    async def stop(self):
        """Stop periodic monitoring."""
        self.is_running = False

        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
            self.cleanup_task = None

        logger.info("Process monitor stopped")

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self.is_running:
            try:
                await self._check_browser_health()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in process monitor loop: {e}")
                await asyncio.sleep(10)

    async def _check_browser_health(self):
        """Log passive browser state; content requests own reconnection attempts."""
        try:
            from ..core.browser_pool import get_browser_pool_status

            status = get_browser_pool_status()
            if status.connected:
                logger.debug("Browser pool connection is healthy")
                await self.alerts.recovery(
                    "browser-pool",
                    "Chrome снова доступен",
                    "CDP-соединение успешно восстановлено.",
                )
            elif status.last_error:
                logger.warning(
                    "Browser pool unavailable: %s (cooldown %.1fs)",
                    status.last_error,
                    status.cooldown_remaining_seconds,
                )
                await self.alerts.failure(
                    "browser-pool",
                    "Chrome/CDP недоступен",
                    f"{status.last_error}. Cooldown: {status.cooldown_remaining_seconds:.1f} с.",
                )
            else:
                logger.debug("Browser pool is idle; no connection has been requested")
        except Exception as e:
            logger.error(f"Error during browser health check: {e}")

    async def manual_cleanup(self) -> dict:
        """Return a passive status snapshot without mutating the pool."""
        from ..core.browser_pool import get_browser_pool_status

        status = get_browser_pool_status()

        return {
            "browser_connected": status.connected,
            "last_error": status.last_error,
            "cooldown_remaining_seconds": status.cooldown_remaining_seconds,
            "timestamp": datetime.utcnow().isoformat()
        }


# Global process monitor instance
_process_monitor: Optional[ProcessMonitor] = None


def get_process_monitor() -> ProcessMonitor:
    """Get global process monitor instance."""
    global _process_monitor

    if _process_monitor is None:
        _process_monitor = ProcessMonitor()

    return _process_monitor


async def start_process_monitor():
    """Start the global process monitor."""
    monitor = get_process_monitor()
    await monitor.start()


async def stop_process_monitor():
    """Stop the global process monitor."""
    monitor = get_process_monitor()
    await monitor.stop()
