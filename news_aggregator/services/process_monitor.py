"""Compatibility status for browser operations owned by the Go service."""


class ProcessMonitor:
    """Never infer remote browser health from a Python-local connection pool."""

    async def get_status_snapshot(self) -> dict:
        return {"owner": "go-collector", "browser_status": "unknown"}

    async def get_status(self) -> dict:
        return await self.get_status_snapshot()


async def start_process_monitor():
    """Retained as a no-op for old lifecycle imports."""
    return None


async def stop_process_monitor():
    """Retained as a no-op for old lifecycle imports."""
    return None
