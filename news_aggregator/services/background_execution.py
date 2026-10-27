"""Retired Python worker compatibility helpers.

The Go execution service owns job admission, claims, lifecycle, scheduling, and
execution. This module keeps only the process healthcheck used by the retired
Python CLI shim.
"""

import asyncio
from ..config import settings
from ..database import AsyncSessionLocal
from .background_jobs import read_worker_status


async def worker_healthcheck(session_factory=None) -> int:
    """Return a process-check exit code from the persisted worker heartbeat."""
    if settings.background_execution_mode != "worker":
        return 1
    factory = session_factory or AsyncSessionLocal

    async def read_status():
        async with factory() as session:
            return await read_worker_status(session)

    try:
        status = await asyncio.wait_for(read_status(), timeout=5)
    except Exception:
        return 1
    return 0 if status and status.get("healthy") else 1
