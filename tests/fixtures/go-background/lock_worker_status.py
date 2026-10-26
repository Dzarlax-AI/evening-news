"""Disposable-only helper: hold the worker status row lock without printing SQL."""

import asyncio
import os
import sys

import asyncpg


async def hold(seconds: float) -> None:
    connection = await asyncpg.connect(os.environ["WORKER_DATABASE_URL"], timeout=5)
    transaction = connection.transaction()
    await transaction.start()
    try:
        # Holding this UPDATE row lock makes the real worker heartbeat time out.
        await connection.execute(
            "UPDATE background_worker_status SET heartbeat_at=heartbeat_at WHERE id=1"
        )
        print("fixture_lock_acquired", flush=True)
        await asyncio.sleep(seconds)
        print("fixture_lock_released", flush=True)
    finally:
        await transaction.rollback()
        await connection.close()


if __name__ == "__main__":
    duration = float(sys.argv[1]) if len(sys.argv) == 2 else 14.0
    if not 8 <= duration <= 30:
        raise SystemExit("fixture lock duration is outside the safe test bound")
    asyncio.run(hold(duration))
