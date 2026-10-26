"""Fixture-only private executor and durable receipt probes."""

import asyncio
import json
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def private(path, authenticated=True):
    headers = {"Authorization": "Bearer fixture-only-executor-token"} if authenticated else {}
    request = Request("http://executor:8100" + path, headers=headers)
    try:
        with urlopen(request, timeout=5) as response:
            return {"http_status": response.status, "response": json.loads(response.read())}
    except HTTPError as error:
        return {"http_status": error.code}


def processing_health():
    request = Request("http://processing:8094/healthz")
    try:
        with urlopen(request, timeout=5) as response:
            return {"http_status": response.status, "response": json.loads(response.read())}
    except HTTPError as error:
        return {"http_status": error.code}


async def receipts():
    from sqlalchemy import text
    from news_aggregator.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(text("""
            SELECT job_id::text, attempt_generation, executor_instance_id::text, state
            FROM background_job_executions ORDER BY accepted_at, attempt_generation
        """))).mappings().all()
        status = (await session.execute(text("""
            SELECT generation, lock_backend_pid, status FROM background_worker_status WHERE id=1
        """))).mappings().first()
        return {"receipts": [dict(row) for row in rows], "worker": dict(status or {})}


async def processing_runs():
    from sqlalchemy import text
    from news_aggregator.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(text("""
            SELECT job_id::text, state, error_code
            FROM article_processing_runs ORDER BY started_at
        """))).mappings().all()
        return {"runs": [dict(row) for row in rows]}


async def enqueue_kind(kind):
    from sqlalchemy import select
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import Source
    from news_aggregator.services.background_jobs import enqueue_job_in_session

    payloads = {"news_digest": {"run_processing": False, "send_telegram": False},
                "reprocess_failed": {"dry_run": True}, "summaries_generate": {}, "backup": {}}
    async with AsyncSessionLocal() as session:
        if kind == "source_test":
            source_id = await session.scalar(select(Source.id).where(Source.name == "Background Gate RSS"))
            payload = {"source_id": source_id}
        elif kind in payloads:
            payload = payloads[kind]
        else:
            raise ValueError("Unsupported fixture job kind")
        job = await enqueue_job_in_session(session, kind, payload, "go-kind:" + kind, coalesce_active=False)
        await session.commit()
        return {"job_id": str(job.id)}


async def legacy_lease(hold=False):
    import signal
    from pathlib import Path
    from news_aggregator.database import engine
    from news_aggregator.services.background_jobs import BackgroundWorkerLease

    lease = BackgroundWorkerLease(engine)
    try:
        owned = await lease.acquire()
        if owned and hold:
            stop = asyncio.Event()
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
            Path("/tmp/legacy-lease-ready").write_text("ready")
            await asyncio.wait_for(stop.wait(), timeout=600)
        return {"acquired": owned}
    finally:
        await lease.release()
        await engine.dispose()


if __name__ == "__main__":
    action = sys.argv[1]
    if action == "status":
        result = private("/internal/v1/status")
    elif action == "processing-health":
        result = processing_health()
    elif action == "unauthorized":
        result = private("/internal/v1/status", False)
    elif action == "legacy-lease":
        result = asyncio.run(legacy_lease())
    elif action == "legacy-hold":
        result = asyncio.run(legacy_lease(hold=True))
    elif action == "enqueue-kind":
        result = asyncio.run(enqueue_kind(sys.argv[2]))
    elif action == "public-home":
        with urlopen("http://web:8000/", timeout=5) as response:
            result = {"http_status": response.status}
    elif action == "receipts":
        result = asyncio.run(receipts())
    elif action == "processing-runs":
        result = asyncio.run(processing_runs())
    else:
        raise SystemExit("Unknown fixture action")
    print(json.dumps(result, sort_keys=True))
