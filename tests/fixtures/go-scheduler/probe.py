"""Fixed-destination probes used only inside the isolated fixture network."""

import asyncio
import base64
import hashlib
import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOTS = {
    "web": "http://web:8000",
    "scheduler": "http://scheduler:8080",
    "fake": "http://fixture-services:8090",
}
ADMIN = "Basic " + base64.b64encode(b"fixture-admin:fixture-admin-password").decode()
SCHEDULER = "Bearer fixture-scheduler-only"
TARGET = "Bearer fixture-news-target"


def request(service, method, path, payload=None, auth="none", headers=None, timeout=10):
    if service not in ROOTS or not path.startswith("/") or ".." in path.split("/"):
        raise ValueError("unknown_fixture_probe_target")
    request_headers = {"Accept": "application/json"}
    if payload is not None:
        request_headers["Content-Type"] = "application/json"
    if auth == "admin":
        request_headers["Authorization"] = ADMIN
    elif auth == "scheduler":
        request_headers["Authorization"] = SCHEDULER
    elif auth == "target":
        request_headers["Authorization"] = TARGET
    elif auth != "none":
        raise ValueError("unknown_fixture_probe_auth")
    request_headers.update(headers or {})
    raw = json.dumps(payload, separators=(",", ":")).encode() if payload is not None else None
    req = Request(ROOTS[service] + path, data=raw, headers=request_headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read(16385)
            return response.status, decode(body, response.headers.get("Content-Type", ""))
    except HTTPError as exc:
        return exc.code, decode(exc.read(16385), exc.headers.get("Content-Type", ""))
    except (URLError, TimeoutError) as exc:
        return 0, {"transport_error": type(exc).__name__}


def decode(raw, content_type):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return {"non_json": True, "bytes": len(raw), "content_type": content_type[:80]}


async def snapshot():
    from sqlalchemy import select, func
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import BackgroundJob, BackgroundWorkerStatus, ScheduleSettings

    async with AsyncSessionLocal() as session:
        jobs = await session.execute(select(
            BackgroundJob.id, BackgroundJob.kind, BackgroundJob.state,
            BackgroundJob.schedule_key, BackgroundJob.schedule_revision,
            BackgroundJob.occurrence_id, BackgroundJob.scheduled_for,
        ).order_by(BackgroundJob.created_at, BackgroundJob.id))
        settings = await session.execute(select(
            ScheduleSettings.task_name, ScheduleSettings.enabled,
            ScheduleSettings.next_run, ScheduleSettings.last_status,
        ).order_by(ScheduleSettings.task_name))
        queue_counts = await session.execute(select(
            BackgroundJob.state, func.count()
        ).group_by(BackgroundJob.state))
        worker = await session.get(BackgroundWorkerStatus, 1)
    return {
        "jobs": [{"id": str(row.id), "kind": row.kind, "state": row.state,
                  "schedule_key": row.schedule_key, "schedule_revision": row.schedule_revision,
                  "occurrence_id": str(row.occurrence_id) if row.occurrence_id else None,
                  "scheduled_for": row.scheduled_for.isoformat() + "Z" if row.scheduled_for else None}
                 for row in jobs],
        "legacy_schedule_rows": [{"task_name": row.task_name, "enabled": row.enabled,
                                  "next_run": row.next_run.isoformat() + "Z" if row.next_run else None,
                                  "last_status": row.last_status} for row in settings],
        "job_counts": {state: count for state, count in queue_counts},
        "worker": {"status": worker.status, "generation": worker.generation,
                   "heartbeat_at": worker.heartbeat_at.isoformat() + "Z" if worker.heartbeat_at else None}
                  if worker else None,
    }


async def cleanup_saturation():
    import hashlib
    from sqlalchemy import delete, select
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import Article, BackgroundJob, BackgroundJobIdempotencyKey, Source

    prefix = "go-scheduler-gate-saturation-"
    keys = [hashlib.sha256((prefix + str(index)).encode("ascii")).hexdigest()
            for index in range(32)]
    async with AsyncSessionLocal() as session:
        rows = await session.execute(select(BackgroundJob.id).join(
            BackgroundJobIdempotencyKey,
            BackgroundJobIdempotencyKey.job_id == BackgroundJob.id,
        ).where(BackgroundJobIdempotencyKey.key.in_(keys), BackgroundJob.state == "queued"))
        ids = [row.id for row in rows]
        if ids:
            await session.execute(delete(BackgroundJob).where(BackgroundJob.id.in_(ids)))
        source_id = await session.scalar(select(Source.id).where(
            Source.name == "Go Scheduler Fixture RSS"))
        articles = 0
        if source_id is not None:
            article_rows = await session.execute(select(Article.id).where(
                Article.source_id == source_id,
                Article.title.like("Go Scheduler Fixture Saturation %"),
                Article.url.like("https://fixture.invalid/articles/go-scheduler-gate-saturation-%"),
            ))
            article_ids = [row.id for row in article_rows]
            articles = len(article_ids)
            if article_ids:
                await session.execute(delete(Article).where(Article.id.in_(article_ids)))
        await session.commit()
    return {"deleted_queued_fixture_jobs": len(ids), "deleted_fixture_articles": articles}


async def create_saturation_articles():
    from sqlalchemy import select
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import Article, Source

    async with AsyncSessionLocal() as session:
        source_id = await session.scalar(select(Source.id).where(
            Source.name == "Go Scheduler Fixture RSS"))
        if source_id is None:
            raise RuntimeError("fixture_source_missing")
        articles = [Article(
            source_id=source_id,
            title=f"Go Scheduler Fixture Saturation {index}",
            url=f"https://fixture.invalid/articles/go-scheduler-gate-saturation-{index}",
            content="Synthetic fixture-only queue saturation record.",
            published_at=None,
            processing_state="pending",
            processing_attempts=0,
            summary_processed=False,
            category_processed=False,
            ad_processed=False,
            processed=False,
        ) for index in range(32)]
        session.add_all(articles)
        await session.commit()
        return [article.id for article in articles]


async def enqueue_gemini_blocker():
    from sqlalchemy import select
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import Article, Source

    async with AsyncSessionLocal() as session:
        source = await session.scalar(select(Source).where(Source.name == "Go Scheduler Fixture RSS"))
        if source is None:
            return {"http_status": 500, "response": {}}
        article = Article(
            source_id=source.id,
            title="Fixture queue blocker article",
            url="https://fixture.invalid/articles/queue-blocker",
            content=("Synthetic fixture content for a worker queue hold. " * 40),
            published_at=None,
            processing_state="pending",
            processing_attempts=0,
            summary_processed=False,
            category_processed=False,
            ad_processed=False,
            processed=False,
        )
        session.add(article)
        await session.commit()
        article_id = article.id
    status, body = request(
        "web", "POST", f"/api/v1/articles/{article_id}/reprocess",
        {"force": True, "reextract_media": False}, auth="admin",
        headers={"Idempotency-Key": "go-scheduler-gate-gemini-blocker"},
    )
    return {"http_status": status, "response": body, "article_id": article_id}


def runtime_app_hashes():
    root = Path("/app")
    paths = sorted(
        path.relative_to(root).as_posix()
        for folder in ("news_aggregator", "web")
        for path in (root / folder).rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
        and (folder == "web" or path.suffix == ".py")
    )
    return {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in paths}


def main():
    action = sys.argv[1]
    if action == "request":
        _, _action, service, method, path, auth, *encoded = sys.argv
        payload = json.loads(encoded[0]) if encoded and encoded[0] != "-" else None
        status, body = request(service, method, path, payload, auth=auth)
        print(json.dumps({"http_status": status, "response": body}, separators=(",", ":")))
        return
    if action == "saturate":
        index = sys.argv[2]
        status, body = request("web", "POST", "/api/v1/process/run", {}, auth="admin",
                               headers={"Idempotency-Key": "go-scheduler-gate-saturation-" + index})
        print(json.dumps({"http_status": status, "response": body}, separators=(",", ":")))
        return
    if action == "saturate-batch":
        count = int(sys.argv[2])
        if not 1 <= count <= 32:
            raise SystemExit("fixture_batch_count_out_of_range")
        statuses = []
        article_ids = asyncio.run(create_saturation_articles())[:count]
        job_ids = []
        for index, article_id in enumerate(article_ids):
            status, _body = request(
                "web", "POST", f"/api/v1/articles/{article_id}/reprocess",
                {"force": True, "reextract_media": False}, auth="admin",
                headers={"Idempotency-Key": "go-scheduler-gate-saturation-" + str(index)},
            )
            statuses.append(status)
            if status == 202:
                job_ids.append(_body.get("job_id"))
            if status != 202:
                break
        print(json.dumps({"statuses": statuses, "accepted": sum(status == 202 for status in statuses),
                          "unique_job_ids": len(set(job_ids)), "job_ids": job_ids},
                         separators=(",", ":")))
        return
    if action == "snapshot":
        print(json.dumps(asyncio.run(snapshot()), separators=(",", ":")))
        return
    if action == "runtime-app-hashes":
        print(json.dumps(runtime_app_hashes(), separators=(",", ":")))
        return
    if action == "cleanup-saturation":
        print(json.dumps(asyncio.run(cleanup_saturation()), separators=(",", ":")))
        return
    if action == "enqueue-gemini-blocker":
        print(json.dumps(asyncio.run(enqueue_gemini_blocker()), separators=(",", ":")))
        return
    if action == "seed":
        import runpy
        runpy.run_path("/fixtures/seed_fixture.py", run_name="__main__")
        return
    if action == "fake-counters":
        status, body = request("fake", "GET", "/__gate/counters")
        print(json.dumps({"http_status": status, "response": body}, separators=(",", ":")))
        return
    if action == "fake-control":
        changes = json.loads(sys.argv[2])
        status, body = request("fake", "POST", "/__gate/control", changes)
        print(json.dumps({"http_status": status, "response": body}, separators=(",", ":")))
        return
    raise SystemExit("unknown_fixture_probe_action")


if __name__ == "__main__":
    main()
