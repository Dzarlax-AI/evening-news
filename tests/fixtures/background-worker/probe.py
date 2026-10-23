"""Container-side API and database probes used by the Docker gate."""

import asyncio
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE = "http://127.0.0.1:8000"
AUTH = ("Basic " + __import__("base64").b64encode(
    ("fixture-admin:fixture-admin-password").encode()).decode())


def request(method, path, *, payload=None, auth=True, key=None):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = AUTH
    if key:
        headers["Idempotency-Key"] = key
    req = Request(BASE + path, data=json.dumps(payload).encode() if payload is not None else None,
                  headers=headers, method=method)
    try:
        with urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read() or b"{}")
    except HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")
    except URLError as exc:
        return 0, {"error": type(exc).__name__}


def fake_request(method, path, *, payload=None):
    req = Request("http://fixture-services:8090" + path,
                  data=json.dumps(payload).encode() if payload is not None else None,
                  headers={"Content-Type": "application/json"}, method=method)
    with urlopen(req, timeout=5) as response:
        return json.loads(response.read() or b"{}")


async def database_snapshot():
    from sqlalchemy import func, select
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import Article, BackgroundJob, ScheduleSettings

    async with AsyncSessionLocal() as session:
        articles = await session.execute(select(
            Article.id, Article.title, Article.processing_state, Article.summary_processed,
            Article.category_processed, Article.ad_processed, Article.last_processing_error,
            Article.processing_attempts,
        ))
        jobs = await session.execute(select(BackgroundJob.id, BackgroundJob.kind, BackgroundJob.state,
                                           BackgroundJob.attempt_generation, BackgroundJob.payload))
        due_schedules = await session.scalar(select(func.count()).select_from(ScheduleSettings).where(
            ScheduleSettings.enabled.is_(True), ScheduleSettings.is_running.is_(False),
            ScheduleSettings.next_run.is_not(None)))
        return {
            "articles": [{"id": row.id, "title": row.title, "state": row.processing_state,
                          "summary_processed": row.summary_processed,
                          "category_processed": row.category_processed,
                          "ad_processed": row.ad_processed,
                          "last_processing_error": row.last_processing_error,
                          "processing_attempts": row.processing_attempts}
                         for row in articles],
            "jobs": [{"id": str(row.id), "kind": row.kind, "state": row.state,
                      "attempt_generation": row.attempt_generation,
                      "scheduled": bool((row.payload or {}).get("schedule_id"))} for row in jobs],
            "due_schedules": int(due_schedules or 0),
        }


async def advance_article_retry(article_id):
    from datetime import datetime, timedelta
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import Article

    async with AsyncSessionLocal() as session:
        article = await session.get(Article, article_id)
        if article is None or article.processing_state != "retry_wait":
            return {"advanced": False}
        previous_attempts = article.processing_attempts
        previous_summary = article.summary_processed
        previous_category = article.category_processed
        previous_ad = article.ad_processed
        article.next_attempt_at = datetime.utcnow() - timedelta(seconds=1)
        await session.commit()
        return {
            "advanced": True,
            "attempts_preserved": article.processing_attempts == previous_attempts,
            "stages_preserved": (
                article.summary_processed == previous_summary
                and article.category_processed == previous_category
                and article.ad_processed == previous_ad
            ),
        }


def main():
    action = sys.argv[1]
    if action == "health":
        status, value = request("GET", "/health", auth=False)
    elif action == "unauthorized":
        status, value = request("GET", "/api/v1/jobs/00000000-0000-0000-0000-000000000001", auth=False)
    elif action == "status":
        status, value = request("GET", "/api/v1/process/status")
    elif action == "dashboard":
        status, value = request("GET", "/api/v1/stats/dashboard")
    elif action == "enqueue-process":
        key = sys.argv[2]
        status, value = request("POST", "/api/v1/process/run", payload={}, key=key)
    elif action == "enqueue-digest":
        status, value = request("POST", "/api/v1/telegram/send-digest", payload={}, key=sys.argv[2])
    elif action == "reprocess":
        article_id, key = int(sys.argv[2]), sys.argv[3]
        status, value = request("POST", f"/api/v1/articles/{article_id}/reprocess",
                                payload={"force": True}, key=key)
    elif action == "job":
        status, value = request("GET", "/api/v1/jobs/" + sys.argv[2])
    elif action == "source-not-found":
        status, value = request("POST", "/api/v1/sources/2147483000/test", payload={}, key=sys.argv[2])
    elif action == "telegram-test":
        status, value = request("POST", "/api/v1/telegram/test", payload={})
    elif action == "snapshot":
        value = asyncio.run(database_snapshot())
        status = 200
    elif action == "advance-retry":
        value = asyncio.run(advance_article_retry(int(sys.argv[2])))
        status = 200
    elif action == "fake-counters":
        value = fake_request("GET", "/__gate/counters")
        status = 200
    elif action == "fake-control":
        value = fake_request("POST", "/__gate/control", payload=json.loads(sys.argv[2]))
        status = 200
    else:
        raise SystemExit("unknown_probe_action")
    print(json.dumps({"http_status": status, "response": value}, separators=(",", ":")))


if __name__ == "__main__":
    main()
