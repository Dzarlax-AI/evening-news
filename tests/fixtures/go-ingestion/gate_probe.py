"""Bounded in-container probes for the Go ingestion fixture gate."""

import asyncio
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

WEB_BASE_URL = os.environ.get("GATE_WEB_URL", "http://127.0.0.1:8000").rstrip("/")


def request(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = Request(WEB_BASE_URL + path, data=data,
                  headers={"Content-Type": "application/json", "Authorization":
                           "Basic Zml4dHVyZS1hZG1pbjpmaXh0dXJlLWFkbWluLXBhc3N3b3Jk"}, method=method)
    try:
        with urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read() or b"{}")
    except HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")
    except (URLError, ValueError):
        return 0, {}


async def snapshot():
    from sqlalchemy import select, text
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.models import (Article, ArticleIngestionRun, ArticleIngestionSourceRun,
                                       Source, SourceCollectionState)
    async with AsyncSessionLocal() as session:
        articles = (await session.execute(select(Article.id, Article.source_id, Article.title, Article.url,
                                                 Article.processing_state, Article.summary_processed))).all()
        runs = (await session.execute(select(ArticleIngestionRun.state, ArticleIngestionRun.result,
                                            ArticleIngestionRun.error_code))).all()
        processing_runs = (await session.execute(text("SELECT state FROM article_processing_runs"))).scalars().all()
        processing_items = (await session.execute(text("SELECT state FROM article_processing_items"))).scalars().all()
        source_runs = (await session.execute(select(ArticleIngestionSourceRun.source_id,
                                                    ArticleIngestionSourceRun.state,
                                                    ArticleIngestionSourceRun.candidates,
                                                    ArticleIngestionSourceRun.saved,
                                                    ArticleIngestionSourceRun.updated,
                                                    ArticleIngestionSourceRun.error_code,
                                                    ArticleIngestionSourceRun.selector_study_sha256))).all()
        cursors = (await session.execute(select(SourceCollectionState.source_id,
                                                SourceCollectionState.cursor_offset,
                                                SourceCollectionState.snapshot_at))).all()
        sources = (await session.execute(select(Source.id, Source.name)
                                         .where(Source.name.like("Go Ingestion Gate %")))).all()
        duplicate = await session.scalar(select(Article).where(
            Article.url == "http://rss.fixture.test/story/one/1"))
        enabled_source_count = len((await session.scalars(select(Source.id).where(Source.enabled.is_(True)))).all())
        return {"articles": [{"id": row.id, "source_id": row.source_id, "title": row.title,
                              "url": row.url,
                              "processing_state": row.processing_state,
                              "summary_processed": row.summary_processed} for row in articles],
                "processing_run_states": list(processing_runs),
                "processing_item_states": list(processing_items),
                "duplicate_article": (None if duplicate is None else {
                    "id": duplicate.id,
                    "title": duplicate.title,
                    "content_replaced": duplicate.content != "Pre-existing fixture content",
                    "hash_content_present": isinstance(duplicate.hash_content, str)
                    and len(duplicate.hash_content) == 64
                    and all(char in "0123456789abcdef" for char in duplicate.hash_content),
                }),
                "runs": [{"state": row.state, "result": row.result, "error_code": row.error_code}
                         for row in runs],
                "source_runs": [{"source_id": row.source_id, "state": row.state,
                                 "candidates": row.candidates, "saved": row.saved,
                                 "updated": row.updated,
                                 "error_code": row.error_code,
                                 "selector_study_sha256": row.selector_study_sha256}
                                for row in source_runs],
                "cursors": [{"source_id": row.source_id, "cursor_offset": row.cursor_offset,
                             "snapshot_at": row.snapshot_at.isoformat() if row.snapshot_at else None}
                            for row in cursors],
                "enabled_source_count": enabled_source_count,
                "source_ids": {name: source_id for source_id, name in sources}}


def main():
    action = sys.argv[1]
    if action == "health":
        status, value = request("GET", "/health")
    elif action == "enqueue":
        status, value = request("POST", "/api/v1/process/run", {})
    elif action == "job":
        status, value = request("GET", "/api/v1/jobs/" + sys.argv[2])
    elif action == "snapshot":
        value = asyncio.run(snapshot())
        status = 200
    elif action == "fake-counters":
        with urlopen("http://fixture-services:8090/__gate/counters", timeout=5) as response:
            value = json.loads(response.read())
        status = 200
    elif action == "origin-counters":
        with urlopen("http://pm.fixture.test/__gate/counters", timeout=5) as response:
            value = json.loads(response.read())
        status = 200
    else:
        raise SystemExit("unknown_action")
    print(json.dumps({"http_status": status, "response": value}, separators=(",", ":")))


if __name__ == "__main__":
    main()
