"""Cancellation behavior at the Go SourceManager adapter boundary."""

import asyncio
from types import SimpleNamespace

from pydantic import SecretStr

from news_aggregator.config import settings
from news_aggregator.services.source_manager import SourceManager
from news_aggregator.sources import source_manager_service_client
from news_aggregator.sources.base import SourceType


def test_cancelling_go_fetch_releases_admission_contexts(monkeypatch):
    monkeypatch.setattr(settings, "extraction_service_url", "http://source-manager:8080")
    monkeypatch.setattr(settings, "extraction_service_token", SecretStr("synthetic-service-token"))
    manager = SourceManager()
    entered = asyncio.Event()
    cleaned_up = asyncio.Event()
    calls = []

    class BlockingClient:
        def __init__(self, *_args):
            pass

        async def collect(self, descriptors, **kwargs):
            calls.append((descriptors, kwargs))
            if len(calls) == 1:
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    cleaned_up.set()
            return {
                "results": [{
                    "source_id": 1,
                    "type": "rss",
                    "outcome": "success",
                    "rss_articles": [{
                        "title": "Recovered after cancellation",
                        "url": "https://example.test/recovered",
                        "raw_data": {},
                    }],
                }],
            }

    monkeypatch.setattr(source_manager_service_client, "SourceManagerServiceClient", BlockingClient)
    item = SimpleNamespace(
        id=1,
        name="synthetic rss",
        source_type=SourceType.RSS,
        url="https://example.test/feed.xml",
        enabled=True,
        config={},
    )

    async def run():
        task = asyncio.create_task(manager.fetch_from_all_sources_no_db([item], per_source_timeout=5))
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Go source collection swallowed cancellation")
        assert cleaned_up.is_set()

        results = await asyncio.wait_for(
            manager.fetch_from_all_sources_no_db([item], per_source_timeout=1), timeout=1
        )
        assert [article.title for article in results[1]] == [
            "Recovered after cancellation"
        ]

    asyncio.run(run())
    assert len(calls) == 2
    assert calls[0][0][0]["source_id"] == 1
    assert calls[0][1]["admission_timeout_seconds"] > 0
