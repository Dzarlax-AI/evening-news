"""Domain behavior retained from the retired Python web boundary."""

from types import SimpleNamespace

import pytest


async def test_worker_source_test_probes_disabled_source_without_mutating_enabled_state(monkeypatch):
    from news_aggregator.config import Settings
    import news_aggregator.config as config
    from news_aggregator.services.background_execution import execute_job

    source = SimpleNamespace(
        id=43, name="Disabled worker feed", source_type="rss", url="https://feed.example/rss",
        enabled=False, config={},
    )
    observed = {}

    class SuccessfulClient:
        def __init__(self, *_args):
            pass

        async def collect(self, sources, **_kwargs):
            observed["enabled"] = sources[0]["enabled"]
            return {"results": [{"source_id": 43, "type": "rss", "outcome": "success",
                                  "rss_articles": []}]}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _model, source_id):
            assert source_id == source.id
            return source

        async def commit(self):
            pass

    settings = Settings(_env_file=None,
                        EXTRACTION_SERVICE_URL="http://extractor:8080",
                        EXTRACTION_SERVICE_TOKEN="private-token-value")
    monkeypatch.setattr(config, "get_settings", lambda: settings)
    monkeypatch.setattr("news_aggregator.services.background_execution.AsyncSessionLocal",
                        FakeSession)
    monkeypatch.setattr(
        "news_aggregator.sources.source_manager_service_client.SourceManagerServiceClient",
        SuccessfulClient)

    result = await execute_job(SimpleNamespace(kind="source_test", payload={"source_id": 43}),
                               object())

    assert result == {"success": True}
    assert observed["enabled"] is True
    assert source.enabled is False
