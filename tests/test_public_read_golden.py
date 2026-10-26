"""Synthetic public read responses from the existing Python DataService contract."""
import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from news_aggregator.config import settings
if not settings.admin_password and not settings.jwt_secret:
    # tests/conftest.py imports config before test modules, so provide harmless
    # import-time values only when the runner did not set real test values.
    os.environ["DATABASE_URL"] = "postgresql+asyncpg://fixture:fixture@127.0.0.1/fixture"
    os.environ["GEMINI_API_KEY"] = "fixture-gemini-key"
    os.environ["ADMIN_PASSWORD"] = "fixture-admin-password"
    os.environ["JWT_SECRET"] = "fixture-jwt-secret"
    settings.admin_password = "fixture-admin-password"
    settings.jwt_secret = "fixture-jwt-secret"

from news_aggregator import public
from news_aggregator.main import app

GOLDEN = Path(__file__).parent / "fixtures/web-contract-v1/public-read-golden.json"
FIXTURE = json.loads(GOLDEN.read_text())


class FakeDataService:
    async def get_articles_feed(self, **kwargs):
        result = dict(FIXTURE["GET /api/public/feed?limit=4&offset=2"])
        result["limit"] = kwargs["limit"]
        result["offset"] = kwargs["offset"]
        return result["articles"]

    async def get_article_by_id(self, article_id, *, include_unpublished=False):
        assert article_id == 42
        return FIXTURE["GET /api/public/article/42"]

    async def get_categories_config(self):
        return FIXTURE["GET /api/public/categories/config"]

    async def get_categories_stats(self, **kwargs):
        return FIXTURE["GET /api/public/categories?since_hours=24"]

    async def get_sources_stats(self, **kwargs):
        return FIXTURE["GET /api/public/sources?hide_ads=false"]


async def asgi_request(url):
    split = urlsplit(url)
    messages = []
    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": "GET", "scheme": "http",
        "path": split.path, "raw_path": split.path.encode(),
        "query_string": split.query.encode(), "root_path": "",
        "headers": [(b"host", b"localhost")], "client": ("127.0.0.1", 12345),
        "server": ("localhost", 80),
    }
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)
    start = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return start["status"], json.loads(body)


def test_public_data_service_success_responses_match_golden():
    async def check():
        app.dependency_overrides[public.get_data_service] = lambda: FakeDataService()
        public._FEED_CACHE.clear()
        try:
            for key in FIXTURE:
                url = key.removeprefix("GET ")
                status, payload = await asgi_request(url)
                assert status == 200, (url, status, payload)
                assert payload == FIXTURE[key], url
        finally:
            app.dependency_overrides.pop(public.get_data_service, None)
            public._FEED_CACHE.clear()
    asyncio.run(check())
