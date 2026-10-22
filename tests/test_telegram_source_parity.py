"""Offline differential tests for the Go Telegram channel parser."""

import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from news_aggregator.sources.base import SourceInfo, SourceType
from news_aggregator.telegram.telegram_source import TelegramSource


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/telegram/channel.html"
BASELINE = ROOT / "tests/fixtures/telegram/python_baseline.json"
PAGE_URL = "https://t.me/s/testchannel"
SOURCE_NAME = "Offline Telegram Fixture"
FALLBACK = datetime(2026, 10, 22, 12, 34, 56, tzinfo=timezone.utc)
CASES = {
    "channel": (FIXTURE, PAGE_URL, SOURCE_NAME),
    "fallback_message": (ROOT / "tests/fixtures/telegram/fallback_message.html", "https://t.me/testchannel", SOURCE_NAME),
    "fallback_data_post": (ROOT / "tests/fixtures/telegram/fallback_data_post.html", "https://t.me/s/testchannel", SOURCE_NAME),
    "malformed": (ROOT / "tests/fixtures/telegram/malformed.html", PAGE_URL, SOURCE_NAME),
}


def _utc_string(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _python_baseline(html, page_url=PAGE_URL, source_name=SOURCE_NAME):
    source = TelegramSource(
        SourceInfo(
            name=source_name,
            source_type=SourceType.TELEGRAM,
            url=page_url,
            description="synthetic offline fixture",
        )
    )
    parser = source.message_parser
    async def external_content_stub(_link, _short_content):
        return None

    parser._try_extract_full_content = external_content_stub
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return FALLBACK.replace(tzinfo=None)
            return FALLBACK.astimezone(tz)

    with patch("news_aggregator.telegram.message_parser.datetime", FixedDateTime):
        articles = asyncio.run(source._parse_html(html, page_url))
    result = []
    for article in articles:
        row = asdict(article)
        row["published_at"] = _utc_string(row["published_at"])
        for media in row["media_files"] or []:
            if media.get("type") in {"poll", "location", "contact"}:
                prefix = media["type"]
                if isinstance(media.get("url"), str) and media["url"].startswith(f"#{prefix}_"):
                    media["url"] = f"#{prefix}_<runtime-hash>"
        result.append(row)
    return result


def test_go_telegram_channel_matches_python_article_baseline():
    python_articles = {
        name: _python_baseline(path.read_text(encoding="utf-8"), page_url, source_name)
        for name, (path, page_url, source_name) in CASES.items()
    }
    assert BASELINE.exists(), "run tests/fixtures/telegram/export_baseline.py to create the Python baseline"
    assert json.loads(BASELINE.read_text(encoding="utf-8")) == python_articles
