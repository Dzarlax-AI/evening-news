"""Integration tests — real browser, real websites.

Run with: pytest tests/test_integration_real.py -v -s
Requires Chrome, network access, and explicit opt-in.
"""

import asyncio
import os

import pytest
import nodriver as uc

# Real network/browser checks are intentionally outside the deterministic suite.
pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_REAL_BROWSER_TESTS") != "1",
    reason="Set RUN_REAL_BROWSER_TESTS=1 to run real browser/network checks",
)


# ── Website extraction ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_real_website_get_content():
    """Load a real page and get rendered HTML."""
    browser = await uc.start(headless=True)
    try:
        tab = await browser.get("https://example.com")
        html = await tab.get_content()

        assert "example domain" in html.lower()
        assert len(html) > 100
    finally:
        browser.stop()


@pytest.mark.asyncio
async def test_real_news_site_extraction():
    """Load Hacker News and extract article titles."""
    from bs4 import BeautifulSoup

    browser = await uc.start(headless=True)
    try:
        tab = await browser.get("https://news.ycombinator.com")
        await asyncio.sleep(2)
        html = await tab.get_content()

        soup = BeautifulSoup(html, "html.parser")
        titles = soup.select(".titleline > a")

        assert len(titles) >= 10, f"Expected ≥10 HN titles, got {len(titles)}"
        for t in titles[:5]:
            assert len(t.get_text(strip=True)) > 0
            assert t.get("href")
    finally:
        browser.stop()


# ── Telegram extraction ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_real_telegram_channel():
    """Load a public Telegram channel and find messages."""
    from bs4 import BeautifulSoup

    browser = await uc.start(headless=True)
    try:
        tab = await browser.get("https://t.me/s/durov")
        await asyncio.sleep(3)
        html = await tab.get_content()

        soup = BeautifulSoup(html, "html.parser")
        messages = soup.select(".tgme_widget_message")

        assert len(messages) >= 1, "Should find at least 1 Telegram message"
    finally:
        browser.stop()


@pytest.mark.asyncio
async def test_real_telegram_full_pipeline():
    """End-to-end: load Telegram, parse with TelegramSource._parse_html."""
    from news_aggregator.sources.base import SourceInfo, SourceType
    from news_aggregator.telegram.telegram_source import TelegramSource

    browser = await uc.start(headless=True)
    try:
        tab = await browser.get("https://t.me/s/durov")
        await asyncio.sleep(3)
        html = await tab.get_content()

        info = SourceInfo(
            name="Durov",
            source_type=SourceType.TELEGRAM,
            url="https://t.me/durov",
            description="Pavel Durov's channel",
        )
        source = TelegramSource(info)
        articles = await source._parse_html(html, "https://t.me/s/durov")

        assert len(articles) >= 1, "Should parse at least 1 article"
        for a in articles[:3]:
            assert a.title
            assert len(a.title) > 0
    finally:
        browser.stop()
