"""Tests for website article extraction via browser (nodriver)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_aggregator.sources.base import SourceType
from news_aggregator.sources.page_monitor_source import (
    PageMonitorSource,
    PageMonitorConfig,
    PageSnapshot,
)


SAMPLE_NEWS_HTML = """
<html>
<head>
  <title>Tech News Daily</title>
  <meta property="og:title" content="Tech News Daily">
</head>
<body>
<main>
  <article>
    <h2><a href="/articles/ai-breakthrough">AI Makes Major Breakthrough in Language Understanding</a></h2>
    <time datetime="2026-04-06">April 6, 2026</time>
    <p>Researchers announced a significant advancement in natural language processing
    that allows models to understand context with unprecedented accuracy.</p>
  </article>

  <article>
    <h2><a href="/articles/quantum-computing">Quantum Computing Reaches New Milestone</a></h2>
    <time datetime="2026-04-05">April 5, 2026</time>
    <p>A new quantum processor has demonstrated error correction capabilities
    that bring practical quantum computing closer to reality.</p>
  </article>

  <article>
    <h2><a href="/articles/space-mission">New Space Mission Announced for 2027</a></h2>
    <time datetime="2026-04-04">April 4, 2026</time>
    <p>The space agency revealed plans for an ambitious deep space mission
    targeting a near-Earth asteroid for resource exploration.</p>
  </article>
</main>
</body>
</html>
"""

SAMPLE_BLOG_HTML = """
<html>
<body>
<div class="blog-posts">
  <div class="post">
    <h3><a href="/blog/docker-tips">10 Docker Tips You Should Know</a></h3>
    <span class="date">2026-04-06</span>
    <div class="excerpt">Learn the most useful Docker tricks for optimizing your
    container builds and improving development workflow.</div>
  </div>
  <div class="post">
    <h3><a href="/blog/go-patterns">Go Design Patterns in Practice</a></h3>
    <span class="date">2026-04-05</span>
    <div class="excerpt">Practical examples of implementing common design patterns
    in Go with real-world use cases and benchmarks.</div>
  </div>
</div>
</body>
</html>
"""


@pytest.mark.parametrize("wait_timeout_ms", [0, -1, float("inf"), float("nan")])
def test_page_monitor_config_rejects_invalid_browser_timeout(wait_timeout_ms):
    with pytest.raises(ValueError, match="wait_timeout_ms"):
        PageMonitorConfig(
            url="https://example.com",
            name="Example",
            wait_timeout_ms=wait_timeout_ms,
        )


@pytest.fixture
def monitor_config():
    return PageMonitorConfig(
        url="https://technews.example.com",
        name="Tech News",
        article_selectors=["article", ".post", ".entry"],
        title_selectors=["h2", "h3", ".title"],
        use_browser=True,
        wait_for_js=False,
        wait_timeout_ms=30000,
        max_articles_per_check=20,
        min_title_length=5,
        enable_ai_analysis=False,
        reanalyze_after_failures=5,
    )


@pytest.fixture
def page_monitor(monitor_config):
    return PageMonitorSource(monitor_config)


@pytest.fixture
def mock_browser():
    browser = MagicMock()
    browser.connection = MagicMock()
    browser.connection.closed = False
    return browser


def make_mock_tab(html_content):
    tab = AsyncMock()
    tab.get_content = AsyncMock(return_value=html_content)
    tab.send = AsyncMock()
    tab.get = AsyncMock()
    tab.close = AsyncMock()
    return tab


@pytest.mark.asyncio
async def test_extract_articles_from_news_html(page_monitor):
    """Should extract articles from a standard news site HTML."""
    articles = await page_monitor._extract_articles_from_html(SAMPLE_NEWS_HTML)

    assert len(articles) >= 2
    titles = [a.get("title", "") for a in articles]
    assert any("AI" in t or "Breakthrough" in t for t in titles)


@pytest.mark.asyncio
async def test_extract_articles_from_blog_html(page_monitor):
    """Should extract articles from a blog-style HTML."""
    # Override selectors for blog format
    page_monitor.config.article_selectors = [".post", "article"]
    page_monitor.config.title_selectors = ["h3", "h2"]

    articles = await page_monitor._extract_articles_from_html(SAMPLE_BLOG_HTML)

    assert len(articles) >= 2
    titles = [a.get("title", "") for a in articles]
    assert any("Docker" in t for t in titles)


@pytest.mark.asyncio
async def test_extract_articles_preserves_links(page_monitor):
    """Extracted articles should have URLs from href attributes."""
    articles = await page_monitor._extract_articles_from_html(SAMPLE_NEWS_HTML)

    links = [a.get("link", "") for a in articles if a.get("link")]
    assert len(links) >= 2
    assert any("ai-breakthrough" in link for link in links)


@pytest.mark.asyncio
async def test_extract_articles_empty_html(page_monitor):
    """Empty HTML should return no articles."""
    articles = await page_monitor._extract_articles_from_html(
        "<html><body><p>No articles here</p></body></html>"
    )
    assert len(articles) == 0


@pytest.mark.asyncio
async def test_browser_snapshot_returns_page_snapshot(page_monitor, mock_browser):
    """_take_browser_snapshot should return a PageSnapshot with articles."""
    mock_tab = make_mock_tab(SAMPLE_NEWS_HTML)
    mock_browser.get = AsyncMock(return_value=mock_tab)
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        snapshot = await page_monitor._take_browser_snapshot()

    assert snapshot is not None
    assert isinstance(snapshot, PageSnapshot)
    assert snapshot.url == "https://technews.example.com"
    assert len(snapshot.content_hash) > 0
    # Tab should be closed after snapshot
    mock_tab.close.assert_awaited()


@pytest.mark.asyncio
async def test_browser_snapshot_sets_headers(page_monitor, mock_browser):
    """Browser snapshot should set extra HTTP headers via CDP."""
    mock_tab = make_mock_tab(SAMPLE_NEWS_HTML)
    mock_browser.get = AsyncMock(return_value=mock_tab)
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        await page_monitor._take_browser_snapshot()

    # Should have called send() for setting headers
    assert mock_tab.send.call_count >= 1


@pytest.mark.asyncio
async def test_browser_snapshot_waits_for_js(page_monitor, mock_browser):
    """When wait_for_js=True, should add delay for JS execution."""
    page_monitor.config.wait_for_js = True
    mock_tab = make_mock_tab(SAMPLE_NEWS_HTML)
    mock_browser.get = AsyncMock(return_value=mock_tab)

    import time
    start = time.time()
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        await page_monitor._take_browser_snapshot()
    elapsed = time.time() - start

    # Should have waited ~2 seconds for JS
    assert elapsed >= 1.5


@pytest.mark.asyncio
async def test_browser_snapshot_closes_tab_on_error(page_monitor, mock_browser):
    """Tab should be closed even if extraction fails."""
    failing_tab = AsyncMock()
    failing_tab.send = AsyncMock(side_effect=Exception("CDP failure"))
    failing_tab.get_content = AsyncMock(side_effect=Exception("CDP failure"))
    failing_tab.close = AsyncMock()
    mock_browser.get = AsyncMock(return_value=failing_tab)

    # _take_browser_snapshot propagates the error, _take_page_snapshot catches it
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        try:
            await page_monitor._take_browser_snapshot()
        except Exception:
            pass

    # Tab should still be closed
    failing_tab.close.assert_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_timeout_ms", [0, -1, float("inf"), float("nan")])
async def test_browser_snapshot_rejects_invalid_wait_timeout_override(
    page_monitor, mock_browser, wait_timeout_ms
):
    page_monitor.config.wait_timeout_ms = wait_timeout_ms
    mock_tab = make_mock_tab(SAMPLE_NEWS_HTML)
    mock_browser.get = AsyncMock(return_value=mock_tab)

    with (
        patch(
            "news_aggregator.core.browser_pool.get_browser",
            AsyncMock(return_value=mock_browser),
        ),
        pytest.raises(ValueError, match="finite positive"),
    ):
        await page_monitor._take_browser_snapshot()

    mock_tab.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_content_change_detection(page_monitor):
    """Should detect new articles between snapshots."""
    import hashlib
    import json
    from datetime import datetime

    def _hash(title, link, desc=""):
        key_data = {"title": title, "link": link, "description": desc[:100]}
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()

    old_articles = [
        {"title": "Old Article 1", "link": "/old1", "description": ""},
        {"title": "Old Article 2", "link": "/old2", "description": ""},
    ]
    new_articles_data = [
        {"title": "Old Article 2", "link": "/old2", "description": ""},
        {"title": "New Article 3", "link": "/new3", "description": ""},
    ]

    snapshot1 = PageSnapshot(
        url="https://test.com",
        content_hash=hashlib.md5(b"old").hexdigest(),
        article_hashes={_hash(a["title"], a["link"]) for a in old_articles},
        extracted_items=old_articles,
        timestamp=datetime.utcnow(),
        selectors_used=["article"],
    )

    snapshot2 = PageSnapshot(
        url="https://test.com",
        content_hash=hashlib.md5(b"new").hexdigest(),
        article_hashes={_hash(a["title"], a["link"]) for a in new_articles_data},
        extracted_items=new_articles_data,
        timestamp=datetime.utcnow(),
        selectors_used=["article"],
    )

    page_monitor.last_snapshot = snapshot1
    detected = await page_monitor._detect_new_content(snapshot1, snapshot2)

    # Should detect "New Article 3" as new
    assert len(detected) == 1
    assert detected[0].title == "New Article 3"
