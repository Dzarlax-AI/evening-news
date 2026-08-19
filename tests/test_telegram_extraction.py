"""Tests for Telegram article extraction via browser."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_aggregator.sources.base import Article, SourceInfo, SourceType
from news_aggregator.telegram.telegram_source import TelegramSource


SAMPLE_TELEGRAM_HTML = """
<html>
<head><meta property="og:title" content="Test Channel"></head>
<body>
<section class="tgme_channel_history">

  <div class="tgme_widget_message" data-post="testchannel/101">
    <div class="tgme_widget_message_bubble">
      <div class="tgme_widget_message_text">
        Первая новость дня: произошло важное событие в мире технологий.
        Компания представила новый продукт, который изменит рынок.
      </div>
      <div class="tgme_widget_message_info">
        <a class="tgme_widget_message_date" href="https://t.me/testchannel/101">
          <time datetime="2026-04-06T10:00:00+00:00">10:00</time>
        </a>
      </div>
    </div>
  </div>

  <div class="tgme_widget_message" data-post="testchannel/102">
    <div class="tgme_widget_message_bubble">
      <div class="tgme_widget_message_text">
        Вторая новость: обновление платформы добавляет поддержку новых функций.
        Пользователи могут теперь использовать расширенные возможности API.
      </div>
      <div class="tgme_widget_message_info">
        <a class="tgme_widget_message_date" href="https://t.me/testchannel/102">
          <time datetime="2026-04-06T11:00:00+00:00">11:00</time>
        </a>
      </div>
    </div>
  </div>

  <div class="tgme_widget_message" data-post="testchannel/103">
    <div class="tgme_widget_message_bubble">
      <div class="tgme_widget_message_text">Short</div>
    </div>
  </div>

</section>
</body>
</html>
"""


@pytest.fixture
def telegram_source():
    info = SourceInfo(
        name="Test Channel",
        source_type=SourceType.TELEGRAM,
        url="https://t.me/testchannel",
        description="Test Telegram channel",
    )
    return TelegramSource(info)


@pytest.fixture
def mock_browser():
    browser = MagicMock()
    browser.connection = MagicMock()
    browser.connection.closed = False
    return browser


@pytest.fixture
def mock_tab():
    tab = AsyncMock()
    tab.get_content = AsyncMock(return_value=SAMPLE_TELEGRAM_HTML)
    tab.evaluate = AsyncMock(return_value=None)
    tab.wait_for = AsyncMock()
    tab.close = AsyncMock()
    return tab


@pytest.mark.asyncio
async def test_parse_telegram_html(telegram_source):
    """Should extract articles from Telegram HTML."""
    articles = await telegram_source._parse_html(
        SAMPLE_TELEGRAM_HTML, "https://t.me/s/testchannel"
    )

    # Should find 2 valid articles (third is too short)
    assert len(articles) >= 2

    # First article should have content
    first = articles[0]
    assert isinstance(first, Article)
    assert len(first.title) > 0
    assert "технологий" in first.content or "технологий" in first.title


@pytest.mark.asyncio
async def test_parse_telegram_html_skips_short_messages(telegram_source):
    """Messages with very short content should be skipped."""
    articles = await telegram_source._parse_html(
        SAMPLE_TELEGRAM_HTML, "https://t.me/s/testchannel"
    )

    # "Short" message (< 10 chars) should not appear
    for article in articles:
        assert article.content is None or len(article.content or article.title) >= 10


@pytest.mark.asyncio
async def test_parse_telegram_empty_html(telegram_source):
    """Empty page should return no articles."""
    articles = await telegram_source._parse_html(
        "<html><body></body></html>", "https://t.me/s/testchannel"
    )
    assert articles == []


@pytest.mark.asyncio
async def test_fetch_with_browser_yields_articles(telegram_source, mock_browser, mock_tab):
    """_fetch_with_browser should yield Article objects from rendered page."""
    mock_browser.get = AsyncMock(return_value=mock_tab)
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        articles = []
        async for article in telegram_source._fetch_with_browser():
            articles.append(article)

    assert len(articles) >= 2
    # Tab should be closed
    mock_tab.close.assert_awaited()


@pytest.mark.asyncio
async def test_fetch_with_browser_scrolls(telegram_source, mock_browser, mock_tab):
    """Browser fetch should perform scrolling to load more messages."""
    mock_browser.get = AsyncMock(return_value=mock_tab)
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        articles = []
        async for article in telegram_source._fetch_with_browser():
            articles.append(article)

    # Should have called evaluate for scrolling (scrollTo, scrollBy)
    scroll_calls = [
        call for call in mock_tab.evaluate.call_args_list
        if "scroll" in str(call).lower()
    ]
    assert len(scroll_calls) >= 2


@pytest.mark.asyncio
async def test_fetch_with_browser_tries_multiple_urls(telegram_source, mock_browser):
    """Should try alternative URLs if first one fails."""
    success_tab = AsyncMock()
    success_tab.wait_for = AsyncMock()
    success_tab.get_content = AsyncMock(return_value=SAMPLE_TELEGRAM_HTML)
    success_tab.evaluate = AsyncMock()
    success_tab.close = AsyncMock()

    # First call fails, second succeeds
    mock_browser.get = AsyncMock(side_effect=[Exception("CDP navigation failed"), success_tab])
    with patch(
        "news_aggregator.core.browser_pool.get_browser",
        AsyncMock(return_value=mock_browser),
    ):
        articles = []
        async for article in telegram_source._fetch_with_browser():
            articles.append(article)

    assert len(articles) >= 2
    assert mock_browser.get.call_count == 2
