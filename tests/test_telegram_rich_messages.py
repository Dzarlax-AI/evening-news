from unittest.mock import AsyncMock

import pytest

from news_aggregator.processing.digest_builder import DigestBuilder
from news_aggregator.services.telegram_service import TelegramService
from news_aggregator.utils.html_utils import validate_telegram_rich_html


class _FakeResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return ""


class _FakeClient:
    def __init__(self):
        self.posts = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json):
        self.posts.append((url, json))
        return _FakeResponse()


def make_service() -> TelegramService:
    service = TelegramService.__new__(TelegramService)
    service.api_url = "https://api.telegram.org/bottoken"
    service.chat_id = "@news"
    service.service_chat_id = "@service"
    return service


def test_validate_telegram_rich_html_keeps_rich_blocks_and_strips_unsafe_attrs():
    html = '<h3 onclick="x()">Tech</h3><p><a href="javascript:bad()">bad</a></p><details open><summary>More</summary><ul><li>One</li></ul></details>'

    cleaned = validate_telegram_rich_html(html)

    assert "<h3>Tech</h3>" in cleaned
    assert "onclick" not in cleaned
    assert "javascript:" not in cleaned
    assert "<details open" in cleaned
    assert "<ul><li>One</li></ul>" in cleaned


def test_summary_to_rich_blocks_escapes_text_and_groups_bullets():
    builder = DigestBuilder.__new__(DigestBuilder)

    rendered = builder._summary_to_rich_blocks("Intro <script>\n- First item\n* Second item")

    assert "<p>Intro &lt;script&gt;</p>" in rendered
    assert "<ul>" in rendered
    assert "<li>First item</li>" in rendered
    assert "<li>Second item</li>" in rendered


@pytest.mark.asyncio
async def test_send_rich_message_uses_sendrichmessage_payload(monkeypatch):
    service = make_service()
    fake_client = _FakeClient()

    def fake_get_http_client():
        return fake_client

    monkeypatch.setattr("news_aggregator.services.telegram_service.get_http_client", fake_get_http_client)

    sent = await service.send_rich_message("<h2>Digest</h2><p>Hello</p>")

    assert sent is True
    assert len(fake_client.posts) == 1
    url, payload = fake_client.posts[0]
    assert url == "https://api.telegram.org/bottoken/sendRichMessage"
    assert payload["chat_id"] == "@news"
    assert payload["rich_message"]["html"] == "<h2>Digest</h2><p>Hello</p>"
    assert "parse_mode" not in payload


@pytest.mark.asyncio
async def test_send_rich_message_falls_back_to_regular_message():
    service = make_service()
    service._send_rich_to_chat = AsyncMock(return_value=False)
    service.send_message = AsyncMock(return_value=True)

    sent = await service.send_rich_message("<h2>Digest</h2>", fallback_text="<b>Digest</b>")

    assert sent is True
    service._send_rich_to_chat.assert_awaited_once_with("<h2>Digest</h2>", "@news")
    service.send_message.assert_awaited_once_with("<b>Digest</b>")
