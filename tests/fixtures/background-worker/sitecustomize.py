"""Fixture-only process guards and delivery routing for the Docker gate."""

import os

role = os.environ.get("BG_GATE_ROLE")

if role == "web":
    from news_aggregator.services.ai_client import AIClient

    def _forbid_web_ai_client(self, *args, **kwargs):
        raise RuntimeError("fixture_web_ai_client_forbidden")

    AIClient.__init__ = _forbid_web_ai_client

if role in {"web", "worker", "executor"}:
    from news_aggregator.services.telegram_service import TelegramService

    _original_init = TelegramService.__init__

    def _fixture_telegram_init(self, *args, **kwargs):
        _original_init(self, *args, **kwargs)
        self.api_url = os.environ["BG_GATE_TELEGRAM_URL"]

    TelegramService.__init__ = _fixture_telegram_init
