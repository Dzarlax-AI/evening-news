"""Fixture-only provider routing and web/executor process separation guards."""

import os

role = os.environ.get("SCHEDULER_GATE_ROLE")

if role == "web":
    from news_aggregator.orchestrator import NewsOrchestrator
    from news_aggregator.services.ai_client import AIClient

    def _forbid_web_orchestrator(self, *args, **kwargs):
        raise RuntimeError("fixture_web_orchestrator_forbidden")

    def _forbid_web_ai_client(self, *args, **kwargs):
        raise RuntimeError("fixture_web_ai_client_forbidden")

    NewsOrchestrator.__init__ = _forbid_web_orchestrator
    AIClient.__init__ = _forbid_web_ai_client

if role in {"web", "executor"}:
    from news_aggregator.services.telegram_service import TelegramService

    _original_init = TelegramService.__init__

    def _fixture_telegram_init(self, *args, **kwargs):
        _original_init(self, *args, **kwargs)
        self.api_url = "http://fixture-services:8090/telegram"

    TelegramService.__init__ = _fixture_telegram_init
