"""Telegram service for sending news digests."""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from ..config import settings
from ..core.http_client import get_http_client
from ..core.exceptions import TelegramError
from ..utils.html_utils import validate_telegram_html, validate_telegram_rich_html, strip_html_tags


class TelegramService:
    """Service for sending messages to Telegram.

    Uses two separate chat IDs:
    - chat_id         — main news channel (digests)
    - service_chat_id — service channel (errors, alerts); falls back to chat_id if not configured

    Optional constructor params allow DB-stored overrides to take precedence over env vars.
    """

    def __init__(
        self,
        chat_id: Optional[str] = None,
        service_chat_id: Optional[str] = None,
    ):
        self.bot_token = self._get_config("TELEGRAM_TOKEN")
        self.chat_id = chat_id or self._get_config("TELEGRAM_CHAT_ID")
        self.service_chat_id = (
            service_chat_id
            or self._get_config("TELEGRAM_SERVICE_CHAT_ID")
            or self.chat_id
        )

        if not all([self.bot_token, self.chat_id]):
            raise TelegramError("Telegram configuration incomplete")

        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"

    def _get_config(self, key: str) -> Optional[str]:
        """Get config value from settings / env."""
        if hasattr(settings, 'get_legacy_config'):
            value = settings.get_legacy_config(key)
            if value:
                return value
        import os
        return os.getenv(key)
    
    async def send_daily_digest(self, digest: str, message_part: Optional[int] = None) -> bool:
        """
        Send daily digest to Telegram.
        
        Args:
            digest: HTML formatted digest
            message_part: Optional part number for split messages
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Validate message length for Telegram limits (4096 chars)
            if len(digest) > 4000:
                logging.warning(f"Digest too long ({len(digest)} chars), truncating")
                digest = digest[:3900] + "..."
            
            # Send message
            success = await self.send_message(digest)
            
            if success:
                part_info = f" (part {message_part})" if message_part else ""
                logging.info(f"Daily digest sent to Telegram{part_info}")
                return True
            else:
                logging.error("Failed to send digest to Telegram")
                return False
                
        except Exception as e:
            logging.error(f"Error sending digest to Telegram: {e}")
            return False
    
    async def send_alert(self, title: str, message: str) -> bool:
        """Send alert to the service channel."""
        try:
            alert_text = f"🚨 <b>{title}</b>\n\n{message}"
            return await self.send_service_message(alert_text)
        except Exception as e:
            logging.error(f"Error sending alert to Telegram: {e}")
            return False
    
    async def send_processing_summary(self, stats: Dict[str, Any]) -> bool:
        """Send processing summary to the service channel."""
        try:
            duration = stats.get('duration_seconds', 0)
            summary = (
                f"📊 <b>Обработка новостей завершена</b>\n\n"
                f"📥 Статей получено: {stats.get('articles_fetched', 0)}\n"
                f"🤖 Статей обработано: {stats.get('articles_processed', 0)}\n"
                f"⏱ Время обработки: {duration:.1f}с\n"
            )
            if stats.get('errors'):
                summary += f"⚠️ Ошибок: {len(stats['errors'])}\n"

            return await self.send_service_message(summary)
        except Exception as e:
            logging.error(f"Error sending processing summary to Telegram: {e}")
            return False
    
    async def send_message(self, text: str) -> bool:
        """Send message to the main news channel (HTML formatted)."""
        return await self._send_to_chat(text, self.chat_id)

    async def send_rich_message(self, html: str, fallback_text: Optional[str] = None) -> bool:
        """Send a Rich Message to the main news channel, falling back to sendMessage."""
        sent = await self._send_rich_to_chat(html, self.chat_id)
        if sent:
            return True

        if fallback_text:
            logging.warning("Telegram Rich Message failed; falling back to regular HTML message")
            return await self.send_message(fallback_text)

        return False

    async def send_service_message(self, text: str) -> bool:
        """Send message to the service channel (errors, alerts)."""
        return await self._send_to_chat(text, self.service_chat_id)

    async def _send_to_chat(self, text: str, chat_id: str) -> bool:
        """Low-level send to a specific chat_id."""
        if not text or not str(text).strip():
            logging.warning("Telegram _send_to_chat called with empty text")
            return False

        cleaned_text = validate_telegram_html(text)
        if cleaned_text is None:
            cleaned_text = strip_html_tags(text)

        url = f"{self.api_url}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": cleaned_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with get_http_client() as client:
                response = await client.post(url, json=data)
                async with response:
                    if response.status == 200:
                        return True
                    error_text = await response.text()
                    logging.error(f"Telegram API error {response.status}: {error_text}")
                    return False
        except Exception as e:
            logging.error(f"Telegram request failed: {e}")
            return False

    async def _send_rich_to_chat(self, html: str, chat_id: str) -> bool:
        """Low-level Rich Message send to a specific chat_id."""
        if not html or not str(html).strip():
            logging.warning("Telegram _send_rich_to_chat called with empty HTML")
            return False

        cleaned_html = validate_telegram_rich_html(html)
        if cleaned_html is None:
            logging.warning("Telegram Rich Message HTML validation failed")
            return False

        url = f"{self.api_url}/sendRichMessage"
        data = {
            "chat_id": chat_id,
            "rich_message": {
                "html": cleaned_html,
            },
        }

        try:
            async with get_http_client() as client:
                response = await client.post(url, json=data)
                async with response:
                    if response.status == 200:
                        return True
                    error_text = await response.text()
                    logging.error(f"Telegram Rich Message API error {response.status}: {error_text}")
                    return False
        except Exception as e:
            logging.error(f"Telegram Rich Message request failed: {e}")
            return False
    
    async def test_connection(self) -> bool:
        """Test Telegram bot connection by sending to the service channel."""
        try:
            test_message = f"🧪 Test message from Evening News v2\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            return await self.send_service_message(test_message)
        except Exception as e:
            logging.error(f"Telegram connection test failed: {e}")
            return False
    
    async def send_message_with_keyboard(self, message: str, inline_keyboard: List[List[dict]]) -> bool:
        """Send message with inline keyboard to the main news channel."""
        if not message or not message.strip():
            logging.warning("Empty message provided")
            return False

        cleaned_message = validate_telegram_html(message)
        if cleaned_message is None:
            cleaned_message = strip_html_tags(message)

        try:
            url = f"{self.api_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": cleaned_message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if inline_keyboard:
                payload["reply_markup"] = {"inline_keyboard": inline_keyboard}

            async with get_http_client() as client:
                response = await client.post(url, json=payload)
                async with response:
                    if response.status == 200:
                        logging.info("Telegram message with keyboard sent successfully")
                        return True
                    error_text = await response.text()
                    logging.error(f"Telegram API error {response.status}: {error_text}")
                    return False
        except Exception as e:
            logging.error(f"Failed to send Telegram message with keyboard: {e}")
            return False


# Global Telegram service instance
_telegram_service: Optional[TelegramService] = None


def get_telegram_service(
    chat_id: Optional[str] = None,
    service_chat_id: Optional[str] = None,
) -> TelegramService:
    """Get (or create) the Telegram service singleton.

    Pass explicit chat_id / service_chat_id to override env/config values —
    used when settings are loaded from DB.
    """
    global _telegram_service

    if _telegram_service is None or chat_id or service_chat_id:
        _telegram_service = TelegramService(
            chat_id=chat_id,
            service_chat_id=service_chat_id,
        )

    return _telegram_service


def reset_telegram_service() -> None:
    """Reset the singleton so the next call re-creates it with fresh settings."""
    global _telegram_service
    _telegram_service = None
