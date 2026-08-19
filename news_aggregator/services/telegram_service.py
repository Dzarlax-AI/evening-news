"""Telegram service for sending news digests."""

import logging
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime

from ..config import settings
from ..core.http_client import get_http_client
from ..core.exceptions import TelegramError
from ..utils.html_utils import validate_telegram_html, validate_telegram_rich_html, strip_html_tags


@dataclass(frozen=True)
class TelegramSendResult:
    """Truthful result of one Telegram API send attempt."""

    success: bool
    method: str
    chat_id: str
    status_code: Optional[int] = None
    error_code: Optional[int] = None
    description: Optional[str] = None

    def __bool__(self) -> bool:
        return self.success

    def error_summary(self) -> str:
        parts = []
        if self.status_code is not None:
            parts.append(f"HTTP {self.status_code}")
        if self.error_code is not None and self.error_code != self.status_code:
            parts.append(f"Telegram {self.error_code}")
        if self.description:
            parts.append(self.description)
        return ": ".join(parts) or "unknown Telegram error"


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
    
    async def send_daily_digest(self, digest: str, message_part: Optional[int] = None) -> TelegramSendResult:
        """
        Send daily digest to Telegram.
        
        Args:
            digest: HTML formatted digest
            message_part: Optional part number for split messages
            
        Returns:
            Structured Telegram API result.
        """
        try:
            # Validate message length for Telegram limits (4096 chars)
            if len(digest) > 4000:
                logging.warning(f"Digest too long ({len(digest)} chars), truncating")
                digest = digest[:3900] + "..."
            
            # Send message
            result = await self.send_message(digest)
            
            if result:
                part_info = f" (part {message_part})" if message_part else ""
                logging.info(f"Daily digest sent to Telegram{part_info}")
                return result
            else:
                logging.error("Failed to send digest to Telegram: %s", result.error_summary())
                return result
                
        except Exception as e:
            logging.error(f"Error sending digest to Telegram: {e}")
            return TelegramSendResult(False, "sendMessage", self.chat_id, description=str(e))
    
    async def send_alert(self, title: str, message: str) -> TelegramSendResult:
        """Send alert to the service channel."""
        try:
            alert_text = f"🚨 <b>{title}</b>\n\n{message}"
            return await self.send_service_message(alert_text)
        except Exception as e:
            logging.error(f"Error sending alert to Telegram: {e}")
            return TelegramSendResult(False, "sendMessage", self.service_chat_id, description=str(e))
    
    async def send_processing_summary(self, stats: Dict[str, Any]) -> TelegramSendResult:
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
            return TelegramSendResult(False, "sendMessage", self.service_chat_id, description=str(e))
    
    async def send_message(self, text: str) -> TelegramSendResult:
        """Send message to the main news channel (HTML formatted)."""
        return await self._send_to_chat(text, self.chat_id)

    async def send_rich_message(self, html: str, fallback_text: Optional[str] = None) -> TelegramSendResult:
        """Send a Rich Message to the main news channel, falling back to sendMessage."""
        sent = await self._send_rich_to_chat(html, self.chat_id)
        if sent:
            return sent

        if fallback_text:
            logging.warning("Telegram Rich Message failed; falling back to regular HTML message")
            return await self.send_message(fallback_text)

        return sent

    async def send_service_message(self, text: str) -> TelegramSendResult:
        """Send message to the service channel (errors, alerts)."""
        return await self._send_to_chat(text, self.service_chat_id)

    async def _send_to_chat(self, text: str, chat_id: str) -> TelegramSendResult:
        """Low-level send to a specific chat_id."""
        if not text or not str(text).strip():
            logging.warning("Telegram _send_to_chat called with empty text")
            return TelegramSendResult(False, "sendMessage", chat_id, description="empty message")

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
                    error_text = await response.text()
                    payload = self._decode_response(error_text)
                    success = response.status == 200 and payload.get("ok", True) is True
                    result = TelegramSendResult(
                        success=success,
                        method="sendMessage",
                        chat_id=chat_id,
                        status_code=response.status,
                        error_code=payload.get("error_code"),
                        description=payload.get("description"),
                    )
                    if not result:
                        logging.error("Telegram API error: %s", result.error_summary())
                    return result
        except Exception as e:
            logging.error(f"Telegram request failed: {e}")
            return TelegramSendResult(False, "sendMessage", chat_id, description=str(e))

    async def _send_rich_to_chat(self, html: str, chat_id: str) -> TelegramSendResult:
        """Low-level Rich Message send to a specific chat_id."""
        if not html or not str(html).strip():
            logging.warning("Telegram _send_rich_to_chat called with empty HTML")
            return TelegramSendResult(False, "sendRichMessage", chat_id, description="empty HTML")

        cleaned_html = validate_telegram_rich_html(html)
        if cleaned_html is None:
            logging.warning("Telegram Rich Message HTML validation failed")
            return TelegramSendResult(False, "sendRichMessage", chat_id, description="HTML validation failed")

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
                    error_text = await response.text()
                    payload = self._decode_response(error_text)
                    success = response.status == 200 and payload.get("ok", True) is True
                    result = TelegramSendResult(
                        success=success,
                        method="sendRichMessage",
                        chat_id=chat_id,
                        status_code=response.status,
                        error_code=payload.get("error_code"),
                        description=payload.get("description"),
                    )
                    if not result:
                        logging.error("Telegram Rich Message API error: %s", result.error_summary())
                    return result
        except Exception as e:
            logging.error(f"Telegram Rich Message request failed: {e}")
            return TelegramSendResult(False, "sendRichMessage", chat_id, description=str(e))

    @staticmethod
    def _decode_response(body: str) -> Dict[str, Any]:
        if not body:
            return {}
        try:
            payload = json.loads(body)
            return payload if isinstance(payload, dict) else {}
        except (TypeError, ValueError):
            return {"description": body[:500]}
    
    async def test_connection(self) -> TelegramSendResult:
        """Test Telegram bot connection by sending to the service channel."""
        try:
            test_message = f"🧪 Test message from Evening News v2\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            return await self.send_service_message(test_message)
        except Exception as e:
            logging.error(f"Telegram connection test failed: {e}")
            return TelegramSendResult(False, "sendMessage", self.service_chat_id, description=str(e))
    
    async def send_message_with_keyboard(self, message: str, inline_keyboard: List[List[dict]]) -> TelegramSendResult:
        """Send message with inline keyboard to the main news channel."""
        if not message or not message.strip():
            logging.warning("Empty message provided")
            return TelegramSendResult(False, "sendMessage", self.chat_id, description="empty message")

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
                    error_text = await response.text()
                    payload = self._decode_response(error_text)
                    result = TelegramSendResult(
                        success=response.status == 200 and payload.get("ok", True) is True,
                        method="sendMessage",
                        chat_id=self.chat_id,
                        status_code=response.status,
                        error_code=payload.get("error_code"),
                        description=payload.get("description"),
                    )
                    if result:
                        logging.info("Telegram message with keyboard sent successfully")
                    else:
                        logging.error("Telegram API error: %s", result.error_summary())
                    return result
        except Exception as e:
            logging.error(f"Failed to send Telegram message with keyboard: {e}")
            return TelegramSendResult(False, "sendMessage", self.chat_id, description=str(e))


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
