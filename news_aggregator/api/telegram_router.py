"""Telegram API router - handles Telegram digest operations and settings."""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..orchestrator import NewsOrchestrator
from ..config import settings as app_settings

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Settings helpers ──────────────────────────────────────────────────────────

async def _load_telegram_settings_from_db(db: AsyncSession) -> dict:
    """Read telegram chat ID overrides from the Setting table."""
    from ..models import Setting
    chat_id_row = await db.get(Setting, "telegram_chat_id")
    service_chat_id_row = await db.get(Setting, "telegram_service_chat_id")
    return {
        "chat_id_db": chat_id_row.value if chat_id_row else None,
        "service_chat_id_db": service_chat_id_row.value if service_chat_id_row else None,
    }


async def _get_telegram_service_with_db_overrides(db: AsyncSession):
    """Build TelegramService using DB overrides on top of env config."""
    from ..services.telegram_service import get_telegram_service
    db_settings = await _load_telegram_settings_from_db(db)
    return get_telegram_service(
        chat_id=db_settings["chat_id_db"] or None,
        service_chat_id=db_settings["service_chat_id_db"] or None,
    )


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class TelegramSettingsUpdate(BaseModel):
    chat_id: Optional[str] = None
    service_chat_id: Optional[str] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/settings")
async def get_telegram_settings(db: AsyncSession = Depends(get_db)):
    """Return current effective Telegram settings."""
    db_vals = await _load_telegram_settings_from_db(db)

    effective_chat_id = db_vals["chat_id_db"] or app_settings.telegram_chat_id
    effective_service_chat_id = (
        db_vals["service_chat_id_db"]
        or app_settings.telegram_service_chat_id
        or effective_chat_id
    )

    return {
        "token_configured": bool(app_settings.telegram_token),
        "chat_id": effective_chat_id,
        "chat_id_source": "db" if db_vals["chat_id_db"] else "env",
        "service_chat_id": effective_service_chat_id,
        "service_chat_id_source": "db" if db_vals["service_chat_id_db"] else (
            "env" if app_settings.telegram_service_chat_id else "fallback"
        ),
    }


@router.post("/settings")
async def update_telegram_settings(
    payload: TelegramSettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Save Telegram channel ID overrides to DB and reset the service singleton."""
    from ..models import Setting
    from ..services.telegram_service import reset_telegram_service

    updates = {
        "telegram_chat_id": (payload.chat_id or "").strip(),
        "telegram_service_chat_id": (payload.service_chat_id or "").strip(),
    }

    for key, value in updates.items():
        existing = await db.get(Setting, key)
        if value:
            if existing:
                existing.value = value
            else:
                db.add(Setting(key=key, value=value, description=f"Telegram {key} (UI override)"))
        else:
            # Empty → remove override, fall back to env
            if existing:
                await db.delete(existing)

    await db.commit()
    reset_telegram_service()

    return {"success": True, "message": "Настройки сохранены"}


@router.post("/test")
async def test_telegram_connection(db: AsyncSession = Depends(get_db)):
    """Send a test message to the service channel."""
    try:
        service = await _get_telegram_service_with_db_overrides(db)
        result = await service.test_connection()
        return {
            "success": bool(result),
            "message": "Тест отправлен" if result else "Ошибка отправки",
            "error": None if result else result.error_summary(),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send-digest")
async def send_telegram_digest():
    """Generate and send Telegram digest."""
    orchestrator = None
    try:
        logger.info("📱 Generating Telegram digest...")
        orchestrator = NewsOrchestrator()
        await orchestrator.start()

        result = await orchestrator.send_telegram_digest()

        success = result.get('success', False)
        response_body = {
            "success": success,
            "message": "Telegram digest sent successfully" if success else "Digest generation failed",
            "result": result,
            "timestamp": datetime.utcnow().isoformat(),
        }
        if not success:
            return JSONResponse(status_code=502, content=response_body)
        return response_body

    except Exception as e:
        logger.error(f"Error sending Telegram digest: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send digest: {str(e)}")
    finally:
        if orchestrator is not None:
            await orchestrator.stop()
