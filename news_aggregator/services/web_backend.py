"""Narrow operations retained by the Go web frontend."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text

logger = logging.getLogger(__name__)

MAX_BACKUP_BYTES = 64 * 1024 * 1024
_SAFE_BACKUP = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.tar\.gz$")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CategoryAnalysisRequest(StrictModel):
    limit: int = Field(default=50, ge=1, le=50)
    include_examples: bool = True
    confidence_threshold: float = Field(default=0.7, ge=0, le=1)


class BackupFilename(StrictModel):
    filename: str = Field(min_length=1, max_length=190)


class BackupCleanupRequest(StrictModel):
    keep_count: int = Field(default=5, ge=0, le=100)


class BackupScheduleRequest(StrictModel):
    enabled: bool
    frequency: str = Field(pattern=r"^(daily|weekly|monthly)$")
    time: str = Field(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    keep_count: int = Field(ge=1, le=100)


class EmptyRequest(StrictModel):
    pass


class BackupTooLarge(ValueError):
    """Upload file exceeded its configured cap."""


def validate_backup_name(name: str) -> str:
    if not _SAFE_BACKUP.fullmatch(name) or name in {".", ".."}:
        raise ValueError("invalid backup filename")
    return name


def backup_path(root: Path, name: str) -> Path:
    validate_backup_name(name)
    root = root.resolve()
    candidate = root / name
    # Reject symlinks, including a symlink that resolves back into the root.
    if candidate.is_symlink() or candidate.resolve().parent != root:
        raise ValueError("invalid backup path")
    return candidate


def list_backups(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    items = []
    for path in root.glob("*.tar.gz"):
        if path.is_symlink() or not path.is_file():
            continue
        stat = path.stat()
        # The existing admin backup page consumes both the exact byte count
        # and its display-oriented fields. Keep the relative path confined to
        # the backup root; never expose the container's absolute filesystem.
        items.append({"filename": path.name, "filepath": f"backups/{path.name}",
                      "size": stat.st_size, "size_mb": round(stat.st_size / (1024 * 1024), 2),
                      "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat()})
    items.sort(key=lambda item: item["created_at"], reverse=True)
    return {"backups": items, "total": len(items), "timestamp": datetime.utcnow().isoformat()}


async def save_upload(root: Path, name: str, source: BinaryIO) -> int:
    """Write an upload through a same-directory temporary file and atomic rename."""
    path = backup_path(root, name)
    root.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".upload-", suffix=".tmp", dir=root)
    size = 0
    try:
        with os.fdopen(fd, "wb") as output:
            while True:
                chunk = await asyncio.to_thread(source.read, 1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_BACKUP_BYTES:
                    raise BackupTooLarge("backup exceeds upload limit")
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        if path.is_symlink():
            raise ValueError("invalid backup path")
        os.replace(temporary, path)
        return size
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def cleanup_backups(root: Path, keep_count: int) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    files = [p for p in root.glob("*.tar.gz") if not p.is_symlink() and p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    removed = 0
    for path in files[keep_count:]:
        path.unlink()
        removed += 1
    return {"message": f"Cleanup completed. Kept {min(len(files), keep_count)} backups.",
            "deleted": removed, "remaining": len(files) - removed,
            "timestamp": datetime.utcnow().isoformat()}


async def invalidate_category_cache() -> None:
    """Clear category cache after a category CRUD operation."""
    from .category_cache import get_category_cache
    cache = get_category_cache()
    await cache.invalidate()


async def analyze_categories(db, request: CategoryAnalysisRequest) -> dict:
    """Retained AI category operation with the legacy prompt and response shape."""
    from fastapi import HTTPException
    from ..config import settings
    from ..models import Category
    from .ai_client import get_ai_client
    if settings.background_execution_mode == "worker":
        raise HTTPException(status_code=409, detail="AI category analysis is unavailable in queue-only web mode")
    started = time.time()
    query = text("""
        SELECT DISTINCT ac.ai_category, COUNT(*) AS usage_count,
               STRING_AGG(DISTINCT a.title, ' | ') AS example_titles
        FROM article_categories ac
        JOIN articles a ON ac.article_id = a.id
        LEFT JOIN category_mapping cm ON LOWER(ac.ai_category) = LOWER(cm.ai_category)
        WHERE ac.ai_category IS NOT NULL AND ac.ai_category != '' AND cm.ai_category IS NULL
        GROUP BY ac.ai_category ORDER BY usage_count DESC, ac.ai_category LIMIT :limit
    """)
    rows = (await db.execute(query, {"limit": request.limit})).fetchall()
    if not rows:
        return {"suggestions": [], "total_analyzed": 0,
                "execution_time_seconds": time.time() - started}
    categories = (await db.execute(select(Category.name, Category.display_name).order_by(Category.name))).fetchall()
    category_names = [f"{name} ({display})" for name, display in categories]
    ai = get_ai_client()
    output = []
    for row in rows:
        name, usage_count = row.ai_category, row.usage_count
        examples = (row.example_titles or "").split(" | ")[:5]
        prompt = f'''Analyze this AI-generated category and suggest the top 3 most appropriate mappings to our fixed categories.

AI CATEGORY: "{name}"
USAGE COUNT: {usage_count} articles
EXAMPLE ARTICLES: {examples[:3]}

AVAILABLE FIXED CATEGORIES:
{chr(10).join([f"- {cat}" for cat in category_names])}

TASK: Provide 3 ranked suggestions for mapping this AI category to fixed categories.

Response in JSON format:
{{
    "suggestions": [
        {{"category": "Most appropriate category name (exact match from list above)", "confidence": 0.85, "reasoning": "Brief explanation why this is the best match"}},
        {{"category": "Second best category name", "confidence": 0.65, "reasoning": "Brief explanation for second choice"}},
        {{"category": "Third alternative category name", "confidence": 0.45, "reasoning": "Brief explanation for third choice"}}
    ]
}}

Requirements:
- Always provide exactly 3 suggestions
- Categories must be exact matches from the available list
- Confidence scores should reflect semantic similarity
- Consider semantic similarity, example articles context, and categorization patterns
- Order suggestions by confidence (highest first)'''
        try:
            answer = await ai._call_summary_llm(prompt=prompt, system_prompt=(
                "You are a news categorization expert. Analyze categories and provide accurate mappings. "
                "Always respond with valid JSON."))
            if answer is None:
                raise ValueError("AI response is None")
            clean = answer.strip()
            if not clean.startswith("{"):
                index = clean.find("{")
                if index == -1:
                    raise json.JSONDecodeError("No JSON found in response", answer, 0)
                clean = clean[index:]
            suggestion_data = json.loads(clean).get("suggestions", [])
            if suggestion_data:
                primary = suggestion_data[0]
                chosen = primary.get("category", "Other").split(" (")[0]
                confidence = float(primary.get("confidence", 0.5))
                reasoning = primary.get("reasoning", "AI analysis")
                all_suggestions = [{"category": item.get("category", "Other").split(" (")[0],
                                    "confidence": float(item.get("confidence", 0.0)),
                                    "reasoning": item.get("reasoning", "")}
                                   for item in suggestion_data[:3]]
            else:
                chosen, confidence, reasoning = "Other", 0.3, "No AI suggestions provided"
                all_suggestions = [{"category": "Other", "confidence": 0.3,
                                    "reasoning": "Fallback suggestion"}]
        except json.JSONDecodeError as error:
            logger.info("JSON parsing failed for category %s: %s", name, str(error)[:50])
            chosen = _suggest_fixed_category(name)
            confidence = 0.3
            reasoning = f"Fallback to keyword-based analysis (JSON error: {str(error)[:50]})"
            all_suggestions = [{"category": chosen, "confidence": confidence, "reasoning": reasoning}]
        except Exception as error:
            logger.info("AI analysis failed for category %s: %s", name, type(error).__name__)
            chosen = _suggest_fixed_category(name)
            confidence = 0.3
            reasoning = f"Keyword-based analysis (AI error: {type(error).__name__})"
            all_suggestions = [{"category": chosen, "confidence": confidence, "reasoning": reasoning}]
        output.append({"ai_category": name, "suggested_fixed_category": chosen,
                       "confidence": confidence, "usage_count": usage_count,
                       "example_articles": examples if request.include_examples else [],
                       "reasoning": reasoning, "all_suggestions": all_suggestions})
    output.sort(key=lambda item: item["confidence"], reverse=True)
    return {"suggestions": output, "total_analyzed": len(output),
            "execution_time_seconds": time.time() - started}


def _suggest_fixed_category(ai_category: str) -> str:
    value = (ai_category or "").lower()
    rules = (
        ("Tech", ("tech", "software", "innovation", "digital", "ai", "computer", "programming")),
        ("Business", ("business", "economy", "finance", "market", "trade", "investment", "company")),
        ("Politics", ("politics", "government", "election", "policy", "law", "legal", "parliament")),
        ("International", ("international", "world", "global", "foreign", "europe", "russia", "china")),
        ("Serbia", ("serbia", "belgrade", "serbian", "вучич", "белград", "сербия")),
        ("Science", ("science", "research", "study", "health", "medicine", "environment", "education")),
    )
    for category, keywords in rules:
        if any(keyword in value for keyword in keywords):
            return category
    return "Other"


async def telegram_test(db) -> dict:
    from ..models import Setting
    from .telegram_service import get_telegram_service
    chat_id = await db.get(Setting, "telegram_chat_id")
    service_chat_id = await db.get(Setting, "telegram_service_chat_id")
    service = get_telegram_service(
        chat_id=chat_id.value if chat_id else None,
        service_chat_id=service_chat_id.value if service_chat_id else None,
    )
    if asyncio.iscoroutine(service):
        service = await service
    result = await service.test_connection()
    return {"success": bool(result), "message": "Тест отправлен" if result else "Ошибка отправки",
            "error": None if result else result.error_summary(), "timestamp": datetime.utcnow().isoformat()}


async def telegram_configuration(db) -> dict:
    """Return effective Telegram destination metadata without exposing its token."""
    from ..config import settings
    from ..models import Setting
    chat = await db.get(Setting, "telegram_chat_id")
    service = await db.get(Setting, "telegram_service_chat_id")
    chat_override = chat.value if chat else None
    service_override = service.value if service else None
    effective_chat = chat_override or settings.telegram_chat_id
    effective_service = service_override or settings.telegram_service_chat_id or effective_chat
    return {
        "token_configured": bool(settings.telegram_token),
        "chat_id": effective_chat,
        "chat_id_source": "db" if chat_override else "env",
        "service_chat_id": effective_service,
        "service_chat_id_source": "db" if service_override else (
            "env" if settings.telegram_service_chat_id else "fallback"
        ),
    }


def reset_telegram_service() -> None:
    from .telegram_service import reset_telegram_service as reset
    reset()
