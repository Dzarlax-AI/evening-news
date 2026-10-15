"""Explicit article outcomes and durable retry scheduling."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional


def publication_filter(include_classified_ads=False):
    """Expose completed work, with confirmed ads available in explicit ad views."""
    from sqlalchemy import and_, or_
    from ..models import Article
    completed = Article.processing_state == 'completed'
    if include_classified_ads:
        return or_(completed, and_(Article.processing_state == 'rejected',
                                  Article.ad_processed.is_(True),
                                  Article.is_advertisement.is_(True)))
    return completed


@dataclass(frozen=True)
class ProcessingOutcome:
    state: str
    fields: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    retry_after_seconds: Optional[float] = None


def outcome_fields(outcome, attempts, now, *, max_attempts=3,
                   initial_seconds=1800, later_seconds=21600):
    """Translate one whole-pipeline outcome into persisted scheduling fields."""
    state = outcome.state
    next_attempt = None
    if state == "retry_wait":
        if attempts >= max_attempts:
            state = "exhausted"
        else:
            delay = initial_seconds if attempts == 1 else later_seconds
            delay = max(delay, outcome.retry_after_seconds or 0)
            next_attempt = now + timedelta(seconds=delay)
    return {
        **outcome.fields,
        "processing_state": state,
        "next_attempt_at": next_attempt,
        "last_processing_error": (outcome.error or "")[:2000] or None,
        "processed": state in {"completed", "rejected", "exhausted"},
    }


def is_challenge_text(value):
    """Identify challenge instructions; a vendor or subject name is insufficient."""
    value = (value or "").strip().lower()
    if len(value) > 1500:
        return False
    instructions = (
        "enable javascript and cookies to continue",
        "checking your browser before accessing",
        "checking if the site connection is secure",
        "verify you are human", "verify that you are human",
        "performing security verification", "проверка вашего браузера",
        "включите javascript и файлы cookie",
    )
    return value in {"just a moment", "just a moment...", "access denied", "forbidden"} or any(
        marker in value for marker in instructions
    )
