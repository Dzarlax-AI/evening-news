"""Explicit article outcomes and durable retry scheduling."""

import math
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


def _valid_retry_after(value, now):
    """Return a usable Retry-After delay without letting malformed values break scheduling."""
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if isinstance(value, bool) or not math.isfinite(seconds) or seconds < 0:
        return 0
    return min(seconds, max(0, (datetime.max - now).total_seconds() - 1))


def outcome_fields(outcome, attempts, now, *, max_attempts=3,
                   initial_seconds=1800, later_seconds=21600):
    """Translate one whole-pipeline outcome into persisted scheduling fields."""
    state = outcome.state
    next_attempt = None
    persisted_attempts = attempts
    if state == "retry_wait":
        local_admission_busy = outcome.error in {"busy", "native_browser_busy"}
        if local_admission_busy:
            # The claim was persisted before work began, but local admission
            # pressure did not start a genuine processing attempt.
            persisted_attempts = max(0, attempts - 1)
            delay = 60
            next_attempt = now + timedelta(seconds=max(delay, _valid_retry_after(
                outcome.retry_after_seconds, now)))
        elif attempts >= max_attempts:
            state = "exhausted"
        else:
            delay = initial_seconds if attempts == 1 else later_seconds
            delay = max(delay, _valid_retry_after(outcome.retry_after_seconds, now))
            next_attempt = now + timedelta(seconds=delay)
    return {
        **outcome.fields,
        "processing_attempts": persisted_attempts,
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
