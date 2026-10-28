"""Thin compatibility adapter for native article analysis RPC."""

from __future__ import annotations

from datetime import datetime

from .ai_runtime import AIRuntimeError, AIRuntimeProviderError, get_ai_runtime


class ArticleAnalysisError(RuntimeError):
    """Finite analysis failure without provider payload or article content."""

    def __init__(self, code: str, *, status_code: int | None = None,
                 retry_after_seconds: float | None = None, retryable_transport: bool = False):
        self.code = code
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.retryable_transport = retryable_transport
        super().__init__(code)


async def invalidate_category_metadata_cache() -> None:
    await get_ai_runtime().invalidate_categories()


async def analyze_article(title: str, content: str, url: str, original_context: str | None,
                          *, model: str, max_text_chars: int, deadline: datetime) -> dict:
    try:
        return await get_ai_runtime().analyze(
            title, content, url, original_context, max_text_chars, model, deadline
        )
    except AIRuntimeProviderError as exc:
        raise ArticleAnalysisError(exc.code, status_code=exc.status_code,
                                   retry_after_seconds=exc.retry_after_seconds) from None
    except AIRuntimeError as exc:
        # A local RPC failure or deadline is deliberately status-less. Callers
        # must not mistake it for a retryable provider 5xx.
        raise ArticleAnalysisError(exc.code) from None
