"""Telegram source registry adapter; collection is owned by Go ingestion."""

from typing import AsyncGenerator, Optional

from ..sources.base import Article, BaseSource
from ..core.exceptions import SourceError


class TelegramSource(BaseSource):
    """Minimal registry adapter used to construct Go probe descriptors."""

    @staticmethod
    def go_collection_config(limit=None):
        """Return the bounded Telegram config accepted by the Go probe DTO."""
        if limit is None:
            return {}
        if type(limit) is not int or not 1 <= limit <= 100:
            raise SourceError('invalid_telegram_limit')
        return {'limit': limit}

    async def fetch_articles(self, limit: Optional[int] = None) -> AsyncGenerator[Article, None]:
        raise SourceError('telegram_collection_owned_by_go')
        yield  # Preserve BaseSource's async-generator interface.

    async def test_connection(self) -> bool:
        raise SourceError('source_test_owned_by_go')
