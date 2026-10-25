"""RSS source registry adapter; collection is owned by Go ingestion."""

from typing import AsyncGenerator, Optional

from .base import Article, BaseSource
from ..core.exceptions import SourceError


class RSSSource(BaseSource):
    async def fetch_articles(self, limit: Optional[int] = None) -> AsyncGenerator[Article, None]:
        raise SourceError('rss_collection_owned_by_go')
        yield  # Preserve BaseSource's async-generator interface.

    async def test_connection(self) -> bool:
        raise SourceError('source_test_owned_by_go')
