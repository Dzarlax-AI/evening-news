"""RSS compatibility adapter for Go-collected source results."""

from datetime import datetime
from typing import AsyncGenerator, Optional

import pytz

from .base import BaseSource, Article, SourceType
from ..core.exceptions import SourceError


class RSSSource(BaseSource):
    """Convert the Go RSS DTO into the established Python Article model."""

    async def fetch_articles(self, limit: Optional[int] = None) -> AsyncGenerator[Article, None]:
        raise SourceError('rss_collection_owned_by_go')
        yield  # Keep the async-generator interface used by BaseSource.

    @staticmethod
    def article_from_go(data) -> Article:
        published_at = data.get('published_at')
        if published_at is not None:
            if not isinstance(published_at, str):
                raise SourceError('invalid_rss_published_at')
            try:
                published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            except ValueError:
                raise SourceError('invalid_rss_published_at') from None
            if published_at.tzinfo is not None:
                published_at = published_at.astimezone(pytz.UTC).replace(tzinfo=None)
        raw_data = data.get('raw_data')
        if not isinstance(raw_data, dict):
            raise SourceError('invalid_rss_raw_data')
        return Article(
            title=data['title'], url=data['url'], content=data.get('content'),
            summary=data.get('summary'), image_url=data.get('image_url'),
            published_at=published_at, source_type=data.get('source_type', SourceType.RSS),
            source_name=data.get('source_name', ''),
            raw_data={key: raw_data.get(key) for key in ('guid', 'author', 'tags')},
        )

    async def test_connection(self) -> bool:
        """Collection checks are performed by SourceManagerServiceClient."""
        raise SourceError('source_test_owned_by_go')
