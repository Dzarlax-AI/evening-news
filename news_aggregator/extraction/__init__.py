"""Content extraction compatibility facade backed exclusively by the Go service."""

import asyncio
import weakref

from .core_extractor import CoreExtractor
from .extraction_utils import ExtractionUtils
from .extraction_logger import ExtractionLogger, get_extraction_logger


class ContentExtractor:
    """Keep the established Python API while delegating all extraction to Go."""

    def __init__(self):
        self.utils = ExtractionUtils()
        self.core_extractor = CoreExtractor(self.utils)
        _ACTIVE_EXTRACTORS.add(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_browser()

    async def close_browser(self):
        return await self.core_extractor.close_browser()

    async def extract_article_content_with_metadata(self, url: str, retry_count: int = 3):
        return await self.core_extractor.extract_article_content_with_metadata(url, retry_count)

    async def extract_article_content(self, url: str, retry_count: int = 2):
        return await self.core_extractor.extract_article_content(url, retry_count)


async def get_content_extractor():
    return ContentExtractor()


async def cleanup_content_extractor():
    extractors = [extractor for extractor in list(_ACTIVE_EXTRACTORS) if extractor]
    if extractors:
        await asyncio.gather(*(extractor.close_browser() for extractor in extractors),
                             return_exceptions=True)
    _ACTIVE_EXTRACTORS.clear()


_ACTIVE_EXTRACTORS = weakref.WeakSet()

__all__ = ['ContentExtractor', 'get_content_extractor', 'cleanup_content_extractor',
           'CoreExtractor', 'ExtractionUtils', 'ExtractionLogger', 'get_extraction_logger']
