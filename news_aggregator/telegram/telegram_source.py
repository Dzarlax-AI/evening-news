"""Modern modular Telegram source using specialized components."""

import re
from datetime import datetime, timezone
import logging
import asyncio
import time
from urllib.parse import urlsplit
from typing import AsyncGenerator, Optional, Dict, List, Any

from ..sources.base import BaseSource, Article
from ..core.exceptions import SourceError

logger = logging.getLogger(__name__)

class TelegramSource(BaseSource):
    """Modern Telegram public channel source with modular architecture."""
    
    def __init__(self, source_info):
        super().__init__(source_info)
        self.channel_username = self._extract_channel_username()
        self.browser = None

    def _extract_channel_username(self) -> str:
        """Extract channel username from URL."""
        url = self.url.lower().strip()
        
        # Remove protocol and common prefixes
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'^(www\.)?', '', url)
        
        # Extract username from different URL patterns
        patterns = [
            r't\.me/s/([^/?]+)',        # t.me/s/username
            r't\.me/([^/?]+)',          # t.me/username
            r'telegram\.me/s/([^/?]+)', # telegram.me/s/username
            r'telegram\.me/([^/?]+)',   # telegram.me/username
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                username = match.group(1)
                # Clean username
                username = username.split('?')[0]  # Remove query params
                username = username.split('#')[0]  # Remove fragments
                return username
        
        # If no pattern matches, assume the entire cleaned URL is the username
        username = url.split('/')[0].split('?')[0].split('#')[0]
        return username
    
    async def fetch_articles(self, limit: Optional[int] = None) -> AsyncGenerator[Article, None]:
        """Fetch and normalize a bounded Go Telegram collection."""
        async for article in self._fetch_with_go(limit):
            yield article

    async def _fetch_with_go(self, limit: Optional[int]) -> AsyncGenerator[Article, None]:
        """Fetch a validated Telegram batch through the private Go service."""
        from ..config import get_settings
        from .service_client import TelegramServiceClient, TelegramServiceError

        if limit is not None and (type(limit) is not int or limit <= 0):
            raise SourceError('invalid_telegram_limit')
        settings = get_settings()
        if not settings.extraction_service_url or not settings.extraction_service_token:
            raise SourceError('telegram_service_not_configured')
        request_limit = min(limit or 100, 100)
        # SourceManager wraps the full generator in a 120-second timeout and discards
        # accumulated items if that outer timeout fires. Reserve time for final yields.
        source_budget = min(settings.extraction_service_total_budget_seconds, 120.0)
        yield_reserve = min(5.0, source_budget * 0.1)
        deadline = time.monotonic() + max(0.001, source_budget - yield_reserve)
        try:
            client = TelegramServiceClient(
                settings.extraction_service_url,
                settings.extraction_service_token.get_secret_value())
            admission_budget = max(0.0, deadline - time.monotonic())
            response = await client.fetch(
                self.url, self.name, limit=request_limit,
                timeout_seconds=min(30.0, max(0.001, admission_budget)),
                admission_timeout_seconds=admission_budget)
        except (TelegramServiceError, ValueError) as exc:
            code = exc.code if isinstance(exc, TelegramServiceError) else 'invalid_configuration'
            logger.warning('Telegram Go source failed code=%s', code)
            raise SourceError(f'telegram_service_{code}') from None
        except Exception:
            logger.warning('Telegram Go source failed code=unavailable')
            raise SourceError('telegram_service_unavailable') from None

        async for article in self.consume_go_collection(response, settings=settings, deadline=deadline):
            yield article

    async def consume_go_collection(self, response, *, settings=None, deadline=None):
        """Normalize a captured Go Telegram payload and enrich links without refetching the channel."""
        from ..config import get_settings
        settings = settings or get_settings()
        deadline = deadline or (time.monotonic() + min(settings.extraction_service_total_budget_seconds, 120.0))
        articles = [self._article_from_go(item) for item in response.get('articles', [])]
        if response['outcome'] == 'partial':
            logger.warning('Telegram Go source returned partial articles count=%d code=%s',
                           len(articles), response['error']['code'])
        for article in articles:
            remaining = deadline - time.monotonic()
            if (article.raw_data.get('original_link') and article.content
                    and len(article.content) < 200 and remaining > 0.05):
                try:
                    await self._enrich_go_article(article, settings, remaining)
                except Exception:
                    logger.info('Telegram external enrichment skipped code=unavailable')
            # Keep every validated post even when enrichment fails or exhausts budget.
            yield article

        if response['outcome'] in {'failed', 'rejected'}:
            error = response.get('error') or {'code': response['outcome']}
            logger.warning('Telegram Go source returned no articles code=%s', error['code'])
            raise SourceError(f"telegram_service_{error['code']}")

    @staticmethod
    def go_collection_config(limit=None):
        """Return the narrow Telegram config accepted by the Go SourceManager DTO."""
        if limit is None:
            return {}
        if type(limit) is not int or not 1 <= limit <= 100:
            raise SourceError('invalid_telegram_limit')
        return {'limit': limit}

    @staticmethod
    def _article_from_go(data: Dict[str, Any]) -> Article:
        published_at = data['published_at']
        if published_at is not None:
            published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            published_at = published_at.astimezone(timezone.utc).replace(tzinfo=None)
        return Article(
            title=data['title'], url=data['url'], content=data['content'],
            summary=data['summary'], published_at=published_at,
            source_type=data['source_type'], source_name=data['source_name'],
            image_url=data['image_url'], media_files=data['media_files'],
            raw_data=data['raw_data'])

    async def _enrich_go_article(self, article: Article, settings, timeout_seconds: float) -> None:
        """Use the existing Go article API and preserve the source post on failure."""
        from ..extraction.service_client import ExtractionServiceClient, ExtractionServiceError
        from ..extraction.core_extractor import CoreExtractor

        link = article.raw_data.get('original_link')
        if not isinstance(link, str) or not link:
            return
        try:
            parsed = urlsplit(link)
            browser_eligible = (
                settings.extraction_service_native_browser and parsed.scheme == 'https'
                and parsed.hostname and parsed.port in (None, 443)
                and not parsed.username and not parsed.password)
        except ValueError:
            browser_eligible = False
        enrichment_deadline = time.monotonic() + timeout_seconds
        request_timeout = min(30.0, timeout_seconds)
        try:
            client = ExtractionServiceClient(
                settings.extraction_service_url,
                settings.extraction_service_token.get_secret_value(),
                max_bytes=settings.extraction_http_max_bytes,
                max_chars=settings.extraction_text_max_chars)
            response = await client.extract(
                link, timeout_seconds=request_timeout, source_type='article',
                admission_timeout_seconds=timeout_seconds)
            failure = None
        except (ExtractionServiceError, ValueError) as exc:
            response = None
            failure = exc if isinstance(exc, ExtractionServiceError) else ExtractionServiceError('invalid_configuration')

        if response is not None and response.get('outcome') == 'success':
            self._apply_go_enrichment(article, response)
            return
        if response is not None:
            error = response.get('error') or {}
            failure = ExtractionServiceError(error.get('code', 'no_content'), error.get('retryable', False))
        if (browser_eligible and response is not None and failure is not None
                and CoreExtractor._native_browser_retry_allowed(failure, response)):
            remaining = enrichment_deadline - time.monotonic()
            if remaining <= 0.05:
                return
            try:
                response = await client.extract(
                    link, timeout_seconds=min(30.0, remaining),
                    source_type='article', use_browser=True,
                    admission_timeout_seconds=remaining)
            except (ExtractionServiceError, ValueError):
                return
            if response.get('outcome') == 'success':
                self._apply_go_enrichment(article, response)

    def _apply_go_enrichment(self, article: Article, response: Dict[str, Any]) -> None:
        content = response['result'].get('content') or ''
        if len(content) <= 2 * len(article.content or ''):
            return
        article.content = content
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        title = lines[0] if lines else 'Telegram Post'
        title = re.sub(r'^(Forwarded from|Reply to|@\w+:?\s*)', '', title, flags=re.IGNORECASE)
        article.title = title[:120].rstrip()
        tags = [re.sub(r'[^\w_]+', '', tag).lower()
                for tag in re.findall(r'(?:(?<=\s)|^)#(\w+)', content)]
        article.raw_data['hashtags'] = list(dict.fromkeys(tag for tag in tags if tag))[:20]
        article.raw_data['content_length'] = len(content)
    
    async def test_connection(self) -> bool:
        """Probe the Go Telegram collector without enriching or persisting posts."""
        from ..config import get_settings
        from .service_client import TelegramServiceClient, TelegramServiceError

        settings = get_settings()
        if not settings.extraction_service_url or not settings.extraction_service_token:
            raise SourceError('telegram_service_not_configured')
        try:
            client = TelegramServiceClient(
                settings.extraction_service_url,
                settings.extraction_service_token.get_secret_value())
            response = await client.fetch(self.url, self.name, limit=1,
                timeout_seconds=min(30.0, settings.extraction_service_total_budget_seconds),
                admission_timeout_seconds=settings.extraction_service_total_budget_seconds)
            return response['outcome'] in {'success', 'partial'}
        except asyncio.CancelledError:
            raise
        except (TelegramServiceError, ValueError) as exc:
            logger.warning('Telegram Go source test failed code=%s',
                           getattr(exc, 'code', 'invalid_configuration'))
            return False

    async def close(self):
        """Compatibility hook; remote browser lifecycle belongs to the Go service."""
        self.browser = None
