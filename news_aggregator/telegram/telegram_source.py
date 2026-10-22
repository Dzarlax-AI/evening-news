"""Modern modular Telegram source using specialized components."""

import re
import random
from datetime import datetime, timedelta, timezone
import logging
import pytz
import asyncio
import time
from urllib.parse import urlsplit
from typing import AsyncGenerator, Optional, Dict, List, Any
from bs4 import BeautifulSoup
from ..core.resource_limits import ResourceLimitError, read_page_text, read_browser_html

from ..sources.base import BaseSource, Article
from ..core.http_client import get_http_client
from ..core.exceptions import BrowserUnavailableError, SourceError
from .message_parser import MessageParser
from .media_extractor import MediaExtractor

logger = logging.getLogger(__name__)

try:
    import nodriver as uc
    BROWSER_AVAILABLE = True
except ImportError:
    BROWSER_AVAILABLE = False
    uc = None


class TelegramSource(BaseSource):
    """Modern Telegram public channel source with modular architecture."""
    
    # Real browser headers to bypass Cloudflare
    BROWSER_HEADERS = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    ]
    
    def __init__(self, source_info):
        super().__init__(source_info)
        self.channel_username = self._extract_channel_username()
        self.browser: Optional[uc.Browser] = None if uc else None
        
        # Initialize modular components
        self.message_parser = MessageParser(self.name, self.channel_username)
        self.media_extractor = MediaExtractor()
        
        # Multiple access URLs to try
        self.access_urls = [
            f"https://t.me/s/{self.channel_username}",  # Standard preview
            f"https://telegram.me/s/{self.channel_username}",  # Alternative domain
        ]
        
        # Retry configuration
        self.max_retries = 3
        self.base_delay = 2.0
        self.max_delay = 30.0
    
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
        """Fetch articles from Telegram channel using multiple methods."""
        from ..config import get_settings
        if get_settings().telegram_source_mode == 'go':
            async for article in self._fetch_with_go(limit):
                yield article
            return
        if limit is not None and (type(limit) is not int or limit <= 0):
            raise SourceError('invalid_telegram_limit')
        logger.info(f"🔍 Fetching from Telegram: @{self.channel_username} (limit: {limit})")
        articles_found = 0
        last_error = None
        
        # Try browser first for better media extraction (JS widgets)
        if BROWSER_AVAILABLE:
            try:
                logger.info("  🎭 Trying browser access for comprehensive media extraction...")
                async for article in self._fetch_with_browser():
                    articles_found += 1
                    yield article
                    if limit and articles_found >= limit:
                        break
                
                if articles_found > 0:
                    logger.info(f"  ✅ Browser method successful - found {articles_found} articles")
                    return
                    
            except ResourceLimitError:
                raise
            except Exception as e:
                last_error = e
                logger.error(f"  ❌ Browser method failed: {e}")
        # Try HTTP as fallback
        try:
            logger.info("  📡 Trying HTTP access as fallback...")
            async for article in self._fetch_with_http():
                articles_found += 1
                yield article
                if limit and articles_found >= limit:
                    break
            
            if articles_found > 0:
                logger.info(f"  ✅ HTTP method successful - found {articles_found} articles")
                return
                
        except ResourceLimitError:
            raise
        except Exception as e:
            last_error = e
            logger.error(f"  ❌ HTTP method failed: {e}")
        if articles_found == 0:
            error_msg = f"All access methods failed for @{self.channel_username}"
            if last_error:
                error_msg += f". Last error: {last_error}"
            raise SourceError(error_msg)

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
            response = await client.fetch(
                self.url, self.name, limit=request_limit,
                timeout_seconds=min(30.0, max(0.001, deadline - time.monotonic())))
        except (TelegramServiceError, ValueError) as exc:
            code = exc.code if isinstance(exc, TelegramServiceError) else 'invalid_configuration'
            logger.warning('Telegram Go source failed code=%s', code)
            raise SourceError(f'telegram_service_{code}') from None
        except Exception:
            logger.warning('Telegram Go source failed code=unavailable')
            raise SourceError('telegram_service_unavailable') from None

        articles = [self._article_from_go(item) for item in response['articles']]
        if response['outcome'] == 'partial':
            logger.warning('Telegram Go source returned partial articles count=%d code=%s',
                           len(articles), response['error']['code'])
        for article in articles:
            remaining = deadline - time.monotonic()
            if (article.raw_data.get('original_link') and article.content
                    and len(article.content) < 200 and remaining > 0.05):
                try:
                    await self._enrich_go_article(article, settings, min(30.0, remaining))
                except Exception:
                    logger.info('Telegram external enrichment skipped code=unavailable')
            # Keep every validated post even when enrichment fails or exhausts budget.
            yield article

        if response['outcome'] in {'failed', 'rejected'}:
            error = response.get('error') or {'code': response['outcome']}
            logger.warning('Telegram Go source returned no articles code=%s', error['code'])
            raise SourceError(f"telegram_service_{error['code']}")

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
        try:
            client = ExtractionServiceClient(
                settings.extraction_service_url,
                settings.extraction_service_token.get_secret_value(),
                max_bytes=settings.extraction_http_max_bytes,
                max_chars=settings.extraction_text_max_chars)
            response = await client.extract(
                link, timeout_seconds=timeout_seconds, source_type='article')
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
                    source_type='article', use_browser=True)
            except (ExtractionServiceError, ValueError):
                return
            if response.get('outcome') == 'success':
                self._apply_go_enrichment(article, response)

    def _apply_go_enrichment(self, article: Article, response: Dict[str, Any]) -> None:
        content = response['result'].get('content') or ''
        if len(content) <= 2 * len(article.content or ''):
            return
        article.content = content
        article.title = self.message_parser._extract_title(content)
        article.raw_data['hashtags'] = self.message_parser._extract_hashtags(content)
        article.raw_data['content_length'] = len(content)
    
    async def _fetch_with_http(self) -> AsyncGenerator[Article, None]:
        """Fetch articles using HTTP requests."""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            for url in self.access_urls:
                try:
                    headers = random.choice(self.BROWSER_HEADERS)
                    
                    async with session.get(url, headers=headers, timeout=30, auto_decompress=False) as response:
                        if response.status == 200:
                            html = await read_page_text(response)
                            articles = await self._parse_html(html, url)
                            
                            for article in articles:
                                yield article
                            
                            if articles:
                                return  # Success with this URL
                        else:
                            logger.warning(f"  ⚠️ HTTP {response.status} for {url}")
                except ResourceLimitError:
                    raise
                except Exception as e:
                    logger.error(f"  ❌ HTTP error for {url}: {e}")
                    continue
        
        raise SourceError("All HTTP methods failed")
    
    async def _fetch_with_browser(self) -> AsyncGenerator[Article, None]:
        """Fetch articles using browser automation."""
        if not BROWSER_AVAILABLE:
            raise SourceError("nodriver not available for browser access")

        from ..core.browser_pool import browser_tab, run_browser_operation

        for url in self.access_urls:
            try:
                async with browser_tab(url) as tab:
                    # Wait for content to load, ignoring CDP node resolution errors
                    try:
                        await run_browser_operation(
                            tab.wait_for(selector='.tgme_widget_message', timeout=8),
                            "Telegram message wait",
                            timeout_seconds=8,
                            browser_session=tab,
                        )
                    except BrowserUnavailableError:
                        raise
                    except Exception as wait_err:
                        logger.warning(f"  wait_for warning (ignoring): {wait_err}")

                    # Enhanced scrolling strategy to load fresh messages
                    try:
                        logger.info("  Enhanced scrolling to load latest messages...")
                        for _ in range(3):
                            await run_browser_operation(
                                tab.evaluate("window.scrollTo(0, document.body.scrollHeight)"),
                                "Telegram scroll",
                                browser_session=tab,
                            )
                            await asyncio.sleep(2)
                        logger.info("  Enhanced scrolling completed")
                    except BrowserUnavailableError:
                        raise
                    except Exception as scroll_error:
                        logger.warning(f"  Scrolling failed: {scroll_error}")

                    # Get HTML content
                    html = await read_browser_html(tab)
                    articles = await self._parse_html(html, url)

                    for article in articles:
                        yield article

                    if articles:
                        return  # Success with this URL

            except ResourceLimitError:
                raise
            except Exception as e:
                logger.error(f"  Browser error for {url}: {e}")
                continue

        raise SourceError("All browser methods failed")
    
    async def _parse_html(self, html: str, base_url: str) -> List[Article]:
        """Parse HTML content and extract articles using modular components."""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        # Find message containers
        message_selectors = [
            '.tgme_widget_message',
            '.message',
            '[data-post]'
        ]
        
        messages = []
        for selector in message_selectors:
            messages = soup.select(selector)
            if messages:
                break
        
        if not messages:
            logger.warning(f"  ⚠️ No messages found in HTML")
            return articles
        
        logger.info(f"  📊 Found {len(messages)} messages to parse")
        for i, message_div in enumerate(messages):
            try:
                # Use modular message parser with soup for Open Graph extraction
                article = await self.message_parser.parse_message_element(message_div, base_url, soup)
                if article:

                    articles.append(article)
                    if len(articles) % 5 == 0:  # Progress indicator
                        logger.info(f"  ✅ Parsed {len(articles)} articles...")
                else:
                    logger.warning(f"  ❌ [DEBUG SKIPPED]: Message {i+1} returned None from parser")
            except Exception as e:
                logger.warning(f"  ⚠️ Error parsing message {i+1}: {e}")
                continue
        
        logger.warning(f"  🎯 Successfully parsed {len(articles)}/{len(messages)} messages total.")
        return articles
    
    def _normalize_external_url(self, url: str) -> Optional[str]:
        """Normalize external URL (delegated to message parser)."""
        return self.message_parser._normalize_external_url(url)
    
    async def test_connection(self) -> bool:
        """Test Telegram channel connection."""
        try:
            headers = random.choice(self.BROWSER_HEADERS)
            
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                for url in self.access_urls:
                    try:
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                return True
                    except:
                        continue
            
            return False
                    
        except Exception:
            return False
    
    async def close(self):
        """Clean up resources. For remote browser, only disconnect — don't kill the server."""
        # Browser lifecycle is managed by the shared pool
        self.browser = None
