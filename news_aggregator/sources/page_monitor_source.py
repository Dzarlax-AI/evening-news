import logging
"""Adaptive page monitor source for websites without RSS feeds."""

import re
import json
import hashlib
import asyncio
import math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set, Tuple, Any
from dataclasses import dataclass, replace
from urllib.parse import urlparse

from .base import BaseSource, SourceInfo, Article, SourceType
from ..core.exceptions import SourceError
from ..core.resource_limits import ResourceLimitError

logger = logging.getLogger(__name__)


@dataclass
class PageSnapshot:
    """Snapshot of page content for change detection."""
    url: str
    content_hash: str
    article_hashes: Set[str]
    extracted_items: List[Dict[str, Any]]
    timestamp: datetime
    selectors_used: List[str]


@dataclass 
class PageMonitorConfig:
    """Configuration for page monitoring."""
    url: str
    name: str
    
    # Content detection settings
    article_selectors: List[str] = None
    title_selectors: List[str] = None
    date_selectors: List[str] = None
    link_selectors: List[str] = None
    
    # Update frequency
    check_interval_minutes: int = 30
    
    # Content filtering
    min_title_length: int = 10
    max_articles_per_check: int = 20
    
    # Browser settings
    use_browser: bool = True
    wait_for_js: bool = True
    wait_timeout_ms: int = 30000
    
    # AI optimization
    enable_ai_analysis: bool = True
    reanalyze_after_failures: int = 5
    
    def __post_init__(self):
        try:
            wait_timeout_ms = float(self.wait_timeout_ms)
        except (TypeError, ValueError) as exc:
            raise ValueError("wait_timeout_ms must be finite and positive") from exc
        if not math.isfinite(wait_timeout_ms) or wait_timeout_ms <= 0:
            raise ValueError("wait_timeout_ms must be finite and positive")

        if self.article_selectors is None:
            self.article_selectors = [
                # Common article/news item patterns
                'article', '.article', '.news-item', '.post', '.entry',
                '.changelog-item', '.update-item', '.release-note',
                
                # List items that might contain news
                '.content li', '.main li', 'ul.updates li', 'ul.news li',
                
                # Generic content containers
                '.content > div', '.main > div', '.updates > div',
                '[class*="item"]', '[class*="post"]', '[class*="article"]',
                
                # Headings that might indicate news items
                'h2, h3, h4', '.title', '.headline', '.heading'
            ]
        
        if self.title_selectors is None:
            self.title_selectors = [
                'h1', 'h2', 'h3', 'h4', '.title', '.headline', '.heading',
                'a[href]', '.link', '.post-title', '.article-title'
            ]
        
        if self.date_selectors is None:
            self.date_selectors = [
                # Standard HTML5 time elements
                'time', 'time[datetime]', '[datetime]',
                
                # Common date classes
                '.date', '.timestamp', '.published', '.created', '.updated',
                '.publish-date', '.published-date', '.creation-date', '.post-date',
                '.article-date', '.news-date', '.entry-date', '.blog-date',
                
                # Meta information
                '.meta .date', '.meta time', '.metadata .date', '.info .date',
                '.post-meta .date', '.article-meta .date', '.entry-meta .date',
                
                # GitHub/Changelog specific
                '.commit-date', '.release-date', '.version-date', '.update-date',
                '.changelog-date', '.timeline .date', 
                
                # Blog specific
                '.byline .date', '.author-date', '.publish-info .date',
                
                # Generic patterns
                '[class*="date"]', '[class*="time"]', '[class*="publish"]',
                '[id*="date"]', '[id*="time"]'
            ]
        
        if self.link_selectors is None:
            self.link_selectors = [
                'a[href]', '.link', '.read-more', '.permalink'
            ]


class PageMonitorSource(BaseSource):
    """Adaptive source for monitoring pages without RSS feeds."""
    
    def __init__(self, config: PageMonitorConfig):
        info = SourceInfo(
            name=config.name,
            source_type=SourceType.CUSTOM,
            url=config.url,
            description=f"Page monitor for {config.url}"
        )
        super().__init__(info)
        
        self.config = config
        self.browser = None
        self.last_snapshot: Optional[PageSnapshot] = None
        self.failure_count = 0
        self.ai_analysis_count = 0
        # Go pagination retains only bounded offset/hash metadata, never HTML or
        # article payloads. Position keys index the configured first-Limit window.
        self._go_offset = 0
        self._go_previous_segments: Dict[int, str] = {}
        self._go_scan_segments: Dict[int, str] = {}
        self._go_scan_acknowledged: Dict[int, str] = {}
        self._go_scan_active = False
        
        # Dynamic selectors learned through AI analysis
        self.learned_selectors: Dict[str, float] = {}  # selector -> confidence
        
        # Specific structure selectors learned from AI
        self.learned_container_selectors: List[str] = []
        self.learned_title_selectors: List[str] = []
        self.learned_link_selectors: List[str] = []
        self.learned_date_selectors: List[str] = []
        
        self._learned_structure_loaded = False
        
        # Content patterns for intelligent extraction
        self.content_patterns = {
            'changelog': [
                r'(?i)\b(version|v\d+|\d+\.\d+)',
                r'(?i)\b(released?|updated?|fixed?|added?|improved?)',
                r'(?i)\b(feature|bug|improvement|enhancement)'
            ],
            'news': [
                r'(?i)\b(breaking|urgent|announced?|launched?)',
                r'(?i)\b(today|yesterday|this week|latest)',
                r'(?i)\b(update|news|press|release)'
            ],
            'blog': [
                r'(?i)\b(posted|published|written|authored)',
                r'(?i)\b(tutorial|guide|how.?to|tips)',
                r'(?i)\b(learn|understand|master)'
            ]
        }
    
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.browser = None

    async def fetch_articles(self, limit: Optional[int] = None) -> List[Article]:
        """Fetch and finalize Page Monitor results through the Go collector."""
        from ..config import get_settings
        total_budget = min(float(get_settings().extraction_service_total_budget_seconds), 120.0)
        deadline = asyncio.get_running_loop().time() + total_budget
        try:
            return await self._fetch_articles_go(limit, deadline)
        except ResourceLimitError:
            raise
        except TimeoutError:
            logger.warning("Page Monitor request exceeded its source deadline")
            self.failure_count += 1
            return []
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure_count += 1
            logger.warning("Page Monitor Go request failed: %s", type(exc).__name__)
            return []

    def _delivery_limit(self, limit):
        if limit is None:
            return self.config.max_articles_per_check
        if type(limit) is not int or limit < 0:
            raise ValueError('invalid_page_monitor_limit')
        return min(limit, self.config.max_articles_per_check)

    async def prepare_go_collection(self):
        """Load learned selectors and return the bounded Go manager descriptor config."""
        await self._load_learned_structure()
        config = self._service_config()
        scan_limit = max(1, min(self.config.max_articles_per_check, 100))
        offset = self._go_offset if self._go_scan_active else 0
        if offset >= scan_limit:
            offset = 0
        return {
            'use_browser': config.use_browser,
            'wait_for_js': config.wait_for_js,
            'min_title_length': config.min_title_length,
            'article_selectors': config.article_selectors,
            'title_selectors': config.title_selectors,
            'date_selectors': config.date_selectors,
            'link_selectors': config.link_selectors,
            'learned_container_selectors': config.learned_container_selectors,
            'learned_title_selectors': config.learned_title_selectors,
            'learned_link_selectors': config.learned_link_selectors,
            'learned_date_selectors': config.learned_date_selectors,
            'learned_general_selectors': [
                {'selector': selector, 'confidence': confidence}
                for selector, confidence in config.learned_selectors
            ],
            'limit': scan_limit,
            'offset': offset,
        }

    async def finalize_go_collection(self, result, limit=None, deadline=None):
        """Normalize one already-collected Page Monitor payload; this never fetches its page."""
        if deadline is None:
            from ..config import get_settings
            total_budget = min(float(get_settings().extraction_service_total_budget_seconds), 120.0)
            deadline = asyncio.get_running_loop().time() + total_budget
        return await self._fetch_articles_go(limit, deadline, captured_result=result)

    async def _fetch_articles_go(self, limit, deadline, captured_result=None):
        from ..config import get_settings
        from .page_monitor_service_client import PageMonitorServiceClient, PageMonitorServiceError

        delivery_limit = self._delivery_limit(limit)
        settings = get_settings()
        token = settings.extraction_service_token.get_secret_value()
        client = PageMonitorServiceClient(settings.extraction_service_url, token)
        loop = asyncio.get_running_loop()
        total_budget = max(0.0, deadline - loop.time())
        reserve = min(5.0, total_budget * 0.10)
        usable_deadline = deadline - reserve
        scan_limit = max(1, min(self.config.max_articles_per_check, 100))
        offset = self._go_offset if self._go_scan_active else 0
        if offset >= scan_limit:
            offset = 0
        scan_segments = dict(self._go_scan_segments) if self._go_scan_active else {}
        scan_acknowledged = dict(self._go_scan_acknowledged) if self._go_scan_active else {}
        previous_segments = dict(self._go_previous_segments)
        if not previous_segments and isinstance(self.last_snapshot, PageSnapshot):
            acknowledged = self.last_snapshot.article_hashes
            previous_segments = {
                index: self._hash_article(item)
                for index, item in enumerate(self.last_snapshot.extracted_items[:scan_limit])
                if self._hash_article(item) in acknowledged
            }

        def hashes_in(segments):
            return set(segments.values())

        def prune_from_offset(segments, start):
            for position in tuple(segments):
                if position >= start:
                    segments.pop(position, None)

        def remaining():
            return max(0.0, usable_deadline - loop.time())

        def snapshot_from(result):
            timestamp = self._parse_service_timestamp(result['snapshot_at'])
            items = []
            for item in result['items']:
                items.append({
                    'title': item['title'], 'link': item['link'], 'description': item['description'],
                    'published': (self._parse_service_timestamp(item['published'])
                                  if item['published'] is not None else timestamp),
                    'content_type': item['content_type'], 'id': item['id'],
                    'source': item['source'], 'source_url': item['source_url'],
                })
            hashes = {self._hash_article(item) for item in items}
            return PageSnapshot(self.config.url, result.get('content_hash') or '', hashes, items,
                                timestamp, result['selectors_used'])

        try:
            if captured_result is None:
                async with asyncio.timeout_at(usable_deadline):
                    await self._load_learned_structure()
                    fetch_budget = remaining()
                    if fetch_budget <= 0:
                        raise TimeoutError
                    result = await client.fetch(self._service_config(), limit=scan_limit,
                                                offset=offset,
                                                timeout_seconds=min(30.0, fetch_budget,
                                                                    self.config.wait_timeout_ms / 1000.0),
                                                admission_timeout_seconds=fetch_budget)
            else:
                # The Go batch already collected this response. Its prefix is
                # valid even if the pass deadline has expired; that deadline only
                # suppresses optional study and same-snapshot reparsing.
                result = captured_result
            if result['outcome'] == 'partial':
                logger.warning("Page Monitor Go returned a partial prefix: %s",
                               result['error']['code'])
                if not result['items']:
                    if result.get('next_offset') is not None:
                        # An unrepresentable item is skipped for this bounded
                        # scan only. Keep its previous hash acknowledged if one
                        # was known; no new item is acknowledged here.
                        prior_scan_prefix = {position: article_hash
                                             for position, article_hash in scan_segments.items()
                                             if position >= offset}
                        prior_scan_ack = {position: article_hash
                                          for position, article_hash in scan_acknowledged.items()
                                          if position >= offset}
                        old_acknowledged = hashes_in(previous_segments) | set(prior_scan_ack.values())
                        prior_prefix_pending = any(
                            article_hash not in old_acknowledged
                            for article_hash in prior_scan_prefix.values()
                        )
                        if (not prior_prefix_pending and offset not in scan_segments
                                and offset in previous_segments):
                            scan_segments[offset] = previous_segments[offset]
                            scan_acknowledged[offset] = previous_segments[offset]
                        self._go_scan_segments = scan_segments
                        self._go_scan_acknowledged = scan_acknowledged
                        next_offset = result['next_offset']
                        if prior_prefix_pending:
                            self._go_offset = offset
                            self._go_scan_active = True
                        elif next_offset >= scan_limit:
                            current_hashes = hashes_in(scan_segments)
                            acknowledged = (hashes_in(previous_segments) & current_hashes) | hashes_in(scan_acknowledged)
                            self._go_previous_segments = {
                                position: article_hash for position, article_hash in scan_segments.items()
                                if article_hash in acknowledged
                            }
                            self._go_offset = 0
                            self._go_scan_segments = {}
                            self._go_scan_acknowledged = {}
                            self._go_scan_active = False
                        else:
                            self._go_offset = next_offset
                            self._go_scan_active = True
                    if result['error']['code'] == 'resource_limit':
                        raise ResourceLimitError('resource_limit')
                    if result.get('next_offset') is None:
                        self.failure_count += 1
                    return []
            current_snapshot = snapshot_from(result)
            # A single optional study pass operates on the exact captured HTML.
            if (offset == 0 and result['outcome'] == 'success' and result.get('needs_selector_study')
                    and self.config.enable_ai_analysis
                    and result.get('study_html')):
                study_html = result['study_html']
                try:
                    if remaining() <= 0:
                        raise TimeoutError
                    async with asyncio.timeout_at(usable_deadline):
                        studied = await self._study_source_structure(study_html)
                    if studied and remaining() > 0:
                        async with asyncio.timeout_at(usable_deadline):
                            reparsed = await client.parse(
                                self._service_config(), study_html, current_snapshot.timestamp,
                                limit=scan_limit, timeout_seconds=min(30.0, remaining()), offset=0,
                                admission_timeout_seconds=remaining())
                        if reparsed['outcome'] == 'partial':
                            logger.warning("Page Monitor Go reparse returned a partial prefix: %s",
                                           reparsed['error']['code'])
                            if not reparsed['items']:
                                raise PageMonitorServiceError(reparsed['error']['code'])
                        reparsed_snapshot = snapshot_from(reparsed)
                        current_snapshot = reparsed_snapshot
                        result = reparsed
                except (PageMonitorServiceError, TimeoutError):
                    logger.warning("Page Monitor selector study or same-snapshot reparse failed; keeping extracted prefix")
            elif (offset == 0 and result['outcome'] == 'success' and result.get('needs_selector_study')
                  and self.config.enable_ai_analysis):
                logger.warning("Page Monitor selector study HTML was omitted or unavailable")
        except ResourceLimitError:
            raise
        except TimeoutError:
            self.failure_count += 1
            logger.warning("Page Monitor request exceeded the usable source deadline")
            return []
        except PageMonitorServiceError as exc:
            if exc.code == 'resource_limit':
                raise ResourceLimitError('resource_limit') from None
            self.failure_count += 1
            logger.warning("Page Monitor Go request failed: %s", exc.code)
            return []
        except Exception as exc:
            self.failure_count += 1
            logger.warning("Page Monitor Go request failed: %s", type(exc).__name__)
            return []

        self.failure_count = 0
        base_acknowledged = hashes_in(previous_segments)
        segment_hashes = [self._hash_article(item) for item in current_snapshot.extracted_items]
        prior_segment_ack = {position: article_hash for position, article_hash in scan_acknowledged.items()
                             if position >= offset}
        prune_from_offset(scan_segments, offset)
        prune_from_offset(scan_acknowledged, offset)
        for index, article_hash in enumerate(segment_hashes):
            position = offset + index
            scan_segments[position] = article_hash
            if prior_segment_ack.get(position) == article_hash:
                scan_acknowledged[position] = article_hash
        current_scan_acknowledged = hashes_in(scan_acknowledged)
        candidates = [item for item, article_hash in zip(current_snapshot.extracted_items, segment_hashes)
                      if article_hash not in base_acknowledged and article_hash not in current_scan_acknowledged]
        articles = await self._convert_extracted_to_articles(candidates)
        delivered = articles[:delivery_limit]
        delivered_titles_urls = {(article.title, article.url) for article in delivered}
        for index, (item, article_hash) in enumerate(zip(current_snapshot.extracted_items, segment_hashes)):
            if (item.get('title'), item.get('link')) in delivered_titles_urls:
                scan_acknowledged[offset + index] = article_hash

        # A service cursor can advance only after every item it returned has
        # either been acknowledged earlier in this scan or delivered now.
        scan_acknowledged_values = hashes_in(scan_acknowledged)
        consumed = all(article_hash in base_acknowledged or article_hash in scan_acknowledged_values
                       for article_hash in segment_hashes)
        next_offset = result.get('next_offset')
        if consumed and next_offset is not None:
            cursor = next_offset
            complete = cursor >= scan_limit
        elif consumed and next_offset is None and result['outcome'] in {'success', 'partial'}:
            cursor = scan_limit
            complete = True
        else:
            cursor = offset
            complete = False

        # Partial responses without a continuation cursor preserve the current
        # offset. They may still acknowledge the delivered prefix safely.
        if result['outcome'] == 'partial' and next_offset is None and not consumed:
            complete = False

        if complete:
            current_hashes = hashes_in(scan_segments)
            acknowledged = (base_acknowledged & current_hashes) | scan_acknowledged_values
            previous_segments = {
                position: article_hash for position, article_hash in scan_segments.items()
                if article_hash in acknowledged
            }
            self._go_previous_segments = previous_segments
            self._go_offset = 0
            self._go_scan_segments = {}
            self._go_scan_acknowledged = {}
            self._go_scan_active = False
        else:
            self._go_offset = cursor
            self._go_scan_segments = scan_segments
            self._go_scan_acknowledged = scan_acknowledged
            self._go_scan_active = True

        segment_ack = (set(current_snapshot.article_hashes) &
                       (base_acknowledged | scan_acknowledged_values))
        self.last_snapshot = replace(current_snapshot, article_hashes=segment_ack)
        logger.info("Page Monitor Go delivered %d new articles", len(delivered))
        return delivered

    def _service_config(self):
        return type('PageMonitorServiceConfig', (), {
            'url': self.config.url, 'name': self.config.name,
            'article_selectors': self.config.article_selectors,
            'title_selectors': self.config.title_selectors,
            'date_selectors': self.config.date_selectors,
            'link_selectors': self.config.link_selectors,
            'learned_container_selectors': self.learned_container_selectors,
            'learned_title_selectors': self.learned_title_selectors,
            'learned_link_selectors': self.learned_link_selectors,
            'learned_date_selectors': self.learned_date_selectors,
            'learned_selectors': list(self.learned_selectors.items()),
            'min_title_length': self.config.min_title_length,
            'use_browser': self.config.use_browser,
            'wait_for_js': self.config.wait_for_js,
        })()

    @staticmethod
    def _parse_service_timestamp(value):
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    
    async def _convert_extracted_to_articles(self, extracted_items: List[Dict[str, Any]]) -> List[Article]:
        """Convert extracted items to Article objects (for first run)."""
        articles = []
        
        for article_data in extracted_items:
            # Convert to Article object
            article = Article(
                title=article_data.get('title', ''),
                url=article_data.get('link', ''),
                content=article_data.get('description', ''),
                published_at=article_data.get('published', datetime.utcnow()),
                source_name=self.config.name
            )
            articles.append(article)
        
        return articles
    
    async def _load_learned_structure(self):
        """Load learned source structure from memory."""
        if self._learned_structure_loaded:
            return
            
        try:
            from ..services.extraction_memory import get_extraction_memory
            
            domain = urlparse(self.config.url).netloc.lower()
            memory = await get_extraction_memory()
            structure = await memory.get_page_structure(domain)
            
            self.learned_container_selectors = structure.get('container_selectors', [])
            self.learned_title_selectors = structure.get('title_selectors', [])
            self.learned_link_selectors = structure.get('link_selectors', [])
            self.learned_date_selectors = structure.get('date_selectors', [])
            
            if self.learned_container_selectors:
                logger.info(f"  🧠 Loaded learned structure for {domain} ({len(self.learned_container_selectors)} containers)")
            self._learned_structure_loaded = True
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to load learned structure: {e}")
    async def _study_source_structure(self, html: Optional[str] = None):
        """Use AI to study the source structure and discover selectors."""
        try:
            if not html:
                return False
                
            from .ai_page_analyzer import get_ai_page_analyzer
            from ..services.extraction_memory import get_extraction_memory
            
            analyzer = await get_ai_page_analyzer()
            analysis = await analyzer.analyze_page_structure(self.config.url, html, context="news")
            
            if analysis:
                # Update our in-memory selectors
                self.learned_container_selectors = analysis.content_selectors
                self.learned_title_selectors = analysis.title_selectors
                self.learned_date_selectors = analysis.date_selectors
                
                # Persist to memory service
                domain = urlparse(self.config.url).netloc.lower()
                memory = await get_extraction_memory()
                await memory.record_page_structure(domain, analysis)
                
                logger.info(f"  🎯 AI discovered new structure for {domain}")
                return True
        except Exception as e:
            logger.error(f"  ❌ Source study failed: {e}")
        return False

    def _hash_article(self, article_data: Dict[str, Any]) -> str:
        """Create hash for article to detect changes."""
        key_data = {
            'title': article_data.get('title', ''),
            'link': article_data.get('link', ''),
            'description': article_data.get('description', '')[:100]  # First 100 chars
        }
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()
    
    async def _trigger_ai_analysis(self):
        """Trigger AI analysis to improve selectors."""
        try:
            logger.info(f"🤖 Triggering AI analysis for {self.config.url}")
            # 1. Try source structure study if we have HTML
            # We don't have HTML here easily, but we can try to fetch it again 
            # or just use the heuristic below
            
            self.ai_analysis_count += 1
            
            # Add some smart selectors based on domain analysis
            domain = urlparse(self.config.url).netloc.lower()
            
            if 'changelog' in self.config.url.lower() or 'changelog' in domain:
                self.learned_selectors.update({
                    '.changelog-entry': 0.9,
                    '.release-note': 0.8,
                    '.version-info': 0.8,
                    'li[data-version]': 0.7,
                    '.update-item': 0.7
                })
            
            if 'blog' in domain or 'news' in domain:
                self.learned_selectors.update({
                    '.post-item': 0.9,
                    '.news-item': 0.9,
                    'article.post': 0.8,
                    '.blog-entry': 0.8
                })
            
            logger.info(f"  🎯 Added {len(self.learned_selectors)} learned selectors")
        except Exception as e:
            logger.error(f"  ❌ AI analysis failed: {e}")
    async def test_connection(self) -> bool:
        """Probe Go collection without changing snapshot, cursor, or learned state."""
        from ..config import get_settings
        from .page_monitor_service_client import PageMonitorServiceClient, PageMonitorServiceError

        settings = get_settings()
        try:
            client = PageMonitorServiceClient(
                settings.extraction_service_url,
                settings.extraction_service_token.get_secret_value())
            response = await client.fetch(
                self._service_config(), limit=1,
                timeout_seconds=min(30.0, settings.extraction_service_total_budget_seconds,
                                    self.config.wait_timeout_ms / 1000.0),
                admission_timeout_seconds=settings.extraction_service_total_budget_seconds)
            return response['outcome'] in {'success', 'partial'}
        except asyncio.CancelledError:
            raise
        except (PageMonitorServiceError, ValueError) as exc:
            logger.warning("Page Monitor Go connection test failed: %s",
                           getattr(exc, 'code', 'invalid_configuration'))
            return False
