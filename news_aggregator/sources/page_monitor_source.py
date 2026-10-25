"""Page Monitor settings and retained AI selector-learning callback."""

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .base import Article, BaseSource, SourceInfo, SourceType
from ..core.exceptions import SourceError

logger = logging.getLogger(__name__)


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
    """Typed source configuration plus the executor's selector-learning hook.

    Page collection and probes are performed by Go. The executor constructs this
    object only to learn selectors from an already captured page snapshot.
    """

    def __init__(self, config: PageMonitorConfig):
        info = SourceInfo(name=config.name, source_type=SourceType.CUSTOM, url=config.url,
                          description=f"Page monitor for {config.url}")
        super().__init__(info)
        self.config = config
        self.learned_selectors: Dict[str, float] = {}
        self.learned_container_selectors: List[str] = []
        self.learned_title_selectors: List[str] = []
        self.learned_link_selectors: List[str] = []
        self.learned_date_selectors: List[str] = []

    async def fetch_articles(self, limit=None):
        raise SourceError('page_monitor_collection_owned_by_go')
        yield  # Preserve BaseSource's async-generator interface.

    async def test_connection(self) -> bool:
        raise SourceError('source_test_owned_by_go')

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
