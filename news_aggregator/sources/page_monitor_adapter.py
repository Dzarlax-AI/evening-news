"""Registry adapter exposing Page Monitor configuration to Go probes."""

from typing import Optional, AsyncGenerator

from .base import BaseSource, SourceInfo, Article
from .page_monitor_source import PageMonitorSource, PageMonitorConfig
from ..core.exceptions import SourceError


class PageMonitorAdapter(BaseSource):
    def __init__(self, source_info: SourceInfo):
        super().__init__(source_info)
        data = source_info.config or {}
        self.monitor = PageMonitorSource(PageMonitorConfig(
            url=source_info.url,
            name=source_info.name,
            article_selectors=data.get('article_selectors'),
            title_selectors=data.get('title_selectors'),
            date_selectors=data.get('date_selectors'),
            link_selectors=data.get('link_selectors'),
            check_interval_minutes=data.get('check_interval_minutes', 30),
            min_title_length=data.get('min_title_length', 10),
            max_articles_per_check=data.get('max_articles_per_check', 20),
            use_browser=data.get('use_browser', True),
            wait_for_js=data.get('wait_for_js', True),
            wait_timeout_ms=data.get('wait_timeout_ms', 30000),
            enable_ai_analysis=data.get('enable_ai_analysis', True),
            reanalyze_after_failures=data.get('reanalyze_after_failures', 5),
        ))

    async def fetch_articles(self, limit: Optional[int] = None) -> AsyncGenerator[Article, None]:
        raise SourceError('page_monitor_collection_owned_by_go')
        yield  # Preserve BaseSource's async-generator interface.

    async def test_connection(self) -> bool:
        raise SourceError('source_test_owned_by_go')
