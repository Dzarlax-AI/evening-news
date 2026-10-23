"""Adapter to integrate PageMonitorSource with the source registry system."""

from typing import List, Optional, AsyncGenerator
from .base import BaseSource, SourceInfo, Article
from .page_monitor_source import PageMonitorSource, PageMonitorConfig


class PageMonitorAdapter(BaseSource):
    """Adapter to make PageMonitorSource compatible with SourceInfo system."""
    
    def __init__(self, source_info: SourceInfo):
        super().__init__(source_info)
        
        # Convert SourceInfo to PageMonitorConfig
        config = self._create_config_from_source_info(source_info)
        self.monitor = PageMonitorSource(config)
    
    def _create_config_from_source_info(self, source_info: SourceInfo) -> PageMonitorConfig:
        """Convert SourceInfo to PageMonitorConfig."""
        
        # Extract config from source_info.config (JSON field)
        config_data = source_info.config or {}
        
        # Create PageMonitorConfig with defaults
        config = PageMonitorConfig(
            url=source_info.url,
            name=source_info.name,
            
            # Extraction settings with overrides from config
            article_selectors=config_data.get('article_selectors'),
            title_selectors=config_data.get('title_selectors'),
            date_selectors=config_data.get('date_selectors'),
            link_selectors=config_data.get('link_selectors'),
            
            # Update frequency
            check_interval_minutes=config_data.get('check_interval_minutes', 30),
            
            # Content filtering
            min_title_length=config_data.get('min_title_length', 10),
            max_articles_per_check=config_data.get('max_articles_per_check', 20),
            
            # Browser settings
            use_browser=config_data.get('use_browser', True),
            wait_for_js=config_data.get('wait_for_js', True),
            wait_timeout_ms=config_data.get('wait_timeout_ms', 30000),
            
            # AI optimization
            enable_ai_analysis=config_data.get('enable_ai_analysis', True),
            reanalyze_after_failures=config_data.get('reanalyze_after_failures', 5)
        )
        
        return config
    
    async def fetch_articles(self, limit: Optional[int] = None):
        """Fetch articles using the underlying PageMonitorSource."""
        async with self.monitor:
            articles = await self.monitor.fetch_articles(limit=limit)
            
            # Yield each article as an async generator
            for article in articles:
                yield article

    async def prepare_go_collection(self):
        return await self.monitor.prepare_go_collection()

    async def finalize_go_collection(self, result, limit=None, deadline=None):
        return await self.monitor.finalize_go_collection(result, limit=limit, deadline=deadline)
    
    async def test_connection(self) -> bool:
        """Test connection using the underlying PageMonitorSource."""
        return await self.monitor.test_connection()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.monitor.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.monitor.__aexit__(exc_type, exc_val, exc_tb)
