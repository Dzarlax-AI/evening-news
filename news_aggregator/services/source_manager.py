import logging
"""Source manager for handling multiple news sources."""

import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models import Article, Source
from ..sources import get_source_registry, BaseSource, SourceInfo, SourceType
from ..core.exceptions import SourceError

logger = logging.getLogger(__name__)


class SourceManager:
    """Manager for news sources."""
    
    def __init__(self):
        self.source_registry = get_source_registry()
        self._source_instances: Dict[int, BaseSource] = {}
    
    async def create_source(self, db: AsyncSession, name: str, source_type: str, 
                           url: str, config: Optional[Dict[str, Any]] = None,
                           skip_connection_test: bool = False) -> Source:
        """Create new source in database."""
        try:
            # Validate source type
            source_type_enum = SourceType(source_type)
            if not self.source_registry.is_supported(source_type_enum):
                raise SourceError(f"Unsupported source type: {source_type}")
            
            # Create source record
            source = Source(
                name=name,
                source_type=source_type,
                url=url,
                config=config or {},
                enabled=True
            )
            
            db.add(source)
            await db.commit()
            await db.refresh(source)
            
            if not skip_connection_test and not await self._probe_source(source):
                source.enabled = False
                source.last_error = "Go source connection test failed"
                source.error_count = 1
                await db.commit()
            
            return source
        
        except Exception as e:
            await db.rollback()
            raise SourceError(f"Failed to create source: {e}")
    
    async def get_sources(self, db: AsyncSession, enabled_only: bool = False) -> List[Source]:
        """Get all sources from database."""
        query = select(Source)
        if enabled_only:
            query = query.where(Source.enabled == True)
        
        result = await db.execute(query)
        return result.scalars().all()
    
    async def get_source_by_id(self, db: AsyncSession, source_id: int) -> Optional[Source]:
        """Get source by ID."""
        result = await db.execute(select(Source).where(Source.id == source_id))
        return result.scalar_one_or_none()
    
    async def update_source(self, db: AsyncSession, source_id: int, **updates) -> Optional[Source]:
        """Update source."""
        try:
            source = await self.get_source_by_id(db, source_id)
            if not source:
                return None
            for key, value in updates.items():
                if hasattr(source, key):
                    setattr(source, key, value)
            source.updated_at = datetime.utcnow()
            await db.commit()
            await db.refresh(source)
            if source_id in self._source_instances:
                del self._source_instances[source_id]
            return source
        except Exception as e:
            await db.rollback()
            raise SourceError(f"Failed to update source {source_id}: {e}")
    
    async def delete_source(self, db: AsyncSession, source_id: int, delete_articles: bool = False) -> bool:
        """
        Delete source and optionally its articles.
        
        Args:
            db: Database session
            source_id: ID of source to delete
            delete_articles: Whether to delete associated articles
        """
        try:
            source = await self.get_source_by_id(db, source_id)
            if not source:
                return False
                
            # Check if source has articles
            from ..models import Article
            from sqlalchemy import select, func, delete
            
            # If delete_articles is requested, remove them first
            if delete_articles:
                await db.execute(delete(Article).where(Article.source_id == source_id))
            
            # Delete source
            await db.delete(source)
            await db.commit()
            
            # Cleanup in-memory instance
            if source_id in self._source_instances:
                del self._source_instances[source_id]
                
            return True
        except Exception as e:
            await db.rollback()
            raise SourceError(f"Failed to delete source {source_id}: {e}")
    
    def _create_source_instance(self, source: Source) -> BaseSource:
        """Create source instance from database record."""
        source_info = SourceInfo(
            name=source.name,
            source_type=SourceType(source.source_type),
            url=source.url,
            description=f"Source: {source.name}",
            enabled=source.enabled,
            config=source.config
        )
        
        return self.source_registry.create_source(source_info)
    
    async def get_source_instance(self, source: Source) -> BaseSource:
        """Get cached source instance."""
        if source.id not in self._source_instances:
            self._source_instances[source.id] = self._create_source_instance(source)
        
        return self._source_instances[source.id]
    
    async def test_source_connection(self, db: AsyncSession, source_id: int) -> bool:
        """Probe a source through Go without applying Telegram or Page Monitor results."""
        source = await self.get_source_by_id(db, source_id)
        if not source:
            return False
        # Release the read transaction before network admission can wait. Reload
        # the row afterward so the operational test metadata is still persisted.
        source_snapshot = self._source_test_snapshot(source)
        await db.commit()
        try:
            connected = await self._probe_source(source_snapshot)
            source = await self.get_source_by_id(db, source_id)
            if not source:
                return connected
            if connected:
                source.last_success = datetime.utcnow()
                source.error_count = 0
                source.last_error = None
            else:
                source.error_count += 1
                source.last_error = "Go source connection test failed"
            await db.commit()
            return connected
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            source = await self.get_source_by_id(db, source_id)
            if not source:
                return False
            source.error_count += 1
            source.last_error = type(exc).__name__
            await db.commit()
            return False
    
    async def _probe_source(self, source: Source) -> bool:
        """Probe one source through the Go collector without applying its result."""
        from ..config import get_settings
        from ..sources.source_manager_service_client import SourceManagerServiceClient

        source_type = source.source_type.value if isinstance(source.source_type, SourceType) else source.source_type
        if source_type not in {'rss', 'telegram', 'custom'}:
            return False
        settings = get_settings()
        descriptor = {
            'source_id': source.id,
            'type': source_type,
            'source_name': source.name,
            'url': source.url,
            # A manual probe must be able to test disabled sources without
            # changing their persisted enabled state.
            'enabled': True,
            'per_source_budget_ms': 30_000,
        }
        try:
            source_config = source.config or {}
            if source_type == 'rss':
                limit = source_config.get('limit')
                if type(limit) is int and limit > 0:
                    descriptor['limit'] = limit
            elif source_type == 'telegram':
                instance = await self.get_source_instance(source)
                limit = source_config.get('limit')
                config_limit = min(limit, 100) if type(limit) is int and limit > 0 else limit
                descriptor['telegram'] = instance.go_collection_config(config_limit)
                if type(limit) is int:
                    descriptor['limit'] = min(limit, 100)
            else:
                instance = await self.get_source_instance(source)
                monitor = getattr(instance, 'monitor', instance)
                typed_config = monitor.config
                descriptor['page_monitor'] = {
                    'use_browser': typed_config.use_browser,
                    'wait_for_js': typed_config.wait_for_js,
                    'min_title_length': typed_config.min_title_length,
                    'article_selectors': list(typed_config.article_selectors),
                    'title_selectors': list(typed_config.title_selectors),
                    'date_selectors': list(typed_config.date_selectors),
                    'link_selectors': list(typed_config.link_selectors),
                }
                limit = typed_config.max_articles_per_check
                if type(limit) is int and limit > 0:
                    descriptor['limit'] = min(limit, 100)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning('Go source probe preparation failed source_id=%s', source.id)
            return False

        token = settings.extraction_service_token
        if not settings.extraction_service_url or not token:
            return False
        client = SourceManagerServiceClient(
            settings.extraction_service_url, token.get_secret_value())
        try:
            response = await client.collect([descriptor], timeout_seconds=30,
                                            admission_timeout_seconds=30)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning('Go source probe failed source_id=%s', source.id)
            return False
        item = response['results'][0]
        if item['outcome'] == 'deferred':
            error = item.get('error') or {}
            delay = error.get('retry_after_seconds', 0)
            if type(delay) is int and 0 < delay < 5:
                await asyncio.sleep(delay)
                try:
                    response = await client.collect([descriptor], timeout_seconds=30,
                                                    admission_timeout_seconds=30)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning('Go source probe retry failed source_id=%s', source.id)
                    return False
                item = response['results'][0]
        return item['outcome'] in {'success', 'partial'}

    @staticmethod
    def _source_test_snapshot(source):
        """Copy the scalar source contract before a caller releases its DB read."""
        from types import SimpleNamespace

        return SimpleNamespace(
            id=source.id, name=source.name, source_type=source.source_type,
            url=source.url, enabled=source.enabled, config=source.config,
        )

    async def get_source_stats(self, db: AsyncSession) -> Dict[str, Any]:
        """Get statistics about sources."""
        # Get all sources
        all_sources = await self.get_sources(db)
        enabled_sources = [s for s in all_sources if s.enabled]
        
        # Get article counts
        from sqlalchemy import func
        article_counts = await db.execute(
            select(Source.name, func.count(Article.id).label('count'))
            .outerjoin(Article)
            .group_by(Source.id, Source.name)
        )
        
        source_article_counts = {name: count for name, count in article_counts}
        
        return {
            'total_sources': len(all_sources),
            'enabled_sources': len(enabled_sources),
            'disabled_sources': len(all_sources) - len(enabled_sources),
            'sources_with_errors': len([s for s in all_sources if s.error_count > 0]),
            'article_counts_by_source': source_article_counts,
            'supported_source_types': [t.value for t in self.source_registry.get_supported_types()]
        }
