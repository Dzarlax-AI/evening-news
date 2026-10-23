import logging
"""Source manager for handling multiple news sources."""

import asyncio
import math
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from ..models import Source, Article
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
            
            # Worker-mode web requests save the source without network/browser work.
            if not skip_connection_test:
                source_instance = self._create_source_instance(source)
                if not await source_instance.test_connection():
                    source.enabled = False
                    source.last_error = "Failed connection test"
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
        """Test source connection."""
        source = await self.get_source_by_id(db, source_id)
        if not source:
            return False
        
        try:
            source_instance = await self.get_source_instance(source)
            is_connected = await source_instance.test_connection()
            
            # Update source status
            if is_connected:
                source.last_success = datetime.utcnow()
                source.error_count = 0
                source.last_error = None
            else:
                source.error_count += 1
                source.last_error = "Connection test failed"
            
            try:
                await db.commit()
            except Exception as e:
                await db.rollback()
                raise SourceError(f"Failed to update source status {source_id}: {e}")
            return is_connected
        
        except Exception as e:
            source.error_count += 1
            source.last_error = str(e)
            try:
                await db.commit()
            except Exception:
                await db.rollback()
            return False
    
    async def get_sources_from_db(self) -> List[Source]:
        """Quick fetch of enabled sources from database."""
        from sqlalchemy import select
        from ..database_helpers import fetch_all

        # Define query
        query = select(Source).where(Source.enabled == True)

        # Fetch using database queue
        return await fetch_all(query)

    async def fetch_from_all_sources_no_db(self, sources: List[Source],
                                           max_concurrent: int = 5,
                                           per_source_timeout: int = 120) -> Dict[str | int, List[Article]]:
        """
        Fetch source I/O without holding a database transaction.
        Go-backed sources share a batch admission queue; other sources remain
        concurrent. Each admitted fetch has its own operation timeout.
        """
        results = {}

        # Create semaphore to limit concurrent fetches
        semaphore = asyncio.Semaphore(max_concurrent)
        go_source_lock = asyncio.Lock()

        from ..config import get_settings
        source_settings = get_settings()

        if source_settings.source_manager_mode == 'go':
            return await self._fetch_from_go_source_manager(sources, per_source_timeout)

        def is_go_backed_source(source: Source) -> bool:
            source_type = getattr(source, 'source_type', None)
            if source_type == SourceType.CUSTOM or source_type == SourceType.CUSTOM.value:
                return source_settings.page_monitor_source_mode == 'go'
            if source_type == SourceType.TELEGRAM or source_type == SourceType.TELEGRAM.value:
                return source_settings.telegram_source_mode == 'go'
            return False

        async def fetch_source_body(source: Source):
            """Fetch articles from source without DB access or admission waits."""
            try:
                source_instance = await self.get_source_instance(source)
                articles = []

                async for article in source_instance.fetch_articles():
                    articles.append(article)

                logger.info(f"  ✅ Fetched {len(articles)} articles from {source.name}")
                return source.name, articles
            except Exception as e:
                logger.warning(f"Error fetching from {source.name}: {e}")
                return source.name, []

        async def fetch_source_raw(source: Source):
            """Apply the shared concurrency limit before starting a normal fetch."""
            async with semaphore:
                return await fetch_source_body(source)

        async def fetch_with_timeout(source: Source):
            """Serialize Go-backed clients, then apply the admitted operation timeout."""
            try:
                if is_go_backed_source(source):
                    # Queue for both Go serialization and shared I/O capacity
                    # before starting the source operation deadline.
                    async with go_source_lock:
                        async with semaphore:
                            return await asyncio.wait_for(
                                fetch_source_body(source), timeout=per_source_timeout
                            )
                return await asyncio.wait_for(
                    fetch_source_raw(source),
                    timeout=per_source_timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"  ⏰ Source '{source.name}' timed out after {per_source_timeout}s — skipping")
                return source.name, []

        # Keep ordinary source concurrency while queuing the shared Go path.
        tasks = [fetch_with_timeout(source) for source in sources]
        fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in fetch_results:
            if isinstance(result, Exception):
                logger.error(f"Fetch task failed with exception: {result}")
                continue

            source_name, articles = result
            results[source_name] = articles

        logger.warning(f"[FETCH COMPLETE] All sources done. Total: {sum(len(a) for a in results.values())} articles from {len(results)} sources")
        return results

    async def _fetch_from_go_source_manager(self, sources: List[Source], total_timeout: int):
        """Collect all source types in bounded Go batches and retain source IDs for persistence."""
        from ..config import get_settings
        from ..sources.source_manager_service_client import (
            SourceManagerServiceClient, SourceManagerServiceError,
        )

        if (isinstance(total_timeout, bool) or not isinstance(total_timeout, (int, float))
                or not math.isfinite(total_timeout) or total_timeout <= 0):
            raise SourceManagerServiceError('invalid_request')
        per_source_budget_ms = min(120_000, max(1, int(float(total_timeout) * 1000)))
        settings = get_settings()
        input_ids = []
        for source in sources:
            source_id = getattr(source, 'id', None)
            if type(source_id) is not int or source_id <= 0 or source_id in input_ids:
                raise SourceManagerServiceError('invalid_request')
            input_ids.append(source_id)

        def normalized_type(source):
            source_type = getattr(source, 'source_type', None)
            return source_type.value if isinstance(source_type, SourceType) else source_type

        # Keep a stable empty entry for every supported source even if adapter
        # preparation, collection, normalization, or the pass deadline fails.
        results: Dict[int, List[Article]] = {
            source_id: [] for source_id, source in zip(input_ids, sources)
            if normalized_type(source) in {'rss', 'telegram', 'custom'}
        }
        batch_count = max(1, (len(sources) + 7) // 8)
        postprocess_reserve = min(30.0, float(total_timeout)) * min(8, len(sources))
        overall_budget = min(600.0, max(0.01, float(total_timeout)) * batch_count
                             + postprocess_reserve)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + overall_budget
        client = SourceManagerServiceClient(
            settings.extraction_service_url,
            settings.extraction_service_token.get_secret_value(),
        )
        by_id = dict(zip(input_ids, sources))
        descriptors = {}

        for source_id, source in by_id.items():
            source_type = normalized_type(source)
            if source_type not in {'rss', 'telegram', 'custom'}:
                logger.warning('Go SourceManager skipped unsupported source type id=%s', source_id)
                continue
            source_config = source.config or {}
            descriptor = {
                'source_id': source_id,
                'type': source_type,
                'source_name': source.name,
                'url': source.url,
                'enabled': bool(source.enabled),
                'per_source_budget_ms': per_source_budget_ms,
            }
            if source_type == 'rss':
                rss_limit = source_config.get('limit')
                if type(rss_limit) is int and rss_limit > 0:
                    descriptor['limit'] = rss_limit
            elif source_type == 'telegram':
                try:
                    instance = await self.get_source_instance(source)
                    tg_limit = source_config.get('limit')
                    # Match Python Telegram's existing upper bound before its
                    # typed config validator; malformed and nonpositive values
                    # still reach the validator unchanged and are rejected.
                    config_limit = min(tg_limit, 100) if type(tg_limit) is int and tg_limit > 0 else tg_limit
                    descriptor['telegram'] = instance.go_collection_config(config_limit)
                    if type(tg_limit) is int:
                        descriptor['limit'] = min(tg_limit, 100)
                except Exception:
                    logger.warning('Go SourceManager cannot prepare Telegram source id=%s code=preparation_failed',
                                   source_id)
                    continue
            else:
                try:
                    instance = await self.get_source_instance(source)
                    prepare = getattr(instance, 'prepare_go_collection', None)
                    if prepare is None:
                        logger.warning('Go SourceManager cannot prepare page monitor id=%s code=unsupported',
                                       source_id)
                        continue
                    pm_config = await prepare()
                    pm_limit = pm_config.pop('limit', None)
                    if type(pm_limit) is int and pm_limit > 0:
                        descriptor['limit'] = min(pm_limit, 100)
                    descriptor['page_monitor'] = pm_config
                except Exception:
                    logger.warning('Go SourceManager cannot prepare page monitor id=%s code=preparation_failed',
                                   source_id)
                    continue
            descriptors[source_id] = descriptor

        pending = list(descriptors.values())
        deferred_retries: Dict[int, int] = {}
        while pending and loop.time() < deadline:
            batch = pending[:8]
            pending = pending[8:]
            remaining = max(0.0, deadline - loop.time())
            if remaining <= 0:
                break
            try:
                response = await client.collect(
                    batch, timeout_seconds=min(120.0, remaining),
                    admission_timeout_seconds=remaining)
            except (SourceManagerServiceError, ValueError) as exc:
                code = exc.code if isinstance(exc, SourceManagerServiceError) else 'invalid_configuration'
                logger.warning('Go SourceManager batch failed code=%s sources=%d', code, len(batch))
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning('Go SourceManager batch failed code=unavailable sources=%d', len(batch))
                continue

            deferred = []
            retry_after = 0.0
            for item in response['results']:
                source_id = item['source_id']
                source = by_id[source_id]
                if item['outcome'] == 'rejected':
                    logger.warning('Go SourceManager rejected source id=%s', source_id)
                    continue
                if item['outcome'] == 'failed':
                    error = item.get('error') or {}
                    logger.warning('Go SourceManager source failed id=%s code=%s',
                                   source_id, error.get('code', 'failed'))
                    continue
                # Consume any typed prefix before scheduling deferred work. The Go
                # contract correlates this payload with the same source ID.
                try:
                    articles = await self._normalize_go_source_result(
                        source, item, deadline=deadline)
                    payload_key = {'rss': 'rss_articles', 'telegram': 'telegram',
                                   'custom': 'page_monitor'}[item['type']]
                    if articles or item['outcome'] in {'success', 'partial'} or payload_key in item:
                        results[source_id] = results.get(source_id, []) + articles
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning('Go SourceManager normalization failed id=%s code=normalization_failed',
                                   source_id)
                if item['outcome'] == 'deferred':
                    retries = deferred_retries.get(source_id, 0)
                    if retries < 8:
                        deferred_retries[source_id] = retries + 1
                        deferred.append(descriptors[source_id])
                        error = item.get('error') or {}
                        delay = error.get('retry_after_seconds', 0)
                        if type(delay) is int and delay > 0:
                            retry_after = max(retry_after, min(float(delay), 5.0))
            if deferred and loop.time() < deadline:
                # Retry only a few times, with bounded Retry-After where supplied.
                # This keeps instant deferred responses from creating a tight loop.
                await asyncio.sleep(min(max(0.25, retry_after), max(0.0, deadline - loop.time())))
                pending.extend(deferred)
        if pending:
            logger.warning('Go SourceManager pass ended with deferred source count=%d', len(pending))
        return results

    async def _normalize_go_source_result(self, source, result, *, deadline):
        source_type = getattr(source, 'source_type', None)
        source_type = source_type.value if isinstance(source_type, SourceType) else source_type
        if source_type == 'rss':
            from ..sources.rss_source import RSSSource
            return [RSSSource.article_from_go(item) for item in (result.get('rss_articles') or [])]
        instance = await self.get_source_instance(source)
        if source_type == 'telegram':
            from ..config import get_settings
            response = result.get('telegram')
            if response is None:
                return []
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            # The source helper runs only after the manager call has returned, so
            # optional linked-article enrichment cannot retain a Go batch slot.
            return [article async for article in instance.consume_go_collection(
                response, settings=get_settings(), deadline=time.monotonic() + remaining)]
        response = result.get('page_monitor')
        if response is None:
            return []
        remaining = max(0.0, deadline - asyncio.get_running_loop().time())
        return await instance.finalize_go_collection(
            response, deadline=asyncio.get_running_loop().time() + remaining)

    @staticmethod
    def _collect_candidate_urls(article) -> List[str]:
        """Build URL list from article.url + raw_data telegram/original links."""
        urls = [article.url]
        try:
            raw = getattr(article, 'raw_data', None)
            if raw:
                telegram_url = raw.get('telegram_url') or raw.get('message_url')
                original_link = raw.get('original_link')
                for u in (telegram_url, original_link):
                    if u and u not in urls:
                        urls.append(u)
        except Exception as e:
            logger.debug(f"Error collecting candidate URLs: {e}")
        return urls

    @staticmethod
    def _is_batch_duplicate(urls: List[str], title: str,
                            seen_urls: set, seen_titles: set) -> bool:
        """Check in-memory batch-level dedup by URLs and title."""
        if any(u in seen_urls for u in urls if u):
            return True
        if title and title in seen_titles:
            return True
        return False

    @staticmethod
    async def _check_db_duplicate(db: AsyncSession, urls: List[str]) -> Optional[Article]:
        """Check if any of the candidate URLs already exist in DB."""
        result = await db.execute(
            select(Article).where(Article.url.in_(urls)).limit(1)
        )
        return result.scalars().first()

    @staticmethod
    async def _check_title_duplicate(db: AsyncSession, source_id: int, title: str) -> bool:
        """Check for near-duplicate by title within the same source (7-day window)."""
        if not title:
            return False
        try:
            cutoff = datetime.utcnow() - timedelta(days=7)
            result = await db.execute(
                select(Article.id).where(
                    and_(
                        Article.source_id == source_id,
                        func.lower(Article.title) == title,
                        Article.fetched_at >= cutoff,
                    )
                ).limit(1)
            )
            return result.scalars().first() is not None
        except Exception as e:
            logger.debug(f"Error checking title duplicate: {e}")
            return False

    @staticmethod
    def _apply_raw_data_fields(db_article: Article, raw_data) -> None:
        """Apply ad detection and metadata from raw_data to article."""
        try:
            if isinstance(raw_data, dict):
                if 'advertising_detection' in raw_data or 'is_advertisement' in raw_data:
                    db_article.is_advertisement = bool(raw_data.get('is_advertisement', False))
                    if 'ad_confidence' in raw_data:
                        db_article.ad_confidence = float(raw_data.get('ad_confidence') or 0.0)
                    if 'ad_type' in raw_data:
                        db_article.ad_type = raw_data.get('ad_type')
                    if 'ad_reasoning' in raw_data:
                        db_article.ad_reasoning = raw_data.get('ad_reasoning')
                    if 'ad_markers' in raw_data:
                        db_article.ad_markers = raw_data.get('ad_markers')
                    db_article.ad_processed = True
        except Exception as e:
            logger.debug(f"Error applying raw_data fields: {e}")

    @staticmethod
    def _record_in_seen_sets(urls: List[str], title: str,
                             seen_urls: set, seen_titles: set) -> None:
        """Record URLs and title into batch dedup sets."""
        for u in urls:
            if u:
                seen_urls.add(u)
        if title:
            seen_titles.add(title)

    async def save_fetched_articles_with_sources(self, raw_articles: Dict[str | int, List[Article]],
                                                  source_map: Dict[str | int, Source], db: AsyncSession) -> Dict[str | int, List[Article]]:
        """
        Save fetched articles to database using provided source mapping.
        This is pure DB writes, no HTTP fetching.
        """
        results = {}

        # Debug logs removed after verification

        for source_key, articles in raw_articles.items():
            try:

                source = source_map.get(source_key)
                if not source:
                    logger.info(f"Source '{source_key}' not found in source_map — skipping")
                    results[source_key] = []
                    continue

                source.last_fetch = datetime.utcnow()
                saved_articles = []
                seen_urls: set[str] = set()
                seen_titles: set[str] = set()

                for article in articles:
                    urls = self._collect_candidate_urls(article)
                    normalized_title = (article.title or "").strip().lower()

                    # In-memory batch dedup
                    if self._is_batch_duplicate(urls, normalized_title, seen_urls, seen_titles):

                        continue

                    # DB URL dedup
                    existing = await self._check_db_duplicate(db, urls)
                    if existing is not None:
                        if existing.summary and existing.processed:

                            continue

                        # Update existing unprocessed article
                        logger.info(f"  🔄 Updating existing unprocessed article: {existing.url[:50]}...")
                        existing.title = article.title or existing.title
                        existing.content = article.content or existing.content
                        existing.url = article.url or existing.url
                        existing.image_url = article.image_url or existing.image_url
                        existing.published_at = article.published_at or existing.published_at
                        existing.hash_content = self._calculate_content_hash(article) or existing.hash_content
                        saved_articles.append(existing)
                        self._record_in_seen_sets(urls, normalized_title, seen_urls, seen_titles)
                        continue

                    # DB title dedup (same source, 7-day window)
                    if await self._check_title_duplicate(db, source.id, normalized_title):

                        continue

                    # Create new article
                    db_article = Article(
                        source_id=source.id,
                        title=article.title,
                        url=article.url,
                        content=article.content,
                        summary=article.summary,
                        image_url=article.image_url,
                        media_files=article.media_files or [],
                        published_at=article.published_at,
                        processed=False,
                        hash_content=self._calculate_content_hash(article)
                    )
                    self._apply_raw_data_fields(db_article, getattr(article, 'raw_data', None))

                    db.add(db_article)
                    saved_articles.append(db_article)
                    self._record_in_seen_sets(urls, normalized_title, seen_urls, seen_titles)

                results[source_key] = saved_articles

            except Exception as e:
                logger.info(f"Error saving articles for {source_key}: {e}")
                results[source_key] = []

        return results

    def _calculate_content_hash(self, article) -> str:
        """Calculate hash for article content (for deduplication)."""
        import hashlib
        
        content = f"{article.title}:{article.url}"
        return hashlib.sha256(content.encode()).hexdigest()
    
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
