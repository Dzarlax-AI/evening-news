"""Refactored main orchestrator for Evening News v2."""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text, and_, or_, update

from .models import Source, Article, ProcessingStat, DailySummary
from .services.source_manager import SourceManager
from .processing.ai_processor import AIProcessor
from .processing.summarization_processor import SummarizationProcessor
from .processing.categorization_processor import CategorizationProcessor
from .processing.digest_builder import DigestBuilder
from .processing.stats_collector import StatsCollector
from .services.ai_client import get_ai_client
from .services.telegram_service import get_telegram_service, TelegramService
from .services.database_queue import get_database_queue, DatabaseQueueManager
from .services.article_limiter import get_article_limiter, ArticleLimiter
from .core.exceptions import NewsAggregatorError
from .config import settings
from .processing.processing_outcome import ProcessingOutcome, outcome_fields

logger = logging.getLogger(__name__)

# All scheduler and manual entry points share this process-local reservation.
# Deploy one worker process; this is deliberately not a distributed lease.
_processing_cycle_token = None


def reserve_processing_cycle():
    global _processing_cycle_token
    if _processing_cycle_token is not None:
        return None
    _processing_cycle_token = object()
    return _processing_cycle_token


def release_processing_cycle(token):
    global _processing_cycle_token
    if token is _processing_cycle_token:
        _processing_cycle_token = None


def processing_cycle_busy():
    return _processing_cycle_token is not None


async def _release_browser_lease(lease):
    """Finish resource cleanup despite repeated cancellation of a manual run."""
    from .core.browser_pool import release_browser_cycle
    cleanup = asyncio.create_task(release_browser_cycle(lease))
    cancelled = False
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    try:
        await cleanup
    except Exception:
        logger.exception('Browser lease cleanup failed')
    if cancelled:
        raise asyncio.CancelledError


@dataclass
class ArticleDTO:
    """Lightweight article data transfer object for AI processing."""
    id: int
    url: str
    title: str
    content: Optional[str]
    summary: Optional[str]
    published_at: Optional[datetime]

    @classmethod
    def for_summarization(cls, data: Dict[str, Any]) -> "ArticleDTO":
        return cls(
            id=data['id'],
            url=data['url'],
            title=data['title'],
            content=data['content'],
            summary=data['summary'],
            published_at=data['published_at'],
        )

    @classmethod
    def for_categorization(cls, data: Dict[str, Any]) -> "ArticleDTO":
        return cls(
            id=data['id'],
            url=data['url'],
            title=data['title'],
            content=data['content'],
            summary=data.get('summary') or data['content'],
            published_at=data['published_at'],
        )


class NewsOrchestrator:
    """Main orchestrator for news processing - refactored with modular processors."""
    
    def __init__(self):
        self.source_manager = SourceManager()
        self.ai_processor = AIProcessor()
        self.ai_client = get_ai_client()
        self.telegram_service = get_telegram_service()

        # Use new universal database queue system
        self.db_queue_manager = get_database_queue()

        # Initialize specialized processors
        self.summarization_processor = SummarizationProcessor()
        self.categorization_processor = CategorizationProcessor()
        self.digest_builder = DigestBuilder()
        self.stats_collector = StatsCollector()

        # Article limiter for processing limits
        self.article_limiter = get_article_limiter()
        # Log limit status on startup
        self.article_limiter.log_limit_status()

        # Legacy queue for backward compatibility (will be removed)
        self.db_queue = None

    @classmethod
    def for_article_processing(cls, ai_processor):
        """Build the selected-article facade without unrelated integrations."""
        orchestrator = cls.__new__(cls)
        orchestrator.ai_processor = ai_processor
        orchestrator.db_queue_manager = get_database_queue()
        orchestrator.article_limiter = get_article_limiter()
        return orchestrator

    def _daily_digest_url(self, date) -> str:
        base_url = (settings.site_base_url or "https://news.dzarlax.dev").rstrip("/")
        return f"{base_url}/digest/{date.strftime('%Y-%m-%d')}"
    
    async def start(self):
        """Start the orchestrator and its database queue."""
        await self.db_queue_manager.start()
        
    async def stop(self):
        """Stop the orchestrator and its database queue."""
        await self.db_queue_manager.stop()
        
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get database queue statistics."""
        return self.db_queue_manager.get_stats()
        
    async def run_full_cycle(self, reservation=None) -> Dict[str, Any]:
        token = reservation or reserve_processing_cycle()
        if token is None:
            return {"status": "busy", "articles_processed": 0, "fatal_error": None,
                    "errors": []}
        if token is not _processing_cycle_token:
            raise ValueError("Invalid processing cycle reservation")
        try:
            return await self._run_full_cycle()
        finally:
            if reservation is None:
                release_processing_cycle(token)

    async def _run_full_cycle(self) -> Dict[str, Any]:
        """Run complete news processing cycle."""
        from .core.browser_pool import acquire_browser_cycle

        browser_cycle_lease = await acquire_browser_cycle()
        start_time = datetime.utcnow()
        stats = {
            'start_time': start_time.isoformat(),
            'sources_synced': 0,
            'articles_fetched': 0,
            'articles_processed': 0,
            'articles_summarized': 0,
            'articles_categorized': 0,
            'categories_found': set(),
            'api_calls_made': 0,
            'errors': [],
            'fatal_error': None,
            'performance': {}
        }
        
        try:
            logger.info(f"🚀 Starting full news processing cycle at {start_time.strftime('%H:%M:%S')}")
            # Step 1: Sync sources and fetch articles
            logger.info("📥 Step 1: Syncing sources and fetching articles...")
            sync_start = time.time()

            # Step 1a: Get sources list (quick DB read)
            logger.info("  📋 Getting enabled sources...")
            sources = await self.source_manager.get_sources_from_db()
            logger.info(f"  ✅ Found {len(sources)} enabled sources")
            # Step 1b: HTTP fetching (NO DB transaction - semaphore free!)
            logger.info("  🌐 Fetching articles via HTTP (no DB lock)...")
            fetch_start = time.time()

            # HTTP fetching happens here WITHOUT database lock
            raw_articles = await self.source_manager.fetch_from_all_sources_no_db(sources)

            fetch_duration = time.time() - fetch_start
            logger.info(f"  ✅ HTTP fetching completed in {fetch_duration:.1f}s")
            # Step 1c: Save articles to database (quick DB write)
            logger.info("  💾 Saving articles to database...")
            save_start = time.time()

            async def save_operation(db):
                # Refresh sources with DB session
                from sqlalchemy import select
                source_query = select(Source).where(Source.enabled == True)
                result = await db.execute(source_query)
                sources_db = result.scalars().all()

                # Go collection is correlated and persisted by stable source ID;
                # Python mode keeps its historical name keyed return shape.
                if settings.source_manager_mode == 'go':
                    source_map = {s.id: s for s in sources_db}
                else:
                    source_map = {s.name: s for s in sources_db}

                return await self.source_manager.save_fetched_articles_with_sources(raw_articles, source_map, db)

            sync_result = await self.db_queue_manager.execute_write(save_operation, timeout=30.0)
            total_articles = sum(len(articles) for articles in sync_result.values())

            save_duration = time.time() - save_start
            logger.info(f"  ✅ Saved {total_articles} articles in {save_duration:.1f}s")
            stats.update({
                'sources_synced': len(sync_result),
                'articles_fetched': total_articles
            })

            sync_duration = time.time() - sync_start
            stats['performance']['sync_duration'] = sync_duration
            logger.info(f"  ✅ Total sync: {stats['sources_synced']} sources, {stats['articles_fetched']} articles in {sync_duration:.1f}s")
            # Step 2: Process articles with AI
            logger.info("🤖 Step 2: Processing articles with AI...")
            process_start = time.time()
            
            processing_result = await self._process_unprocessed_articles(stats)
            stats.update(processing_result)
            
            process_duration = time.time() - process_start
            stats['performance']['processing_duration'] = process_duration
            logger.info(f"  ✅ Processed {stats['articles_processed']} articles in {process_duration:.1f}s")
            # Calculate total duration
            end_time = datetime.utcnow()
            total_duration = (end_time - start_time).total_seconds()
            stats['end_time'] = end_time.isoformat()
            stats['total_duration'] = total_duration
            stats['duration_seconds'] = total_duration
            
            # Summary
            logger.info(f"Processing cycle completed in {total_duration:.1f}s")
            logger.info(f"   {stats['articles_processed']} articles processed")
            logger.info(f"   {len(stats['categories_found'])} categories found")
            logger.info(f"   {stats['api_calls_made']} API calls made")
            # Persist processing stats for dashboard
            try:
                from .processing.processing_stats_service import get_processing_stats_service
                processing_stats_service = get_processing_stats_service()
                await self.db_queue_manager.execute_write(
                    lambda db: processing_stats_service.update_processing_stats(db, stats)
                )
            except Exception as e:
                logger.info(f"  Failed to update processing stats: {e}")
            return stats
            
        except Exception as e:
            import traceback
            error_msg = f"Error in full processing cycle: {str(e)}"
            logger.error(f"❌ {error_msg}")
            logger.info(f"📍 Traceback:\n{traceback.format_exc()}")
            stats['errors'].append(error_msg)
            stats['fatal_error'] = error_msg
            return stats
        finally:
            # News cycles share one CDP client session but never own Chrome itself.
            # Release this cycle's lease on every outcome; the pool disconnects
            # only after the last overlapping cycle has exited.
            from .core.browser_pool import release_browser_cycle

            cleanup_task = asyncio.create_task(
                release_browser_cycle(browser_cycle_lease)
            )
            cancelled = False
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError:
                    cancelled = True
                except Exception:
                    break
            try:
                await cleanup_task
            except Exception as cleanup_error:
                # Cleanup is secondary and must not replace the cycle outcome.
                logger.warning(
                    "Browser session cleanup failed (%s): %s",
                    type(cleanup_error).__name__,
                    cleanup_error,
                )
            if cancelled:
                raise asyncio.CancelledError
    
    async def _get_telegram_service_with_db_overrides(self) -> TelegramService:
        """Load telegram channel IDs from DB and return a properly configured service."""
        from .models import Setting

        async def _load(db):
            chat_id_row = await db.get(Setting, "telegram_chat_id")
            service_chat_id_row = await db.get(Setting, "telegram_service_chat_id")
            return (
                chat_id_row.value if chat_id_row else None,
                service_chat_id_row.value if service_chat_id_row else None,
            )

        chat_id, service_chat_id = await self.db_queue_manager.execute_read(_load, timeout=5.0)
        return get_telegram_service(
            chat_id=chat_id or None,
            service_chat_id=service_chat_id or None,
        )

    async def send_telegram_digest(self) -> Dict[str, Any]:
        """Generate and send Telegram digest."""
        try:
            logger.info("📱 Generating and sending Telegram digest...")
            today = datetime.utcnow().date()
            digest_url = self._daily_digest_url(today)

            # Reload telegram service with DB overrides so admin UI changes take effect
            self.telegram_service = await self._get_telegram_service_with_db_overrides()

            # Step 1: Check if daily summaries exist (fast read)
            async def check_summaries_operation(db):
                from .models import DailySummary
                from sqlalchemy import select, func

                result = await db.execute(
                    select(func.count(DailySummary.id)).where(DailySummary.date == today)
                )
                return int(result.scalar() or 0)

            summaries_count = await self.db_queue_manager.execute_read(check_summaries_operation, timeout=30.0)

            # Step 2: Generate missing daily summaries (potentially slow write + AI)
            if summaries_count == 0:
                logger.info("📊 No daily summaries found for today - generating them first...")
                summary_result = await self._generate_daily_summaries(timeout=300.0)
                logger.info(f"  ✅ Generated {summary_result.get('summaries_generated', 0)} daily summaries")
            else:
                logger.info(f"📊 Using existing {summaries_count} daily summaries for today")
            # Step 3: Build digest (read-only operation)
            async def build_digest_operation(db):
                rich_digest = None
                if settings.telegram_rich_messages_enabled:
                    rich_digest = await self.digest_builder.create_rich_digest(db, today, digest_url)

                digest_content = await self.digest_builder.create_combined_digest(db, today)

                if digest_content == "SPLIT_NEEDED":
                    # Use new method that handles splitting internally
                    digest_parts = await self.digest_builder.create_digest_parts(db, today)

                    if not digest_parts or digest_parts[0] == "Сводки новостей пока не готовы.":
                        return {'error': 'No valid summaries found'}

                    return {'digest_parts': digest_parts, 'split': True, 'rich_digest': rich_digest}

                return {'digest_content': digest_content, 'split': False, 'rich_digest': rich_digest}

            digest_result = await self.db_queue_manager.execute_read(build_digest_operation, timeout=30.0)

            if settings.telegram_rich_messages_enabled and digest_result.get('rich_digest'):
                rich_result = await self.telegram_service.send_rich_message(digest_result['rich_digest'])
                if rich_result:
                    return {'success': True, 'parts_sent': 1, 'rich_message': True}

                logger.warning(
                    "⚠️ Rich Telegram digest failed (%s); sending regular HTML digest fallback",
                    rich_result.error_summary(),
                )

            link_line = f"\n<b>Полная версия:</b> <a href=\"{digest_url}\">Evening News</a>"
            if digest_result.get('split'):
                # Send multiple parts
                digest_result['digest_parts'][0] = digest_result['digest_parts'][0] + link_line

                sent_ok = 0
                errors = []
                for part in digest_result['digest_parts']:
                    send_result = await self.telegram_service.send_message(part)
                    if send_result:
                        sent_ok += 1
                    else:
                        errors.append(send_result.error_summary())
                success = sent_ok == len(digest_result['digest_parts'])
                return {
                    'success': success,
                    'parts_sent': sent_ok,
                    'parts_total': len(digest_result['digest_parts']),
                    'error': '; '.join(errors) if errors else None,
                }
            else:
                # Send single message
                digest_content = digest_result['digest_content'] + link_line
                send_result = await self.telegram_service.send_message(digest_content)
                return {
                    'success': bool(send_result),
                    'parts_sent': 1 if send_result else 0,
                    'parts_total': 1,
                    'error': None if send_result else send_result.error_summary(),
                }
                
        except Exception as e:
            error_msg = f"Error sending Telegram digest ({type(e).__name__}): {e}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}

    async def send_operational_alert(self, title: str, message: str):
        """Send an alert, falling back to environment config when DB is unavailable."""
        try:
            self.telegram_service = await self._get_telegram_service_with_db_overrides()
        except Exception as exc:
            logger.warning(
                "Could not load Telegram overrides from the database; "
                "using environment configuration for operational alert: %s",
                exc,
            )
            self.telegram_service = get_telegram_service()
        return await self.telegram_service.send_alert(title, message)
    
    def _processing_limits(self):
        return {
            'max_attempts': getattr(settings, 'processing_max_attempts', 3),
            'initial_seconds': getattr(settings, 'processing_retry_initial_seconds', 1800),
            'later_seconds': getattr(settings, 'processing_retry_later_seconds', 21600),
        }

    async def _process_unprocessed_articles(self, stats: Dict[str, Any], selected_ids=None,
                                          max_articles=None, force_refresh=False,
                                          preserve_completed_stages=False) -> Dict[str, Any]:
        """Walk due articles by stable ID in bounded batches, persisting each outcome."""
        counters = {'articles_processed': 0, 'articles_summarized': 0,
                    'articles_categorized': 0}
        limits = self._processing_limits()
        batch_size = max(1, min(100, getattr(settings, 'processing_batch_size', 25)))
        attempt_timeout = getattr(settings, 'processing_attempt_timeout_seconds', 600)
        workers = max(1, settings.max_workers)
        cursor = 0
        selected_count = 0
        cycle_time = datetime.utcnow()
        cycle_max = self.article_limiter.get_max_articles() if self.article_limiter.is_enabled() else None
        if max_articles is not None:
            cycle_max = min(cycle_max, max_articles) if cycle_max is not None else max_articles
        per_source = self.article_limiter.get_per_source_limit() if self.article_limiter.is_enabled() else None
        source_counts = {}
        try:
            # A crashed final attempt must become terminal when its recovery deadline is due.
            async def recover_exhausted(db):
                await db.execute(update(Article).where(
                    Article.processing_state.in_(['pending', 'retry_wait']),
                    Article.processing_attempts >= limits['max_attempts'],
                    or_(Article.next_attempt_at.is_(None), Article.next_attempt_at <= cycle_time),
                ).values(processing_state='exhausted', next_attempt_at=None, processed=True))
            await self.db_queue_manager.execute_write(recover_exhausted)

            while cycle_max is None or selected_count < cycle_max:
                remaining = batch_size if cycle_max is None else min(batch_size, cycle_max - selected_count)
                async def fetch_batch(db):
                    query = select(
                        Article.id, Article.title, Article.url, Article.content, Article.summary,
                        Article.source_id, Source.source_type, Article.published_at,
                        Article.summary_processed, Article.category_processed, Article.ad_processed,
                        Article.is_advertisement,
                        Article.processing_attempts, Article.image_url, Article.media_files,
                    ).outerjoin(Source, Source.id == Article.source_id).where(
                        Article.id > cursor,
                        Article.processing_attempts < limits['max_attempts'],
                        or_(Article.processing_state == 'pending', and_(
                            Article.processing_state == 'retry_wait',
                            Article.next_attempt_at <= cycle_time)),
                    ).order_by(Article.id).limit(remaining)
                    if selected_ids is not None:
                        query = query.where(Article.id.in_(selected_ids))
                    else:
                        query = self.article_limiter.apply_date_filters_to_query(query)
                    result = await db.execute(query)
                    return [dict(row) for row in result.mappings().all()]

                batch = await self.db_queue_manager.execute_read(fetch_batch, timeout=10.0)
                if not batch:
                    break
                cursor = batch[-1]['id']
                eligible = []
                for article in batch:
                    source_id = article['source_id']
                    if per_source and source_counts.get(source_id, 0) >= per_source:
                        continue
                    source_counts[source_id] = source_counts.get(source_id, 0) + 1
                    eligible.append(article)
                selected_count += len(eligible)
                semaphore = asyncio.Semaphore(workers)

                async def process_one(article):
                    async with semaphore:
                        attempt = article['processing_attempts'] + 1
                        # Persist a finite recovery deadline BEFORE any extraction/AI work.
                        async def claim(db):
                            result = await db.execute(update(Article).where(
                                Article.id == article['id'],
                                Article.processing_attempts == attempt - 1,
                                Article.processing_state.in_(['pending', 'retry_wait']),
                            ).values(processing_attempts=attempt, processing_state='retry_wait',
                                     next_attempt_at=datetime.utcnow() + timedelta(seconds=max(
                                         attempt_timeout + 60,
                                         limits['initial_seconds'] if attempt == 1 else limits['later_seconds'])),
                                     last_processing_error='Attempt interrupted before outcome was saved'))
                            return result.rowcount == 1
                        if not await self.db_queue_manager.execute_write(claim):
                            return
                        try:
                            article_force_refresh = force_refresh and not (
                                preserve_completed_stages and any(article.get(flag) for flag in (
                                    'summary_processed', 'category_processed', 'ad_processed'))
                            )
                            processing = (
                                self.ai_processor.process_article_outcome(article, stats, force_refresh=True)
                                if article_force_refresh else self.ai_processor.process_article_outcome(article, stats)
                            )
                            outcome = await asyncio.wait_for(
                                processing,
                                timeout=attempt_timeout,
                            )
                        except Exception as exc:
                            outcome = ProcessingOutcome('retry_wait', error=type(exc).__name__)
                        fields = outcome_fields(outcome, attempt, datetime.utcnow(), **limits)
                        categories = fields.pop('categories', None)
                        async def save(db):
                            # Category writes and flags share one auto-committed transaction.
                            if categories is not None:
                                await self._save_ai_categories_to_database(db, article['id'], categories)
                            await db.execute(update(Article).where(
                                Article.id == article['id'], Article.processing_attempts == attempt,
                            ).values(**fields))
                        try:
                            await self.db_queue_manager.execute_write(save, timeout=30.0)
                        except (ValueError, LookupError) as exc:
                            # Invalid category mappings belong to this article.
                            # The failed transaction rolled back; retain its claim
                            # deadline and attempt budget while siblings continue.
                            async def record_mapping_error(db):
                                await db.execute(update(Article).where(
                                    Article.id == article['id'],
                                    Article.processing_attempts == attempt,
                                ).values(last_processing_error='category_mapping_failed'))
                            await self.db_queue_manager.execute_write(record_mapping_error)
                            stats.setdefault('errors', []).append(
                                f"Article {article['id']} category mapping failed ({type(exc).__name__})")
                            return
                        if outcome.state == 'completed':
                            counters['articles_processed'] += 1
                        if fields.get('summary'):
                            counters['articles_summarized'] += 1
                        if categories:
                            counters['articles_categorized'] += 1
                # Only one batch of tasks and payloads exists at a time.
                async with asyncio.TaskGroup() as tasks:
                    for article in eligible:
                        tasks.create_task(process_one(article))
            return counters
        except Exception as exc:
            stats.setdefault('errors', []).append(f'Error processing unprocessed articles: {exc}')
            logger.exception('Article processing failed')
            # Infrastructure failures must abort the cycle before digest delivery.
            raise

    async def retry_selected_articles(self, article_ids, reservation=None):
        """Reset only explicit selections, preserving successful stage results."""
        token = reservation or reserve_processing_cycle()
        if token is None:
            raise RuntimeError('News processing is already running')
        if token is not _processing_cycle_token:
            raise ValueError('Invalid processing cycle reservation')
        try:
            return await self._reset_selected_articles(article_ids)
        finally:
            if reservation is None:
                release_processing_cycle(token)

    async def _reset_selected_articles(self, article_ids, force=False):
        ids = sorted(set(article_ids))
        if not ids or len(ids) > 100 or any(type(i) is not int or i < 1 for i in ids):
            raise ValueError('Select between 1 and 100 positive article IDs')
        async def reset(db):
            fields = dict(processing_state='pending', processing_attempts=0,
                          next_attempt_at=None, last_processing_error=None, processed=False)
            if force:
                fields.update(summary_processed=False, category_processed=False, ad_processed=False)
            result = await db.execute(update(Article).where(Article.id.in_(ids)).values(**fields))
            return result.rowcount
        return await self.db_queue_manager.execute_write(reset)

    async def process_selected_articles(self, article_ids, reservation=None, force=False, reset=True,
                                        preserve_completed_stages=False):
        """Retry an explicit selection through the same guarded, durable pipeline."""
        token = reservation or reserve_processing_cycle()
        if token is None:
            return {'status': 'busy', 'success': False}
        if token is not _processing_cycle_token:
            raise ValueError('Invalid processing cycle reservation')
        browser_lease = None
        try:
            from .core.browser_pool import acquire_browser_cycle
            browser_lease = await acquire_browser_cycle()
            if reset:
                await self._reset_selected_articles(article_ids, force=force)
            stats = {'api_calls_made': 0, 'errors': []}
            stats.update(await self._process_unprocessed_articles(
                stats, selected_ids=article_ids, force_refresh=force,
                preserve_completed_stages=preserve_completed_stages))
            # Selected requests report durable outcomes, while cycle counters
            # continue to count completed news articles only.
            async def read_outcomes(db):
                result = await db.execute(select(
                    Article.id, Article.processing_state, Article.last_processing_error,
                    Article.next_attempt_at,
                ).where(Article.id.in_(article_ids)).order_by(Article.id))
                return [
                    {'article_id': row.id, 'state': row.processing_state,
                     'reason': row.last_processing_error,
                     'next_attempt_at': row.next_attempt_at.isoformat() if row.next_attempt_at else None}
                    for row in result.all()
                ]
            outcomes = await self.db_queue_manager.execute_read(read_outcomes)
            stats['article_outcomes'] = outcomes
            stats['settled'] = len(outcomes) == len(set(article_ids)) and all(
                item['state'] in {'completed', 'rejected'} for item in outcomes)
            stats['success'] = stats['settled'] and not stats['errors']
            stats['retryable'] = any(item['state'] in {'pending', 'retry_wait'} for item in outcomes)
            stats['status'] = outcomes[0]['state'] if len(outcomes) == 1 else (
                'settled' if stats['settled'] else 'incomplete')
            return stats
        finally:
            try:
                if browser_lease is not None:
                    await _release_browser_lease(browser_lease)
            finally:
                if reservation is None:
                    release_processing_cycle(token)

    async def _generate_daily_summaries(self, timeout: float = 60.0) -> Dict[str, Any]:
        """Generate daily summaries: read articles, generate AI summaries, save to DB."""
        try:
            today = datetime.utcnow().date()

            # Step 1: Read articles grouped by category (fast read, no AI)
            async def read_op(db):
                return await self._group_articles_by_category(db, today)

            categories = await self.db_queue_manager.execute_read(read_op, timeout=30.0)

            if not categories:
                logger.info("  📭 No articles found for daily summary generation")
                return {'summaries_generated': 0}

            # Step 2: Generate AI summaries outside any DB lock
            summary_generator = self.digest_builder.summary_generator
            generated: Dict[str, Any] = {}
            for category, articles in categories.items():
                if not articles:
                    continue
                summary_text = await summary_generator.generate_category_summary(
                    category, articles, today
                )
                if summary_text:
                    generated[category] = (summary_text, len(articles))

            if not generated:
                logger.warning("  ⚠️ No summaries generated by AI")
                return {'summaries_generated': 0}

            # Step 3: Save summaries to DB (fast write, no AI)
            async def write_op(db):
                for cat, (text, count) in generated.items():
                    await summary_generator.save_summary(db, today, cat, text, count)
                return {'summaries_generated': len(generated)}

            return await self.db_queue_manager.execute_write(write_op, timeout=30.0)

        except Exception as e:
            error_msg = f"Error generating daily summaries: {e}"
            logger.error(f"  ❌ {error_msg}")
            return {'summaries_generated': 0, 'error': error_msg}
    
    async def _group_articles_by_category(self, db: AsyncSession, date) -> Dict[str, List[Article]]:
        """Fetch today's articles grouped by category (best confidence per article, excluding ads)."""
        from .models import ArticleCategory, Category

        # Join through category_id directly — no mapping service needed
        result = await db.execute(
            select(Article, Category.name, ArticleCategory.confidence)
            .join(ArticleCategory, Article.id == ArticleCategory.article_id)
            .join(Category, Category.id == ArticleCategory.category_id)
            .where(func.date(Article.fetched_at) == date)
            .where(Article.processing_state == 'completed')
            .where(Article.is_advertisement != True)  # Filter out ads
            .order_by(Article.fetched_at.desc())
        )

        # Keep only the best (highest confidence) category per article
        best_by_article: Dict[int, Dict] = {}
        for article, category_name, confidence in result.all():
            current = best_by_article.get(article.id)
            if current is None or (confidence or 0) > (current['confidence'] or 0):
                best_by_article[article.id] = {
                    'article': article,
                    'category': category_name or 'Other',
                    'confidence': confidence or 0,
                }

        grouped: Dict[str, List[Article]] = {}
        for item in best_by_article.values():
            grouped.setdefault(item['category'], []).append(item['article'])

        return grouped

    async def _save_ai_categories_to_database(self, db: AsyncSession, article_id: int,
                                              ai_categories: List[Dict[str, Any]],
                                              preloaded_categories: Optional[Dict[str, int]] = None):
        """Save AI categories to database, resolving category_id via display service mapping.

        Pass preloaded_categories to avoid repeated SELECT Category queries when saving
        multiple articles in the same transaction.
        """
        try:
            from sqlalchemy import text
            from .services.category_display_service import get_category_display_service

            # Clear existing categories for this article
            await db.execute(
                text("DELETE FROM article_categories WHERE article_id = :article_id"),
                {'article_id': article_id}
            )

            category_display_service = await get_category_display_service(db)

            # Use preloaded categories if provided, otherwise load from DB
            if preloaded_categories is not None:
                categories_by_name = preloaded_categories
            else:
                from .models import Category
                result = await db.execute(select(Category))
                categories_by_name = {c.name.lower(): c.id for c in result.scalars().all()}

            other_id = categories_by_name.get('other')

            for cat_data in ai_categories:
                ai_category_name = cat_data.get('name', 'Other')
                confidence = cat_data.get('confidence', 1.0)
                original_category = cat_data.get('ai_category')
                if not isinstance(original_category, str) or not original_category.strip():
                    original_category = ai_category_name

                # Fast path: if AI returned a known fixed category name, use it directly
                category_id = categories_by_name.get(ai_category_name.lower())
                if category_id is None:
                    # Fallback: try mapping service for free-form AI categories
                    display = await category_display_service.map_ai_category_to_display(ai_category_name, commit=False)
                    display_name = (display.get('name') or display.get('display_name') or ai_category_name).lower()
                    category_id = categories_by_name.get(display_name) or other_id

                if category_id is None:
                    raise ValueError('AI category could not be mapped to a configured category')

                await db.execute(
                    text(
                        "INSERT INTO article_categories "
                        "(article_id, category_id, confidence, ai_category, created_at) "
                        "VALUES (:article_id, :category_id, :confidence, :ai_category, now()) "
                        "ON CONFLICT (article_id, category_id) DO NOTHING"
                    ),
                    {
                        'article_id': article_id,
                        'category_id': category_id,
                        'confidence': confidence,
                        'ai_category': original_category.strip(),
                    }
                )

            logger.debug(f"  Saved {len(ai_categories)} AI categories for article {article_id}")
        except Exception as e:
            logger.warning(f"  ⚠️ Error saving AI categories: {e}")
            raise
    # Delegate methods to specialized processors
    async def get_processing_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get processing statistics."""
        return await self.stats_collector.get_processing_stats(days)
    
    async def reprocess_failed_extractions(self, limit: int = 50, dry_run: bool = False,
                                           reservation=None) -> Dict[str, Any]:
        """Retry due bounded work without resetting terminal historical articles."""
        if dry_run:
            async def count_candidates(db):
                query = select(Article.id).where(
                    Article.processing_attempts < self._processing_limits()['max_attempts'],
                    or_(Article.processing_state == 'pending', and_(
                        Article.processing_state == 'retry_wait',
                        Article.next_attempt_at <= datetime.utcnow())),
                ).order_by(Article.id).limit(max(1, limit))
                query = self.article_limiter.apply_date_filters_to_query(query)
                return len((await db.execute(query)).scalars().all())
            count = await self.db_queue_manager.execute_read(count_candidates)
            return {'status': 'dry_run', 'total_candidates': count,
                    'found_candidates': count, 'processed': 0, 'improved': 0, 'errors': []}
        token = reservation or reserve_processing_cycle()
        if token is None:
            return {'status': 'busy', 'processed': 0, 'improved': 0, 'errors': []}
        if token is not _processing_cycle_token:
            raise ValueError('Invalid processing cycle reservation')
        browser_lease = None
        try:
            from .core.browser_pool import acquire_browser_cycle
            browser_lease = await acquire_browser_cycle()
            stats = {'api_calls_made': 0, 'errors': []}
            result = await self._process_unprocessed_articles(stats, max_articles=max(1, limit))
            return {**result, 'processed': result['articles_processed'],
                    'improved': result['articles_summarized'], 'errors': stats['errors']}
        finally:
            try:
                if browser_lease is not None:
                    await _release_browser_lease(browser_lease)
            finally:
                if reservation is None:
                    release_processing_cycle(token)
    
    # Legacy method compatibility (delegate to processors)
    async def _get_summary_by_source_type(self, article: Article, source_type: str, stats: Dict[str, Any]) -> str:
        """Legacy method - delegate to summarization processor."""
        return await self.summarization_processor.get_summary_by_source_type(article, source_type, stats)
    
    async def _categorize_by_source_type_new(self, article: Article, source_type: str, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Legacy method - delegate to categorization processor."""
        return await self.categorization_processor.categorize_by_source_type_new(article, source_type, stats)
    
    async def _create_combined_digest(self, db: AsyncSession, date) -> str:
        """Legacy method - delegate to digest builder."""
        return await self.digest_builder.create_combined_digest(db, date)
