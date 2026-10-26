"""Refactored main orchestrator for Evening News v2."""

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text

from .models import Article
from .processing.digest_builder import DigestBuilder
from .processing.stats_collector import StatsCollector
from .services.telegram_service import get_telegram_service, TelegramService
from .services.database_queue import get_database_queue, DatabaseQueueManager
from .config import settings

logger = logging.getLogger(__name__)

# All scheduler and manual entry points share this process-local reservation.
# Deploy one worker process; this is deliberately not a distributed lease.
_processing_cycle_token = None
_SELECTED_PROCESSING_RECEIPT_FIELDS = frozenset({
    "success", "settled", "retryable", "article_outcomes",
    "article_outcomes_total", "article_outcomes_omitted",
})


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


class NewsOrchestrator:
    """Main orchestrator for news processing - refactored with modular processors."""
    
    def __init__(self):
        self.telegram_service = get_telegram_service()
        self.db_queue_manager = get_database_queue()
        self.digest_builder = DigestBuilder()
        self.stats_collector = StatsCollector()
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
            logger.info("  🌐 Collecting and saving articles through private Go ingestion")
            from .services.article_ingestion_client import ArticleIngestionClient
            ingestion_result = await ArticleIngestionClient().collect_and_save()
            if ingestion_result.get("fatal_error"):
                raise RuntimeError("Go article ingestion failed")
            stats.update({
                "sources_synced": ingestion_result.get("sources_synced", 0),
                "articles_fetched": ingestion_result.get("articles_fetched", 0),
                "errors": ingestion_result.get("errors", []),
                "errors_count": ingestion_result.get("errors_count", 0),
            })
            sync_duration = time.time() - sync_start

            stats['performance']['sync_duration'] = sync_duration
            logger.info(f"  ✅ Total sync: {stats['sources_synced']} sources, {stats['articles_fetched']} articles in {sync_duration:.1f}s")
            # Step 2: Go owns article selection, extraction, retries and final writes.
            logger.info("🤖 Step 2: Processing articles through private Go processor...")
            process_start = time.time()
            from .services.article_processing_client import ArticleProcessingClient
            processing_result = await ArticleProcessingClient().process_current_job()
            processing_fields = {
                key: value for key, value in processing_result.items()
                if key not in _SELECTED_PROCESSING_RECEIPT_FIELDS
            }
            processing_status = processing_fields.pop("status", None)
            if processing_status in {"success", "warning"}:
                # Keep the normal processor's status vocabulary. A selected
                # receipt (settled/incomplete/retry_wait/etc.) is filtered out.
                if stats.get("status") != "warning" or processing_status == "warning":
                    stats["status"] = processing_status
            processing_errors = processing_fields.pop("errors_count", None)
            if (isinstance(processing_errors, int) and not isinstance(processing_errors, bool)
                    and processing_errors >= 0):
                ingestion_errors = stats.get("errors_count", 0)
                if not isinstance(ingestion_errors, int) or isinstance(ingestion_errors, bool):
                    ingestion_errors = 0
                stats["errors_count"] = min(1_000_000_000, ingestion_errors + processing_errors)
            # The public item receipt belongs to selected reprocess jobs. A full
            # cycle keeps its aggregate stats and lets ingestion/source errors
            # drive the existing scheduler warning policy.
            stats.update(processing_fields)
            
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
    
    async def process_selected_articles(self, article_ids, reservation=None, force=False, reset=True,
                                        preserve_completed_stages=False):
        """Delegate selected-article processing to the active fenced Go job."""
        token = reservation or reserve_processing_cycle()
        if token is None:
            return {'status': 'busy', 'success': False}
        if token is not _processing_cycle_token:
            raise ValueError('Invalid processing cycle reservation')
        try:
            from .services.article_processing_client import ArticleProcessingClient
            return await ArticleProcessingClient().process_current_job()
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

    # Delegate methods to specialized processors
    async def get_processing_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get processing statistics."""
        return await self.stats_collector.get_processing_stats(days)
    
    async def reprocess_failed_extractions(self, limit: int = 50, dry_run: bool = False,
                                           reservation=None) -> Dict[str, Any]:
        """Delegate due retry selection and dry-run counts to Go."""
        token = reservation or reserve_processing_cycle()
        if token is None:
            return {'status': 'busy', 'processed': 0, 'improved': 0, 'errors': []}
        if token is not _processing_cycle_token:
            raise ValueError('Invalid processing cycle reservation')
        try:
            from .services.article_processing_client import ArticleProcessingClient
            return await ArticleProcessingClient().process_current_job()
        finally:
            if reservation is None:
                release_processing_cycle(token)

    async def _create_combined_digest(self, db: AsyncSession, date) -> str:
        """Legacy method - delegate to digest builder."""
        return await self.digest_builder.create_combined_digest(db, date)
