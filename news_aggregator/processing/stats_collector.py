import logging
"""Statistics collector for processing and monitoring."""

from typing import Dict, Any, List
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Article

logger = logging.getLogger(__name__)


class StatsCollector:
    """Handles collection and analysis of processing statistics."""
    
    async def get_processing_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get processing statistics for the last N days."""
        from ..processing.processing_stats_service import get_processing_stats_service
        processing_stats_service = get_processing_stats_service()
        return await processing_stats_service.get_processing_stats(days)
    
    async def get_content_extraction_stats(self) -> Dict[str, Any]:
        """Get statistics about content extraction quality."""
        from ..services.database_queue import get_db_queue_manager

        async def _get_stats(db):
            # Articles with very short content (potential extraction failures)
            short_content_result = await db.execute(
                select(func.count(Article.id)).where(
                    func.length(func.coalesce(Article.content, '')) < 200
                )
            )
            short_content_count = short_content_result.scalar() or 0

            # Articles where title equals summary (likely extraction failures)
            failed_extraction_result = await db.execute(
                select(func.count(Article.id)).where(
                    Article.title == Article.summary
                )
            )
            failed_extraction_count = failed_extraction_result.scalar() or 0

            # Total articles
            total_result = await db.execute(select(func.count(Article.id)))
            total_articles = total_result.scalar() or 0

            # Success rates
            good_extraction_rate = ((total_articles - failed_extraction_count) / total_articles * 100) if total_articles > 0 else 0
            content_quality_rate = ((total_articles - short_content_count) / total_articles * 100) if total_articles > 0 else 0

            return {
                'total_articles': total_articles,
                'short_content_count': short_content_count,
                'failed_extraction_count': failed_extraction_count,
                'good_extraction_rate': round(good_extraction_rate, 2),
                'content_quality_rate': round(content_quality_rate, 2),
                'extraction_success_count': total_articles - failed_extraction_count,
                'quality_content_count': total_articles - short_content_count
            }

        return await get_db_queue_manager().execute_read(_get_stats)
    
    async def get_source_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics by source type."""
        from ..services.database_queue import get_db_queue_manager

        async def _get_perf_stats(db):
            # Articles by source type
            source_stats_result = await db.execute(
                text("""
                    SELECT
                        s.source_type,
                        COUNT(a.id) as article_count,
                        AVG(LENGTH(COALESCE(a.content, ''))) as avg_content_length,
                        COUNT(CASE WHEN a.title = a.summary THEN 1 END) as failed_extractions
                    FROM articles a
                    JOIN sources s ON a.source_id = s.id
                    GROUP BY s.source_type
                    ORDER BY article_count DESC
                """)
            )

            source_stats = []
            for row in source_stats_result.fetchall():
                source_type, count, avg_length, failed = row
                success_rate = ((count - failed) / count * 100) if count > 0 else 0

                source_stats.append({
                    'source_type': source_type,
                    'article_count': count,
                    'avg_content_length': round(avg_length or 0, 2),
                    'failed_extractions': failed,
                    'success_rate': round(success_rate, 2)
                })

            return {
                'source_stats': source_stats,
                'total_sources': len(source_stats)
            }

        return await get_db_queue_manager().execute_read(_get_perf_stats)
