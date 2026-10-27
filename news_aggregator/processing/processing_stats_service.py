import logging
"""Processing Statistics Service for news aggregation."""

from datetime import datetime, timedelta
from typing import Dict, Any, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models import ProcessingStat
from ..database_helpers import fetch_all

logger = logging.getLogger(__name__)


class ProcessingStatsService:
    """Service for managing processing statistics."""
    
    def __init__(self):
        """Initialize processing stats service."""
        pass
    
    async def update_processing_stats(self, db: AsyncSession, stats: Dict[str, Any]):
        """Update processing statistics in database."""
        today = datetime.utcnow().date()
        errors_count = stats.get('errors_count')
        if not isinstance(errors_count, int) or isinstance(errors_count, bool) or errors_count < 0:
            errors_count = len(stats.get('errors', []))
        
        # Get or create today's stats
        existing_stats = await db.execute(
            select(ProcessingStat).where(ProcessingStat.date == today)
        )
        existing_stats = existing_stats.scalar_one_or_none()
        
        if existing_stats:
            # Update existing stats
            existing_stats.articles_fetched += stats.get('articles_fetched', 0)
            existing_stats.articles_processed += stats.get('articles_processed', 0)
            existing_stats.api_calls_made += stats.get('api_calls_made', 0)
            existing_stats.errors_count += errors_count
            existing_stats.processing_time_seconds += int(stats.get('duration_seconds', 0))
            logger.info(f"  📊 Updated daily stats: {existing_stats.articles_processed} processed, {existing_stats.api_calls_made} API calls")
        else:
            # Create new stats
            processing_stat = ProcessingStat(
                date=today,
                articles_fetched=stats.get('articles_fetched', 0),
                articles_processed=stats.get('articles_processed', 0),
                api_calls_made=stats.get('api_calls_made', 0),
                errors_count=errors_count,
                processing_time_seconds=int(stats.get('duration_seconds', 0))
            )
            db.add(processing_stat)
            logger.info(f"  📊 Created new daily stats: {processing_stat.articles_processed} processed, {processing_stat.api_calls_made} API calls")
        await db.commit()
    
    async def get_processing_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get processing statistics for the last N days."""
        since_date = datetime.utcnow().date() - timedelta(days=days)
        
        query = select(ProcessingStat).where(
            ProcessingStat.date >= since_date
        ).order_by(ProcessingStat.date.desc())
        
        stats = await fetch_all(query)
        
        return {
            'daily_stats': [
                {
                    'date': stat.date.isoformat(),
                    'articles_fetched': stat.articles_fetched,
                    'articles_processed': stat.articles_processed,
                    'api_calls_made': stat.api_calls_made,
                    'errors_count': stat.errors_count,
                    'processing_time_seconds': stat.processing_time_seconds
                }
                for stat in stats
            ],
            'totals': {
                'articles_fetched': sum(s.articles_fetched for s in stats),
                'articles_processed': sum(s.articles_processed for s in stats),
                'api_calls_made': sum(s.api_calls_made for s in stats),
                'errors_count': sum(s.errors_count for s in stats),
                'total_processing_time': sum(s.processing_time_seconds for s in stats)
            }
        }


# Factory function
def get_processing_stats_service() -> ProcessingStatsService:
    """Get processing stats service instance."""
    return ProcessingStatsService()
