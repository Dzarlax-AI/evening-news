"""Digest builder for creating combined daily news digests."""

import datetime
import html
import re
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models import DailySummary
from ..config import get_settings
from ..utils import TelegramMessageSplitter, get_logger, log_operation
from ..services.summary_generator import SummaryGenerator

logger = get_logger(__name__, 'DIGEST_BUILDER')


class DigestBuilder:
    """Handles building daily news digests from category summaries."""

    def __init__(self):
        """Initialize digest builder with configuration and services."""
        self.settings = get_settings()
        # Initialize Telegram message splitter with proper limits
        # TelegramMessageSplitter has max_length=4096, we calculate safety_margin to get effective_limit=3600
        safety_margin = 4096 - self.settings.digest_telegram_limit
        self.splitter = TelegramMessageSplitter(safety_margin=safety_margin)
        self.summary_generator = SummaryGenerator()

    def _valid_summaries(self, summaries: List[DailySummary]) -> List[DailySummary]:
        return [
            s for s in summaries
            if s.summary_text and len(s.summary_text.strip()) >= self.settings.digest_min_summary_length
        ]

    def _summary_to_rich_blocks(self, summary_text: str) -> str:
        """Render generated summary text as safe Rich Message paragraphs/list items."""
        lines = [line.strip() for line in summary_text.strip().splitlines() if line.strip()]
        if not lines:
            return ""

        rendered: List[str] = []
        list_items: List[str] = []

        def flush_list() -> None:
            nonlocal list_items
            if list_items:
                rendered.append("<ul>" + "".join(list_items) + "</ul>")
                list_items = []

        for line in lines:
            normalized = re.sub(r"^[-*•]\s+", "", line).strip()
            if normalized != line:
                list_items.append(f"<li>{html.escape(normalized)}</li>")
                continue

            flush_list()
            rendered.append(f"<p>{html.escape(line)}</p>")

        flush_list()
        return "\n".join(rendered)

    async def create_rich_digest(
        self,
        db: AsyncSession,
        date: datetime.date,
        full_url: str
    ) -> str:
        """Create a Telegram Rich Message digest from ready category summaries."""
        log_operation(
            logger,
            'create_rich_digest',
            'started',
            date=date.strftime('%Y-%m-%d')
        )

        try:
            result = await db.execute(
                select(DailySummary)
                .where(DailySummary.date == date)
                .order_by(DailySummary.articles_count.desc())
            )
            valid_summaries = self._valid_summaries(result.scalars().all())

            if not valid_summaries:
                logger.warning(f"No valid summaries found for {date}")
                return ""

            total_articles = sum(s.articles_count for s in valid_summaries)
            categories_count = len(valid_summaries)
            date_label = date.strftime('%d.%m.%Y')

            blocks = [
                f"<h2>Сводка новостей за {html.escape(date_label)}</h2>",
                (
                    f"<p><strong>Всего:</strong> {total_articles} новостей "
                    f"в {categories_count} категориях.</p>"
                ),
            ]

            if full_url:
                blocks.append(
                    f'<p><a href="{html.escape(full_url, quote=True)}">Открыть полную версию на сайте</a></p>'
                )

            for summary in valid_summaries:
                category = html.escape(summary.category or "Other")
                blocks.extend([
                    "<hr>",
                    f"<h3>{category} <em>({summary.articles_count})</em></h3>",
                    self._summary_to_rich_blocks(summary.summary_text),
                ])

            log_operation(
                logger,
                'create_rich_digest',
                'completed',
                categories=categories_count,
                articles=total_articles
            )
            return "\n".join(block for block in blocks if block)

        except Exception as e:
            log_operation(
                logger,
                'create_rich_digest',
                'failed',
                error=str(e),
                date=date.strftime('%Y-%m-%d')
            )
            return ""

    async def create_combined_digest(
        self,
        db: AsyncSession,
        date: datetime.date
    ) -> str:
        """
        Create digest by combining ready category summaries (no AI needed).

        Args:
            db: Database session
            date: Date for the digest

        Returns:
            Combined digest text or "SPLIT_NEEDED" marker
        """
        log_operation(
            logger,
            'create_combined_digest',
            'started',
            date=date.strftime('%Y-%m-%d')
        )

        try:
            # Get all category summaries for today
            result = await db.execute(
                select(DailySummary)
                .where(DailySummary.date == date)
                .order_by(DailySummary.articles_count.desc())
            )
            summaries = result.scalars().all()

            # Filter out empty summaries
            valid_summaries = self._valid_summaries(summaries)

            if not valid_summaries:
                logger.warning(f"No valid summaries found for {date}")
                return "Сводки новостей пока не готовы."

            # Calculate statistics
            total_articles = sum(s.articles_count for s in valid_summaries)
            categories_count = len(valid_summaries)

            log_operation(
                logger,
                'create_combined_digest',
                'processing',
                categories=categories_count,
                articles=total_articles
            )

            # Build components for message splitter
            header = f"<b>Сводка новостей за {date.strftime('%d.%m.%Y')}</b>"
            footer = f"\n📊 Всего: {total_articles} новостей в {categories_count} категориях"

            content_blocks = [
                (summary.category, summary.summary_text.strip())
                for summary in valid_summaries
            ]

            metadata = {
                "total_articles": total_articles,
                "categories_count": categories_count
            }

            # Use message splitter to check if splitting needed
            split_result = self.splitter.split_digest(
                header=header,
                content_blocks=content_blocks,
                footer=footer,
                metadata=metadata
            )

            if not split_result.was_split:
                log_operation(
                    logger,
                    'create_combined_digest',
                    'completed',
                    single_message=True,
                    length=split_result.original_length
                )
                return split_result.parts[0].content
            else:
                log_operation(
                    logger,
                    'create_combined_digest',
                    'splitting_needed',
                    total_parts=split_result.total_parts,
                    length=split_result.original_length
                )
                return "SPLIT_NEEDED"

        except Exception as e:
            log_operation(
                logger,
                'create_combined_digest',
                'failed',
                error=str(e),
                date=date.strftime('%Y-%m-%d')
            )
            return f"<b>Сводка новостей за {date.strftime('%d.%m.%Y')}</b>\n\nОшибка при создании сводки."

    async def create_digest_parts(
        self,
        db: AsyncSession,
        date: datetime.date
    ) -> List[str]:
        """
        Create and split digest into multiple parts.

        Args:
            db: Database session
            date: Date for the digest

        Returns:
            List of message parts ready for sending
        """
        log_operation(
            logger,
            'create_digest_parts',
            'started',
            date=date.strftime('%Y-%m-%d')
        )

        try:
            # Get all category summaries for today
            result = await db.execute(
                select(DailySummary)
                .where(DailySummary.date == date)
                .order_by(DailySummary.articles_count.desc())
            )
            summaries = result.scalars().all()

            # Filter out empty summaries
            valid_summaries = self._valid_summaries(summaries)

            if not valid_summaries:
                logger.warning(f"No valid summaries found for {date}")
                return ["Сводки новостей пока не готовы."]

            # Calculate statistics
            total_articles = sum(s.articles_count for s in valid_summaries)
            categories_count = len(valid_summaries)

            # Build components for message splitter
            header = f"<b>Сводка новостей за {date.strftime('%d.%m.%Y')}</b>"
            footer = f"\n📊 Всего: {total_articles} новостей в {categories_count} категориях"

            content_blocks = [
                (summary.category, summary.summary_text.strip())
                for summary in valid_summaries
            ]

            metadata = {
                "total_articles": total_articles,
                "categories_count": categories_count
            }

            # Split digest into parts
            split_result = self.splitter.split_digest(
                header=header,
                content_blocks=content_blocks,
                footer=footer,
                metadata=metadata
            )

            # Extract message contents
            message_parts = [part.content for part in split_result.parts]

            log_operation(
                logger,
                'create_digest_parts',
                'completed',
                parts=len(message_parts),
                total_length=split_result.original_length
            )

            return message_parts

        except Exception as e:
            log_operation(
                logger,
                'create_digest_parts',
                'failed',
                error=str(e),
                date=date.strftime('%Y-%m-%d')
            )
            return [f"<b>Сводка новостей за {date.strftime('%d.%m.%Y')}</b>\n\nОшибка при создании сводки."]

    async def generate_and_save_daily_summaries(
        self,
        db: AsyncSession,
        date: datetime.date,
        categories: Dict[str, List]
    ) -> Dict[str, int]:
        """
        Generate and save daily summaries by category.

        This method delegates to the SummaryGenerator service which handles
        both sequential and parallel processing based on configuration.

        Args:
            db: Database session
            date: Date for the summaries
            categories: Dictionary mapping category names to article lists

        Returns:
            Statistics dictionary about generated summaries
        """
        log_operation(
            logger,
            'generate_and_save_daily_summaries',
            'started',
            date=date.strftime('%Y-%m-%d'),
            categories=len(categories)
        )

        try:
            stats = await self.summary_generator.generate_and_save_summaries(
                db=db,
                date=date,
                categories=categories
            )

            log_operation(
                logger,
                'generate_and_save_daily_summaries',
                'completed',
                successful=stats.get('successful', 0),
                failed=stats.get('failed', 0),
                fallback=stats.get('fallback_used', 0)
            )

            return stats

        except Exception as e:
            log_operation(
                logger,
                'generate_and_save_daily_summaries',
                'failed',
                error=str(e),
                date=date.strftime('%Y-%m-%d')
            )
            raise

    async def validate_digest_quality(
        self,
        db: AsyncSession,
        date: datetime.date
    ) -> Dict[str, Any]:
        """
        Validate the quality of generated digest for a date.

        Args:
            db: Database session
            date: Date to validate

        Returns:
            Dictionary with validation results
        """
        result = await db.execute(
            select(DailySummary)
            .where(DailySummary.date == date)
        )
        summaries = result.scalars().all()

        validation = {
            "date": date.strftime('%Y-%m-%d'),
            "total_categories": len(summaries),
            "valid_summaries": 0,
            "empty_summaries": 0,
            "short_summaries": 0,
            "total_articles": 0,
            "is_valid": False
        }

        for summary in summaries:
            validation["total_articles"] += summary.articles_count

            if not summary.summary_text or len(summary.summary_text.strip()) < 10:
                validation["empty_summaries"] += 1
            elif len(summary.summary_text.strip()) < self.settings.digest_min_summary_length:
                validation["short_summaries"] += 1
            else:
                validation["valid_summaries"] += 1

        # Consider digest valid if we have at least one good summary
        validation["is_valid"] = validation["valid_summaries"] > 0

        return validation

    async def get_digest_statistics(
        self,
        db: AsyncSession,
        date: datetime.date
    ) -> Dict[str, Any]:
        """
        Get statistics about digest for a specific date.

        Args:
            db: Database session
            date: Date to get statistics for

        Returns:
            Dictionary with digest statistics
        """
        result = await db.execute(
            select(DailySummary)
            .where(DailySummary.date == date)
            .order_by(DailySummary.articles_count.desc())
        )
        summaries = result.scalars().all()

        if not summaries:
            return {
                "date": date.strftime('%Y-%m-%d'),
                "categories": 0,
                "total_articles": 0,
                "avg_summary_length": 0,
                "longest_summary": 0,
                "shortest_summary": 0
            }

        total_articles = sum(s.articles_count for s in summaries)
        summary_lengths = [len(s.summary_text) for s in summaries if s.summary_text]

        return {
            "date": date.strftime('%Y-%m-%d'),
            "categories": len(summaries),
            "total_articles": total_articles,
            "avg_summary_length": sum(summary_lengths) / len(summary_lengths) if summary_lengths else 0,
            "longest_summary": max(summary_lengths) if summary_lengths else 0,
            "shortest_summary": min(summary_lengths) if summary_lengths else 0,
            "categories_by_name": {s.category: s.articles_count for s in summaries}
        }
