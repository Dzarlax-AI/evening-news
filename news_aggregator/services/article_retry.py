"""Short database-only selected retry reset shared by web and worker actions."""

from sqlalchemy import select, text, update

from ..models import Article, BackgroundJob
from .database_queue import get_database_queue


async def reset_selected_articles(article_ids, *, force=False):
    ids = sorted(set(article_ids))
    if not ids or len(ids) > 100 or any(type(value) is not int or value < 1 for value in ids):
        raise ValueError("Select between 1 and 100 positive article IDs")

    async def reset(session):
        await session.execute(text("""
            SELECT pg_advisory_xact_lock(hashtext(current_database()),
                                         hashtext(current_schema()) # 1145132611)
        """))
        active = await session.scalar(select(BackgroundJob.id).where(
            BackgroundJob.state == "running",
            BackgroundJob.kind.in_(("news_processing", "news_digest", "reprocess_failed", "article_reprocess")),
        ).limit(1))
        if active is not None:
            raise RuntimeError("News processing is already running")
        fields = dict(processing_state="pending", processing_attempts=0,
                      next_attempt_at=None, last_processing_error=None, processed=False)
        if force:
            fields.update(summary_processed=False, category_processed=False, ad_processed=False)
        result = await session.execute(update(Article).where(Article.id.in_(ids)).values(**fields))
        return result.rowcount

    return await get_database_queue().execute_write(reset)
