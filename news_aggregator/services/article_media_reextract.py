"""Refresh Telegram media from the authoritative Go collector response."""

from types import SimpleNamespace

from sqlalchemy import select, update

from ..database import AsyncSessionLocal
from ..models import Article, Source


async def reextract_article_media(article_id: int) -> bool:
    """Refresh stored Telegram media without scraping or parsing channel HTML."""
    async with AsyncSessionLocal() as session:
        article = await session.scalar(select(Article).where(Article.id == article_id))
        if article is None:
            raise ValueError("article no longer exists")
        source = await session.get(Source, article.source_id)
        if source is None or source.source_type != "telegram":
            return False
        article_url = article.url
        source_snapshot = SimpleNamespace(
            id=source.id, name=source.name, source_type=source.source_type,
            url=source.url, enabled=source.enabled, config=source.config,
        )
        source_id = source.id
        await session.commit()

    source_manager = __import__(
        "news_aggregator.services.source_manager", fromlist=["SourceManager"]
    ).SourceManager()
    result = await source_manager._probe_source_result(source_snapshot)
    if result is None:
        return False
    telegram = result.get("telegram") or {}
    captured = next((item for item in telegram.get("articles", [])
                    if item.get("url") == article_url), None)
    if captured is None:
        return False
    media = captured.get("media_files") or []
    image_url = captured.get("image_url") or next(
        (item.get("url") for item in media
         if isinstance(item, dict) and item.get("type") == "image"), None)
    async with AsyncSessionLocal() as session:
        current = await session.scalar(select(Article).where(
            Article.id == article_id, Article.source_id == source_id, Article.url == article_url))
        if current is None:
            return False
        source = await session.get(Source, source_id)
        if source is None or source.source_type != "telegram":
            return False
        await session.execute(update(Article).where(Article.id == article_id).values(
            media_files=media, image_url=image_url
        ))
        await session.commit()
        return True
