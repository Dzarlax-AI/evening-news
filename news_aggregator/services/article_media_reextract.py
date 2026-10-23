"""Shared worker/API action for refreshing Telegram article media."""

import random

from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from ..database import AsyncSessionLocal
from ..models import Article
from .database_queue import get_database_queue


async def reextract_article_media(article_id: int) -> bool:
    """Refresh Telegram media using the same source parser used by admin reprocess."""
    async with AsyncSessionLocal() as session:
        article = await session.scalar(select(Article).options(
            selectinload(Article.source)
        ).where(Article.id == article_id))
        if article is None:
            raise ValueError("article no longer exists")
        source = article.source
        url = article.url
        if source is None or source.source_type != "telegram":
            return False
        source_manager = __import__(
            "news_aggregator.services.source_manager", fromlist=["SourceManager"]
        ).SourceManager()
        source_instance = await source_manager.get_source_instance(source)

    import aiohttp
    from bs4 import BeautifulSoup
    from ..core.resource_limits import read_page_text

    headers = random.choice(source_instance.BROWSER_HEADERS) if getattr(
        source_instance, "BROWSER_HEADERS", None
    ) else {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession() as client:
        async with client.get(url, headers=headers, timeout=30, auto_decompress=False) as response:
            if response.status != 200:
                raise RuntimeError("Telegram media page request failed")
            html = await read_page_text(response)
    soup = BeautifulSoup(html, "html.parser")
    message_div = (soup.find("div", class_=["tgme_widget_message", "message", "post"])
                   or soup.find("div", {"data-post": True}) or soup.find("body"))
    if message_div is None or not getattr(source_instance, "media_extractor", None):
        return False
    media = source_instance.media_extractor.extract_media_files(message_div) or []
    image_url = next((item.get("url") for item in media
                      if isinstance(item, dict) and item.get("type") == "image"), None)

    async def save(session):
        await session.execute(update(Article).where(Article.id == article_id).values(
            media_files=media, image_url=image_url
        ))

    await get_database_queue().execute_write(save)
    return True
