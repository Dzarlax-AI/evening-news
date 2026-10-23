"""Refresh one Telegram post without running source collection or a browser."""

import re
from urllib.parse import urlsplit

import aiohttp
from bs4 import BeautifulSoup

from sqlalchemy import select, update

from ..database import AsyncSessionLocal
from ..models import Article, Source
from ..core.resource_limits import read_page_text
from ..telegram.media_extractor import MediaExtractor


def _post_target(article_url: str, source_url: str):
    """Allow only an exact public Telegram post belonging to this source."""
    def parts(raw):
        try:
            value = urlsplit(raw)
            port = value.port
        except (TypeError, ValueError):
            return None
        if (value.scheme != "https" or value.hostname not in {"t.me", "telegram.me"}
                or value.username or value.password or port not in {None, 443}
                or value.query or value.fragment or "?" in raw or "#" in raw):
            return None
        path = value.path.strip("/").split("/")
        if path and path[0] == "s":
            path = path[1:]
        return path
    post, source = parts(article_url), parts(source_url)
    if (not post or not source or len(post) != 2 or len(source) != 1
            or not re.fullmatch(r"[A-Za-z0-9_]{1,64}", post[0])
            or post[0].lower() != source[0].lower()
            or not re.fullmatch(r"[1-9][0-9]{0,19}", post[1])):
        return None
    return f"https://t.me/s/{post[0]}/{post[1]}", f"{post[0]}/{post[1]}"


async def _fetch_post_media(article_url: str, source_url: str):
    """Keep historical-post media refresh until Go exposes a post-specific API."""
    target = _post_target(article_url, source_url)
    if target is None:
        return None
    url, post_id = target
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as client:
        async with client.get(url, headers={"User-Agent": "Mozilla/5.0"},
                              auto_decompress=False, allow_redirects=False) as response:
            if response.status != 200:
                return None
            html = await read_page_text(response)
    soup = BeautifulSoup(html, "html.parser")
    message = soup.find(attrs={"data-post": post_id})
    if message is None:
        return None
    return MediaExtractor().extract_media_files(message) or []


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
        source_url = source.url
        source_id = source.id
        await session.commit()

    media = await _fetch_post_media(article_url, source_url)
    if media is None:
        return False
    image_url = next((item.get("url") for item in media
                      if isinstance(item, dict) and item.get("type") == "image"), None)
    async with AsyncSessionLocal() as session:
        current = await session.scalar(select(Article).where(
            Article.id == article_id, Article.source_id == source_id, Article.url == article_url))
        if current is None:
            return False
        source = await session.get(Source, source_id)
        if source is None or source.source_type != "telegram" or source.url != source_url:
            return False
        await session.execute(update(Article).where(Article.id == article_id).values(
            media_files=media, image_url=image_url
        ))
        await session.commit()
        return True
