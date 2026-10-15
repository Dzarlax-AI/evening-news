import logging
"""RSS source implementation."""

import feedparser
from datetime import datetime
from typing import AsyncGenerator, Optional
from dateutil import parser as date_parser
import pytz

from .base import BaseSource, Article, SourceType
from ..core.http_client import get_http_client
from ..core.exceptions import SourceError

logger = logging.getLogger(__name__)


class RSSSource(BaseSource):
    """RSS feed source."""
    
    async def fetch_articles(self, limit: Optional[int] = None) -> AsyncGenerator[Article, None]:
        """Fetch articles from RSS feed."""
        try:
            async with get_http_client() as client:
                content = await client.fetch_text(self.url)
            
            # Parse RSS feed
            feed = feedparser.parse(content)
            
            if feed.bozo:
                raise SourceError(f"RSS feed parsing error: {feed.bozo_exception}")
            
            articles_processed = 0
            
            for entry in feed.entries:
                if limit and articles_processed >= limit:
                    break
                
                try:
                    article = self._parse_entry(entry)
                    if article:
                        yield article
                        articles_processed += 1
                except Exception as e:
                    # Log error but continue processing other entries
                    logger.info(f"Error parsing RSS entry: {e}")
                    continue
        
        except Exception as e:
            raise SourceError(f"Failed to fetch RSS feed {self.url}: {e}")
    
    def _parse_entry(self, entry) -> Optional[Article]:
        """Parse RSS entry into Article."""
        try:
            # Extract title
            title = getattr(entry, 'title', 'No title')
            
            # Extract URL
            url = getattr(entry, 'link', '')
            if not url:
                return None
            
            # Extract content/summary
            content = None
            summary = None
            
            if hasattr(entry, 'content') and entry.content:
                content = entry.content[0].value if isinstance(entry.content, list) else entry.content
            elif hasattr(entry, 'description'):
                summary = entry.description
            elif hasattr(entry, 'summary'):
                summary = entry.summary
            
            # Extract image URL — try multiple sources in priority order
            image_url = self._extract_entry_image(entry)
            
            # Parse published date
            published_at = self._parse_date(entry)
            
            return Article(
                title=title,
                url=url,
                content=content,
                summary=summary,
                image_url=image_url,
                published_at=published_at,
                source_type=SourceType.RSS,
                source_name=self.name,
                raw_data={
                    'guid': getattr(entry, 'id', url),
                    'author': getattr(entry, 'author', ''),
                    'tags': [tag.term for tag in getattr(entry, 'tags', [])]
                }
            )
        
        except Exception as e:
            raise SourceError(f"Error parsing RSS entry: {e}")
    
    def _extract_entry_image(self, entry) -> Optional[str]:
        """Extract image URL from RSS entry, trying multiple common locations."""
        # 1. <enclosure type="image/...">
        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enclosure in entry.enclosures:
                if hasattr(enclosure, 'type') and enclosure.type.startswith('image/'):
                    return enclosure.href

        # 2. <media:content medium="image"> or <media:content url="...">
        if hasattr(entry, 'media_content') and entry.media_content:
            for media in entry.media_content:
                url = media.get('url')
                if url:
                    medium = media.get('medium', '')
                    mime = media.get('type', '')
                    if medium == 'image' or mime.startswith('image/') or self._looks_like_image_url(url):
                        return url

        # 3. <media:thumbnail url="...">
        if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
            url = entry.media_thumbnail[0].get('url') if entry.media_thumbnail else None
            if url:
                return url

        # 4. First <img> from the HTML content or description
        html_blob = None
        if hasattr(entry, 'content') and entry.content:
            blob = entry.content[0].value if isinstance(entry.content, list) else entry.content
            if blob:
                html_blob = blob
        if not html_blob and hasattr(entry, 'description') and entry.description:
            html_blob = entry.description
        if not html_blob and hasattr(entry, 'summary') and entry.summary:
            html_blob = entry.summary

        if html_blob and '<img' in html_blob:
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html_blob, 'html.parser')
                img = soup.find('img', src=True)
                if img:
                    src = img['src']
                    if src.startswith(('http://', 'https://')) and self._looks_like_image_url(src):
                        return src
            except Exception:
                pass

        return None

    def _looks_like_image_url(self, url: str) -> bool:
        """Quick heuristic: does the URL point to an image?"""
        if not url:
            return False
        lower = url.lower().split('?')[0]
        return any(lower.endswith(ext) for ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'))

    def _parse_date(self, entry) -> Optional[datetime]:
        """Parse date from RSS entry."""
        # Try different date fields
        date_fields = ['published', 'updated', 'created']
        
        for field in date_fields:
            if hasattr(entry, field):
                date_str = getattr(entry, field)
                if date_str:
                    try:
                        # Try parsing with dateutil
                        dt = date_parser.parse(date_str)
                        # Convert to UTC and make naive for database
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(pytz.UTC).replace(tzinfo=None)
                        return dt
                    except (ValueError, TypeError):
                        continue
        
        # Try structured time fields
        struct_fields = ['published_parsed', 'updated_parsed']
        for field in struct_fields:
            if hasattr(entry, field):
                time_struct = getattr(entry, field)
                if time_struct:
                    try:
                        dt = datetime(*time_struct[:6])
                        return dt  # Already naive
                    except (ValueError, TypeError):
                        continue
        
        # Return current time as fallback (naive)
        return datetime.utcnow()
    
    async def test_connection(self) -> bool:
        """Test RSS feed connectivity."""
        try:
            async with get_http_client() as client:
                response = await client.get(self.url, auto_decompress=False)
                async with response:
                    if response.status == 200:
                        from ..core.resource_limits import read_page_text
                        content = await read_page_text(response)
                        feed = feedparser.parse(content)
                        return not feed.bozo or len(feed.entries) > 0
                    return False
        except Exception:
            return False
