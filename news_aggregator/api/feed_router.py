"""Feed API router - handles news feed endpoints."""

import html
from datetime import datetime, timedelta
from typing import Optional
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from sqlalchemy.orm import selectinload

from ..database import get_db
from ..models import Article, ArticleCategory, Category
from ..processing.processing_outcome import publication_filter


router = APIRouter()


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower()
    except Exception:
        return "unknown"


def clean_html_entities(text: str) -> str:
    """Clean HTML entities from text, preventing double escaping issues."""
    if not text:
        return text
    
    # First, unescape any existing HTML entities
    cleaned = html.unescape(text)
    
    # Clean up any remaining problematic sequences that might cause display issues
    import re
    
    # Remove sequences like "> or '> that might appear from malformed entities
    cleaned = re.sub(r'["\']>', ' ', cleaned)
    
    # Remove problematic HTML sequences manually instead of using BeautifulSoup
    # This is more reliable for malformed HTML
    import re
    
    # Remove malformed HTML links like "a href='url text /a"
    cleaned = re.sub(r'<?\s*a\s+href=[\'"]?[^\s\'"<>]*[\'"]?\s*[^<>]*/?a\s*>?', ' ', cleaned)
    
    # Remove any remaining HTML tags
    cleaned = re.sub(r'<[^>]*>', '', cleaned)
    
    # Remove patterns like "@serbia /url_path Читать оригинал"
    cleaned = re.sub(r'@\w+\s+/[^\s]*\s+Читать оригинал', '', cleaned)
    
    # Remove any remaining < > characters
    cleaned = cleaned.replace('<', ' ').replace('>', ' ')
    
    # Clean up extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned


def clean_summary_text(raw_summary: str) -> str:
    """Clean AI-generated summary from service text and prompt repetition."""
    if not raw_summary:
        return ""
    
    import re
    
    # Remove common service phrases that AI sometimes includes
    service_phrases = [
        r'^Краткое содержание статьи на русском языке с основными тезисами:\s*',
        r'^Краткое содержание статьи на русском языке:\s*',
        r'^Краткое содержание:\s*',
        r'^Основные тезисы статьи:\s*',
        r'^Основные тезисы:\s*',
        r'^Суммаризация статьи:\s*',
        r'^Пересказ статьи:\s*',
        r'^Содержание статьи:\s*',
        r'^Вот краткое содержание:\s*',
        r'^Вот основные тезисы:\s*',
        r'^Статья содержит следующие основные моменты:\s*',
        r'^Статья рассказывает о следующем:\s*',
        r'^ПЕРЕСКАЗ:\s*'
    ]
    
    cleaned_summary = raw_summary
    
    # Remove service phrases from the beginning
    for phrase_pattern in service_phrases:
        cleaned_summary = re.sub(phrase_pattern, '', cleaned_summary, flags=re.IGNORECASE | re.MULTILINE)
    
    # Remove leading/trailing whitespace and newlines
    cleaned_summary = cleaned_summary.strip()
    
    # Remove empty bullet points or dashes at the beginning
    cleaned_summary = re.sub(r'^[-•·*]\s*', '', cleaned_summary, flags=re.MULTILINE)
    cleaned_summary = re.sub(r'^\d+\.\s*$', '', cleaned_summary, flags=re.MULTILINE)
    
    # Clean up multiple newlines
    cleaned_summary = re.sub(r'\n\s*\n', '\n\n', cleaned_summary)
    
    # Remove trailing periods/colons if they look like service text endings
    cleaned_summary = re.sub(r':\s*$', '', cleaned_summary)
    
    return cleaned_summary.strip()


@router.get("/")
async def api_root():
    """API root endpoint with available endpoints."""
    return {
        "message": "Evening News v2 API",
        "version": "2.0.0",
        "endpoints": {
            "feed": "/api/v1/feed - Get news feed",
            "rss": "/api/v1/feed.rss - RSS feed",
            "categories": "/api/v1/categories - Get categories",
            "sources": "/api/v1/sources - Get sources",
            "summaries": "/api/v1/summaries/daily - Daily summaries",
            "generate_summaries": "/api/v1/summaries/generate - Generate daily summaries",
            "process": "/api/v1/process - Trigger processing",
            "telegram": "/api/v1/telegram/send-digest - Send Telegram digest",
            "backup": "/api/v1/backup - Backup management",
            "restore": "/api/v1/restore - Restore from backup",
            "system": {
                "process_monitor": "/api/v1/system/process-monitor - Process monitor status",
                "cleanup": "/api/v1/system/process-monitor/cleanup - Manual process cleanup"
            }
        },
        "documentation": "/docs",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/feed")
async def get_main_feed(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    since_hours: Optional[int] = Query(None, ge=1, le=168),  # Max 1 week
    category: Optional[str] = Query(None, description="Filter by category"),
    db: AsyncSession = Depends(get_db)
):
    """Get main news feed in JSON format - optimized for performance."""
    # Build optimized query - select only needed fields for better performance
    query = select(
        Article.id,
        Article.title,
        Article.summary, 
        Article.content,
        Article.url,
        Article.image_url,
        Article.media_files,
        Article.published_at,
        Article.fetched_at,
        Article.is_advertisement,
        Article.ad_confidence,
        Article.ad_type,
        Article.ad_reasoning,
        Article.source_id
    ).where(
        publication_filter(
            include_classified_ads=bool(category and category.lower() == 'advertisements')
        )
    ).order_by(desc(Article.fetched_at))
    
    # Apply time filter
    if since_hours:
        since_time = datetime.utcnow() - timedelta(hours=since_hours)
        query = query.where(Article.published_at >= since_time)
    
    # Apply category filter
    if category and category.lower() != 'all':
        if category.lower() == 'advertisements':
            # Filter for advertising content
            query = query.where(Article.is_advertisement.is_(True))
        else:
            # Filter by category using new junction table
            query = query.join(ArticleCategory).join(Category).where(
                func.lower(Category.name) == category.lower()
            )
    
    # Apply pagination
    query = query.offset(offset).limit(limit)
    
    # Execute query
    result = await db.execute(query)
    article_rows = result.all()
    
    # Separate queries for categories to avoid N+1 problem
    categories_by_article = {}
    sources_by_article = {}
    
    if article_rows:
        article_ids = [row.id for row in article_rows]
        
        # Get categories for all articles in one query
        categories_result = await db.execute(
            select(
                ArticleCategory.article_id,
                ArticleCategory.ai_category,
                ArticleCategory.confidence,
                Category.name.label('category_name'),
                Category.display_name
            ).join(Category)
            .where(ArticleCategory.article_id.in_(article_ids))
        )
        for row in categories_result.all():
            if row.article_id not in categories_by_article:
                categories_by_article[row.article_id] = []
            categories_by_article[row.article_id].append({
                'ai_category': row.ai_category or 'Other',
                'confidence': row.confidence or 1.0,
                'name': row.category_name,
                'display_name': row.display_name
            })
        
        # Get sources for all articles in one query  
        sources_result = await db.execute(
            select(
                Article.id,
                Article.source_id
            ).where(Article.id.in_(article_ids))
        )
        sources_by_article = {row.id: row.source_id for row in sources_result.all()}
    
    # Format response - optimized processing
    feed_items = []
    
    for article_row in article_rows:
        domain = extract_domain(article_row.url) if article_row.url else "unknown"
        
        # Clean summary from service text
        cleaned_summary = clean_summary_text(article_row.summary) if article_row.summary else None
        
        # Get categories for this article
        article_categories = categories_by_article.get(article_row.id, [])
        
        # Simple category mapping - get first category or default
        primary_category = article_categories[0]['display_name'] if article_categories else 'Other'
        
        # Extract media files safely
        media_files = article_row.media_files if hasattr(article_row, 'media_files') and article_row.media_files else []
        
        feed_items.append({
            "article_id": article_row.id,
            "title": clean_html_entities(article_row.title) if article_row.title else None,
            "summary": clean_html_entities(cleaned_summary) if cleaned_summary else None,
            "content": clean_html_entities(article_row.content[:500] + "..." if article_row.content and len(article_row.content) > 500 else article_row.content) if article_row.content else None,
            "image_url": article_row.image_url,  # Legacy single image field for backward compatibility
            "media_files": media_files,  # New multiple media files
            "url": article_row.url,
            "domain": domain,
            "category": primary_category,  # Primary mapped category for backward compatibility
            "categories": [{'name': cat['name'], 'display_name': cat['display_name']} for cat in article_categories],
            "ai_categories": [{'ai_category': cat['ai_category'], 'confidence': cat['confidence']} for cat in article_categories],
            "published_at": article_row.published_at.isoformat() if article_row.published_at else None,
            "fetched_at": article_row.fetched_at.isoformat() if article_row.fetched_at else None,
            # Advertising detection data
            "is_advertisement": bool(article_row.is_advertisement or False),
            "ad_confidence": float(article_row.ad_confidence or 0.0),
            "ad_type": article_row.ad_type,
            "ad_reasoning": article_row.ad_reasoning,
            "source_id": article_row.source_id
        })
    
    return {
        "version": "2.0",
        "title": "Evening News v2 Feed",
        "updated_at": datetime.utcnow().isoformat(),
        "items": feed_items,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "count": len(feed_items),
            "category": category  # Include current filter in response
        }
    }


@router.get("/feed.rss")
async def get_rss_feed(
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """RSS compatibility endpoint."""
    
    # Get articles
    query = (
        select(Article)
        .where(publication_filter())
        .where(Article.is_advertisement.is_(False))
        .order_by(desc(Article.published_at))
        .limit(limit)
    )
    
    result = await db.execute(query)
    articles = result.scalars().all()
    
    # Create RSS XML
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    
    # Channel metadata
    SubElement(channel, "title").text = "Evening News v2"
    SubElement(channel, "link").text = "http://localhost:8000"
    SubElement(channel, "description").text = "Aggregated news feed"
    SubElement(channel, "generator").text = "Evening News v2.0"
    SubElement(channel, "lastBuildDate").text = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    
    # Add items
    for article in articles:
        item = SubElement(channel, "item")
        SubElement(item, "title").text = clean_html_entities(article.title) if article.title else ""
        SubElement(item, "link").text = article.url
        SubElement(item, "description").text = clean_html_entities(article.summary or article.title) if (article.summary or article.title) else ""
        SubElement(item, "guid").text = str(article.id)
        if article.published_at:
            SubElement(item, "pubDate").text = article.published_at.strftime("%a, %d %b %Y %H:%M:%S GMT")
    
    # Format XML
    rough_string = tostring(rss, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    
    return Response(
        content=reparsed.toprettyxml(indent="  "),
        media_type="application/rss+xml"
    )

