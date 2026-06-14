"""Public interface routes."""

import logging
import asyncio
import os
from time import monotonic
from fastapi import APIRouter, Request, Query, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func, text
from sqlalchemy.orm import selectinload
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
try:
    import magic
    MAGIC_AVAILABLE = True
except ImportError:
    MAGIC_AVAILABLE = False
    logger.warning("⚠️ python-magic not available - MIME type detection disabled")
from .database import get_db
from .database_helpers import fetch_raw_all, count_query, execute_custom_read
from .models import Article, Category, ArticleCategory, DailySummary
from .config import get_settings
from .services.data_service import DataService, get_data_service
from .services.database_queue import get_database_queue
from .security import verify_jwt_token
from .api.articles_router import ReprocessRequest, reprocess_article

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")
templates.env.globals["config"] = get_settings()

_FEED_CACHE_TTL_SECONDS = int(os.getenv("PUBLIC_FEED_CACHE_TTL_SECONDS", "30"))
_FEED_CACHE_MAX_ENTRIES = int(os.getenv("PUBLIC_FEED_CACHE_MAX_ENTRIES", "200"))
_FEED_CACHE: dict[tuple, tuple[float, dict]] = {}
_FEED_CACHE_LOCK = asyncio.Lock()


def _has_public_admin_access(request: Request) -> bool:
    cookie_token = request.cookies.get("admin_token")
    if not cookie_token:
        return False

    payload = verify_jwt_token(cookie_token)
    return bool(payload and payload.get("level") == "admin")


def _make_feed_cache_key(
    limit: int,
    offset: int,
    since_hours: Optional[int],
    category: Optional[str],
    source: Optional[str],
    hide_ads: bool,
) -> tuple:
    return (limit, offset, since_hours, category, source, hide_ads)


def extract_images_from_content(content: str) -> list:
    """Extract images from HTML content and return as media file objects."""
    if not content:
        return []
    
    import re
    import html
    
    content_images = []
    # Find all img tags in content
    img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
    img_matches = re.findall(img_pattern, content, re.IGNORECASE)
    
    for i, img_url in enumerate(img_matches):
        # Clean up URL (decode HTML entities)
        clean_url = html.unescape(img_url)
        
        # Create media file object
        content_images.append({
            'type': 'image',
            'url': clean_url,
            'thumbnail_url': clean_url,
            'filename': f'content_image_{i+1}.jpg',
            'source': 'content_html'
        })
    
    return content_images


@router.get("/news", response_class=RedirectResponse)
async def news_redirect():
    """Redirect /news to root."""
    return RedirectResponse(url="/", status_code=301)


@router.get("/feed", response_class=RedirectResponse)
async def public_feed_redirect():
    """Redirect legacy /feed to root."""
    return RedirectResponse(url="/", status_code=301)


@router.get("/cards", response_class=RedirectResponse)
async def public_cards_redirect():
    """Redirect legacy /cards to root."""
    return RedirectResponse(url="/", status_code=301)


@router.get("/list", response_class=HTMLResponse)
async def public_list_view(request: Request):
    """List view for news feed (alias for compatibility)."""
    return templates.TemplateResponse(request, "public/list.html", {
        "title": "Лента новостей"
    })


@router.get("/search", response_class=HTMLResponse)
async def public_search_view(request: Request):
    """Public search page."""
    return templates.TemplateResponse(request, "public/search.html", {
        "title": "Поиск новостей"
    })


@router.get("/digest/{digest_date}", response_class=HTMLResponse)
async def public_digest_view(
    digest_date: str,
    request: Request,
):
    """Daily digest permalink used by Telegram full-version links."""
    try:
        parsed_date = datetime.strptime(digest_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=404, detail="Digest not found")

    async def read_digest_data(db):
        summary_result = await db.execute(
            select(DailySummary)
            .where(DailySummary.date == parsed_date)
            .order_by(DailySummary.articles_count.desc())
        )
        article_result = await db.execute(
            select(Article, Category.name, ArticleCategory.confidence)
            .options(
                selectinload(Article.source),
                selectinload(Article.article_categories).selectinload(ArticleCategory.category),
            )
            .join(ArticleCategory, Article.id == ArticleCategory.article_id)
            .join(Category, Category.id == ArticleCategory.category_id)
            .where(func.date(Article.fetched_at) == parsed_date)
            .where(Article.is_advertisement.is_not(True))
            .order_by(Article.fetched_at.desc())
        )
        return summary_result.scalars().all(), article_result.all()

    summaries, article_rows = await get_database_queue().execute_read(read_digest_data, timeout=30.0)

    best_by_article = {}
    for article, category_name, confidence in article_rows:
        current = best_by_article.get(article.id)
        if current is None or (confidence or 0) > (current["confidence"] or 0):
            best_by_article[article.id] = {
                "article": article,
                "category": category_name or "Other",
                "confidence": confidence or 0,
            }

    articles_by_category = {}
    for item in best_by_article.values():
        article = item["article"]
        articles_by_category.setdefault(item["category"], []).append({
            "id": article.id,
            "title": article.title,
            "summary": article.summary,
            "url": article.url,
            "source_name": article.source.name if article.source else "Unknown",
            "fetched_at": article.fetched_at,
            "published_at": article.published_at,
            "image_url": article.primary_image or article.image_url,
        })

    summary_categories = [summary.category for summary in summaries]
    ordered_categories = summary_categories + [
        category for category in articles_by_category
        if category not in summary_categories
    ]

    if not summaries and not articles_by_category:
        raise HTTPException(status_code=404, detail="Digest not found")

    return templates.TemplateResponse(request, "public/digest.html", {
        "title": f"Сводка новостей за {parsed_date.strftime('%d.%m.%Y')}",
        "digest_date": parsed_date,
        "summaries": summaries,
        "articles_by_category": articles_by_category,
        "ordered_categories": ordered_categories,
        "total_articles": sum(len(items) for items in articles_by_category.values()),
    })


@router.get("/api/public/feed")
async def get_public_feed(
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    since_hours: Optional[int] = Query(None, ge=1, le=168),
    category: Optional[str] = Query(None),
    source: Optional[str] = Query(None, description="Filter by source ID(s), comma-separated"),
    hide_ads: bool = Query(True, description="Hide advertisements from feed"),
    data_service: DataService = Depends(get_data_service)
):
    """Public feed endpoint using unified DataService."""

    try:
        if _FEED_CACHE_TTL_SECONDS > 0:
            cache_key = _make_feed_cache_key(
                limit=limit,
                offset=offset,
                since_hours=since_hours,
                category=category,
                source=source,
                hide_ads=hide_ads,
            )
            now = monotonic()
            async with _FEED_CACHE_LOCK:
                cached = _FEED_CACHE.get(cache_key)
                if cached and cached[0] > now:
                    return cached[1]

        # Use DataService for clean database access
        articles_data = await data_service.get_articles_feed(
            limit=limit,
            offset=offset,
            since_hours=since_hours,
            category=category,
            source=source,
            hide_ads=hide_ads
        )

        response_payload = {
            "articles": articles_data,
            "total": len(articles_data),
            "limit": limit,
            "offset": offset
        }

        if _FEED_CACHE_TTL_SECONDS > 0:
            async with _FEED_CACHE_LOCK:
                if len(_FEED_CACHE) >= _FEED_CACHE_MAX_ENTRIES:
                    _FEED_CACHE.pop(next(iter(_FEED_CACHE)))
                _FEED_CACHE[cache_key] = (monotonic() + _FEED_CACHE_TTL_SECONDS, response_payload)

        return response_payload
    
    except Exception as e:
        # If DB fails, return mock data
        logger.info(f"Database error: {e}")
        mock_articles = [
            {
                "id": 1,
                "title": "⚠️ База данных недоступна",
                "summary": "Показываем тестовые данные. Проверьте подключение к БД.",
                "url": "https://example.com/db-error",
                "source": "Система",
                "category": "Other",
                "published_at": datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
                "is_advertisement": False,
                "ad_confidence": 0.0,
                "ad_type": None
            }
        ]
        
        return {
            "articles": mock_articles,
            "total": len(mock_articles),
            "limit": limit,
            "offset": offset
        }


@router.get("/api/public/article/{article_id}")
async def get_public_article(
    article_id: int, 
    data_service: DataService = Depends(get_data_service)
):
    """Get full article content using unified DataService."""
    try:
        # Handle mock data case (when database is not available)
        if article_id == 1:
            return {
                "id": 1,
                "title": "⚠️ База данных недоступна",
                "summary": "Показываем тестовые данные. Проверьте подключение к БД.",
                "content": "К сожалению, база данных недоступна. Это тестовая статья. Пожалуйста, проверьте подключение к базе данных и перезапустите приложение.",
                "url": "https://example.com/db-error",
                "image_url": None,
                "source_name": "Система",
                "category": "other",
                "categories": [],
                "published_at": datetime.utcnow().isoformat(),
                "fetched_at": datetime.utcnow().isoformat(),
                "is_advertisement": False,
                "ad_confidence": 0.0,
                "ad_type": None,
                "ad_reasoning": None,
                "ad_markers": [],
                "media_files": [],
                "images": [],
                "videos": [],
                "documents": [],
                "primary_image": None,
                "ai_categories": [
                    {"category": "system", "confidence": 0.95},
                    {"category": "notification", "confidence": 0.80}
                ]
            }
        
        # Use DataService for clean database access
        article_data = await data_service.get_article_by_id(article_id)
        
        if not article_data:
            raise HTTPException(status_code=404, detail="Article not found")
        
        return article_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting article {article_id}: {e}")
        import traceback
        traceback.print_exc()
        
        # Return mock error data instead of 500 error
        return {
            "id": article_id,
            "title": "❌ Ошибка загрузки статьи",
            "summary": f"Не удалось загрузить статью с ID {article_id}",
            "content": f"Произошла ошибка при загрузке статьи: {str(e)[:200]}...",
            "url": "#",
            "image_url": None,
            "source_name": "Система",
            "category": "other",
            "categories": [],
            "published_at": datetime.utcnow().isoformat(),
            "fetched_at": datetime.utcnow().isoformat(),
            "is_advertisement": False,
            "ad_confidence": 0.0,
            "ad_type": None,
            "ad_reasoning": None,
            "ad_markers": [],
            "media_files": [],
            "images": [],
            "videos": [],
            "documents": [],
            "primary_image": None,
            "ai_categories": []
        }


@router.post("/api/public/article/{article_id}/reprocess")
async def reprocess_public_article(
    article_id: int,
    payload: ReprocessRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Reprocess an article from the public modal using app-level admin auth.

    The public page cannot fetch Traefik/Authentik-protected /api/v1 routes
    without cross-origin redirects. This endpoint keeps the request same-origin
    and relies on the admin JWT cookie set after visiting /admin/.
    """
    if not _has_public_admin_access(request):
        raise HTTPException(status_code=401, detail="Admin access required")

    return await reprocess_article(article_id, payload, db)


@router.get("/api/public/categories/config")
async def get_public_categories_config(data_service: DataService = Depends(get_data_service)):
    """Get categories configuration using unified DataService."""
    
    try:
        return await data_service.get_categories_config()
            
    except Exception as e:
        logger.info(f"Categories config error: {e}")
        # Return minimal fallback config
        return {
            "categories": {
                "all": {"name": "Все", "color": "#17a2b8"},
                "other": {"name": "Разное", "color": "#6c757d"}
            }
        }


@router.get("/api/public/categories")
async def get_public_categories(
    since_hours: Optional[int] = Query(None, ge=1, le=168),
    hide_ads: bool = Query(True),
    data_service: DataService = Depends(get_data_service)
):
    """Get category statistics using unified DataService."""

    try:
        return await data_service.get_categories_stats(
            since_hours=since_hours,
            hide_ads=hide_ads
        )

    except Exception as e:
        logger.info(f"Categories error: {e}")
        return {"categories": {"all": 0}}


@router.get("/api/public/sources")
async def get_public_sources(
    since_hours: Optional[int] = Query(None, ge=1, le=168),
    hide_ads: bool = Query(True),
    data_service: DataService = Depends(get_data_service)
):
    """Get source statistics using unified DataService."""

    try:
        return await data_service.get_sources_stats(
            since_hours=since_hours,
            hide_ads=hide_ads
        )

    except Exception as e:
        logger.info(f"Sources error: {e}")
        return {"sources": {}, "source_names": {}, "total": 0}


@router.get("/api/public/search")
async def search_articles(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    category: Optional[str] = Query(None, description="Filter by category"),
    since_hours: Optional[int] = Query(None, ge=1, le=8760, description="Filter articles from last N hours"),
    sort: str = Query("relevance", regex="^(relevance|date|title)$", description="Sort by: relevance, date, title"),
    hide_ads: bool = Query(True, description="Hide advertisements from results"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search articles by content, title, and summary.
    
    Supports:
    - Full-text search across title, summary, and content
    - Category filtering
    - Time-based filtering
    - Multiple sorting options
    - Pagination
    - Relevance scoring
    """
    try:
        from .models import Source, ArticleCategory, Category
        from sqlalchemy.orm import selectinload
        from sqlalchemy import or_, and_, case
        
        # Clean and prepare search query
        search_query = q.strip()
        if len(search_query) < 2:
            raise HTTPException(status_code=400, detail="Search query must be at least 2 characters")
        
        # Build base query with relationships
        query = select(Article).options(
            selectinload(Article.source),
            selectinload(Article.article_categories).selectinload(ArticleCategory.category)
        )
        
        # Full-text search across title, summary, and content
        # Using PostgreSQL ILIKE for case-insensitive partial matching
        search_conditions = []
        
        # Split search query into words for better matching
        search_words = [word.strip() for word in search_query.split() if len(word.strip()) >= 2]
        
        for word in search_words:
            word_pattern = f"%{word}%"
            word_condition = or_(
                Article.title.ilike(word_pattern),
                Article.summary.ilike(word_pattern),
                Article.content.ilike(word_pattern)
            )
            search_conditions.append(word_condition)
        
        # Combine all word conditions (AND logic for better precision)
        if search_conditions:
            if len(search_conditions) == 1:
                query = query.where(search_conditions[0])
            else:
                query = query.where(and_(*search_conditions))
        
        # Apply category filter
        if category and category.lower() != 'all':
            cats = [c.strip().lower() for c in category.split(',') if c.strip()]
            if len(cats) == 1 and cats[0] == 'advertisements':
                query = query.where(Article.is_advertisement == True)
            elif cats:
                subquery_categories = (
                    select(Article.id)
                    .join(ArticleCategory)
                    .join(Category)
                    .where(func.lower(Category.name).in_(cats))
                )
                query = query.where(Article.id.in_(subquery_categories))
        
        # Apply time filter
        if since_hours:
            since_time = datetime.utcnow() - timedelta(hours=since_hours)
            query = query.where(Article.published_at >= since_time)
        
        # Hide advertisements if requested (default: true)
        if hide_ads:
            query = query.where(Article.is_advertisement != True)
        
        # Apply sorting
        if sort == "date":
            query = query.order_by(desc(Article.published_at))
        elif sort == "title":
            query = query.order_by(Article.title)
        else:  # relevance (default)
            # Simple relevance: prioritize title matches, then summary, then content
            # Using CASE to create a relevance score
            relevance_score = case(
                (Article.title.ilike(f"%{search_query}%"), 3),
                (Article.summary.ilike(f"%{search_query}%"), 2),
                else_=1
            )
            query = query.order_by(desc(relevance_score), desc(Article.published_at))
        
        # Count total results for pagination
        count_query_stmt = select(func.count()).select_from(
            query.order_by(None).offset(None).limit(None).alias("search_results")
        )
        
        # Apply pagination
        query = query.offset(offset).limit(limit)
        
        # Execute queries using dependency injection
        # Get articles
        result = await db.execute(query)
        articles = result.scalars().all()
        
        # Get total count
        try:
            count_result = await db.execute(count_query_stmt)
            total_count = count_result.scalar()
        except Exception as count_error:
            logging.warning(f"Count query failed, using fallback: {count_error}")
            # Fallback: count current results
            total_count = len(articles) + offset
        
        # Convert to dict format (reuse logic from get_public_feed)
        articles_data = []
        for article in articles:
            # Extract images from content using shared function
            existing_media = article.media_files or []
            content_images = extract_images_from_content(article.content or '')
            all_media_files = list(existing_media) + content_images
            all_images = [m for m in all_media_files if m.get('type') == 'image']
            
            # Determine primary image
            primary_image = article.image_url
            if not primary_image and all_images:
                primary_image = all_images[0]['url']
            
            # Calculate simple relevance score for display
            relevance_score = 0
            title_lower = (article.title or '').lower()
            summary_lower = (article.summary or '').lower()
            content_lower = (article.content or '').lower()
            query_lower = search_query.lower()
            
            if query_lower in title_lower:
                relevance_score += 3
            if query_lower in summary_lower:
                relevance_score += 2
            if query_lower in content_lower:
                relevance_score += 1
            
            article_dict = {
                "id": article.id,
                "title": article.title,
                "summary": article.summary,
                "url": article.url,
                "image_url": article.image_url,
                "primary_image": primary_image,
                "source_id": article.source_id,
                "source_name": article.source.name if article.source else "Unknown",
                "category": article.primary_category,
                "categories": [
                    {
                        "name": ac.category.name,
                        "display_name": ac.category.display_name,
                        "color": ac.category.color,
                        "confidence": ac.confidence if ac.confidence is not None else 0.0
                    }
                    for ac in article.article_categories
                ],
                "published_at": article.published_at.isoformat() if article.published_at else None,
                "fetched_at": article.fetched_at.isoformat() if article.fetched_at else None,
                "is_advertisement": article.is_advertisement,
                "media_files": all_media_files,
                "images": all_images,
                "relevance_score": relevance_score
            }
            articles_data.append(article_dict)
        
        return {
            "articles": articles_data,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            },
            "query": search_query,
            "filters": {
                "category": category,
                "since_hours": since_hours,
                "sort": sort,
                "hide_ads": hide_ads
            },
            "results_count": len(articles_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in search_articles: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Media caching endpoint removed - functionality disabled

