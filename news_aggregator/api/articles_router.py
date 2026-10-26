import logging
"""Articles API router - handles individual article operations."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
import sqlalchemy

from ..database import get_db
from ..models import Article, ArticleCategory

logger = logging.getLogger(__name__)


router = APIRouter()


# ============================================================================
# Pydantic Models for Articles
# ============================================================================

class ReprocessRequest(BaseModel):
    force: bool = True  # Enable force processing by default for reprocessing
    reextract_media: bool = False


# ============================================================================
# Articles Endpoints
# ============================================================================

@router.post("/{article_id}/reprocess")
async def reprocess_article(
    article_id: int,
    request: ReprocessRequest,
    db: AsyncSession = Depends(get_db),
    http_request: Request = None,
):
    """Queue a single article reprocess under the durable worker fence."""
    exists = await db.scalar(select(Article.id).where(Article.id == article_id))
    if exists is None:
        raise HTTPException(status_code=404, detail="Article not found")
    from .background_jobs_router import accepted_response, enqueue_from_request
    accepted = await enqueue_from_request("article_reprocess", {
        "article_ids": [article_id], "force": request.force,
        "reextract_media": request.reextract_media,
    }, http_request)
    return accepted_response(accepted)


@router.get("/{article_id}")
async def get_article(
    article_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a single article with full details."""
    try:
        # Get article with all related data
        result = await db.execute(
            select(Article)
            .options(
                selectinload(Article.source),
                selectinload(Article.article_categories).selectinload(ArticleCategory.category)
            )
            .where(Article.id == article_id)
        )
        article = result.scalar_one_or_none()
        
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        
        # Get media files
        image_url = article.image_url
        media_files = article.media_files or []
        
        return {
            "id": article.id,
            "title": article.title,
            "summary": article.summary,
            "content": article.content,
            "url": article.url,
            "image_url": image_url,
            "media_files": media_files,
            "published_at": article.published_at.isoformat() if article.published_at else None,
            "fetched_at": article.fetched_at.isoformat() if article.fetched_at else None,
            "processed": article.processed,
            "processing_state": article.processing_state,
            "processing_attempts": article.processing_attempts,
            "next_attempt_at": article.next_attempt_at.isoformat() if article.next_attempt_at else None,
            "summary_processed": article.summary_processed,
            "category_processed": article.category_processed,
            "ad_processed": article.ad_processed,
            # Source information
            "source": {
                "id": article.source.id if article.source else None,
                "name": article.source.name if article.source else None,
                "type": article.source.source_type if article.source else None
            },
            # Categories
            "categories": article.categories_with_confidence,
            "primary_category": article.primary_category,
            # Advertising detection
            "is_advertisement": bool(getattr(article, 'is_advertisement', False)),
            "ad_confidence": float(getattr(article, 'ad_confidence', 0.0)),
            "ad_type": getattr(article, 'ad_type', None),
            "ad_reasoning": getattr(article, 'ad_reasoning', None),
            "ad_markers": getattr(article, 'ad_markers', [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.info(f"Error getting article: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get article: {str(e)}")
