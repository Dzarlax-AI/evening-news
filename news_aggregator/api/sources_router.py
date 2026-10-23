import logging
"""Sources API router - handles news sources management."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete

from ..database import get_db
from ..models import Article, Source
from ..services.source_manager import SourceManager

logger = logging.getLogger(__name__)


router = APIRouter()


# ============================================================================
# Pydantic Models for Sources
# ============================================================================

class CreateSourceRequest(BaseModel):
    name: str
    source_type: str
    url: str
    is_active: bool = True
    update_interval: int = 60


class UpdateSourceRequest(BaseModel):
    name: Optional[str] = None
    source_type: Optional[str] = None
    url: Optional[str] = None
    is_active: Optional[bool] = None
    update_interval: Optional[int] = None


# ============================================================================
# Sources Endpoints
# ============================================================================

@router.get("")
async def get_sources_api(
    db: AsyncSession = Depends(get_db)
):
    """Get all news sources."""
    source_manager = SourceManager()
    sources = await source_manager.get_sources(db)
    
    # Get article counts for each source
    article_counts = {}
    for source in sources:
        count_result = await db.execute(
            select(func.count(Article.id)).where(Article.source_id == source.id)
        )
        article_counts[source.id] = count_result.scalar() or 0
    
    return {
        "sources": [
            {
                "id": source.id,
                "name": source.name,
                "source_type": source.source_type,
                "url": source.url,
                "is_active": source.enabled,
                "last_updated": source.last_fetch.isoformat() if source.last_fetch else None,
                "last_success": source.last_success.isoformat() if source.last_success else None,
                "articles_count": article_counts.get(source.id, 0),
                "error_count": source.error_count,
                "last_error": source.last_error
            }
            for source in sources
        ]
    }


@router.post("")
async def create_source(
    source_data: CreateSourceRequest,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth when security is fixed
):
    """Create a new news source."""
    try:
        logger.info(f"Creating source with data: {source_data}")
        source_manager = SourceManager()
        from ..config import settings
        worker_mode = settings.background_execution_mode == "worker"
        
        # Create source
        source = await source_manager.create_source(
            db,
            name=source_data.name,
            source_type=source_data.source_type,
            url=source_data.url,
            skip_connection_test=worker_mode,
        )
        
        # Set active status
        source.enabled = source_data.is_active
        await db.commit()
        
        return {
            "id": source.id,
            "name": source.name,
            "source_type": source.source_type,
            "url": source.url,
            "is_active": source.enabled,
            "created_at": source.created_at.isoformat() if source.created_at else None
        }
        
    except Exception as e:
        logger.info(f"Error creating source: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{source_id}/test")
async def test_source(
    source_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth when security is fixed
):
    """Test a news source connection."""
    try:
        source_manager = SourceManager()
        
        # Get source
        result = await db.execute(select(Source).where(Source.id == source_id))
        source = result.scalar_one_or_none()
        
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")

        from ..config import settings
        if settings.background_execution_mode == "worker":
            from .background_jobs_router import accepted_response, enqueue_from_request
            accepted = await enqueue_from_request("source_test", {"source_id": source_id}, request)
            return accepted_response(accepted)
        
        # Test source connection
        source_instance = await source_manager.get_source_instance(source)
        success = await source_instance.test_connection()
        
        return {
            "source_id": source_id,
            "success": success,
            "message": "Connection successful" if success else "Connection failed"
        }
        
    except Exception as e:
        logger.info(f"Error testing source: {e}")
        if isinstance(e, HTTPException):
            raise
        return {
            "source_id": source_id,
            "success": False,
            "message": f"Test failed: {str(e)}"
        }


@router.put("/{source_id}")
async def update_source(
    source_id: int,
    source_data: UpdateSourceRequest,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth when security is fixed
):
    """Update a news source."""
    try:
        # Get source
        result = await db.execute(select(Source).where(Source.id == source_id))
        source = result.scalar_one_or_none()
        
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        
        # Update source fields
        if source_data.name is not None:
            source.name = source_data.name
        if source_data.source_type is not None:
            source.source_type = source_data.source_type
        if source_data.url is not None:
            source.url = source_data.url
        if source_data.is_active is not None:
            source.enabled = source_data.is_active
        if source_data.update_interval is not None:
            source.fetch_interval = source_data.update_interval
        
        await db.commit()
        await db.refresh(source)
        
        return {
            "id": source.id,
            "name": source.name,
            "source_type": source.source_type,
            "url": source.url,
            "is_active": source.enabled,
            "update_interval": source.fetch_interval,
            "updated_at": source.updated_at.isoformat() if source.updated_at else None
        }
        
    except Exception as e:
        logger.info(f"Error updating source: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{source_id}/toggle")
async def toggle_source(
    source_id: int,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth when security is fixed
):
    """Toggle source active status."""
    try:
        # Get source
        result = await db.execute(select(Source).where(Source.id == source_id))
        source = result.scalar_one_or_none()
        
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        
        # Toggle enabled status
        source.enabled = not source.enabled
        await db.commit()
        
        return {
            "id": source.id,
            "name": source.name,
            "is_active": source.enabled,
            "message": f"Source {'enabled' if source.enabled else 'disabled'}"
        }
        
    except Exception as e:
        logger.info(f"Error toggling source: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{source_id}")
async def delete_source(
    source_id: int,
    delete_articles: bool = False,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth when security is fixed
):
    """Delete a news source."""
    try:
        # Get source
        result = await db.execute(select(Source).where(Source.id == source_id))
        source = result.scalar_one_or_none()
        
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        
        # Check if source has articles
        articles_count_result = await db.execute(
            select(func.count(Article.id)).where(Article.source_id == source_id)
        )
        articles_count = articles_count_result.scalar() or 0
        
        if articles_count > 0 and not delete_articles:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Cannot delete source with {articles_count} articles. "
                    "Disable it instead or pass delete_articles=true to purge."
                )
            )

        # Delete via manager (handles articles and cleanup)
        source_manager = SourceManager()
        await source_manager.delete_source(db, source_id, delete_articles=delete_articles)
        
        return {
            "message": f"Source '{source.name}' deleted successfully",
            "deleted_source_id": source_id,
            "deleted_articles": articles_count if delete_articles else 0
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.info(f"Error deleting source: {e}")
        raise HTTPException(status_code=400, detail=str(e))

