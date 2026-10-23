"""Summaries API router - handles daily summaries."""

from datetime import datetime, date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, and_

from ..database import get_db
from ..models import DailySummary


router = APIRouter()


# ============================================================================
# Pydantic Models
# ============================================================================

class DailySummaryResponse(BaseModel):
    id: int
    date: date
    category: str
    summary_text: str
    articles_count: int
    created_at: datetime
    updated_at: Optional[datetime] = None


class SummaryCategory(BaseModel):
    category: str
    summaries_count: int


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/daily")
async def get_daily_summaries(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    category: Optional[str] = None,
    date_filter: Optional[str] = Query(None, alias="date"),
    db: AsyncSession = Depends(get_db)
):
    """Get daily summaries with pagination and filtering."""
    
    # Build query
    query = select(DailySummary)
    
    # Apply filters
    if category:
        query = query.where(DailySummary.category == category)
    
    if date_filter:
        try:
            filter_date = datetime.strptime(date_filter, "%Y-%m-%d").date()
            query = query.where(DailySummary.date == filter_date)
        except ValueError:
            pass  # Ignore invalid date format
    
    # Apply ordering, limit, and offset
    query = query.order_by(desc(DailySummary.date), desc(DailySummary.created_at))
    query = query.limit(limit).offset(offset)
    
    # Execute query
    result = await db.execute(query)
    summaries = result.scalars().all()
    
    # Convert to response format
    summaries_data = []
    for summary in summaries:
        summaries_data.append({
            "id": summary.id,
            "date": summary.date.isoformat(),
            "category": summary.category,
            "summary_text": summary.summary_text,
            "articles_count": summary.articles_count,
            "created_at": summary.created_at.isoformat(),
            "updated_at": summary.updated_at.isoformat() if summary.updated_at else summary.created_at.isoformat()
        })
    
    return {
        "summaries": summaries_data,
        "limit": limit,
        "offset": offset,
        "total": len(summaries_data)
    }


@router.get("/categories")
async def get_summary_categories(db: AsyncSession = Depends(get_db)):
    """Get available categories for summaries with counts."""
    
    result = await db.execute(
        select(
            DailySummary.category,
            func.count(DailySummary.id).label('summaries_count')
        )
        .group_by(DailySummary.category)
        .order_by(func.count(DailySummary.id).desc())
    )
    
    categories = []
    for category, count in result.all():
        categories.append({
            "category": category,
            "summaries_count": count
        })
    
    return {
        "categories": categories,
        "total_categories": len(categories)
    }


@router.post("/generate")
async def generate_daily_summaries(request: Request, db: AsyncSession = Depends(get_db)):
    """Generate daily summaries for today."""
    from ..config import settings
    if settings.background_execution_mode == "worker":
        from .background_jobs_router import accepted_response, enqueue_from_request
        accepted = await enqueue_from_request("summaries_generate", {}, request)
        return accepted_response(accepted)
    try:
        from ..orchestrator import NewsOrchestrator
        
        orchestrator = NewsOrchestrator()
        result = await orchestrator._generate_daily_summaries()
        
        return {
            "success": True,
            "message": "Daily summaries generated successfully",
            "summaries_generated": result.get('summaries_generated', 0),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to generate daily summaries: {str(e)}",
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/{summary_id}")
async def get_summary_by_id(
    summary_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific summary by ID."""
    
    result = await db.execute(
        select(DailySummary).where(DailySummary.id == summary_id)
    )
    summary = result.scalar_one_or_none()
    
    if not summary:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Summary not found")
    
    return {
        "id": summary.id,
        "date": summary.date.isoformat(),
        "category": summary.category,
        "summary_text": summary.summary_text,
        "articles_count": summary.articles_count,
        "created_at": summary.created_at.isoformat(),
        "updated_at": summary.updated_at.isoformat() if summary.updated_at else summary.created_at.isoformat()
    }
