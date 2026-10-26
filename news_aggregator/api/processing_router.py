import logging
"""Processing API router - handles news processing operations."""

from datetime import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from ..orchestrator import NewsOrchestrator

logger = logging.getLogger(__name__)


router = APIRouter()


@router.post("/run")
async def trigger_processing(request: Request):
    """Queue a durable cycle; Go owns article processing and Python keeps digest work."""
    from .background_jobs_router import accepted_response, enqueue_from_request
    accepted = await enqueue_from_request("news_processing", {}, request)
    return accepted_response(accepted)


@router.get("/status")  
async def get_processing_status():
    """Get current processing status."""
    try:
        from sqlalchemy import select, func
        from ..database import AsyncSessionLocal
        from ..models import BackgroundJob
        from ..services.background_jobs import read_worker_status
        async with AsyncSessionLocal() as session:
            worker = await read_worker_status(session)
            queued = await session.scalar(select(func.count()).select_from(BackgroundJob).where(
                BackgroundJob.state.in_(("queued", "interrupted"))))
            active = await session.scalar(select(func.count()).select_from(BackgroundJob).where(
                BackgroundJob.state == "running"))
        return {
            "busy": bool(active),
            "is_running": bool(active),
            "worker_available": bool(worker and worker["healthy"]),
            "queued_jobs": int(queued or 0),
            "active_jobs": int(active or 0),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


class SelectedRetryRequest(BaseModel):
    article_ids: list[int] = Field(min_length=1, max_length=100)


@router.post("/retry")
async def retry_selected_articles(request: SelectedRetryRequest, http_request: Request):
    """Queue an explicit selection while preserving completed stages."""
    from .background_jobs_router import accepted_response, enqueue_from_request
    accepted = await enqueue_from_request("article_reprocess", {
        "article_ids": request.article_ids, "force": False, "reextract_media": False,
    }, http_request)
    return accepted_response(accepted)

