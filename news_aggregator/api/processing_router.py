import logging
"""Processing API router - handles news processing operations."""

import asyncio
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel, Field

from ..orchestrator import (NewsOrchestrator, reserve_processing_cycle,
                            release_processing_cycle, processing_cycle_busy)

logger = logging.getLogger(__name__)


router = APIRouter()


async def _run_processing_cycle(reservation):
    """Background helper to run and clean up the orchestrator cycle."""
    orchestrator = None
    try:
        orchestrator = NewsOrchestrator()
        await orchestrator.start()
        await orchestrator.run_full_cycle(reservation=reservation)
    except Exception as e:
        logger.error(f"Background processing failed: {e}")
    finally:
        try:
            if orchestrator is not None:
                await orchestrator.stop()
        except Exception:
            pass
        finally:
            release_processing_cycle(reservation)

@router.post("/run")
async def trigger_processing(background_tasks: BackgroundTasks, request: Request = None):
    """Trigger news processing cycle in background."""
    from .background_jobs_router import accepted_response, enqueue_from_request
    accepted = await enqueue_from_request("news_processing", {}, request)
    if accepted is not None:
        return accepted_response(accepted)
    reservation = reserve_processing_cycle()
    if reservation is None:
        return {"success": False, "status": "busy", "message": "News processing is already running"}
    try:
        logger.info("📰 Starting news processing cycle...")
        # Run processing safely in FastAPI BackgroundTasks 
        # (This prevents 'Task was destroyed' caused by Python Garbage Collection)
        background_tasks.add_task(_run_processing_cycle, reservation)
        
        return {
            "success": True,
            "message": "News processing started",
            "started_at": datetime.utcnow().isoformat(),
            "status": "running"
        }
        
    except Exception as e:
        release_processing_cycle(reservation)
        logger.info(f"Error starting processing: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start processing: {str(e)}")


@router.get("/status")  
async def get_processing_status():
    """Get current processing status."""
    from ..config import settings
    if settings.background_execution_mode == "worker":
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
            "timestamp": datetime.utcnow().isoformat(),
        }
    try:
        orchestrator = NewsOrchestrator()
        stats = orchestrator.get_queue_stats()
        
        return {
            "busy": processing_cycle_busy(),
            "queue_stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


class SelectedRetryRequest(BaseModel):
    article_ids: list[int] = Field(min_length=1, max_length=100)


@router.post("/retry")
async def retry_selected_articles(request: SelectedRetryRequest):
    """Requeue an explicit selection; completed stage results are preserved."""
    reservation = reserve_processing_cycle()
    if reservation is None:
        raise HTTPException(status_code=409, detail="News processing is already running")
    try:
        from ..config import settings
        if settings.background_execution_mode == "worker":
            from ..services.article_retry import reset_selected_articles
            try:
                count = await reset_selected_articles(request.article_ids, force=False)
                return {"success": True, "articles_requeued": count}
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
        count = await NewsOrchestrator().retry_selected_articles(request.article_ids, reservation=reservation)
        return {"success": True, "articles_requeued": count}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        release_processing_cycle(reservation)

