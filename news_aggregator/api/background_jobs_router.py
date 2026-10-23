"""Admin endpoints for accepted durable jobs."""

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response, status
from sqlalchemy import select

from ..config import settings
from ..database import AsyncSessionLocal
from ..models import BackgroundJob
from ..services.background_jobs import (
    IdempotencyConflict, QueueFull, enqueue_job,
    read_worker_status,
)

router = APIRouter()


async def enqueue_from_request(kind: str, payload: dict, request: Request | None):
    """Return None in embedded mode; otherwise accept work durably or raise HTTP errors."""
    if settings.background_execution_mode != "worker":
        return None
    key = request.headers.get("Idempotency-Key") if request is not None else None
    if key is None:
        import secrets
        key = secrets.token_hex(24)
    try:
        job = await enqueue_job(kind, payload, key)
    except QueueFull as exc:
        raise HTTPException(status_code=503, detail="Background queue is full",
                            headers={"Retry-After": "10"}) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail="Idempotency key conflicts with an existing job") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "success": True,
        "status": "queued",
        "job_id": str(job.id),
        "status_url": f"/api/v1/jobs/{job.id}",
    }


def accepted_response(body: dict):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=body)


@router.get("/{job_id}")
async def get_job(job_id: UUID, response: Response):
    async with AsyncSessionLocal() as session:
        job = await session.scalar(select(BackgroundJob).where(BackgroundJob.id == job_id))
        worker = await read_worker_status(session)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    available = bool(worker and worker["healthy"])
    return {
        "success": True,
        "job_id": str(job.id),
        "kind": job.kind,
        "status": job.state,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "result": job.result,
        "error_code": job.error_code,
        "worker_available": available,
    }
