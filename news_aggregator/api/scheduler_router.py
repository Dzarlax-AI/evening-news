"""Scheduler API router - handles task scheduling operations."""

from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, func, desc

from ..database import get_db
from ..models import ScheduleSettings
from ..config import settings
from ..models import BackgroundJob
from ..services.database_queue import get_database_queue
from ..services.scheduler_service_client import SchedulerServiceClient, SchedulerServiceError


router = APIRouter()

SERVICE_OPERATIONS = frozenset({
    "news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed",
})


def _is_news_service_resource(resource):
    if not isinstance(resource, dict):
        return False
    key = resource.get("schedule_key")
    operation = resource.get("operation")
    if resource.get("target_alias") != "news-worker" or operation not in SERVICE_OPERATIONS:
        return False
    if not isinstance(key, str) or not key.startswith("news:"):
        return False
    suffix = key[len("news:"):]
    return (suffix == operation
            or (suffix.isascii() and suffix.isdigit() and int(suffix) > 0))


def _scheduler_client():
    token = settings.scheduler_api_token
    token = token.get_secret_value() if token else None
    return SchedulerServiceClient(
        settings.scheduler_api_url, token,
        allow_http=settings.scheduler_api_allow_http,
        timeout_seconds=settings.scheduler_http_timeout_seconds,
    )


def _raise_scheduler_error(exc):
    status_code = getattr(exc, "status", None)
    if status_code not in {401, 403, 404, 409, 422, 429, 503}:
        status_code = 503
    detail = {
        401: "Scheduler service authentication failed",
        403: "Scheduler service denied the request",
        404: "Schedule not found",
        409: "Schedule changed concurrently",
        422: "Invalid schedule configuration",
        429: "Scheduler service is rate limited",
        503: "Scheduler service is unavailable",
    }.get(status_code, "Scheduler service is unavailable")
    raise HTTPException(status_code=status_code, detail=detail) from None


def _as_local_datetime(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _service_schedule_to_config(resource):
    calendar = resource["calendar"]
    schedule_type = calendar["type"]
    task_config = dict(resource["payload"])
    if schedule_type == "interval":
        task_config["interval_minutes"] = calendar["interval_minutes"]
    key = resource["schedule_key"]
    legacy_id = key[5:] if key.startswith("news:") else key
    setting_id = int(legacy_id) if legacy_id.isdigit() else key
    return {
        "id": setting_id,
        "task_name": resource["operation"],
        "enabled": resource["enabled"],
        "schedule_type": schedule_type,
        "hour": calendar.get("hour", 0),
        "minute": calendar.get("minute", 0),
        "weekdays": calendar.get("weekdays", []),
        "timezone": calendar["timezone"],
        "task_config": task_config,
        "last_run": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_status": None,
        "last_error": None,
        "last_duration_seconds": None,
        "next_run": _as_local_datetime(resource["next_due_at"]).isoformat()
            if resource["next_due_at"] else None,
        "is_running": False,
        "created_at": resource["created_at"],
        "updated_at": resource["updated_at"],
        "schedule_key": key,
        "schedule_revision": resource["revision"],
    }


async def _service_settings_snapshot(client=None):
    client = client or _scheduler_client()
    resources = await client.list_schedules()
    resources = [r for r in resources if _is_news_service_resource(r)]
    keys = {r["schedule_key"] for r in resources}
    if not keys:
        return []

    async def read_jobs(session):
        ranked_latest = select(
            BackgroundJob.id.label("job_id"),
            func.row_number().over(
                partition_by=BackgroundJob.schedule_key,
                order_by=(desc(BackgroundJob.created_at), desc(BackgroundJob.id)),
            ).label("rank"),
        ).where(BackgroundJob.schedule_key.in_(keys)).subquery()
        latest_rows = (await session.execute(
            select(BackgroundJob).join(ranked_latest, ranked_latest.c.job_id == BackgroundJob.id)
            .where(ranked_latest.c.rank == 1)
        )).scalars().all()

        running_rows = (await session.execute(
            select(BackgroundJob.schedule_key).where(
                BackgroundJob.schedule_key.in_(keys), BackgroundJob.state == "running"
            ).distinct()
        )).scalars().all()

        ranked_success = select(
            BackgroundJob.id.label("job_id"),
            func.row_number().over(
                partition_by=BackgroundJob.schedule_key,
                order_by=(desc(BackgroundJob.finished_at), desc(BackgroundJob.id)),
            ).label("rank"),
        ).where(
            BackgroundJob.schedule_key.in_(keys),
            BackgroundJob.state == "succeeded",
            func.coalesce(BackgroundJob.result["status"].as_string(), "").notin_(
                ("warning", "deferred")
            ),
        ).subquery()
        success_rows = (await session.execute(
            select(BackgroundJob).join(ranked_success, ranked_success.c.job_id == BackgroundJob.id)
            .where(ranked_success.c.rank == 1)
        )).scalars().all()
        return {
            "latest": {job.schedule_key: job for job in latest_rows},
            "running": set(running_rows),
            "last_success": {job.schedule_key: job for job in success_rows},
        }

    try:
        job_snapshot = await get_database_queue().execute_read(read_jobs)
    except Exception:
        raise HTTPException(status_code=503, detail="News execution status is unavailable") from None

    mapped = []
    for resource in resources:
        setting = _service_schedule_to_config(resource)
        key = resource["schedule_key"]
        job = job_snapshot["latest"].get(key)
        if job:
            setting.update(
                last_run=job.created_at.isoformat() if job.created_at else None,
                last_finished_at=job.finished_at.isoformat() if job.finished_at else None,
                last_status=(job.result or {}).get("status") if job.state == "succeeded" else job.state,
                last_error=job.error_code,
                last_duration_seconds=(job.result or {}).get("duration_seconds"),
                is_running=key in job_snapshot["running"],
            )
        else:
            setting["is_running"] = key in job_snapshot["running"]
        last_success = job_snapshot["last_success"].get(key)
        if last_success:
            setting["last_success_at"] = (
                last_success.finished_at.isoformat() if last_success.finished_at else None
            )
        if job and job.state == "succeeded" and (job.result or {}).get("status") not in {"warning", "deferred"}:
            # Preserve the legacy response's success status when the newest
            # occurrence itself succeeded, while keeping last_success_at
            # independent from a newer queued/running/failed occurrence.
            if last_success:
                setting["last_status"] = "success"
        mapped.append(setting)
    return mapped


def _service_resource_body(request, current=None):
    schedule_type = request.schedule_type or (
        current["calendar"]["type"] if current else "daily"
    )
    timezone_name = request.timezone or (
        current["calendar"]["timezone"] if current else "Europe/Belgrade"
    )
    old_calendar = current["calendar"] if current else {}
    calendar = {"type": schedule_type, "timezone": timezone_name}
    if schedule_type in {"daily", "weekly"}:
        calendar["hour"] = request.hour if request.hour is not None else old_calendar.get("hour", 0)
        calendar["minute"] = request.minute if request.minute is not None else old_calendar.get("minute", 0)
        days = request.weekdays if request.weekdays is not None else old_calendar.get("weekdays")
        if days is not None:
            calendar["weekdays"] = days
    elif schedule_type == "hourly":
        calendar["minute"] = request.minute if request.minute is not None else old_calendar.get("minute", 0)
        days = request.weekdays if request.weekdays is not None else old_calendar.get("weekdays")
        if days:
            calendar["weekdays"] = days
    elif schedule_type == "interval":
        config = request.task_config if request.task_config is not None else (current["payload"] if current else {})
        calendar["interval_minutes"] = config.get(
            "interval_minutes", old_calendar.get("interval_minutes", 30)
        )
    else:
        raise HTTPException(status_code=422, detail="Invalid schedule type")
    config = request.task_config if request.task_config is not None else (current["payload"] if current else {})
    payload = {key: value for key, value in config.items() if key != "interval_minutes"}
    try:
        from ..services.background_jobs import validate_payload
        payload = validate_payload(current["operation"] if current else request.task_name, payload)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid scheduled task configuration") from None
    return {
        "expected_revision": current["revision"] if current else 0,
        "enabled": request.enabled if request.enabled is not None else (
            current["enabled"] if current else True
        ),
        "calendar": calendar,
        "target_alias": current["target_alias"] if current else "news-worker",
        "operation": current["operation"] if current else request.task_name,
        "payload": payload,
    }


async def _find_service_schedule(client, task_name):
    matches = [s for s in await client.list_schedules()
               if _is_news_service_resource(s) and s["operation"] == task_name]
    if len(matches) > 1:
        raise HTTPException(status_code=409, detail="Multiple scheduler resources map to this task")
    return matches[0] if matches else None


# ============================================================================
# Pydantic Models for Scheduler
# ============================================================================

class ScheduleCreateRequest(BaseModel):
    task_name: str
    enabled: bool = True
    schedule_type: str = "daily"
    hour: int = 0
    minute: int = 0
    weekdays: List[int] = [1, 2, 3, 4, 5, 6, 7]
    timezone: str = "Europe/Belgrade"
    task_config: Optional[dict] = None


class ScheduleUpdateRequest(BaseModel):
    enabled: Optional[bool] = None
    schedule_type: Optional[str] = None
    hour: Optional[int] = None
    minute: Optional[int] = None
    weekdays: Optional[List[int]] = None  # Changed to list of integers
    timezone: Optional[str] = None
    task_config: Optional[dict] = None  # Added task_config support


# ============================================================================
# Scheduler Endpoints
# ============================================================================

@router.get("/settings")
async def get_schedule_settings(db: AsyncSession = Depends(get_db)):
    """Get all scheduled task settings."""
    if settings.schedule_dispatch_mode == "service":
        try:
            return {"settings": await _service_settings_snapshot(),
                    "timestamp": datetime.utcnow().isoformat()}
        except SchedulerServiceError as exc:
            _raise_scheduler_error(exc)
        except ValueError:
            raise HTTPException(status_code=503, detail="Scheduler service is not configured") from None
    try:
        result = await db.execute(select(ScheduleSettings))
        local_settings = result.scalars().all()

        return {
            "settings": [
                {
                    "id": setting.id,
                    "task_name": setting.task_name,
                    "enabled": setting.enabled,
                    "schedule_type": setting.schedule_type,
                    "hour": setting.hour,
                    "minute": setting.minute,
                    "weekdays": setting.weekdays,
                    "timezone": setting.timezone,
                    "task_config": setting.task_config or {},  # Added task_config
                    "last_run": setting.last_run.isoformat() if setting.last_run else None,
                    "last_finished_at": setting.last_finished_at.isoformat() if setting.last_finished_at else None,
                    "last_success_at": setting.last_success_at.isoformat() if setting.last_success_at else None,
                    "last_status": setting.last_status,
                    "last_error": setting.last_error,
                    "last_duration_seconds": float(setting.last_duration_seconds) if setting.last_duration_seconds is not None else None,
                    "next_run": setting.next_run.isoformat() if setting.next_run else None,
                    "is_running": setting.is_running,
                    "created_at": setting.created_at.isoformat() if setting.created_at else None,
                    "updated_at": setting.updated_at.isoformat() if setting.updated_at else None
                }
                for setting in local_settings
            ],
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get settings: {str(e)}")


@router.post("/settings")
async def create_schedule_setting(
    request: ScheduleCreateRequest,
    db: AsyncSession = Depends(get_db)
):
    """Create a new schedule setting for a task."""
    if settings.schedule_dispatch_mode == "service":
        if request.task_name not in SERVICE_OPERATIONS:
            raise HTTPException(status_code=422, detail="Unsupported scheduled task")
        try:
            client = _scheduler_client()
            if await _find_service_schedule(client, request.task_name):
                raise HTTPException(status_code=400,
                                    detail=f"Schedule for task '{request.task_name}' already exists")
            key = "news:" + request.task_name
            resource = await client.put_schedule(key, _service_resource_body(request))
            validate_resource = getattr(client, "validate_resource", None)
            if validate_resource:
                validate_resource(resource)
            setting = _service_schedule_to_config(resource)
            return {"success": True, "message": "Schedule created successfully", "setting": setting}
        except HTTPException:
            raise
        except SchedulerServiceError as exc:
            _raise_scheduler_error(exc)
        except ValueError:
            raise HTTPException(status_code=503, detail="Scheduler service is not configured") from None
    try:
        # Check if task with this name already exists
        result = await db.execute(
            select(ScheduleSettings).where(ScheduleSettings.task_name == request.task_name)
        )
        existing = result.scalar_one_or_none()

        if existing:
            raise HTTPException(status_code=400, detail=f"Schedule for task '{request.task_name}' already exists")

        # Calculate next_run
        from ..services.scheduler import calculate_next_run

        next_run = None
        if request.enabled:
            next_run = calculate_next_run(
                schedule_type=request.schedule_type,
                hour=request.hour,
                minute=request.minute,
                weekdays=request.weekdays,
                timezone=request.timezone,
                interval_minutes=(request.task_config or {}).get('interval_minutes', 30)
            )

        # Create new schedule setting
        new_setting = ScheduleSettings(
            task_name=request.task_name,
            enabled=request.enabled,
            schedule_type=request.schedule_type,
            hour=request.hour,
            minute=request.minute,
            weekdays=request.weekdays,
            timezone=request.timezone,
            task_config=request.task_config or {},
            next_run=next_run,
            is_running=False,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        db.add(new_setting)
        await db.commit()
        await db.refresh(new_setting)

        # Return response
        return {
            "success": True,
            "message": "Schedule created successfully",
            "setting": {
                "id": new_setting.id,
                "task_name": new_setting.task_name,
                "enabled": new_setting.enabled,
                "schedule_type": new_setting.schedule_type,
                "hour": new_setting.hour,
                "minute": new_setting.minute,
                "weekdays": new_setting.weekdays,
                "timezone": new_setting.timezone,
                "task_config": new_setting.task_config or {},
                "last_run": None,
                "last_finished_at": None,
                "last_success_at": None,
                "last_status": None,
                "last_error": None,
                "last_duration_seconds": None,
                "next_run": new_setting.next_run.isoformat() if new_setting.next_run else None,
                "is_running": new_setting.is_running,
                "created_at": new_setting.created_at.isoformat() if new_setting.created_at else None,
                "updated_at": new_setting.updated_at.isoformat() if new_setting.updated_at else None
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to create schedule: {str(e)}")


@router.put("/settings/{task_name}")
async def update_schedule_setting(
    task_name: str,
    request: ScheduleUpdateRequest,
    db: AsyncSession = Depends(get_db)
):
    """Update schedule settings for a task."""
    if settings.schedule_dispatch_mode == "service":
        if task_name not in SERVICE_OPERATIONS:
            raise HTTPException(status_code=404, detail="Schedule setting not found")
        try:
            client = _scheduler_client()
            current = await _find_service_schedule(client, task_name)
            if current is None:
                raise HTTPException(status_code=404, detail="Schedule setting not found")
            body = _service_resource_body(request, current)
            resource = await client.put_schedule(current["schedule_key"], body)
            setting = _service_schedule_to_config(resource)
            rows = await _service_settings_snapshot(client)
            setting = next((row for row in rows if row["task_name"] == task_name), setting)
            return {"success": True, "message": "Schedule updated successfully", "setting": setting}
        except HTTPException:
            raise
        except SchedulerServiceError as exc:
            _raise_scheduler_error(exc)
        except ValueError:
            raise HTTPException(status_code=503, detail="Scheduler service is not configured") from None
    try:
        # Get existing setting
        result = await db.execute(
            select(ScheduleSettings).where(ScheduleSettings.task_name == task_name)
        )
        setting = result.scalar_one_or_none()

        if not setting:
            raise HTTPException(status_code=404, detail="Schedule setting not found")

        # Update fields
        update_data = {}
        if request.enabled is not None:
            update_data['enabled'] = request.enabled
        if request.schedule_type is not None:
            update_data['schedule_type'] = request.schedule_type
        if request.hour is not None:
            update_data['hour'] = request.hour
        if request.minute is not None:
            update_data['minute'] = request.minute
        if request.weekdays is not None:
            # Convert list to JSON-compatible format for storage
            update_data['weekdays'] = request.weekdays
        if request.timezone is not None:
            update_data['timezone'] = request.timezone
        if request.task_config is not None:
            # Update task_config
            update_data['task_config'] = request.task_config

        if update_data:
            update_data['updated_at'] = datetime.utcnow()
            if settings.background_execution_mode == "worker":
                # Revoke schedule ownership from an already accepted run. The
                # job may finish but must not overwrite the edited schedule.
                update_data['is_running'] = False

            # Calculate next_run if schedule parameters changed
            if any(key in update_data for key in ['enabled', 'schedule_type', 'hour', 'minute', 'weekdays', 'timezone']):
                # Import scheduler service to recalculate next_run
                from ..services.scheduler import calculate_next_run

                # Get current values or updated ones
                enabled = update_data.get('enabled', setting.enabled)
                schedule_type = update_data.get('schedule_type', setting.schedule_type)
                hour = update_data.get('hour', setting.hour)
                minute = update_data.get('minute', setting.minute)
                weekdays = update_data.get('weekdays', setting.weekdays)
                timezone = update_data.get('timezone', setting.timezone)
                task_config = update_data.get('task_config', setting.task_config or {})

                if enabled:
                    next_run = calculate_next_run(
                        schedule_type=schedule_type,
                        hour=hour,
                        minute=minute,
                        weekdays=weekdays,
                        timezone=timezone,
                        interval_minutes=task_config.get('interval_minutes', 30)
                    )
                    update_data['next_run'] = next_run
                else:
                    update_data['next_run'] = None

            await db.execute(
                update(ScheduleSettings)
                .where(ScheduleSettings.task_name == task_name)
                .values(**update_data)
            )
            await db.commit()

        # Get updated setting
        result = await db.execute(
            select(ScheduleSettings).where(ScheduleSettings.task_name == task_name)
        )
        updated_setting = result.scalar_one()

        # Return response in the format expected by frontend
        return {
            "success": True,
            "message": "Schedule updated successfully",
            "setting": {
                "id": updated_setting.id,
                "task_name": updated_setting.task_name,
                "enabled": updated_setting.enabled,
                "schedule_type": updated_setting.schedule_type,
                "hour": updated_setting.hour,
                "minute": updated_setting.minute,
                "weekdays": updated_setting.weekdays,
                "timezone": updated_setting.timezone,
                "task_config": updated_setting.task_config or {},
                "last_run": updated_setting.last_run.isoformat() if updated_setting.last_run else None,
                "last_finished_at": updated_setting.last_finished_at.isoformat() if updated_setting.last_finished_at else None,
                "last_success_at": updated_setting.last_success_at.isoformat() if updated_setting.last_success_at else None,
                "last_status": updated_setting.last_status,
                "last_error": updated_setting.last_error,
                "last_duration_seconds": float(updated_setting.last_duration_seconds) if updated_setting.last_duration_seconds is not None else None,
                "next_run": updated_setting.next_run.isoformat() if updated_setting.next_run else None,
                "is_running": updated_setting.is_running,
                "created_at": updated_setting.created_at.isoformat() if updated_setting.created_at else None,
                "updated_at": updated_setting.updated_at.isoformat() if updated_setting.updated_at else None
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to update schedule: {str(e)}")


@router.delete("/settings/{task_name}")
async def delete_schedule_setting(
    task_name: str,
    db: AsyncSession = Depends(get_db)
):
    """Delete a schedule setting for a task."""
    if settings.schedule_dispatch_mode == "service":
        if task_name not in SERVICE_OPERATIONS:
            raise HTTPException(status_code=404, detail=f"Schedule for task '{task_name}' not found")
        try:
            client = _scheduler_client()
            current = await _find_service_schedule(client, task_name)
            if current is None:
                raise HTTPException(status_code=404,
                                    detail=f"Schedule for task '{task_name}' not found")
            await client.delete_schedule(current["schedule_key"], current["revision"])
            return {"success": True, "message": f"Schedule for task '{task_name}' deleted successfully"}
        except HTTPException:
            raise
        except SchedulerServiceError as exc:
            _raise_scheduler_error(exc)
        except ValueError:
            raise HTTPException(status_code=503, detail="Scheduler service is not configured") from None
    try:
        # Check if task exists
        result = await db.execute(
            select(ScheduleSettings).where(ScheduleSettings.task_name == task_name)
        )
        setting = result.scalar_one_or_none()

        if not setting:
            raise HTTPException(status_code=404, detail=f"Schedule for task '{task_name}' not found")

        # Delete the setting
        await db.delete(setting)
        await db.commit()

        return {
            "success": True,
            "message": f"Schedule for task '{task_name}' deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to delete schedule: {str(e)}")


@router.get("/status")
async def get_schedule_status(db: AsyncSession = Depends(get_db)):
    """Get current scheduler status."""
    if settings.schedule_dispatch_mode == "service":
        try:
            mapped = await _service_settings_snapshot()
            service_status = await _scheduler_client().get_status()
            enabled_tasks = [item for item in mapped if item["enabled"]]
            running_tasks = [item for item in mapped if item["is_running"]]
            return {"enabled_tasks": len(enabled_tasks),
                    "running_tasks": len(running_tasks),
                    "tasks": [{key: item[key] for key in (
                        "task_name", "enabled", "is_running", "last_run", "last_finished_at",
                        "last_success_at", "last_status", "last_error", "last_duration_seconds", "next_run"
                    )} for item in enabled_tasks],
                    "timestamp": datetime.utcnow().isoformat(),
                    "scheduler_service": {
                        "paused": service_status["paused"],
                        "pending_outbox": service_status["pending_outbox"],
                        "control_revision": service_status.get("control_revision"),
                        "last_tick_at": service_status.get("last_tick_at"),
                        "last_dispatch_at": service_status.get("last_dispatch_at"),
                    }}
        except SchedulerServiceError as exc:
            _raise_scheduler_error(exc)
        except ValueError:
            raise HTTPException(status_code=503, detail="Scheduler service is not configured") from None
    try:
        # Get all enabled tasks
        result = await db.execute(
            select(ScheduleSettings).where(ScheduleSettings.enabled == True)
        )
        enabled_tasks = result.scalars().all()
        
        # Get running tasks
        running_result = await db.execute(
            select(ScheduleSettings).where(ScheduleSettings.is_running == True)
        )
        running_tasks = running_result.scalars().all()
        
        return {
            "enabled_tasks": len(enabled_tasks),
            "running_tasks": len(running_tasks),
            "tasks": [
                {
                    "task_name": task.task_name,
                    "enabled": task.enabled,
                    "is_running": task.is_running,
                    "last_run": task.last_run.isoformat() if task.last_run else None,
                    "last_finished_at": task.last_finished_at.isoformat() if task.last_finished_at else None,
                    "last_success_at": task.last_success_at.isoformat() if task.last_success_at else None,
                    "last_status": task.last_status,
                    "last_error": task.last_error,
                    "last_duration_seconds": float(task.last_duration_seconds) if task.last_duration_seconds is not None else None,
                    "next_run": task.next_run.isoformat() if task.next_run else None
                }
                for task in enabled_tasks
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")
