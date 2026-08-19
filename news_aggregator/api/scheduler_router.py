"""Scheduler API router - handles task scheduling operations."""

from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete

from ..database import get_db
from ..models import ScheduleSettings


router = APIRouter()


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
    try:
        result = await db.execute(select(ScheduleSettings))
        settings = result.scalars().all()

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
                for setting in settings
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
