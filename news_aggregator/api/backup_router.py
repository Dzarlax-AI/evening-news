"""Backup API router - handles backup and restore operations."""

import os
from datetime import datetime
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..config import settings


router = APIRouter()


# ============================================================================
# Pydantic Models for Backup
# ============================================================================

class BackupInfo(BaseModel):
    filename: str
    size: int
    created_at: str


class BackupScheduleSettings(BaseModel):
    enabled: bool
    frequency: str  # daily, weekly, monthly
    time: str  # HH:MM format
    keep_count: int  # number of backups to keep


# ============================================================================
# Backup Endpoints
# ============================================================================

@router.get("")
async def list_backups():
    """List all available backups."""
    try:
        backup_dir = Path("backups")
        if not backup_dir.exists():
            return {"backups": [], "total": 0}
        
        backups = []
        for backup_file in backup_dir.glob("*.tar.gz"):
            stat = backup_file.stat()
            backups.append({
                "filename": backup_file.name,
                "size": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat()
            })
        
        # Sort by creation time (newest first)
        backups.sort(key=lambda x: x['created_at'], reverse=True)
        
        return {
            "backups": backups,
            "total": len(backups),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list backups: {str(e)}")


@router.get("/download/{filename}")
async def download_backup(filename: str):
    """Download a backup file."""
    try:
        backup_path = Path("backups") / filename
        
        if not backup_path.exists():
            raise HTTPException(status_code=404, detail="Backup file not found")
        
        # Validate filename to prevent directory traversal
        if ".." in filename or "/" in filename:
            raise HTTPException(status_code=400, detail="Invalid filename")
        
        return FileResponse(
            path=str(backup_path),
            filename=filename,
            media_type="application/gzip"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download backup: {str(e)}")


@router.post("")
async def create_backup(request: Request, db: AsyncSession = Depends(get_db)):
    """Create a new backup."""
    from ..config import settings
    if settings.background_execution_mode == "worker":
        from .background_jobs_router import accepted_response, enqueue_from_request
        accepted = await enqueue_from_request("backup", {}, request)
        return accepted_response(accepted)
    try:
        # Import backup service
        from ..services.backup_service import BackupService
        
        backup_service = BackupService()
        backup_path = await backup_service.create_backup()
        
        return {
            "message": "Backup created successfully",
            "backup_path": str(backup_path),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create backup: {str(e)}")


@router.post("/sync")
async def create_sync_backup(request: Request):
    """Create a synchronous backup (for testing)."""
    from ..config import settings
    if settings.background_execution_mode == "worker":
        from .background_jobs_router import accepted_response, enqueue_from_request
        accepted = await enqueue_from_request("backup", {}, request)
        return accepted_response(accepted)
    try:
        # Import backup service
        from ..services.backup_service import BackupService
        
        backup_service = BackupService()
        backup_path = await backup_service.create_backup()
        
        return {
            "message": "Sync backup created successfully",
            "backup_path": str(backup_path),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create sync backup: {str(e)}")


@router.post("/restore")
async def restore_backup(filename: str, db: AsyncSession = Depends(get_db)):
    """Restore from a backup file."""
    if settings.background_execution_mode == "worker":
        raise HTTPException(
            status_code=409,
            detail="Restore is disabled in worker mode; stop the worker and use offline maintenance.",
        )
    try:
        backup_path = Path("backups") / filename
        
        if not backup_path.exists():
            raise HTTPException(status_code=404, detail="Backup file not found")
        
        # Import backup service
        from ..services.backup_service import BackupService
        
        backup_service = BackupService()
        await backup_service.restore_backup(backup_path)
        
        return {
            "message": "Backup restored successfully",
            "restored_from": filename,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to restore backup: {str(e)}")


@router.post("/upload")
async def upload_backup(file: UploadFile = File(...)):
    """Upload a backup file."""
    if settings.background_execution_mode == "worker":
        raise HTTPException(
            status_code=409,
            detail="Backup uploads are disabled in worker mode because the web backup mount is read-only.",
        )
    try:
        # Validate file type
        if not file.filename or not file.filename.endswith('.tar.gz'):
            raise HTTPException(status_code=400, detail="Invalid file type. Only .tar.gz files are allowed.")
        
        # Ensure backups directory exists
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        
        # Save uploaded file
        backup_path = backup_dir / file.filename
        
        with open(backup_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        return {
            "message": "Backup uploaded successfully",
            "filename": file.filename,
            "size": len(content),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload backup: {str(e)}")


@router.post("/cleanup")
async def cleanup_old_backups(keep_count: int = 5):
    """Clean up old backup files, keeping only the most recent ones."""
    if settings.background_execution_mode == "worker":
        raise HTTPException(
            status_code=409,
            detail="Backup cleanup is disabled in worker mode because the web backup mount is read-only.",
        )
    try:
        backup_dir = Path("backups")
        if not backup_dir.exists():
            return {"message": "No backups directory found", "deleted": 0}
        
        # Get all backup files sorted by modification time
        backup_files = list(backup_dir.glob("*.tar.gz"))
        backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Delete files beyond keep_count
        deleted_count = 0
        for backup_file in backup_files[keep_count:]:
            backup_file.unlink()
            deleted_count += 1
        
        return {
            "message": f"Cleanup completed. Kept {min(len(backup_files), keep_count)} backups.",
            "deleted": deleted_count,
            "remaining": len(backup_files) - deleted_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup backups: {str(e)}")


@router.get("/schedule")
async def get_backup_schedule():
    """Get backup schedule settings."""
    try:
        # For now, return default settings
        # This could be stored in database or config file later
        return {
            "enabled": False,
            "frequency": "daily",
            "time": "02:00",
            "keep_count": 5,
            "last_backup": None,
            "next_backup": None,
            "status": "disabled"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get backup schedule: {str(e)}")


@router.post("/schedule")
async def update_backup_schedule(settings: BackupScheduleSettings):
    """Update backup schedule settings."""
    try:
        # For now, just return success
        # In a real implementation, this would save to database or config
        return {
            "message": "Backup schedule updated successfully",
            "settings": {
                "enabled": settings.enabled,
                "frequency": settings.frequency,
                "time": settings.time,
                "keep_count": settings.keep_count
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update backup schedule: {str(e)}")

