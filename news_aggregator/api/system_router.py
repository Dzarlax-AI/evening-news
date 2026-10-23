"""System API router - handles system operations and health checks."""

import subprocess
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db, engine
# Migration manager will be imported dynamically to avoid circular imports


router = APIRouter()


@router.get("/health/db")
async def health_db():
    """Database health and pool metrics."""
    # Pool metrics
    pool_status = "unavailable"
    pool_details = {}
    
    try:
        pool = engine.pool
        pool_status = pool.status()
        pool_details = {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid()
        }
        
        # Test simple database query
        async with engine.begin() as conn:
            result = await conn.execute("SELECT 1")
            test_result = result.scalar()
            
        return {
            "ok": True,
            "pool": {
                "status": str(pool_status),
                **pool_details
            },
            "test_query": test_result == 1,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "ok": False, 
            "pool": {"status": str(pool_status), **pool_details}, 
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/process-monitor")
async def get_process_monitor_status():
    """Get process monitor status and running processes."""
    try:
        from ..config import settings
        if settings.background_execution_mode == "worker":
            from ..database import AsyncSessionLocal
            from ..services.background_jobs import read_worker_status
            async with AsyncSessionLocal() as session:
                worker = await read_worker_status(session)
            return {"status": {"running": bool(worker and worker["healthy"]),
                               "mode": "worker", "worker_available": bool(worker and worker["healthy"])},
                    "timestamp": datetime.utcnow().isoformat()}
        from ..services.process_monitor import ProcessMonitor
        
        monitor = ProcessMonitor()
        status = await monitor.get_status()
        
        return {
            "status": status,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.post("/process-monitor/cleanup")
async def cleanup_processes():
    """Manually trigger process cleanup."""
    from ..config import settings
    if settings.background_execution_mode == "worker":
        raise HTTPException(status_code=409, detail="Process cleanup is owned by the background worker")
    try:
        from ..services.process_monitor import ProcessMonitor
        
        monitor = ProcessMonitor()
        result = await monitor.cleanup_zombie_processes()
        
        return {
            "message": "Process cleanup completed",
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")


@router.post("/migrations/run")
async def run_migrations(db: AsyncSession = Depends(get_db)):
    """Run pending database migrations."""
    try:
        # Run migrations
        result = await migration_manager.run_pending_migrations()
        
        return {
            "message": "Migrations completed",
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Migrations failed: {str(e)}")


@router.get("/info")
async def get_system_info():
    """Get system information."""
    try:
        # Get Python version
        import sys
        python_version = sys.version
        
        # Get git commit info if available
        git_info = "unknown"
        try:
            git_info = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=".",
                stderr=subprocess.DEVNULL
            ).decode().strip()
        except:
            pass
        
        # Get disk usage
        import shutil
        disk_usage = shutil.disk_usage(".")
        
        return {
            "python_version": python_version,
            "git_commit": git_info,
            "disk_usage": {
                "total": disk_usage.total,
                "used": disk_usage.used,
                "free": disk_usage.free,
                "usage_percent": (disk_usage.used / disk_usage.total * 100)
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

