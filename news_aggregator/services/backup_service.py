"""Backup service for managing backup schedules and operations."""

import asyncio
import json
import logging
import os
import re
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Any, Tuple
from urllib.parse import urlparse

import pytz
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import ScheduleSettings
from ..config import settings


logger = logging.getLogger(__name__)


class BackupService:
    """Service for managing backup schedules and operations."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.config_file = self.project_root / "backup_schedule.json"
        self.backups_dir = self.project_root / "backups"
        self.container_name = "v2-app-1"

    def _parse_database_url(self) -> Tuple[str, str, str, str, str]:
        """Parse DATABASE_URL to extract connection details."""
        db_url = settings.database_url

        # Parse URL
        parsed = urlparse(db_url)

        # Extract components
        host = parsed.hostname or "localhost"
        port = str(parsed.port or 5432)
        username = parsed.username or "root"
        password = parsed.password or ""
        database = parsed.path.lstrip('/') or "newsdb"

        return host, port, username, password, database

    async def _run_pg_dump(self, backup_file: Path) -> bool:
        """Run pg_dump via Docker container to create database backup."""
        process = None
        try:
            host, port, username, password, database = self._parse_database_url()

            logger.info(f"Creating database backup: {database} @ {host}:{port}")

            if settings.background_execution_mode == "worker":
                # The worker shares the application image, so it runs pg_dump
                # locally and passes the password through the child environment.
                cmd = [
                    "pg_dump", "-h", host, "-p", port, "-U", username,
                    "--no-owner", "--no-acl", database,
                ]
                child_env = os.environ.copy()
                child_env["PGPASSWORD"] = password
            else:
                # Keep the existing embedded deployment behavior unchanged.
                cmd = [
                    "docker", "exec", "-e", f"PGPASSWORD={password}",
                    self.container_name, "pg_dump", "-h", host, "-p", port,
                    "-U", username, "--no-owner", "--no-acl", database,
                ]
                child_env = None

            # Run pg_dump and redirect output to file
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=child_env,
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"pg_dump failed: {error_msg}")
                return False

            # Write dump to file
            backup_file.write_bytes(stdout)
            logger.info(f"Database dump saved: {backup_file} ({backup_file.stat().st_size / 1024 / 1024:.2f} MB)")

            return True

        except Exception as e:
            logger.error(f"Error running pg_dump: {e}")
            return False
        finally:
            if process is not None and process.returncode is None:
                cleanup = asyncio.create_task(self._stop_pg_dump_process(process))
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    # Finish bounded child cleanup even if the worker is draining.
                    await cleanup
                    raise

    async def _stop_pg_dump_process(self, process) -> None:
        """Terminate and reap a pg_dump child after cancellation or timeout."""
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        except Exception:
            logger.warning("Could not request pg_dump termination")
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
            return
        except asyncio.TimeoutError:
            pass
        except Exception:
            logger.warning("Could not wait for pg_dump shutdown")
        try:
            process.kill()
        except ProcessLookupError:
            pass
        except Exception:
            logger.warning("Could not force-stop pg_dump")
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except Exception:
            logger.warning("Could not reap pg_dump child after force-stop")

    async def create_backup(self) -> Path:
        """Create a complete backup including database, config, and data.

        Returns:
            Path to the created backup archive.

        Raises:
            RuntimeError: If backup creation fails.
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"backup_{timestamp}"
            backup_dir = self.backups_dir / backup_name

            # Create backup directory
            backup_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created backup directory: {backup_dir}")

            # 1. Database Backup
            logger.info("Creating database backup...")
            db_dump_file = backup_dir / "database.sql"
            if not await self._run_pg_dump(db_dump_file):
                raise RuntimeError("Database backup failed")

            # 2. Configuration Backup
            logger.info("Backing up configuration files...")
            env_file = self.project_root / ".env"
            if env_file.exists():
                import shutil
                shutil.copy2(env_file, backup_dir / ".env")
                logger.info("✅ .env copied")

            compose_file = self.project_root / "docker-compose.yml"
            if compose_file.exists():
                import shutil
                shutil.copy2(compose_file, backup_dir / "docker-compose.yml")
                logger.info("✅ docker-compose.yml copied")

            # 3. Application Data Backup
            logger.info("Backing up application data...")
            data_dir = self.project_root / "data"
            if data_dir.exists():
                import shutil
                shutil.copytree(data_dir, backup_dir / "data")
                logger.info("✅ Application data copied")

            logs_dir = self.project_root / "logs"
            if logs_dir.exists():
                import shutil
                shutil.copytree(logs_dir, backup_dir / "logs")
                logger.info("✅ Logs copied")

            # 4. Create backup metadata
            logger.info("Creating backup metadata...")
            _, _, _, _, database = self._parse_database_url()
            metadata = {
                "backup_date": datetime.utcnow().isoformat(),
                "database": database,
                "database_type": "PostgreSQL",
                "version": "v2.0",
                "host": "internal_hostname",
                "contents": [
                    "database.sql - Full PostgreSQL dump",
                    ".env - Environment configuration",
                    "docker-compose.yml - Docker configuration",
                    "data/ - Application data files",
                    "logs/ - Application logs"
                ]
            }

            metadata_file = backup_dir / "backup_info.json"
            metadata_file.write_text(json.dumps(metadata, indent=2))
            logger.info("✅ Backup metadata created")

            # 5. Create archive
            logger.info("Creating backup archive...")
            archive_name = f"news_aggregator_backup_{timestamp}.tar.gz"
            archive_path = self.backups_dir / archive_name

            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(backup_dir, arcname=backup_name)

            logger.info(f"✅ Archive created: {archive_path} ({archive_path.stat().st_size / 1024 / 1024:.2f} MB)")

            # 6. Cleanup temporary backup directory
            import shutil
            shutil.rmtree(backup_dir)
            logger.info("✅ Temporary backup directory cleaned up")

            # 7. Update last backup timestamp in config
            config = await self.get_schedule_config()
            config["last_backup"] = datetime.utcnow().isoformat()
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)

            logger.info(f"🎉 Backup completed successfully: {archive_path}")
            return archive_path

        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError(f"Backup creation failed: {e}")
    
    async def get_schedule_config(self) -> Dict[str, Any]:
        """Get backup schedule configuration."""
        try:
            if self.config_file.exists():
                with open(self.config_file) as f:
                    return json.load(f)
            else:
                return {
                    "enabled": False,
                    "schedule_time": "03:00",
                    "keep_days": 30,
                    "last_backup": None,
                    "created_at": datetime.utcnow().isoformat()
                }
        except Exception as e:
            logger.error(f"Error reading backup schedule config: {e}")
            return {
                "enabled": False,
                "schedule_time": "03:00", 
                "keep_days": 30,
                "last_backup": None,
                "error": str(e)
            }
    
    async def set_schedule_config(self, enabled: bool, schedule_time: str, keep_days: int) -> Dict[str, Any]:
        """Set backup schedule configuration."""
        try:
            config = {
                "enabled": enabled,
                "schedule_time": schedule_time,
                "keep_days": keep_days,
                "updated_at": datetime.utcnow().isoformat()
            }
            
            # Save to config file
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Update database schedule settings
            if enabled:
                await self._create_or_update_schedule_task(schedule_time, keep_days)
            else:
                await self._disable_schedule_task()
            
            logger.info(f"Backup schedule updated: enabled={enabled}, time={schedule_time}")
            return config
            
        except Exception as e:
            logger.error(f"Error setting backup schedule config: {e}")
            raise
    
    async def _create_or_update_schedule_task(self, schedule_time: str, keep_days: int):
        """Create or update backup schedule task in database."""
        from .database_queue import get_db_queue_manager

        try:
            hour, minute = map(int, schedule_time.split(':'))
            next_run = self._calculate_next_run(hour, minute)

            async def _upsert_task(db):
                result = await db.execute(
                    select(ScheduleSettings).where(
                        ScheduleSettings.task_name == "backup"
                    )
                )
                existing_task = result.scalar_one_or_none()

                if existing_task:
                    existing_task.enabled = True
                    existing_task.hour = hour
                    existing_task.minute = minute
                    existing_task.task_config = {"keep_days": keep_days}
                    existing_task.next_run = next_run
                    existing_task.updated_at = datetime.utcnow()
                    logger.info(f"Updated existing backup task: {existing_task.id}")
                else:
                    new_task = ScheduleSettings(
                        task_name="backup",
                        schedule_type="daily",
                        enabled=True,
                        hour=hour,
                        minute=minute,
                        task_config={"keep_days": keep_days},
                        next_run=next_run,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    db.add(new_task)
                    logger.info(f"Created new backup task at {schedule_time}")

            await get_db_queue_manager().execute_write(_upsert_task)

        except Exception as e:
            logger.error(f"Error creating/updating schedule task: {e}")
            raise
    
    async def _disable_schedule_task(self):
        """Disable backup schedule task in database."""
        from .database_queue import get_db_queue_manager

        try:
            async def _disable(db):
                result = await db.execute(
                    select(ScheduleSettings).where(
                        ScheduleSettings.task_name == "backup"
                    )
                )
                task = result.scalar_one_or_none()
                if task:
                    task.enabled = False
                    task.updated_at = datetime.utcnow()
                    logger.info("Disabled backup schedule task")

            await get_db_queue_manager().execute_write(_disable)

        except Exception as e:
            logger.error(f"Error disabling schedule task: {e}")
            raise
    
    def _calculate_next_run(self, hour: int, minute: int) -> datetime:
        """Calculate next run time for backup task."""
        try:
            now = datetime.utcnow().replace(tzinfo=pytz.UTC)
            
            # Set to today at specified time
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # If time has passed today, schedule for tomorrow
            if next_run <= now:
                next_run += timedelta(days=1)
            
            return next_run.replace(tzinfo=None)
            
        except Exception as e:
            logger.error(f"Error calculating next run: {e}")
            # Fallback to tomorrow at 3 AM
            return (datetime.utcnow() + timedelta(days=1)).replace(
                hour=3, minute=0, second=0, microsecond=0
            )
    
    async def get_backup_history(self) -> Dict[str, Any]:
        """Get backup history and statistics."""
        try:
            backups_dir = self.project_root / "backups"
            
            if not backups_dir.exists():
                return {
                    "total_backups": 0,
                    "total_size_mb": 0,
                    "latest_backup": None,
                    "oldest_backup": None,
                    "backups": []
                }
            
            backups = []
            total_size = 0
            
            for backup_file in backups_dir.glob("*.tar.gz"):
                stat = backup_file.stat()
                size_mb = stat.st_size / (1024 * 1024)
                total_size += size_mb
                
                backups.append({
                    "filename": backup_file.name,
                    "size_mb": round(size_mb, 2),
                    "created_at": datetime.fromtimestamp(stat.st_ctime),
                    "age_days": (datetime.utcnow() - datetime.fromtimestamp(stat.st_ctime)).days
                })
            
            # Sort by creation time
            backups.sort(key=lambda x: x["created_at"], reverse=True)
            
            return {
                "total_backups": len(backups),
                "total_size_mb": round(total_size, 2),
                "latest_backup": backups[0] if backups else None,
                "oldest_backup": backups[-1] if backups else None,
                "backups": backups[:10]  # Return latest 10
            }
            
        except Exception as e:
            logger.error(f"Error getting backup history: {e}")
            return {
                "total_backups": 0,
                "total_size_mb": 0,
                "latest_backup": None,
                "oldest_backup": None,
                "backups": [],
                "error": str(e)
            }
    
    async def cleanup_old_backups(self, keep_days: int = 30) -> Dict[str, Any]:
        """Clean up backup files older than specified days."""
        try:
            backups_dir = self.project_root / "backups"
            
            if not backups_dir.exists():
                return {"cleaned": 0, "message": "Backups directory not found"}
            
            cutoff_date = datetime.utcnow() - timedelta(days=keep_days)
            cleaned_count = 0
            cleaned_size = 0
            
            for backup_file in backups_dir.glob("*.tar.gz"):
                file_time = datetime.fromtimestamp(backup_file.stat().st_mtime)
                
                if file_time < cutoff_date:
                    file_size = backup_file.stat().st_size / (1024 * 1024)
                    backup_file.unlink()
                    cleaned_count += 1
                    cleaned_size += file_size
                    logger.info(f"Deleted old backup: {backup_file.name}")
            
            return {
                "cleaned": cleaned_count,
                "size_mb": round(cleaned_size, 2),
                "message": f"Deleted {cleaned_count} old backup files"
            }
            
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {e}")
            return {"cleaned": 0, "error": str(e)}


# Global backup service instance
_backup_service: Optional[BackupService] = None


def get_backup_service() -> BackupService:
    """Get the global backup service instance."""
    global _backup_service
    if _backup_service is None:
        _backup_service = BackupService()
    return _backup_service
