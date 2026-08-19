"""Background task scheduler service."""

import asyncio
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

import pytz
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ..database_helpers import fetch_all, execute_custom_write
from ..models import ScheduleSettings
from ..orchestrator import NewsOrchestrator
from .operational_alerts import OperationalAlertManager


logger = logging.getLogger(__name__)


def calculate_next_run(
    schedule_type: str,
    hour: int,
    minute: int,
    weekdays: list,
    timezone: str,
    interval_minutes: int = 30
) -> Optional[datetime]:
    """Calculate the next run time for a scheduled task.

    Args:
        schedule_type: Type of schedule ('daily', 'hourly', 'interval')
        hour: Hour for daily schedule (0-23)
        minute: Minute for daily/hourly schedule (0-59)
        weekdays: List of weekday numbers (1=Monday, 7=Sunday)
        timezone: Timezone string (e.g., 'Europe/Belgrade')
        interval_minutes: Minutes between runs for interval schedule

    Returns:
        Next run datetime in UTC, or None if error
    """
    try:
        tz = pytz.timezone(timezone)
        now = datetime.now(tz)

        if schedule_type == "daily":
            # Next run at specified time
            next_run = now.replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0
            )

            # If time has passed today, schedule for tomorrow
            if next_run <= now:
                next_run += timedelta(days=1)

            # Check weekdays if specified
            if weekdays:
                while next_run.isoweekday() not in weekdays:
                    next_run += timedelta(days=1)

        elif schedule_type == "hourly":
            # Next run at specified minute of next hour
            next_run = now.replace(minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(hours=1)

            # Check weekdays if specified
            if weekdays and next_run.isoweekday() not in weekdays:
                # Skip to next valid weekday
                while next_run.isoweekday() not in weekdays:
                    next_run += timedelta(days=1)
                next_run = next_run.replace(hour=0, minute=minute)

        elif schedule_type == "interval":
            # Generic interval in minutes
            interval_minutes = max(1, min(interval_minutes, 24*60))
            next_run = now + timedelta(minutes=interval_minutes)

        else:
            logger.warning(f"Unknown schedule type: {schedule_type}")
            return None

        return next_run.astimezone(pytz.UTC).replace(tzinfo=None)

    except Exception as e:
        logger.error(f"Error calculating next run: {e}", exc_info=True)
        return None


class TaskScheduler:
    """Background task scheduler."""
    
    def __init__(self):
        self.orchestrator = NewsOrchestrator()
        self.alerts = OperationalAlertManager(self.orchestrator.send_operational_alert)
        self.running = False
        self._tasks: Dict[str, asyncio.Task] = {}
        self._check_interval = max(
            1, int(os.getenv("SCHEDULER_CHECK_INTERVAL_SECONDS", "60"))
        )
        self._reset_counter = 0
        self._reset_every = max(
            1, int(os.getenv("SCHEDULER_RESET_EVERY_CHECKS", "10"))
        )
        self._stuck_hours = max(1, int(os.getenv("SCHEDULER_STUCK_HOURS", "4")))
        self._task_timeout_seconds = max(
            0, int(os.getenv("SCHEDULER_TASK_TIMEOUT_SECONDS", "3600"))
        )
        self._max_concurrent_tasks = max(
            1, int(os.getenv("SCHEDULER_MAX_CONCURRENT_TASKS", "1"))
        )
        self._task_semaphore = asyncio.Semaphore(self._max_concurrent_tasks)
        # Maps setting_id → running asyncio.Task so stuck checker can cancel them
        self._task_handles: Dict[int, asyncio.Task] = {}
        
    async def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
            
        self.running = True
        logger.info("Starting task scheduler")
        
        # NOTE: Do NOT start orchestrator's queue - use global queue from main.py
        # The orchestrator will use the global database queue automatically
        
        # Start main scheduler loop
        self._tasks['main'] = asyncio.create_task(self._scheduler_loop())
        
    async def stop(self):
        """Stop the scheduler."""
        if not self.running:
            return
            
        logger.info("Stopping task scheduler")
        self.running = False
        
        # Cancel all running tasks
        for task_name, task in self._tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._tasks.clear()
        
        # NOTE: Do NOT stop orchestrator's queue - it's managed globally by main.py
        
    async def _scheduler_loop(self):
        """Main scheduler loop."""
        logger.info("Scheduler loop started")

        # On startup reset ALL is_running tasks — asyncio tasks don't survive restarts
        await self._reset_stuck_tasks(startup=True)

        while self.running:
            try:
                self._reset_counter += 1
                if self._reset_counter >= self._reset_every:
                    self._reset_counter = 0
                    await self._reset_stuck_tasks()

                await self._check_and_run_tasks()
                await asyncio.sleep(self._check_interval)

            except asyncio.CancelledError:
                logger.info("Scheduler loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                await self.alerts.failure(
                    "scheduler-loop",
                    "Ошибка цикла scheduler",
                    f"{type(e).__name__}: {e}",
                )
                await asyncio.sleep(self._check_interval)

    async def _reset_stuck_tasks(self, startup: bool = False):
        """Reset tasks that are stuck in running state for too long.

        On startup (startup=True) ALL is_running tasks are reset unconditionally,
        because asyncio tasks cannot survive a process restart.  During normal
        periodic checks only tasks running longer than _stuck_hours are reset.
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=self._stuck_hours)
            label = "startup" if startup else f"running > {self._stuck_hours}h"
            logger.info(f"Checking for stuck scheduler tasks ({label})...")

            async def reset_operation(session):
                from sqlalchemy import update

                if startup:
                    # Reset every task currently marked as running — no time threshold
                    query = select(ScheduleSettings).where(
                        ScheduleSettings.is_running == True
                    )
                else:
                    query = select(ScheduleSettings).where(
                        ScheduleSettings.is_running == True,
                        or_(
                            ScheduleSettings.last_run < cutoff_time,
                            ScheduleSettings.last_run.is_(None),
                        )
                    )

                result = await session.execute(query)
                stuck_tasks = result.scalars().all()

                if stuck_tasks:
                    logger.warning(f"Found {len(stuck_tasks)} stuck tasks ({label}):")
                    for task in stuck_tasks:
                        logger.warning(
                            f"Resetting stuck task: {task.task_name} "
                            f"(last_run={task.last_run})"
                        )

                    # Cancel in-process asyncio tasks BEFORE resetting DB state
                    for task in stuck_tasks:
                        handle = self._task_handles.pop(task.id, None)
                        if handle and not handle.done():
                            logger.warning(
                                f"Cancelling hung asyncio task for {task.task_name}"
                            )
                            handle.cancel()

                    # On startup: reset is_running, keep next_run as-is
                    #   (past next_run → fires immediately on first check)
                    # On stuck detection: also bump next_run to now+10min to
                    #   avoid hammering a repeatedly-failing task
                    retry_next_run = datetime.utcnow() + timedelta(minutes=10)

                    if startup:
                        stmt = update(ScheduleSettings).where(
                            ScheduleSettings.is_running == True
                        ).values(
                            is_running=False,
                            last_finished_at=datetime.utcnow(),
                            last_status="interrupted",
                            last_error="Service restarted while task was marked running",
                        )
                    else:
                        stmt = update(ScheduleSettings).where(
                            ScheduleSettings.is_running == True,
                            or_(
                                ScheduleSettings.last_run < cutoff_time,
                                ScheduleSettings.last_run.is_(None),
                            )
                        ).values(
                            is_running=False,
                            next_run=retry_next_run,
                            last_finished_at=datetime.utcnow(),
                            last_status="stuck",
                            last_error=f"Task exceeded {self._stuck_hours} hour stuck threshold",
                        )

                    await session.execute(stmt)
                    # No explicit commit — execute_custom_write commits automatically
                    logger.warning(f"Reset {len(stuck_tasks)} stuck tasks")
                    return [(task.task_name, task.id) for task in stuck_tasks]
                else:
                    logger.info("No stuck tasks found")
                    return []

            stuck_tasks = await execute_custom_write(reset_operation)
            if not startup:
                for task_name, setting_id in stuck_tasks or []:
                    await self.alerts.failure(
                        f"scheduler-task-{setting_id}",
                        f"Зависла задача {task_name}",
                        f"Задача выполнялась более {self._stuck_hours} ч. Она остановлена и будет повторена позже.",
                    )

        except Exception as e:
            logger.error(f"Error checking stuck tasks: {e}", exc_info=True)
            await self.alerts.failure(
                "scheduler-stuck-check",
                "Ошибка проверки зависших задач",
                f"{type(e).__name__}: {e}",
            )

                
    async def _check_and_run_tasks(self):
        """Check for tasks that need to run and execute them."""
        try:
            # Trim completed tasks to avoid unbounded growth
            if self._tasks:
                self._tasks = {
                    name: task for name, task in self._tasks.items() if not task.done()
                }

            # Get all enabled schedule settings (quick DB query through read queue)
            query = select(ScheduleSettings).where(
                ScheduleSettings.enabled == True,
                ScheduleSettings.is_running == False
            )
            settings = await fetch_all(query)
                
            now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
            
            # Debug output every 10 minutes or when tasks are overdue
            if now_utc.minute % 10 == 0 or any(now_utc >= (s.next_run.replace(tzinfo=pytz.UTC) if s.next_run and s.next_run.tzinfo is None else s.next_run.astimezone(pytz.UTC) if s.next_run else now_utc) for s in settings if s.next_run):
                logger.info(f"Scheduler check at {now_utc}: {len(settings)} tasks available")
                for setting in settings:
                    if setting.next_run:
                        next_run_utc = setting.next_run.replace(tzinfo=pytz.UTC) if setting.next_run.tzinfo is None else setting.next_run.astimezone(pytz.UTC)
                        should_run = now_utc >= next_run_utc
                        delay = (now_utc - next_run_utc).total_seconds() / 60 if should_run else (next_run_utc - now_utc).total_seconds() / 60
                        status = "OVERDUE" if should_run else f"in {delay:.1f}m"
                        logger.info(f"  {setting.task_name}: {status}")
            for setting in settings:
                # Check if task should run
                should_run = await self._should_run_task(setting, now_utc)
                
                if should_run:
                    try:
                        # Wait longer for semaphore to avoid skipping important tasks
                        await asyncio.wait_for(self._task_semaphore.acquire(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Scheduler capacity reached (%s max); delaying %s",
                            self._max_concurrent_tasks,
                            setting.task_name,
                        )
                        await self.alerts.failure(
                            "scheduler-capacity",
                            "Scheduler достиг лимита задач",
                            f"Задача {setting.task_name} отложена: одновременно выполняется максимум {self._max_concurrent_tasks}.",
                        )
                        continue

                    logger.info(f"Running scheduled task: {setting.task_name}")
                    
                    # Mark as running through write queue
                    async def mark_running_operation(session):
                        from sqlalchemy import update
                        stmt = update(ScheduleSettings).where(
                            ScheduleSettings.id == setting.id
                        ).values(
                            is_running=True,
                            last_run=now_utc.replace(tzinfo=None)
                        )
                        await session.execute(stmt)
                        await session.commit()
                        
                    try:
                        await execute_custom_write(mark_running_operation)
                    except Exception:
                        self._task_semaphore.release()
                        raise
                    
                    # Run task in background, track handle for stuck-task cancellation
                    task_key = f"task_{setting.task_name}_{setting.id}"
                    try:
                        t = asyncio.create_task(
                            self._run_task(setting.task_name, setting.task_config, setting.id)
                        )
                        self._tasks[task_key] = t
                        self._task_handles[setting.id] = t
                    except Exception:
                        self._task_semaphore.release()
                        raise
                        
        except Exception as e:
            logger.error(f"Error checking tasks: {e}", exc_info=True)
            await self.alerts.failure(
                "scheduler-task-check",
                "Ошибка проверки расписания",
                f"{type(e).__name__}: {e}",
            )
            
    async def _should_run_task(self, setting: ScheduleSettings, now_utc: datetime) -> bool:
        """Check if a task should run based on its schedule."""
        try:
            if not setting.next_run:
                return False
                
            # Convert next_run to UTC for comparison
            next_run_utc = setting.next_run
            if next_run_utc.tzinfo is None:
                next_run_utc = next_run_utc.replace(tzinfo=pytz.UTC)
            elif next_run_utc.tzinfo != pytz.UTC:
                next_run_utc = next_run_utc.astimezone(pytz.UTC)
                
            should_run = now_utc >= next_run_utc
            
            # Debug logging for overdue tasks
            if should_run:
                delay_minutes = (now_utc - next_run_utc).total_seconds() / 60
                if delay_minutes > 5:  # If more than 5 minutes late
                    logger.info(f"Task {setting.task_name} is {delay_minutes:.1f} minutes overdue")
            return should_run
            
        except Exception as e:
            logger.error(f"Error checking if task {setting.task_name} should run: {e}", exc_info=True)
            return False
        
    async def _run_task(self, task_name: str, task_config: Dict[str, Any], setting_id: int):
        """Run a specific task."""
        start_time = datetime.utcnow()
        timed_out = False
        cancelled = False
        status = "success"
        error_message = None
        try:
            logger.info(f"Executing task: {task_name}")

            # Per-task default timeouts (seconds). Can be overridden via task_config.
            _default_timeouts = {
                "telegram_digest": 600,    # 10 min — only Telegram API calls
                "news_processing": 3600,   # 60 min — fetch + Playwright + AI
                "news_digest": 3600,       # 60 min — full cycle
                "backup": 1800,            # 30 min
                "reprocess_failed": 1800,  # 30 min
            }
            timeout_seconds = _default_timeouts.get(task_name, self._task_timeout_seconds)
            if isinstance(task_config, dict):
                try:
                    override = int(task_config.get("timeout_seconds", 0))
                    if override > 0:
                        timeout_seconds = override
                except Exception:
                    pass

            task_coro = None
            if task_name == "telegram_digest":
                task_coro = self._run_telegram_digest(task_config)
            elif task_name == "news_processing":
                task_coro = self._run_news_processing(task_config)
            elif task_name == "backup":
                task_coro = self._run_backup_task(task_config)
            elif task_name == "reprocess_failed":
                task_coro = self._run_reprocess_failed(task_config)
            elif task_name == "news_digest":
                task_coro = self._run_news_digest_cycle(task_config)
            else:
                raise ValueError(f"Unknown task type: {task_name}")

            if timeout_seconds > 0:
                task_result = await asyncio.wait_for(task_coro, timeout=timeout_seconds)
            else:
                task_result = await task_coro

            if isinstance(task_result, dict) and task_result.get("status") == "warning":
                status = "warning"
                error_message = str(task_result.get("error") or "Task completed with warnings")
                await self.alerts.failure(
                    f"scheduler-warning-{setting_id}",
                    f"Задача {task_name} завершилась с предупреждениями",
                    error_message,
                )
            else:
                await self.alerts.recovery(
                    f"scheduler-warning-{setting_id}",
                    f"Задача {task_name} восстановилась",
                    "Последний запуск завершился без предупреждений.",
                )

            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Task completed with status {status}: {task_name} (took {duration:.1f}s)")

        except asyncio.CancelledError:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.warning(
                f"Task {task_name} was cancelled after {duration:.1f}s "
                f"(stuck-task recovery)"
            )
            cancelled = True
            status = "cancelled"
            error_message = "Task was cancelled by stuck-task recovery"
            raise

        except asyncio.TimeoutError:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error(
                f"Task {task_name} timed out after {duration:.1f}s "
                f"(limit={timeout_seconds}s); will retry in 10 minutes"
            )
            timed_out = True
            status = "timeout"
            error_message = f"Timed out after {duration:.1f}s (limit={timeout_seconds}s)"
            await self.alerts.failure(
                f"scheduler-task-{setting_id}",
                f"Таймаут задачи {task_name}",
                error_message,
            )

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"Error running task {task_name} after {duration:.1f}s: {e}", exc_info=True)
            status = "failed"
            error_message = f"{type(e).__name__}: {e}"
            await self.alerts.failure(
                f"scheduler-task-{setting_id}",
                f"Ошибка задачи {task_name}",
                error_message,
            )

        finally:
            self._task_handles.pop(setting_id, None)
            if cancelled:
                # Stuck checker already reset DB state (is_running + next_run).
                # Just release the semaphore — do NOT await anything here because
                # awaiting inside a CancelledError finally is unreliable.
                self._task_semaphore.release()
            else:
                try:
                    await self._update_task_schedule(
                        setting_id,
                        retry_immediately=timed_out,
                        status=status,
                        error=error_message,
                        duration_seconds=(datetime.utcnow() - start_time).total_seconds(),
                    )
                    if status in {"success", "warning"}:
                        await self.alerts.recovery(
                            f"scheduler-task-{setting_id}",
                            f"Задача {task_name} восстановилась",
                            f"Последний запуск завершён со статусом {status}.",
                        )
                except Exception as e:
                    logger.error(
                        f"Error updating schedule for {task_name}: {e}", exc_info=True
                    )
                    await self.alerts.failure(
                        f"scheduler-persistence-{setting_id}",
                        f"Не сохранён статус задачи {task_name}",
                        f"{type(e).__name__}: {e}",
                    )
                finally:
                    self._task_semaphore.release()
            
    async def _run_telegram_digest(self, config: Dict[str, Any]):
        """Run telegram digest task using unified orchestrator logic."""
        try:
            logger.info("Starting telegram digest task...")
            
            # Use the same logic as button-triggered digest for consistency
            result = await self.orchestrator.send_telegram_digest()
            
            if result.get('success'):
                parts_sent = result.get('parts_sent', 0)
                logger.info(f"Telegram digest sent successfully ({parts_sent} parts)")
            else:
                error = result.get('error') or 'unknown Telegram error'
                raise RuntimeError(f"Telegram digest was not delivered: {error}")
            return result
                
        except Exception as e:
            logger.error(f"Error in telegram digest task: {e}", exc_info=True)
            raise
                
    async def _run_news_processing(self, config: Dict[str, Any]):
        """Run news processing task."""
        try:
            logger.info("Starting news processing cycle...")
            stats = await self.orchestrator.run_full_cycle()
            processed = stats.get('articles_processed', 0)
            logger.info(f"News processing completed: {processed} articles processed")
            errors = stats.get('errors') or []
            if errors:
                return {
                    "status": "warning",
                    "error": f"Ошибок источников/обработки: {len(errors)}. " + "; ".join(map(str, errors[:5])),
                }
            return {"status": "success"}
        except Exception as e:
            logger.error(f"News processing failed: {e}", exc_info=True)
            raise
        
    async def _run_news_digest_cycle(self, config: Dict[str, Any]):
        """Run complete news digest cycle using unified orchestrator logic."""
        logger.info("Starting complete news digest cycle")
        
        try:
            # Step 1: Run full news processing cycle (if enabled)
            if config.get('run_processing', True):
                processing_stats = await self.orchestrator.run_full_cycle()
                logger.info(f"News processing completed: {processing_stats.get('articles_processed', 0)} articles processed")
            
            # Step 2: Send digest using unified logic (if enabled)
            if config.get('send_telegram', True):
                digest_result = await self.orchestrator.send_telegram_digest()
                
                if digest_result.get('success'):
                    logger.info(f"Digest sent successfully ({digest_result.get('parts_sent', 0)} parts)")
                else:
                    error = digest_result.get('error') or 'unknown Telegram error'
                    raise RuntimeError(f"Telegram digest was not delivered: {error}")
            
            logger.info("Complete news digest cycle finished successfully")
            processing_errors = (processing_stats.get('errors') or []) if config.get('run_processing', True) else []
            if processing_errors:
                return {
                    "status": "warning",
                    "error": f"Ошибок источников/обработки: {len(processing_errors)}. " + "; ".join(map(str, processing_errors[:5])),
                }
            return {"status": "success"}
                
        except Exception as e:
            logger.error(f"Error in news digest cycle: {e}", exc_info=True)
            raise

    async def _update_task_schedule(
        self,
        setting_id: int,
        retry_immediately: bool = False,
        status: str = "success",
        error: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ):
        """Update task schedule after completion.

        retry_immediately=True: schedule next_run 10 minutes from now
        (used after timeout so the task retries quickly instead of waiting
        until the next normal schedule slot).
        """
        try:
            async def update_operation(db: AsyncSession):
                result = await db.execute(
                    select(ScheduleSettings).where(ScheduleSettings.id == setting_id)
                )
                setting = result.scalar_one_or_none()

                if not setting:
                    return

                setting.is_running = False
                setting.last_finished_at = datetime.utcnow()
                if status == "success":
                    setting.last_success_at = setting.last_finished_at
                setting.last_status = status
                setting.last_error = error[:4000] if error else None
                setting.last_duration_seconds = duration_seconds

                if setting.enabled:
                    if retry_immediately:
                        setting.next_run = datetime.utcnow() + timedelta(minutes=10)
                        logger.warning(
                            f"Task {setting.task_name} timed out; "
                            f"retrying at {setting.next_run} UTC"
                        )
                    else:
                        next_run = await self._calculate_next_run(setting)
                        if next_run is None:
                            logger.error(
                                f"_calculate_next_run returned None for {setting.task_name} "
                                f"(type={setting.schedule_type}, tz={setting.timezone}); "
                                f"task will not fire again until manually re-enabled"
                            )
                        setting.next_run = next_run
                else:
                    setting.next_run = None

                # No explicit commit — execute_custom_write commits automatically
                logger.info(f"Updated schedule for {setting.task_name}, next run: {setting.next_run}")
                return setting

            await execute_custom_write(update_operation)
            
        except Exception as e:
            logger.error(f"Error updating task schedule: {e}", exc_info=True)
            raise
            
    async def _calculate_next_run(self, setting: ScheduleSettings) -> Optional[datetime]:
        """Calculate the next run time for a task."""
        try:
            interval_minutes = 30
            if setting.schedule_type == "interval":
                try:
                    interval_minutes = int((setting.task_config or {}).get('interval_minutes', 30))
                except Exception:
                    pass
            return calculate_next_run(
                schedule_type=setting.schedule_type,
                hour=setting.hour,
                minute=setting.minute,
                weekdays=setting.weekdays or [],
                timezone=setting.timezone,
                interval_minutes=interval_minutes,
            )
        except Exception as e:
            logger.error(f"Error calculating next run: {e}", exc_info=True)
            return None
    
    async def _run_backup_task(self, config: Dict[str, Any]):
        """Run backup task."""
        try:
            logger.info("Starting scheduled backup")
            
            # Get project root directory
            project_root = Path(__file__).parent.parent.parent
            backup_script = project_root / "scripts" / "backup.sh"
            
            if not backup_script.exists():
                raise FileNotFoundError(f"Backup script not found: {backup_script}")
            
            # Run backup script
            process = await asyncio.create_subprocess_exec(
                str(backup_script),
                cwd=str(project_root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                logger.info(f"Scheduled backup completed successfully: {stdout.decode()}")
                
                # Cleanup old backups if configured
                keep_days = config.get('keep_days', 30)
                await self._cleanup_old_backups(project_root, keep_days)
                return {"status": "success"}
                
            else:
                raise RuntimeError(
                    f"Backup exited with code {process.returncode}: {stderr.decode()[:1000]}"
                )
                
        except Exception as e:
            logger.error(f"Error running backup task: {e}", exc_info=True)
            raise
    
    async def _cleanup_old_backups(self, project_root: Path, keep_days: int):
        """Clean up old backup files."""
        try:
            
            backups_dir = project_root / "backups"
            if not backups_dir.exists():
                return
            
            cutoff_date = datetime.utcnow() - timedelta(days=keep_days)
            
            for backup_file in backups_dir.glob("*.tar.gz"):
                if backup_file.stat().st_mtime < cutoff_date.timestamp():
                    backup_file.unlink()
                    logger.info(f"Deleted old backup: {backup_file.name}")
                    
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {e}", exc_info=True)
    
    async def _run_reprocess_failed(self, config: Dict[str, Any]):
        """Run reprocess failed extractions task."""
        try:
            logger.info("Starting failed extraction reprocessing task...")
            logger.info("Starting failed extraction reprocessing task")
            
            # Get configuration
            limit = config.get('limit', 20)  # Process max 20 articles per run by default
            dry_run = config.get('dry_run', False)
            
            logger.info(f"  Reprocessing up to {limit} failed extractions (dry_run={dry_run})")
            # Run the reprocessing
            results = await self.orchestrator.reprocess_failed_extractions(limit=limit, dry_run=dry_run)
            
            # Log results
            candidates_count = results.get('total_candidates', 0)
            processed_count = results.get('processed', 0)
            improved_count = results.get('improved', 0)
            failed_count = results.get('failed', 0)
            
            if dry_run:
                logger.info(f"  Dry run completed: {candidates_count} articles would be reprocessed")
            else:
                logger.info(f"  Reprocessing completed: {processed_count} processed, {improved_count} improved, {failed_count} failed")
                    
                # Log any errors
                errors = results.get('errors', [])
                if errors:
                    logger.info(f"  {len(errors)} errors during reprocessing")
                    for error in errors:
                        logger.warning(f"Reprocessing error: {error}")
                    return {
                        "status": "warning",
                        "error": f"Ошибок повторной обработки: {len(errors)}. " + "; ".join(map(str, errors[:5])),
                    }
            return {"status": "success"}
                        
        except Exception as e:
            logger.error(f"Error in failed extraction reprocessing task: {e}", exc_info=True)
            raise
            
    async def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        query = select(ScheduleSettings)
        settings = await fetch_all(query)
        
        return {
            "running": self.running,
            "active_tasks": len([s for s in settings if s.enabled]),
            "total_tasks": len(settings),
            "running_tasks": len([s for s in settings if s.is_running]),
            "next_runs": [
                {
                    "task_name": s.task_name,
                    "next_run": s.next_run.isoformat() if s.next_run else None,
                    "enabled": s.enabled,
                    "last_status": s.last_status,
                    "last_error": s.last_error,
                    "last_finished_at": s.last_finished_at.isoformat() if s.last_finished_at else None,
                    "last_success_at": s.last_success_at.isoformat() if s.last_success_at else None,
                    "last_duration_seconds": float(s.last_duration_seconds) if s.last_duration_seconds is not None else None,
                }
                for s in settings
            ]
        }


# Global scheduler instance
_scheduler: Optional[TaskScheduler] = None


def get_scheduler() -> TaskScheduler:
    """Get the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = TaskScheduler()
    return _scheduler


async def start_scheduler():
    """Start the global scheduler."""
    scheduler = get_scheduler()
    await scheduler.start()


async def stop_scheduler():
    """Stop the global scheduler."""
    scheduler = get_scheduler()
    await scheduler.stop()
