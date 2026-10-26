"""Worker-only execution of durable, validated background jobs."""

import asyncio
import logging
import math
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from sqlalchemy import delete, select, text, update

from ..config import settings
from ..database import AsyncSessionLocal, engine
from ..models import BackgroundJob, ScheduleSettings
from .background_jobs import (
    BackgroundJobError, BackgroundWorkerLease, HEARTBEAT_INTERVAL_SECONDS,
    IdempotencyConflict, QueueFull, StaleWorker, claim_next_job, enqueue_job_in_session,
    job_may_deliver,
    finish_job, read_worker_status,
)
from .job_fence import JobExecutionFence, current_job_fence

logger = logging.getLogger(__name__)
POLL_SECONDS = settings.worker_poll_interval_seconds
SHUTDOWN_DRAIN_SECONDS = settings.worker_shutdown_drain_seconds
RETENTION_DAYS = settings.worker_job_retention_days
RETENTION_BATCH = 100


async def worker_healthcheck(session_factory=None) -> int:
    """Return a process-check exit code from the persisted worker heartbeat."""
    if settings.background_execution_mode != "worker":
        return 1
    factory = session_factory or AsyncSessionLocal

    async def read_status():
        async with factory() as session:
            return await read_worker_status(session)

    try:
        status = await asyncio.wait_for(read_status(), timeout=5)
    except Exception:
        return 1
    return 0 if status and status.get("healthy") else 1


@asynccontextmanager
async def job_execution(fence):
    token = current_job_fence.set(fence)
    try:
        yield
    finally:
        # Child tasks inherit this object; invalidating it prevents late writes.
        fence.active = False
        current_job_fence.reset(token)


def _bounded_result(kind, value):
    """Reduce domain results to content-free scalar metrics accepted by Phase 1."""
    value = value if isinstance(value, dict) else {}
    result = {"status": "success"}
    status = value.get("status")
    article_statuses = {
        "success", "warning", "deferred", "dry_run", "completed", "rejected",
        "exhausted", "retry_wait", "settled", "incomplete", "busy",
    }
    explicit_status = status in article_statuses
    if explicit_status:
        result["status"] = status
    for name in ("success", "settled", "retryable"):
        if type(value.get(name)) is bool:
            result[name] = value[name]
    if ((value.get("success") is False or value.get("fatal_error") or value.get("errors") or value.get("error"))
            and (not explicit_status or status == "success")):
        result["status"] = "warning"
    if isinstance(value.get("errors"), list):
        result["errors_count"] = min(len(value["errors"]), 1_000_000_000)
    mappings = {
        "articles_fetched": ("articles_fetched", "total_fetched"),
        "articles_processed": ("articles_processed", "processed"),
        "articles_summarized": ("articles_summarized",),
        "articles_categorized": ("articles_categorized",),
        "articles_requeued": ("articles_requeued", "requeued_count"),
        "articles_rejected": ("articles_rejected",),
        "articles_retrying": ("articles_retrying",),
        "articles_exhausted": ("articles_exhausted",),
        "total_candidates": ("total_candidates",),
        "found_candidates": ("found_candidates",),
        "processed": ("processed",),
        "improved": ("improved",),
        "summaries_generated": ("summaries_generated",),
        "parts_sent": ("parts_sent",),
        "backup_bytes": ("backup_bytes",),
        "errors_count": ("errors_count", "error_count"),
        "duration_seconds": ("duration_seconds", "total_duration"),
        "requeued_count": ("articles_requeued", "requeued_count"),
    }
    for output, candidates in mappings.items():
        for candidate in candidates:
            metric = value.get(candidate)
            if (isinstance(metric, (int, float)) and not isinstance(metric, bool)
                    and (not isinstance(metric, float) or math.isfinite(metric))):
                bounded_metric = max(0, min(1_000_000_000, metric))
                if output == "errors_count":
                    result[output] = max(result.get(output, 0), bounded_metric)
                else:
                    result[output] = bounded_metric
                break
    if kind == "reprocess_failed":
        result["dry_run"] = bool(value.get("status") == "dry_run")
    if kind == "source_test":
        result["connection_ok"] = bool(value.get("success"))
        if not result["connection_ok"]:
            result["status"] = "warning"
    outcomes = value.get("article_outcomes")
    if isinstance(outcomes, list):
        safe_reasons = {
            "advertisement", "resource_limit", "category_mapping_failed", "missing_summary",
            "smart_filter_rejected", "extraction_challenge", "provider_timeout",
        }
        valid_outcomes = [
            {"article_id": item["article_id"], "state": item["state"],
             "reason": item.get("reason") if item.get("reason") in safe_reasons else "other"}
            for item in outcomes
            if isinstance(item, dict) and isinstance(item.get("article_id"), int)
            and item.get("state") in {"completed", "rejected", "exhausted", "pending", "retry_wait", "processing"}
        ]
        # Keep a compact sample while preserving the aggregate for all selected
        # articles. This leaves room for the 4096-byte durable result limit.
        sample_limit = 20 if len(valid_outcomes) > 1 else 1
        result["article_outcomes"] = valid_outcomes[:sample_limit]
        prior_total = value.get("article_outcomes_total")
        if isinstance(prior_total, int) and not isinstance(prior_total, bool):
            total = max(len(valid_outcomes), min(100, max(0, prior_total)))
        else:
            total = len(valid_outcomes)
        result["article_outcomes_total"] = total
        result["article_outcomes_omitted"] = max(0, total - len(result["article_outcomes"]))
        if any(item["state"] != "completed" for item in valid_outcomes) and result["status"] == "success":
            result["status"] = "warning"
    if value.get("status") == "dry_run":
        result["status"] = "dry_run"
    return result


async def execute_job(job: BackgroundJob, orchestrator, reservation=None):
    """Run a previously validated job using existing domain services."""
    kind, payload = job.kind, job.payload or {}
    if kind == "news_processing":
        result = await orchestrator.run_full_cycle(reservation=reservation)
        if result.get("fatal_error"):
            raise RuntimeError("processing cycle failed")
        return result
    if kind == "news_digest":
        config = {"run_processing": payload.get("run_processing", True),
                  "send_telegram": payload.get("send_telegram", True)}
        output = {}
        if config["run_processing"]:
            output.update(await orchestrator.run_full_cycle(reservation=reservation))
            if output.get("fatal_error"):
                raise RuntimeError("processing cycle failed")
        if config["send_telegram"]:
            digest = await orchestrator.send_telegram_digest()
            output["parts_sent"] = digest.get("parts_sent", 0)
            if digest.get("success") is not True:
                raise RuntimeError("digest delivery failed")
        return output
    if kind == "telegram_digest":
        output = await orchestrator.send_telegram_digest()
        if output.get("success") is not True:
            raise RuntimeError("digest delivery failed")
        return output
    if kind == "summaries_generate":
        return await orchestrator._generate_daily_summaries()
    if kind == "reprocess_failed":
        return await orchestrator.reprocess_failed_extractions(
            limit=payload.get("limit", 20), dry_run=payload.get("dry_run", False),
            reservation=reservation,
        )
    if kind == "article_reprocess":
        if payload.get("reextract_media"):
            from .article_media_reextract import reextract_article_media
            for article_id in payload["article_ids"]:
                try:
                    await reextract_article_media(article_id)
                except StaleWorker:
                    raise
                except Exception:
                    logger.warning(
                        "Could not refresh article media before reprocessing (article_id=%s)",
                        article_id,
                        exc_info=True,
                    )
        result = await orchestrator.process_selected_articles(
            payload["article_ids"], reservation=reservation, force=payload.get("force", True),
            reset=False, preserve_completed_stages=True,
        )
        return result
    if kind == "source_test":
        from ..models import Source
        from .source_manager import SourceManager
        source_manager = SourceManager()
        async with AsyncSessionLocal() as session:
            source = await session.get(Source, payload["source_id"])
            if source is None:
                raise ValueError("source no longer exists")
            source_snapshot = source_manager._source_test_snapshot(source)
            await session.commit()
        success = await source_manager._probe_source(source_snapshot)
        return {"success": bool(success)}
    if kind == "backup":
        from .backup_service import BackupService
        path = await BackupService().create_backup()
        return {"status": "success", "backup_bytes": path.stat().st_size}
    raise ValueError("unsupported job kind")


class DurableBackgroundWorker:
    """Single-lease worker with separate heartbeat, scheduler, and consumer tasks."""

    def __init__(self, *, stop_event=None, db_engine=None, session_factory=None):
        self.engine = db_engine or engine
        self.session_factory = session_factory or AsyncSessionLocal
        self.lease = BackgroundWorkerLease(self.engine)
        self.stop_event = stop_event or asyncio.Event()
        self.lost_event = asyncio.Event()
        self.active_task = None
        self._scheduler_task = None
        self._heartbeat_task = None
        self._cleanup_task = None
        self.orchestrator = None

    async def check_schema(self):
        """Fail closed unless all worker and scheduler metadata migrations exist."""
        last_error = None
        for attempt in range(6):
            try:
                async with self.engine.connect() as connection:
                    await connection.execute(text(
                        "SELECT id, generation, owner_id, lock_backend_pid, heartbeat_at, status "
                        "FROM background_worker_status WHERE id = 1"
                    ))
                    await connection.execute(text(
                        "SELECT job_id, attempt_generation, envelope_sha256, executor_instance_id, "
                        "state, accepted_at, started_at, finished_at, result, error_code, "
                        "legacy_completion_applied_at, legacy_alert_state "
                        "FROM background_job_executions LIMIT 0"
                    ))
                    await connection.execute(text(
                        "SELECT id, queue_position, kind, payload, idempotency_key, "
                        "coalesce_key, state, created_at, started_at, finished_at, "
                        "owner_generation, attempt_generation, initialized_at, result, error_code "
                        "FROM background_jobs LIMIT 0"
                    ))
                    await connection.execute(text(
                        "SELECT key, job_id, created_at "
                        "FROM background_job_idempotency_keys LIMIT 0"
                    ))
                    await connection.execute(text(
                        "SELECT schedule_key, schedule_revision, occurrence_id, scheduled_for "
                        "FROM background_jobs LIMIT 0"
                    ))
                    await connection.execute(text(
                        "SELECT occurrence_id, envelope_sha256, job_id, accepted_at "
                        "FROM scheduler_occurrence_receipts LIMIT 0"
                    ))
                return
            except Exception as exc:
                last_error = exc
                if attempt < 5:
                    await asyncio.sleep(min(5, 0.5 * (2 ** attempt)))
        raise RuntimeError(
            "Worker database schema is unavailable; apply database migrations before starting the worker"
        ) from last_error

    async def run(self):
        settings.validate_runtime_services()
        if settings.background_execution_mode != "worker":
            raise RuntimeError(
                "Background worker refused to start: set BACKGROUND_EXECUTION_MODE=worker"
            )
        await self.check_schema()
        if not await self.lease.acquire():
            logger.error("Another background worker holds the PostgreSQL lease")
            return 2
        queue = None
        try:
            from .database_queue import get_database_queue
            queue = get_database_queue()
            await queue.start()
            async with self.session_factory() as session:
                recovered = await self.lease.recover_jobs(session)
                await session.commit()
            logger.info("Worker lease acquired; recovered jobs: %s", recovered)

            from ..orchestrator import NewsOrchestrator
            self.orchestrator = NewsOrchestrator()
            from .operational_alerts import OperationalAlertManager
            self.alerts = OperationalAlertManager(self.orchestrator.send_operational_alert)
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name="worker-heartbeat"
            )
            # Scheduled occurrences are dispatched by the Go scheduler service.
            self._cleanup_task = asyncio.create_task(
                self._retention_loop(), name="worker-retention"
            )
            await self._consume_loop()
        finally:
            self.stop_event.set()
            for task in (self._heartbeat_task, self._cleanup_task):
                if task and not task.done():
                    task.cancel()
            for task in (self._heartbeat_task, self._cleanup_task):
                if task:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        logger.exception("Worker background task failed during shutdown")
            if self.active_task and not self.active_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(self.active_task), SHUTDOWN_DRAIN_SECONDS)
                except asyncio.TimeoutError:
                    self.active_task.cancel()
                    try:
                        await self.active_task
                    except asyncio.CancelledError:
                        pass
            try:
                if self.orchestrator is not None:
                    await self.orchestrator.stop()
            except Exception:
                logger.exception("Worker orchestrator did not stop cleanly")
            if queue is not None:
                try:
                    await queue.stop()
                except Exception:
                    logger.exception("Worker database queue did not stop cleanly")
            await self.lease.release()
        return 0

    async def _heartbeat_loop(self):
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), HEARTBEAT_INTERVAL_SECONDS)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.lease.heartbeat()
            except Exception:
                logger.exception("Worker lease heartbeat failed; cancelling active work")
                self.lost_event.set()
                if self.active_task and not self.active_task.done():
                    self.active_task.cancel()
                self.stop_event.set()
                return

    async def _consume_loop(self):
        while not self.stop_event.is_set():
            try:
                async with self.session_factory() as session:
                    job = await claim_next_job(session, self.lease)
                    await session.commit()
                if job is None:
                    await self._wait_for_work()
                    continue
                # Detach the loaded row from its claim session before dispatch.
                self.active_task = asyncio.create_task(self._run_claim(job), name=f"job-{job.id}")
                stop_waiter = asyncio.create_task(self.stop_event.wait())
                done, _ = await asyncio.wait(
                    {self.active_task, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_waiter in done and not self.active_task.done():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(self.active_task), SHUTDOWN_DRAIN_SECONDS
                        )
                    except asyncio.TimeoutError:
                        self.active_task.cancel()
                        try:
                            await self.active_task
                        except asyncio.CancelledError:
                            pass
                stop_waiter.cancel()
                try:
                    await stop_waiter
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker consumer iteration failed")
                await asyncio.sleep(POLL_SECONDS)

    async def _run_claim(self, claimed):
        fence = JobExecutionFence(self.lease, claimed.id, claimed.attempt_generation)
        state, error_code, result = "succeeded", None, None
        started = asyncio.get_running_loop().time()
        try:
            async with job_execution(fence):
                # Keep reservations as a process-local guard in addition to the DB gate.
                from ..orchestrator import reserve_processing_cycle, release_processing_cycle
                reservation = reserve_processing_cycle()
                if reservation is None:
                    result = {"status": "deferred"}
                else:
                    try:
                        await self.orchestrator.start()
                        timeout_seconds = await self._job_timeout_seconds(claimed)
                        run = execute_job(claimed, self.orchestrator, reservation)
                        value = await asyncio.wait_for(run, timeout_seconds) if timeout_seconds else await run
                        result = _bounded_result(claimed.kind, value)
                        if claimed.kind == "source_test" and not result.get("connection_ok"):
                            state, error_code = "failed", "worker_error"
                    finally:
                        # The tracker previously persisted each update. Flush it
                        # after both successful and failed attempts, while the
                        # current attempt fence still protects the cache file.
                        from .domain_stability_tracker import get_stability_tracker
                        try:
                            tracker = await get_stability_tracker()
                            await tracker.flush_fenced_state(fence)
                        except StaleWorker:
                            raise
                        except Exception:
                            logger.warning("Could not publish fenced domain stability cache", exc_info=True)
                        release_processing_cycle(reservation)
        except asyncio.CancelledError:
            if job_may_deliver(claimed.kind, claimed.payload):
                state, error_code = "delivery_unknown", "external_outcome_unknown"
            else:
                state, error_code = "interrupted", "worker_interrupted"
            result = {"status": "warning"}
            raise
        except asyncio.TimeoutError:
            logger.warning("Background job timed out (kind=%s)", claimed.kind)
            if job_may_deliver(claimed.kind, claimed.payload):
                state, error_code = "delivery_unknown", "external_outcome_unknown"
            else:
                state, error_code = "failed", "worker_error"
            result = {"status": "warning"}
        except Exception as exc:
            logger.exception("Background job failed (kind=%s)", claimed.kind)
            if job_may_deliver(claimed.kind, claimed.payload):
                state, error_code = "delivery_unknown", "external_outcome_unknown"
            else:
                state, error_code = "failed", "worker_error"
            result = {"status": "warning"}
        finally:
            fence.active = False
            duration = asyncio.get_running_loop().time() - started
            if self.lease.owned and not self.lost_event.is_set():
                try:
                    # Execution results are normalized before reaching this
                    # block; exception and cancellation results are fixed safe
                    # literals. Avoid normalizing twice because doing so can
                    # reinterpret already-normalized aggregate fields.
                    safe_result = dict(result or {"status": "warning"})
                    safe_result["duration_seconds"] = duration
                    async with self.session_factory() as session:
                        await finish_job(
                            session, self.lease, claimed.id, state,
                            attempt_generation=claimed.attempt_generation,
                            result=safe_result,
                            error_code=error_code,
                        )
                        if state != "interrupted":
                            await self._finish_schedule_in_session(session, claimed, state, safe_result)
                        await session.commit()
                    await self._notify_schedule(claimed, state, safe_result)
                except Exception:
                    logger.exception("Could not persist job completion")
                    self.stop_event.set()

    async def _job_timeout_seconds(self, job):
        defaults = {
            "telegram_digest": 600,
            "news_processing": 3600,
            "news_digest": 3600,
            "backup": 1800,
            "reprocess_failed": 1800,
            "article_reprocess": 3600,
            "summaries_generate": 3600,
            "source_test": 300,
        }
        timeout = defaults.get(job.kind, 3600)
        payload = job.payload or {}
        if job.schedule_key and isinstance(payload.get("timeout_seconds"), int):
            return min(24 * 3600, payload["timeout_seconds"])
        schedule_id = (job.payload or {}).get("schedule_id")
        if schedule_id:
            async with self.session_factory() as session:
                setting = await session.get(ScheduleSettings, schedule_id)
            config = setting.task_config if setting and isinstance(setting.task_config, dict) else {}
            try:
                override = int(config.get("timeout_seconds", 0))
                if override > 0:
                    timeout = min(24 * 3600, override)
            except (TypeError, ValueError):
                pass
        return timeout

    async def _notify_schedule(self, job, state, result):
        schedule_id = (job.payload or {}).get("schedule_id")
        schedule_key = getattr(job, "schedule_key", None)
        if (not schedule_id and not schedule_key) or not getattr(self, "alerts", None):
            return
        status = (result or {}).get("status")
        alert_key = f"scheduler-warning-{schedule_key or schedule_id}"
        if state != "succeeded" or status in {"warning", "deferred"}:
            await self.alerts.failure(
                alert_key,
                f"Scheduled task {job.kind} needs attention",
                "The scheduled task failed, was deferred, or completed with warnings.",
            )
        else:
            await self.alerts.recovery(
                alert_key,
                f"Scheduled task {job.kind} recovered",
                "The latest scheduled run completed without warnings.",
            )

    async def _wait_for_work(self):
        try:
            await asyncio.wait_for(self.stop_event.wait(), POLL_SECONDS)
        except asyncio.TimeoutError:
            return

    async def _retention_loop(self):
        """Prune expired terminal jobs while the worker owns the lease."""
        while not self.stop_event.is_set():
            try:
                await self._prune_terminal_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Could not prune expired terminal jobs")
            try:
                await asyncio.wait_for(self.stop_event.wait(), 3600)
            except asyncio.TimeoutError:
                pass

    async def _prune_terminal_jobs(self):
        cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
        async with self.session_factory() as session:
            await self.lease.assert_generation(session, lock_row=True)
            stale = (await session.execute(select(BackgroundJob.id).where(
                BackgroundJob.state.in_(("succeeded", "failed")),
                BackgroundJob.finished_at < cutoff,
            ).order_by(BackgroundJob.finished_at).limit(RETENTION_BATCH))).scalars().all()
            if stale:
                await session.execute(delete(BackgroundJob).where(BackgroundJob.id.in_(stale)))
            await session.commit()

    async def _finish_schedule_in_session(self, session, job, state, result):
        # The Go service owns this schedule's next due time and configuration.
        # News records execution only on the job; it never writes the legacy row.
        if getattr(job, "schedule_key", None):
            return
        payload = job.payload or {}
        schedule_id = payload.get("schedule_id")
        if not schedule_id:
            return
        now = datetime.utcnow()
        await self.lease.assert_generation(session, lock_row=True)
        setting = await session.get(ScheduleSettings, schedule_id, with_for_update=True)
        if setting is None or not setting.is_running or (
            job.created_at and setting.updated_at and setting.updated_at > job.created_at
        ):
            return
        setting.is_running = False
        setting.last_finished_at = now
        status = (result or {}).get("status")
        setting.last_status = "delivery_unknown" if state == "delivery_unknown" else (
            "success" if state == "succeeded" and status not in {"warning", "deferred"} else (
                status if status in {"warning", "deferred"} else "failed"))
        setting.last_error = "Background job failed" if state != "succeeded" else None
        setting.last_duration_seconds = (result or {}).get("duration_seconds")
        if setting.last_status == "success":
            setting.last_success_at = now
        if setting.enabled:
            if setting.last_status == "deferred":
                setting.next_run = now + timedelta(minutes=5)
            elif setting.last_status == "failed":
                setting.next_run = now + timedelta(minutes=10)
        else:
            setting.next_run = None


async def _run_worker():
    worker = DurableBackgroundWorker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop_event.set)
        except NotImplementedError:
            pass
    try:
        return await worker.run()
    finally:
        await worker.engine.dispose()


def main():
    try:
        code = asyncio.run(_run_worker())
    except Exception:
        logger.exception("Background worker failed readiness or startup")
        code = 1
    raise SystemExit(code)
