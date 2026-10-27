"""Terminal operational/source alert handling after Go-owned finalization."""

import asyncio
import json
import logging
import re
import uuid
import weakref
from dataclasses import dataclass
from datetime import timedelta

import aiohttp
from sqlalchemy import select, text

from ..config import settings
from ..database import AsyncSessionLocal
from ..models import BackgroundJob, BackgroundJobExecution, BackgroundWorkerStatus
from .background_jobs import HEARTBEAT_STALE_SECONDS, StaleWorker
from .domain_fence import GO_EXECUTION_GUARD_XOR, _advisory_lock_held, canonical_uuid
from .operational_alerts import OperationalAlertManager
from . import scheduled_source_alerts

logger = logging.getLogger(__name__)
_EVENT_LOCKS: weakref.WeakValueDictionary[tuple[uuid.UUID, int], asyncio.Lock] = weakref.WeakValueDictionary()
_ALERT_ACTIVE: set[tuple[uuid.UUID, int]] = set()
_ALERTS: OperationalAlertManager | None = None


class TerminalEventError(ValueError):
    pass


@dataclass(frozen=True)
class TerminalEvent:
    job_id: uuid.UUID
    attempt_generation: int
    owner_id: uuid.UUID
    generation: int
    worker_backend_pid: int
    executor_instance_id: uuid.UUID
    executor_backend_pid: int


def parse_terminal_event(body):
    required = {"version", "job_id", "attempt_generation", "owner_id", "generation", "backend_pid",
                "executor_instance_id", "executor_backend_pid"}
    if not isinstance(body, dict) or set(body) != required:
        raise TerminalEventError("terminal event fields are invalid")
    if type(body["version"]) is not int or body["version"] != 1:
        raise TerminalEventError("terminal event version is invalid")
    job_id = canonical_uuid(body["job_id"], "job_id")
    owner_id = canonical_uuid(body["owner_id"], "owner_id")
    executor_id = canonical_uuid(body["executor_instance_id"], "executor_instance_id")
    attempt = body["attempt_generation"]
    if type(attempt) is not int or attempt < 0 or attempt > 2_147_483_647:
        raise TerminalEventError("attempt_generation is invalid")
    for field in ("generation", "backend_pid", "executor_backend_pid"):
        value = body[field]
        if type(value) is not int or value <= 0 or value > 2_147_483_647:
            raise TerminalEventError(f"{field} is invalid")
    return TerminalEvent(job_id, attempt, owner_id, body["generation"], body["backend_pid"],
                         executor_id, body["executor_backend_pid"])


async def _current_go_execution_instance():
    base = (settings.execution_service_url or "").rstrip("/")
    token = settings.job_executor_token.get_secret_value() if settings.job_executor_token else ""
    if not base or not token:
        raise TerminalEventError("Go execution status is unavailable")
    timeout = aiohttp.ClientTimeout(total=4, connect=2, sock_connect=2, sock_read=4)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.get(f"{base}/internal/v1/status",
                                  headers={"Authorization": f"Bearer {token}"},
                                  allow_redirects=False) as response:
                if response.status != 200:
                    raise TerminalEventError("Go execution status is unavailable")
                data = bytearray()
                async for chunk in response.content.iter_chunked(2048):
                    if len(data) + len(chunk) > 4096:
                        raise TerminalEventError("Go execution status is invalid")
                    data.extend(chunk)
    except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
        raise TerminalEventError("Go execution status is unavailable") from exc
    try:
        body = json.loads(data)
    except (ValueError, TypeError, RecursionError) as exc:
        raise TerminalEventError("Go execution status is invalid") from exc
    if (not isinstance(body, dict) or set(body) != {"executor_instance_id", "active_executions", "ready"}
            or type(body["ready"]) is not bool or body["ready"] is not True
            or type(body["active_executions"]) is not int or body["active_executions"] < 0):
        raise TerminalEventError("Go execution service is not ready")
    return canonical_uuid(body["executor_instance_id"], "executor_instance_id")


async def _validate_terminal_db(session, event: TerminalEvent, *, lock_alert=False):
    status = (await session.execute(select(
        BackgroundWorkerStatus.owner_id, BackgroundWorkerStatus.generation,
        BackgroundWorkerStatus.status, BackgroundWorkerStatus.lock_backend_pid,
        BackgroundWorkerStatus.heartbeat_at,
    ).where(BackgroundWorkerStatus.id == 1).with_for_update())).one_or_none()
    db_now = await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'"))
    if (status is None or status.owner_id != event.owner_id or status.generation != event.generation
            or status.status != "running" or status.lock_backend_pid != event.worker_backend_pid
            or status.heartbeat_at is None or status.heartbeat_at > db_now
            or db_now - status.heartbeat_at > timedelta(seconds=HEARTBEAT_STALE_SECONDS)):
        raise StaleWorker("worker lease is no longer current")
    if not await _advisory_lock_held(session, event.worker_backend_pid, 0):
        raise StaleWorker("worker advisory lease is no longer held")
    if not await _advisory_lock_held(session, event.executor_backend_pid, GO_EXECUTION_GUARD_XOR):
        raise StaleWorker("Go execution guard is no longer held")
    job = (await session.execute(select(BackgroundJob).where(
        BackgroundJob.id == event.job_id).with_for_update())).scalar_one_or_none()
    if (job is None or job.state not in {"succeeded", "failed", "interrupted", "delivery_unknown"}
            or job.attempt_generation != event.attempt_generation):
        raise StaleWorker("job attempt is not the requested terminal attempt")
    receipt = (await session.execute(select(BackgroundJobExecution).where(
        BackgroundJobExecution.job_id == event.job_id,
        BackgroundJobExecution.attempt_generation == event.attempt_generation,
    ).with_for_update())).scalar_one_or_none()
    if (receipt is None or receipt.state != job.state or receipt.legacy_completion_applied_at is None
            or receipt.legacy_alert_state not in {"pending", "attempting", "delivered", "unknown", "not_needed"}):
        raise StaleWorker("Go finalization receipt is not ready for domain alerts")
    return job, receipt


async def _alert_sender(title, message):
    from ..models import Setting
    from .telegram_service import get_telegram_service
    try:
        async with AsyncSessionLocal() as session:
            chat_id_row = await session.get(Setting, "telegram_chat_id")
            service_chat_id_row = await session.get(Setting, "telegram_service_chat_id")
            chat_id = chat_id_row.value if chat_id_row else None
            service_chat_id = service_chat_id_row.value if service_chat_id_row else None
            await session.rollback()
        service = get_telegram_service(chat_id=chat_id or None, service_chat_id=service_chat_id or None)
    except Exception:
        logger.warning("Could not load Telegram overrides for terminal alert", exc_info=False)
        service = get_telegram_service()
    return await service.send_alert(title, message)


def _alert_manager():
    global _ALERTS
    if _ALERTS is None:
        _ALERTS = OperationalAlertManager(_alert_sender)
    return _ALERTS


def _event_lock(key: tuple[uuid.UUID, int]) -> asyncio.Lock:
    """Return the shared per-attempt lock while active callers retain it."""
    lock = _EVENT_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _EVENT_LOCKS[key] = lock
    return lock


async def process_finalized_event(event: TerminalEvent):
    """Claim a Go-marked pending alert once and never replay an uncertain send."""
    current_instance = await _current_go_execution_instance()
    if current_instance != event.executor_instance_id:
        raise TerminalEventError("terminal event executor instance is not current")
    key = (event.job_id, event.attempt_generation)
    # Keep a strong local reference while awaiting so concurrent duplicate
    # deliveries share this lock. The weak map retires it after all callers
    # leave, avoiding one permanent entry per historical job attempt.
    lock = _event_lock(key)
    async with lock:
        send_source_alert = False
        source_alert = None
        source_identity = None
        source_events = []
        should_alert = False
        alert_kind = None
        alert_key = None
        async with AsyncSessionLocal() as session:
            try:
                job, receipt = await _validate_terminal_db(session, event)
            except StaleWorker:
                await session.rollback()
                raise
            alert_state = receipt.legacy_alert_state
            if alert_state == "attempting":
                receipt.legacy_alert_state = "unknown"
                source_identity = scheduled_source_alerts.schedule_identity(job)
                if source_identity:
                    await scheduled_source_alerts.abandon_inflight(session, source_identity, job.id)
                await session.commit()
                return _event_response(event, "unknown")
            if alert_state != "pending":
                await session.rollback()
                return _event_response(event, alert_state or "not_needed")

            payload = job.payload if isinstance(job.payload, dict) else {}
            schedule_id = payload.get("schedule_id")
            schedule_key = getattr(job, "schedule_key", None)
            alert_key = f"scheduler-warning-{schedule_key or schedule_id}"
            source_identity = scheduled_source_alerts.schedule_identity(job)
            observation_valid = False
            if source_identity is not None and job.kind == "news_processing":
                try:
                    async with session.begin_nested():
                        observation = None
                        if job.state == "succeeded":
                            go_run, source_runs = await scheduled_source_alerts.read_current_go_observation(
                                session, job, event.attempt_generation,
                            )
                            observation = scheduled_source_alerts.classify_observation(job, go_run, source_runs)
                        if observation is None:
                            await scheduled_source_alerts.break_streaks(
                                session, source_identity, job.queue_position, _active_source_alert_job_ids(),
                            )
                        else:
                            previous_position = await scheduled_source_alerts.previous_scheduled_position(
                                session, job, source_identity,
                            )
                            decision = await scheduled_source_alerts.prepare_alert(
                                session, source_identity, job.queue_position, job.id, observation,
                                previous_position, _active_source_alert_job_ids(),
                            )
                            observation_valid = decision is not None
                            if decision and decision.get("event"):
                                source_alert = decision["event"]
                                source_events = decision["events"]
                                send_source_alert = True
                            elif observation_valid:
                                if (observation["warning"] is None
                                        and (job.result or {}).get("status") == "success"
                                        and _alert_manager().is_active(alert_key)):
                                    alert_kind, should_alert = "recovery", True
                except Exception as exc:
                    logger.warning("Scheduled source alert policy failed safely (job_id=%s error_type=%s)",
                                   event.job_id, type(exc).__name__)
                    observation_valid = False
                    send_source_alert = False
                    source_alert = None
                    source_events = []
                    should_alert = False
            if not observation_valid:
                result = job.result if isinstance(job.result, dict) else {}
                failure = (job.state != "succeeded" or result.get("status") in {"warning", "deferred"})
                if failure:
                    alert_kind, should_alert = "failure", True
                elif _alert_manager().is_active(alert_key):
                    alert_kind, should_alert = "recovery", True
            if not should_alert and not send_source_alert:
                receipt.legacy_alert_state = "not_needed"
                await session.commit()
                return _event_response(event, "not_needed")
            job_kind = job.kind
            receipt.legacy_alert_state = "attempting"
            await session.commit()

        _ALERT_ACTIVE.add(key)
        try:
            sent = False
            if send_source_alert:
                sent = bool(await _alert_sender(source_alert["title"], source_alert["message"]))
            elif alert_kind == "failure":
                sent = await _alert_manager().failure(
                    alert_key,
                    f"Scheduled task {job_kind} needs attention",
                    "The scheduled task failed, was deferred, or completed with warnings.",
                )
            else:
                sent = await _alert_manager().recovery(
                    alert_key,
                    f"Scheduled task {job_kind} recovered",
                    "The latest scheduled task completed without warnings.",
                )
        except Exception as exc:
            logger.warning("Terminal alert delivery outcome is unknown (job_id=%s error_type=%s)",
                           event.job_id, type(exc).__name__)
            sent = False
        finally:
            _ALERT_ACTIVE.discard(key)

        # Re-read authenticated Go status before opening any transaction; SQL
        # then verifies that this exact PID still owns the singleton lock.
        current_instance = await _current_go_execution_instance()
        if current_instance != event.executor_instance_id:
            return _event_response(event, "unknown")
        async with AsyncSessionLocal() as session:
            try:
                job, receipt = await _validate_terminal_db(session, event)
            except StaleWorker:
                await session.rollback()
                return _event_response(event, "unknown")
            if receipt.legacy_alert_state == "attempting":
                receipt.legacy_alert_state = "delivered" if sent else "unknown"
                if send_source_alert and source_identity:
                    await scheduled_source_alerts.finish_alert(
                        session, source_identity, source_events, event.job_id, bool(sent),
                    )
                await session.commit()
                return _event_response(event, receipt.legacy_alert_state)
            await session.rollback()
            return _event_response(event, receipt.legacy_alert_state or "unknown")


def _active_source_alert_job_ids():
    return frozenset(str(job_id) for job_id, _attempt in _ALERT_ACTIVE)


def _event_response(event: TerminalEvent, alert_state: str):
    return {"job_id": str(event.job_id), "attempt_generation": event.attempt_generation,
            "legacy_completion_applied": True, "alert_state": alert_state}
