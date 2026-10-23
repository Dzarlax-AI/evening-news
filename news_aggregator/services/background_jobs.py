"""Durable job acceptance and PostgreSQL-fenced worker ownership.

The queue stores identifiers and flags only. Job executors load application data
from PostgreSQL after claiming a job; article content, URLs and credentials never
belong in a job payload or result.
"""

import hashlib
import json
import math
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from ..database import engine as default_engine
from ..models import (
    Article, BackgroundJob, BackgroundJobIdempotencyKey, BackgroundWorkerStatus,
    ScheduleSettings, SchedulerOccurrenceReceipt,
)

MAX_PENDING_JOBS = 32
MAX_SELECTED_IDS = 100
MAX_PAYLOAD_BYTES = 4096
MAX_RESULT_BYTES = 4096
MAX_IDEMPOTENCY_ALIASES_PER_JOB = 32
MAX_SCHEDULER_OCCURRENCE_RECEIPTS = 100_000
HEARTBEAT_INTERVAL_SECONDS = 5
HEARTBEAT_STALE_SECONDS = 20
ALLOWED_KINDS = frozenset({
    "news_processing", "news_digest", "telegram_digest", "backup",
    "reprocess_failed", "article_reprocess", "summaries_generate", "source_test",
})
ERROR_CODES = frozenset({
    "worker_error", "worker_interrupted", "worker_recovery_exhausted", "external_outcome_unknown",
    "invalid_job", "resource_limit", "database_unavailable",
})
IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+-]{0,199}$")


def _utcnow_naive() -> datetime:
    """Match this project's existing timestamp columns, which are naive UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def lock_job_producers(session: AsyncSession) -> None:
    """Serialize queue producers before inspecting receipts or the pending cap."""
    await session.execute(text("""
        SELECT pg_advisory_xact_lock(hashtext(current_database()),
                                     hashtext(current_schema()) # 1145132610)
    """))


async def is_worker_available(session: AsyncSession, now: Optional[datetime] = None) -> bool:
    """Require a fresh fenced worker heartbeat before accepting scheduler work."""
    status = await session.get(BackgroundWorkerStatus, 1)
    now = now or _utcnow_naive()
    return bool(status and status.status == "running" and status.heartbeat_at
                and now - status.heartbeat_at <= timedelta(seconds=HEARTBEAT_STALE_SECONDS))


def job_may_deliver(kind: str, payload: Optional[Dict[str, Any]]) -> bool:
    """Return whether execution may make an external digest delivery attempt."""
    if kind == "telegram_digest":
        return True
    if kind == "news_digest":
        return (payload or {}).get("send_telegram", True) is not False
    return False


async def _close_connection_releasing_lock(connection: Optional[AsyncConnection]) -> None:
    """Return a connection only after its session advisory lock is released.

    If the server or connection is unhealthy, invalidation closes the backend
    socket so PostgreSQL releases the lock instead of a pooled connection
    silently retaining ownership.
    """
    if connection is None:
        return
    try:
        try:
            await connection.rollback()
        except BaseException:
            pass
        try:
            await connection.execute(text("""
                SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(current_schema()))
            """))
            await connection.commit()
        except BaseException:
            try:
                await connection.invalidate()
            except BaseException:
                pass
    finally:
        try:
            await connection.close()
        except BaseException:
            pass


class BackgroundJobError(Exception):
    """Base class for safe, finite queue errors."""


class QueueFull(BackgroundJobError):
    pass


class IdempotencyConflict(BackgroundJobError):
    pass


class WorkerLeaseUnavailable(BackgroundJobError):
    pass


class StaleWorker(WorkerLeaseUnavailable):
    pass


def validate_payload(kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a compact identifier/flag payload and return a canonical copy."""
    if kind not in ALLOWED_KINDS:
        raise ValueError("unsupported job kind")
    if not isinstance(payload, dict):
        raise ValueError("job payload must be an object")

    allowed = {
        "news_processing": {"schedule_id", "scheduled_for", "timeout_seconds"},
        "news_digest": {"schedule_id", "scheduled_for", "run_processing", "send_telegram", "timeout_seconds"},
        "telegram_digest": {"schedule_id", "scheduled_for", "timeout_seconds"},
        "backup": {"schedule_id", "scheduled_for", "timeout_seconds"},
        "reprocess_failed": {"schedule_id", "scheduled_for", "limit", "dry_run", "timeout_seconds"},
        "article_reprocess": {"article_ids", "force", "reextract_media"},
        "summaries_generate": set(),
        "source_test": {"source_id"},
    }[kind]
    if set(payload) - allowed:
        raise ValueError("unsupported job payload field")

    normalized: Dict[str, Any] = {}
    schedule_keys = {"schedule_id", "scheduled_for"} & set(payload)
    if schedule_keys and schedule_keys != {"schedule_id", "scheduled_for"}:
        raise ValueError("schedule_id and scheduled_for must be supplied together")
    if schedule_keys:
        schedule_id, scheduled_for = payload["schedule_id"], payload["scheduled_for"]
        if isinstance(schedule_id, bool) or not isinstance(schedule_id, int) or schedule_id <= 0:
            raise ValueError("schedule_id must be a positive integer")
        if not isinstance(scheduled_for, str):
            raise ValueError("scheduled_for must be an ISO timestamp")
        try:
            parsed_due = datetime.fromisoformat(scheduled_for)
        except ValueError as exc:
            raise ValueError("scheduled_for must be an ISO timestamp") from exc
        if parsed_due.isoformat() != scheduled_for:
            raise ValueError("scheduled_for must be a canonical ISO timestamp")
        normalized.update(schedule_id=schedule_id, scheduled_for=scheduled_for)

    if "timeout_seconds" in payload:
        value = payload["timeout_seconds"]
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 24 * 60 * 60:
            raise ValueError("timeout_seconds must be an integer from 1 to 86400")
        normalized["timeout_seconds"] = value

    if kind == "news_digest":
        for key in ("run_processing", "send_telegram"):
            if key in payload:
                if not isinstance(payload[key], bool):
                    raise ValueError(f"{key} must be a boolean")
                normalized[key] = payload[key]
    if kind == "reprocess_failed":
        if "limit" in payload:
            value = payload["limit"]
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
                raise ValueError("limit must be an integer from 1 to 100")
            normalized["limit"] = value
        if "dry_run" in payload:
            if not isinstance(payload["dry_run"], bool):
                raise ValueError("dry_run must be a boolean")
            normalized["dry_run"] = payload["dry_run"]
    if kind == "article_reprocess":
        ids = payload.get("article_ids")
        if not isinstance(ids, list) or not 1 <= len(ids) <= MAX_SELECTED_IDS:
            raise ValueError("article_ids must contain 1 to 100 IDs")
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in ids):
            raise ValueError("article_ids must contain positive integer IDs")
        if len(set(ids)) != len(ids):
            raise ValueError("article_ids must not contain duplicates")
        normalized["article_ids"] = sorted(ids)
        for key, default in (("force", True), ("reextract_media", False)):
            value = payload.get(key, default)
            if not isinstance(value, bool):
                raise ValueError(f"{key} must be a boolean")
            normalized[key] = value
    if kind == "source_test":
        source_id = payload.get("source_id")
        if isinstance(source_id, bool) or not isinstance(source_id, int) or source_id <= 0:
            raise ValueError("source_id must be a positive integer")
        normalized["source_id"] = source_id

    encoded = json.dumps(normalized, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise ValueError("job payload exceeds the 4096-byte limit")
    return normalized


def _safe_result(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if result is None:
        return None
    if not isinstance(result, dict):
        raise ValueError("job result must be an object")
    encoded = json.dumps(result, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > MAX_RESULT_BYTES:
        raise ValueError("job result exceeds the 4096-byte limit")
    # Fixed scalar fields prevent content, URLs, prompts, and free-form errors
    # from being persisted even if an executor accidentally passes them.
    allowed = {
        "status", "articles_fetched", "articles_processed", "summaries_generated",
        "parts_sent", "errors_count", "duration_seconds", "backup_bytes",
        "requeued_count", "dry_run", "connection_ok", "article_outcomes",
        "article_outcomes_total", "article_outcomes_omitted",
    }
    if set(result) - allowed:
        raise ValueError("job result contains a disallowed field")
    for key, value in result.items():
        if key == "status":
            if not isinstance(value, str) or value not in {
                "success", "warning", "deferred", "dry_run", "completed",
                "rejected", "exhausted", "incomplete", "settled",
            }:
                raise ValueError("job result status is invalid")
        elif key == "article_outcomes":
            allowed_states = {"completed", "rejected", "exhausted", "pending", "retry_wait", "processing"}
            allowed_reasons = {"advertisement", "resource_limit", "category_mapping_failed", "other"}
            if not isinstance(value, list) or len(value) > MAX_SELECTED_IDS:
                raise ValueError("article outcomes must be a bounded list")
            for item in value:
                if (not isinstance(item, dict) or set(item) - {"article_id", "state", "reason"}
                        or isinstance(item.get("article_id"), bool)
                        or not isinstance(item.get("article_id"), int)
                        or item.get("article_id") <= 0
                        or item.get("state") not in allowed_states
                        or item.get("reason") not in allowed_reasons):
                    raise ValueError("article outcome contains an invalid field")
        elif key in {"dry_run", "connection_ok"}:
            if not isinstance(value, bool):
                raise ValueError(f"job result {key} must be a boolean")
        elif (isinstance(value, bool) or not isinstance(value, (int, float))
              or not math.isfinite(value) or value < 0 or value > 1_000_000_000):
            raise ValueError("job result metrics must be bounded non-negative numbers")
    return result


async def enqueue_job_in_session(
    session: AsyncSession,
    kind: str,
    payload: Dict[str, Any],
    idempotency_key: str,
    *,
    coalesce_active: bool = True,
    schedule_metadata: Optional[Dict[str, Any]] = None,
    return_created: bool = False,
) -> BackgroundJob:
    """Accept a job in the caller's transaction (used for atomic scheduler state)."""
    normalized = validate_payload(kind, payload)
    if not isinstance(idempotency_key, str) or not IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key):
        raise ValueError("idempotency key must be a safe 1-200 character token")
    # Keep only a fixed-length digest in PostgreSQL in case a caller mistakenly
    # supplies a sensitive value as its request key.
    stored_key = hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()
    canonical = json.dumps(normalized, separators=(",", ":"), sort_keys=True)
    coalesce_key = hashlib.sha256((kind + "\0" + canonical).encode()).hexdigest() if coalesce_active else None

    schedule_key = schedule_revision = occurrence_id = scheduled_for = None
    if schedule_metadata is not None:
        if (not isinstance(schedule_metadata, dict)
                or set(schedule_metadata) != {"schedule_key", "schedule_revision", "occurrence_id", "scheduled_for"}):
            raise ValueError("invalid scheduled job metadata")
        schedule_key = schedule_metadata["schedule_key"]
        schedule_revision = schedule_metadata["schedule_revision"]
        occurrence_id = schedule_metadata["occurrence_id"]
        scheduled_for = schedule_metadata["scheduled_for"]
        if (not isinstance(schedule_key, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._:-]{0,127}", schedule_key)
                or type(schedule_revision) is not int or schedule_revision < 1
                or not isinstance(occurrence_id, uuid.UUID)
                or not isinstance(scheduled_for, datetime) or scheduled_for.tzinfo is not None):
            raise ValueError("invalid scheduled job metadata")
    metadata_tuple = (schedule_key, schedule_revision, occurrence_id, scheduled_for)

    def matches(existing):
        return (existing.kind == kind and existing.payload == normalized
                and (existing.schedule_key, existing.schedule_revision,
                     existing.occurrence_id, existing.scheduled_for) == metadata_tuple)

    # Serialize all producers in this schema so the queued cap is safe under
    # concurrent web requests and schedulers. This transaction-level lock does
    # not conflict with the long-lived worker ownership lock.
    await lock_job_producers(session)
    existing = await session.scalar(select(BackgroundJob)
        .join(BackgroundJobIdempotencyKey,
              BackgroundJobIdempotencyKey.job_id == BackgroundJob.id)
        .where(BackgroundJobIdempotencyKey.key == stored_key)
        .with_for_update())
    if existing is None:
        # Backfill-compatible lookup for rows created before alias tracking.
        existing = await session.scalar(select(BackgroundJob).where(
            BackgroundJob.idempotency_key == stored_key).with_for_update())
    if existing is not None:
        if not matches(existing):
            raise IdempotencyConflict("idempotency key was already used for a different job")
        if await session.get(BackgroundJobIdempotencyKey, stored_key) is None:
            session.add(BackgroundJobIdempotencyKey(key=stored_key, job_id=existing.id))
        return (existing, False) if return_created else existing

    if occurrence_id is not None:
        existing = await session.scalar(select(BackgroundJob).where(
            BackgroundJob.occurrence_id == occurrence_id).with_for_update())
        if existing is not None:
            if not matches(existing):
                raise IdempotencyConflict("occurrence id was already used for different content")
            session.add(BackgroundJobIdempotencyKey(key=stored_key, job_id=existing.id))
            await session.flush()
            return (existing, False) if return_created else existing

    if coalesce_key:
        existing = await session.scalar(select(BackgroundJob).where(
            BackgroundJob.coalesce_key == coalesce_key,
            BackgroundJob.state.in_(("queued", "running", "interrupted"))).with_for_update())
        if existing is not None:
            # Hash equality is not treated as semantic authority.
            if matches(existing):
                aliases = await session.scalar(select(func.count()).select_from(
                    BackgroundJobIdempotencyKey
                ).where(BackgroundJobIdempotencyKey.job_id == existing.id))
                if aliases >= MAX_IDEMPOTENCY_ALIASES_PER_JOB:
                    raise QueueFull("background job idempotency alias limit reached")
                session.add(BackgroundJobIdempotencyKey(key=stored_key, job_id=existing.id))
                await session.flush()
                return (existing, False) if return_created else existing
            raise IdempotencyConflict("active job key collision")

    pending = await session.scalar(select(func.count()).select_from(BackgroundJob).where(
        BackgroundJob.state.in_(("queued", "interrupted"))))
    if pending >= MAX_PENDING_JOBS:
        raise QueueFull("background queue is full")

    job = BackgroundJob(kind=kind, payload=normalized, idempotency_key=stored_key,
                        coalesce_key=coalesce_key, state="queued",
                        schedule_key=schedule_key, schedule_revision=schedule_revision,
                        occurrence_id=occurrence_id, scheduled_for=scheduled_for)
    session.add(job)
    await session.flush()
    session.add(BackgroundJobIdempotencyKey(key=stored_key, job_id=job.id))
    await session.flush()
    return (job, True) if return_created else job


async def enqueue_job(kind: str, payload: Dict[str, Any], idempotency_key: str,
                      *, coalesce_active: bool = True) -> BackgroundJob:
    """Enqueue via DatabaseQueueManager and commit before returning."""
    from .database_queue import get_database_queue

    async def operation(session):
        return await enqueue_job_in_session(session, kind, payload, idempotency_key,
                                            coalesce_active=coalesce_active)
    return await get_database_queue().execute_write(operation)


class BackgroundWorkerLease:
    """Own the schema's PostgreSQL session advisory lock and fencing generation."""

    def __init__(self, db_engine=None):
        self.engine = db_engine or default_engine
        self.connection: Optional[AsyncConnection] = None
        self.owner_id: Optional[uuid.UUID] = None
        self.generation: Optional[int] = None

    @property
    def owned(self) -> bool:
        return self.connection is not None and self.owner_id is not None and self.generation is not None

    async def acquire(self) -> bool:
        if self.owned:
            return True
        conn = await self.engine.connect()
        try:
            acquired = await conn.scalar(text("""
                SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()))
            """))
            await conn.commit()
            if not acquired:
                await conn.close()
                return False
            owner_id = uuid.uuid4()
            async with conn.begin():
                generation = await conn.scalar(text("""
                    UPDATE background_worker_status
                    SET generation = generation + 1, owner_id = :owner,
                        heartbeat_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC', status = 'running'
                    WHERE id = 1
                    RETURNING generation
                """), {"owner": owner_id})
                if generation is None:
                    raise WorkerLeaseUnavailable("worker status row is missing")
            self.connection, self.owner_id, self.generation = conn, owner_id, generation
            return True
        except BaseException:
            # Cancellation can hide the result of pg_try_advisory_lock; always
            # unlock explicitly or invalidate the backend before pooling it.
            await _close_connection_releasing_lock(conn)
            raise

    async def heartbeat(self) -> None:
        if not self.owned:
            raise StaleWorker("worker lease is not held")
        try:
            async with self.connection.begin():
                result = await self.connection.execute(text("""
                    UPDATE background_worker_status
                    SET heartbeat_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                    WHERE id = 1 AND owner_id = :owner AND generation = :generation
                          AND status = 'running'
                """), {"owner": self.owner_id, "generation": self.generation})
                if result.rowcount != 1:
                    raise StaleWorker("worker generation is no longer current")
        except BaseException:
            # Unlock on a live connection or invalidate it if its state is
            # uncertain. Never return a still-owning session to the pool.
            await _close_connection_releasing_lock(self.connection)
            self.owner_id = self.generation = self.connection = None
            raise

    async def release(self) -> None:
        conn = self.connection
        if conn is None:
            return
        try:
            async with conn.begin():
                await conn.execute(text("""
                    UPDATE background_worker_status
                    SET owner_id = NULL, heartbeat_at = NULL, status = 'stopped'
                    WHERE id = 1 AND owner_id = :owner AND generation = :generation
                """), {"owner": self.owner_id, "generation": self.generation})
        finally:
            self.connection = self.owner_id = self.generation = None
            await _close_connection_releasing_lock(conn)

    async def recover_jobs(self, session: AsyncSession) -> Dict[str, int]:
        """Recover abandoned jobs and orphaned scheduler ownership under the lease."""
        from ..config import settings

        await self.assert_generation(session, lock_row=True)
        rows = (await session.execute(select(BackgroundJob).where(
            BackgroundJob.state.in_(("running", "interrupted"))
        ).with_for_update())).scalars().all()
        counts = {"interrupted": 0, "delivery_unknown": 0, "recovery_exhausted": 0}
        now = _utcnow_naive()
        for job in rows:
            if job_may_deliver(job.kind, job.payload):
                job.state = "delivery_unknown"
                job.error_code = "external_outcome_unknown"
                job.result = {"status": "warning"}
                counts["delivery_unknown"] += 1
                schedule_state = "delivery_unknown"
                schedule_error = "External delivery outcome is unknown"
            elif job.attempt_generation >= settings.worker_max_job_attempts:
                job.state = "failed"
                job.error_code = "worker_recovery_exhausted"
                job.result = {"status": "incomplete"}
                counts["recovery_exhausted"] += 1
                schedule_state = "failed"
                schedule_error = "Worker restart limit reached"
            else:
                job.state = "interrupted"
                job.error_code = "worker_interrupted"
                job.result = None
                counts["interrupted"] += 1
                schedule_state = "interrupted"
                schedule_error = "Worker stopped before the scheduled job finished"
            job.finished_at = now
            job.owner_generation = None
            schedule_id = (job.payload or {}).get("schedule_id")
            if schedule_id and schedule_state in {"delivery_unknown", "failed"}:
                schedule = await session.get(
                    ScheduleSettings, schedule_id, with_for_update=True
                )
                # A setting edited after this attempt began belongs to the
                # administrator and must not be overwritten by stale recovery.
                if schedule and schedule.is_running and not (
                    job.created_at and schedule.updated_at
                    and schedule.updated_at > job.created_at
                ):
                    schedule.is_running = False
                    schedule.last_finished_at = now
                    schedule.last_status = schedule_state
                    schedule.last_error = schedule_error
                    if not schedule.enabled:
                        schedule.next_run = None
                    elif schedule_state == "failed":
                        schedule.next_run = now + timedelta(minutes=10)
                    elif schedule_state == "delivery_unknown" and (
                        schedule.next_run is None or schedule.next_run <= now
                    ):
                        config = schedule.task_config if isinstance(schedule.task_config, dict) else {}
                        schedule.next_run = self._next_schedule_occurrence(schedule, config, now)

        # Older embedded scheduler runs can leave is_running set without a
        # durable job row. Reconcile those only after preserving all live,
        # queued, and resumable scheduled work above.
        await session.flush()
        active_jobs = (await session.execute(select(BackgroundJob.id, BackgroundJob.payload).where(
            BackgroundJob.state.in_(("queued", "running", "interrupted"))
        ))).all()
        active_schedule_ids = {
            payload.get("schedule_id")
            for _, payload in active_jobs
            if isinstance(payload, dict) and isinstance(payload.get("schedule_id"), int)
        }
        schedules = (await session.execute(select(ScheduleSettings).where(
            ScheduleSettings.is_running.is_(True)
        ).with_for_update())).scalars().all()
        now = _utcnow_naive()
        for schedule in schedules:
            if schedule.id in active_schedule_ids:
                continue

            config = schedule.task_config if isinstance(schedule.task_config, dict) else {}
            delivery_may_have_started = schedule.task_name == "telegram_digest" or (
                schedule.task_name == "news_digest"
                and config.get("send_telegram", True) is not False
            )
            schedule.is_running = False
            schedule.last_finished_at = now
            if delivery_may_have_started:
                schedule.last_status = "delivery_unknown"
                schedule.last_error = "External delivery outcome is unknown after service restart"
                if schedule.enabled:
                    if schedule.next_run is None or schedule.next_run <= now:
                        schedule.next_run = self._next_schedule_occurrence(schedule, config, now)
                else:
                    schedule.next_run = None
            else:
                # Match the existing embedded scheduler's startup behavior:
                # clear the abandoned running flag and preserve its next due time.
                schedule.last_status = "interrupted"
                schedule.last_error = "Service restarted while task was marked running"
                if not schedule.enabled:
                    schedule.next_run = None
                elif schedule.next_run is None:
                    schedule.next_run = self._next_schedule_occurrence(schedule, config, now)
        await session.flush()
        return counts

    @staticmethod
    def _next_schedule_occurrence(schedule, config, now):
        """Compute a future occurrence without replaying an ambiguous digest."""
        from .scheduler import calculate_next_run

        interval = config.get("interval_minutes", 30)
        if isinstance(interval, bool) or not isinstance(interval, int):
            interval = 30
        interval = max(1, min(interval, 24 * 60))
        next_run = calculate_next_run(
            schedule_type=schedule.schedule_type,
            hour=schedule.hour,
            minute=schedule.minute,
            weekdays=schedule.weekdays or [],
            timezone=schedule.timezone,
            interval_minutes=interval,
        )
        return next_run if next_run and next_run > now else now + timedelta(minutes=30)

    async def assert_generation(self, session: AsyncSession, *, lock_row: bool = False) -> None:
        if not self.owned:
            raise StaleWorker("worker lease is not held")
        query = select(BackgroundWorkerStatus).where(BackgroundWorkerStatus.id == 1)
        if lock_row:
            query = query.with_for_update()
        status = await session.scalar(query)
        if (status is None or status.generation != self.generation
                or status.owner_id != self.owner_id or status.status != "running"):
            raise StaleWorker("worker generation is no longer current")

    @asynccontextmanager
    async def transaction_fence(self, session: AsyncSession, job_id: uuid.UUID,
                                attempt_generation: int):
        """Lock the lease row through the caller's transaction commit.

        Call within DatabaseQueueManager.execute_write. The queue commits only
        after this context exits, so a takeover cannot advance generation until
        the protected result write has committed or rolled back.
        """
        await self.assert_generation(session, lock_row=True)
        job = await session.scalar(select(BackgroundJob).where(
            BackgroundJob.id == job_id).with_for_update())
        if (job is None or job.state != "running"
                or job.owner_generation != self.generation
                or job.attempt_generation != attempt_generation):
            raise StaleWorker("job attempt is no longer current")
        yield


async def claim_next_job(session: AsyncSession, lease: BackgroundWorkerLease) -> Optional[BackgroundJob]:
    """Atomically claim the oldest queued or resumable interrupted job."""
    # Coordinate claims with short manual DB retry operations. The job itself
    # remains the durable busy gate after this transaction commits.
    await session.execute(text("""
        SELECT pg_advisory_xact_lock(hashtext(current_database()),
                                     hashtext(current_schema()) # 1145132611)
    """))
    await lease.assert_generation(session, lock_row=True)
    active = await session.scalar(select(BackgroundJob.id).where(
        BackgroundJob.state == "running").limit(1).with_for_update())
    if active is not None:
        return None
    job = await session.scalar(select(BackgroundJob).where(
        BackgroundJob.state.in_(("queued", "interrupted")))
        .order_by(BackgroundJob.queue_position)
        .limit(1).with_for_update(skip_locked=True))
    if job is None:
        return None
    job.state = "running"
    job.started_at = _utcnow_naive()
    job.finished_at = None
    job.error_code = None
    job.owner_generation = lease.generation
    job.attempt_generation += 1
    if job.kind == "article_reprocess" and job.initialized_at is None:
        payload = job.payload or {}
        article_ids = payload.get("article_ids", [])
        fields = {
            "processing_state": "pending",
            "processing_attempts": 0,
            "next_attempt_at": None,
            "last_processing_error": None,
            "processed": False,
        }
        if payload.get("force", True):
            fields.update(
                summary_processed=False,
                category_processed=False,
                ad_processed=False,
            )
        await session.execute(update(Article).where(Article.id.in_(article_ids)).values(**fields))
        # Resetting article state and recording this marker commit atomically.
        # A reclaimed job therefore preserves attempts and completed stages.
        job.initialized_at = _utcnow_naive()
    await session.flush()
    return job


async def finish_job(session: AsyncSession, lease: BackgroundWorkerLease, job_id: uuid.UUID,
                     state: str, *, attempt_generation: int,
                     result: Optional[Dict[str, Any]] = None,
                     error_code: Optional[str] = None) -> None:
    """Fence a terminal transition and persist only bounded safe diagnostics."""
    if state not in {"succeeded", "failed", "interrupted", "delivery_unknown"}:
        raise ValueError("invalid terminal job state")
    if error_code is not None and error_code not in ERROR_CODES:
        raise ValueError("invalid job error code")
    safe_result = _safe_result(result)
    await lease.assert_generation(session, lock_row=True)
    job = await session.scalar(select(BackgroundJob).where(
        BackgroundJob.id == job_id).with_for_update())
    if (job is None or job.state != "running" or job.owner_generation != lease.generation
            or job.attempt_generation != attempt_generation):
        raise StaleWorker("job attempt is no longer current")
    job.state = state
    job.result = safe_result
    job.error_code = error_code
    job.finished_at = _utcnow_naive()
    await session.flush()


async def read_worker_status(session: AsyncSession) -> Optional[Dict[str, Any]]:
    """Read persisted worker status without constructing an orchestrator."""
    row = await session.get(BackgroundWorkerStatus, 1)
    if row is None:
        return None
    return {"generation": row.generation, "status": row.status,
            "heartbeat_at": row.heartbeat_at, "healthy": bool(
                row.status == "running" and row.heartbeat_at and
                (_utcnow_naive() - row.heartbeat_at).total_seconds() <= HEARTBEAT_STALE_SECONDS)}
