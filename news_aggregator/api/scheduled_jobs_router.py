"""Private receiver for scheduler dispatch commands."""

import hmac
import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import func, select

from ..config import settings
from ..models import SchedulerOccurrenceReceipt
from ..services.background_jobs import (
    MAX_SCHEDULER_OCCURRENCE_RECEIPTS,
    IdempotencyConflict,
    QueueFull,
    WorkerLeaseUnavailable,
    enqueue_job_in_session,
    is_worker_available,
    lock_job_producers,
)
from ..services.database_queue import get_database_queue


router = APIRouter()
MAX_ENVELOPE_BYTES = 8192
RFC3339_UTC = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z$")
SCHEDULE_KEY = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")
SCHEDULED_KINDS = frozenset({
    "news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed",
})


def _invalid():
    raise HTTPException(status_code=422, detail="invalid scheduled job envelope")


def _validate_envelope(value, idempotency_key):
    fields = {"version", "occurrence_id", "schedule_key", "schedule_revision",
              "scheduled_for", "operation", "payload"}
    if not isinstance(value, dict) or set(value) != fields:
        _invalid()
    if type(value["version"]) is not int or value["version"] != 1:
        _invalid()
    occurrence = value["occurrence_id"]
    if not isinstance(occurrence, str):
        _invalid()
    try:
        if str(UUID(occurrence)) != occurrence:
            _invalid()
    except (ValueError, AttributeError):
        _invalid()
    if idempotency_key != occurrence:
        _invalid()
    if (not isinstance(value["schedule_key"], str)
            or not SCHEDULE_KEY.fullmatch(value["schedule_key"])):
        _invalid()
    revision = value["schedule_revision"]
    if type(revision) is not int or revision < 1:
        _invalid()
    scheduled_for = value["scheduled_for"]
    if not isinstance(scheduled_for, str) or not RFC3339_UTC.fullmatch(scheduled_for):
        _invalid()
    try:
        datetime.fromisoformat(scheduled_for[:-1] + "+00:00")
    except ValueError:
        _invalid()
    operation = value["operation"]
    if not isinstance(operation, str) or operation not in SCHEDULED_KINDS:
        _invalid()
    if not isinstance(value["payload"], dict):
        _invalid()
    return value


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _reject_json_constant(_value):
    raise ValueError("non-finite JSON number")


async def _read_limited_body(request):
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_ENVELOPE_BYTES:
            raise HTTPException(status_code=413, detail="scheduled job envelope too large")
        body.extend(chunk)
    return bytes(body)


@router.post("/internal/scheduled-jobs")
async def receive_scheduled_job(request: Request):
    """Authenticate and durably enqueue one versioned scheduler occurrence."""
    configured_token = settings.scheduled_jobs_token
    configured_token = configured_token.get_secret_value() if configured_token else None
    authorization = request.headers.get("authorization", "")
    supplied_token = authorization[7:] if authorization.startswith("Bearer ") else ""
    if not configured_token:
        raise HTTPException(status_code=503, detail="scheduled job receiver unavailable")
    try:
        supplied_token_bytes = supplied_token.encode("utf-8")
        configured_token_bytes = configured_token.encode("utf-8")
    except UnicodeEncodeError:
        raise HTTPException(status_code=401, detail="unauthorized") from None
    if not supplied_token_bytes or not hmac.compare_digest(
            supplied_token_bytes, configured_token_bytes):
        raise HTTPException(status_code=401, detail="unauthorized")
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise HTTPException(status_code=415, detail="application/json required")
    if request.headers.get("content-encoding", "identity").lower() != "identity":
        raise HTTPException(status_code=415, detail="content encoding is not supported")
    content_length = request.headers.get("content-length")
    if content_length and (not content_length.isdigit() or int(content_length) > MAX_ENVELOPE_BYTES):
        raise HTTPException(status_code=413, detail="scheduled job envelope too large")
    raw = await _read_limited_body(request)
    try:
        envelope = _validate_envelope(json.loads(
            raw, object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        ), request.headers.get("idempotency-key", ""))
    except (ValueError, UnicodeError, RecursionError):
        _invalid()

    kind = envelope["operation"]
    payload = envelope["payload"]
    try:
        from ..services.background_jobs import validate_payload
        if {"schedule_id", "scheduled_for"} & set(payload):
            _invalid()
        normalized = validate_payload(kind, payload)
    except (TypeError, ValueError):
        _invalid()
    scheduled_for = datetime.fromisoformat(
        envelope["scheduled_for"][:-1] + "+00:00"
    ).astimezone(timezone.utc).replace(tzinfo=None)
    occurrence_id = UUID(envelope["occurrence_id"])
    metadata = {
        "schedule_key": envelope["schedule_key"],
        "schedule_revision": envelope["schedule_revision"],
        "occurrence_id": occurrence_id,
        "scheduled_for": scheduled_for,
    }
    try:
        canonical_envelope = json.dumps(
            envelope, separators=(",", ":"), sort_keys=True,
            ensure_ascii=False, allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        _invalid()
    envelope_hash = hashlib.sha256(canonical_envelope).hexdigest()

    async def operation(session):
        await lock_job_producers(session)
        existing = await session.get(SchedulerOccurrenceReceipt, occurrence_id)
        if existing is not None:
            if existing.envelope_sha256 != envelope_hash:
                raise IdempotencyConflict("occurrence id was already used for different content")
            return existing.job_id, False
        if (settings.background_execution_mode != "worker"
                or settings.schedule_dispatch_mode != "service"):
            raise WorkerLeaseUnavailable("service-mode worker is not enabled")
        available = await is_worker_available(session)
        if not available:
            raise WorkerLeaseUnavailable("background worker unavailable")
        receipt_count = await session.scalar(select(func.count()).select_from(
            SchedulerOccurrenceReceipt
        ))
        if receipt_count >= min(settings.scheduled_jobs_receipt_limit,
                                MAX_SCHEDULER_OCCURRENCE_RECEIPTS):
            raise QueueFull("scheduler receipt capacity reached")
        job, created = await enqueue_job_in_session(
            session, kind, normalized,
            "scheduler:v1:" + envelope["occurrence_id"],
            coalesce_active=False,
            schedule_metadata=metadata,
            return_created=True,
        )
        session.add(SchedulerOccurrenceReceipt(
            occurrence_id=occurrence_id,
            envelope_sha256=envelope_hash,
            job_id=job.id,
        ))
        await session.flush()
        return job.id, created

    try:
        (job_id, created) = await get_database_queue().execute_write(operation)
    except WorkerLeaseUnavailable:
        raise HTTPException(status_code=503, detail="background worker unavailable") from None
    except QueueFull:
        raise HTTPException(status_code=503, detail="background queue is full") from None
    except IdempotencyConflict:
        raise HTTPException(status_code=409, detail="occurrence content conflicts") from None
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=503, detail="scheduled job receiver unavailable") from None

    receipt = {
        "version": 1,
        "occurrence_id": envelope["occurrence_id"],
        "job_id": str(job_id),
        "status": "accepted",
    }
    return Response(
        content=json.dumps(receipt, separators=(",", ":")),
        status_code=202 if created else 200,
        media_type="application/json",
    )
