"""Go-authorized, typed Python domain operations for the background runtime."""

import asyncio
import hashlib
import json
import logging
import math
import re
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from ..config import settings
from ..database import AsyncSessionLocal
from .domain_fence import (
    DomainAuthorityError, ExecutionContext, GoExecutionAuthorityClient,
    RemoteDomainFence, StaleWorker,
)
from .job_fence import JobExecutionFence, current_job_fence

logger = logging.getLogger(__name__)

DOMAIN_INSTANCE_ID = uuid.uuid4()
MAX_DOMAIN_BODY_BYTES = 64 * 1024
MAX_DOMAIN_RESPONSE_BYTES = 4096
MAX_DOMAIN_ACTIVE_OPERATIONS = 1
MAX_DOMAIN_OPERATION_ENTRIES = 512
MAX_DOMAIN_OPERATION_CACHE_BYTES = 4 * 1024 * 1024
MAX_DOMAIN_OPERATION_RESULT_BYTES = 2 * 1024 * 1024
ALLOWED_DOMAIN_OPERATIONS = frozenset({
    "summary_status", "summaries_generate", "digest_prepare", "digest_publish",
    "media_refresh", "source_test", "backup", "stats_publish",
})
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ERROR_CODES = {
    "domain_operation_failed", "stale_fence", "authority_unavailable",
    "external_outcome_unknown", "prepared_digest_missing", "resource_limit",
    "editorial_not_ready", "editorial_resource_limit", "editorial_provider_timeout",
    "editorial_provider_unavailable", "editorial_invalid_result", "editorial_generation_failed",
}

_RECORDS: OrderedDict[tuple, "OperationRecord"] = OrderedDict()
_RECORDS_LOCK = asyncio.Lock()
_ACTIVE: dict[tuple, asyncio.Task] = {}
_RECORD_BYTES = 0
_RESULT_BYTES = 0
_AUTHORITY_CLIENT = GoExecutionAuthorityClient()
_CURRENT_FENCE: ContextVar[RemoteDomainFence | None] = ContextVar("current_domain_fence", default=None)


class DomainOperationInputError(ValueError):
    pass


class DomainOperationConflict(DomainOperationInputError):
    pass


@dataclass
class OperationRecord:
    job_id: str
    attempt_generation: int
    operation: str
    operation_id: str
    domain_instance_id: str
    expected_envelope: dict[str, Any]
    input_fingerprint: str
    input: dict[str, Any]
    deadline: datetime
    expires_at: datetime
    request_size: int = 0
    state: str = "accepted"
    result: dict[str, Any] | None = None
    error_code: str | None = None
    result_size: int = 0


def domain_status(*, ready: bool) -> dict[str, Any]:
    return {
        "domain_instance_id": str(DOMAIN_INSTANCE_ID),
        "active_operations": len(_ACTIVE),
        "ready": bool(ready),
    }


async def cancel_active_operations(timeout: float = 2.0):
    tasks = tuple(_ACTIVE.values())
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        _, pending = await asyncio.wait(tasks, timeout=max(0.0, timeout))
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.wait(pending, timeout=1.0)
    # Remove completed tasks even when callers registered a task directly
    # (for example, during guard-loss handling). Still-live cancellation
    # resistant tasks remain tracked until their operation wrapper exits.
    for key, task in tuple(_ACTIVE.items()):
        if task.done():
            _ACTIVE.pop(key, None)


def canonical_input_fingerprint(value: dict[str, Any]) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DomainOperationInputError("operation input is invalid") from exc
    return hashlib.sha256(encoded).hexdigest()


def validate_operation_body(body: Any) -> OperationRecord:
    required = {"version", "operation", "operation_id", "domain_instance_id",
                "envelope", "input_fingerprint", "input"}
    if not isinstance(body, dict) or set(body) != required:
        raise DomainOperationInputError("domain operation fields are invalid")
    if type(body["version"]) is not int or body["version"] != 1:
        raise DomainOperationInputError("domain operation version is invalid")
    operation = body["operation"]
    if not isinstance(operation, str) or operation not in ALLOWED_DOMAIN_OPERATIONS:
        raise DomainOperationInputError("domain operation is unsupported")
    from .domain_fence import canonical_uuid, parse_envelope
    operation_id = canonical_uuid(body["operation_id"], "operation_id")
    domain_instance_id = canonical_uuid(body["domain_instance_id"], "domain_instance_id")
    envelope = parse_envelope(body["envelope"])
    now = datetime.now(timezone.utc)
    if (envelope["deadline"] <= now
            or envelope["deadline"] > now + timedelta(hours=24)):
        raise DomainOperationInputError("execution envelope deadline is outside its allowed window")
    fingerprint = body["input_fingerprint"]
    if not isinstance(fingerprint, str) or _HEX_SHA256.fullmatch(fingerprint) is None:
        raise DomainOperationInputError("input_fingerprint is invalid")
    if not isinstance(body["input"], dict) or canonical_input_fingerprint(body["input"]) != fingerprint:
        raise DomainOperationInputError("input fingerprint does not match")
    input_size = len(json.dumps(body["input"], sort_keys=True, separators=(",", ":"),
                                ensure_ascii=False).encode("utf-8"))
    if input_size > 16 * 1024:
        raise DomainOperationInputError("domain operation input exceeds its limit")
    key = (str(envelope["job_uuid"]), envelope["attempt_generation"], operation, str(operation_id))
    request_size = len(json.dumps(
        {"operation": operation, "operation_id": str(operation_id), "domain_instance_id": str(domain_instance_id),
         "envelope": body["envelope"], "input_fingerprint": fingerprint, "input": body["input"]},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode("utf-8"))
    return OperationRecord(
        job_id=str(envelope["job_uuid"]),
        attempt_generation=envelope["attempt_generation"],
        operation=operation,
        operation_id=str(operation_id),
        domain_instance_id=str(domain_instance_id),
        expected_envelope=dict(body["envelope"]),
        input_fingerprint=fingerprint,
        input=body["input"],
        deadline=envelope["deadline"],
        expires_at=envelope["deadline"] + timedelta(seconds=300),
        request_size=request_size,
    )


def _operation_key(record: OperationRecord) -> tuple:
    return (record.job_id, record.attempt_generation, record.operation, record.operation_id)


def _response(record: OperationRecord) -> dict[str, Any]:
    return {
        "version": 1,
        "job_id": record.job_id,
        "attempt_generation": record.attempt_generation,
        "operation": record.operation,
        "operation_id": record.operation_id,
        "domain_instance_id": record.domain_instance_id,
        "input_fingerprint": record.input_fingerprint,
        "state": record.state,
        "result": record.result,
        "error_code": record.error_code,
    }


def _result_size(result: dict[str, Any] | None) -> int:
    try:
        return len(json.dumps(result, separators=(",", ":"), sort_keys=True,
                             ensure_ascii=False, allow_nan=False).encode("utf-8")) if result is not None else 0
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DomainOperationInputError("domain operation result is invalid") from exc


async def _prune_expired_locked(now: datetime):
    global _RECORD_BYTES, _RESULT_BYTES
    for key, record in tuple(_RECORDS.items()):
        if key in _ACTIVE or record.expires_at > now:
            continue
        _RECORDS.pop(key, None)
        _RECORD_BYTES -= record.request_size
        _RESULT_BYTES -= record.result_size


async def accept_domain_operation(record: OperationRecord) -> tuple[dict[str, Any], bool]:
    """Register a durable-in-process operation receipt before task launch."""
    global _RECORD_BYTES
    key = _operation_key(record)
    now = datetime.now(timezone.utc)
    async with _RECORDS_LOCK:
        await _prune_expired_locked(now)
        existing = _RECORDS.get(key)
        if existing is not None:
            if (existing.input_fingerprint != record.input_fingerprint
                    or existing.domain_instance_id != record.domain_instance_id
                    or existing.expected_envelope != record.expected_envelope):
                raise DomainOperationConflict("operation key conflicts with its receipt")
            if existing.expires_at <= now:
                raise DomainOperationConflict("operation receipt expired; do not retry")
            _RECORDS.move_to_end(key)
            return _response(existing), False
        if len(_ACTIVE) >= MAX_DOMAIN_ACTIVE_OPERATIONS:
            raise RuntimeError("domain operation capacity is full")
        if len(_RECORDS) >= MAX_DOMAIN_OPERATION_ENTRIES:
            raise RuntimeError("domain operation receipt capacity is full")
        if _RECORD_BYTES + _RESULT_BYTES + record.request_size > MAX_DOMAIN_OPERATION_CACHE_BYTES:
            raise RuntimeError("domain operation receipt byte capacity is full")
        _RECORDS[key] = record
        _RECORD_BYTES += record.request_size
        task = asyncio.create_task(_run_operation(key, record), name=f"domain-{record.operation}-{record.operation_id}")
        _ACTIVE[key] = task
        return _response(record), True


async def read_operation_status(job_id: str, attempt: int, operation: str, operation_id: str,
                                domain_instance_id: str) -> dict[str, Any] | None:
    key = (job_id, attempt, operation, operation_id)
    async with _RECORDS_LOCK:
        record = _RECORDS.get(key)
        if record is None or record.expires_at <= datetime.now(timezone.utc):
            return None
        if record.domain_instance_id != domain_instance_id:
            raise DomainOperationConflict("domain instance does not own this operation")
        return _response(record)


async def _get_current_context(record: OperationRecord) -> ExecutionContext:
    from .domain_fence import context_matches_envelope
    current = await _AUTHORITY_CLIENT.get_context(uuid.UUID(record.job_id), record.attempt_generation)
    if not context_matches_envelope(current, record.expected_envelope):
        raise DomainAuthorityError("Go execution context does not match the submitted envelope")
    if (current.kind, current.payload) != await _read_persisted_job_identity(current):
        raise DomainAuthorityError("Go execution context does not match persisted job")
    return current


async def _read_persisted_job_identity(context: ExecutionContext):
    from ..models import BackgroundJob
    from .background_jobs import validate_payload
    async with AsyncSessionLocal() as session:
        row = (await session.execute(select(BackgroundJob.kind, BackgroundJob.payload)
                                     .where(BackgroundJob.id == context.job_id))).one_or_none()
        await session.rollback()
    if row is None:
        raise DomainAuthorityError("Go execution job was not found")
    try:
        return row.kind, validate_payload(row.kind, dict(row.payload or {}))
    except ValueError as exc:
        raise DomainAuthorityError("Go execution job payload is invalid") from exc


async def _revalidate_context(record: OperationRecord, expected: ExecutionContext) -> RemoteDomainFence:
    current = await _get_current_context(record)
    if (current.envelope != expected.envelope
            or current.executor_backend_pid != expected.executor_backend_pid
            or current.kind != expected.kind or current.payload != expected.payload):
        raise DomainAuthorityError("Go execution authority changed during operation")
    fence = RemoteDomainFence(current)
    async with AsyncSessionLocal() as session:
        await fence.validate(session)
        await session.rollback()
    return fence


async def _run_operation(key: tuple, record: OperationRecord):
    global _RESULT_BYTES
    try:
        async with _RECORDS_LOCK:
            current = _RECORDS.get(key)
            if current is None:
                return
            current.state = "running"
        context = await _get_current_context(record)
        _assert_operation_allowed(record.operation, record.input, context.kind, context.payload)
        fence = RemoteDomainFence(context)
        async with AsyncSessionLocal() as session:
            await fence.validate(session)
            await session.rollback()
        job_fence = JobExecutionFence(
            lease=fence, job_id=fence.job_id, attempt_generation=fence.attempt_generation,
            executor_instance_id=fence.executor_instance_id,
        )
        token = current_job_fence.set(job_fence)
        fence_token = _CURRENT_FENCE.set(fence)
        try:
            remaining = (record.deadline - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                raise StaleWorker("job attempt deadline has expired")
            async with asyncio.timeout(remaining):
                result = await _dispatch_operation(record, context, fence)
            await _revalidate_context(record, context)
            result = _validate_operation_result(record.operation, result)
            result_size = _result_size(result)
            if result_size > MAX_DOMAIN_RESPONSE_BYTES:
                raise DomainOperationInputError("domain operation result exceeds its limit")
            async with _RECORDS_LOCK:
                current = _RECORDS.get(key)
                if current is not None:
                    if result_size > MAX_DOMAIN_OPERATION_RESULT_BYTES:
                        raise RuntimeError("domain operation result cache is full")
                    if _RECORD_BYTES + _RESULT_BYTES + result_size > MAX_DOMAIN_OPERATION_CACHE_BYTES:
                        raise RuntimeError("domain operation result cache is full")
                    current.result = result
                    current.result_size = result_size
                    current.state = "succeeded"
                    _RESULT_BYTES += result_size
        finally:
            fence.active = False
            job_fence.active = False
            _CURRENT_FENCE.reset(fence_token)
            current_job_fence.reset(token)
    except StaleWorker:
        await _finish_failed(key, "external_outcome_unknown" if record.operation == "digest_publish" else "stale_fence")
    except asyncio.CancelledError:
        await _finish_failed(key, "external_outcome_unknown" if record.operation == "digest_publish" else "stale_fence")
        raise
    except DomainAuthorityError:
        await _finish_failed(
            key, "external_outcome_unknown" if record.operation == "digest_publish" else "authority_unavailable",
        )
    except Exception as exc:
        code = "external_outcome_unknown" if record.operation == "digest_publish" else "domain_operation_failed"
        if record.operation != "digest_publish":
            from .ai_runtime import DigestOperationError
            if isinstance(exc, DigestOperationError):
                code = exc.code
        logger.warning("Domain operation failed (operation=%s error_type=%s)", record.operation, type(exc).__name__)
        await _finish_failed(key, code)
    finally:
        async with _RECORDS_LOCK:
            _ACTIVE.pop(key, None)


async def _finish_failed(key, error_code):
    async with _RECORDS_LOCK:
        record = _RECORDS.get(key)
        if record is not None and record.state not in {"succeeded", "failed"}:
            record.state = "failed"
            record.result = None
            record.error_code = error_code if error_code in _SAFE_ERROR_CODES else "domain_operation_failed"


def _assert_operation_allowed(operation, operation_input, kind, payload):
    payload = payload if isinstance(payload, dict) else {}
    if operation in {"summary_status", "summaries_generate", "digest_prepare", "digest_publish"}:
        if operation == "summaries_generate" and kind == "summaries_generate":
            pass
        elif kind in {"news_digest", "telegram_digest"} and payload.get("send_telegram", True) is not False:
            pass
        else:
            raise DomainOperationInputError("job kind does not permit this digest operation")
    elif operation == "media_refresh":
        if kind != "article_reprocess":
            raise DomainOperationInputError("job kind does not permit media refresh")
        article_id = operation_input.get("article_id")
        ids = payload.get("article_ids")
        if (set(operation_input) != {"article_id"} or type(article_id) is not int or article_id <= 0
                or not isinstance(ids, list) or article_id not in ids or payload.get("reextract_media") is not True):
            raise DomainOperationInputError("media refresh is outside the selected job payload")
    elif operation == "source_test":
        if (kind != "source_test" or set(operation_input) != {"source_id"}
                or type(operation_input.get("source_id")) is not int
                or operation_input["source_id"] != payload.get("source_id")):
            raise DomainOperationInputError("source test is outside the selected job payload")
    elif operation == "backup":
        if kind != "backup" or operation_input:
            raise DomainOperationInputError("job kind does not permit backup")
    elif operation == "stats_publish":
        if kind not in {"news_processing", "news_digest", "article_reprocess", "reprocess_failed"}:
            raise DomainOperationInputError("job kind does not permit stats publication")
        _validate_stats_input(operation_input)
    else:
        raise DomainOperationInputError("domain operation is unsupported")
    if operation == "digest_publish" and set(operation_input) != {"prepared_token"}:
        raise DomainOperationInputError("digest publish input is invalid")
    if operation == "digest_publish":
        from .domain_fence import canonical_uuid
        try:
            canonical_uuid(operation_input["prepared_token"], "prepared_token")
        except DomainAuthorityError as exc:
            raise DomainOperationInputError("prepared digest token is invalid") from exc
    if operation in {"summary_status", "summaries_generate", "digest_prepare", "backup"} and operation_input:
        raise DomainOperationInputError("operation input is invalid")


def _validate_stats_input(operation_input):
    if set(operation_input) != {"stats"} or not isinstance(operation_input["stats"], dict):
        raise DomainOperationInputError("stats input is invalid")
    stats = operation_input["stats"]
    allowed = {"articles_fetched", "articles_processed", "api_calls_made", "errors_count", "duration_seconds"}
    if set(stats) - allowed or not stats:
        raise DomainOperationInputError("stats input fields are invalid")
    for key, value in stats.items():
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                or value < 0 or value > 1_000_000_000):
            raise DomainOperationInputError("stats values must be bounded non-negative numbers")


def _validate_operation_result(operation, result):
    if not isinstance(result, dict):
        raise DomainOperationInputError("domain operation returned an invalid result")
    allowed = {
        "summary_status": {"summaries_count"},
        "summaries_generate": {"summaries_generated"},
        "digest_prepare": {"prepared_token"},
        "digest_publish": {"success", "parts_sent", "parts_total", "rich_message"},
        "media_refresh": {"refreshed"},
        "source_test": {"success"},
        "backup": {"backup_bytes"},
        "stats_publish": {"published"},
    }[operation]
    if set(result) - allowed:
        raise DomainOperationInputError("domain operation result contains unsupported fields")
    if operation == "digest_prepare":
        if set(result) != {"prepared_token"}:
            raise DomainOperationInputError("prepared digest result is invalid")
        from .domain_fence import canonical_uuid
        try:
            canonical_uuid(result["prepared_token"], "prepared_token")
        except DomainAuthorityError as exc:
            raise DomainOperationInputError("prepared digest result is invalid") from exc
    elif operation == "digest_publish":
        if (type(result.get("success")) is not bool or type(result.get("parts_sent")) is not int
                or type(result.get("parts_total")) is not int or type(result.get("rich_message")) is not bool
                or result["parts_sent"] < 0 or result["parts_total"] < result["parts_sent"]):
            raise DomainOperationInputError("digest publish result is invalid")
    elif operation in {"media_refresh", "source_test", "stats_publish"}:
        expected = {"refreshed"} if operation == "media_refresh" else (
            {"success"} if operation == "source_test" else {"published"})
        if set(result) != expected or type(next(iter(result.values()))) is not bool:
            raise DomainOperationInputError("domain operation result is invalid")
    else:
        for value in result.values():
            if type(value) is not int or value < 0 or value > 1_000_000_000:
                raise DomainOperationInputError("domain operation result is invalid")
    return result


async def _dispatch_operation(record, context, fence):
    operation = record.operation
    data = record.input
    if operation in {"summary_status", "summaries_generate", "digest_prepare", "digest_publish"}:
        from .ai_runtime import get_ai_runtime
        return await get_ai_runtime().digest_operation(record, context)
    if operation == "media_refresh":
        from .article_media_reextract import reextract_article_media
        refreshed = await reextract_article_media(data["article_id"], fence=fence)
        return {"refreshed": bool(refreshed)}
    if operation == "source_test":
        from .source_manager import SourceManager
        from ..models import Source
        async with AsyncSessionLocal() as session:
            async with fence.transaction_fence(session, fence.job_id, fence.attempt_generation):
                source = await session.get(Source, data["source_id"])
                if source is None:
                    raise DomainOperationInputError("source no longer exists")
                source_snapshot = SourceManager()._source_test_snapshot(source)
                await session.rollback()
        await _revalidate_context(record, context)
        success = await SourceManager()._probe_source(source_snapshot)
        await _revalidate_context(record, context)
        return {"success": bool(success)}
    if operation == "backup":
        from .backup_service import BackupService
        path = await BackupService().create_backup()
        return {"backup_bytes": path.stat().st_size}
    if operation == "stats_publish":
        return await _publish_stats(fence, data["stats"])
    raise DomainOperationInputError("domain operation is unsupported")


async def _publish_stats(fence, stats):
    from ..processing.processing_stats_service import ProcessingStatsService
    result_stats = dict(stats)
    result_stats["errors_count"] = int(result_stats.get("errors_count", 0))
    result_stats["errors"] = []
    result_stats["duration_seconds"] = int(result_stats.get("duration_seconds", 0))
    async with AsyncSessionLocal() as session:
        async with fence.transaction_fence(session, fence.job_id, fence.attempt_generation):
            await ProcessingStatsService().update_processing_stats(session, result_stats)
    return {"published": True}
