"""Fencing for Python domain leaf operations authorized by Go execution."""

import asyncio
import json
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any

import aiohttp
from sqlalchemy import select, text

from ..config import settings
from ..models import BackgroundJob, BackgroundJobExecution, BackgroundWorkerStatus
from .background_jobs import HEARTBEAT_STALE_SECONDS, StaleWorker

GO_EXECUTION_GUARD_XOR = 0x454E4732
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ENVELOPE_FIELDS = {
    "version", "job_id", "attempt_generation", "owner_id", "generation",
    "backend_pid", "executor_instance_id", "deadline_at",
}


class DomainAuthorityError(ValueError):
    """Go did not authorize the requested active attempt."""


def canonical_uuid(value: Any, field: str) -> uuid.UUID:
    if not isinstance(value, str):
        raise DomainAuthorityError(f"{field} is invalid")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise DomainAuthorityError(f"{field} is invalid") from exc
    if str(parsed) != value:
        raise DomainAuthorityError(f"{field} must use canonical UUID form")
    return parsed


def parse_envelope(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _ENVELOPE_FIELDS:
        raise DomainAuthorityError("execution envelope fields are invalid")
    if type(value["version"]) is not int or value["version"] != 1:
        raise DomainAuthorityError("execution envelope version is invalid")
    job_id = canonical_uuid(value["job_id"], "job_id")
    owner_id = canonical_uuid(value["owner_id"], "owner_id")
    executor_id = canonical_uuid(value["executor_instance_id"], "executor_instance_id")
    for name in ("attempt_generation", "generation", "backend_pid"):
        if type(value[name]) is not int or value[name] <= 0 or value[name] > 2_147_483_647:
            raise DomainAuthorityError(f"{name} is invalid")
    if not isinstance(value["deadline_at"], str):
        raise DomainAuthorityError("deadline_at is invalid")
    try:
        deadline = datetime.fromisoformat(value["deadline_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainAuthorityError("deadline_at is invalid") from exc
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise DomainAuthorityError("deadline_at must include a timezone")
    return {
        **value,
        "job_uuid": job_id,
        "owner_uuid": owner_id,
        "executor_uuid": executor_id,
        "deadline": deadline.astimezone(timezone.utc),
        "deadline_db": deadline.astimezone(timezone.utc).replace(tzinfo=None),
    }


@dataclass(frozen=True)
class ExecutionContext:
    envelope: dict[str, Any]
    executor_backend_pid: int
    kind: str
    payload: dict[str, Any]

    @property
    def job_id(self):
        return self.envelope["job_uuid"]

    @property
    def attempt_generation(self):
        return self.envelope["attempt_generation"]


class GoExecutionAuthorityClient:
    """Read active attempt authority from the private Go execution service."""

    def __init__(self, *, session_factory=aiohttp.ClientSession):
        self._session_factory = session_factory

    async def get_context(self, job_id: uuid.UUID, attempt_generation: int) -> ExecutionContext:
        base = (settings.execution_service_url or "").rstrip("/")
        token = settings.job_executor_token.get_secret_value() if settings.job_executor_token else ""
        if not _valid_service_base(base) or not token:
            raise DomainAuthorityError("Go execution authority is not configured")
        timeout = aiohttp.ClientTimeout(total=4, connect=2, sock_connect=2, sock_read=4)
        headers = {"Authorization": f"Bearer {token}"}
        path = f"{base}/internal/v1/job-executions/{job_id}/{attempt_generation}/context"
        try:
            async with self._session_factory(timeout=timeout) as client:
                async with client.get(path, headers=headers, allow_redirects=False) as response:
                    if response.status != 200:
                        raise DomainAuthorityError("Go execution attempt is not active")
                    body = await _read_json_bounded(response, 16 * 1024)
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            raise DomainAuthorityError("Go execution authority is unavailable") from exc
        if (not isinstance(body, dict) or set(body) != {
                "version", "envelope", "executor_backend_pid", "kind", "payload"}
                or type(body["version"]) is not int or body["version"] != 1
                or type(body["executor_backend_pid"]) is not int or body["executor_backend_pid"] <= 0
                or not isinstance(body["kind"], str) or not isinstance(body["payload"], dict)):
            raise DomainAuthorityError("Go execution context is invalid")
        envelope = parse_envelope(body["envelope"])
        if envelope["job_uuid"] != job_id or envelope["attempt_generation"] != attempt_generation:
            raise DomainAuthorityError("Go execution context identity does not match")
        if envelope["deadline"] <= datetime.now(timezone.utc):
            raise DomainAuthorityError("Go execution attempt deadline has expired")
        return ExecutionContext(envelope, body["executor_backend_pid"], body["kind"], body["payload"])


_AUTHORITY_CLIENT = GoExecutionAuthorityClient()


async def get_execution_context(job_id: uuid.UUID, attempt_generation: int) -> ExecutionContext:
    return await _AUTHORITY_CLIENT.get_context(job_id, attempt_generation)


def context_matches_envelope(context: ExecutionContext, raw_envelope: Any) -> bool:
    if not isinstance(raw_envelope, dict) or set(raw_envelope) != _ENVELOPE_FIELDS:
        return False
    return all(context.envelope.get(key) == raw_envelope.get(key) for key in _ENVELOPE_FIELDS)


async def _read_json_bounded(response, limit: int):
    data = bytearray()
    async for chunk in response.content.iter_chunked(4096):
        if len(data) + len(chunk) > limit:
            raise DomainAuthorityError("Go execution context exceeds its limit")
        data.extend(chunk)
    try:
        value = json.loads(data)
    except (ValueError, TypeError, RecursionError) as exc:
        raise DomainAuthorityError("Go execution context is invalid") from exc
    return value


def _valid_service_base(value: str) -> bool:
    from urllib.parse import urlsplit
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        return False
    return bool(parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment
                and parsed.path in {"", "/"} and not any(ch.isspace() for ch in value))


class RemoteDomainFence:
    """Verify both Go-owned advisory authorities and the current job receipt."""

    def __init__(self, context: ExecutionContext, *, allow_terminal=False):
        self.context = context
        self.allow_terminal = allow_terminal
        envelope = context.envelope
        self.job_id = envelope["job_uuid"]
        self.attempt_generation = envelope["attempt_generation"]
        self.owner_id = envelope["owner_uuid"]
        self.generation = envelope["generation"]
        self.worker_backend_pid = envelope["backend_pid"]
        self.executor_instance_id = envelope["executor_uuid"]
        self.executor_backend_pid = context.executor_backend_pid
        self.deadline_at = envelope["deadline_db"]

    async def _assert_authorities(self, session):
        with session.no_autoflush:
            status = (await session.execute(select(
                BackgroundWorkerStatus.owner_id, BackgroundWorkerStatus.generation,
                BackgroundWorkerStatus.status, BackgroundWorkerStatus.lock_backend_pid,
                BackgroundWorkerStatus.heartbeat_at,
            ).where(BackgroundWorkerStatus.id == 1).with_for_update())).one_or_none()
            db_now = await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'"))
        if (status is None or status.owner_id != self.owner_id or status.generation != self.generation
                or status.status != "running" or status.lock_backend_pid != self.worker_backend_pid
                or status.heartbeat_at is None or status.heartbeat_at > db_now
                or db_now - status.heartbeat_at > timedelta(seconds=HEARTBEAT_STALE_SECONDS)):
            raise StaleWorker("worker lease is no longer current")
        worker_held = await _advisory_lock_held(session, self.worker_backend_pid, 0)
        if not worker_held:
            raise StaleWorker("worker advisory lease is no longer held")
        execution_held = await _advisory_lock_held(
            session, self.executor_backend_pid, GO_EXECUTION_GUARD_XOR,
        )
        if not execution_held:
            raise StaleWorker("Go execution guard is no longer held")
        return db_now

    @staticmethod
    async def _advisory_lock_held(session, backend_pid, lock_xor):
        return await _advisory_lock_held(session, backend_pid, lock_xor)

    async def validate(self, session, *, allow_terminal=None):
        if allow_terminal is None:
            allow_terminal = self.allow_terminal
        db_now = await self._assert_authorities(session)
        if not allow_terminal and db_now >= self.deadline_at:
            raise StaleWorker("job attempt deadline has expired")
        job = (await session.execute(select(
            BackgroundJob.state, BackgroundJob.owner_generation,
            BackgroundJob.attempt_generation, BackgroundJob.kind, BackgroundJob.payload,
        ).where(BackgroundJob.id == self.job_id).with_for_update())).one_or_none()
        receipt = (await session.execute(select(
            BackgroundJobExecution.executor_instance_id, BackgroundJobExecution.owner_id,
            BackgroundJobExecution.worker_generation, BackgroundJobExecution.deadline_at,
            BackgroundJobExecution.state, BackgroundJobExecution.legacy_alert_state,
        ).where(BackgroundJobExecution.job_id == self.job_id,
                BackgroundJobExecution.attempt_generation == self.attempt_generation)
         .with_for_update())).one_or_none()
        if job is None or receipt is None or job.attempt_generation != self.attempt_generation:
            raise StaleWorker("job attempt is no longer current")
        from .background_jobs import validate_payload
        try:
            payload = validate_payload(job.kind, job.payload or {})
        except ValueError as exc:
            raise StaleWorker("job payload is invalid") from exc
        if (job.kind != self.context.kind or payload != self.context.payload
                or job.owner_generation != self.generation):
            raise StaleWorker("job payload changed after execution authorization")
        if (receipt.executor_instance_id != self.executor_instance_id
                or receipt.owner_id != self.owner_id or receipt.worker_generation != self.generation
                or receipt.deadline_at != self.deadline_at):
            raise StaleWorker("execution receipt identity is no longer current")
        if allow_terminal:
            if job.state not in {"succeeded", "failed", "interrupted", "delivery_unknown"}:
                raise StaleWorker("job is not terminal")
            if receipt.state != job.state:
                raise StaleWorker("terminal execution receipt does not match job")
        elif job.state != "running" or receipt.state != "running":
            raise StaleWorker("job attempt is not running")
        return db_now

    @asynccontextmanager
    async def transaction_fence(self, session, job_id, attempt_generation):
        if job_id != self.job_id or attempt_generation != self.attempt_generation:
            raise StaleWorker("job attempt is no longer current")
        await self.validate(session)
        yield

    @asynccontextmanager
    async def publication_fence(self, session):
        await self.validate(session)
        yield


async def _advisory_lock_held(session, backend_pid, lock_xor):
    classid = "(((hashtext(current_database())::bigint + 4294967296) % 4294967296)::oid)"
    objid = f"((((hashtext(current_schema()) # {int(lock_xor)})::bigint + 4294967296) % 4294967296)::oid)"
    return bool(await session.scalar(text(f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_locks
            WHERE locktype='advisory' AND granted AND pid=:backend_pid AND objsubid=2
              AND classid={classid} AND objid={objid}
              AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
        )
    """), {"backend_pid": backend_pid}))
