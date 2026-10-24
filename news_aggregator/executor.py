"""Private HTTP entrypoint for executing Go-owned durable job attempts."""

import asyncio
import hashlib
import hmac
import json
import logging
import signal
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .config import settings
from .database import AsyncSessionLocal, engine
from .models import (
    Article, BackgroundJob, BackgroundJobExecution, BackgroundWorkerStatus,
    ScheduleSettings,
)
from .services.background_execution import _bounded_result, execute_job
from .services.background_jobs import (
    HEARTBEAT_STALE_SECONDS, StaleWorker, _safe_result,
    job_may_deliver, validate_payload,
)
from .services.job_fence import JobExecutionFence, current_job_fence

logger = logging.getLogger(__name__)

MAX_ENVELOPE_BYTES = 8192
MAX_DEADLINE_SECONDS = 24 * 60 * 60
EXECUTOR_INSTANCE_ID = uuid.uuid4()
_ACTIVE: dict[tuple[uuid.UUID, int], asyncio.Task] = {}
_ACTIVE_FENCES: dict[tuple[uuid.UUID, int], JobExecutionFence] = {}
_ADMISSION_LOCK = asyncio.Lock()
_LEGACY_EVENT_LOCKS: dict[tuple[uuid.UUID, int], asyncio.Lock] = {}
_ALERT_ACTIVE: set[tuple[uuid.UUID, int]] = set()
_EXECUTOR_GUARD_CONNECTION: AsyncConnection | None = None
_EXECUTOR_GUARD_BACKEND_PID: int | None = None
_EXECUTOR_GUARD_HEALTHY = False
_EXECUTOR_GUARD_LOCK = asyncio.Lock()
_EXECUTOR_SERVER = None
_EXECUTOR_GUARD_QUERY_TIMEOUT = 2.0

# This two-key advisory lock is distinct from the Go worker lease key and its
# retry coordination key. Only one private executor process may own a schema.
_EXECUTOR_GUARD_KEY_XOR = 1145132609


class InvalidEnvelope(ValueError):
    pass


class AttemptDeadlineExpired(StaleWorker):
    """The current attempt is still owned, but its immutable deadline passed."""


async def _acquire_executor_guard() -> None:
    """Hold a dedicated session lock for this executor process."""
    global _EXECUTOR_GUARD_CONNECTION, _EXECUTOR_GUARD_BACKEND_PID, _EXECUTOR_GUARD_HEALTHY
    async with _EXECUTOR_GUARD_LOCK:
        connection = await engine.connect()
        try:
            async with asyncio.timeout(_EXECUTOR_GUARD_QUERY_TIMEOUT):
                acquired, backend_pid = (await connection.execute(text("""
                    SELECT pg_try_advisory_lock(
                        hashtext(current_database()), hashtext(current_schema()) # :lock_xor
                    ), pg_backend_pid()
                """), {"lock_xor": _EXECUTOR_GUARD_KEY_XOR})).one()
                await connection.commit()
        except BaseException:
            await _discard_guard_connection(connection)
            raise
        if not acquired:
            await connection.close()
            raise RuntimeError("another Python job executor already owns this database")
        _EXECUTOR_GUARD_CONNECTION = connection
        _EXECUTOR_GUARD_BACKEND_PID = int(backend_pid)
        _EXECUTOR_GUARD_HEALTHY = True


async def _release_executor_guard() -> None:
    global _EXECUTOR_GUARD_CONNECTION, _EXECUTOR_GUARD_BACKEND_PID, _EXECUTOR_GUARD_HEALTHY
    async with _EXECUTOR_GUARD_LOCK:
        connection = _EXECUTOR_GUARD_CONNECTION
        _EXECUTOR_GUARD_CONNECTION = None
        _EXECUTOR_GUARD_BACKEND_PID = None
        _EXECUTOR_GUARD_HEALTHY = False
        if connection is None:
            return
        try:
            async with asyncio.timeout(_EXECUTOR_GUARD_QUERY_TIMEOUT):
                await connection.execute(text("""
                    SELECT pg_advisory_unlock(
                        hashtext(current_database()), hashtext(current_schema()) # :lock_xor
                    )
                """), {"lock_xor": _EXECUTOR_GUARD_KEY_XOR})
                await connection.commit()
        except BaseException:
            logger.warning("Could not explicitly release Python executor singleton lock")
            await _discard_guard_connection(connection)
        else:
            await connection.close()


async def _discard_guard_connection(connection: AsyncConnection) -> None:
    """Never return a connection with uncertain lock/transaction state to the pool."""
    async def discard():
        try:
            async with asyncio.timeout(1):
                await connection.invalidate()
        except BaseException:
            pass
        try:
            async with asyncio.timeout(1):
                await connection.close()
        except BaseException:
            pass

    cleanup = asyncio.create_task(discard())
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        # Finish pool cleanup even when startup/shutdown is being cancelled.
        await asyncio.shield(cleanup)
        raise


async def _executor_guard_is_held() -> bool:
    global _EXECUTOR_GUARD_CONNECTION, _EXECUTOR_GUARD_BACKEND_PID, _EXECUTOR_GUARD_HEALTHY
    async with _EXECUTOR_GUARD_LOCK:
        connection = _EXECUTOR_GUARD_CONNECTION
        expected_pid = _EXECUTOR_GUARD_BACKEND_PID
        if connection is None or expected_pid is None or not _EXECUTOR_GUARD_HEALTHY:
            return False
        try:
            async with asyncio.timeout(_EXECUTOR_GUARD_QUERY_TIMEOUT):
                backend_pid, held = (await connection.execute(text("""
                    SELECT pg_backend_pid(), EXISTS (
                        SELECT 1 FROM pg_locks
                        WHERE locktype='advisory' AND granted AND pid=pg_backend_pid()
                          AND objsubid=2
                          AND classid=(((hashtext(current_database())::bigint + 4294967296) % 4294967296)::oid)
                          AND objid=((((hashtext(current_schema()) # :lock_xor)::bigint + 4294967296) % 4294967296)::oid)
                          AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
                    )
                """), {"lock_xor": _EXECUTOR_GUARD_KEY_XOR})).one()
                valid = int(backend_pid) == expected_pid and bool(held)
                await connection.commit()
                return valid
        except asyncio.CancelledError:
            _EXECUTOR_GUARD_CONNECTION = None
            _EXECUTOR_GUARD_BACKEND_PID = None
            _EXECUTOR_GUARD_HEALTHY = False
            await _discard_guard_connection(connection)
            await _revoke_executor_work(_EXECUTOR_SERVER)
            raise
        except BaseException:
            _EXECUTOR_GUARD_CONNECTION = None
            _EXECUTOR_GUARD_BACKEND_PID = None
            _EXECUTOR_GUARD_HEALTHY = False
            await _discard_guard_connection(connection)
            return False


async def _revoke_executor_work(server) -> None:
    global _EXECUTOR_GUARD_HEALTHY
    _EXECUTOR_GUARD_HEALTHY = False
    for fence in tuple(_ACTIVE_FENCES.values()):
        fence.active = False
    tasks = tuple(_ACTIVE.values())
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.wait(tasks, timeout=2)
    for key, task in tuple(_ACTIVE.items()):
        if task.done():
            _ACTIVE.pop(key, None)
            _ACTIVE_FENCES.pop(key, None)
    target = server or _EXECUTOR_SERVER
    if target is not None:
        target.should_exit = True


async def _check_executor_guard(server=None) -> bool:
    had_guard_connection = _EXECUTOR_GUARD_CONNECTION is not None
    believed_owner = _EXECUTOR_GUARD_HEALTHY or had_guard_connection
    if await _executor_guard_is_held():
        return True
    if believed_owner:
        logger.error("Python executor singleton lock was lost; revoking active work")
        await _revoke_executor_work(server or _EXECUTOR_SERVER)
    elif (server or _EXECUTOR_SERVER) is not None:
        # The request path may be the first detector and does not receive the
        # Uvicorn server argument. A failed owner check still must stop serving.
        (server or _EXECUTOR_SERVER).should_exit = True
    return False


async def _watch_executor_guard(server) -> None:
    while not server.should_exit:
        await asyncio.sleep(1)
        if not await _check_executor_guard(server):
            return


async def _require_executor_guard() -> None:
    if not await _check_executor_guard():
        raise HTTPException(status_code=503, detail="Executor instance is not the database owner")


async def _assert_remote_worker_lease(session, owner_id, generation, backend_pid, *, lock_row=True):
    with session.no_autoflush:
        query = select(
            BackgroundWorkerStatus.owner_id, BackgroundWorkerStatus.generation,
            BackgroundWorkerStatus.status, BackgroundWorkerStatus.lock_backend_pid,
            BackgroundWorkerStatus.heartbeat_at,
        ).where(BackgroundWorkerStatus.id == 1)
        if lock_row:
            query = query.with_for_update()
        status = (await session.execute(query)).one_or_none()
        db_now = await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'"))
    if (status is None or status.owner_id != owner_id or status.generation != generation
            or status.status != "running" or status.lock_backend_pid != backend_pid
            or status.heartbeat_at is None or status.heartbeat_at > db_now
            or db_now - status.heartbeat_at > timedelta(seconds=HEARTBEAT_STALE_SECONDS)):
        raise StaleWorker("worker generation or heartbeat is no longer current")
    classid = RemoteJobFence._u32_hash("current_database()")
    objid = RemoteJobFence._u32_hash("current_schema()")
    held = await session.scalar(text(f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_locks
            WHERE locktype = 'advisory' AND granted
              AND pid = :backend_pid AND objsubid = 2
              AND classid = {classid} AND objid = {objid}
              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
        )
    """), {"backend_pid": backend_pid})
    if not held:
        raise StaleWorker("worker advisory lease is no longer held")
    return status, db_now


class RemoteJobFence:
    """Validate the Go lease and lock its rows through each application commit."""

    def __init__(self, *, job_id: uuid.UUID, owner_id: uuid.UUID, generation: int,
                 attempt_generation: int, backend_pid: int, deadline_at=None,
                 allow_after_deadline=False):
        self.job_id = job_id
        self.owner_id = owner_id
        self.generation = generation
        self.attempt_generation = attempt_generation
        self.backend_pid = backend_pid
        self.deadline_at = deadline_at
        self.allow_after_deadline = allow_after_deadline

    @staticmethod
    def _u32_hash(expression: str) -> str:
        # pg_locks exposes advisory keys as OIDs; this maps signed hashtext()
        # int4 values to their unsigned 32-bit representation without a cast overflow.
        return f"(((hashtext({expression})::bigint + 4294967296) % 4294967296)::oid)"

    @asynccontextmanager
    async def transaction_fence(self, session, job_id, attempt_generation):
        if job_id != self.job_id or attempt_generation != self.attempt_generation:
            raise StaleWorker("job attempt is no longer current")
        with session.no_autoflush:
            _, db_now = await _assert_remote_worker_lease(
                session, self.owner_id, self.generation, self.backend_pid,
            )
            if (self.deadline_at is not None and not self.allow_after_deadline
                    and db_now >= self.deadline_at):
                raise AttemptDeadlineExpired("job attempt deadline has expired")
            job = (await session.execute(select(
                BackgroundJob.state, BackgroundJob.owner_generation,
                BackgroundJob.attempt_generation,
            ).where(BackgroundJob.id == self.job_id).with_for_update())).one_or_none()
        if (job is None or job.state != "running" or job.owner_generation != self.generation
                or job.attempt_generation != self.attempt_generation):
            raise StaleWorker("job attempt is no longer current")

        yield

    @asynccontextmanager
    async def publication_fence(self, session):
        """Hold the same validated row locks around a filesystem publication."""
        active = current_job_fence.get()
        if (active is None or not active.active or active.lease is not self
                or active.job_id != self.job_id
                or active.attempt_generation != self.attempt_generation):
            raise StaleWorker("job attempt is no longer active")
        async with self.transaction_fence(session, self.job_id, self.attempt_generation):
            yield


def _reject_duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidEnvelope("duplicate JSON field")
        result[key] = value
    return result


def _parse_envelope(raw: bytes) -> tuple[dict[str, Any], str]:
    if len(raw) > MAX_ENVELOPE_BYTES:
        raise InvalidEnvelope("request body exceeds 8192 bytes")
    try:
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(InvalidEnvelope("invalid JSON number")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEnvelope("request body must be a JSON object") from exc
    required = {"version", "job_id", "owner_id", "generation", "attempt_generation", "deadline_at"}
    if not isinstance(body, dict) or set(body) != required:
        raise InvalidEnvelope("request fields are invalid")
    if type(body["version"]) is not int or body["version"] != 1:
        raise InvalidEnvelope("unsupported envelope version")
    try:
        job_id = uuid.UUID(body["job_id"])
        owner_id = uuid.UUID(body["owner_id"])
    except (ValueError, TypeError, AttributeError) as exc:
        raise InvalidEnvelope("job or owner identifier is invalid") from exc
    if str(job_id) != body["job_id"] or str(owner_id) != body["owner_id"]:
        raise InvalidEnvelope("identifiers must use canonical UUID form")
    for field in ("generation", "attempt_generation"):
        if type(body[field]) is not int or body[field] <= 0 or body[field] > 2_147_483_647:
            raise InvalidEnvelope(f"{field} is invalid")
    if not isinstance(body["deadline_at"], str):
        raise InvalidEnvelope("deadline_at is invalid")
    try:
        deadline = datetime.fromisoformat(body["deadline_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEnvelope("deadline_at is invalid") from exc
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise InvalidEnvelope("deadline_at must include a timezone")
    deadline = deadline.astimezone(timezone.utc)
    canonical = {
        "version": 1, "job_id": str(job_id), "owner_id": str(owner_id),
        "generation": body["generation"], "attempt_generation": body["attempt_generation"],
        "deadline_at": body["deadline_at"],
    }
    digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {**canonical, "deadline": deadline}, digest


def _receipt_response(receipt: BackgroundJobExecution) -> dict[str, Any]:
    deadline = receipt.deadline_at
    if deadline is not None and deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=timezone.utc)
    return {
        "job_id": str(receipt.job_id),
        "attempt_generation": receipt.attempt_generation,
        "owner_id": str(receipt.owner_id) if receipt.owner_id else None,
        "generation": receipt.worker_generation,
        "deadline_at": deadline.isoformat().replace("+00:00", "Z") if deadline else None,
        "executor_instance_id": str(receipt.executor_instance_id),
        "state": receipt.state,
        "accepted_at": receipt.accepted_at.isoformat() if receipt.accepted_at else None,
        "started_at": receipt.started_at.isoformat() if receipt.started_at else None,
        "finished_at": receipt.finished_at.isoformat() if receipt.finished_at else None,
        "result": receipt.result,
        "error_code": receipt.error_code,
    }


def _auth(request: Request) -> None:
    token = settings.job_executor_token.get_secret_value() if settings.job_executor_token else None
    if not token:
        raise HTTPException(status_code=503, detail="Executor authentication is unavailable")
    supplied = request.headers.get("authorization", "")
    expected = f"Bearer {token}"
    if not hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _validate_executor_runtime() -> None:
    token = settings.job_executor_token.get_secret_value() if settings.job_executor_token else None
    if not token or not 16 <= len(token) <= 4096 or any(ord(ch) < 0x21 or ord(ch) > 0x7E for ch in token):
        raise RuntimeError("JOB_EXECUTOR_TOKEN must be configured with a valid service token")
    if not settings.gemini_api_key or not settings.gemini_api_key.strip():
        raise RuntimeError("GEMINI_API_KEY must be configured for background job execution")
    settings.validate_runtime_services()


async def _read_body_bounded(request: Request) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > MAX_ENVELOPE_BYTES:
                raise HTTPException(status_code=413, detail="Request body exceeds 8192 bytes")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid content length") from exc
    chunks = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_ENVELOPE_BYTES:
            raise HTTPException(status_code=413, detail="Request body exceeds 8192 bytes")
        chunks.append(chunk)
    return b"".join(chunks)


class PythonJobExecutor:
    def __init__(self):
        self.instance_id = EXECUTOR_INSTANCE_ID
        self.orchestrator = None
        self.alerts = None

    async def accept(self, envelope: dict[str, Any], digest: str) -> tuple[dict, bool]:
        key = (uuid.UUID(envelope["job_id"]), envelope["attempt_generation"])
        async with AsyncSessionLocal() as session:
            receipt = await session.get(BackgroundJobExecution, key)
            if receipt is not None:
                if receipt.envelope_sha256 != digest:
                    raise HTTPException(status_code=409, detail="Execution attempt conflicts with its receipt")
                return _receipt_response(receipt), False
            if _ACTIVE:
                raise HTTPException(status_code=503, detail="Executor is busy")
            now_utc = datetime.now(timezone.utc)
            if (envelope["deadline"] <= now_utc
                    or envelope["deadline"] > now_utc + timedelta(seconds=MAX_DEADLINE_SECONDS)):
                raise HTTPException(status_code=422, detail="deadline_at is outside the allowed window")

            status = await session.scalar(select(BackgroundWorkerStatus).where(
                BackgroundWorkerStatus.id == 1).with_for_update())
            # A competing identical POST may have waited on the worker row
            # after its first receipt lookup, so issue a fresh query now. The
            # explicit SELECT also avoids relying on a cached negative Session.get.
            receipt = (await session.execute(select(BackgroundJobExecution).where(
                BackgroundJobExecution.job_id == key[0],
                BackgroundJobExecution.attempt_generation == key[1],
            ).with_for_update().execution_options(populate_existing=True))).scalar_one_or_none()
            if receipt is not None:
                if receipt.envelope_sha256 != digest:
                    raise HTTPException(status_code=409, detail="Execution attempt conflicts with its receipt")
                return _receipt_response(receipt), False
            now = await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'"))
            if (status is None or status.status != "running"
                    or status.owner_id != uuid.UUID(envelope["owner_id"])
                    or status.generation != envelope["generation"]
                    or status.lock_backend_pid is None or status.heartbeat_at is None
                    or status.heartbeat_at > now
                    or now - status.heartbeat_at > timedelta(seconds=HEARTBEAT_STALE_SECONDS)):
                raise HTTPException(status_code=409, detail="Worker lease is not current")
            job = await session.scalar(select(BackgroundJob).where(
                BackgroundJob.id == key[0]).with_for_update())
            if (job is None or job.state != "running"
                    or job.owner_generation != envelope["generation"]
                    or job.attempt_generation != envelope["attempt_generation"]):
                raise HTTPException(status_code=409, detail="Job attempt is not current")
            try:
                payload = validate_payload(job.kind, job.payload or {})
            except ValueError as exc:
                raise HTTPException(status_code=422, detail="Stored job is invalid") from exc
            if (envelope["deadline"] <= now.replace(tzinfo=timezone.utc)
                    or envelope["deadline"] > now.replace(tzinfo=timezone.utc) + timedelta(seconds=MAX_DEADLINE_SECONDS)):
                raise HTTPException(status_code=422, detail="deadline_at is outside the allowed window")
            fence = RemoteJobFence(
                job_id=job.id, owner_id=status.owner_id, generation=status.generation,
                attempt_generation=job.attempt_generation, backend_pid=status.lock_backend_pid,
                deadline_at=envelope["deadline"].replace(tzinfo=None),
            )
            try:
                async with fence.transaction_fence(session, job.id, job.attempt_generation):
                    session.add(BackgroundJobExecution(
                        job_id=job.id, attempt_generation=job.attempt_generation,
                        envelope_sha256=digest, executor_instance_id=self.instance_id, state="accepted",
                        owner_id=status.owner_id, worker_generation=status.generation,
                        deadline_at=envelope["deadline"].replace(tzinfo=None),
                    ))
                    await session.commit()
            except StaleWorker as exc:
                raise HTTPException(status_code=409, detail="Worker advisory lease is not current") from exc
            receipt = await session.get(BackgroundJobExecution, key)
            response = _receipt_response(receipt)
            response["kind"] = job.kind
            response["payload"] = payload
            response["deadline_at"] = envelope["deadline_at"]
            response["owner_id"] = envelope["owner_id"]
            response["generation"] = envelope["generation"]
            response["backend_pid"] = status.lock_backend_pid
            return response, True

    async def launch(self, accepted: dict):
        key = (uuid.UUID(accepted["job_id"]), accepted["attempt_generation"])
        if len(_ACTIVE) >= 1:
            # A receipt is already durable; leave it accepted for the Go caller
            # to observe rather than run multiple attempts in one process.
            return
        task = asyncio.create_task(self._run(accepted), name=f"python-job-{key[0]}-{key[1]}")
        _ACTIVE[key] = task
        task.add_done_callback(lambda _: _ACTIVE.pop(key, None))

    async def _run(self, accepted: dict):
        started_monotonic = asyncio.get_running_loop().time()
        job_id = uuid.UUID(accepted["job_id"])
        attempt = accepted["attempt_generation"]
        owner_id = uuid.UUID(accepted["owner_id"])
        deadline = datetime.fromisoformat(accepted["deadline_at"].replace("Z", "+00:00")).astimezone(timezone.utc)
        fence = RemoteJobFence(
            job_id=job_id, owner_id=owner_id, generation=accepted["generation"],
            attempt_generation=attempt, backend_pid=accepted["backend_pid"],
            deadline_at=deadline.replace(tzinfo=None),
        )
        fence_context = JobExecutionFence(fence, job_id, attempt)
        key = (job_id, attempt)
        _ACTIVE_FENCES[key] = fence_context
        state, error_code, result = "succeeded", None, None
        try:
            async with _execution_context(fence_context):
                await self._set_receipt_state(job_id, attempt, "running", started=True)
                async with AsyncSessionLocal() as session:
                    job = await session.get(BackgroundJob, job_id)
                    if job is None:
                        raise StaleWorker("job no longer exists")
                    # Detach values needed by execution from this short read session.
                    job.kind, job.payload = job.kind, dict(job.payload or {})
                    if job.kind == "article_reprocess" and job.initialized_at is None:
                        async with fence.transaction_fence(session, job.id, attempt):
                            article_ids = job.payload.get("article_ids", [])
                            fields = {
                                "processing_state": "pending", "processing_attempts": 0,
                                "next_attempt_at": None, "last_processing_error": None,
                                "processed": False,
                            }
                            if job.payload.get("force", True):
                                fields.update(summary_processed=False, category_processed=False, ad_processed=False)
                            await session.execute(update(Article).where(Article.id.in_(article_ids)).values(**fields))
                            job.initialized_at = datetime.now(timezone.utc).replace(tzinfo=None)
                            await session.commit()
                from .orchestrator import reserve_processing_cycle, release_processing_cycle
                reservation = reserve_processing_cycle()
                if reservation is None:
                    result = {"status": "deferred"}
                else:
                    try:
                        try:
                            if self.orchestrator is None:
                                from .orchestrator import NewsOrchestrator
                                self.orchestrator = NewsOrchestrator()
                            await self.orchestrator.start()
                            from .services.background_execution import DurableBackgroundWorker
                            timeout_worker = DurableBackgroundWorker(session_factory=AsyncSessionLocal)
                            configured_timeout = await timeout_worker._job_timeout_seconds(job)
                            timeout = min(float(configured_timeout),
                                          (deadline - datetime.now(timezone.utc)).total_seconds())
                            if timeout <= 0:
                                raise asyncio.TimeoutError
                            value = await asyncio.wait_for(
                                execute_job(job, self.orchestrator, reservation), timeout=timeout
                            )
                            async with AsyncSessionLocal() as session:
                                db_now = await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'"))
                            if db_now >= deadline.replace(tzinfo=None):
                                raise asyncio.TimeoutError
                            result = _bounded_result(job.kind, value)
                            if job.kind == "source_test" and not result.get("connection_ok"):
                                state, error_code = "failed", "worker_error"
                        finally:
                            # Keep the cache's established persistence behavior
                            # for failed jobs, with this attempt's publication fence.
                            if fence_context.active:
                                from .services.domain_stability_tracker import get_stability_tracker
                                try:
                                    tracker = await get_stability_tracker()
                                    await tracker.flush_fenced_state(fence_context)
                                except StaleWorker:
                                    raise
                                except Exception:
                                    # The cache was historically best-effort;
                                    # its I/O failure must not fail a news job.
                                    logger.warning("Could not publish fenced domain stability cache", exc_info=True)
                    finally:
                        release_processing_cycle(reservation)
        except asyncio.CancelledError:
            state = "delivery_unknown" if accepted["kind"] and job_may_deliver(accepted["kind"], accepted["payload"]) else "interrupted"
            error_code = "external_outcome_unknown" if state == "delivery_unknown" else "worker_interrupted"
            result = {"status": "warning"}
        except asyncio.TimeoutError:
            state = "delivery_unknown" if job_may_deliver(accepted["kind"], accepted["payload"]) else "failed"
            error_code = "external_outcome_unknown" if state == "delivery_unknown" else "worker_error"
            result = {"status": "warning"}
        except AttemptDeadlineExpired:
            state = "delivery_unknown" if job_may_deliver(accepted["kind"], accepted["payload"]) else "failed"
            error_code = "external_outcome_unknown" if state == "delivery_unknown" else "worker_error"
            result = {"status": "warning"}
        except StaleWorker:
            logger.warning("Executor stopped writing after lease loss (job_id=%s attempt=%s)", job_id, attempt)
            return
        except Exception:
            logger.exception("Python job execution failed (job_id=%s attempt=%s)", job_id, attempt)
            state = "delivery_unknown" if job_may_deliver(accepted["kind"], accepted["payload"]) else "failed"
            error_code = "external_outcome_unknown" if state == "delivery_unknown" else "worker_error"
            result = {"status": "warning"}
        finally:
            fence_context.active = False
        elapsed = asyncio.get_running_loop().time() - started_monotonic
        if not isinstance(result, dict):
            result = {"status": "warning"}
        result = dict(result)
        result["duration_seconds"] = round(min(24 * 60 * 60, max(0.0, elapsed)), 3)
        try:
            receipt_fence = RemoteJobFence(
                job_id=job_id, owner_id=owner_id, generation=accepted["generation"],
                attempt_generation=attempt, backend_pid=accepted["backend_pid"],
                deadline_at=deadline.replace(tzinfo=None), allow_after_deadline=True,
            )
            async with _execution_context(JobExecutionFence(receipt_fence, job_id, attempt)):
                await self._set_receipt_state(
                    job_id, attempt, state, result=result,
                    error_code=error_code, finished=True,
                )
        except StaleWorker:
            logger.warning("Executor receipt remains nonterminal after lease loss (job_id=%s attempt=%s)", job_id, attempt)
        finally:
            _ACTIVE_FENCES.pop(key, None)

    async def _set_receipt_state(self, job_id, attempt, state, *, result=None, error_code=None,
                                 started=False, finished=False):
        async with AsyncSessionLocal() as session:
            fence = current_job_fence.get()
            if fence is None or not fence.active:
                raise StaleWorker("execution fence is no longer active")
            async with fence.lease.transaction_fence(session, job_id, attempt):
                receipt = await session.get(BackgroundJobExecution, (job_id, attempt), with_for_update=True)
                if receipt is None:
                    raise StaleWorker("execution receipt no longer exists")
                receipt.state = state
                if started:
                    receipt.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
                if finished:
                    receipt.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
                if result is not None:
                    receipt.result = _safe_result(result)
                receipt.error_code = error_code
                await session.commit()

    async def apply_legacy_completion(self, job_id: uuid.UUID, attempt: int):
        key = (job_id, attempt)
        event_lock = _LEGACY_EVENT_LOCKS.setdefault(key, asyncio.Lock())
        async with event_lock:
            should_alert = False
            async with AsyncSessionLocal() as session:
                worker_status = await session.scalar(select(BackgroundWorkerStatus).where(
                    BackgroundWorkerStatus.id == 1).with_for_update().execution_options(
                        populate_existing=True))
                if worker_status is None or worker_status.lock_backend_pid is None:
                    raise HTTPException(status_code=409, detail="Worker lease is not current")
                try:
                    await _assert_remote_worker_lease(
                        session, worker_status.owner_id, worker_status.generation,
                        worker_status.lock_backend_pid,
                    )
                except StaleWorker as exc:
                    raise HTTPException(status_code=409, detail="Worker lease is not current") from exc
                job = await session.scalar(select(BackgroundJob).where(
                    BackgroundJob.id == job_id).with_for_update().execution_options(
                        populate_existing=True))
                if (job is None or job.state not in {"succeeded", "failed", "delivery_unknown"}
                        or attempt < 0 or job.attempt_generation != attempt):
                    raise HTTPException(status_code=409, detail="Job is not in the requested terminal attempt")
                receipt = await session.get(BackgroundJobExecution, key, with_for_update=True)
                if receipt is None:
                    marker = hashlib.sha256(f"legacy-recovery:{job_id}:{attempt}".encode()).hexdigest()
                    receipt = BackgroundJobExecution(
                        job_id=job_id, attempt_generation=attempt, envelope_sha256=marker,
                        executor_instance_id=self.instance_id, state=job.state,
                        owner_id=None, worker_generation=job.owner_generation,
                        deadline_at=None,
                        result=_safe_result(job.result), error_code=job.error_code,
                    )
                    session.add(receipt)
                    await session.flush()
                elif receipt.state in {"accepted", "running"}:
                    # Go may have recovered an executor that stopped before
                    # producing a terminal receipt. Its persisted job state is
                    # now authoritative for this attempt.
                    receipt.state = job.state
                    receipt.result = _safe_result(job.result)
                    receipt.error_code = job.error_code
                    receipt.finished_at = job.finished_at

                if receipt.legacy_completion_applied_at is None:
                    payload = job.payload if isinstance(job.payload, dict) else {}
                    schedule_id = payload.get("schedule_id")
                    if schedule_id:
                        schedule = await session.get(ScheduleSettings, schedule_id, with_for_update=True)
                        if schedule and schedule.is_running and not (
                            job.created_at and schedule.updated_at and schedule.updated_at > job.created_at
                        ):
                            now = datetime.utcnow()
                            schedule.is_running = False
                            schedule.last_finished_at = now
                            result = job.result if isinstance(job.result, dict) else {}
                            status = result.get("status")
                            schedule.last_status = "delivery_unknown" if job.state == "delivery_unknown" else (
                                "success" if job.state == "succeeded" and status not in {"warning", "deferred"}
                                else status if status in {"warning", "deferred"} else "failed"
                            )
                            schedule.last_error = "Background job failed" if job.state != "succeeded" else None
                            schedule.last_duration_seconds = result.get("duration_seconds")
                            if schedule.last_status == "success":
                                schedule.last_success_at = now
                            if schedule.enabled:
                                if schedule.last_status == "deferred":
                                    schedule.next_run = now + timedelta(minutes=5)
                                elif schedule.last_status == "failed":
                                    schedule.next_run = now + timedelta(minutes=10)
                            else:
                                schedule.next_run = None
                    receipt.legacy_completion_applied_at = datetime.utcnow()

                schedule_id = (job.payload or {}).get("schedule_id") if isinstance(job.payload, dict) else None
                schedule_key = getattr(job, "schedule_key", None)
                alert_key = f"scheduler-warning-{schedule_key or schedule_id}"
                if not (schedule_id or schedule_key):
                    receipt.legacy_alert_state = receipt.legacy_alert_state or "not_needed"
                elif receipt.legacy_alert_state is None:
                    result = job.result if isinstance(job.result, dict) else {}
                    failure = job.state != "succeeded" or result.get("status") in {"warning", "deferred"}
                    if failure:
                        receipt.legacy_alert_state = "attempting"
                        alert_kind, should_alert = "failure", True
                    elif self.alerts is not None and self.alerts.is_active(alert_key):
                        receipt.legacy_alert_state = "attempting"
                        alert_kind, should_alert = "recovery", True
                    else:
                        receipt.legacy_alert_state = "not_needed"
                elif receipt.legacy_alert_state == "attempting" and key not in _ALERT_ACTIVE:
                    # A previous process may have sent the alert before it
                    # stopped. Preserve uncertainty and never replay it blindly.
                    receipt.legacy_alert_state = "unknown"
                await session.commit()
                response = {
                    "job_id": str(job_id), "attempt_generation": attempt,
                    "legacy_completion_applied": receipt.legacy_completion_applied_at is not None,
                    "alert_state": receipt.legacy_alert_state,
                }

            if should_alert:
                _ALERT_ACTIVE.add(key)
                try:
                    if self.alerts is None:
                        if self.orchestrator is None:
                            from .orchestrator import NewsOrchestrator
                            self.orchestrator = NewsOrchestrator()
                        from .services.operational_alerts import OperationalAlertManager
                        self.alerts = OperationalAlertManager(self.orchestrator.send_operational_alert)
                    if alert_kind == "failure":
                        sent = await self.alerts.failure(
                            alert_key,
                            f"Scheduled task {job.kind} needs attention",
                            "The scheduled task failed, was deferred, or completed with warnings.",
                        )
                    else:
                        sent = await self.alerts.recovery(
                            alert_key,
                            f"Scheduled task {job.kind} recovered",
                            "The latest scheduled run completed without warnings.",
                        )
                finally:
                    _ALERT_ACTIVE.discard(key)
                async with AsyncSessionLocal() as session:
                    receipt = await session.get(BackgroundJobExecution, key, with_for_update=True)
                    if receipt and receipt.legacy_alert_state == "attempting":
                        receipt.legacy_alert_state = "delivered" if sent else "unknown"
                        await session.commit()
                        response["alert_state"] = receipt.legacy_alert_state
            return response

    def status(self):
        return {"executor_instance_id": str(self.instance_id), "active_executions": len(_ACTIVE),
                "ready": _EXECUTOR_GUARD_HEALTHY}


@asynccontextmanager
async def _execution_context(fence):
    token = current_job_fence.set(fence)
    try:
        yield
    finally:
        fence.active = False
        current_job_fence.reset(token)


executor = PythonJobExecutor()
app = FastAPI(title="Evening News private job executor", docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/internal/v1/status")
async def get_status(request: Request):
    _auth(request)
    await _require_executor_guard()
    try:
        _validate_executor_runtime()
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
            await session.execute(text("""
                SELECT job_id, attempt_generation, envelope_sha256, executor_instance_id,
                       owner_id, worker_generation, deadline_at, state, accepted_at,
                       started_at, finished_at, result, error_code,
                       legacy_completion_applied_at, legacy_alert_state
                FROM background_job_executions LIMIT 0
            """))
            await session.execute(text("""
                SELECT id, generation, owner_id, lock_backend_pid, heartbeat_at, status
                FROM background_worker_status WHERE id=1
            """))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Executor database is unavailable") from exc
    return executor.status()


@app.get("/internal/v1/job-executions/{job_id}/policy")
async def get_execution_policy(job_id: uuid.UUID, request: Request):
    """Expose only Python's configured timeout so Go can set the immutable deadline."""
    _auth(request)
    await _require_executor_guard()
    async with AsyncSessionLocal() as session:
        job = await session.get(BackgroundJob, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        from .services.background_execution import DurableBackgroundWorker
        worker = DurableBackgroundWorker(session_factory=AsyncSessionLocal)
        timeout = await worker._job_timeout_seconds(job)
    return {"timeout_seconds": timeout}


@app.post("/internal/v1/job-executions")
async def accept_execution(request: Request):
    _auth(request)
    await _require_executor_guard()
    raw = await _read_body_bounded(request)
    try:
        envelope, digest = _parse_envelope(raw)
    except InvalidEnvelope as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    async with _ADMISSION_LOCK:
        accepted, created = await executor.accept(envelope, digest)
        if created:
            await executor.launch(accepted)
            accepted = {key: accepted[key] for key in (
                "job_id", "attempt_generation", "executor_instance_id", "state", "accepted_at",
            )}
    return JSONResponse(status_code=202, content={**accepted, "duplicate": not created})


@app.get("/internal/v1/job-executions/{job_id}/{attempt_generation}")
async def read_execution(job_id: uuid.UUID, attempt_generation: int, request: Request):
    _auth(request)
    await _require_executor_guard()
    if attempt_generation <= 0 or attempt_generation > 2_147_483_647:
        raise HTTPException(status_code=404, detail="Execution receipt not found")
    async with AsyncSessionLocal() as session:
        receipt = await session.get(BackgroundJobExecution, (job_id, attempt_generation))
    if receipt is None:
        raise HTTPException(status_code=404, detail="Execution receipt not found")
    return _receipt_response(receipt)


@app.post("/internal/v1/job-executions/{job_id}/{attempt_generation}/events")
async def apply_job_event(job_id: uuid.UUID, attempt_generation: int, request: Request):
    _auth(request)
    await _require_executor_guard()
    try:
        event = json.loads(await _read_body_bounded(request), object_pairs_hook=_reject_duplicate_pairs)
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidEnvelope) as exc:
        raise HTTPException(status_code=422, detail="Event body is invalid") from exc
    if not isinstance(event, dict) or set(event) != {"event"} or event.get("event") != "finalized":
        raise HTTPException(status_code=422, detail="Unsupported job event")
    return await executor.apply_legacy_completion(job_id, attempt_generation)


@app.post("/internal/v1/worker-events")
async def apply_worker_event(request: Request):
    _auth(request)
    await _require_executor_guard()
    try:
        event = json.loads(await _read_body_bounded(request), object_pairs_hook=_reject_duplicate_pairs)
        if (not isinstance(event, dict) or set(event) != {"event", "owner_id", "generation"}
                or event.get("event") != "recovered"):
            raise InvalidEnvelope("unsupported worker event")
        owner_id = uuid.UUID(event["owner_id"])
        if str(owner_id) != event["owner_id"] or type(event["generation"]) is not int or event["generation"] <= 0:
            raise InvalidEnvelope("worker identity is invalid")
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidEnvelope, ValueError, TypeError, AttributeError) as exc:
        raise HTTPException(status_code=422, detail="Worker event body is invalid") from exc

    async with AsyncSessionLocal() as session:
        status = await session.get(BackgroundWorkerStatus, 1)
        if status is None or status.lock_backend_pid is None:
            raise HTTPException(status_code=409, detail="Worker lease is not current")
        try:
            await _assert_remote_worker_lease(
                session, owner_id, event["generation"], status.lock_backend_pid,
            )
        except StaleWorker as exc:
            raise HTTPException(status_code=409, detail="Worker lease is not current") from exc
        active_jobs = (await session.execute(select(BackgroundJob.id, BackgroundJob.payload).where(
            BackgroundJob.state.in_(("queued", "running", "interrupted"))
        ))).all()
        active_schedule_ids = {
            payload.get("schedule_id") for _, payload in active_jobs
            if isinstance(payload, dict) and isinstance(payload.get("schedule_id"), int)
        }
        schedules = (await session.execute(select(ScheduleSettings).where(
            ScheduleSettings.is_running.is_(True)
        ).with_for_update())).scalars().all()
        now = datetime.utcnow()
        recovered = 0
        from .services.background_jobs import BackgroundWorkerLease
        for schedule in schedules:
            if schedule.id in active_schedule_ids:
                continue
            config = schedule.task_config if isinstance(schedule.task_config, dict) else {}
            delivery_may_have_started = schedule.task_name == "telegram_digest" or (
                schedule.task_name == "news_digest" and config.get("send_telegram", True) is not False
            )
            schedule.is_running = False
            schedule.last_finished_at = now
            if delivery_may_have_started:
                schedule.last_status = "delivery_unknown"
                schedule.last_error = "External delivery outcome is unknown after service restart"
                if schedule.enabled:
                    if schedule.next_run is None or schedule.next_run <= now:
                        schedule.next_run = BackgroundWorkerLease._next_schedule_occurrence(schedule, config, now)
                else:
                    schedule.next_run = None
            else:
                schedule.last_status = "interrupted"
                schedule.last_error = "Service restarted while task was marked running"
                if not schedule.enabled:
                    schedule.next_run = None
                elif schedule.next_run is None:
                    schedule.next_run = BackgroundWorkerLease._next_schedule_occurrence(schedule, config, now)
            recovered += 1
        await session.commit()
    return {"event": "recovered", "schedules_reconciled": recovered}


@app.post("/internal/v1/job-executions/{job_id}/{attempt_generation}/cancel")
async def cancel_execution(job_id: uuid.UUID, attempt_generation: int, request: Request):
    _auth(request)
    await _require_executor_guard()
    key = (job_id, attempt_generation)
    task = _ACTIVE.get(key)
    stopped = task is None or task.done()
    if task is not None and not task.done():
        active_fence = _ACTIVE_FENCES.get(key)
        if active_fence is not None:
            # Child tasks inherit the same fence object; revoke it before
            # cancellation so a cancellation-resistant child cannot publish.
            active_fence.active = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        stopped = task.done()
    async with AsyncSessionLocal() as session:
        receipt = await session.get(BackgroundJobExecution, (job_id, attempt_generation))
    if receipt is None:
        raise HTTPException(status_code=404, detail="Execution receipt not found")
    return {
        **_receipt_response(receipt),
        "execution_stopped": stopped,
        "current_executor_instance_id": str(executor.instance_id),
    }


async def _serve():
    _validate_executor_runtime()
    try:
        await _acquire_executor_guard()
    except Exception:
        await engine.dispose()
        raise
    config = uvicorn.Config(app, host=settings.job_executor_host, port=settings.job_executor_port,
                            log_level=settings.log_level.lower(), access_log=False)
    server = uvicorn.Server(config)
    global _EXECUTOR_SERVER
    _EXECUTOR_SERVER = server
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, setattr, server, "should_exit", True)
        except NotImplementedError:
            pass
    guard_monitor = asyncio.create_task(_watch_executor_guard(server))
    try:
        await server.serve()
    finally:
        guard_monitor.cancel()
        try:
            await guard_monitor
        except asyncio.CancelledError:
            pass
        await _shutdown_executor()
        _EXECUTOR_SERVER = None


async def _shutdown_executor(*, drain_seconds=None):
    """Cancel and drain accepted work before stopping shared services and DB."""
    timeout = (settings.worker_shutdown_drain_seconds if drain_seconds is None
               else max(0, float(drain_seconds)))
    tasks = list(_ACTIVE.items())
    for fence in tuple(_ACTIVE_FENCES.values()):
        fence.active = False
    for _, task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        _, pending = await asyncio.wait([task for _, task in tasks], timeout=timeout)
        if pending:
            logger.error("Executor shutdown drain timed out with %d active execution(s)", len(pending))
            for task in pending:
                task.cancel()
            _, pending = await asyncio.wait(pending, timeout=min(2.0, max(0.1, timeout)))
            if pending:
                logger.error("Executor shutdown left %d cancellation-resistant execution(s)", len(pending))
        for key, task in tasks:
            if task.done():
                _ACTIVE.pop(key, None)

    if executor.orchestrator is not None:
        try:
            await asyncio.wait_for(executor.orchestrator.stop(), timeout=max(1.0, min(10.0, timeout)))
        except asyncio.TimeoutError:
            logger.error("Orchestrator shutdown timed out")
        except Exception:
            logger.exception("Orchestrator shutdown failed")
        finally:
            executor.orchestrator = None
    await _release_executor_guard()
    await engine.dispose()


def main():
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
