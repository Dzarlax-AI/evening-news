"""Private HTTP entrypoint for executing Go-owned durable job attempts."""

import asyncio
import hashlib
import hmac
import json
import logging
import math
import re
import signal
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from starlette.datastructures import UploadFile
from fastapi.responses import JSONResponse, FileResponse
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .config import settings
from .database import AsyncSessionLocal, engine
from .models import Article
from .services.background_jobs import StaleWorker
from .services.job_fence import JobExecutionFence, current_job_fence
from .services import scheduled_source_alerts
from .services.web_backend import (
    BackupCleanupRequest, BackupFilename, BackupScheduleRequest, BackupTooLarge,
    CategoryAnalysisRequest, EmptyRequest, analyze_categories, backup_path,
    cleanup_backups, invalidate_category_cache, list_backups, reset_telegram_service,
    save_upload, telegram_configuration, telegram_test,
)

logger = logging.getLogger(__name__)
app = FastAPI(title="Evening News private job executor", docs_url=None, redoc_url=None, openapi_url=None)

MAX_ENVELOPE_BYTES = 8192
MAX_SELECTOR_CALLBACK_BYTES = 5 * 1024 * 1024 + MAX_ENVELOPE_BYTES
MAX_SELECTOR_CALLBACK_HTML_BYTES = 5 * 1024 * 1024
MAX_ARTICLE_AI_CALLBACK_BYTES = 5 * 1024 * 1024 + MAX_ENVELOPE_BYTES
MAX_ARTICLE_AI_RESPONSE_BYTES = 1024 * 1024
_ARTICLE_AI_CACHE = OrderedDict()
_ARTICLE_AI_CACHE_BYTES = 0
_ARTICLE_AI_ACTIVE = set()
_ARTICLE_AI_ADMISSION_LOCK = asyncio.Lock()
_ARTICLE_AI_CONTEXT_PATTERNS = OrderedDict()
MAX_ARTICLE_AI_ACTIVE = 16
MAX_ARTICLE_AI_CACHE_ENTRIES = 64
MAX_ARTICLE_AI_CACHE_BYTES = 1024 * 1024
MAX_ARTICLE_AI_CONTEXT_PATTERNS = 128
ARTICLE_EXTRACTION_METHODS = frozenset({
    "source_content", "enhanced_selectors", "readability", "browser_rendering",
    "browser_heuristics", "fallback_extraction", "json_ld", "open_graph",
    "meta_description", "enhanced_heuristics", "reddit_json", "reddit_linked_article",
    "learned_pattern_browser_rendering", "learned_pattern_browser_heuristics",
})
_EXECUTOR_GUARD_CONNECTION: AsyncConnection | None = None
_EXECUTOR_GUARD_BACKEND_PID: int | None = None
_EXECUTOR_GUARD_HEALTHY = False
_EXECUTOR_GUARD_LOCK = asyncio.Lock()
_EXECUTOR_SERVER = None
_EXECUTOR_GUARD_QUERY_TIMEOUT = 2.0
MAX_PRIVATE_JSON_BODY_BYTES = 64 * 1024
MAX_PRIVATE_UPLOAD_BODY_BYTES = 64 * 1024 * 1024 + 64 * 1024
PRIVATE_JSON_TIMEOUT_SECONDS = 70.0
PRIVATE_STREAM_TIMEOUT_SECONDS = 120.0
_WORKER_DISABLED_BACKUP_OPERATIONS = {
    ("POST", "/internal/web/v1/backups/upload"):
        "Backup uploads are disabled in worker mode; use offline maintenance with the worker stopped.",
    ("POST", "/internal/web/v1/backups/cleanup"):
        "Backup cleanup is disabled in worker mode; use offline maintenance with the worker stopped.",
    ("POST", "/internal/web/v1/backups/restore"):
        "Restore is disabled in worker mode; stop the worker and use offline maintenance.",
}


class WebBackendBoundaryMiddleware:
    """Authenticate and bound private web-backend requests before parsing bodies."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope.get("path", "").startswith("/internal/web/v1/"):
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        auth_values = [value for name, value in headers if name.lower() == b"authorization"]
        token = settings.web_backend_token.get_secret_value() if settings.web_backend_token else None
        if not token:
            await self._reject(send, 503, "Web backend authentication is unavailable")
            return
        expected = f"Bearer {token}".encode("utf-8")
        if len(auth_values) != 1 or not hmac.compare_digest(auth_values[0], expected):
            await self._reject(send, 401, "Unauthorized")
            return

        path = scope.get("path", "")
        if settings.background_execution_mode == "worker":
            detail = _WORKER_DISABLED_BACKUP_OPERATIONS.get((scope.get("method", ""), path))
            if detail:
                # Preserve the legacy precedence and reject before reading or
                # spooling an upload body, parsing restore input, or touching
                # the backup directory.
                await self._reject(send, 409, detail)
                return
        is_upload = path == "/internal/web/v1/backups/upload" and scope.get("method") == "POST"
        limit = MAX_PRIVATE_UPLOAD_BODY_BYTES if is_upload else MAX_PRIVATE_JSON_BODY_BYTES
        lengths = [value for name, value in headers if name.lower() == b"content-length"]
        if len(lengths) > 1:
            await self._reject(send, 400, "Invalid Content-Length")
            return
        if lengths:
            try:
                content_length = int(lengths[0])
            except ValueError:
                await self._reject(send, 400, "Invalid Content-Length")
                return
            if content_length < 0:
                await self._reject(send, 400, "Invalid Content-Length")
                return
            if content_length > limit:
                await self._reject(send, 413, "Request body exceeds limit")
                return

        response_started = False

        async def tracked_send(message):
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        timeout = PRIVATE_STREAM_TIMEOUT_SECONDS if is_upload or scope.get("method") == "GET" else PRIVATE_JSON_TIMEOUT_SECONDS
        try:
            async with asyncio.timeout(timeout):
                # Fully validate and spool the body before FastAPI sees it. This
                # makes chunked/lying Content-Length uploads obey the same cap
                # and prevents multipart parsing from spooling an oversized body.
                import tempfile
                with tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b") as spool:
                    received = 0
                    request_complete = False
                    while True:
                        message = await receive()
                        if message.get("type") == "http.disconnect":
                            # Never dispatch a partially received private
                            # mutation. The peer is gone, so there is no useful
                            # response to send and no handler side effect.
                            return
                        if message.get("type") != "http.request":
                            continue
                        chunk = message.get("body", b"")
                        received += len(chunk)
                        if received > limit:
                            await self._reject(send, 413, "Request body exceeds limit")
                            return
                        spool.write(chunk)
                        if not message.get("more_body", False):
                            request_complete = True
                            break
                    if not request_complete:
                        return
                    spool.seek(0)
                    remaining = received
                    empty_request_pending = received == 0

                    async def replay_receive():
                        nonlocal remaining, empty_request_pending
                        if remaining:
                            chunk = spool.read(min(64 * 1024, remaining))
                            remaining -= len(chunk)
                            return {"type": "http.request", "body": chunk,
                                    "more_body": remaining > 0}
                        if empty_request_pending:
                            empty_request_pending = False
                            return {"type": "http.request", "body": b"", "more_body": False}
                        # The replayed request body is complete. Continue
                        # observing the real client receive channel so
                        # FileResponse/StreamingResponse can distinguish a
                        # live connection from an actual disconnect.
                        return await receive()

                    await self.app(scope, replay_receive, tracked_send)
        except TimeoutError:
            if not response_started:
                detail = ("Operation timed out; outcome may be unknown. Verify before retrying."
                          if scope.get("method") not in {"GET", "HEAD"}
                          else "Operation timed out")
                await self._reject(send, 504, detail)

    @staticmethod
    async def _reject(send, status, detail):
        body = json.dumps({"detail": detail}, separators=(",", ":")).encode("utf-8")
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


app.add_middleware(WebBackendBoundaryMiddleware)


def _require_embedded_backup_mode(operation: str) -> None:
    """Keep mutation handlers safe when invoked outside the ASGI middleware."""
    detail = _WORKER_DISABLED_BACKUP_OPERATIONS.get(("POST", f"/internal/web/v1/backups/{operation}"))
    if detail and settings.background_execution_mode == "worker":
        raise HTTPException(status_code=409, detail=detail)

# This two-key advisory lock is distinct from the Go worker lease key and its
# retry coordination key. Only one private executor process may own a schema.
_EXECUTOR_GUARD_KEY_XOR = 1145132609


class InvalidEnvelope(ValueError):
    pass


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
    from .services.domain_operations import cancel_active_operations
    await cancel_active_operations(timeout=2)
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


class ArticleIngestionCallbackFence:
    """Fence selector-memory writes to both the Python attempt and Go run."""

    def __init__(self, parent, *, run_id: uuid.UUID,
                 executor_instance_id: uuid.UUID, context: JobExecutionFence,
                 source_id: int, source_fingerprint: str):
        self.parent = parent
        self.run_id = run_id
        self.executor_instance_id = executor_instance_id
        self.context = context
        self.source_id = source_id
        self.source_fingerprint = source_fingerprint
        self.job_id = parent.job_id
        self.attempt_generation = parent.attempt_generation

    @asynccontextmanager
    async def transaction_fence(self, session, job_id, attempt_generation):
        if (not self.context.active or job_id != self.job_id
                or attempt_generation != self.attempt_generation):
            raise StaleWorker("article ingestion callback is no longer active")
        async with self.parent.transaction_fence(session, job_id, attempt_generation):
            run = (await session.execute(text("""
                SELECT state FROM article_ingestion_runs
                WHERE id=:run_id AND job_id=:job_id AND attempt_generation=:attempt
                FOR UPDATE
            """), {"run_id": self.run_id, "job_id": self.job_id,
                  "attempt": self.attempt_generation})).one_or_none()
            receipt = (await session.execute(text("""
                SELECT executor_instance_id,owner_id,worker_generation,deadline_at,state
                FROM background_job_executions
                WHERE job_id=:job_id AND attempt_generation=:attempt FOR UPDATE
            """), {"job_id": self.job_id, "attempt": self.attempt_generation})).one_or_none()
            if (run is None or run.state != "running" or receipt is None
                    or receipt.executor_instance_id != self.executor_instance_id
                    or receipt.owner_id != self.parent.owner_id
                    or receipt.worker_generation != self.parent.generation
                    or receipt.deadline_at != self.parent.deadline_at
                    or receipt.state != "running" or not self.context.active):
                raise StaleWorker("article ingestion callback receipt is no longer current")
            source = (await session.execute(text("""
                SELECT name,source_type,url,enabled,config::text AS config_text
                FROM sources WHERE id=:source_id FOR SHARE
            """), {"source_id": self.source_id})).mappings().one_or_none()
            if (source is None or source["source_type"] != "custom" or source["enabled"] is not True
                    or _source_fingerprint(self.source_id, source["name"], source["source_type"],
                                           source["url"], source["enabled"], source["config_text"])
                    != self.source_fingerprint):
                raise StaleWorker("Page Monitor source changed during selector study")
            yield


class ArticleProcessingCallbackFence:
    """Bind callback metadata to one active Go item claim."""

    def __init__(self, lease, *, run_id, envelope_digest, article_id,
                 article_attempt, input_fingerprint):
        self.lease = lease
        self.run_id = run_id
        self.envelope_digest = envelope_digest
        self.article_id = article_id
        self.article_attempt = article_attempt
        self.input_fingerprint = input_fingerprint


def _reject_duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidEnvelope("duplicate JSON field")
        result[key] = value
    return result


def _auth(request: Request) -> None:
    token = settings.job_executor_token.get_secret_value() if settings.job_executor_token else None
    if not token:
        raise HTTPException(status_code=503, detail="Executor authentication is unavailable")
    supplied = request.headers.get("authorization", "")
    expected = f"Bearer {token}"
    if not hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _web_backend_auth(request: Request) -> None:
    token = settings.web_backend_token.get_secret_value() if settings.web_backend_token else None
    if not token:
        raise HTTPException(status_code=503, detail="Web backend authentication is unavailable")
    expected = f"Bearer {token}"
    if not hmac.compare_digest(request.headers.get("authorization", "").encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Unauthorized")


async def _web_backend_ready(request: Request) -> None:
    _web_backend_auth(request)
    await _require_executor_guard()


def _backup_root():
    from pathlib import Path
    return Path("backups").resolve()


@app.post("/internal/web/v1/categories/ai-category-analysis")
async def web_ai_category_analysis(payload: CategoryAnalysisRequest, request: Request):
    await _web_backend_ready(request)
    async with AsyncSessionLocal() as session:
        return await analyze_categories(session, payload)


@app.post("/internal/web/v1/categories/invalidate-cache")
async def web_invalidate_category_cache(payload: EmptyRequest, request: Request):
    await _web_backend_ready(request)
    await invalidate_category_cache()
    return {"success": True}


@app.post("/internal/web/v1/telegram/test")
async def web_telegram_test(payload: EmptyRequest, request: Request):
    await _web_backend_ready(request)
    async with AsyncSessionLocal() as session:
        return await telegram_test(session)


@app.get("/internal/web/v1/telegram/settings")
@app.get("/internal/web/v1/telegram/configuration")
async def web_telegram_settings(request: Request):
    await _web_backend_ready(request)
    async with AsyncSessionLocal() as session:
        return await telegram_configuration(session)


@app.post("/internal/web/v1/telegram/settings-updated")
async def web_telegram_settings_updated(payload: EmptyRequest, request: Request):
    await _web_backend_ready(request)
    reset_telegram_service()
    return {"success": True}


@app.get("/internal/web/v1/backups")
async def web_backup_list(request: Request):
    await _web_backend_ready(request)
    return list_backups(_backup_root())


@app.get("/internal/web/v1/backups/schedule")
async def web_backup_schedule_get(request: Request):
    await _web_backend_ready(request)
    return {"enabled": False, "frequency": "daily", "time": "02:00", "keep_count": 5,
            "last_backup": None, "next_backup": None, "status": "disabled"}


@app.put("/internal/web/v1/backups/schedule")
async def web_backup_schedule_update(payload: BackupScheduleRequest, request: Request):
    await _web_backend_ready(request)
    from datetime import datetime
    return {"message": "Backup schedule updated successfully", "settings": payload.model_dump(),
            "timestamp": datetime.utcnow().isoformat()}


@app.get("/internal/web/v1/backups/{filename}")
async def web_backup_download(filename: str, request: Request):
    await _web_backend_ready(request)
    try:
        path = backup_path(_backup_root(), filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid filename") from exc
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Backup file not found")
    if path.stat().st_size > 64 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Backup exceeds download limit")
    return FileResponse(path, filename=path.name, media_type="application/gzip")


@app.post("/internal/web/v1/backups/upload")
async def web_backup_upload(request: Request):
    await _web_backend_ready(request)
    _require_embedded_backup_mode("upload")
    async with request.form(max_files=1, max_fields=1) as form:
        file = form.get("file")
        if not isinstance(file, UploadFile):
            raise HTTPException(status_code=422, detail="A backup file is required")
        return await _save_web_backup_upload(file)


async def _save_web_backup_upload(file: UploadFile):
    from datetime import datetime
    try:
        size = await save_upload(_backup_root(), file.filename or "", file.file)
    except BackupTooLarge as exc:
        raise HTTPException(status_code=413, detail="Backup exceeds upload limit") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        await file.close()
    return {"message": "Backup uploaded successfully", "filename": file.filename,
            "size": size, "timestamp": datetime.utcnow().isoformat()}


@app.post("/internal/web/v1/backups/restore")
async def web_backup_restore(payload: BackupFilename, request: Request):
    await _web_backend_ready(request)
    _require_embedded_backup_mode("restore")
    from .services.backup_service import BackupService
    from datetime import datetime
    try:
        path = backup_path(_backup_root(), payload.filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid filename") from exc
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Backup file not found")
    restore = getattr(BackupService(), "restore_backup", None)
    if restore is None:
        # The legacy route references this absent method. Do not introduce an
        # unreviewed tar extraction or shell restore implementation here.
        raise HTTPException(status_code=501, detail="Backup restore is not implemented")
    try:
        await restore(path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Backup restore failed") from exc
    return {"message": "Backup restored successfully", "restored_from": payload.filename,
            "timestamp": datetime.utcnow().isoformat()}


@app.post("/internal/web/v1/backups/cleanup")
async def web_backup_cleanup(payload: BackupCleanupRequest, request: Request):
    await _web_backend_ready(request)
    _require_embedded_backup_mode("cleanup")
    return cleanup_backups(_backup_root(), payload.keep_count)


@app.get("/internal/web/v1/system/process-monitor")
async def web_process_monitor(request: Request):
    await _web_backend_ready(request)
    from datetime import datetime
    from .services.process_monitor import ProcessMonitor
    if settings.background_execution_mode == "worker":
        from .services.background_jobs import read_worker_status
        async with AsyncSessionLocal() as session:
            worker = await read_worker_status(session)
        status = {"running": bool(worker and worker["healthy"]), "mode": "worker",
                  "worker_available": bool(worker and worker["healthy"])}
    else:
        status = await ProcessMonitor().get_status()
    return {"status": status, "timestamp": datetime.utcnow().isoformat()}


@app.post("/internal/web/v1/system/process-monitor/cleanup")
async def web_process_cleanup(payload: EmptyRequest, request: Request):
    await _web_backend_ready(request)
    raise HTTPException(status_code=409, detail="Browser process cleanup is owned by the Go collector")


@app.get("/internal/web/v1/system/info")
async def web_system_info(request: Request):
    await _web_backend_ready(request)
    import shutil, subprocess, sys
    from datetime import datetime
    try:
        git = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=".",
                                      stderr=subprocess.DEVNULL, timeout=2).decode().strip()
    except Exception:
        git = "unknown"
    disk = shutil.disk_usage(".")
    return {"python_version": sys.version, "git_commit": git,
            "disk_usage": {"total": disk.total, "used": disk.used, "free": disk.free},
            "timestamp": datetime.utcnow().isoformat()}


@app.post("/internal/web/v1/migrations/run")
async def web_run_migrations(payload: EmptyRequest, request: Request):
    await _web_backend_ready(request)
    from datetime import datetime
    from .migrate import run_migrations
    result = await run_migrations()
    return {"message": "Migrations completed", "result": result,
            "timestamp": datetime.utcnow().isoformat()}


def _article_ai_auth(request: Request) -> None:
    token = settings.article_ai_token.get_secret_value() if settings.article_ai_token else None
    if not token:
        raise HTTPException(status_code=503, detail="Article AI authentication is unavailable")
    supplied = request.headers.get("authorization", "")
    if not hmac.compare_digest(supplied.encode("utf-8"), f"Bearer {token}".encode("utf-8")):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _article_ai_envelope_digest(envelope):
    ordered = {name: envelope[name] for name in (
        "version", "job_id", "attempt_generation", "owner_id", "generation",
        "backend_pid", "executor_instance_id", "deadline_at",
    )}
    return hashlib.sha256(json.dumps(ordered, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()


def _article_ai_input_fingerprint(value):
    identity = {key: value[key] for key in ("title", "content", "url")}
    if "original_context" in value:
        identity["original_context"] = value["original_context"]
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False).replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _article_ai_callback_key(body):
    payload = json.dumps(body["input"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload = payload.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    identity = (body["run_id"], body["article_id"], body["article_attempt"],
                body["input_fingerprint"], body["operation"], hashlib.sha256(payload.encode("utf-8")).hexdigest())
    return identity


def _article_ai_claim_key(body):
    return (body["run_id"], body["article_id"], body["article_attempt"], body["input_fingerprint"])


async def _remember_article_ai_context(body, selector, method):
    async with _ARTICLE_AI_ADMISSION_LOCK:
        key = _article_ai_claim_key(body)
        _ARTICLE_AI_CONTEXT_PATTERNS[key] = (selector, method)
        _ARTICLE_AI_CONTEXT_PATTERNS.move_to_end(key)
        while len(_ARTICLE_AI_CONTEXT_PATTERNS) > MAX_ARTICLE_AI_CONTEXT_PATTERNS:
            _ARTICLE_AI_CONTEXT_PATTERNS.popitem(last=False)


async def _take_article_ai_context(body):
    async with _ARTICLE_AI_ADMISSION_LOCK:
        return _ARTICLE_AI_CONTEXT_PATTERNS.pop(_article_ai_claim_key(body), (None, None))


async def _begin_article_ai_callback(key):
    async with _ARTICLE_AI_ADMISSION_LOCK:
        cached = _ARTICLE_AI_CACHE.get(key)
        if cached is not None:
            _ARTICLE_AI_CACHE.move_to_end(key)
            return cached[0]
        if key in _ARTICLE_AI_ACTIVE:
            raise HTTPException(status_code=429, detail="Article AI callback is already running",
                                headers={"Retry-After": "1"})
        if len(_ARTICLE_AI_ACTIVE) >= MAX_ARTICLE_AI_ACTIVE:
            raise HTTPException(status_code=429, detail="Article AI admission is full",
                                headers={"Retry-After": "1"})
        _ARTICLE_AI_ACTIVE.add(key)
        return None


async def _finish_article_ai_callback(key, response):
    global _ARTICLE_AI_CACHE_BYTES
    async with _ARTICLE_AI_ADMISSION_LOCK:
        _ARTICLE_AI_ACTIVE.discard(key)
        if response is None:
            return
        try:
            size = len(json.dumps(response, ensure_ascii=False, allow_nan=False).encode("utf-8"))
        except (TypeError, ValueError):
            return
        if size > MAX_ARTICLE_AI_CACHE_BYTES:
            return
        previous = _ARTICLE_AI_CACHE.pop(key, None)
        if previous is not None:
            _ARTICLE_AI_CACHE_BYTES -= previous[1]
        _ARTICLE_AI_CACHE[key] = (response, size)
        _ARTICLE_AI_CACHE_BYTES += size
        while (_ARTICLE_AI_CACHE
               and (len(_ARTICLE_AI_CACHE) > MAX_ARTICLE_AI_CACHE_ENTRIES
                    or _ARTICLE_AI_CACHE_BYTES > MAX_ARTICLE_AI_CACHE_BYTES)):
            _, (_, removed_size) = _ARTICLE_AI_CACHE.popitem(last=False)
            _ARTICLE_AI_CACHE_BYTES -= removed_size


def _validate_ai_envelope(envelope):
    required = {"version", "job_id", "attempt_generation", "owner_id", "generation",
                "backend_pid", "executor_instance_id", "deadline_at"}
    if not isinstance(envelope, dict) or set(envelope) != required:
        raise InvalidEnvelope("article AI envelope fields are invalid")
    if type(envelope["version"]) is not int or envelope["version"] != 1:
        raise InvalidEnvelope("article AI envelope version is invalid")
    try:
        job_id, owner_id, executor_id = (uuid.UUID(envelope[name]) for name in
                                         ("job_id", "owner_id", "executor_instance_id"))
        deadline = datetime.fromisoformat(envelope["deadline_at"].replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise InvalidEnvelope("article AI envelope identity is invalid") from exc
    if (str(job_id) != envelope["job_id"] or str(owner_id) != envelope["owner_id"]
            or str(executor_id) != envelope["executor_instance_id"] or deadline.tzinfo is None):
        raise InvalidEnvelope("article AI envelope identity is invalid")
    for name in ("attempt_generation", "generation", "backend_pid"):
        if type(envelope[name]) is not int or envelope[name] <= 0 or envelope[name] > 2_147_483_647:
            raise InvalidEnvelope("article AI envelope counter is invalid")
    return job_id, owner_id, executor_id, deadline.astimezone(timezone.utc)


def _validate_article_ai_input(operation, value):
    if not isinstance(value, dict):
        raise InvalidEnvelope("article AI input is invalid")
    if operation == "analyze":
        allowed = {"title", "content", "url", "original_context"}
        if set(value) not in (allowed - {"original_context"}, allowed):
            raise InvalidEnvelope("article AI analysis fields are invalid")
        if any(not isinstance(value.get(name), str) for name in ("title", "content", "url")):
            raise InvalidEnvelope("article AI analysis fields are invalid")
        if "original_context" in value and value["original_context"] is not None and not isinstance(value["original_context"], str):
            raise InvalidEnvelope("article AI analysis fields are invalid")
        from .core.resource_limits import enforce_text_limit
        try:
            enforce_text_limit(value["title"])
            enforce_text_limit(value["content"])
            enforce_text_limit(value.get("original_context") or "")
        except ValueError as exc:
            raise HTTPException(status_code=413, detail="Article AI text exceeds its limit") from exc
        parsed = urlsplit(value["url"])
        if (len(value["url"].encode("utf-8")) > 8192
                or parsed.scheme not in {"http", "https"} or not parsed.hostname):
            raise InvalidEnvelope("article AI URL is invalid")
    elif operation == "extraction_context":
        if (set(value) != {"url"} or not isinstance(value.get("url"), str)
                or len(value["url"].encode("utf-8")) > 8192):
            raise InvalidEnvelope("article extraction context fields are invalid")
        parsed = urlsplit(value["url"])
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise InvalidEnvelope("article extraction context URL is invalid")
    elif operation == "extraction_feedback":
        required = {"url", "method", "success", "observations"}
        allowed = required | {"selector_used", "content_length", "extraction_time_ms",
                             "quality_score", "error_code"}
        if not required <= set(value) or not set(value) <= allowed:
            raise InvalidEnvelope("article extraction feedback fields are invalid")
        if (not isinstance(value["url"], str) or len(value["url"].encode("utf-8")) > 8192
                or value["method"] not in ARTICLE_EXTRACTION_METHODS
                or type(value["success"]) is not bool):
            raise InvalidEnvelope("article extraction feedback fields are invalid")
        parsed = urlsplit(value["url"])
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise InvalidEnvelope("article extraction feedback URL is invalid")
        selector = value.get("selector_used")
        if selector is not None and (not isinstance(selector, str) or len(selector.encode("utf-8")) > 2048):
            raise InvalidEnvelope("article extraction feedback selector is invalid")
        observations = value["observations"]
        if (not isinstance(observations, list) or len(observations) > 64
                or any(not isinstance(item, dict) or set(item) != {"selector", "method", "success"}
                       or not isinstance(item["selector"], str)
                       or len(item["selector"].encode("utf-8")) > 2048
                       or item["method"] not in ARTICLE_EXTRACTION_METHODS
                       or type(item["success"]) is not bool for item in observations)):
            raise InvalidEnvelope("article extraction feedback observations are invalid")
        for name in ("content_length", "extraction_time_ms"):
            number = value.get(name)
            if number is not None and (type(number) is not int or not 0 <= number <= 10_000_000):
                raise InvalidEnvelope("article extraction feedback metric is invalid")
        score = value.get("quality_score")
        if score is not None and (isinstance(score, bool) or not isinstance(score, (int, float))
                                  or not math.isfinite(float(score)) or not 0 <= score <= 1):
            raise InvalidEnvelope("article extraction feedback quality is invalid")
        code = value.get("error_code")
        if code is not None and (not isinstance(code, str) or re.fullmatch(r"[a-z0-9_-]{1,40}", code) is None):
            raise InvalidEnvelope("article extraction feedback code is invalid")
    else:
        raise InvalidEnvelope("article AI operation is unsupported")


@app.post("/internal/v1/article-ai")
async def article_ai_callback(request: Request):
    """Run existing AI compute or bounded learning for one claimed processor item."""
    _article_ai_auth(request)
    await _require_executor_guard()
    try:
        raw = await _read_body_bounded(request, MAX_ARTICLE_AI_CALLBACK_BYTES)
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(InvalidEnvelope("invalid JSON number")))
        required = {"version", "run_id", "envelope", "article_id", "article_attempt",
                    "input_fingerprint", "operation", "input"}
        if (not isinstance(body, dict) or set(body) != required or type(body["version"]) is not int
                or body["version"] != 1 or type(body["article_id"]) is not int or body["article_id"] <= 0
                or type(body["article_attempt"]) is not int or body["article_attempt"] <= 0
                or not isinstance(body["input_fingerprint"], str)
                or re.fullmatch(r"[0-9a-f]{64}", body["input_fingerprint"]) is None
                or body["operation"] not in {"analyze", "extraction_context", "extraction_feedback"}):
            raise InvalidEnvelope("article AI callback fields are invalid")
        job_id, owner_id, executor_id, deadline = _validate_ai_envelope(body["envelope"])
        run_id = uuid.UUID(body["run_id"])
        if str(run_id) != body["run_id"]:
            raise InvalidEnvelope("article AI run identifier is invalid")
        _validate_article_ai_input(body["operation"], body["input"])
        if (body["operation"] == "analyze"
                and _article_ai_input_fingerprint(body["input"]) != body["input_fingerprint"]):
            raise InvalidEnvelope("article AI input identity does not match the active item")
    except HTTPException:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidEnvelope, ValueError, TypeError, AttributeError) as exc:
        raise HTTPException(status_code=422, detail="Article AI callback request is invalid") from exc

    from .services.article_processing_client import _run_id
    attempt = body["envelope"]["attempt_generation"]
    if run_id != uuid.UUID(_run_id(job_id, attempt)):
        raise HTTPException(status_code=409, detail="Article AI run identifier does not match its job")
    from .services.domain_fence import (
        DomainAuthorityError, RemoteDomainFence, context_matches_envelope, get_execution_context,
    )
    try:
        context = await get_execution_context(job_id, attempt)
        if (not context_matches_envelope(context, body["envelope"])
                or context.kind not in {"news_processing", "news_digest", "article_reprocess", "reprocess_failed"}
                or (context.kind == "news_digest" and context.payload.get("run_processing") is False)):
            raise DomainAuthorityError("article AI callback is not authorized by this job")
    except DomainAuthorityError as exc:
        raise HTTPException(status_code=409, detail="Article AI execution authority is not current") from exc
    lease = RemoteDomainFence(context)
    async with AsyncSessionLocal() as session:
        try:
            await lease.validate(session)
            await session.rollback()
        except StaleWorker as exc:
            await session.rollback()
            raise HTTPException(status_code=409, detail="Article AI execution fence is stale") from exc
    callback_fence = ArticleProcessingCallbackFence(
        lease, run_id=run_id,
        envelope_digest=_article_ai_envelope_digest(body["envelope"]),
        article_id=body["article_id"], article_attempt=body["article_attempt"],
        input_fingerprint=body["input_fingerprint"],
    )

    async def validate_claim():
        try:
            current_context = await get_execution_context(job_id, attempt)
            if not context_matches_envelope(current_context, body["envelope"]):
                raise StaleWorker("Go execution authority changed")
            current_lease = RemoteDomainFence(current_context)
            async with AsyncSessionLocal() as session:
                async with current_lease.transaction_fence(session, job_id, attempt):
                    guarded = (await session.execute(text("""
                        SELECT run_id,run_state FROM fence_article_processing_attempt(
                            :run_id,:job_id,:attempt,:envelope_sha256,:owner_id,:worker_generation,
                            :backend_pid,:executor_instance_id,:deadline_at)
                    """), {
                        "run_id": run_id, "job_id": job_id, "attempt": attempt,
                        "envelope_sha256": callback_fence.envelope_digest,
                        "owner_id": owner_id, "worker_generation": body["envelope"]["generation"],
                        "backend_pid": body["envelope"]["backend_pid"],
                        "executor_instance_id": current_lease.executor_instance_id,
                        "deadline_at": current_lease.deadline_at,
                    })).mappings().one_or_none()
                    item = (await session.execute(text("""
                        SELECT article_attempt,input_fingerprint,state FROM article_processing_items
                        WHERE run_id=:run_id AND article_id=:article_id
                    """), {"run_id": run_id, "article_id": body["article_id"]})).mappings().one_or_none()
                    article_identity = (await session.execute(text(
                        "SELECT processing_attempts,url FROM articles WHERE id=:article_id"),
                        {"article_id": body["article_id"]})
                    ).mappings().one_or_none()
                    if (guarded is None or str(guarded["run_id"]) != str(run_id)
                            or guarded["run_state"] != "running" or item is None
                            or item["article_attempt"] != body["article_attempt"]
                            or item["input_fingerprint"] != body["input_fingerprint"]
                            or item["state"] != "running" or article_identity is None
                            or article_identity["processing_attempts"] != body["article_attempt"]
                            or article_identity["url"] != body["input"]["url"]):
                        raise StaleWorker("processor item claim is stale")
                    await session.rollback()
        except StaleWorker as exc:
            raise HTTPException(status_code=409, detail="Article AI item claim is stale") from exc

    await validate_claim()
    operation, data = body["operation"], body["input"]
    callback_key = _article_ai_callback_key(body)
    cached = await _begin_article_ai_callback(callback_key)
    if cached is not None:
        await validate_claim()
        return cached
    try:
        if operation == "extraction_context":
            from .services.extraction_memory import get_extraction_memory
            domain = (urlsplit(data["url"]).hostname or "").lower()
            pattern = await (await get_extraction_memory()).get_successful_pattern(domain)
            selector = pattern.get("selector") if pattern else None
            method = pattern.get("method") if pattern else None
            result = {
                "selector": selector if isinstance(selector, str) and len(selector.encode("utf-8")) <= 2048 else None,
                "method": method if method in ARTICLE_EXTRACTION_METHODS else None,
            }
            await _remember_article_ai_context(body, result["selector"], result["method"])
        elif operation == "analyze":
            from .services.ai_client import get_ai_client
            result = await get_ai_client().analyze_article_complete(
                data["title"], data["content"], data["url"],
                original_context=data.get("original_context"), deadline=deadline)
        else:
            try:
                learned_selector, learned_method = await _take_article_ai_context(body)
                await _record_article_extraction_feedback(
                    lease, job_id, attempt, callback_fence, data,
                    learned_selector=learned_selector, learned_method=learned_method)
                result = {"recorded": True}
            except StaleWorker as exc:
                raise HTTPException(status_code=409, detail="Article AI learning claim is stale") from exc
            except Exception as exc:
                logger.warning("Article extraction learning failed (%s)", type(exc).__name__)
                result = {"recorded": False}
        await validate_claim()
        response_body = {"version": 1, "run_id": str(run_id), "article_id": body["article_id"],
                         "article_attempt": body["article_attempt"],
                         "input_fingerprint": body["input_fingerprint"],
                         "operation": operation, "result": result}
        try:
            response_bytes = json.dumps(response_body, ensure_ascii=False, allow_nan=False,
                                        separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise HTTPException(status_code=502, detail="Article AI result is invalid") from exc
        if len(response_bytes) > MAX_ARTICLE_AI_RESPONSE_BYTES:
            raise HTTPException(status_code=502, detail="Article AI result exceeded its response limit")
        await _finish_article_ai_callback(callback_key, response_body)
        return response_body
    finally:
        await _finish_article_ai_callback(callback_key, None)


async def _record_article_extraction_feedback(lease, job_id, attempt_generation, fence, metadata,
                                               *, learned_selector=None, learned_method=None):
    """Write extraction analytics under the active Go item and Python job fences."""
    parsed = urlsplit(metadata["url"])
    domain = (parsed.hostname or "").lower()
    elapsed = metadata.get("extraction_time_ms") or 0
    content_length = metadata.get("content_length") or 0
    success, method, selector = metadata["success"], metadata["method"], metadata.get("selector_used")
    supplied_quality = metadata.get("quality_score")
    quality = float(supplied_quality if supplied_quality is not None else (0.8 if success else 0.0))
    observations = metadata["observations"]
    async with AsyncSessionLocal() as session:
        async with lease.transaction_fence(session, job_id, attempt_generation):
            guarded = (await session.execute(text("""
                SELECT run_id,run_state FROM fence_article_processing_attempt(
                    :run_id,:job_id,:attempt,:envelope_sha256,:owner_id,:worker_generation,
                    :backend_pid,:executor_instance_id,:deadline_at)
            """), {
                "run_id": fence.run_id, "job_id": job_id, "attempt": attempt_generation,
                "envelope_sha256": fence.envelope_digest, "owner_id": lease.owner_id,
                "worker_generation": lease.generation, "backend_pid": lease.worker_backend_pid,
                        "executor_instance_id": lease.executor_instance_id, "deadline_at": lease.deadline_at,
            })).mappings().one_or_none()
            item = (await session.execute(text("""
                SELECT article_attempt,input_fingerprint,state FROM article_processing_items
                WHERE run_id=:run_id AND article_id=:article_id FOR UPDATE
            """), {"run_id": fence.run_id, "article_id": fence.article_id})).mappings().one_or_none()
            if (item is None or item["article_attempt"] != fence.article_attempt
                    or item["input_fingerprint"] != fence.input_fingerprint or item["state"] != "running"
                    or guarded is None or str(guarded["run_id"]) != str(fence.run_id)
                    or guarded["run_state"] != "running"):
                raise StaleWorker("processor item claim is stale")
            article = (await session.execute(text(
                "SELECT processing_attempts,url FROM articles WHERE id=:article_id FOR UPDATE"),
                {"article_id": fence.article_id})).mappings().one_or_none()
            if (article is None or article["processing_attempts"] != fence.article_attempt
                    or article["url"] != metadata["url"]):
                raise StaleWorker("article identity changed before learning write")
            await session.execute(text("""
                INSERT INTO extraction_attempts
                    (article_url,domain,extraction_strategy,selector_used,success,content_length,
                     quality_score,extraction_time_ms,error_message)
                VALUES (:url,:domain,:method,:selector,:success,:content_length,:quality,:elapsed,:error)
            """), {"url": metadata["url"], "domain": domain, "method": method,
                    "selector": selector, "success": success, "content_length": content_length,
                    "quality": quality if success else None, "elapsed": elapsed,
                    "error": metadata.get("error_code")})
            for observation in observations:
                await session.execute(text("""
                    INSERT INTO extraction_attempts
                        (article_url,domain,extraction_strategy,selector_used,success,
                         content_length,quality_score,extraction_time_ms,error_message)
                    VALUES (:url,:domain,:method,:selector,:success,NULL,NULL,NULL,:error)
                """), {"url": metadata["url"], "domain": domain,
                        "method": observation["method"], "selector": observation["selector"],
                        "success": observation["success"],
                        "error": metadata.get("error_code") if not observation["success"] else None})
            if selector:
                if success:
                    await session.execute(text("""
                        INSERT INTO extraction_patterns
                            (domain,selector_pattern,extraction_strategy,success_count,failure_count,
                             quality_score_avg,content_length_avg,discovered_by,is_stable,
                             consecutive_successes,consecutive_failures,first_success_at,last_success_at)
                        VALUES (:domain,:selector,:method,1,0,:quality,:content_length,'manual',false,1,0,NOW(),NOW())
                        ON CONFLICT (domain,selector_pattern,extraction_strategy) DO UPDATE SET
                            success_count=COALESCE(extraction_patterns.success_count,0)+1,
                            quality_score_avg=(COALESCE(extraction_patterns.quality_score_avg,0)*
                              COALESCE(extraction_patterns.success_count,0)+EXCLUDED.quality_score_avg)/
                              (COALESCE(extraction_patterns.success_count,0)+1),
                            content_length_avg=(COALESCE(extraction_patterns.content_length_avg,0)*
                              COALESCE(extraction_patterns.success_count,0)+EXCLUDED.content_length_avg)/
                              (COALESCE(extraction_patterns.success_count,0)+1),
                            consecutive_successes=COALESCE(extraction_patterns.consecutive_successes,0)+1,
                            consecutive_failures=0,
                            is_stable=COALESCE(extraction_patterns.consecutive_successes,0)+1>=3,
                            first_success_at=COALESCE(extraction_patterns.first_success_at,NOW()),
                            last_success_at=NOW(),updated_at=NOW()
                    """), {"domain": domain, "selector": selector, "method": method,
                            "quality": quality, "content_length": content_length})
            if learned_selector and learned_method:
                degraded = set()
                for observation in metadata.get("observations", []):
                    if (not observation["success"] and observation["selector"] == learned_selector
                            and observation["method"] == learned_method
                            and (observation["selector"], observation["method"]) not in degraded):
                        degraded.add((observation["selector"], observation["method"]))
                        await session.execute(text("""
                            UPDATE extraction_patterns SET failure_count=COALESCE(failure_count,0)+1,
                                consecutive_failures=COALESCE(consecutive_failures,0)+1,
                                consecutive_successes=0,is_stable=false,updated_at=NOW()
                            WHERE domain=:domain AND selector_pattern=:selector AND extraction_strategy=:method
                        """), {"domain": domain, "selector": learned_selector, "method": learned_method})
            await session.commit()
    from .services.extraction_memory import ExtractionMemoryEntry, get_extraction_memory
    memory = await get_extraction_memory()
    await memory._ensure_initialized()
    if success and selector:
        patterns = memory._patterns.setdefault(domain, [])
        entry = next((item for item in patterns if item.selector_pattern == selector
                      and item.extraction_strategy == method), None)
        if entry is None:
            entry = ExtractionMemoryEntry(domain, selector, method)
            patterns.append(entry)
        entry.success_count += 1
        entry.consecutive_successes += 1
        entry.consecutive_failures = 0
        entry.success_rate = (entry.success_count / max(1, entry.success_count + entry.failure_count)) * 100
        entry.quality_score_avg = ((entry.quality_score_avg * (entry.success_count - 1) + quality)
                                   / entry.success_count)
        entry.content_length_avg = int((entry.content_length_avg * (entry.success_count - 1) + content_length)
                                       / entry.success_count)
        entry.is_stable = entry.consecutive_successes >= 3
    if learned_selector and learned_method:
        learned = next((item for item in memory._patterns.get(domain, [])
                        if item.selector_pattern == learned_selector
                        and item.extraction_strategy == learned_method), None)
        learned_failed = any(
            not observation["success"] and observation["selector"] == learned_selector
            and observation["method"] == learned_method for observation in observations)
        if learned is not None and learned_failed:
            learned.failure_count += 1
            learned.consecutive_failures += 1
            learned.consecutive_successes = 0
            learned.is_stable = False
            learned.success_rate = (learned.success_count / max(1, learned.success_count + learned.failure_count)) * 100
    if success:
        from .services.domain_stability_tracker import get_stability_tracker
        (await get_stability_tracker()).update_domain_stats(
            domain=domain, success=True, extraction_time_ms=elapsed,
            content_length=content_length, quality_score=quality, method=method)


def _validate_executor_runtime() -> None:
    token = settings.job_executor_token.get_secret_value() if settings.job_executor_token else None
    if not token or not 16 <= len(token) <= 4096 or any(ord(ch) < 0x21 or ord(ch) > 0x7E for ch in token):
        raise RuntimeError("JOB_EXECUTOR_TOKEN must be configured with a valid service token")
    if not settings.gemini_api_key or not settings.gemini_api_key.strip():
        raise RuntimeError("GEMINI_API_KEY must be configured for background job execution")
    _validate_execution_service_origin(settings.execution_service_url)
    settings.validate_runtime_services()


def _validate_execution_service_origin(value) -> None:
    """Validate Go authority configuration without probing it during startup."""
    message = "EXECUTION_SERVICE_URL must be an HTTP(S) origin without credentials, path, query, or fragment"
    if not isinstance(value, str) or not value or any(character.isspace() for character in value):
        raise RuntimeError(message)
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise RuntimeError(message) from exc
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
            or "?" in value or "#" in value):
        raise RuntimeError(message)


async def _read_body_bounded(request: Request, max_bytes: int = MAX_ENVELOPE_BYTES) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                raise HTTPException(status_code=413, detail="Request body exceeds its limit")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid content length") from exc
    chunks = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > max_bytes:
            raise HTTPException(status_code=413, detail="Request body exceeds its limit")
        chunks.append(chunk)
    return b"".join(chunks)


@asynccontextmanager
async def _execution_context(fence):
    token = current_job_fence.set(fence)
    try:
        yield
    finally:
        fence.active = False
        current_job_fence.reset(token)




@app.get("/internal/v1/status")
async def get_status(request: Request):
    _auth(request)
    await _require_executor_guard()
    from .services.ai_runtime import get_ai_runtime
    runtime = get_ai_runtime()
    try:
        status = await runtime.status()
        if not runtime.ready or status.get("ready") is not True or status.get("protocol_version") != 1:
            raise RuntimeError("runtime_not_ready")
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Executor runtime is unavailable") from exc
    try:
        _validate_executor_runtime()
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
            await session.rollback()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Domain database is unavailable") from exc
    from .services.domain_operations import domain_status
    return domain_status(ready=True)


@app.post("/internal/v1/domain-operations")
async def post_domain_operation(request: Request):
    _auth(request)
    await _require_executor_guard()
    from .services.domain_operations import (
        DOMAIN_INSTANCE_ID, DomainOperationConflict, DomainOperationInputError,
        MAX_DOMAIN_BODY_BYTES, accept_domain_operation, validate_operation_body,
    )
    try:
        raw = await _read_body_bounded(request, MAX_DOMAIN_BODY_BYTES)
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(InvalidEnvelope("invalid JSON number")))
        record = validate_operation_body(body)
        if (record.domain_instance_id != str(DOMAIN_INSTANCE_ID)
                or request.headers.get("x-domain-instance-id") != str(DOMAIN_INSTANCE_ID)):
            raise DomainOperationConflict("domain instance does not match this service")
        value, created = await accept_domain_operation(record)
    except DomainOperationConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DomainOperationInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidEnvelope) as exc:
        raise HTTPException(status_code=422, detail="Domain operation request is invalid") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail="Domain operation capacity is full") from exc
    return JSONResponse(status_code=202, content=value)


@app.get("/internal/v1/domain-operations/{job_id}/{attempt_generation}/{operation}/{operation_id}")
async def get_domain_operation(job_id: uuid.UUID, attempt_generation: int, operation: str,
                               operation_id: uuid.UUID, request: Request):
    _auth(request)
    await _require_executor_guard()
    from .services.domain_operations import (
        DOMAIN_INSTANCE_ID, DomainOperationConflict, read_operation_status,
    )
    domain_instance = request.headers.get("x-domain-instance-id")
    if domain_instance != str(DOMAIN_INSTANCE_ID):
        raise HTTPException(status_code=409, detail="Domain instance does not match this service")
    value = await read_operation_status(str(job_id), attempt_generation, operation,
                                       str(operation_id), domain_instance)
    if value is None:
        raise HTTPException(status_code=404, detail="Domain operation receipt not found")
    return value


@app.post("/internal/v1/domain-events/finalized")
async def post_domain_finalized(request: Request):
    _auth(request)
    await _require_executor_guard()
    from .services.domain_terminal import (
        TerminalEventError, parse_terminal_event, process_finalized_event,
    )
    try:
        raw = await _read_body_bounded(request, 4096)
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(InvalidEnvelope("invalid JSON number")))
        event = parse_terminal_event(body)
        response = await process_finalized_event(event)
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidEnvelope, TerminalEventError) as exc:
        raise HTTPException(status_code=409, detail="Terminal event is not current") from exc
    except StaleWorker as exc:
        raise HTTPException(status_code=409, detail="Terminal event authority is stale") from exc
    return response


def _source_fingerprint(source_id, name, source_type, url, enabled, config_text):
    parts = (str(source_id), name, source_type, url, "true" if enabled else "false", config_text)
    raw = "".join(f"{len(part.encode('utf-8'))}:{part}" for part in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _validated_selector_arrays(structure):
    output = {}
    for key in ("container_selectors", "title_selectors", "link_selectors", "date_selectors"):
        values = structure.get(key, []) if isinstance(structure, dict) else []
        if (not isinstance(values, list) or len(values) > 64
                or any(not isinstance(value, str) or not value.strip()
                       or len(value.encode("utf-8")) > 2048 for value in values)):
            raise HTTPException(status_code=502, detail="Selector memory returned invalid data")
        output[{"container_selectors": "learned_container_selectors",
                "title_selectors": "learned_title_selectors",
                "link_selectors": "learned_link_selectors",
                "date_selectors": "learned_date_selectors"}[key]] = values
    return output


@app.post("/internal/v1/ingestion/selectors")
async def ingestion_selector_callback(request: Request):
    _auth(request)
    await _require_executor_guard()
    try:
        raw = await _read_body_bounded(request, MAX_SELECTOR_CALLBACK_BYTES)
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(InvalidEnvelope("invalid JSON number")))
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidEnvelope) as exc:
        raise HTTPException(status_code=422, detail="Selector callback request is invalid") from exc
    required = {"version", "job_id", "attempt_generation", "owner_id", "generation", "backend_pid",
                "executor_instance_id", "deadline_at", "operation", "source_id", "source_fingerprint"}
    operation = body.get("operation") if isinstance(body, dict) else None
    expected = required | ({"study_html"} if operation == "study" else set())
    if (not isinstance(body, dict) or set(body) != expected or type(body.get("version")) is not int
            or body["version"] != 1 or operation not in {"load", "study"}
            or type(body.get("attempt_generation")) is not int or body["attempt_generation"] <= 0
            or type(body.get("generation")) is not int or body["generation"] <= 0
            or type(body.get("backend_pid")) is not int or body["backend_pid"] <= 0
            or type(body.get("source_id")) is not int or body["source_id"] <= 0
            or not isinstance(body.get("source_fingerprint"), str)
            or re.fullmatch(r"[0-9a-f]{64}", body["source_fingerprint"]) is None):
        raise HTTPException(status_code=422, detail="Selector callback request is invalid")
    try:
        job_id, owner_id, executor_id = (uuid.UUID(body[name]) for name in
                                         ("job_id", "owner_id", "executor_instance_id"))
        deadline = datetime.fromisoformat(body["deadline_at"].replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise HTTPException(status_code=422, detail="Selector callback fence is invalid") from exc
    if (str(job_id) != body["job_id"] or str(owner_id) != body["owner_id"]
            or str(executor_id) != body["executor_instance_id"] or deadline.tzinfo is None):
        raise HTTPException(status_code=422, detail="Selector callback fence is invalid")
    if operation == "study":
        html = body["study_html"]
        try:
            html_size = len(html.encode("utf-8", errors="strict")) if isinstance(html, str) else 0
        except UnicodeEncodeError as exc:
            raise HTTPException(status_code=422, detail="Captured HTML is invalid Unicode") from exc
        if not isinstance(html, str) or not html or html_size > MAX_SELECTOR_CALLBACK_HTML_BYTES:
            raise HTTPException(status_code=413, detail="Captured HTML exceeds its limit")
    else:
        html = ""

    attempt = body["attempt_generation"]
    from .services.domain_fence import (
        DomainAuthorityError, RemoteDomainFence, context_matches_envelope, get_execution_context,
    )
    try:
        context = await get_execution_context(job_id, attempt)
        if (not context_matches_envelope(context, {name: body[name] for name in (
                "version", "job_id", "attempt_generation", "owner_id", "generation", "backend_pid",
                "executor_instance_id", "deadline_at")})
                or context.kind not in {"news_processing", "news_digest"}
                or (context.kind == "news_digest" and context.payload.get("run_processing") is False)):
            raise DomainAuthorityError("selector callback is not authorized by this job")
    except DomainAuthorityError as exc:
        raise HTTPException(status_code=409, detail="Selector callback execution authority is not current") from exc
    lease = RemoteDomainFence(context)
    active = JobExecutionFence(
        lease, job_id, attempt, executor_instance_id=lease.executor_instance_id,
    )
    async with AsyncSessionLocal() as session:
        try:
            await lease.validate(session)
            await session.rollback()
        except StaleWorker as exc:
            await session.rollback()
            raise HTTPException(status_code=409, detail="Selector callback execution fence is stale") from exc

    from .services.article_ingestion_client import _run_id
    callback_fence = ArticleIngestionCallbackFence(
        lease, run_id=uuid.UUID(_run_id(job_id, attempt)),
        executor_instance_id=lease.executor_instance_id, context=active,
        source_id=body["source_id"], source_fingerprint=body["source_fingerprint"],
    )

    async def validate_current_authority():
        try:
            current = await get_execution_context(job_id, attempt)
            if not context_matches_envelope(current, {name: body[name] for name in (
                    "version", "job_id", "attempt_generation", "owner_id", "generation", "backend_pid",
                    "executor_instance_id", "deadline_at")}):
                raise StaleWorker("Go execution authority changed")
            async with AsyncSessionLocal() as session:
                async with RemoteDomainFence(current).transaction_fence(session, job_id, attempt):
                    await session.rollback()
        except StaleWorker as exc:
            raise HTTPException(status_code=409, detail="Selector callback execution fence is stale") from exc

    async with _execution_context(JobExecutionFence(
            callback_fence, job_id, attempt, executor_instance_id=lease.executor_instance_id)):
        async with AsyncSessionLocal() as session:
            try:
                async with callback_fence.transaction_fence(session, job_id, attempt):
                    row = (await session.execute(text("""
                        SELECT name,source_type,url,enabled,config,config::text AS config_text
                        FROM sources WHERE id=:source_id FOR SHARE
                    """), {"source_id": body["source_id"]})).mappings().one_or_none()
                    if (row is None or row["source_type"] != "custom" or row["enabled"] is not True
                            or _source_fingerprint(body["source_id"], row["name"], row["source_type"],
                                                  row["url"], row["enabled"], row["config_text"])
                            != body["source_fingerprint"]):
                        raise HTTPException(status_code=409, detail="Page Monitor source changed")
                    source = {"name": row["name"], "url": row["url"],
                              "config": row["config"] if isinstance(row["config"], dict) else {}}
                await session.rollback()
            except StaleWorker as exc:
                raise HTTPException(status_code=409, detail="Ingestion callback fence is stale") from exc

        config = source["config"]
        domain = urlsplit(source["url"]).netloc.lower()
        if not domain:
            raise HTTPException(status_code=409, detail="Page Monitor source URL is invalid")
        await validate_current_authority()
        from .services.extraction_memory import get_extraction_memory
        memory = await get_extraction_memory()
        structure = await memory.get_page_structure(domain)
        studied = False
        if operation == "study":
            if config.get("enable_ai_analysis", True) is False:
                return Response(status_code=204)
            from .sources.page_monitor_source import PageMonitorConfig, PageMonitorSource
            monitor = PageMonitorSource(PageMonitorConfig(url=source["url"], name=source["name"],
                                                          enable_ai_analysis=True))
            studied = await monitor._study_source_structure(html)
            if not studied:
                return Response(status_code=204)
            await validate_current_authority()
            structure = await memory.get_page_structure(domain)
        return _validated_selector_arrays(structure) | {"version": 1}


async def _serve():
    global _EXECUTOR_SERVER
    try:
        _validate_executor_runtime()
    except BaseException:
        await _shield_startup_cleanup(engine.dispose())
        raise
    try:
        await _acquire_executor_guard()
    except BaseException:
        await _shield_startup_cleanup(engine.dispose())
        raise
    from .services.ai_runtime import get_ai_runtime
    runtime = get_ai_runtime()
    config = uvicorn.Config(app, host=settings.job_executor_host, port=settings.job_executor_port,
                            log_level=settings.log_level.lower(), access_log=False)
    server = uvicorn.Server(config)
    _EXECUTOR_SERVER = server
    try:
        await runtime.start(on_child_exit=lambda: _revoke_executor_work(_EXECUTOR_SERVER))
    except BaseException:
        _EXECUTOR_SERVER = None
        await _shield_startup_cleanup(_cleanup_failed_executor_start(runtime))
        raise
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


async def _cleanup_failed_executor_start(runtime) -> None:
    try:
        await runtime.stop(timeout=8)
    except Exception:
        logger.error("native AI runtime startup cleanup failed")
    try:
        await _release_executor_guard()
    except Exception:
        logger.error("executor database guard startup cleanup failed")
    try:
        await engine.dispose()
    except Exception:
        logger.error("executor database pool startup cleanup failed")


async def _shield_startup_cleanup(awaitable) -> None:
    """Finish bounded startup cleanup even when _serve was cancelled."""
    cleanup = asyncio.create_task(awaitable)
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        # A repeated cancellation must not strand a singleton guard or child.
        await cleanup


async def _shutdown_executor(*, drain_seconds=None):
    """Cancel leaf work before releasing the Python database singleton guard."""
    timeout = (settings.worker_shutdown_drain_seconds if drain_seconds is None
               else max(0, float(drain_seconds)))
    from .services.domain_operations import cancel_active_operations
    from .services.ai_runtime import get_ai_runtime
    try:
        await cancel_active_operations(timeout=timeout)
    finally:
        try:
            await get_ai_runtime().stop(timeout=min(max(timeout, 1), 8))
        finally:
            try:
                await _release_executor_guard()
            finally:
                await engine.dispose()


def main():
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
