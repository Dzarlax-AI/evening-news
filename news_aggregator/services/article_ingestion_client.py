"""Authenticated client for the private Go article ingestion service."""

import asyncio
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
import uuid
from urllib.parse import urlsplit

import aiohttp

from ..config import settings
from .job_fence import current_job_fence

class ArticleIngestionError(RuntimeError):
    pass


class ArticleIngestionBusy(ArticleIngestionError):
    def __init__(self, retry_after):
        super().__init__("Go article ingestion is busy")
        self.retry_after = retry_after


class ArticleIngestionClient:
    def __init__(self, *, client_factory=aiohttp.ClientSession):
        self._client_factory = client_factory

    async def collect_and_save(self) -> dict:
        active = current_job_fence.get()
        if (active is None or not active.active or active.lease is None
                or active.executor_instance_id is None):
            raise ArticleIngestionError("article ingestion requires an active executor fence")
        lease = active.lease
        deadline = lease.deadline_at
        if deadline is None:
            raise ArticleIngestionError("article ingestion requires an attempt deadline")
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        deadline = deadline.astimezone(timezone.utc)
        body = {
            "version": 1,
            "job_id": str(active.job_id),
            "attempt_generation": active.attempt_generation,
            "owner_id": str(lease.owner_id),
            "generation": lease.generation,
            "backend_pid": lease.worker_backend_pid,
            "executor_instance_id": str(active.executor_instance_id),
            "deadline_at": deadline.isoformat().replace("+00:00", "Z"),
        }
        base = (settings.article_ingestion_url or "").rstrip("/")
        token = settings.article_ingestion_token.get_secret_value() if settings.article_ingestion_token else ""
        if not _valid_service_url(base) or not token:
            raise ArticleIngestionError("Go article ingestion service is not configured")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        timeout = aiohttp.ClientTimeout(total=10, connect=3, sock_connect=3, sock_read=10)
        async with self._client_factory(timeout=timeout) as client:
            run_id = _run_id(active.job_id, active.attempt_generation)
            pass_deadline = min(deadline, datetime.now(timezone.utc) + timedelta(minutes=10))
            try:
                run = await _submit_with_busy_retry(client, base, body, headers, pass_deadline)
            except (asyncio.TimeoutError, aiohttp.ClientError):
                # A lost acknowledgement is reconciled against the deterministic
                # idempotency key before the exact same envelope is retried.
                try:
                    run = await _read_run(client, base, run_id, headers)
                except ArticleIngestionError as exc:
                    if "not found" not in str(exc):
                        raise
                    run = await _submit_with_busy_retry(client, base, body, headers, pass_deadline)
            _validate_identity(run, run_id, active)
            delay = 0.25
            while True:
                try:
                    if not active.active:
                        if not await asyncio.shield(_cancel(client, base, run_id, headers)):
                            raise ArticleIngestionError("Go article ingestion cancellation was not confirmed")
                        raise ArticleIngestionError("active executor fence ended during ingestion")
                    remaining = (pass_deadline - datetime.now(timezone.utc)).total_seconds()
                    if remaining <= 0:
                        if not await asyncio.shield(_cancel(client, base, run_id, headers)):
                            raise ArticleIngestionError("Go article ingestion cancellation was not confirmed")
                        raise ArticleIngestionError("Go article ingestion exceeded the job deadline")
                    if run.get("state") in {"succeeded", "failed", "cancelled", "interrupted"}:
                        if run["state"] != "succeeded":
                            raise ArticleIngestionError("Go article ingestion did not complete successfully")
                        result = run.get("result")
                        if not isinstance(result, dict):
                            raise ArticleIngestionError("Go article ingestion result is invalid")
                        if result.get("status") not in {"success", "warning"}:
                            raise ArticleIngestionError("Go article ingestion status is invalid")
                        outcomes, warning_count = _bounded_source_outcomes(result.get("source_outcomes", {}))
                        errors_count = _bounded_int(result.get("errors_count"))
                        if warning_count != errors_count:
                            raise ArticleIngestionError("Go article ingestion warning totals do not match")
                        return {
                            "sources_synced": _bounded_int(result.get("sources_completed")),
                            "articles_fetched": _bounded_int(result.get("articles_fetched")),
                            "errors": outcomes,
                            "errors_count": errors_count,
                            "fatal_error": None,
                        }
                    await asyncio.sleep(min(delay, max(0.05, remaining)))
                    run = await _read_run(client, base, run_id, headers)
                    _validate_identity(run, run_id, active)
                except asyncio.CancelledError:
                    if not await asyncio.shield(_cancel(client, base, run_id, headers)):
                        raise ArticleIngestionError("Go article ingestion cancellation was not confirmed")
                    raise
                except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                    if not await asyncio.shield(_cancel(client, base, run_id, headers)):
                        raise ArticleIngestionError("Go article ingestion cancellation was not confirmed") from exc
                    raise ArticleIngestionError("Go article ingestion status is unavailable") from exc
                delay = min(delay * 1.5, 2.0)


async def _submit(client, base, body, headers):
    async with client.post(f"{base}/internal/v1/ingestion/runs", json=body, headers=headers,
                           allow_redirects=False) as response:
        if response.status == 429:
            try:
                retry_after = float(response.headers.get("Retry-After", "1"))
            except (TypeError, ValueError):
                retry_after = 1.0
            raise ArticleIngestionBusy(min(5.0, max(0.1, retry_after)))
        if response.status not in (200, 202):
            raise ArticleIngestionError("Go article ingestion rejected the active job")
        return await _bounded_json(response)


async def _submit_with_busy_retry(client, base, body, headers, deadline):
    delay = 0.25
    while True:
        try:
            return await _submit(client, base, body, headers)
        except ArticleIngestionBusy as exc:
            remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                raise ArticleIngestionError("Go article ingestion pass budget expired while busy") from exc
            await asyncio.sleep(min(remaining, exc.retry_after, delay))
            delay = min(delay * 1.5, 2.0)


async def _read_run(client, base, run_id, headers):
    async with client.get(f"{base}/internal/v1/ingestion/runs/{run_id}", headers=headers,
                          allow_redirects=False) as response:
        if response.status == 404:
            raise ArticleIngestionError("Go article ingestion run not found")
        if response.status != 200:
            raise ArticleIngestionError("Go article ingestion status was rejected")
        return await _bounded_json(response)


async def _cancel(client, base, run_id, headers):
    try:
        async with client.post(f"{base}/internal/v1/ingestion/runs/{run_id}/cancel", headers=headers,
                               allow_redirects=False) as response:
            if response.status == 200:
                receipt = await _bounded_json(response)
                if receipt.get("state") in {"cancelled", "failed", "succeeded", "interrupted"}:
                    return True
    except (asyncio.TimeoutError, aiohttp.ClientError, ArticleIngestionError):
        pass
    try:
        receipt = await _read_run(client, base, run_id, headers)
        return receipt.get("state") in {"cancelled", "failed", "succeeded", "interrupted"}
    except (asyncio.TimeoutError, aiohttp.ClientError, ArticleIngestionError):
        return False


async def _bounded_json(response):
    try:
        body = bytearray()
        async for chunk in response.content.iter_chunked(8192):
            if len(body) + len(chunk) > 8192:
                raise ArticleIngestionError("Go article ingestion response exceeded its limit")
            body.extend(chunk)
        value = json.loads(body)
    except (ValueError, TypeError, RecursionError, aiohttp.ClientError) as exc:
        raise ArticleIngestionError("Go article ingestion returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ArticleIngestionError("Go article ingestion response exceeded its limit")
    return value


def _bounded_int(value):
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 1_000_000_000:
        raise ArticleIngestionError("Go article ingestion returned an invalid scalar result")
    return value


def _bounded_source_outcomes(value):
    if not isinstance(value, dict) or len(value) > 64:
        raise ArticleIngestionError("Go article ingestion source outcomes are invalid")
    errors = []
    for code, count in value.items():
        if (not isinstance(code, str) or re.fullmatch(r"[a-z0-9_-]{1,40}", code) is None
                or not isinstance(count, int) or isinstance(count, bool) or not 0 <= count <= 1000):
            raise ArticleIngestionError("Go article ingestion source outcomes are invalid")
        errors.extend([f"Go article ingestion source warning: {code}"] * count)
        if len(errors) > 1000:
            raise ArticleIngestionError("Go article ingestion source outcomes are invalid")
    return errors, len(errors)


def _run_id(job_id, attempt):
    raw = bytearray(hashlib.sha256(
        f"evening-news-ingestion-v1\0{job_id}\0{attempt}".encode("ascii")
    ).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _validate_identity(run, run_id, active):
    try:
        valid_id = str(uuid.UUID(run.get("run_id", ""))) == run_id
        valid_job = str(uuid.UUID(run.get("job_id", ""))) == str(active.job_id)
    except (ValueError, TypeError, AttributeError):
        valid_id = valid_job = False
    if (not valid_id or not valid_job or run.get("attempt_generation") != active.attempt_generation
            or run.get("state") not in {"running", "succeeded", "failed", "cancelled", "interrupted"}):
        raise ArticleIngestionError("Go article ingestion returned a mismatched run receipt")


def _valid_service_url(value):
    parsed = urlsplit(value)
    try:
        _ = parsed.port
    except ValueError:
        return False
    return (parsed.scheme in {"http", "https"} and bool(parsed.hostname)
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment)
