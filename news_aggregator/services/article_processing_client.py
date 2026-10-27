"""Authenticated client for the private Go article processor."""

import asyncio
import hashlib
import json
import math
import uuid
from datetime import datetime, timezone

import aiohttp

from ..config import settings
from .job_fence import current_job_fence

ALLOWED_PROCESSING_STATUSES = frozenset({
    "success", "warning", "dry_run", "completed", "rejected", "exhausted",
    "retry_wait", "settled", "incomplete", "deferred", "busy",
})


class ArticleProcessingError(RuntimeError):
    """The current processing run could not be completed safely."""


class ArticleProcessingBusy(ArticleProcessingError):
    def __init__(self, retry_after):
        super().__init__("Go article processor is busy")
        self.retry_after = retry_after


class ArticleProcessingClient:
    def __init__(self, *, client_factory=aiohttp.ClientSession):
        self._client_factory = client_factory

    async def process_current_job(self) -> dict:
        active = current_job_fence.get()
        if (active is None or not active.active or active.lease is None
                or active.executor_instance_id is None):
            raise ArticleProcessingError("article processing requires an active executor fence")
        lease = active.lease
        deadline = lease.deadline_at
        if deadline is None:
            raise ArticleProcessingError("article processing requires an attempt deadline")
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
        base = (settings.article_processing_url or "").rstrip("/")
        token = (settings.article_processing_token.get_secret_value()
                 if settings.article_processing_token else "")
        if not _valid_service_url(base) or not token:
            raise ArticleProcessingError("Go article processor is not configured")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        timeout = aiohttp.ClientTimeout(total=15, connect=3, sock_connect=3, sock_read=15)
        async with self._client_factory(timeout=timeout) as client:
            run_id = _run_id(active.job_id, active.attempt_generation)
            possible_submit = {"value": False}
            try:
                try:
                    run = await _submit_until_not_busy(
                        client, base, body, headers, active, deadline, possible_submit
                    )
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    # Reconcile a lost acknowledgement before repeating the exact submit.
                    try:
                        run = await _read_run(client, base, run_id, headers)
                    except ArticleProcessingError as exc:
                        if "not found" not in str(exc):
                            raise
                        run = await _submit_until_not_busy(
                            client, base, body, headers, active, deadline, possible_submit
                        )
                _validate_identity(run, run_id, active)
                delay = 0.25
                while True:
                    if not active.active:
                        raise ArticleProcessingError("active executor fence ended during article processing")
                    remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
                    if remaining <= 0:
                        raise ArticleProcessingError("Go article processing exceeded the job deadline")
                    if run.get("state") in {"succeeded", "failed", "cancelled", "interrupted"}:
                        if run["state"] != "succeeded":
                            raise ArticleProcessingError("Go article processing did not complete successfully")
                        result = run.get("result")
                        if not isinstance(result, dict) or result.get("status") not in ALLOWED_PROCESSING_STATUSES:
                            raise ArticleProcessingError("Go article processor returned an invalid receipt")
                        return _bounded_result(result)
                    await asyncio.sleep(min(delay, remaining))
                    run = await _read_run(client, base, run_id, headers)
                    _validate_identity(run, run_id, active)
                    delay = min(delay * 1.5, 2.0)
            except asyncio.CancelledError:
                if possible_submit["value"]:
                    confirmed = await asyncio.shield(
                        _bounded_cancel(client, base, run_id, headers, active)
                    )
                    if not confirmed:
                        raise ArticleProcessingError(
                            "Go article processing cancellation was not confirmed"
                        )
                raise
            except Exception as exc:
                if possible_submit["value"]:
                    confirmed = await asyncio.shield(
                        _bounded_cancel(client, base, run_id, headers, active)
                    )
                    if not confirmed:
                        raise ArticleProcessingError(
                            "Go article processing cancellation was not confirmed"
                        ) from exc
                raise


async def _submit(client, base, body, headers, possible_submit=None):
    if possible_submit is not None:
        possible_submit["value"] = True
    async with client.post(f"{base}/internal/v1/processing/runs", json=body, headers=headers,
                           allow_redirects=False) as response:
        if response.status == 429:
            if possible_submit is not None:
                possible_submit["value"] = False
            try:
                retry_after = float(response.headers.get("Retry-After", "1"))
            except (TypeError, ValueError):
                retry_after = 1.0
            raise ArticleProcessingBusy(min(5.0, max(0.1, retry_after)))
        if response.status not in (200, 202):
            raise ArticleProcessingError("Go article processor rejected the active job")
        return await _bounded_json(response)


async def _submit_until_not_busy(client, base, body, headers, active, deadline, possible_submit=None):
    delay = 0.25
    while True:
        try:
            return await _submit(client, base, body, headers, possible_submit)
        except ArticleProcessingBusy as exc:
            remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0 or not active.active:
                raise ArticleProcessingError("Go article processing deadline expired while busy") from exc
            await asyncio.sleep(min(remaining, exc.retry_after, delay))
            delay = min(delay * 1.5, 2.0)


async def _read_run(client, base, run_id, headers):
    async with client.get(f"{base}/internal/v1/processing/runs/{run_id}", headers=headers,
                          allow_redirects=False) as response:
        if response.status == 404:
            raise ArticleProcessingError("Go article processing run not found")
        if response.status != 200:
            raise ArticleProcessingError("Go article processing status was rejected")
        return await _bounded_json(response)


async def _cancel(client, base, run_id, headers, active):
    try:
        async with client.post(f"{base}/internal/v1/processing/runs/{run_id}/cancel", headers=headers,
                               allow_redirects=False) as response:
            if response.status == 200:
                receipt = await _bounded_json(response)
                _validate_identity(receipt, run_id, active)
                if receipt.get("state") in {"cancelled", "failed", "succeeded", "interrupted"}:
                    return True
    except (asyncio.TimeoutError, aiohttp.ClientError, ArticleProcessingError):
        pass
    try:
        receipt = await _read_run(client, base, run_id, headers)
        _validate_identity(receipt, run_id, active)
        return receipt.get("state") in {"cancelled", "failed", "succeeded", "interrupted"}
    except (asyncio.TimeoutError, aiohttp.ClientError, ArticleProcessingError):
        return False


async def _bounded_cancel(client, base, run_id, headers, active):
    """Attempt cancellation and reconciliation within a bounded cleanup window."""
    task = asyncio.create_task(_cancel(client, base, run_id, headers, active))
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=5)
    except asyncio.TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return False


async def _bounded_json(response):
    try:
        raw = bytearray()
        async for chunk in response.content.iter_chunked(8192):
            if len(raw) + len(chunk) > 16 * 1024:
                raise ArticleProcessingError("Go article processor response exceeded its limit")
            raw.extend(chunk)
        value = json.loads(raw)
    except (ValueError, TypeError, RecursionError, aiohttp.ClientError) as exc:
        raise ArticleProcessingError("Go article processor returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ArticleProcessingError("Go article processor response is invalid")
    return value


def _bounded_result(value):
    fields = ("articles_fetched", "articles_processed", "articles_summarized", "articles_categorized",
              "articles_requeued", "articles_rejected", "articles_retrying", "articles_exhausted",
              "requeued_count", "errors_count", "total_duration", "duration_seconds",
              "total_candidates", "processed", "improved", "found_candidates")
    result = {"status": value["status"]}
    for name in ("success", "settled", "retryable"):
        if type(value.get(name)) is bool:
            result[name] = value[name]
    for name in fields:
        number = value.get(name)
        if isinstance(number, int) and not isinstance(number, bool) and 0 <= number <= 1_000_000_000:
            result[name] = number
        elif isinstance(number, float) and math.isfinite(number) and 0 <= number <= 1_000_000_000:
            result[name] = number
    outcomes = value.get("article_outcomes")
    if isinstance(outcomes, list) and len(outcomes) <= 100:
        safe_states = {"completed", "rejected", "exhausted", "pending", "retry_wait", "processing"}
        safe_reasons = {"advertisement", "resource_limit", "category_mapping_failed", "missing_summary",
                        "smart_filter_rejected", "extraction_challenge", "provider_timeout"}
        compact = []
        for item in outcomes:
            if (isinstance(item, dict) and isinstance(item.get("article_id"), int)
                    and not isinstance(item.get("article_id"), bool)
                    and item.get("state") in safe_states):
                compact.append({"article_id": item["article_id"], "state": item["state"],
                                "reason": item.get("reason") if item.get("reason") in safe_reasons else "other"})
        result["article_outcomes"] = compact[:20]
        total = value.get("article_outcomes_total")
        if isinstance(total, int) and not isinstance(total, bool):
            total = max(len(compact), min(100, max(0, total)))
        else:
            total = len(compact)
        result["article_outcomes_total"] = total
        result["article_outcomes_omitted"] = max(
            total - len(result["article_outcomes"]),
            value.get("article_outcomes_omitted", 0)
            if isinstance(value.get("article_outcomes_omitted"), int)
            and not isinstance(value.get("article_outcomes_omitted"), bool) else 0,
        )
    for name in ("article_outcomes_total", "article_outcomes_omitted"):
        number = value.get(name)
        if isinstance(number, int) and not isinstance(number, bool) and 0 <= number <= 1_000_000_000:
            result[name] = number
    return result


def _validate_identity(run, run_id, active):
    try:
        job_id = str(active.job_id)
        attempt = active.attempt_generation
    except Exception as exc:
        raise ArticleProcessingError("active executor fence is invalid") from exc
    if (run.get("id") != run_id or run.get("job_id") != job_id
            or run.get("attempt_generation") != attempt
            or run.get("state") not in {"running", "succeeded", "failed", "cancelled", "interrupted"}):
        raise ArticleProcessingError("Go article processor returned a mismatched run")


def _valid_service_url(url):
    from urllib.parse import urlsplit
    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError:
        return False
    return (parsed.scheme in {"http", "https"} and bool(parsed.hostname)
            and not parsed.username and not parsed.password and not parsed.query
            and not parsed.fragment and not any(ch.isspace() for ch in url))


def _run_id(job_id, attempt):
    raw = bytearray(hashlib.sha256(
        f"evening-news-processing-v1\0{job_id}\0{attempt}".encode("ascii")
    ).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))
