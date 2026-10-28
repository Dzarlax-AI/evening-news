"""Lifecycle and narrow authenticated RPC client for the native AI runtime."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import secrets
import stat
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp

from ..config import settings
from ..core.exceptions import APIError

logger = logging.getLogger(__name__)
MAX_RPC_BYTES = 5 * 1024 * 1024
MAX_DIAGNOSTIC_BYTES = 16 * 1024
PROVIDER_STATUS_CODES = range(100, 600)
PROVIDER_RESPONSE_CODES = {"provider_http_error", "provider_empty_response", "provider_invalid_json",
                           "provider_invalid_response"}


def _strict_json(raw: bytes | bytearray) -> Any:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(_value):
        raise ValueError("non-finite number")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject_constant)


def _serialize_request(request_body: dict) -> bytes:
    try:
        payload = json.dumps(
            request_body, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise AIRuntimeError("runtime_invalid_input") from None
    if len(payload) > MAX_RPC_BYTES:
        raise AIRuntimeError("runtime_invalid_input")
    return payload


class AIRuntimeError(RuntimeError):
    """Finite, non-retryable local runtime failure."""

    def __init__(self, code: str = "runtime_unavailable"):
        self.code = code if code in {
            "runtime_unavailable", "runtime_protocol_error", "runtime_authentication_failed",
            "runtime_not_ready", "runtime_draining", "runtime_deadline", "runtime_invalid_input",
        } else "runtime_unavailable"
        super().__init__(self.code)


class DigestOperationError(AIRuntimeError):
    def __init__(self, code: str):
        allowed = {"domain_operation_failed", "stale_fence", "authority_unavailable", "external_outcome_unknown",
                   "prepared_digest_missing", "resource_limit", "editorial_not_ready", "editorial_resource_limit",
                   "editorial_provider_timeout", "editorial_provider_unavailable", "editorial_invalid_result", "editorial_generation_failed"}
        super().__init__()
        self.code = code if isinstance(code, str) and code in allowed else "domain_operation_failed"
        self.args = (self.code,)


class AIRuntimeProviderError(APIError):
    """Known provider error returned by the runtime, never a local HTTP status."""

    def __init__(self, code: str, status_code: int | None, retry_after_seconds: float | None = None):
        self.code = code
        self.retry_after_seconds = retry_after_seconds
        messages = {
            "provider_empty_response": "Empty Gemini response",
            "provider_invalid_json": "Failed to parse JSON from Gemini structured response",
            "provider_invalid_response": "Gemini API returned an invalid response",
        }
        message = ("Rate limit exceeded" if status_code == 429 else
                   f"Gemini API error: {status_code}" if code == "provider_http_error" else
                   messages.get(code, code))
        super().__init__(message, status_code=status_code)


class AIRuntime:
    def __init__(self, binary: str | None = None):
        self.binary = binary or settings.news_ai_runtime_binary
        self.process: asyncio.subprocess.Process | None = None
        self.socket_path: Path | None = None
        self._owned_dir: Path | None = None
        self._socket_identity: tuple[int, int] | None = None
        self._token: str | None = None
        self._session: aiohttp.ClientSession | None = None
        self._watch_task: asyncio.Task | None = None
        self._stdout_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._closing = False
        self._ready = False
        self._on_child_exit = None
        self._inflight: set[asyncio.Task] = set()
        self._notify_tasks: set[asyncio.Task] = set()

    @property
    def ready(self) -> bool:
        return self._ready and not self._closing and self.process is not None and self.process.returncode is None

    async def start(self, on_child_exit=None) -> None:
        async with self._lock:
            if self.ready:
                return
            if self.process is not None:
                raise AIRuntimeError("runtime_not_ready")
            if not isinstance(self.binary, str) or not os.path.isabs(self.binary):
                raise AIRuntimeError("runtime_unavailable")
            self._on_child_exit = on_child_exit
            self._closing = False
            self._owned_dir = Path(tempfile.mkdtemp(prefix="nai-", dir="/tmp"))
            os.chmod(self._owned_dir, 0o700)
            self.socket_path = self._owned_dir / "ai.sock"
            self._token = secrets.token_urlsafe(48)
            child_env = self._child_environment(str(self.socket_path), self._token)
            try:
                self.process = await asyncio.create_subprocess_exec(
                    self.binary, env=child_env, stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                self._stdout_task = asyncio.create_task(self._drain(self.process.stdout))
                self._stderr_task = asyncio.create_task(self._drain(self.process.stderr))
                self._watch_task = asyncio.create_task(self._watch_child())
                connector = aiohttp.UnixConnector(path=str(self.socket_path), limit=32)
                self._session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=None))
                await self._wait_until_ready()
                self._ready = True
            except BaseException:
                await self._stop_locked(force=True)
                raise

    @staticmethod
    def _child_environment(socket_path: str, token: str) -> dict[str, str]:
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "NEWS_AI_SOCKET": socket_path, "NEWS_AI_TOKEN": token,
            "DATABASE_URL": settings.database_url, "GEMINI_API_ENDPOINT": settings.gemini_api_endpoint,
            "SUMMARIZATION_MODEL": settings.summarization_model, "RPS": str(settings.api_rate_limit),
            "NEWS_AI_DB_MAX_CONNS": "2",
        }
        if settings.gemini_api_key:
            env["GEMINI_API_KEY"] = settings.gemini_api_key
        if os.environ.get("NEWS_FIXTURE_MODE") == "1":
            env["NEWS_FIXTURE_MODE"] = "1"
            env["NEWS_TEST_TELEGRAM_ENDPOINT"] = os.environ.get("NEWS_TEST_TELEGRAM_ENDPOINT", "")
        if settings.telegram_token:
            env["TELEGRAM_TOKEN"] = settings.telegram_token.get_secret_value()
        if settings.telegram_chat_id:
            env["TELEGRAM_CHAT_ID"] = settings.telegram_chat_id
        if settings.telegram_service_chat_id:
            env["TELEGRAM_SERVICE_CHAT_ID"] = settings.telegram_service_chat_id
        env["DIGEST_MODEL"] = settings.digest_model
        env["SITE_BASE_URL"] = settings.site_base_url
        env["TELEGRAM_RICH_MESSAGES_ENABLED"] = str(settings.telegram_rich_messages_enabled).lower()
        if settings.local_category:
            env["LOCAL_CATEGORY"] = settings.local_category
        for key, value in (
            ("AI_INPUT_COST_PER_1M", settings.ai_input_cost_per_1m),
            ("AI_OUTPUT_COST_PER_1M", settings.ai_output_cost_per_1m),
            ("AI_CACHED_INPUT_COST_PER_1M", settings.ai_cached_input_cost_per_1m),
        ):
            if value is not None:
                env[key] = str(value)
        return env

    async def _drain(self, stream) -> None:
        if stream is None:
            return
        kept = 0
        while True:
            chunk = await stream.read(8192)
            if not chunk:
                return
            kept += min(len(chunk), max(0, MAX_DIAGNOSTIC_BYTES - kept))

    async def _wait_until_ready(self) -> None:
        deadline = asyncio.get_running_loop().time() + 8
        while asyncio.get_running_loop().time() < deadline:
            if self.process is None or self.process.returncode is not None:
                raise AIRuntimeError("runtime_unavailable")
            if self.socket_path and self.socket_path.exists():
                try:
                    socket_stat = self.socket_path.lstat()
                    if not stat.S_ISSOCK(socket_stat.st_mode):
                        raise AIRuntimeError("runtime_protocol_error")
                    identity = (socket_stat.st_dev, socket_stat.st_ino)
                    if self._socket_identity is None:
                        self._socket_identity = identity
                    elif self._socket_identity != identity:
                        raise AIRuntimeError("runtime_protocol_error")
                    result = await self._rpc("GET", "/v1/status", timeout=0.5, require_ready=False)
                    if result.get("ready") is True and result.get("protocol_version") == 1:
                        try:
                            current = self.socket_path.lstat()
                        except OSError:
                            raise AIRuntimeError("runtime_protocol_error") from None
                        if (not stat.S_ISSOCK(current.st_mode) or current.st_mode & 0o777 != 0o600
                                or (current.st_dev, current.st_ino) != identity):
                            raise AIRuntimeError("runtime_protocol_error")
                        return
                except AIRuntimeError as exc:
                    if exc.code != "runtime_unavailable":
                        raise
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
            await asyncio.sleep(0.05)
        raise AIRuntimeError("runtime_not_ready")

    async def _watch_child(self) -> None:
        process = self.process
        if process is None:
            return
        await process.wait()
        if self._closing:
            return
        self._ready = False
        self._closing = True
        for task in tuple(self._inflight):
            task.cancel()
        logger.error("native AI runtime child exited; executor is becoming unavailable")
        callback = self._on_child_exit
        if callback is not None:
            async def notify_executor():
                try:
                    await callback()
                except Exception:
                    logger.error("native AI runtime exit handler failed")
            task = asyncio.create_task(notify_executor())
            self._notify_tasks.add(task)
            task.add_done_callback(self._notify_tasks.discard)

    async def _rpc(self, method: str, path: str, body: dict | None = None,
                   *, timeout: float = 42, require_ready: bool = True) -> dict:
        if require_ready and not self.ready:
            raise AIRuntimeError("runtime_unavailable")
        if not self._session or not self._token:
            raise AIRuntimeError("runtime_unavailable")
        request_body = body
        payload = None
        if body is not None:
            request_body = {"version": 1, "request_id": str(uuid.uuid4()),
                            "deadline_at": (datetime.now(timezone.utc) + timedelta(seconds=min(timeout, 240))).isoformat(),
                            **body}
            payload = _serialize_request(request_body)
        task = asyncio.current_task()
        if task:
            self._inflight.add(task)
        try:
            async with asyncio.timeout(timeout):
                headers = {"Authorization": f"Bearer {self._token}"}
                if payload is not None:
                    headers["Content-Type"] = "application/json"
                async with self._session.request(
                    method, f"http://unix{path}", data=payload,
                    headers=headers,
                    ) as response:
                        raw = bytearray()
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            raw.extend(chunk)
                            if len(raw) > MAX_RPC_BYTES:
                                raise AIRuntimeError("runtime_protocol_error")
                        if len(raw) > MAX_RPC_BYTES:
                            raise AIRuntimeError("runtime_protocol_error")
                        try:
                            envelope = _strict_json(raw)
                        except (UnicodeError, ValueError, json.JSONDecodeError):
                            raise AIRuntimeError("runtime_protocol_error") from None
                        if not isinstance(envelope, dict) or type(envelope.get("version")) is not int or envelope.get("version") != 1:
                            raise AIRuntimeError("runtime_protocol_error")
                        if envelope.get("ok") is True and isinstance(envelope.get("result"), dict):
                            if response.status != 200:
                                raise AIRuntimeError("runtime_protocol_error")
                            return envelope["result"]
                        detail = envelope.get("error")
                        if envelope.get("ok") is not False or not isinstance(detail, dict):
                            raise AIRuntimeError("runtime_protocol_error")
                        code = detail.get("code")
                        status = detail.get("status_code")
                        retry_after = detail.get("retry_after_seconds")
                        if not isinstance(code, str) or len(code) > 64:
                            raise AIRuntimeError("runtime_protocol_error")
                        if code == "unauthorized":
                            raise AIRuntimeError("runtime_authentication_failed")
                        if code == "runtime_draining":
                            raise AIRuntimeError("runtime_draining")
                        if (code in PROVIDER_RESPONSE_CODES and type(status) is int
                                and status in PROVIDER_STATUS_CODES):
                            try:
                                valid_retry_after = (retry_after is None or (
                                    not isinstance(retry_after, bool)
                                    and isinstance(retry_after, (int, float))
                                    and math.isfinite(float(retry_after)) and retry_after >= 0
                                ))
                            except (OverflowError, ValueError):
                                valid_retry_after = False
                            if not valid_retry_after:
                                retry_after = None
                            raise AIRuntimeProviderError(code, status, float(retry_after) if retry_after is not None else None)
                        raise AIRuntimeError("runtime_deadline" if code == "provider_timeout" else "runtime_unavailable")
        except asyncio.CancelledError:
            raise
        except AIRuntimeError:
            raise
        except AIRuntimeProviderError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            raise AIRuntimeError("runtime_unavailable") from None
        finally:
            if task:
                self._inflight.discard(task)

    async def digest_operation(self, record, context) -> dict:
        """Accept once; reconcile a lost ACK only by reading the same receipt."""
        body = {"operation": record.operation, "operation_id": record.operation_id,
                "domain_instance_id": record.domain_instance_id,
                "envelope": record.expected_envelope,
                "input_fingerprint": record.input_fingerprint, "input": record.input,
                "kind": context.kind, "payload": context.payload,
                "executor_backend_pid": context.executor_backend_pid}
        key = f"{record.job_id}/{record.attempt_generation}/{record.operation}/{record.operation_id}"
        path = "/v2/digest/operations?" + urlencode({"key": key, "domain_instance_id": record.domain_instance_id})
        try:
            receipt = await self._rpc("POST", "/v2/digest/operations", {"input": body}, timeout=4)
        except AIRuntimeError:
            receipt = await self._rpc("GET", path, timeout=4)
        while True:
            if (receipt.get("job_id") != record.job_id
                    or receipt.get("attempt_generation") != record.attempt_generation
                    or receipt.get("operation") != record.operation
                    or receipt.get("operation_id") != record.operation_id
                    or receipt.get("domain_instance_id") != record.domain_instance_id
                    or receipt.get("input_fingerprint") != record.input_fingerprint):
                raise AIRuntimeError("runtime_protocol_error")
            state = receipt.get("state")
            if state == "succeeded" and isinstance(receipt.get("result"), dict):
                return receipt["result"]
            if state == "failed":
                raise DigestOperationError(receipt.get("error_code"))
            if state != "running" or record.deadline <= datetime.now(timezone.utc):
                raise AIRuntimeError("runtime_protocol_error")
            await asyncio.sleep(min(0.5, max(0, (record.deadline - datetime.now(timezone.utc)).total_seconds())))
            receipt = await self._rpc("GET", path, timeout=4)

    async def telegram_send(self, channel: str, method: str, text: str, keyboard=None) -> dict:
        body = {"channel": channel, "method": method, "text": text}
        if keyboard:
            body["keyboard"] = keyboard
        result = await self._rpc("POST", "/v1/telegram/send", body, timeout=35)
        if result.get("state") not in {"sent", "rejected", "unknown"}:
            raise AIRuntimeError("runtime_protocol_error")
        return result

    async def status(self) -> dict:
        return await self._rpc("GET", "/v1/status", timeout=2)

    async def permit(self, timeout: float = 60) -> None:
        await self._rpc("POST", "/v1/permit", {}, timeout=timeout)

    async def invalidate_categories(self) -> None:
        await self._rpc("POST", "/v1/categories/invalidate", {}, timeout=5)

    async def analyze(self, title: str, content: str, url: str, original_context: str | None,
                      max_text_chars: int, model: str, deadline: datetime) -> dict:
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            raise AIRuntimeError("runtime_deadline")
        payload = {"input": {"title": title, "content": content, "url": url,
                              "original_context": original_context, "max_text_chars": max_text_chars},
                   "model": model}
        result = await self._rpc("POST", "/v1/analyze", payload, timeout=remaining)
        value = result.get("result")
        if not isinstance(value, dict):
            raise AIRuntimeError("runtime_protocol_error")
        return value

    async def generate(self, *, mode: str, prompt: str, model: str, schema: dict | None,
                       temperature: float, max_tokens: int, analysis_type: str, domain: str,
                       timeout: float) -> dict:
        body = {"mode": mode, "prompt": prompt, "model": model,
                "options": {"temperature": temperature, "max_tokens": max_tokens},
                "analysis_type": analysis_type, "domain": domain}
        if schema is not None:
            body["schema"] = schema
        # `timeout` is the legacy per-provider-attempt limit. The Go side owns
        # up to three bounded transport attempts and two four-second waits.
        result = await self._rpc("POST", "/v1/generate", body, timeout=timeout * 3 + 8)
        if mode == "text":
            if not isinstance(result.get("choices"), list) or not isinstance(result.get("usage"), dict):
                raise AIRuntimeError("runtime_protocol_error")
        elif not isinstance(result.get("usage"), dict) or "result" not in result:
            raise AIRuntimeError("runtime_protocol_error")
        return result

    async def stop(self, *, timeout: float = 8) -> None:
        async with self._lock:
            await self._stop_locked(force=False, timeout=timeout)

    async def _stop_locked(self, *, force: bool, timeout: float = 8) -> None:
        self._closing = True
        self._ready = False
        current = asyncio.current_task()
        inflight = [task for task in tuple(self._inflight) if task is not current]
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        process = self.process
        if process is not None and process.returncode is None:
            if force:
                process.kill()
            else:
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        for task in (self._stdout_task, self._stderr_task):
            if task:
                task.cancel()
        drains = [task for task in (self._stdout_task, self._stderr_task) if task]
        if drains:
            await asyncio.gather(*drains, return_exceptions=True)
        if self._watch_task and self._watch_task is not asyncio.current_task():
            self._watch_task.cancel()
            await asyncio.gather(self._watch_task, return_exceptions=True)
        self.process = None
        self._watch_task = self._stdout_task = self._stderr_task = None
        self._remove_owned_paths()

    def _remove_owned_paths(self) -> None:
        path = self.socket_path
        if path is not None and self._socket_identity is not None:
            try:
                st = path.lstat()
                if (st.st_dev, st.st_ino) == self._socket_identity and st.st_mode & 0o170000 == 0o140000:
                    path.unlink()
            except FileNotFoundError:
                pass
        directory = self._owned_dir
        if directory is not None:
            try:
                if directory.is_dir() and not directory.is_symlink() and directory.stat().st_uid == os.getuid():
                    # Remove only the socket whose identity we recorded. If
                    # anything else appeared here, leave it untouched.
                    directory.rmdir()
            except OSError:
                pass
        self.socket_path = self._owned_dir = None
        self._socket_identity = None
        self._token = None


_runtime = AIRuntime()


def get_ai_runtime() -> AIRuntime:
    return _runtime
