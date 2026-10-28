import asyncio
import json
import os
import shlex
import stat
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
from aiohttp import web
import pytest

from news_aggregator.services.ai_runtime import (
    AIRuntime, AIRuntimeError, AIRuntimeProviderError, MAX_RPC_BYTES, _serialize_request,
)


def envelope(result=None, *, error=None):
    if error is not None:
        return {"version": 1, "ok": False, "error": error}
    return {"version": 1, "ok": True, "result": result or {}}


def test_request_size_limit_matches_utf8_wire_bytes():
    body = {"prompt": "é" * 1_700_000}
    payload = _serialize_request(body)
    assert len(payload) < MAX_RPC_BYTES
    assert b"\\u00e9" not in payload
    assert json.loads(payload) == body

    with pytest.raises(AIRuntimeError) as caught:
        _serialize_request({"prompt": "é" * (MAX_RPC_BYTES // 2 + 1)})
    assert caught.value.code == "runtime_invalid_input"


async def make_unix_rpc(tmp_path, handler):
    short_dir = Path(tempfile.mkdtemp(prefix="nai-rpc-", dir="/tmp"))
    socket = short_dir / "rpc.sock"
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.UnixSite(runner, str(socket))
    await site.start()
    runtime = AIRuntime("/unused")
    runtime.socket_path = socket
    runtime._token = "t" * 48
    runtime._ready = True
    runtime.process = type("Proc", (), {"returncode": None})()
    runtime._session = aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=str(socket)))
    return runtime, runner, short_dir


@pytest.mark.asyncio
async def test_rpc_auth_and_chunked_json_are_validated(tmp_path):
    async def handler(request):
        assert request.headers.get("Authorization") == "Bearer " + "t" * 48
        response = web.StreamResponse(status=200, headers={"Content-Type": "application/json"})
        await response.prepare(request)
        payload = json.dumps(envelope({"ready": True, "protocol_version": 1})).encode()
        for offset in range(0, len(payload), 5):
            await response.write(payload[offset:offset + 5])
        await response.write_eof()
        return response

    runtime, runner, short_dir = await make_unix_rpc(tmp_path, handler)
    try:
        assert await runtime.status() == {"ready": True, "protocol_version": 1}
    finally:
        await runtime._session.close()
        await runner.cleanup()
        shutil.rmtree(short_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_rpc_400_validation_is_not_provider_400(tmp_path):
    async def handler(request):
        await request.read()
        return web.json_response(envelope(error={"code": "invalid_input", "status_code": 400}), status=400)

    runtime, runner, short_dir = await make_unix_rpc(tmp_path, handler)
    try:
        with pytest.raises(AIRuntimeError) as caught:
            await runtime.permit()
        assert caught.value.code == "runtime_unavailable"
        assert not isinstance(caught.value, AIRuntimeProviderError)
    finally:
        await runtime._session.close()
        await runner.cleanup()
        shutil.rmtree(short_dir, ignore_errors=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 204, 304, 400, 401, 422, 429, 500, 503])
async def test_known_provider_status_survives_rpc_http_status(tmp_path, status):
    async def handler(request):
        await request.read()
        return web.json_response(
            envelope(error={"code": "provider_http_error", "status_code": status,
                            "retry_after_seconds": 7}), status=500
        )

    runtime, runner, short_dir = await make_unix_rpc(tmp_path, handler)
    try:
        with pytest.raises(AIRuntimeProviderError) as caught:
            await runtime.generate(mode="text", prompt="p", model="m", schema=None,
                                   temperature=0.1, max_tokens=1, analysis_type="digest",
                                   domain="example.test", timeout=1)
        assert caught.value.status_code == status
        assert caught.value.retry_after_seconds == 7
    finally:
        await runtime._session.close()
        await runner.cleanup()
        shutil.rmtree(short_dir, ignore_errors=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["provider_empty_response", "provider_invalid_json"])
async def test_provider_parse_errors_keep_legacy_success_status(tmp_path, code):
    async def handler(request):
        await request.read()
        return web.json_response(
            envelope(error={"code": code, "status_code": 200}), status=200
        )

    runtime, runner, short_dir = await make_unix_rpc(tmp_path, handler)
    try:
        with pytest.raises(AIRuntimeProviderError) as caught:
            await runtime.generate(mode="structured", prompt="p", model="m", schema={"type": "object"},
                                   temperature=0.1, max_tokens=1, analysis_type="digest",
                                   domain="example.test", timeout=1)
        assert caught.value.status_code == 200
    finally:
        await runtime._session.close()
        await runner.cleanup()
        shutil.rmtree(short_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_unexpected_disconnect_is_not_retried(tmp_path):
    requests = 0

    async def handler(request):
        nonlocal requests
        requests += 1
        await request.read()
        request.transport.close()
        return web.Response()

    runtime, runner, short_dir = await make_unix_rpc(tmp_path, handler)
    try:
        with pytest.raises(AIRuntimeError):
            await runtime.generate(mode="text", prompt="p", model="m", schema=None,
                                   temperature=0.1, max_tokens=1, analysis_type="digest",
                                   domain="example.test", timeout=1)
        assert requests == 1
    finally:
        await runtime._session.close()
        await runner.cleanup()
        shutil.rmtree(short_dir, ignore_errors=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(("attempt_timeout", "rpc_budget"), [(30, 98), (40, 128)])
async def test_generate_deadline_covers_backend_transport_attempt_series(attempt_timeout, rpc_budget):
    runtime = AIRuntime("/unused")
    runtime._rpc = AsyncMock(return_value={"choices": [], "usage": {}})
    await runtime.generate(mode="text", prompt="p", model="m", schema=None,
                           temperature=0.1, max_tokens=1, analysis_type="digest",
                           domain="example.test", timeout=attempt_timeout)
    assert runtime._rpc.await_args.kwargs["timeout"] == rpc_budget


def _fake_runtime_binary(tmp_path: Path) -> Path:
    source = tmp_path / "fake-ai-runtime.py"
    binary = tmp_path / "fake-ai-runtime"
    script = '''
import asyncio, json, os, stat
from aiohttp import web

async def main():
    path, token = os.environ["NEWS_AI_SOCKET"], os.environ["NEWS_AI_TOKEN"]
    app = web.Application()
    async def route(request):
        if request.headers.get("Authorization") != "Bearer " + token:
            return web.json_response({"version":1,"ok":False,"error":{"code":"unauthorized","status_code":401}}, status=401)
        if request.path == "/v1/status":
            return web.json_response({"version":1,"ok":True,"result":{"ready":True,"protocol_version":1,"usage_failed":0,"usage_dropped":0,"usage_pending":0}})
        if request.path == "/v1/generate":
            body = await request.json()
            if body.get("analysis_type") == "wait":
                await asyncio.Event().wait()
            return web.json_response({"version":1,"ok":True,"result":{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"cached_tokens":0,"total_tokens":2}}})
        return web.json_response({"version":1,"ok":True,"result":{"admitted":True}})
    app.router.add_route("*", "/{tail:.*}", route)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.UnixSite(runner, path)
    await site.start()
    os.chmod(path, 0o600)
    await asyncio.Event().wait()

asyncio.run(main())
'''
    source.write_text(script)
    binary.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(source)) + " \"$@\"\n"
    )
    binary.chmod(0o700)
    return binary


@pytest.mark.asyncio
async def test_runtime_start_auth_stop_and_owned_socket_cleanup(tmp_path, monkeypatch):
    from news_aggregator.services import ai_runtime as module

    monkeypatch.setattr(module.settings, "database_url", "postgresql+asyncpg://user:pass@127.0.0.1/db")
    monkeypatch.setattr(module.settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(module.settings, "gemini_api_endpoint", "https://example.test/v1/models")
    monkeypatch.setattr(module.settings, "summarization_model", "fixture-model")
    monkeypatch.setattr(module.settings, "api_rate_limit", 3)
    monkeypatch.setattr(module.settings, "local_category", None)
    monkeypatch.setattr(module.settings, "ai_input_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_output_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_cached_input_cost_per_1m", None)
    runtime = AIRuntime(str(_fake_runtime_binary(tmp_path)))
    await runtime.start()
    owned = runtime.socket_path
    assert owned and stat.S_IMODE(owned.lstat().st_mode) == 0o600
    assert stat.S_IMODE(owned.parent.stat().st_mode) == 0o700
    assert (await runtime.status())["ready"] is True
    token = runtime._token
    runtime._token = "x" * 48
    with pytest.raises(AIRuntimeError) as caught:
        await runtime.status()
    assert caught.value.code == "runtime_authentication_failed"
    runtime._token = token
    await runtime.stop()
    assert not owned.parent.exists()
    assert runtime.process is None


@pytest.mark.asyncio
async def test_runtime_cleanup_preserves_replaced_socket_path(tmp_path, monkeypatch):
    from news_aggregator.services import ai_runtime as module

    monkeypatch.setattr(module.settings, "database_url", "postgresql+asyncpg://user:pass@127.0.0.1/db")
    monkeypatch.setattr(module.settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(module.settings, "gemini_api_endpoint", "https://example.test/v1/models")
    monkeypatch.setattr(module.settings, "summarization_model", "fixture-model")
    monkeypatch.setattr(module.settings, "api_rate_limit", 3)
    monkeypatch.setattr(module.settings, "local_category", None)
    monkeypatch.setattr(module.settings, "ai_input_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_output_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_cached_input_cost_per_1m", None)
    runtime = AIRuntime(str(_fake_runtime_binary(tmp_path)))
    await runtime.start()
    path = runtime.socket_path
    parent = path.parent
    path.unlink()
    path.write_text("replacement fixture")
    await runtime.stop()
    assert path.read_text() == "replacement fixture"
    path.unlink()
    parent.rmdir()


@pytest.mark.asyncio
async def test_runtime_cleanup_leaves_unexpected_owned_directory_entries(tmp_path, monkeypatch):
    from news_aggregator.services import ai_runtime as module

    monkeypatch.setattr(module.settings, "database_url", "postgresql+asyncpg://user:pass@127.0.0.1/db")
    monkeypatch.setattr(module.settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(module.settings, "gemini_api_endpoint", "https://example.test/v1/models")
    monkeypatch.setattr(module.settings, "summarization_model", "fixture-model")
    monkeypatch.setattr(module.settings, "api_rate_limit", 3)
    monkeypatch.setattr(module.settings, "local_category", None)
    monkeypatch.setattr(module.settings, "ai_input_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_output_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_cached_input_cost_per_1m", None)
    runtime = AIRuntime(str(_fake_runtime_binary(tmp_path)))
    await runtime.start()
    socket_path = runtime.socket_path
    unexpected = socket_path.parent / "preserve-me"
    unexpected.write_text("unrelated fixture")
    await runtime.stop()
    assert unexpected.read_text() == "unrelated fixture"
    assert not socket_path.exists()
    unexpected.unlink()
    socket_path.parent.rmdir()


@pytest.mark.asyncio
async def test_runtime_child_death_marks_not_ready_and_notifies_executor(tmp_path, monkeypatch):
    from news_aggregator.services import ai_runtime as module

    monkeypatch.setattr(module.settings, "database_url", "postgresql+asyncpg://user:pass@127.0.0.1/db")
    monkeypatch.setattr(module.settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(module.settings, "gemini_api_endpoint", "https://example.test/v1/models")
    monkeypatch.setattr(module.settings, "summarization_model", "fixture-model")
    monkeypatch.setattr(module.settings, "api_rate_limit", 3)
    monkeypatch.setattr(module.settings, "local_category", None)
    monkeypatch.setattr(module.settings, "ai_input_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_output_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_cached_input_cost_per_1m", None)
    runtime = AIRuntime(str(_fake_runtime_binary(tmp_path)))
    stopped = asyncio.Event()
    await runtime.start(on_child_exit=lambda: _async_set(stopped))
    runtime.process.kill()
    await asyncio.wait_for(stopped.wait(), 2)
    assert runtime.ready is False
    await runtime.stop()


@pytest.mark.asyncio
async def test_canceling_one_rpc_keeps_shared_runtime_alive(tmp_path, monkeypatch):
    from news_aggregator.services import ai_runtime as module

    monkeypatch.setattr(module.settings, "database_url", "postgresql+asyncpg://user:pass@127.0.0.1/db")
    monkeypatch.setattr(module.settings, "gemini_api_key", "fixture-only-key")
    monkeypatch.setattr(module.settings, "gemini_api_endpoint", "https://example.test/v1/models")
    monkeypatch.setattr(module.settings, "summarization_model", "fixture-model")
    monkeypatch.setattr(module.settings, "api_rate_limit", 3)
    monkeypatch.setattr(module.settings, "local_category", None)
    monkeypatch.setattr(module.settings, "ai_input_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_output_cost_per_1m", None)
    monkeypatch.setattr(module.settings, "ai_cached_input_cost_per_1m", None)
    runtime = AIRuntime(str(_fake_runtime_binary(tmp_path)))
    await runtime.start()
    blocked = asyncio.create_task(runtime.generate(
        mode="text", prompt="fixture", model="fixture-model", schema=None,
        temperature=0.1, max_tokens=1, analysis_type="wait", domain="example.test", timeout=15,
    ))
    await asyncio.sleep(0.1)
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    assert runtime.ready is True
    result = await runtime.generate(
        mode="text", prompt="fixture", model="fixture-model", schema=None,
        temperature=0.1, max_tokens=1, analysis_type="digest", domain="example.test", timeout=15,
    )
    assert result["choices"][0]["message"]["content"] == "ok"
    await runtime.stop()


async def _async_set(event):
    event.set()
