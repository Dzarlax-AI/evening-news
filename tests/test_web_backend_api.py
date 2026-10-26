import asyncio
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from news_aggregator.services.web_backend import backup_path, cleanup_backups, list_backups, save_upload


def test_backup_upload_is_atomic_bounded_and_confined(tmp_path):
    target = tmp_path / "safe.tar.gz"
    assert asyncio.run(save_upload(tmp_path, target.name, io.BytesIO(b"archive"))) == 7
    assert target.read_bytes() == b"archive"
    with pytest.raises(ValueError):
        backup_path(tmp_path, "../outside.tar.gz")
    (tmp_path / "link.tar.gz").symlink_to(target)
    with pytest.raises(ValueError):
        backup_path(tmp_path, "link.tar.gz")


def test_backup_upload_removes_temporary_file_on_oversize(monkeypatch, tmp_path):
    import news_aggregator.services.web_backend as backend
    monkeypatch.setattr(backend, "MAX_BACKUP_BYTES", 3)
    with pytest.raises(ValueError):
        asyncio.run(backend.save_upload(tmp_path, "large.tar.gz", io.BytesIO(b"four")))
    assert not list(tmp_path.iterdir())


def test_backup_cleanup_keeps_newest_files(tmp_path):
    old = tmp_path / "old.tar.gz"
    new = tmp_path / "new.tar.gz"
    old.write_bytes(b"x")
    new.write_bytes(b"x")
    import os, time
    os.utime(old, (time.time() - 100, time.time() - 100))
    result = cleanup_backups(tmp_path, 1)
    assert old.exists() is False
    assert new.exists() is True
    assert result["deleted"] == 1


def test_backup_listing_preserves_admin_page_fields_and_legacy_byte_size(tmp_path):
    backup = tmp_path / "go-web-fixture.tar.gz"
    backup.write_bytes(b"x" * (3 * 1024 * 1024 + 17))

    result = list_backups(tmp_path)

    assert result["total"] == 1
    item = result["backups"][0]
    assert item["filename"] == "go-web-fixture.tar.gz"
    assert item["filepath"] == "backups/go-web-fixture.tar.gz"
    assert item["size"] == 3 * 1024 * 1024 + 17
    assert item["size_mb"] == 3.0
    assert item["created_at"]


def test_private_backend_auth_fails_closed_and_uses_constant_time(monkeypatch):
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", None)
    from starlette.requests import Request
    request = Request({"type": "http", "headers": []})
    with pytest.raises(Exception) as unavailable:
        executor._web_backend_auth(request)
    assert getattr(unavailable.value, "status_code", None) == 503

    from pydantic import SecretStr
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("test-secret"))
    wrong = Request({"type": "http", "headers": [(b"authorization", b"Bearer wrong")]})
    with pytest.raises(Exception) as unauthorized:
        executor._web_backend_auth(wrong)
    assert getattr(unauthorized.value, "status_code", None) == 401
    valid = Request({"type": "http", "headers": [(b"authorization", b"Bearer test-secret")]})
    assert executor._web_backend_auth(valid) is None


def test_private_request_models_reject_extra_and_invalid_values():
    from news_aggregator.services.web_backend import BackupCleanupRequest, BackupScheduleRequest
    with pytest.raises(Exception):
        BackupCleanupRequest(keep_count=101)
    with pytest.raises(Exception):
        BackupScheduleRequest(enabled=True, frequency="hourly", time="99:99", keep_count=5)
    with pytest.raises(Exception):
        BackupScheduleRequest(enabled=True, frequency="daily", time="02:00", keep_count=5,
                              target="https://example.invalid")


def test_telegram_test_uses_service_contract_with_fake(monkeypatch):
    from news_aggregator.services import telegram_service
    from news_aggregator.services.web_backend import telegram_test

    class Result:
        def __bool__(self):
            return True

    class FakeService:
        async def test_connection(self):
            return Result()

    def fake_get(**kwargs):
        return FakeService()

    class FakeDB:
        async def get(self, model, key):
            return None

    monkeypatch.setattr(telegram_service, "get_telegram_service", fake_get)
    result = asyncio.run(telegram_test(FakeDB()))
    assert result["success"] is True
    assert result["error"] is None


def test_telegram_configuration_returns_only_effective_metadata(monkeypatch):
    from pydantic import SecretStr
    from news_aggregator.config import settings
    from news_aggregator.services.web_backend import telegram_configuration
    monkeypatch.setattr(settings, "telegram_token", SecretStr("do-not-return"))
    monkeypatch.setattr(settings, "telegram_chat_id", "env-main")
    monkeypatch.setattr(settings, "telegram_service_chat_id", None)

    class FakeDB:
        async def get(self, model, key):
            return SimpleNamespace(value="db-main") if key == "telegram_chat_id" else None

    result = asyncio.run(telegram_configuration(FakeDB()))
    assert result == {"token_configured": True, "chat_id": "db-main", "chat_id_source": "db",
                      "service_chat_id": "db-main", "service_chat_id_source": "fallback"}
    assert "token" not in result
    assert all("do-not-return" not in str(value) for value in result.values())


def test_category_analysis_uses_fake_ai_and_returns_legacy_shape(monkeypatch):
    from news_aggregator.config import settings
    from news_aggregator.services import ai_client
    from news_aggregator.services.web_backend import CategoryAnalysisRequest, analyze_categories
    monkeypatch.setattr(settings, "background_execution_mode", "embedded")

    class FakeDB:
        calls = 0
        async def execute(self, statement, params=None):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(fetchall=lambda: [SimpleNamespace(
                    ai_category="technology", usage_count=4, example_titles="one | two")])
            return SimpleNamespace(fetchall=lambda: [("Tech", "Technology")])

    class FakeAI:
        async def _call_summary_llm(self, **kwargs):
            assert "AI CATEGORY: \"technology\"" in kwargs["prompt"]
            return '{"suggestions":[{"category":"Tech (Technology)","confidence":0.9,"reasoning":"match"}]}'

    monkeypatch.setattr(ai_client, "get_ai_client", lambda: FakeAI())
    result = asyncio.run(analyze_categories(FakeDB(), CategoryAnalysisRequest()))
    assert result["total_analyzed"] == 1
    item = result["suggestions"][0]
    assert item["suggested_fixed_category"] == "Tech"
    assert item["usage_count"] == 4
    assert item["example_articles"] == ["one", "two"]


def test_category_analysis_stays_disabled_in_queue_only_mode(monkeypatch):
    from fastapi import HTTPException
    from news_aggregator.config import settings
    from news_aggregator.services import ai_client
    from news_aggregator.services.web_backend import CategoryAnalysisRequest, analyze_categories

    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(ai_client, "get_ai_client", lambda: pytest.fail("provider must not be invoked"))
    with pytest.raises(HTTPException) as error:
        asyncio.run(analyze_categories(object(), CategoryAnalysisRequest()))
    assert error.value.status_code == 409


def test_no_private_backup_job_producer_and_restore_has_no_side_effect(monkeypatch, tmp_path):
    from fastapi import HTTPException
    from news_aggregator import executor
    from news_aggregator.config import settings
    from news_aggregator.services.web_backend import BackupFilename
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    async def ready(_request):
        return None
    monkeypatch.setattr(executor, "_web_backend_ready", ready)
    paths = {getattr(route, "path", "") for route in executor.app.routes}
    assert "/internal/web/v1/backups/create" not in paths
    assert "/internal/web/v1/backups/sync" not in paths
    backup = tmp_path / "backups" / "preserve.tar.gz"
    backup.parent.mkdir()
    backup.write_bytes(b"fixture")
    with pytest.raises(HTTPException) as error:
        asyncio.run(executor.web_backup_restore(BackupFilename(filename=backup.name), object()))
    assert error.value.status_code == 409
    assert backup.read_bytes() == b"fixture"


@pytest.mark.parametrize(("path", "method", "content_type", "detail"), [
    ("/internal/web/v1/backups/upload", "POST", "multipart/form-data; boundary=x",
     "Backup uploads are disabled in worker mode; use offline maintenance with the worker stopped."),
    ("/internal/web/v1/backups/cleanup", "POST", "application/json",
     "Backup cleanup is disabled in worker mode; use offline maintenance with the worker stopped."),
    ("/internal/web/v1/backups/restore", "POST", "application/json",
     "Restore is disabled in worker mode; stop the worker and use offline maintenance."),
])
def test_worker_backup_mutations_rejected_before_body_read_or_filesystem(
        monkeypatch, tmp_path, path, method, content_type, detail):
    from pydantic import SecretStr
    from news_aggregator import executor
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(executor, "_backup_root", lambda: pytest.fail("backup path must not be inspected"))
    monkeypatch.setattr(executor, "save_upload", lambda *_: pytest.fail("upload must not be saved"))
    monkeypatch.setattr(executor, "cleanup_backups", lambda *_: pytest.fail("backups must not be deleted"))
    monkeypatch.setattr(executor, "backup_path", lambda *_: pytest.fail("restore path must not be checked"))
    import tempfile
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    headers = [(b"authorization", b"Bearer expected"), (b"content-type", content_type.encode())]
    # The operation-mode response takes precedence over malformed/oversized
    # bodies, matching the legacy route and avoiding request-body spooling.
    headers.append((b"content-length", b"999999999"))
    sent, reads = _asgi_request(executor, path, method, headers, [(b"unread body", False)])
    assert sent[0]["status"] == 409
    assert detail.encode() in sent[1]["body"]
    assert reads == 0
    assert not list(tmp_path.iterdir())


def _asgi_request(executor, path, method="GET", headers=(), chunks=()):
    async def run():
        sent = []
        received = 0
        items = iter(chunks)
        body_done = False

        async def receive():
            nonlocal received, body_done
            received += 1
            if body_done:
                await asyncio.Event().wait()
            try:
                body, more = next(items)
                if not more:
                    body_done = True
                return {"type": "http.request", "body": body, "more_body": more}
            except StopIteration:
                body_done = True
                return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
                 "http_version": "1.1", "method": method, "scheme": "http",
                 "path": path, "raw_path": path.encode(), "query_string": b"",
                 "root_path": "", "headers": list(headers), "client": ("127.0.0.1", 1),
                 "server": ("testserver", 80)}
        await executor.app(scope, receive, send)
        return sent, received
    return asyncio.run(run())


def test_private_auth_runs_before_any_body_read(monkeypatch):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "analyze_categories", lambda *_: pytest.fail("handler must not run"))
    sent, reads = _asgi_request(executor, "/internal/web/v1/categories/ai-category-analysis", "POST",
                                [(b"authorization", b"Bearer wrong"),
                                 (b"content-length", b"999999999")], [(b"x" * 100, False)])
    assert sent[0]["status"] == 401
    assert reads == 0


def test_telegram_settings_checks_executor_before_database(monkeypatch):
    from pydantic import SecretStr
    from fastapi import HTTPException
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    checked = []
    async def not_ready():
        checked.append(True)
        raise HTTPException(status_code=503, detail="not owner")
    monkeypatch.setattr(executor, "_require_executor_guard", not_ready)
    monkeypatch.setattr(executor, "telegram_configuration", lambda *_: pytest.fail("database must not run"))
    sent, _ = _asgi_request(executor, "/internal/web/v1/telegram/configuration", "GET",
                            [(b"authorization", b"Bearer expected")])
    assert sent[0]["status"] == 503
    assert checked == [True]


@pytest.mark.parametrize("content_length", [None, b"1"])
def test_private_stream_body_limit_rejects_chunked_and_lying_lengths(monkeypatch, tmp_path, content_length):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "background_execution_mode", "inline")
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "MAX_PRIVATE_UPLOAD_BODY_BYTES", 16)
    monkeypatch.setattr(executor, "save_upload", lambda *_: pytest.fail("upload handler must not run"))
    import tempfile
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    headers = [(b"authorization", b"Bearer expected"),
               (b"content-type", b"multipart/form-data; boundary=x")]
    if content_length is not None:
        headers.append((b"content-length", content_length))
    sent, _reads = _asgi_request(executor, "/internal/web/v1/backups/upload", "POST", headers,
                                 [(b"a" * 10, True), (b"b" * 10, False)])
    assert sent[0]["status"] == 413
    assert not list(tmp_path.iterdir())


def test_private_upload_deadline_cancels_handler_and_cleans_staging(monkeypatch, tmp_path):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "background_execution_mode", "inline")
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "PRIVATE_STREAM_TIMEOUT_SECONDS", 0.01)
    async def ready(_request):
        return None
    monkeypatch.setattr(executor, "_web_backend_ready", ready)
    stage = tmp_path / "staged.tmp"
    cancelled = []
    async def slow_save(*_args):
        stage.write_bytes(b"partial")
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.append(True)
            stage.unlink(missing_ok=True)
    monkeypatch.setattr(executor, "save_upload", slow_save)
    boundary = "boundary-123"
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x.tar.gz\"\r\n"
            f"Content-Type: application/gzip\r\n\r\narchive\r\n--{boundary}--\r\n").encode()
    sent, _reads = _asgi_request(executor, "/internal/web/v1/backups/upload", "POST",
                                 [(b"authorization", b"Bearer expected"),
                                  (b"content-type", f"multipart/form-data; boundary={boundary}".encode()),
                                  (b"content-length", str(len(body)).encode())], [(body, False)])
    assert sent[0]["status"] == 504
    assert b"outcome may be unknown" in sent[1]["body"]
    assert cancelled == [True]
    assert not stage.exists()


def test_private_file_response_reads_full_body_and_observes_real_disconnect(monkeypatch, tmp_path):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "_backup_root", lambda: tmp_path)
    monkeypatch.setattr(executor, "_require_executor_guard", lambda: asyncio.sleep(0))
    monkeypatch.setattr(executor, "_web_backend_ready", lambda _request: asyncio.sleep(0))
    contents = b"fixture-backup-download-" * 4096
    (tmp_path / "large-fixture.tar.gz").write_bytes(contents)
    sent, reads = _asgi_request(executor, "/internal/web/v1/backups/large-fixture.tar.gz",
                                headers=[(b"authorization", b"Bearer expected")])
    chunks = [event.get("body", b"") for event in sent if event["type"] == "http.response.body"]
    assert sent[0]["status"] == 200
    assert b"".join(chunks) == contents
    assert reads >= 2  # body replay then the real post-body disconnect check


def test_private_backup_schedule_static_route_precedes_filename_download(monkeypatch, tmp_path):
    from pydantic import SecretStr
    from news_aggregator import executor

    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "_backup_root", lambda: tmp_path)
    async def ready(_request):
        return None
    monkeypatch.setattr(executor, "_web_backend_ready", ready)

    # Exercise the complete ASGI app and boundary middleware: this must match
    # the static schedule route, not the earlier dynamic {filename} handler.
    sent, _ = _asgi_request(executor, "/internal/web/v1/backups/schedule", "GET",
                            [(b"authorization", b"Bearer expected")])
    assert sent[0]["status"] == 200
    assert json.loads(sent[1]["body"]) == {
        "enabled": False, "frequency": "daily", "time": "02:00", "keep_count": 5,
        "last_backup": None, "next_backup": None, "status": "disabled",
    }

    contents = b"real backup archive"
    (tmp_path / "fixture.tar.gz").write_bytes(contents)
    sent, _ = _asgi_request(executor, "/internal/web/v1/backups/fixture.tar.gz", "GET",
                            [(b"authorization", b"Bearer expected")])
    assert sent[0]["status"] == 200
    body = b"".join(event.get("body", b"") for event in sent if event["type"] == "http.response.body")
    assert body == contents

    # The same real routes remain protected by the separate service token.
    unauthenticated, reads = _asgi_request(executor, "/internal/web/v1/backups/schedule", "GET")
    assert unauthenticated[0]["status"] == 401
    assert reads == 0


def test_empty_json_object_body_is_replayed_as_complete_request(monkeypatch):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "_web_backend_ready", lambda _request: asyncio.sleep(0))
    async def fake_telegram_test(_db):
        return {"success": True}
    monkeypatch.setattr(executor, "telegram_test", fake_telegram_test)
    sent, reads = _asgi_request(executor, "/internal/web/v1/telegram/test", "POST",
                                [(b"authorization", b"Bearer expected"),
                                 (b"content-type", b"application/json"),
                                 (b"content-length", b"2")], [(b"{}", False)])
    assert sent[0]["status"] == 200
    assert b"\"success\":true" in sent[1]["body"]
    assert reads >= 1


def test_zero_length_json_body_returns_validation_error_without_hanging(monkeypatch):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "_web_backend_ready", lambda _request: asyncio.sleep(0))
    sent, _reads = _asgi_request(executor, "/internal/web/v1/telegram/test", "POST",
                                 [(b"authorization", b"Bearer expected"),
                                  (b"content-type", b"application/json"),
                                  (b"content-length", b"0")])
    assert sent[0]["status"] == 422


def test_partial_private_body_disconnect_never_reaches_handler(monkeypatch):
    from pydantic import SecretStr
    from news_aggregator import executor
    monkeypatch.setattr(executor.settings, "web_backend_token", SecretStr("expected"))
    monkeypatch.setattr(executor, "analyze_categories", lambda *_: pytest.fail("partial body dispatched"))
    async def interrupted_request():
        sent = []
        messages = iter(({"type": "http.request", "body": b'{"limit":', "more_body": True},
                         {"type": "http.disconnect"}))
        async def receive():
            return next(messages)
        async def send(message):
            sent.append(message)
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
                 "http_version": "1.1", "method": "POST",
                 "scheme": "http", "path": "/internal/web/v1/categories/ai-category-analysis",
                 "raw_path": b"/internal/web/v1/categories/ai-category-analysis",
                 "query_string": b"", "root_path": "",
                 "headers": [(b"authorization", b"Bearer expected"),
                             (b"content-type", b"application/json")],
                 "client": ("127.0.0.1", 1), "server": ("testserver", 80)}
        await executor.app(scope, receive, send)
        return sent
    sent = asyncio.run(interrupted_request())
    assert sent == []
