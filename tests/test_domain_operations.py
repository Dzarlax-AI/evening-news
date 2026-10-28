"""Bounded wire, replay, and typed-operation checks for the Python leaf service."""

import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from news_aggregator.services import domain_operations as domain
from news_aggregator.services.domain_fence import parse_envelope


def _envelope(kind_payload=None):
    return {
        "version": 1,
        "job_id": str(uuid.uuid4()),
        "attempt_generation": 2,
        "owner_id": str(uuid.uuid4()),
        "generation": 3,
        "backend_pid": 4567,
        "executor_instance_id": str(uuid.uuid4()),
        "deadline_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
    }


def _request(operation, operation_input=None, *, envelope=None, operation_id=None):
    operation_input = operation_input or {}
    envelope = envelope or _envelope()
    return {
        "version": 1,
        "operation": operation,
        "operation_id": operation_id or str(uuid.uuid4()),
        "domain_instance_id": str(domain.DOMAIN_INSTANCE_ID),
        "envelope": envelope,
        "input_fingerprint": domain.canonical_input_fingerprint(operation_input),
        "input": operation_input,
    }


@pytest.fixture(autouse=True)
def clear_local_receipts(monkeypatch):
    domain._RECORDS.clear()
    domain._ACTIVE.clear()
    domain._RECORD_BYTES = 0
    domain._RESULT_BYTES = 0
    yield
    for task in tuple(domain._ACTIVE.values()):
        task.cancel()
    domain._ACTIVE.clear()
    domain._RECORDS.clear()


def test_operation_wire_is_strict_and_fingerprint_is_compact_sorted_json():
    body = _request("summary_status", {"b": 2, "a": "café"})
    compact = json.dumps(body["input"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert body["input_fingerprint"] == hashlib.sha256(compact.encode()).hexdigest()
    record = domain.validate_operation_body(body)
    assert domain._response(record) == {
        "version": 1, "job_id": body["envelope"]["job_id"], "attempt_generation": 2,
        "operation": "summary_status", "operation_id": body["operation_id"],
        "domain_instance_id": str(domain.DOMAIN_INSTANCE_ID),
        "input_fingerprint": body["input_fingerprint"], "state": "accepted",
        "result": None, "error_code": None,
    }

    with pytest.raises(domain.DomainOperationInputError):
        domain.validate_operation_body({**body, "extra": True})
    with pytest.raises(domain.DomainOperationInputError):
        domain.validate_operation_body({**body, "input_fingerprint": "0" * 64})
    with pytest.raises(ValueError, match="canonical UUID"):
        domain.validate_operation_body({**body, "operation_id": body["operation_id"].upper()})


def test_operation_rejects_expired_or_unbounded_envelope_deadlines():
    body = _request("summary_status")
    expired = {**body, "envelope": {**body["envelope"],
                                    "deadline_at": "2020-01-01T00:00:00Z"}}
    with pytest.raises(domain.DomainOperationInputError, match="deadline"):
        domain.validate_operation_body(expired)
    too_far = {**body, "envelope": {**body["envelope"],
                                   "deadline_at": (datetime.now(timezone.utc) + timedelta(days=2))
                                   .isoformat().replace("+00:00", "Z")}}
    with pytest.raises(domain.DomainOperationInputError, match="deadline"):
        domain.validate_operation_body(too_far)


def test_operation_allowlist_binds_inputs_to_persisted_job_payload():
    domain._assert_operation_allowed("summary_status", {}, "news_digest", {"send_telegram": True})
    domain._assert_operation_allowed("summaries_generate", {}, "summaries_generate", {})
    domain._assert_operation_allowed("digest_prepare", {}, "news_digest", {"send_telegram": True})
    domain._assert_operation_allowed("digest_publish", {"prepared_token": str(uuid.uuid4())},
                                    "news_digest", {"send_telegram": True})
    domain._assert_operation_allowed("media_refresh", {"article_id": 7}, "article_reprocess",
                                    {"article_ids": [7], "reextract_media": True})
    domain._assert_operation_allowed("source_test", {"source_id": 11}, "source_test", {"source_id": 11})
    domain._assert_operation_allowed("backup", {}, "backup", {})
    domain._assert_operation_allowed("stats_publish", {"stats": {"errors_count": 2}},
                                    "news_processing", {})

    denied = [
        ("digest_publish", {"prepared_token": "bad"}, "news_digest", {"send_telegram": True}),
        ("media_refresh", {"article_id": 8}, "article_reprocess",
         {"article_ids": [7], "reextract_media": True}),
        ("source_test", {"source_id": 12}, "source_test", {"source_id": 11}),
        ("backup", {"path": "/tmp/arbitrary"}, "backup", {}),
        ("stats_publish", {"stats": {"errors": ["raw"]}}, "news_processing", {}),
    ]
    for operation, value, kind, payload in denied:
        with pytest.raises(domain.DomainOperationInputError):
            domain._assert_operation_allowed(operation, value, kind, payload)


def test_payload_parser_is_exact_eight_field_fence_and_rejects_bool_integer():
    envelope = _envelope()
    parsed = parse_envelope(envelope)
    assert set(parsed) == set(envelope) | {"job_uuid", "owner_uuid", "executor_uuid", "deadline", "deadline_db"}
    with pytest.raises(ValueError):
        parse_envelope({**envelope, "backend_pid": True})
    with pytest.raises(ValueError):
        parse_envelope({key: value for key, value in envelope.items() if key != "executor_instance_id"})


def test_context_matching_accepts_only_equal_postgres_microsecond_deadlines():
    from news_aggregator.services.domain_fence import ExecutionContext, context_matches_envelope

    trusted = _envelope()
    trusted["deadline_at"] = "2026-10-27T14:45:54.726260Z"
    context = ExecutionContext(parse_envelope(trusted), 7890, "news_processing", {})

    # Go's time.Time JSON marshaler trims the last zero while the execution
    # service emits PostgreSQL timestamps with six fractional digits.
    equivalent = {**trusted, "deadline_at": "2026-10-27T14:45:54.72626Z"}
    assert context_matches_envelope(context, equivalent)
    assert context_matches_envelope(
        context, {**trusted, "deadline_at": "2026-10-27T16:45:54.72626+02:00"})

    whole_seconds = {**trusted, "deadline_at": "2026-10-27T14:45:54.000000Z"}
    whole_seconds_context = ExecutionContext(parse_envelope(whole_seconds), 7890, "news_processing", {})
    assert context_matches_envelope(
        whole_seconds_context, {**whole_seconds, "deadline_at": "2026-10-27T14:45:54Z"})

    changed = {**trusted, "deadline_at": "2026-10-27T14:45:54.726261Z"}
    assert not context_matches_envelope(context, changed)
    assert not context_matches_envelope(context, {**trusted, "deadline_at": "2026-10-27T14:45:54.7262601Z"})
    assert not context_matches_envelope(context, {**trusted, "deadline_at": "not-a-deadline"})
    assert not context_matches_envelope(context, {**equivalent, "unexpected": True})
    assert not context_matches_envelope(context, {**equivalent, "generation": trusted["generation"] + 1})
    assert not context_matches_envelope(context, {**equivalent, "version": True})
    assert not context_matches_envelope(
        context, {**equivalent, "attempt_generation": float(trusted["attempt_generation"])})
    assert not context_matches_envelope(
        context, {**trusted, "deadline_at": "0001-01-01T00:00:00+23:59"})
    assert not context_matches_envelope(
        context, {**trusted, "deadline_at": "2026-10-27T16:15:54.726260+00:90"})


@pytest.mark.asyncio
async def test_post_receipt_is_registered_before_background_leaf_and_conflicts_on_changed_input(monkeypatch):
    started = asyncio.Event()

    async def blocked(_key, _record):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(domain, "_run_operation", blocked)
    body = _request("summary_status", {})
    record = domain.validate_operation_body(body)
    accepted, created = await domain.accept_domain_operation(record)
    assert created is True
    assert accepted["state"] == "accepted"
    assert await started.wait() is True

    reread = await domain.read_operation_status(
        record.job_id, record.attempt_generation, record.operation, record.operation_id,
        record.domain_instance_id,
    )
    assert reread == accepted
    duplicate, created = await domain.accept_domain_operation(domain.validate_operation_body(body))
    assert created is False and duplicate == accepted

    changed = _request("summary_status", {"changed": True}, envelope=body["envelope"],
                       operation_id=body["operation_id"])
    with pytest.raises(domain.DomainOperationConflict):
        await domain.accept_domain_operation(domain.validate_operation_body(changed))

    changed_identity = {**body, "envelope": {**body["envelope"], "generation": 4}}
    with pytest.raises(domain.DomainOperationConflict):
        await domain.accept_domain_operation(domain.validate_operation_body(changed_identity))


@pytest.mark.asyncio
async def test_domain_operation_http_pins_instance_and_returns_accepted_receipt(monkeypatch):
    from news_aggregator import executor

    monkeypatch.setattr(executor.settings, "job_executor_token",
                        SimpleNamespace(get_secret_value=lambda: "fixture-job-token"))

    async def no_guard():
        return None

    accepted = {"version": 1, "job_id": str(uuid.uuid4()), "attempt_generation": 2,
                "operation": "summary_status", "operation_id": str(uuid.uuid4()),
                "domain_instance_id": str(domain.DOMAIN_INSTANCE_ID),
                "input_fingerprint": "a" * 64, "state": "accepted", "result": None,
                "error_code": None}
    register = AsyncMock(return_value=(accepted, True))
    read_status = AsyncMock(return_value=accepted)
    monkeypatch.setattr(executor, "_require_executor_guard", no_guard)
    monkeypatch.setattr(domain, "accept_domain_operation", register)
    monkeypatch.setattr(domain, "read_operation_status", read_status)

    body = _request("summary_status")
    async def call(method, path, request_body=None, headers=None):
        raw_body = json.dumps(request_body).encode() if request_body is not None else b""
        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": method, "scheme": "http", "path": path, "raw_path": path.encode(),
            "query_string": b"", "root_path": "", "server": ("fixture", 80),
            "client": ("127.0.0.1", 12345),
            "headers": [(key.lower().encode(), value.encode()) for key, value in (headers or {}).items()],
        }
        delivered = False
        sent = []

        async def receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": raw_body, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message):
            sent.append(message)

        await executor.app(scope, receive, send)
        status = next(message["status"] for message in sent if message["type"] == "http.response.start")
        raw_response = b"".join(message.get("body", b"") for message in sent
                                 if message["type"] == "http.response.body")
        return status, json.loads(raw_response) if raw_response else None

    auth = {"Authorization": "Bearer fixture-job-token",
            "X-Domain-Instance-ID": str(domain.DOMAIN_INSTANCE_ID)}
    status, response = await call("POST", "/internal/v1/domain-operations", body, auth)
    assert status == 202
    assert response == accepted

    job_id = body["envelope"]["job_id"]
    status_path = (f"/internal/v1/domain-operations/{job_id}/2/summary_status/"
                   f"{body['operation_id']}")
    status, reconciled = await call("GET", status_path, headers=auth)
    assert status == 200
    assert reconciled == accepted

    wrong_instance = {**auth, "X-Domain-Instance-ID": str(uuid.uuid4())}
    status, _ = await call("POST", "/internal/v1/domain-operations", body, wrong_instance)
    assert status == 409
    status, _ = await call("GET", status_path, headers=wrong_instance)
    assert status == 409
    status, _ = await call("POST", "/internal/v1/domain-operations", body)
    assert status == 401
    register.assert_awaited_once()
    read_status.assert_awaited_once()


@pytest.mark.asyncio
async def test_domain_receipt_expiration_prunes_old_history_without_a_permanent_capacity_wall(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(domain, "MAX_DOMAIN_OPERATION_ENTRIES", 4)
    for i in range(12):
        record = domain.OperationRecord(
            str(uuid.uuid4()), 1, "summary_status", str(uuid.uuid4()), str(domain.DOMAIN_INSTANCE_ID),
            {}, "a" * 64, {}, now - timedelta(hours=2), now - timedelta(hours=1),
        )
        domain._RECORDS[domain._operation_key(record)] = record

    async def blocked(_key, _record):
        await asyncio.Future()

    monkeypatch.setattr(domain, "_run_operation", blocked)
    fresh = domain.validate_operation_body(_request("summary_status"))
    response, created = await domain.accept_domain_operation(fresh)
    assert created and response["state"] == "accepted"
    assert len(domain._RECORDS) == 1


@pytest.mark.asyncio
async def test_domain_receipt_cache_enforces_total_request_and_result_bytes(monkeypatch):
    monkeypatch.setattr(domain, "MAX_DOMAIN_OPERATION_CACHE_BYTES", 1)
    record = domain.validate_operation_body(_request("summary_status"))
    with pytest.raises(RuntimeError, match="byte capacity"):
        await domain.accept_domain_operation(record)
    assert not domain._RECORDS


@pytest.mark.asyncio
async def test_go_authority_must_match_the_submitted_envelope_before_dispatch(monkeypatch):
    from news_aggregator.services import domain_fence

    submitted = _envelope()
    trusted = domain_fence.ExecutionContext(parse_envelope(submitted), 7890, "news_processing", {})
    record = domain.validate_operation_body(_request("summary_status", {}, envelope=submitted))
    mutated = {**submitted, "generation": submitted["generation"] + 1}
    record.expected_envelope = mutated
    client = SimpleNamespace(get_context=AsyncMock(return_value=trusted))
    monkeypatch.setattr(domain, "_AUTHORITY_CLIENT", client)
    monkeypatch.setattr(domain, "_read_persisted_job_identity", AsyncMock(return_value=("news_processing", {})))
    with pytest.raises(domain_fence.DomainAuthorityError, match="submitted envelope"):
        await domain._get_current_context(record)


def test_executor_routes_expose_leaf_api_without_python_job_lifecycle_routes():
    from news_aggregator.executor import app

    routes = {(method, route.path) for route in app.routes
              for method in getattr(route, "methods", set())}
    assert ("POST", "/internal/v1/domain-operations") in routes
    assert ("GET", "/internal/v1/domain-operations/{job_id}/{attempt_generation}/{operation}/{operation_id}") in routes
    assert ("POST", "/internal/v1/domain-events/finalized") in routes
    assert ("POST", "/internal/v1/article-ai") in routes
    assert ("POST", "/internal/v1/ingestion/selectors") in routes
    assert not any(path.startswith("/internal/v1/job-executions") or path == "/internal/v1/worker-events"
                   for _, path in routes)


@pytest.mark.asyncio
async def test_digest_dispatch_uses_native_adapter(monkeypatch):
    from unittest.mock import AsyncMock
    native = AsyncMock(return_value={"summaries_count": 2})
    monkeypatch.setattr("news_aggregator.services.ai_runtime.get_ai_runtime", lambda: SimpleNamespace(digest_operation=native))
    record = SimpleNamespace(operation="summary_status", input={})
    context = object()
    assert await domain._dispatch_operation(record, context, object()) == {"summaries_count": 2}
    native.assert_awaited_once_with(record, context)
