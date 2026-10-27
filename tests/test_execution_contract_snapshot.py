"""Frozen synthetic snapshots for the Python execution contract migration."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime
from uuid import UUID

from news_aggregator.services.background_jobs import validate_payload
from news_aggregator.services import domain_operations
from tests.frozen_execution_contract import bounded_result as _bounded_result
from tests.frozen_execution_contract import parse_envelope, receipt_response


FIXTURES = Path(__file__).parent / "fixtures" / "execution-contract-v1"
DOMAIN_FIXTURES = Path(__file__).parent / "fixtures" / "domain-contract-v1"


def _load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _load_domain(name):
    return json.loads((DOMAIN_FIXTURES / name).read_text(encoding="utf-8"))


def test_frozen_envelope_canonical_digest():
    fixture = _load("envelope.json")
    raw = json.dumps(fixture["raw"], separators=(",", ":")).encode()
    parsed, actual_digest = parse_envelope(raw)

    assert parsed["job_id"] == fixture["raw"]["job_id"]
    assert parsed["owner_id"] == fixture["raw"]["owner_id"]
    assert parsed["deadline"].isoformat() == "2026-10-27T10:15:30+00:00"
    assert actual_digest == hashlib.sha256(
        json.dumps({
            key: fixture["raw"][key]
            for key in ("version", "job_id", "owner_id", "generation", "attempt_generation", "deadline_at")
        }, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert actual_digest == fixture["canonical_sha256"]


def test_frozen_payload_normalization_for_all_current_job_kinds():
    fixture = _load("job-payloads.json")
    assert {item["kind"] for item in fixture} == {
        "news_processing", "news_digest", "telegram_digest", "backup",
        "reprocess_failed", "article_reprocess", "summaries_generate", "source_test",
    }
    for item in fixture:
        assert validate_payload(item["kind"], item["input"]) == item["normalized"]


def test_frozen_bounded_result_projection():
    items = _load("bounded-results.json")
    assert {item["kind"] for item in items} == {
        "news_processing", "news_digest", "telegram_digest", "backup",
        "reprocess_failed", "article_reprocess", "summaries_generate", "source_test",
    }
    for item in items:
        actual = _bounded_result(item["kind"], item["input"])
        assert actual == item["expected"]


def test_frozen_status_and_receipt_shapes(monkeypatch):
    status_fixture = _load("status.json")
    monkeypatch.setattr(domain_operations, "DOMAIN_INSTANCE_ID", status_fixture["executor_instance_id"])
    assert domain_operations.domain_status(ready=True) == {
        "domain_instance_id": status_fixture["executor_instance_id"],
        "active_operations": 0,
        "ready": True,
    }
    legacy_receipt = _load("receipt.json")
    receipt = SimpleNamespace(
        job_id=UUID(legacy_receipt["job_id"]),
        attempt_generation=legacy_receipt["attempt_generation"],
        owner_id=UUID(legacy_receipt["owner_id"]),
        worker_generation=legacy_receipt["generation"],
        deadline_at=datetime.fromisoformat(legacy_receipt["deadline_at"].replace("Z", "+00:00")),
        executor_instance_id=UUID(legacy_receipt["executor_instance_id"]),
        state=legacy_receipt["state"],
        accepted_at=datetime.fromisoformat(legacy_receipt["accepted_at"]),
        started_at=datetime.fromisoformat(legacy_receipt["started_at"]),
        finished_at=datetime.fromisoformat(legacy_receipt["finished_at"]),
        result=legacy_receipt["result"],
        error_code=legacy_receipt["error_code"],
    )
    assert receipt_response(receipt) == legacy_receipt

    status_fixture = _load_domain("domain-status.json")
    monkeypatch.setattr(domain_operations, "DOMAIN_INSTANCE_ID", status_fixture["domain_instance_id"])
    assert domain_operations.domain_status(ready=True) == status_fixture
    receipt_fixture = _load_domain("domain-receipt.json")
    record = domain_operations.OperationRecord(
        job_id=receipt_fixture["job_id"], attempt_generation=receipt_fixture["attempt_generation"],
        operation=receipt_fixture["operation"], operation_id=receipt_fixture["operation_id"],
        domain_instance_id=receipt_fixture["domain_instance_id"],
        expected_envelope={},
        input_fingerprint=receipt_fixture["input_fingerprint"], input={},
        deadline=domain_operations.datetime.now(domain_operations.timezone.utc),
        expires_at=domain_operations.datetime.now(domain_operations.timezone.utc),
        state=receipt_fixture["state"], result=receipt_fixture["result"],
    )
    assert domain_operations._response(record) == receipt_fixture
