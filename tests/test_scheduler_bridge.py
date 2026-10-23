"""Offline scheduler bridge transformations never include runtime secrets."""

from datetime import datetime
from types import SimpleNamespace

import pytest

from scripts.scheduler_bridge import (
    is_news_owned_schedule, legacy_item, legacy_rollback_record, legacy_run_history, safe_payload,
    utc_timestamp, write_artifact,
)


def test_legacy_schedule_import_preserves_due_time_and_identity():
    row = SimpleNamespace(
        id=42, task_name="news_digest", enabled=True, schedule_type="interval",
        timezone="Europe/Belgrade", hour=9, minute=0, weekdays=[],
        task_config={"interval_minutes": 45, "run_processing": False,
                     "send_telegram": True, "timeout_seconds": 120},
        next_run=datetime(2026, 10, 23, 17, 30, 19, 728869),
    )
    item = legacy_item(row)
    assert item["schedule_key"] == "news:42"
    assert item["calendar"]["interval_minutes"] == 45
    assert item["payload"] == {
        "run_processing": False, "send_telegram": True, "timeout_seconds": 120,
    }
    assert item["next_due_at"] == "2026-10-23T17:30:19.728869Z"


def test_bridge_refuses_unrecognized_configuration_fields():
    with pytest.raises(ValueError, match="sensitive task_config field"):
        safe_payload("backup", {"keep_days": 30, "api_token": "must-not-export"})


def test_timestamp_serialization_keeps_microseconds_and_treats_legacy_naive_as_utc():
    assert utc_timestamp(datetime(2026, 10, 23, 17, 30, 19, 7)) == "2026-10-23T17:30:19.000007Z"


def test_rollback_snapshot_maps_current_service_config_and_retains_successful_run():
    schedule = {
        "schedule_key": "news:42", "revision": 7, "enabled": True,
        "calendar": {"type": "interval", "timezone": "Europe/Belgrade", "interval_minutes": 45},
        "target_alias": "news-worker", "operation": "news_digest",
        "payload": {"run_processing": False, "send_telegram": True},
        "next_due_at": "2026-10-23T17:30:19.728869Z",
        "created_at": "2026-10-22T10:00:00.000001Z",
        "updated_at": "2026-10-23T10:00:00.000002Z",
    }
    job = {"state": "succeeded", "result_status": "success",
           "created_at": "2026-10-23T16:00:00.000001Z",
           "finished_at": "2026-10-23T16:02:00.000001Z"}
    record = legacy_rollback_record(schedule, job)
    assert record["task_name"] == "news_digest"
    assert record["task_config"] == {
        "run_processing": False, "send_telegram": True, "interval_minutes": 45,
    }
    assert record["next_run"] == schedule["next_due_at"]
    assert record["last_status"] == "success"
    assert record["last_success_at"] == job["finished_at"]


def test_rollback_inventory_ownership_requires_news_key_and_target_alias():
    news = {
        "schedule_key": "news:backup", "target_alias": "news-worker",
        "operation": "backup",
    }
    assert is_news_owned_schedule(news)
    assert not is_news_owned_schedule({**news, "target_alias": "archive-worker"})
    assert not is_news_owned_schedule({**news, "schedule_key": "archive:backup"})


def test_export_fails_closed_for_backup_config_not_supported_by_worker_jobs():
    with pytest.raises(ValueError, match="sensitive task_config field"):
        safe_payload("backup", {"keep_days": 14})


def test_export_preserves_legacy_success_history_without_unbounded_error_text():
    row = SimpleNamespace(
        id=9, task_name="news_digest", last_run=datetime(2026, 10, 23, 16),
        last_finished_at=datetime(2026, 10, 23, 16, 2),
        last_success_at=datetime(2026, 10, 23, 16, 2), last_status="success",
        last_duration_seconds=120.5,
    )
    history = legacy_run_history(row)
    assert history["schedule_key"] == "news:9"
    assert history["last_status"] == "success"
    assert history["last_success_at"] == "2026-10-23T16:02:00.000000Z"
    assert "last_error" not in history


def test_artifact_writer_uses_private_permissions_and_never_overwrites(tmp_path):
    path = tmp_path / "snapshot.json"
    write_artifact(path, {"ok": True})
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        write_artifact(path, {"ok": False})
    assert path.read_text(encoding="utf-8") == '{\n  "ok": true\n}\n'
