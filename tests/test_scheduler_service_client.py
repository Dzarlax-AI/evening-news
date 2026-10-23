"""Bounded and strict Go scheduler API client behavior."""

import json

import pytest

from news_aggregator.services.scheduler_service_client import (
    MAX_RESPONSE_BYTES,
    SCHEDULE_PAGE_SIZE,
    SchedulerServiceClient,
    SchedulerServiceError,
)


def _schedule(key, payload=None, *, alias="other-worker", operation="backup"):
    return {
        "schedule_key": key,
        "revision": 1,
        "enabled": True,
        "calendar": {"type": "daily", "timezone": "UTC", "hour": 1, "minute": 2},
        "target_alias": alias,
        "operation": operation,
        "payload": payload or {},
        "next_due_at": None,
        "created_at": "2026-10-23T00:00:00Z",
        "updated_at": "2026-10-23T00:00:00Z",
    }


def test_schedule_listing_uses_small_pages_for_worst_case_html_escaped_payloads(monkeypatch):
    # A 4KiB payload of HTML characters expands to roughly 6x under Go's
    # encoding/json HTML escaping. Two such resources plus metadata fit the
    # client's bounded 64KiB response cap; larger pages would not.
    html = "<" * 4075
    payload = {"html": html}
    assert len(json.dumps(payload, separators=(",", ":")).encode()) <= 4096
    resources = [
        _schedule(f"archive:backup-{index}", payload)
        for index in range(4)
    ] + [_schedule("news:news_digest", {"send_telegram": True},
                   alias="news-worker", operation="news_digest")]
    pages = [resources[i:i + SCHEDULE_PAGE_SIZE] for i in range(0, len(resources), SCHEDULE_PAGE_SIZE)]
    cursors = ["cursor-1", "cursor-2", None]
    calls = []

    client = SchedulerServiceClient("https://scheduler.example", "fixture-token")

    async def request(method, path):
        assert method == "GET"
        calls.append(path)
        query = path.split("?", 1)[1]
        assert query.startswith(f"limit={SCHEDULE_PAGE_SIZE}")
        page_index = len(calls) - 1
        page = {"schedules": pages[page_index], "next_cursor": cursors[page_index]}
        encoded = json.dumps(page, ensure_ascii=False, separators=(",", ":"))
        encoded = (encoded.replace("&", "\\u0026").replace("<", "\\u003c")
                   .replace(">", "\\u003e"))
        assert len(encoded.encode()) <= MAX_RESPONSE_BYTES
        return page

    monkeypatch.setattr(client, "request", request)
    result = __import__("asyncio").run(client.list_schedules())
    assert len(calls) == 3
    assert len(result) == 5
    assert result[-1]["schedule_key"] == "news:news_digest"


def test_schedule_listing_rejects_page_larger_than_requested_limit(monkeypatch):
    client = SchedulerServiceClient("https://scheduler.example", "fixture-token")

    async def request(_method, _path):
        return {"schedules": [_schedule(f"other:key-{i}") for i in range(3)], "next_cursor": None}

    monkeypatch.setattr(client, "request", request)
    with pytest.raises(SchedulerServiceError, match="invalid_response"):
        __import__("asyncio").run(client.list_schedules())


@pytest.mark.parametrize("change", [
    {"target_alias": None},
    {"lease_active": 1},
    {"lease_expires_at": "2026-10-23T00:00:00+00:00"},
    {"lease_active": True, "lease_expires_at": None},
])
def test_occurrence_listing_requires_target_and_lease_metadata(monkeypatch, change):
    from uuid import uuid4

    client = SchedulerServiceClient("https://scheduler.example", "fixture-token")
    row = {
        "occurrence_id": str(uuid4()), "schedule_key": "news:42",
        "schedule_revision": 3, "scheduled_for": "2026-10-23T00:00:00Z",
        "status": "pending", "attempts": 1,
        "created_at": "2026-10-23T00:00:00Z", "updated_at": "2026-10-23T00:00:01Z",
        "job_id": None, "target_alias": "news-worker",
        "lease_expires_at": None, "lease_active": False,
    }
    row.update(change)

    async def request(_method, _path):
        return {"occurrences": [row], "next_cursor": None}

    monkeypatch.setattr(client, "request", request)
    with pytest.raises(SchedulerServiceError, match="invalid_response"):
        __import__("asyncio").run(client.list_occurrences())


def test_occurrence_listing_accepts_expired_leased_record_with_new_metadata(monkeypatch):
    from uuid import uuid4

    client = SchedulerServiceClient("https://scheduler.example", "fixture-token")
    row = {
        "occurrence_id": str(uuid4()), "schedule_key": "news:42",
        "schedule_revision": 3, "scheduled_for": "2026-10-23T00:00:00Z",
        "status": "leased", "attempts": 2,
        "created_at": "2026-10-23T00:00:00Z", "updated_at": "2026-10-23T00:00:01Z",
        "job_id": None, "target_alias": "news-worker",
        "lease_expires_at": "2026-10-23T00:00:00.000001Z", "lease_active": False,
    }

    async def request(_method, _path):
        return {"occurrences": [row], "next_cursor": None}

    monkeypatch.setattr(client, "request", request)
    result = __import__("asyncio").run(client.list_occurrences())
    assert result["occurrences"] == [row]
