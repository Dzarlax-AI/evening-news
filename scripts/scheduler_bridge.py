#!/usr/bin/env python3
"""Offline scheduler cutover and rollback inventory helper.

The bridge reads only a loopback News PostgreSQL snapshot and communicates with
an explicitly supplied scheduler API. It never writes credentials to artifacts.
"""

import argparse
import asyncio
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

# Support direct invocation as `python scripts/scheduler_bridge.py`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from news_aggregator.models import BackgroundJob, ScheduleSettings

OPERATIONS = {"news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed"}
ACTIVE_STATES = {"queued", "running", "retrying", "interrupted", "delivery_unknown"}
TASK_FIELDS = {
    "news_processing": set(),
    "news_digest": {"run_processing", "send_telegram"},
    "telegram_digest": set(),
    # The durable News job schema does not accept keep_days; exporting it
    # would create a command the receiver cannot enqueue safely.
    "backup": set(),
    "reprocess_failed": {"limit", "dry_run"},
}


def utc_timestamp(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def write_artifact(path, artifact):
    """Create a private snapshot without overwriting an earlier artifact."""
    destination = Path(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(destination, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    except BaseException:
        try:
            destination.unlink()
        except OSError:
            pass
        raise


def snapshot_database_url():
    value = os.environ.get("NEWS_SNAPSHOT_DATABASE_URL") or os.environ.get("TEST_DATABASE_URL")
    if not value:
        raise ValueError("set NEWS_SNAPSHOT_DATABASE_URL to the offline News snapshot database")
    parsed = make_url(value)
    if parsed.host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("snapshot database must be loopback-only")
    return value


def safe_payload(task_name, raw):
    config = raw if isinstance(raw, dict) else {}
    allowed = TASK_FIELDS[task_name]
    if set(config) - allowed - {"interval_minutes", "timeout_seconds"}:
        raise ValueError(f"unsupported or sensitive task_config field for {task_name}")
    result = {key: config[key] for key in allowed | {"timeout_seconds"} if key in config}
    return result


def legacy_item(row):
    if row.task_name not in OPERATIONS:
        raise ValueError(f"unsupported legacy schedule task: {row.task_name}")
    schedule_type = row.schedule_type
    calendar = {"type": schedule_type, "timezone": row.timezone}
    if schedule_type in {"daily", "weekly"}:
        calendar.update(hour=row.hour, minute=row.minute)
        days = row.weekdays or []
        if schedule_type == "weekly":
            calendar["weekdays"] = days
        elif days:
            calendar["weekdays"] = days
    elif schedule_type == "hourly":
        calendar["minute"] = row.minute
        if row.weekdays:
            calendar["weekdays"] = row.weekdays
    elif schedule_type == "interval":
        config = row.task_config if isinstance(row.task_config, dict) else {}
        calendar["interval_minutes"] = config.get("interval_minutes", 30)
    else:
        raise ValueError(f"unsupported legacy calendar type: {schedule_type}")
    return {
        "schedule_key": f"news:{row.id}",
        "enabled": bool(row.enabled),
        "calendar": calendar,
        "target_alias": "news-worker",
        "operation": row.task_name,
        "payload": safe_payload(row.task_name, row.task_config),
        "next_due_at": utc_timestamp(row.next_run),
    }


def legacy_run_history(row):
    statuses = {"success", "warning", "deferred", "failed", "interrupted", "delivery_unknown"}
    status = row.last_status if row.last_status in statuses else None
    duration = float(row.last_duration_seconds) if row.last_duration_seconds is not None else None
    return {
        "schedule_key": f"news:{row.id}",
        "task_name": row.task_name,
        "last_run": utc_timestamp(row.last_run),
        "last_finished_at": utc_timestamp(row.last_finished_at),
        "last_success_at": utc_timestamp(row.last_success_at),
        "last_status": status,
        "last_duration_seconds": duration,
    }


async def export_snapshot(path):
    engine = create_async_engine(snapshot_database_url())
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            schedules = (await session.execute(
                select(ScheduleSettings).order_by(ScheduleSettings.id)
            )).scalars().all()
            active = (await session.execute(
                select(BackgroundJob).where(
                    BackgroundJob.kind.in_(OPERATIONS), BackgroundJob.state.in_(ACTIVE_STATES)
                ).order_by(BackgroundJob.created_at)
            )).scalars().all()
            if active:
                raise ValueError("drain scheduled News jobs before export; active work remains")
            items = [legacy_item(row) for row in schedules]
            legacy_history = [legacy_run_history(row) for row in schedules]
            if len(items) > 100:
                raise ValueError("the scheduler import contract permits at most 100 items per atomic import")
            history = (await session.execute(
                select(BackgroundJob).where(BackgroundJob.kind.in_(OPERATIONS))
                .order_by(BackgroundJob.created_at, BackgroundJob.id)
            )).scalars().all()
            inventory = [{
                "job_id": str(job.id), "kind": job.kind, "state": job.state,
                "schedule_key": job.schedule_key,
                "occurrence_id": str(job.occurrence_id) if job.occurrence_id else None,
                "scheduled_for": utc_timestamp(job.scheduled_for),
                "created_at": utc_timestamp(job.created_at),
                "finished_at": utc_timestamp(job.finished_at),
            } for job in history]
            artifact = {
                "format": "news-scheduler-snapshot-v1",
                "exported_at": utc_timestamp(datetime.now(timezone.utc)),
                "schedules": items,
                "legacy_run_history": legacy_history,
                "news_job_inventory": inventory,
            }
        write_artifact(path, artifact)
    finally:
        await engine.dispose()


def service_config(args):
    base = args.api_url.rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme != "https" and not (args.allow_http and parsed.scheme == "http"):
        raise ValueError("scheduler API must use HTTPS (or explicit --allow-http on an isolated private network)")
    if not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("invalid scheduler API URL")
    token = os.environ.get("SCHEDULER_API_TOKEN")
    if not token:
        raise ValueError("SCHEDULER_API_TOKEN is required at runtime")
    return base, token


def validate_import_item(item):
    required = {"schedule_key", "enabled", "calendar", "target_alias", "operation", "payload", "next_due_at"}
    if not isinstance(item, dict) or set(item) != required:
        raise ValueError("snapshot schedule has an invalid field set")
    if (not isinstance(item["operation"], str)
            or item["operation"] not in OPERATIONS
            or not isinstance(item["schedule_key"], str)
            or not (re.fullmatch(r"news:[1-9][0-9]*", item["schedule_key"])
                    or item["schedule_key"] == "news:" + item["operation"])
            or type(item["enabled"]) is not bool
            or item["target_alias"] != "news-worker"
            or not isinstance(item["calendar"], dict)
            or not isinstance(item["payload"], dict)):
        raise ValueError("snapshot schedule identity or values are invalid")
    calendar = item["calendar"]
    schedule_type = calendar.get("type")
    allowed_calendar = {
        "daily": {"type", "timezone", "hour", "minute", "weekdays"},
        "weekly": {"type", "timezone", "hour", "minute", "weekdays"},
        "hourly": {"type", "timezone", "minute", "weekdays"},
        "interval": {"type", "timezone", "interval_minutes"},
    }
    if schedule_type not in allowed_calendar or set(calendar) - allowed_calendar[schedule_type]:
        raise ValueError("snapshot schedule calendar is invalid")
    timezone_name = calendar.get("timezone")
    if not isinstance(timezone_name, str):
        raise ValueError("snapshot schedule timezone is invalid")
    try:
        ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("snapshot schedule timezone is invalid") from None
    payload = safe_payload(item["operation"], item["payload"])
    if payload != item["payload"]:
        raise ValueError("snapshot schedule payload is invalid")
    from news_aggregator.services.background_jobs import validate_payload
    try:
        if validate_payload(item["operation"], payload) != payload:
            raise ValueError("snapshot schedule payload is invalid")
    except (TypeError, ValueError):
        raise ValueError("snapshot schedule payload is invalid") from None
    due = item["next_due_at"]
    if due is not None and (not isinstance(due, str)
                            or not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", due)):
        raise ValueError("snapshot due time is invalid")
    if due is not None:
        try:
            datetime.fromisoformat(due[:-1] + "+00:00")
        except ValueError:
            raise ValueError("snapshot due time is invalid") from None
    return item


def legacy_rollback_record(resource, latest_job=None):
    operation = resource.get("operation")
    if operation not in OPERATIONS or resource.get("target_alias") != "news-worker":
        raise ValueError("scheduler resource cannot be represented by the News worker")
    payload = safe_payload(operation, resource.get("payload"))
    calendar = resource.get("calendar")
    if not isinstance(calendar, dict):
        raise ValueError("scheduler calendar is invalid")
    next_due = resource.get("next_due_at")
    item = {
        "schedule_key": resource.get("schedule_key"),
        "enabled": resource.get("enabled"),
        "calendar": calendar,
        "target_alias": "news-worker",
        "operation": operation,
        "payload": payload,
        "next_due_at": next_due,
    }
    validate_import_item(item)
    key = resource["schedule_key"]
    suffix = key[5:] if isinstance(key, str) and key.startswith("news:") else None
    task_config = dict(payload)
    if calendar["type"] == "interval":
        task_config["interval_minutes"] = calendar["interval_minutes"]
    run_status = latest_job.get("result_status") if latest_job else None
    if latest_job and latest_job["state"] != "succeeded":
        run_status = latest_job["state"]
    elif run_status in {"success", "completed"}:
        run_status = "success"
    return {
        "id": int(suffix) if suffix and suffix.isdigit() else key,
        "task_name": operation,
        "enabled": item["enabled"],
        "schedule_type": calendar["type"],
        "hour": calendar.get("hour", 0),
        "minute": calendar.get("minute", 0),
        "weekdays": calendar.get("weekdays", []),
        "timezone": calendar["timezone"],
        "task_config": task_config,
        "last_run": latest_job.get("created_at") if latest_job else None,
        "last_finished_at": latest_job.get("finished_at") if latest_job else None,
        "last_success_at": latest_job.get("finished_at") if latest_job and run_status == "success" else None,
        "last_status": run_status,
        "last_error": None,
        "last_duration_seconds": None,
        "next_run": next_due,
        "is_running": bool(latest_job and latest_job["state"] == "running"),
        "created_at": resource.get("created_at"),
        "updated_at": resource.get("updated_at"),
        "schedule_key": key,
        "schedule_revision": resource.get("revision"),
    }


def is_news_owned_schedule(resource):
    if not isinstance(resource, dict) or resource.get("target_alias") != "news-worker":
        return False
    operation = resource.get("operation")
    key = resource.get("schedule_key")
    return isinstance(operation, str) and operation in OPERATIONS and is_news_schedule_key(key)


def is_news_schedule_key(key):
    if not isinstance(key, str) or not key.startswith("news:"):
        return False
    suffix = key[len("news:"):]
    return (suffix in OPERATIONS
            or (suffix.isascii() and suffix.isdigit() and int(suffix) > 0))


def is_news_owned_occurrence(occurrence):
    return (isinstance(occurrence, dict)
            and occurrence.get("target_alias") == "news-worker")


def _is_utc_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", value):
        return False
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return True


def validate_occurrence_record(item):
    required = {
        "occurrence_id", "schedule_key", "schedule_revision", "scheduled_for", "status",
        "attempts", "created_at", "updated_at", "job_id", "target_alias",
        "lease_expires_at", "lease_active",
    }
    if not isinstance(item, dict) or set(item) != required:
        raise ValueError("scheduler occurrence inventory row has an invalid field set")
    try:
        occurrence_id = str(uuid.UUID(item["occurrence_id"]))
        job_id = None if item["job_id"] is None else str(uuid.UUID(item["job_id"]))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("scheduler occurrence inventory identifiers are invalid") from None
    if (occurrence_id != item["occurrence_id"]
            or (job_id is not None and job_id != item["job_id"])
            or not isinstance(item["schedule_key"], str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._:-]{0,127}", item["schedule_key"])
            or type(item["schedule_revision"]) is not int or item["schedule_revision"] < 1
            or type(item["attempts"]) is not int or item["attempts"] < 0
            or not isinstance(item["status"], str)
            or item["status"] not in {"pending", "leased", "blocked", "accepted"}
            or not isinstance(item["target_alias"], str)
            or not item["target_alias"] or len(item["target_alias"]) > 128
            or type(item["lease_active"]) is not bool
            or not _is_utc_timestamp(item["scheduled_for"])
            or not _is_utc_timestamp(item["created_at"])
            or not _is_utc_timestamp(item["updated_at"])
            or (item["lease_expires_at"] is not None
                and not _is_utc_timestamp(item["lease_expires_at"]))
            or (item["lease_active"]
                and (item["status"] != "leased" or item["lease_expires_at"] is None))):
        raise ValueError("scheduler occurrence inventory row is invalid")
    return item


def validate_paused_status(status):
    if (not isinstance(status, dict)
            or status.get("paused") is not True
            or type(status.get("control_revision")) is not int
            or status["control_revision"] < 0
            or type(status.get("pending_outbox")) is not int
            or status["pending_outbox"] < 0):
        raise ValueError("scheduler must report a valid paused status for rollback inventory")
    return status["control_revision"], status["pending_outbox"]


async def request_json(session, method, url, token, body=None):
    async with session.request(method, url, json=body, headers={"Authorization": f"Bearer {token}"},
                               allow_redirects=False) as response:
        raw = await response.content.read(512 * 1024 + 1)
        if len(raw) > 512 * 1024:
            raise ValueError("scheduler response exceeded the bounded response limit")
        if response.status >= 300:
            raise ValueError(f"scheduler API returned HTTP {response.status}")
        if response.status == 204:
            return None
        if response.headers.get("Content-Encoding", "identity").lower() != "identity":
            raise ValueError("scheduler API returned an unsupported content encoding")
        if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise ValueError("scheduler API returned a non-JSON response")
        try:
            return json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("scheduler API returned invalid JSON") from None


async def import_snapshot(args):
    snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
    if not isinstance(snapshot, dict) or snapshot.get("format") != "news-scheduler-snapshot-v1":
        raise ValueError("unsupported snapshot format")
    items = snapshot.get("schedules")
    if not isinstance(items, list) or not 1 <= len(items) <= 100:
        raise ValueError("snapshot must contain 1–100 schedule items")
    items = [validate_import_item(item) for item in items]
    if len({item["schedule_key"] for item in items}) != len(items):
        raise ValueError("snapshot contains duplicate schedule keys")
    if not re.fullmatch(r"\d+", args.expected_control_revision):
        raise ValueError("expected control revision must be a non-negative integer")
    base, token = service_config(args)
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        result = await request_json(session, "POST", base + "/v1/import", token, {
            "expected_control_revision": int(args.expected_control_revision), "items": items,
        })
    print(json.dumps(result, indent=2, sort_keys=True))


async def rollback_inventory(args):
    base, token = service_config(args)
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        status = await request_json(session, "GET", base + "/v1/status", token)
        initial_control_revision, initial_pending_outbox = validate_paused_status(status)
        schedules, next_cursor, seen = [], None, set()
        while True:
            query = "?" + urlencode({"limit": 2, **({"cursor": next_cursor} if next_cursor else {})})
            page = await request_json(session, "GET", base + "/v1/schedules" + query, token)
            if (not isinstance(page, dict) or set(page) != {"schedules", "next_cursor"}
                    or not isinstance(page["schedules"], list) or len(page["schedules"]) > 2
                    or (page["next_cursor"] is not None and not isinstance(page["next_cursor"], str))):
                raise ValueError("scheduler schedule inventory page is invalid")
            schedules.extend(page["schedules"])
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                break
            if next_cursor in seen or len(schedules) > 10000:
                raise ValueError("scheduler schedule inventory cursor or size is invalid")
            seen.add(next_cursor)
        schedules = [schedule for schedule in schedules if is_news_owned_schedule(schedule)]
        occurrences, next_cursor, seen = [], None, set()
        while True:
            query = "?" + urlencode({"limit": 100, **({"cursor": next_cursor} if next_cursor else {})})
            page = await request_json(session, "GET", base + "/v1/occurrences" + query, token)
            if (not isinstance(page, dict) or set(page) != {"occurrences", "next_cursor"}
                    or not isinstance(page["occurrences"], list) or len(page["occurrences"]) > 100
                    or (page["next_cursor"] is not None and not isinstance(page["next_cursor"], str))):
                raise ValueError("scheduler occurrence inventory page is invalid")
            for occurrence in page["occurrences"]:
                validate_occurrence_record(occurrence)
            occurrences.extend(page["occurrences"])
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                break
            if next_cursor in seen or len(occurrences) > 1_000_000:
                raise ValueError("scheduler occurrence inventory cursor or size is invalid")
            seen.add(next_cursor)
        news_occurrences = [occurrence for occurrence in occurrences
                            if is_news_owned_occurrence(occurrence)]
        if any(occurrence["lease_active"] for occurrence in news_occurrences):
            raise ValueError("scheduler has an in-flight leased dispatch; retry rollback inventory")
        current_news_keys = {schedule["schedule_key"] for schedule in schedules}
        occurrence_news_keys = {occurrence["schedule_key"] for occurrence in news_occurrences}
        orphaned_news_keys = sorted(occurrence_news_keys - current_news_keys)
        news_schedule_keys = current_news_keys | set(orphaned_news_keys)
        occurrences = news_occurrences
        final_status = await request_json(session, "GET", base + "/v1/status", token)
        final_control_revision, final_pending_outbox = validate_paused_status(final_status)
        if (final_control_revision != initial_control_revision
                or final_pending_outbox != initial_pending_outbox):
            raise ValueError("scheduler state changed during rollback inventory; retry from a paused state")
        status = final_status
    engine = create_async_engine(snapshot_database_url())
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            keys = sorted(news_schedule_keys)
            jobs = (await session.execute(
                select(BackgroundJob).where(BackgroundJob.schedule_key.in_(keys))
                .order_by(BackgroundJob.created_at, BackgroundJob.id).limit(100001)
            )).scalars().all() if keys else []
            if len(jobs) > 100000:
                raise ValueError("News execution inventory exceeds 100000 rows")
            news_jobs = [{
                "job_id": str(job.id), "schedule_key": job.schedule_key,
                "occurrence_id": str(job.occurrence_id) if job.occurrence_id else None,
                "state": job.state,
                "result_status": (job.result or {}).get("status") if isinstance(job.result, dict) else None,
                "created_at": utc_timestamp(job.created_at),
                "finished_at": utc_timestamp(job.finished_at),
            } for job in jobs]
    finally:
        await engine.dispose()
    latest_jobs = {}
    for job in news_jobs:
        current = latest_jobs.get(job["schedule_key"])
        if current is None or (job["created_at"] or "") > (current["created_at"] or ""):
            latest_jobs[job["schedule_key"]] = job
    rollback_schedules = [
        legacy_rollback_record(schedule, latest_jobs.get(schedule["schedule_key"]))
        for schedule in schedules
    ]
    artifact = {
        "format": "news-scheduler-rollback-inventory-v1",
        "captured_at": utc_timestamp(datetime.now(timezone.utc)),
        "scheduler_status": status,
        "legacy_schedule_snapshot": {
            "format": "news-scheduler-rollback-config-v1",
            "schedules": rollback_schedules,
        },
        "orphaned_news_schedule_keys": orphaned_news_keys,
        "news_execution_inventory": news_jobs,
        "scheduler_occurrences": occurrences,
        "note": "Inventory only. It preserves current configuration, successful News execution history, and pending/blocked/accepted occurrences. It does not delete, replay, or enable Python scheduling.",
    }
    write_artifact(args.output, artifact)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export", help="export a drained loopback News DB snapshot")
    export.add_argument("--output", required=True)
    importing = sub.add_parser("import", help="import a snapshot into a paused Go scheduler")
    importing.add_argument("--snapshot", required=True)
    importing.add_argument("--api-url", required=True)
    importing.add_argument("--expected-control-revision", required=True)
    importing.add_argument("--allow-http", action="store_true", help="permit HTTP only on an isolated private test or deployment network")
    importing.add_argument("--timeout", type=float, default=10)
    inventory = sub.add_parser("rollback-inventory", help="capture Go schedules and durable occurrence state")
    inventory.add_argument("--api-url", required=True)
    inventory.add_argument("--output", required=True)
    inventory.add_argument("--allow-http", action="store_true", help="permit HTTP only on an isolated private test or deployment network")
    inventory.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    try:
        if args.command == "export":
            asyncio.run(export_snapshot(args.output))
        elif args.command == "import":
            asyncio.run(import_snapshot(args))
        else:
            asyncio.run(rollback_inventory(args))
    except Exception as exc:
        message = str(exc) if isinstance(exc, (ValueError, argparse.ArgumentError)) else type(exc).__name__
        print(f"scheduler bridge failed: {message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
