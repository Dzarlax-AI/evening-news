"""Bounded client for the standalone Go schedule service."""

import json
import re
from urllib.parse import quote, urlencode, urlsplit

import aiohttp


MAX_RESPONSE_BYTES = 64 * 1024
MAX_IMPORT_REQUEST_BYTES = 512 * 1024
SCHEDULE_PAGE_SIZE = 2


class SchedulerServiceError(Exception):
    """Finite scheduler service failure safe to expose without remote details."""

    def __init__(self, code, status=None):
        super().__init__(code)
        self.code = code
        self.status = status


class SchedulerServiceClient:
    def __init__(self, endpoint, token, *, allow_http=False,
                 timeout_seconds=5.0, session_factory=aiohttp.ClientSession):
        parsed = urlsplit(endpoint or "")
        if (parsed.scheme not in ({"http", "https"} if allow_http else {"https"})
                or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment):
            raise ValueError("invalid_scheduler_endpoint")
        if not token:
            raise ValueError("missing_scheduler_token")
        if not 0 < float(timeout_seconds) <= 30:
            raise ValueError("invalid_scheduler_timeout")
        self.endpoint = endpoint.rstrip("/")
        self._token = token
        self.timeout_seconds = float(timeout_seconds)
        self._session_factory = session_factory

    async def request(self, method, path, *, body=None, expected_status=(200,)):
        if not path.startswith("/v1/") or ".." in path.split("/"):
            raise ValueError("invalid_scheduler_path")
        url = self.endpoint + path
        data = None
        headers = {"Authorization": "Bearer " + self._token, "Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, separators=(",", ":"), ensure_ascii=False,
                              allow_nan=False).encode("utf-8")
            request_limit = MAX_IMPORT_REQUEST_BYTES if path == "/v1/import" else 16 * 1024
            if len(data) > request_limit:
                raise SchedulerServiceError("request_too_large")
            headers["Content-Type"] = "application/json"
        try:
            async with self._session_factory(
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                    trust_env=False, auto_decompress=False) as session:
                async with session.request(method, url, data=data, headers=headers,
                        allow_redirects=False) as response:
                    if response.status not in expected_status:
                        raise SchedulerServiceError(
                            "http_" + str(response.status), response.status
                        )
                    if response.status == 204:
                        return None
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise SchedulerServiceError("invalid_response")
                    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                    if content_type != "application/json":
                        raise SchedulerServiceError("invalid_response")
                    raw = bytearray()
                    async for chunk in response.content.iter_chunked(8192):
                        if len(raw) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise SchedulerServiceError("response_too_large")
                        raw.extend(chunk)
                    try:
                        result = json.loads(raw)
                    except (ValueError, UnicodeError, RecursionError):
                        raise SchedulerServiceError("invalid_response") from None
                    return result
        except (aiohttp.ClientError, TimeoutError):
            raise SchedulerServiceError("transport_error") from None

    async def list_schedules(self):
        schedules, cursor, seen_cursors = [], None, set()
        while True:
            query = "?limit=" + str(SCHEDULE_PAGE_SIZE) + (
                "&" + urlencode({"cursor": cursor}) if cursor else ""
            )
            result = await self.request("GET", "/v1/schedules" + query)
            if (not isinstance(result, dict)
                    or set(result) != {"schedules", "next_cursor"}
                    or not isinstance(result["schedules"], list)
                    or len(result["schedules"]) > SCHEDULE_PAGE_SIZE
                    or (result["next_cursor"] is not None
                        and not isinstance(result["next_cursor"], str))):
                raise SchedulerServiceError("invalid_response")
            for schedule in result["schedules"]:
                validate_schedule_resource(schedule)
            schedules.extend(result["schedules"])
            if len(schedules) > 10000:
                raise SchedulerServiceError("schedule_inventory_too_large")
            cursor = result["next_cursor"]
            if cursor is None:
                return schedules
            if cursor in seen_cursors:
                raise SchedulerServiceError("invalid_response")
            seen_cursors.add(cursor)

    async def get_schedule(self, schedule_key):
        result = await self.request("GET", "/v1/schedules/" + quote(schedule_key, safe="._:-"))
        validate_schedule_resource(result)
        return result

    async def put_schedule(self, schedule_key, body):
        try:
            result = await self.request("PUT", "/v1/schedules/" + quote(schedule_key, safe="._:-"),
                                        body=body, expected_status=(200, 201))
        except SchedulerServiceError as exc:
            if exc.code != "transport_error":
                raise
            # Resolve a lost PUT response with a read. Never repeat the mutation
            # blindly: the expected revision may already have advanced.
            current = await self.get_schedule(schedule_key)
            desired = {key: value for key, value in body.items() if key != "expected_revision"}
            actual = {key: current[key] for key in desired if key in current}
            if actual != desired:
                raise exc
            result = current
        validate_schedule_resource(result)
        if result["schedule_key"] != schedule_key:
            raise SchedulerServiceError("invalid_response")
        return result

    async def delete_schedule(self, schedule_key, expected_revision):
        return await self.request("DELETE", "/v1/schedules/" + quote(schedule_key, safe="._:-"),
                                  body={"expected_revision": expected_revision},
                                  expected_status=(204,))

    async def get_status(self):
        result = await self.request("GET", "/v1/status")
        if (not isinstance(result, dict) or type(result.get("paused")) is not bool
                or type(result.get("control_revision")) is not int
                or result["control_revision"] < 0
                or type(result.get("pending_outbox")) is not int
                or result["pending_outbox"] < 0
                or any(result.get(name) is not None and not isinstance(result[name], str)
                       for name in ("last_tick_at", "last_dispatch_at"))):
            raise SchedulerServiceError("invalid_response")
        return result

    async def import_schedules(self, expected_control_revision, items):
        if (type(expected_control_revision) is not int or expected_control_revision < 0
                or not isinstance(items, list) or not 1 <= len(items) <= 100):
            raise SchedulerServiceError("invalid_request")
        required = {"schedule_key", "enabled", "calendar", "target_alias", "operation", "payload", "next_due_at"}
        if any(not isinstance(item, dict) or set(item) != required
               or not isinstance(item["payload"], dict)
               or len(json.dumps(item["payload"], separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > 4096
               for item in items):
            raise SchedulerServiceError("invalid_request")
        result = await self.request("POST", "/v1/import", body={
            "expected_control_revision": expected_control_revision,
            "items": items,
        })
        if (not isinstance(result, dict)
                or set(result) != {"control_revision", "created", "unchanged"}
                or type(result["control_revision"]) is not int
                or not isinstance(result["created"], list)
                or not isinstance(result["unchanged"], list)
                or any(not isinstance(key, str) for key in result["created"] + result["unchanged"])):
            raise SchedulerServiceError("invalid_response")
        return result

    async def list_occurrences(self, *, schedule_key=None, limit=100, cursor=None):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise SchedulerServiceError("invalid_request")
        query = {"limit": limit}
        if schedule_key is not None:
            query["schedule_key"] = schedule_key
        if cursor is not None:
            query["cursor"] = cursor
        result = await self.request("GET", "/v1/occurrences?" + urlencode(query))
        if (not isinstance(result, dict)
                or set(result) != {"occurrences", "next_cursor"}
                or not isinstance(result["occurrences"], list)
                or len(result["occurrences"]) > limit
                or (result["next_cursor"] is not None and not isinstance(result["next_cursor"], str))):
            raise SchedulerServiceError("invalid_response")
        expected_fields = {"occurrence_id", "schedule_key", "schedule_revision", "scheduled_for",
                           "status", "attempts", "created_at", "updated_at", "job_id",
                           "target_alias", "lease_expires_at", "lease_active"}
        for item in result["occurrences"]:
            if (not isinstance(item, dict) or set(item) != expected_fields
                    or type(item["schedule_revision"]) is not int
                    or type(item["attempts"]) is not int
                    or not isinstance(item["occurrence_id"], str)
                    or not isinstance(item["schedule_key"], str)
                    or not isinstance(item["target_alias"], str)
                    or not item["target_alias"] or len(item["target_alias"]) > 128
                    or not isinstance(item["scheduled_for"], str)
                    or not isinstance(item["created_at"], str)
                    or not isinstance(item["updated_at"], str)
                    or not isinstance(item["status"], str)
                    or item["status"] not in {"pending", "leased", "blocked", "accepted"}
                    or type(item["lease_active"]) is not bool
                    or (item["lease_expires_at"] is not None
                        and not _is_utc_timestamp(item["lease_expires_at"]))
                    or (item["lease_active"] and
                        (item["status"] != "leased" or item["lease_expires_at"] is None))
                    or (item["job_id"] is not None and not isinstance(item["job_id"], str))):
                raise SchedulerServiceError("invalid_response")
        return result


def validate_schedule_resource(value):
    if (not isinstance(value, dict)
            or set(value) != {"schedule_key", "revision", "enabled", "calendar",
                              "target_alias", "operation", "payload", "next_due_at",
                              "created_at", "updated_at"}
            or not isinstance(value.get("schedule_key"), str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._:-]{0,127}", value["schedule_key"])
            or type(value.get("revision")) is not int or value["revision"] < 1
            or type(value.get("enabled")) is not bool
            or not isinstance(value.get("calendar"), dict)
            or not isinstance(value["calendar"].get("type"), str)
            or not isinstance(value["calendar"].get("timezone"), str)
            or not isinstance(value.get("target_alias"), str)
            or not isinstance(value.get("operation"), str)
            or not isinstance(value.get("payload"), dict)
            or (value.get("next_due_at") is not None
                and not _is_utc_timestamp(value["next_due_at"]))
            or not _is_utc_timestamp(value.get("created_at"))
            or not _is_utc_timestamp(value.get("updated_at"))):
        raise SchedulerServiceError("invalid_response")
    calendar = value["calendar"]
    if calendar["type"] in {"daily", "weekly"}:
        if type(calendar.get("hour")) is not int or type(calendar.get("minute")) is not int:
            raise SchedulerServiceError("invalid_response")
    elif calendar["type"] == "hourly":
        if type(calendar.get("minute")) is not int:
            raise SchedulerServiceError("invalid_response")
    elif calendar["type"] == "interval":
        if type(calendar.get("interval_minutes")) is not int:
            raise SchedulerServiceError("invalid_response")
    else:
        raise SchedulerServiceError("invalid_response")
    return value


def _is_utc_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", value):
        return False
    from datetime import datetime
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return True
