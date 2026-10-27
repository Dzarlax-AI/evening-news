#!/usr/bin/env python3
"""Collect bounded, sanitized runtime evidence from fixtures or an explicit SSH run."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import shlex
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

MAX_SECONDS = 30 * 60
MAX_OUTPUT_BYTES = 5 * 1024 * 1024
MAX_BUNDLES = 3
POLL_SECONDS = 1.0
MAX_INPUT_LINE = 256 * 1024
MAX_EVENT_FINGERPRINTS = 256
SAFE_STAGES = {"admission", "readiness", "discovery", "websocket", "events", "create_context", "create_target", "attach_target", "configure", "navigate", "dom", "cleanup", "parse", "validation"}
SAFE_REASONS = {"context_timeout", "context_cancelled", "event_overflow", "ownership_loss", "unsupported", "client_disconnected", "malformed_detach", "owned_detach", "target_crashed", "protocol_error", "transport_error", "operation_failed", "navigation_failed", "evaluation_failed", "navigation_dns", "navigation_connection", "navigation_tls", "navigation_aborted", "navigation_other", "browser_busy", "resource_limit", "sqlstate", "timeout", "canceled", "transaction_closed", "connection_closed", "other", "lease_mismatch", "policy_denied", "unavailable"}
SAFE_CODES = {"cancelled", "deadline_exceeded", "event_overflow", "invalid_input", "native_browser_busy", "native_browser_policy_denied", "parse_failed", "public_url_denied", "resource_limit", "transport_failed", "unavailable", "unsupported_selector", "response_too_large", "page_monitor_unavailable", "invalid_runner_response", "other"}
EVENTS = {"worker_heartbeat_failure", "page_monitor_failure"}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def safe_time(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except ValueError:
        return None


def safe_log_cursor(value: Any) -> str | None:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value):
        return None
    return value if safe_time(value) is not None else None


def integer(value: Any, minimum: int = 0, maximum: int = 2**63 - 1) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        return None
    return value


def number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0 or value > 1e18:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def finite(value: Any, allowed: set[str]) -> str:
    return value if isinstance(value, str) and value in allowed else "other"


def sanitize_snapshot(raw: Any) -> dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    result: dict[str, Any] = {"captured_at": safe_time(raw.get("captured_at"))}
    missing: list[str] = []
    worker = raw.get("worker") if isinstance(raw.get("worker"), dict) else {}
    generation, pid = integer(worker.get("generation"), 1), integer(worker.get("pid"), 1, 2**31 - 1)
    heartbeat = safe_time(worker.get("heartbeat_at"))
    result["worker"] = {"generation": generation, "pid": pid, "heartbeat_at": heartbeat}
    if generation is None or pid is None or heartbeat is None:
        missing.append("worker")

    activity_out = []
    activities = raw.get("pg_stat_activity")
    if isinstance(activities, list):
        for item in activities[:64]:
            if not isinstance(item, dict):
                continue
            blockers = item.get("blocker_pids")
            activity_out.append({
                "pid": integer(item.get("pid"), 1, 2**31 - 1),
                "state": item.get("state") if isinstance(item.get("state"), str) and item.get("state") in {"active", "idle", "idle in transaction", "idle in transaction (aborted)", "fastpath function call", "disabled"} else "other",
                "wait_type": item.get("wait_type") if isinstance(item.get("wait_type"), str) and item.get("wait_type") in {"LWLock", "Lock", "BufferPin", "Activity", "Client", "Extension", "IO", "IPC", "Timeout"} else ("other" if item.get("wait_type") is not None else None),
                "wait_event": item.get("wait_event") if isinstance(item.get("wait_event"), str) and re.fullmatch(r"[A-Za-z0-9_]{1,64}", item["wait_event"]) else None,
                "transaction_age_seconds": number(item.get("transaction_age_seconds")),
                "blocker_pids": [v for v in (integer(pid, 1, 2**31 - 1) for pid in blockers[:16]) if v is not None] if isinstance(blockers, list) else [],
            })
        result["pg_stat_activity"] = activity_out
    else:
        result["pg_stat_activity"] = []
        missing.append("pg_stat_activity")

    metric_names = ("wal_bytes", "checkpoints_timed", "checkpoints_requested", "checkpoint_write_time_ms", "checkpoint_sync_time_ms", "buffers_checkpoint", "buffers_clean")
    metrics = raw.get("pg_metrics") if isinstance(raw.get("pg_metrics"), dict) else {}
    result["pg_metrics"] = {key: number(metrics.get(key)) for key in metric_names}
    if any(value is None for value in result["pg_metrics"].values()):
        missing.append("pg_metrics")

    host = raw.get("host") if isinstance(raw.get("host"), dict) else {}
    psi = host.get("memory_psi") if isinstance(host.get("memory_psi"), dict) else {}
    result["host"] = {
        "memory_total_bytes": integer(host.get("memory_total_bytes")),
        "memory_available_bytes": integer(host.get("memory_available_bytes")),
        "swap_total_bytes": integer(host.get("swap_total_bytes")),
        "swap_free_bytes": integer(host.get("swap_free_bytes")),
        "memory_psi_some_avg10": number(psi.get("some_avg10")),
        "memory_psi_full_avg10": number(psi.get("full_avg10")),
    }
    if any(value is None for value in result["host"].values()):
        missing.append("host")

    containers = raw.get("containers") if isinstance(raw.get("containers"), dict) else {"background": raw.get("cgroup", {})}
    result["containers"] = {}
    for name in ("background", "extractor", "postgres"):
        cgroup = containers.get(name) if isinstance(containers.get(name), dict) else {}
        container_id = cgroup.get("container_id")
        if not isinstance(container_id, str) or not re.fullmatch(r"[a-fA-F0-9]{12,64}", container_id):
            container_id = None
        result["containers"][name] = {
            "working_set_bytes": integer(cgroup.get("working_set_bytes")),
            "cache_bytes": integer(cgroup.get("cache_bytes")),
            "file_bytes": integer(cgroup.get("file_bytes")),
            "cpu_throttled_usec": integer(cgroup.get("cpu_throttled_usec")),
            "cpu_throttled_usec_delta": integer(cgroup.get("cpu_throttled_usec_delta")),
            "cpu_periods": integer(cgroup.get("cpu_periods")),
            "cpu_throttled_periods": integer(cgroup.get("cpu_throttled_periods")),
            "cpu_throttled_periods_delta": integer(cgroup.get("cpu_throttled_periods_delta")),
            "container_id": container_id,
            "started_at": safe_time(cgroup.get("started_at")),
            "restart_count": integer(cgroup.get("restart_count")),
            "oom_count": integer(cgroup.get("oom_count")),
            "oom_killed": cgroup.get("oom_killed") if isinstance(cgroup.get("oom_killed"), bool) else None,
        }
        required = ("working_set_bytes", "file_bytes", "cpu_throttled_usec", "cpu_periods", "cpu_throttled_periods", "container_id", "started_at", "restart_count", "oom_count", "oom_killed")
        if any(result["containers"][name][key] is None for key in required):
            missing.append("container_" + name)
        if cgroup.get("cpu_throttled_usec_delta") is None or cgroup.get("cpu_throttled_periods_delta") is None:
            missing.append("container_" + name + "_cpu_delta")
    errors = raw.get("collection_errors")
    allowed_errors = {"ssh_failed", "timeout", "log_overflow", "remote_snapshot_failed", "database_unavailable", "container_unavailable", "cgroup_unavailable", "log_unavailable"}
    result["collection_errors"] = [finite(item, allowed_errors) for item in errors[:16]] if isinstance(errors, list) else []
    result["missing"] = missing
    return result


def sanitize_event(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or not isinstance(raw.get("event"), str) or raw.get("event") not in EVENTS:
        return None
    event_utc = safe_time(raw.get("event_utc"))
    if event_utc is None:
        return None
    source = raw.get("source")
    event: dict[str, Any] = {"event": raw["event"], "event_utc": event_utc, "source": source if isinstance(source, str) and source in {"background", "extractor"} else None}
    if raw["event"] == "worker_heartbeat_failure":
        event.update({
            "stage": finite(raw.get("stage"), {"begin", "update", "commit", "unknown"}),
            "reason": finite(raw.get("reason"), SAFE_REASONS),
            "sqlstate": raw.get("sqlstate") if isinstance(raw.get("sqlstate"), str) and re.fullmatch(r"[A-Z0-9]{5}", raw["sqlstate"]) else None,
            "backend_pid": integer(raw.get("backend_pid"), 1, 2**31 - 1),
            "generation": integer(raw.get("generation"), 1),
            "mutex_wait_ms": integer(raw.get("mutex_wait_ms")),
            "begin_ms": integer(raw.get("begin_ms")),
            "update_ms": integer(raw.get("update_ms")),
            "commit_ms": integer(raw.get("commit_ms")),
        })
    else:
        token = raw.get("request_token")
        event.update({
            "request_token": token if isinstance(token, str) and re.fullmatch(r"[a-f0-9]{64}", token) else None,
            "source_id": integer(raw.get("source_id"), 1, 2**63 - 1),
            "stage": finite(raw.get("stage"), SAFE_STAGES),
            "reason": finite(raw.get("reason"), SAFE_REASONS),
            "elapsed_ms": integer(raw.get("elapsed_ms")),
            "outcome": finite(raw.get("outcome"), {"success", "partial", "failed", "rejected"}),
            "error_code": finite(raw.get("error_code"), SAFE_CODES),
        })
    return event


def parse_log_line(line: str) -> dict[str, Any] | None:
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, TypeError, RecursionError):
        record = None
    if isinstance(record, dict) and isinstance(record.get("event"), str) and record["event"] in EVENTS:
        if "event_utc" not in record and isinstance(record.get("time"), str):
            record = {**record, "event_utc": record["time"]}
        return sanitize_event(record)
    try:
        fields = dict(part.split("=", 1) for part in shlex.split(line) if "=" in part)
    except ValueError:
        return None
    if fields.get("event") not in EVENTS:
        return None
    numeric_fields = {"generation", "backend_pid", "mutex_wait_ms", "begin_ms", "update_ms", "commit_ms", "source_id", "elapsed_ms"}
    for key in numeric_fields & fields.keys():
        fields[key] = _int_text(fields[key])
    return sanitize_event(fields)


def bundle(trigger: dict[str, Any], ring: deque[dict[str, Any]]) -> dict[str, Any]:
    snapshots = list(ring)
    current = snapshots[-1] if snapshots else None
    event_time = safe_time(trigger.get("event_utc"))
    current_time = safe_time(current.get("captured_at")) if current else None
    late = True
    if event_time and current_time:
        e = dt.datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        c = dt.datetime.fromisoformat(current_time.replace("Z", "+00:00"))
        late = abs((e - c).total_seconds()) > POLL_SECONDS
    return {
        "collected_at": utc_now(),
        "trigger": trigger,
        "pre_event_snapshots": snapshots[:-1],
        "current_snapshot": current,
        "evidence_status": {"current_snapshot": "late_or_missing" if late else "within_one_second", "snapshot_count": len(snapshots)},
    }


def event_fingerprint(event: dict[str, Any]) -> str:
    identity = {key: event.get(key) for key in ("event", "event_utc", "source", "request_token", "source_id", "backend_pid", "generation", "stage", "reason")}
    return json.dumps(identity, sort_keys=True, separators=(",", ":"))


def remember_fingerprint(seen: set[str], fingerprint: str) -> None:
    if len(seen) >= MAX_EVENT_FINGERPRINTS:
        seen.clear()
    seen.add(fingerprint)


def create_output_dir(parent: Path) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for _ in range(8):
        candidate = parent / f"incident-{stamp}-{os.urandom(3).hex()}"
        try:
            candidate.mkdir(mode=0o700)
            return candidate
        except FileExistsError:
            continue
    raise FileExistsError("could not create a unique incident output directory")


def write_bundle(directory: Path, value: dict[str, Any], count: int, byte_count: int, byte_limit: int) -> int:
    payload = (json.dumps(value, separators=(",", ":"), ensure_ascii=True) + "\n").encode()
    if byte_count + len(payload) > byte_limit:
        raise RuntimeError("output_limit")
    target = directory / f"bundle-{count + 1}.json"
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
    return byte_count + len(payload)


def replay_fixture(path: Path, output: Path, max_bundles: int, byte_limit: int, duration_seconds: int = MAX_SECONDS) -> tuple[int, int]:
    if not path.is_file():
        raise ValueError("fixture must be a regular file")
    if not 1 <= duration_seconds <= MAX_SECONDS:
        raise ValueError("fixture duration exceeds the hard safety bound")
    if not 1 <= max_bundles <= MAX_BUNDLES or not 1 <= byte_limit <= MAX_OUTPUT_BYTES:
        raise ValueError("fixture output exceeds the hard safety bound")
    directory = create_output_dir(output)
    deadline = time.monotonic() + duration_seconds
    ring: deque[dict[str, Any]] = deque(maxlen=30)
    count = size = 0
    seen: set[str] = set()
    with path.open(encoding="utf-8", errors="replace") as source:
        while time.monotonic() < deadline and count < max_bundles and size < byte_limit:
            raw_line = source.readline(MAX_INPUT_LINE + 1)
            if not raw_line:
                break
            if len(raw_line) > MAX_INPUT_LINE:
                while raw_line and not raw_line.endswith("\n") and time.monotonic() < deadline:
                    raw_line = source.readline(MAX_INPUT_LINE + 1)
                if time.monotonic() >= deadline:
                    break
                continue
            try:
                item = json.loads(raw_line)
            except (json.JSONDecodeError, UnicodeError, RecursionError):
                continue
            if not isinstance(item, dict):
                continue
            if item.get("kind") == "snapshot":
                ring.append(sanitize_snapshot(item.get("snapshot")))
            elif item.get("kind") == "event":
                event = sanitize_event(item.get("event"))
                if event is not None:
                    fingerprint = event_fingerprint(event)
                    if fingerprint in seen:
                        continue
                    remember_fingerprint(seen, fingerprint)
                    if count >= max_bundles:
                        break
                    size = write_bundle(directory, bundle(event, ring), count, size, byte_limit)
                    count += 1
                    if count >= max_bundles:
                        break
            elif item.get("kind") == "log":
                event = parse_log_line(item.get("line", "")) if isinstance(item.get("line"), str) else None
                if event is not None:
                    fingerprint = event_fingerprint(event)
                    if fingerprint in seen:
                        continue
                    remember_fingerprint(seen, fingerprint)
                    if count >= max_bundles:
                        break
                    size = write_bundle(directory, bundle(event, ring), count, size, byte_limit)
                    count += 1
                    if count >= max_bundles:
                        break
            if size >= byte_limit:
                break
    return count, size


REMOTE_SCRIPT = r'''set +e
set -o pipefail
BACKGROUND='@BACKGROUND@'; EXTRACTOR='@EXTRACTOR@'; DB='@DB@'; DBUSER='@DBUSER@'; DBNAME='@DBNAME@'; SCHEMA='@SCHEMA@'
LOG_CURSOR_BACKGROUND='@LOG_CURSOR_BACKGROUND@'; LOG_CURSOR_EXTRACTOR='@LOG_CURSOR_EXTRACTOR@'
POLL_START_UTC=$(date -u +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)
case "$POLL_START_UTC" in ????-??-??T??:??:??Z) printf 'log_cursor_seed=%s\n' "$POLL_START_UTC" ;; *) POLL_START_UTC= ;; esac
awk '/^(MemTotal|MemAvailable|SwapTotal|SwapFree):/ {gsub(":", "", $1); print tolower($1) "=" $2}' /proc/meminfo 2>/dev/null
awk '/^(some|full) / {for(i=1;i<=NF;i++) if($i ~ /^avg10=/) {split($i,a,"="); print "psi_" $1 "=" a[2]}}' /proc/pressure/memory 2>/dev/null
container_snapshot() {
  role="$1"; name="$2"; [ -n "$name" ] || return
  meta=$(docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.RestartCount}} {{.State.OOMKilled}} {{.State.Pid}}' "$name" 2>/dev/null) || { echo "${role}_error=container_unavailable"; return; }
  set -- $meta; cid="$1"; started="$2"; restarts="$3"; oom="$4"; hostpid="$5"
  case "$cid" in *[!a-fA-F0-9]*|'') return;; esac
  printf '%s\n' "${role}_container=$cid" "${role}_started=$started" "${role}_restarts=$restarts" "${role}_oom=$oom"
  [ "$hostpid" -gt 1 ] 2>/dev/null || return
  cgpath=$(awk -F: '$1 == "0" {print $3; exit}' "/proc/$hostpid/cgroup" 2>/dev/null)
  [ -n "$cgpath" ] || { echo "${role}_error=cgroup_unavailable"; return; }
  base="/sys/fs/cgroup$cgpath"
  current=$(cat "$base/memory.current" 2>/dev/null); file=$(awk '$1 == "file" {print $2}' "$base/memory.stat" 2>/dev/null); inactive=$(awk '$1 == "inactive_file" {print $2}' "$base/memory.stat" 2>/dev/null)
  periods=$(awk '$1 == "nr_periods" {print $2}' "$base/cpu.stat" 2>/dev/null); throttled=$(awk '$1 == "nr_throttled" {print $2}' "$base/cpu.stat" 2>/dev/null); usec=$(awk '$1 == "throttled_usec" {print $2}' "$base/cpu.stat" 2>/dev/null); oomkills=$(awk '$1 == "oom_kill" {print $2}' "$base/memory.events" 2>/dev/null)
  case "$current:$file:$inactive:$periods:$throttled:$usec:$oomkills" in *[!0-9:]*|'') echo "${role}_error=cgroup_unavailable"; return;; esac
  printf '%s\n' "${role}_memory_current=$current" "${role}_file=$file" "${role}_inactive_file=$inactive" "${role}_periods=$periods" "${role}_throttled_periods=$throttled" "${role}_throttled_usec=$usec" "${role}_oomkills=$oomkills"
}
container_snapshot background "$BACKGROUND"; container_snapshot extractor "$EXTRACTOR"; container_snapshot postgres "$DB"
sql="BEGIN READ ONLY; SET LOCAL statement_timeout='500ms'; SELECT jsonb_build_object('captured_at',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z','worker',(SELECT jsonb_build_object('generation',generation,'pid',lock_backend_pid,'heartbeat_at',to_char(heartbeat_at,'YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z') FROM \"$SCHEMA\".background_worker_status WHERE id=1),'pg_stat_activity',(SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'state',state,'wait_type',wait_event_type,'wait_event',wait_event,'transaction_age_seconds',extract(epoch FROM (clock_timestamp()-xact_start)),'blocker_pids',pg_blocking_pids(pid))),'[]'::jsonb) FROM (SELECT pid,state,wait_event_type,wait_event,xact_start FROM pg_stat_activity WHERE pid<>pg_backend_pid() ORDER BY pid LIMIT 64) a),'pg_metrics',(SELECT jsonb_build_object('wal_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0'),'checkpoints_timed',nullif(to_jsonb(b)->>'checkpoints_timed','')::numeric,'checkpoints_requested',nullif(to_jsonb(b)->>'checkpoints_req','')::numeric,'checkpoint_write_time_ms',nullif(to_jsonb(b)->>'checkpoint_write_time','')::numeric,'checkpoint_sync_time_ms',nullif(to_jsonb(b)->>'checkpoint_sync_time','')::numeric,'buffers_checkpoint',nullif(to_jsonb(b)->>'buffers_checkpoint','')::numeric,'buffers_clean',nullif(to_jsonb(b)->>'buffers_clean','')::numeric) FROM pg_stat_bgwriter b)); COMMIT;"
dbresult=$(docker exec "$DB" psql -XAtq -v ON_ERROR_STOP=1 -U "$DBUSER" -d "$DBNAME" -c "$sql" 2>/dev/null)
if [ -n "$dbresult" ]; then echo "db_json=$dbresult"; else echo "database_error=database_unavailable"; fi
capture_logs() {
  role="$1"; name="$2"; cursor="$3"; [ -n "$name" ] || return
  case "$cursor" in ????-??-??T??:??:??Z) since="$cursor" ;; *) since="$POLL_START_UTC" ;; esac
  case "$since" in ????-??-??T??:??:??Z) ;; *) echo "${role}_log_error=log_unavailable"; return ;; esac
  boundary=$(date -u +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)
  case "$boundary" in ????-??-??T??:??:??Z) ;; *) echo "${role}_log_error=log_unavailable"; return ;; esac
  python3 - "$role" "$name" "$since" "$boundary" <<'PYLOG'
import datetime
import json
import os
import re
import selectors
import shlex
import subprocess
import sys
import time

role, container, since, boundary = sys.argv[1:5]
MAX_BYTES = 262144
MAX_LINES = 64
MAX_LINE_BYTES = 8192
EVENTS = {"worker_heartbeat_failure", "page_monitor_failure"}
REASONS = "context_timeout|context_cancelled|event_overflow|ownership_loss|unsupported|protocol_error|transport_error|operation_failed|navigation_failed|evaluation_failed|navigation_dns|navigation_connection|navigation_tls|navigation_aborted|navigation_other|browser_busy|resource_limit|sqlstate|timeout|canceled|cancelled|transaction_closed|connection_closed|other|lease_mismatch|malformed_detach|owned_detach|target_crashed|client_disconnected|policy_denied|unavailable".split("|")
CODES = "cancelled|deadline_exceeded|event_overflow|invalid_input|native_browser_busy|native_browser_policy_denied|parse_failed|public_url_denied|resource_limit|transport_failed|unavailable|unsupported_selector|response_too_large|page_monitor_unavailable|invalid_runner_response|other".split("|")
STAGES = "admission|readiness|discovery|websocket|events|create_context|create_target|attach_target|configure|navigate|dom|cleanup|parse|validation".split("|")

def valid_time(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z", value):
        return False
    try:
        datetime.datetime.fromisoformat(value[:-1] + "+00:00")
        return True
    except ValueError:
        return False

def finite(value, choices):
    return value if value in choices else "other"

def uint(value, maximum=2**63-1, minimum=0):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and re.fullmatch(r"\d{1,18}", value):
        result = int(value)
    else:
        return None
    return result if minimum <= result <= maximum else None

def sanitize(line):
    if len(line) > MAX_LINE_BYTES:
        return None
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, RecursionError):
        record = None
    if isinstance(record, dict):
        fields = record
    else:
        try:
            fields = dict(part.split("=", 1) for part in shlex.split(line) if "=" in part)
        except ValueError:
            return None
    event_type = fields.get("event")
    if not isinstance(event_type, str) or event_type not in EVENTS:
        return None
    stamp = fields.get("event_utc") or fields.get("time")
    if not valid_time(stamp):
        return None
    output = ["log=event=" + event_type, "event_utc=" + stamp, "source=" + role]
    if event_type == "worker_heartbeat_failure":
        output += ["stage=" + finite(fields.get("stage"), ["begin", "update", "commit", "unknown"]), "reason=" + finite(fields.get("reason"), REASONS)]
        sqlstate = fields.get("sqlstate", "")
        if isinstance(sqlstate, str) and re.fullmatch(r"[A-Z0-9]{5}", sqlstate):
            output.append("sqlstate=" + sqlstate)
        for key, minimum in (("backend_pid", 1), ("generation", 1), ("mutex_wait_ms", 0), ("begin_ms", 0), ("update_ms", 0), ("commit_ms", 0)):
            parsed = uint(fields.get(key), minimum=minimum)
            if parsed is not None:
                output.append(key + "=" + str(parsed))
    else:
        token = fields.get("request_token", "")
        if isinstance(token, str) and re.fullmatch(r"[a-f0-9]{64}", token):
            output.append("request_token=" + token)
        source_id = uint(fields.get("source_id"), minimum=1)
        if source_id is not None:
            output.append("source_id=" + str(source_id))
        output += ["stage=" + finite(fields.get("stage"), STAGES), "reason=" + finite(fields.get("reason"), REASONS)]
        elapsed = uint(fields.get("elapsed_ms"))
        if elapsed is not None:
            output.append("elapsed_ms=" + str(elapsed))
        output += ["outcome=" + finite(fields.get("outcome"), ["success", "partial", "failed", "rejected"]), "error_code=" + finite(fields.get("error_code"), CODES)]
    return " ".join(output)

def stop_observer(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

try:
    if not valid_time(since) or not valid_time(boundary) or role not in {"background", "extractor"}:
        raise ValueError
    process = subprocess.Popen(["docker", "logs", "--since", since, "--tail", "65", container], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    data = bytearray()
    deadline = time.monotonic() + 3.0
    overflow = False
    timed_out = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            break
        if not selector.select(remaining):
            timed_out = True
            break
        chunk = os.read(process.stdout.fileno(), min(65536, MAX_BYTES + 1 - len(data)))
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > MAX_BYTES:
            overflow = True
            break
    selector.close()
    if overflow or timed_out:
        stop_observer(process)
        lines = bytes(data).decode("utf-8", errors="replace").splitlines()
        if len(lines) > MAX_LINES:
            lines = lines[-MAX_LINES:]
        for raw_line in lines:
            safe = sanitize(raw_line)
            if safe is not None:
                print(safe)
        print(role + "_log_error=" + ("log_overflow" if overflow else "timeout"))
    else:
        try:
            return_code = process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            stop_observer(process)
            print(role + "_log_error=timeout")
            raise SystemExit(0)
        lines = bytes(data).decode("utf-8", errors="replace").splitlines()
        overflow = len(lines) > MAX_LINES
        if overflow:
            lines = lines[-MAX_LINES:]
        for raw_line in lines:
            safe = sanitize(raw_line)
            if safe is not None:
                print(safe)
        if overflow:
            print(role + "_log_error=log_overflow")
        elif return_code != 0:
            print(role + "_log_error=log_unavailable")
        else:
            print("log_cursor_" + role + "=" + boundary)
except (OSError, ValueError, subprocess.SubprocessError):
    if "process" in locals():
        stop_observer(process)
    if "selector" in locals():
        selector.close()
    print(role + "_log_error=log_unavailable")
PYLOG
}
capture_logs background "$BACKGROUND" "$LOG_CURSOR_BACKGROUND"
capture_logs extractor "$EXTRACTOR" "$LOG_CURSOR_EXTRACTOR"
'''


def safe_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
        raise ValueError(f"invalid configured {label}")
    return value


def safe_pg_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value):
        raise ValueError(f"invalid configured {label}")
    return value


def parse_live_output(output: str, log_cursors: dict[str, str] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    raw: dict[str, Any] = {"pg_stat_activity": [], "pg_metrics": {}, "host": {"memory_psi": {}}, "containers": {}, "collection_errors": []}
    events = []
    for line in output[:1024 * 1024].splitlines()[:1024]:
        if line.startswith("log="):
            event = parse_log_line(line[4:])
            if event:
                events.append(event)
        elif line.startswith("db_json="):
            try:
                db = json.loads(line[8:])
                if isinstance(db, dict):
                    raw.update({key: db.get(key, raw.get(key)) for key in ("captured_at", "worker", "pg_stat_activity", "pg_metrics")})
            except json.JSONDecodeError:
                pass
        elif "=" in line:
            key, value = line.split("=", 1)
            cursor_match = re.fullmatch(r"log_cursor_(background|extractor)", key)
            if key == "log_cursor_seed":
                seed = safe_log_cursor(value)
                if seed is not None and log_cursors is not None:
                    log_cursors.setdefault("background", seed)
                    log_cursors.setdefault("extractor", seed)
            elif cursor_match:
                cursor = safe_log_cursor(value)
                if cursor is not None and log_cursors is not None:
                    log_cursors[cursor_match.group(1)] = cursor
            elif key in {"background_error", "extractor_error", "postgres_error", "background_log_error", "extractor_log_error", "database_error"}:
                raw["collection_errors"].append(value)
            elif key in {"memtotal", "memavailable", "swaptotal", "swapfree"}:
                raw["host"][{"memtotal":"memory_total_bytes", "memavailable":"memory_available_bytes", "swaptotal":"swap_total_bytes", "swapfree":"swap_free_bytes"}[key]] = _int_text(value) * 1024 if _int_text(value) is not None else None
            elif key in {"psi_some", "psi_full"}:
                raw["host"]["memory_psi"][key.removeprefix("psi_") + "_avg10"] = _float_text(value)
            else:
                match = re.fullmatch(r"(background|extractor|postgres)_(container|started|restarts|oom|memory_current|file|inactive_file|periods|throttled_periods|throttled_usec|oomkills)", key)
                if match:
                    role, field = match.groups()
                    cgroup = raw["containers"].setdefault(role, {})
                    if field == "container": cgroup["container_id"] = value
                    elif field == "started": cgroup["started_at"] = value
                    elif field == "oom": cgroup["oom_killed"] = True if value == "true" else False if value == "false" else None
                    elif field in {"restarts", "oomkills", "memory_current", "file", "inactive_file", "periods", "throttled_periods", "throttled_usec"}:
                        cgroup[{"restarts":"restart_count", "oomkills":"oom_count", "memory_current":"memory_current_bytes", "file":"file_bytes", "inactive_file":"inactive_file_bytes", "periods":"cpu_periods", "throttled_periods":"cpu_throttled_periods", "throttled_usec":"cpu_throttled_usec"}[field]] = _int_text(value)
                        if field == "file":
                            cgroup["cache_bytes"] = _int_text(value)
    for cgroup in raw["containers"].values():
        current, inactive = cgroup.get("memory_current_bytes"), cgroup.get("inactive_file_bytes")
        if isinstance(current, int) and isinstance(inactive, int):
            cgroup["working_set_bytes"] = max(0, current - inactive)
        cgroup.pop("memory_current_bytes", None)
        cgroup.pop("inactive_file_bytes", None)
    raw["cgroup"] = raw["containers"].get("background", {})
    return raw, events


def _int_text(value: str) -> int | None:
    return int(value) if isinstance(value, str) and value.isdecimal() and len(value) <= 18 else None


def _float_text(value: str) -> float | None:
    try:
        result = float(value)
        return result if 0 <= result <= 100 else None
    except (TypeError, ValueError):
        return None


def live_poll(host: str, background: str, extractor: str, db: str, dbuser: str, dbname: str, schema: str, timeout: float = 12.0, log_cursors: dict[str, str] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    command = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, "bash", "-s"]
    cursors = log_cursors if log_cursors is not None else {}
    script = (REMOTE_SCRIPT.replace("@BACKGROUND@", background).replace("@EXTRACTOR@", extractor)
              .replace("@DB@", db).replace("@DBUSER@", dbuser).replace("@DBNAME@", dbname).replace("@SCHEMA@", schema)
              .replace("@LOG_CURSOR_BACKGROUND@", safe_log_cursor(cursors.get("background")) or "")
              .replace("@LOG_CURSOR_EXTRACTOR@", safe_log_cursor(cursors.get("extractor")) or ""))
    try:
        completed = subprocess.run(command, input=script, text=True, capture_output=True, timeout=max(0.01, timeout), check=False)
        output = completed.stdout[:1024 * 1024]
        returncode = completed.returncode
    except subprocess.TimeoutExpired as error:
        output = error.stdout or ""
        if isinstance(output, bytes):
            output = output[:1024 * 1024].decode("utf-8", errors="replace")
        else:
            output = output[:1024 * 1024]
        returncode = -1
    raw, events = parse_live_output(output, cursors)
    if returncode == -1:
        raw["collection_errors"].append("timeout")
    elif returncode != 0:
        raw["collection_errors"].append("ssh_failed")
    if "captured_at" not in raw:
        raw["captured_at"] = None
    return sanitize_snapshot(raw), events


def collect_live(output: Path, host: str, background: str, extractor: str, db: str, dbuser: str, dbname: str, schema: str, seconds: int, max_bundles: int, byte_limit: int) -> tuple[int, int]:
    directory = create_output_dir(output)
    deadline = time.monotonic() + min(seconds, MAX_SECONDS)
    ring: deque[dict[str, Any]] = deque(maxlen=30)
    count = size = 0
    last_poll = 0.0
    previous: dict[str, Any] | None = None
    seen: set[str] = set()
    log_cursors: dict[str, str] = {}
    while time.monotonic() < deadline and count < max_bundles:
        remaining = deadline - time.monotonic()
        wait = min(remaining, POLL_SECONDS - (time.monotonic() - last_poll))
        if last_poll and wait > 0:
            time.sleep(wait)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            snapshot, events = live_poll(host, background, extractor, db, dbuser, dbname, schema, timeout=min(12.0, remaining), log_cursors=log_cursors)
        except (OSError, subprocess.SubprocessError):
            snapshot = sanitize_snapshot({"captured_at": utc_now(), "collection_errors": ["timeout"]})
            events = []
        last_poll = time.monotonic()
        for name, current in snapshot["containers"].items():
            old_container = previous["containers"].get(name) if previous is not None else None
            same_container = old_container and old_container.get("container_id") == current.get("container_id") and old_container.get("started_at") == current.get("started_at")
            for field, delta in (("cpu_throttled_usec", "cpu_throttled_usec_delta"), ("cpu_throttled_periods", "cpu_throttled_periods_delta")):
                old = old_container.get(field) if same_container else None
                new = current.get(field)
                current[delta] = max(0, new - old) if isinstance(new, int) and isinstance(old, int) and new >= old else None
            missing_delta = "container_" + name + "_cpu_delta"
            if isinstance(current["cpu_throttled_usec_delta"], int) and isinstance(current["cpu_throttled_periods_delta"], int):
                snapshot["missing"] = [item for item in snapshot["missing"] if item != missing_delta]
            elif missing_delta not in snapshot["missing"]:
                snapshot["missing"].append(missing_delta)
        previous = snapshot
        ring.append(snapshot)
        for event in events:
            if count >= max_bundles:
                break
            fingerprint = event_fingerprint(event)
            if fingerprint in seen:
                continue
            remember_fingerprint(seen, fingerprint)
            size = write_bundle(directory, bundle(event, ring), count, size, byte_limit)
            count += 1
            if size >= byte_limit:
                break
        if size >= byte_limit:
            break
    return count, size


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, help="offline JSONL fixture (default mode)")
    parser.add_argument("--live", action="store_true", help="explicitly poll through a configured SSH host alias")
    parser.add_argument("--output", type=Path, default=Path("runtime-incidents"))
    parser.add_argument("--duration-seconds", type=int, default=MAX_SECONDS)
    parser.add_argument("--max-bundles", type=int, default=MAX_BUNDLES)
    parser.add_argument("--max-output-bytes", type=int, default=MAX_OUTPUT_BYTES)
    parser.add_argument("--db-name", help="validated PostgreSQL database name (overrides EVENING_NEWS_DB_NAME)")
    parser.add_argument("--db-schema", help="validated PostgreSQL schema name (overrides EVENING_NEWS_DB_SCHEMA)")
    args = parser.parse_args(argv)
    if not 1 <= args.duration_seconds <= MAX_SECONDS or not 1 <= args.max_bundles <= MAX_BUNDLES or not 1 <= args.max_output_bytes <= MAX_OUTPUT_BYTES:
        parser.error("duration, bundle count, or output size exceeds the hard safety bound")
    try:
        if args.live:
            host = safe_identifier(os.getenv("EVENING_NEWS_SSH_HOST", ""), "SSH host alias")
            background = safe_identifier(os.getenv("EVENING_NEWS_BACKGROUND_CONTAINER", ""), "background worker container")
            extractor = os.getenv("EVENING_NEWS_EXTRACTOR_CONTAINER", "")
            if extractor:
                extractor = safe_identifier(extractor, "extractor container")
            db = safe_identifier(os.getenv("EVENING_NEWS_DB_CONTAINER", ""), "database container")
            dbuser = safe_identifier(os.getenv("EVENING_NEWS_DB_USER", ""), "database user")
            dbname = safe_pg_identifier(args.db_name or os.getenv("EVENING_NEWS_DB_NAME", ""), "database name")
            schema = safe_pg_identifier(args.db_schema or os.getenv("EVENING_NEWS_DB_SCHEMA", ""), "database schema")
            count, size = collect_live(args.output, host, background, extractor, db, dbuser, dbname, schema, args.duration_seconds, args.max_bundles, args.max_output_bytes)
        else:
            if args.fixture is None:
                parser.error("offline mode requires --fixture; live collection requires explicit --live")
            count, size = replay_fixture(args.fixture, args.output, args.max_bundles, args.max_output_bytes, args.duration_seconds)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"collector stopped: {type(error).__name__}", file=sys.stderr)
        return 2
    print(f"bundles={count} output_bytes={size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
