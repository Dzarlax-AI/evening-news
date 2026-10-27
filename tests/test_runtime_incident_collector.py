import importlib.util
import json
import os
from collections import deque
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("runtime_incident_collector", ROOT / "scripts" / "collect_runtime_incident.py")
collector = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(collector)


class RuntimeIncidentCollectorTests(unittest.TestCase):
    def test_replay_prunes_nested_secrets_and_records_missing_evidence(self):
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        fixture = [
            {
                "kind": "snapshot",
                "snapshot": {
                    "captured_at": now,
                    "worker": {"generation": 7, "pid": 123, "heartbeat_at": now, "owner_id": "OWNER_CANARY"},
                    "pg_stat_activity": [{"pid": 99, "state": "active", "wait_type": "Lock", "wait_event": "tuple", "transaction_age_seconds": 1.25, "blocker_pids": [88], "query": "SQL_CANARY", "client_addr": "ADDRESS_CANARY"}],
                    "pg_metrics": {"wal_bytes": 100, "checkpoints_timed": 2, "checkpoints_requested": 1, "checkpoint_write_time_ms": 3, "checkpoint_sync_time_ms": 4, "buffers_checkpoint": 5, "buffers_clean": 6, "sql": "NESTED_SQL_CANARY"},
                    "host": {"memory_total_bytes": 1000, "memory_available_bytes": 500, "swap_total_bytes": 200, "swap_free_bytes": 100, "memory_psi": {"some_avg10": 0.1, "full_avg10": 0.0, "payload": "PSI_CANARY"}, "env": {"token": "TOKEN_CANARY"}},
                    "cgroup": {"working_set_bytes": 50, "cache_bytes": 10, "cpu_throttled_usec": 9, "cpu_throttled_usec_delta": 1, "cpu_periods": 3, "cpu_throttled_periods": 2, "cpu_throttled_periods_delta": 1, "container_id": "0123456789abcdef", "started_at": now, "restart_count": 0, "oom_count": 0, "env": "ENV_CANARY"},
                    "raw_logs": ["LOG_CANARY"],
                },
            },
            {
                "kind": "event",
                "event": {
                    "event": "page_monitor_failure", "event_utc": now, "request_token": "a" * 64,
                    "source_id": 41, "stage": "navigate", "reason": "navigation_dns", "elapsed_ms": 85,
                    "outcome": "failed", "error_code": "transport_failed", "url": "URL_CANARY",
                    "nested": {"credentials": "CREDENTIAL_CANARY", "html": "HTML_CANARY"},
                },
            },
            {"kind": "log", "line": "event=page_monitor_failure event_utc=" + now + " request_token=" + ("b" * 64) + " stage=attacker reason=SQL_CANARY url=URL_CANARY"},
        ]
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "fixture.jsonl"
            output = Path(temporary) / "out"
            source.write_text("\n".join(json.dumps(item) for item in fixture), encoding="utf-8")
            count, size = collector.replay_fixture(source, output, 3, collector.MAX_OUTPUT_BYTES)
            self.assertEqual(count, 2)
            self.assertLessEqual(size, collector.MAX_OUTPUT_BYTES)
            files = sorted(output.glob("incident-*/bundle-*.json"))
            self.assertEqual(len(files), 2)
            text = "\n".join(path.read_text(encoding="utf-8") for path in files)
            for canary in ("OWNER_CANARY", "SQL_CANARY", "ADDRESS_CANARY", "PSI_CANARY", "TOKEN_CANARY", "ENV_CANARY", "LOG_CANARY", "URL_CANARY", "CREDENTIAL_CANARY", "HTML_CANARY"):
                self.assertNotIn(canary, text)
            first = json.loads(files[0].read_text(encoding="utf-8"))
            self.assertEqual(first["trigger"]["source_id"], 41)
            self.assertEqual(first["current_snapshot"]["worker"]["generation"], 7)
            self.assertEqual(first["current_snapshot"]["pg_stat_activity"][0]["blocker_pids"], [88])
            self.assertEqual(first["evidence_status"]["current_snapshot"], "within_one_second")
            second = json.loads(files[1].read_text(encoding="utf-8"))
            self.assertEqual(second["trigger"]["stage"], "other")
            self.assertEqual(second["trigger"]["reason"], "other")

    def test_missing_sources_are_explicit_and_malformed_nested_values_are_safe(self):
        snapshot = collector.sanitize_snapshot({"worker": ["bad"], "pg_stat_activity": [{"pid": []}], "host": {"memory_psi": []}, "cgroup": {"cpu_periods": {"payload": "CANARY"}}})
        self.assertIn("worker", snapshot["missing"])
        self.assertIn("pg_metrics", snapshot["missing"])
        self.assertIn("host", snapshot["missing"])
        self.assertIn("container_background", snapshot["missing"])
        self.assertIsNone(snapshot["pg_stat_activity"][0]["pid"])
        self.assertIsNone(collector.sanitize_event({"event": ["malicious"]}))

    def test_database_activity_missing_is_distinct_from_observed_empty(self):
        raw, _ = collector.parse_live_output("database_error=database_unavailable")
        failed = collector.sanitize_snapshot(raw)
        self.assertIn("pg_stat_activity", failed["missing"])
        raw, _ = collector.parse_live_output('db_json=' + json.dumps({"captured_at": "2026-10-27T10:00:00Z", "pg_stat_activity": []}))
        observed_empty = collector.sanitize_snapshot(raw)
        self.assertNotIn("pg_stat_activity", observed_empty["missing"])
        self.assertEqual(observed_empty["pg_stat_activity"], [])

    def test_limits_stop_without_overwriting_existing_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "output"
            directory.mkdir()
            prior = directory / "bundle-1.json"
            prior.write_text("KEEP", encoding="utf-8")
            run = collector.create_output_dir(directory)
            with self.assertRaisesRegex(RuntimeError, "output_limit"):
                collector.write_bundle(run, {"payload": "x" * 256}, 0, 0, 32)
            self.assertEqual(prior.read_text(encoding="utf-8"), "KEEP")
            self.assertEqual(list(run.iterdir()), [])

    def test_remote_query_is_read_only_allowlisted_and_live_requires_operator_config(self):
        sql = collector.REMOTE_SCRIPT.lower()
        self.assertIn("begin read only", sql)
        self.assertIn("statement_timeout='500ms'", sql)
        self.assertIn('from \\"$schema\\".background_worker_status', sql)
        self.assertIn('"$dbname"', sql)
        self.assertIn("nullif(to_jsonb(b)->>'checkpoints_timed','')::numeric", sql)
        self.assertIn('"/proc/$hostpid/cgroup"', sql)
        self.assertIn('"/sys/fs/cgroup$cgpath"', sql)
        self.assertNotIn('docker exec "$background"', sql)
        self.assertNotIn('docker exec "$extractor"', sql)
        self.assertIn('["docker", "logs", "--since", since, "--tail", "65", container]', sql)
        self.assertIn("max_bytes = 262144", sql)
        self.assertIn("max_lines = 64", sql)
        self.assertIn("process.terminate()", sql)
        self.assertIn("set -o pipefail", sql)
        self.assertIn('print("log_cursor_" + role + "=" + boundary)', sql)
        self.assertIn('print(role + "_log_error=log_overflow")', sql)
        self.assertLess(sql.index("poll_start_utc=$(date"), sql.index("container_snapshot background"))
        self.assertIn("log_cursor_seed=%s", sql)
        self.assertNotIn('pg_stat_activity.*query', sql)
        with self.assertRaises(ValueError):
            collector.safe_identifier("host;cat ~/.ssh/id_rsa", "SSH host alias")
        with self.assertRaises(ValueError):
            collector.safe_pg_identifier("news;drop schema public", "database schema")

    def test_log_parser_converts_numeric_fields_and_rejects_bad_timestamp(self):
        now = "2026-10-27T10:00:00.123Z"
        line = f'time={now} level=ERROR msg="heartbeat failed" event=worker_heartbeat_failure event_utc={now} stage=update reason=timeout generation=7 backend_pid=123 mutex_wait_ms=4 begin_ms=5 update_ms=6 commit_ms=7'
        event = collector.parse_log_line(line)
        self.assertEqual(event["generation"], 7)
        self.assertEqual(event["backend_pid"], 123)
        self.assertEqual(event["commit_ms"], 7)
        self.assertIsNone(collector.parse_log_line("event=worker_heartbeat_failure stage=commit reason=timeout generation=7"))
        json_log = json.dumps({"time": now, "level": "ERROR", "msg": "heartbeat failed JSON_CANARY", "event": "worker_heartbeat_failure", "event_utc": now, "stage": "commit", "reason": "timeout", "sqlstate": "57014", "backend_pid": 321, "generation": 9, "mutex_wait_ms": 2, "begin_ms": 3, "update_ms": 4, "commit_ms": 5, "error": "RAW_JSON_ERROR"})
        json_event = collector.parse_log_line(json_log)
        self.assertEqual(json_event["backend_pid"], 321)
        self.assertEqual(json_event["commit_ms"], 5)
        self.assertNotIn("msg", json_event)
        self.assertIsNone(collector.parse_log_line('{"event":[],"event_utc":"' + now + '"}'))
        for reason in ("policy_denied", "unavailable"):
            self.assertEqual(collector.sanitize_event({"event": "page_monitor_failure", "event_utc": now, "reason": reason})["reason"], reason)

    def test_fake_live_runner_captures_stderr_logs_from_two_sources_and_sanitizes_before_bundle(self):
        now = "2026-10-27T10:00:00.123Z"
        event = f'log=event=page_monitor_failure event_utc={now} source=extractor request_token={"a" * 64} source_id=41 stage=navigate reason=navigation_dns elapsed_ms=85 outcome=failed error_code=transport_failed'
        output = "\n".join([
            "memtotal=4096", "memavailable=2048", "swaptotal=0", "swapfree=0", "psi_some=0.1", "psi_full=0.0",
            "background_container=" + "1" * 64, "background_started=" + now, "background_restarts=0", "background_oom=false",
            "background_memory_current=100", "background_file=20", "background_inactive_file=10", "background_periods=4", "background_throttled_periods=1", "background_throttled_usec=9", "background_oomkills=0",
            "extractor_container=" + "2" * 64, "extractor_started=" + now, "extractor_restarts=1", "extractor_oom=false",
            "postgres_container=" + "3" * 64, "postgres_started=" + now, "postgres_restarts=0", "postgres_oom=false",
            "db_json=" + json.dumps({"captured_at": "2026-10-27T09:59:50.123Z", "worker": {"generation": 7, "pid": 123, "heartbeat_at": now}, "pg_stat_activity": [], "pg_metrics": {"wal_bytes": 2, "checkpoints_timed": None}}),
            event,
        ])
        completed = type("Completed", (), {"stdout": output, "stderr": "RAW_STDERR_CANARY", "returncode": 0})()
        with patch.object(collector.subprocess, "run", return_value=completed) as runner, patch.object(collector, "utc_now", return_value="2026-10-27T10:00:01.123Z"):
            snapshot, events = collector.live_poll("news-vps", "background", "extractor", "postgres", "postgres", "aistack", "news")
        command = runner.call_args.args[0]
        remote = runner.call_args.kwargs["input"]
        self.assertEqual(command[-3:], ["news-vps", "bash", "-s"])
        self.assertIn('subprocess.Popen(["docker", "logs", "--since", since, "--tail", "65", container]', remote)
        self.assertIn('background_worker_status WHERE id=1', remote)
        self.assertIn("SCHEMA='news'", remote)
        self.assertIn('background_worker_status WHERE id=1', remote)
        self.assertNotIn("newsdb", remote)
        self.assertEqual(events[0]["source_id"], 41)
        self.assertEqual(events[0]["elapsed_ms"], 85)
        self.assertEqual(snapshot["captured_at"], "2026-10-27T09:59:50.123Z")
        status = collector.bundle(events[0], deque([snapshot]))["evidence_status"]["current_snapshot"]
        self.assertEqual(status, "late_or_missing")
        self.assertEqual(snapshot["containers"]["background"]["working_set_bytes"], 90)
        self.assertEqual(snapshot["containers"]["background"]["file_bytes"], 20)
        self.assertIn("pg_metrics", snapshot["missing"])
        self.assertNotIn("RAW_STDERR_CANARY", json.dumps(snapshot) + json.dumps(events))

    def test_remote_filter_executes_locally_with_fake_docker_and_drops_raw_logs(self):
        now = "2026-10-27T10:00:00.123Z"
        with tempfile.TemporaryDirectory() as temporary:
            fakebin = Path(temporary) / "bin"
            fakebin.mkdir()
            docker = fakebin / "docker"
            docker.write_text("""#!/bin/sh
case "$1" in
  inspect) echo "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 2026-10-27T10:00:00.000000000Z 0 false 0" ;;
  exec) printf '%s\\n' '{"worker":{"generation":7,"pid":123,"heartbeat_at":"2026-10-27T10:00:00Z"},"pg_stat_activity":[],"pg_metrics":{"wal_bytes":1}}' ;;
  logs)
    printf '%s\\n' '{"time":"2026-10-27T10:00:00.100Z","event":"page_monitor_failure","event_utc":"2026-10-27T10:00:00.100Z","request_token":[],"source_id":8,"stage":"navigate","reason":"timeout","elapsed_ms":1,"outcome":"failed","error_code":"deadline_exceeded","canary":"BAD_TOKEN_FIELD"}' >&2
    printf '%s\\n' '{"time":"2026-10-27T10:00:00.200Z","event":"worker_heartbeat_failure","event_utc":"2026-10-27T10:00:00.200Z","stage":"commit","reason":"timeout","sqlstate":{},"generation":7,"backend_pid":123,"canary":"BAD_SQLSTATE_FIELD"}'
    printf '%s\\n' 'event=page_monitor_failure event_utc=2026-10-27T10:00:00.250Z source_id=8 stage=navigate reason=policy_denied elapsed_ms=1 outcome=rejected error_code=public_url_denied'
    printf '%s\\n' 'event=page_monitor_failure event_utc=2026-10-27T10:00:00.300Z source_id=9 stage=navigate reason=unavailable elapsed_ms=1 outcome=failed error_code=unavailable'
    printf '%s\\n' 'event=page_monitor_failure event_utc=2026-10-27T10:00:00.123Z request_token=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa source_id=7 stage=navigate reason=context_timeout elapsed_ms=9 outcome=failed error_code=deadline_exceeded url=https://CANARY.invalid html=CANARY_HTML'
    printf '%s\\n' '{"time":"2026-10-27T10:00:00.456Z","level":"ERROR","msg":"heartbeat failed JSON_CANARY","event":"worker_heartbeat_failure","event_utc":"2026-10-27T10:00:00.456Z","stage":"commit","reason":"timeout","generation":7,"backend_pid":123,"sqlstate":"08006","mutex_wait_ms":2,"begin_ms":3,"update_ms":4,"commit_ms":5,"error":"CANARY_ERROR"}' >&2
    ;;
esac
""", encoding="utf-8")
            docker.chmod(0o700)
            script = (collector.REMOTE_SCRIPT.replace("@BACKGROUND@", "background").replace("@EXTRACTOR@", "extractor")
                      .replace("@DB@", "postgres").replace("@DBUSER@", "postgres").replace("@DBNAME@", "aistack").replace("@SCHEMA@", "news")
                      .replace("@LOG_CURSOR_BACKGROUND@", "").replace("@LOG_CURSOR_EXTRACTOR@", ""))
            result = collector.subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=False, env={**os.environ, "PATH": str(fakebin) + os.pathsep + os.environ["PATH"]})
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("source=background", result.stdout, result.stderr)
        self.assertIn("source=extractor", result.stdout)
        self.assertIn("event=worker_heartbeat_failure", result.stdout)
        self.assertIn("event=page_monitor_failure", result.stdout)
        self.assertIn("reason=context_timeout", result.stdout)
        self.assertIn("reason=policy_denied", result.stdout)
        self.assertIn("reason=unavailable", result.stdout)
        self.assertIn("log_cursor_background=", result.stdout)
        self.assertIn("log_cursor_extractor=", result.stdout)
        live_snapshot, _ = collector.parse_live_output(result.stdout)
        background = collector.sanitize_snapshot(live_snapshot)["containers"]["background"]
        self.assertEqual(background["container_id"], "a" * 32)
        fixture = collector.subprocess.run(
            ["sh", "-c", "printf '%s\\n' 'background_container=" + "a" * 32 + "' 'background_started=2026-10-27T10:00:00.000Z' 'background_restarts=0' 'background_oom=false' 'background_memory_current=100' 'background_file=20' 'background_inactive_file=10' 'background_periods=5' 'background_throttled_periods=2' 'background_throttled_usec=30' 'background_oomkills=1'"],
            capture_output=True, text=True, check=True,
        ).stdout
        self.assertEqual(fixture.splitlines(), [
            "background_container=" + "a" * 32, "background_started=2026-10-27T10:00:00.000Z", "background_restarts=0", "background_oom=false",
            "background_memory_current=100", "background_file=20", "background_inactive_file=10", "background_periods=5", "background_throttled_periods=2", "background_throttled_usec=30", "background_oomkills=1",
        ])
        fixture_snapshot, _ = collector.parse_live_output(fixture)
        background = collector.sanitize_snapshot(fixture_snapshot)["containers"]["background"]
        self.assertEqual(background["working_set_bytes"], 90)
        self.assertEqual(background["cache_bytes"], 20)
        self.assertEqual(background["file_bytes"], 20)
        self.assertEqual(background["cpu_periods"], 5)
        self.assertEqual(background["cpu_throttled_periods"], 2)
        self.assertEqual(background["cpu_throttled_usec"], 30)
        self.assertEqual(background["oom_count"], 1)
        for canary in ("CANARY.invalid", "CANARY_HTML", "CANARY_ERROR", "JSON_CANARY", "BAD_TOKEN_FIELD", "BAD_SQLSTATE_FIELD", "url=", "html="):
            self.assertNotIn(canary, result.stdout)

    def test_remote_log_reader_marks_65_lines_and_262145_bytes_as_overflow(self):
        with tempfile.TemporaryDirectory() as temporary:
            fakebin = Path(temporary) / "bin"
            fakebin.mkdir()
            docker = fakebin / "docker"
            docker.write_text("""#!/bin/sh
case "$1" in
  inspect) echo "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 2026-10-27T10:00:00.000000000Z 0 false 0" ;;
  exec) printf '%s\\n' '{"worker":{"generation":7,"pid":123,"heartbeat_at":"2026-10-27T10:00:00Z"},"pg_stat_activity":[],"pg_metrics":{"wal_bytes":1}}' ;;
  logs)
    if [ "$FAKE_LOG_MODE" = lines ]; then
      i=1; while [ "$i" -le 65 ]; do printf 'event=page_monitor_failure event_utc=2026-10-27T10:00:00Z request_token=%064d source_id=7 stage=navigate reason=timeout elapsed_ms=1 outcome=failed error_code=deadline_exceeded\\n' "$i"; i=$((i+1)); done
    else
      dd if=/dev/zero bs=262145 count=1 2>/dev/null
    fi
    ;;
esac
""", encoding="utf-8")
            docker.chmod(0o700)
            script = (collector.REMOTE_SCRIPT.replace("@BACKGROUND@", "background").replace("@EXTRACTOR@", "extractor")
                      .replace("@DB@", "postgres").replace("@DBUSER@", "postgres").replace("@DBNAME@", "aistack").replace("@SCHEMA@", "news")
                      .replace("@LOG_CURSOR_BACKGROUND@", "2026-10-27T10:00:00Z").replace("@LOG_CURSOR_EXTRACTOR@", "2026-10-27T10:00:00Z"))
            for mode in ("lines", "bytes"):
                result = collector.subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=False, env={**os.environ, "PATH": str(fakebin) + os.pathsep + os.environ["PATH"], "FAKE_LOG_MODE": mode})
                self.assertEqual(result.returncode, 0, result.stderr)
                cursors = {"background": "2026-10-27T10:00:00Z", "extractor": "2026-10-27T10:00:00Z"}
                collector.parse_live_output(result.stdout, cursors)
                self.assertIn("background_log_error=log_overflow", result.stdout)
                self.assertIn("extractor_log_error=log_overflow", result.stdout)
                self.assertEqual(cursors, {"background": "2026-10-27T10:00:00Z", "extractor": "2026-10-27T10:00:00Z"})
                self.assertNotIn("log_cursor_background=", result.stdout)
                self.assertNotIn("log_cursor_extractor=", result.stdout)

    def test_replay_deduplicates_events_and_skips_oversized_lines_with_private_output(self):
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        event = {"event": "worker_heartbeat_failure", "event_utc": now, "stage": "commit", "reason": "timeout", "generation": 7}
        with tempfile.TemporaryDirectory() as temporary:
            source, output = Path(temporary) / "events.jsonl", Path(temporary) / "out"
            later = datetime.fromisoformat(now.replace("Z", "+00:00")).replace(microsecond=123456).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            rows = [json.dumps({"kind": "event", "event": event}), json.dumps({"kind": "event", "event": event}), "x" * (collector.MAX_INPUT_LINE + 100), json.dumps({"kind": "event", "event": {**event, "event_utc": later}})]
            source.write_text("\n".join(rows), encoding="utf-8")
            count, _ = collector.replay_fixture(source, output, 3, collector.MAX_OUTPUT_BYTES)
            files = list(output.glob("incident-*/bundle-*.json"))
            self.assertEqual(count, 2)
            self.assertEqual(len(files), 2)
            self.assertEqual(files[0].stat().st_mode & 0o777, 0o600)
            self.assertEqual(files[0].parent.stat().st_mode & 0o777, 0o700)

    def test_replay_stops_reading_immediately_after_bundle_limit(self):
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        event_line = json.dumps({"kind": "event", "event": {"event": "worker_heartbeat_failure", "event_utc": now, "stage": "commit", "reason": "timeout", "generation": 1}}) + "\n"
        class Reader:
            def __init__(self): self.calls = 0
            def __enter__(self): return self
            def __exit__(self, *_args): return None
            def readline(self, _limit):
                self.calls += 1
                if self.calls == 1: return event_line
                raise AssertionError("replay read beyond max bundle count")

        with tempfile.TemporaryDirectory() as temporary:
            source, output = Path(temporary) / "fixture.jsonl", Path(temporary) / "out"
            source.write_text("regular fixture", encoding="utf-8")
            reader = Reader()
            original_open = Path.open
            def fixture_open(path, *args, **kwargs):
                return reader if path == source else original_open(path, *args, **kwargs)
            with patch.object(Path, "open", fixture_open):
                count, _ = collector.replay_fixture(source, output, 1, collector.MAX_OUTPUT_BYTES, 1)
            self.assertEqual(count, 1)
            self.assertEqual(reader.calls, 1)

    def test_replay_deadline_interrupts_oversized_line_drain_and_rejects_fifo(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, output = Path(temporary) / "oversized.jsonl", Path(temporary) / "out"
            source.write_text("x" * (collector.MAX_INPUT_LINE * 3) + "\n", encoding="utf-8")
            ticks = iter([0.0, 0.1, 0.2, 1.1, 1.2])
            with patch.object(collector.time, "monotonic", side_effect=lambda: next(ticks)):
                count, _ = collector.replay_fixture(source, output, 3, collector.MAX_OUTPUT_BYTES, 1)
            self.assertEqual(count, 0)
            fifo = Path(temporary) / "fixture.fifo"
            os.mkfifo(fifo)
            with self.assertRaisesRegex(ValueError, "regular file"):
                collector.replay_fixture(fifo, output, 3, collector.MAX_OUTPUT_BYTES, 1)

    def test_log_cursor_advances_only_after_success_and_recovers_events_from_gap(self):
        cursor0 = "2026-10-27T10:00:00Z"
        cursor1 = "2026-10-27T10:00:13Z"
        recovered = "log=event=page_monitor_failure event_utc=2026-10-27T10:00:12Z source=background request_token=" + "a" * 64 + " source_id=7 stage=navigate reason=timeout elapsed_ms=2 outcome=failed error_code=deadline_exceeded"
        outputs = [
            "log_cursor_seed=2026-10-27T09:59:59Z\nlog_cursor_background=" + cursor0 + "\nlog_cursor_extractor=" + cursor0,
            "background_log_error=log_unavailable\nlog_cursor_extractor=2026-10-27T10:00:02Z",
            recovered + "\nlog_cursor_background=" + cursor1,
        ]
        cursor_state: dict[str, str] = {}
        scripts: list[str] = []
        def fake_run(command, **kwargs):
            scripts.append(kwargs["input"])
            return type("Completed", (), {"stdout": outputs[len(scripts) - 1], "stderr": "", "returncode": 0})()

        with patch.object(collector.subprocess, "run", side_effect=fake_run):
            collector.live_poll("host", "background", "extractor", "db", "user", "aistack", "news", log_cursors=cursor_state)
            self.assertEqual(cursor_state["background"], cursor0)
            self.assertEqual(cursor_state["extractor"], cursor0)
            collector.live_poll("host", "background", "extractor", "db", "user", "aistack", "news", log_cursors=cursor_state)
            self.assertEqual(cursor_state["background"], cursor0)
            self.assertEqual(cursor_state["extractor"], "2026-10-27T10:00:02Z")
            _, events = collector.live_poll("host", "background", "extractor", "db", "user", "aistack", "news", log_cursors=cursor_state)
        self.assertIn("LOG_CURSOR_BACKGROUND='2026-10-27T10:00:00Z'", scripts[1])
        self.assertIn("LOG_CURSOR_BACKGROUND='2026-10-27T10:00:00Z'", scripts[2])
        self.assertIn("LOG_CURSOR_EXTRACTOR='2026-10-27T10:00:02Z'", scripts[2])
        self.assertEqual(cursor_state["background"], cursor1)
        self.assertEqual(events[0]["event_utc"], "2026-10-27T10:00:12.000Z")

    def test_live_poll_salvages_sanitized_partial_output_after_ssh_timeout(self):
        seed = "2026-10-27T10:00:00Z"
        recovered = "log=event=page_monitor_failure event_utc=2026-10-27T10:00:12Z source=background request_token=" + "a" * 64 + " source_id=7 stage=navigate reason=timeout elapsed_ms=2 outcome=failed error_code=deadline_exceeded"
        timeout = collector.subprocess.TimeoutExpired(["ssh"], 12, output=("log_cursor_seed=" + seed + "\n" + recovered + " url=RAW_TIMEOUT_CANARY").encode())
        success = type("Completed", (), {"stdout": recovered + "\nlog_cursor_background=2026-10-27T10:00:13Z", "stderr": "", "returncode": 0})()
        cursor_state: dict[str, str] = {}
        calls = []
        def fake_run(command, **kwargs):
            calls.append(kwargs["input"])
            if len(calls) == 1:
                raise timeout
            return success

        with patch.object(collector.subprocess, "run", side_effect=fake_run):
            first_snapshot, first_events = collector.live_poll("host", "background", "extractor", "db", "user", "aistack", "news", timeout=12, log_cursors=cursor_state)
            second_snapshot, second_events = collector.live_poll("host", "background", "extractor", "db", "user", "aistack", "news", timeout=12, log_cursors=cursor_state)
        self.assertEqual(cursor_state["background"], "2026-10-27T10:00:13Z")
        self.assertIn("timeout", first_snapshot["collection_errors"])
        self.assertIn("pg_stat_activity", first_snapshot["missing"])
        self.assertEqual(first_events[0]["event_utc"], "2026-10-27T10:00:12.000Z")
        self.assertEqual(second_events[0]["event_utc"], "2026-10-27T10:00:12.000Z")
        self.assertIn("LOG_CURSOR_BACKGROUND='2026-10-27T10:00:00Z'", calls[1])
        self.assertNotIn("RAW_TIMEOUT_CANARY", json.dumps(first_snapshot) + json.dumps(first_events))

    def test_live_polling_respects_remaining_deadline_deduplicates_and_resets_container_deltas(self):
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        token1, token2 = "a" * 64, "b" * 64
        event1 = collector.sanitize_event({"event": "page_monitor_failure", "event_utc": now, "request_token": token1, "source_id": 1, "stage": "navigate", "reason": "timeout", "elapsed_ms": 1, "outcome": "failed", "error_code": "deadline_exceeded"})
        event2 = collector.sanitize_event({"event": "page_monitor_failure", "event_utc": now, "request_token": token2, "source_id": 2, "stage": "navigate", "reason": "timeout", "elapsed_ms": 1, "outcome": "failed", "error_code": "deadline_exceeded"})
        event3 = collector.sanitize_event({"event": "page_monitor_failure", "event_utc": now, "request_token": "c" * 64, "source_id": 3, "stage": "navigate", "reason": "timeout", "elapsed_ms": 1, "outcome": "failed", "error_code": "deadline_exceeded"})
        calls, ticks, sleeps = [], [0.0], []

        def monotonic():
            value = ticks[0]
            ticks[0] += 0.1
            return value

        def poll(*_args, timeout, log_cursors=None):
            calls.append(timeout)
            container_id = "a" * 32 if len(calls) <= 2 else "b" * 32
            throttled_usec = 100 if len(calls) == 1 else 110 if len(calls) == 2 else 1
            snapshot = collector.sanitize_snapshot({"captured_at": now, "worker": {"generation": 1, "pid": 2, "heartbeat_at": now}, "pg_stat_activity": [], "pg_metrics": {"wal_bytes": 1, "checkpoints_timed": 1, "checkpoints_requested": 1, "checkpoint_write_time_ms": 1, "checkpoint_sync_time_ms": 1, "buffers_checkpoint": 1, "buffers_clean": 1}, "host": {"memory_total_bytes": 1, "memory_available_bytes": 1, "swap_total_bytes": 0, "swap_free_bytes": 0, "memory_psi": {"some_avg10": 0, "full_avg10": 0}}, "containers": {"background": {"container_id": container_id, "started_at": now, "restart_count": 0, "oom_count": 0, "oom_killed": False, "working_set_bytes": 1, "file_bytes": 1, "cpu_periods": 5, "cpu_throttled_periods": 3, "cpu_throttled_usec": throttled_usec}, "extractor": {}, "postgres": {}}})
            if len(calls) == 1:
                return snapshot, [event1]
            if len(calls) == 2:
                return snapshot, [event1, event2]
            return snapshot, [event1, event2, event3]

        with tempfile.TemporaryDirectory() as temporary, patch.object(collector.time, "monotonic", side_effect=monotonic), patch.object(collector.time, "sleep", side_effect=sleeps.append), patch.object(collector, "live_poll", side_effect=poll):
            count, _ = collector.collect_live(Path(temporary), "host", "background", "extractor", "db", "user", "aistack", "news", 2, 3, collector.MAX_OUTPUT_BYTES)
            files = sorted(Path(temporary).glob("incident-*/bundle-*.json"))
            second = json.loads(files[1].read_text(encoding="utf-8")) if len(files) > 1 else None
            third = json.loads(files[2].read_text(encoding="utf-8")) if len(files) > 2 else None
            first = json.loads(files[0].read_text(encoding="utf-8")) if files else None
        self.assertEqual(count, 3)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(0 < timeout <= 2 for timeout in calls))
        self.assertTrue(all(0 <= delay <= 2 for delay in sleeps))
        self.assertIn("container_background_cpu_delta", first["current_snapshot"]["missing"])
        self.assertEqual(second["current_snapshot"]["containers"]["background"]["cpu_throttled_usec_delta"], 10)
        self.assertNotIn("container_background_cpu_delta", second["current_snapshot"]["missing"])
        self.assertIn("container_background_cpu_delta", third["current_snapshot"]["missing"])
        self.assertIsNone(third["current_snapshot"]["containers"]["background"]["cpu_throttled_usec_delta"])


if __name__ == "__main__":
    unittest.main()
