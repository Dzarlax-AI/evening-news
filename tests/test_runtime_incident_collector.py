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
        self.assertIn('docker logs --since 2s --tail 64 "$name" 2>&1 | head -c 262144 | awk', sql)
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
        self.assertEqual(command[-3:], ["news-vps", "sh", "-s"])
        self.assertIn("docker logs", remote)
        self.assertIn('docker logs --since 2s --tail 64 "$name" 2>&1', remote)
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
    printf '%s\\n' 'event=page_monitor_failure event_utc=2026-10-27T10:00:00.123Z request_token=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa source_id=7 stage=navigate reason=context_timeout elapsed_ms=9 outcome=failed error_code=deadline_exceeded url=https://CANARY.invalid html=CANARY_HTML'
    printf '%s\\n' 'event=worker_heartbeat_failure event_utc=2026-10-27T10:00:00.456Z stage=commit reason=timeout generation=7 backend_pid=123 sqlstate=08006 error=CANARY_ERROR' >&2
    ;;
esac
""", encoding="utf-8")
            docker.chmod(0o700)
            script = (collector.REMOTE_SCRIPT.replace("@BACKGROUND@", "background").replace("@EXTRACTOR@", "extractor")
                      .replace("@DB@", "postgres").replace("@DBUSER@", "postgres").replace("@DBNAME@", "aistack").replace("@SCHEMA@", "news"))
            result = collector.subprocess.run(["sh", "-c", script], capture_output=True, text=True, check=False, env={**os.environ, "PATH": str(fakebin) + os.pathsep + os.environ["PATH"]})
        self.assertEqual(result.returncode, 0)
        self.assertIn("source=background", result.stdout, result.stderr)
        self.assertIn("source=extractor", result.stdout)
        self.assertIn("event=worker_heartbeat_failure", result.stdout)
        self.assertIn("event=page_monitor_failure", result.stdout)
        self.assertIn("reason=context_timeout", result.stdout)
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
        for canary in ("CANARY.invalid", "CANARY_HTML", "CANARY_ERROR", "url=", "html="):
            self.assertNotIn(canary, result.stdout)

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

    def test_live_polling_respects_remaining_deadline_deduplicates_and_resets_container_deltas(self):
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        token1, token2 = "a" * 64, "b" * 64
        event1 = collector.sanitize_event({"event": "page_monitor_failure", "event_utc": now, "request_token": token1, "source_id": 1, "stage": "navigate", "reason": "timeout", "elapsed_ms": 1, "outcome": "failed", "error_code": "deadline_exceeded"})
        event2 = collector.sanitize_event({"event": "page_monitor_failure", "event_utc": now, "request_token": token2, "source_id": 2, "stage": "navigate", "reason": "timeout", "elapsed_ms": 1, "outcome": "failed", "error_code": "deadline_exceeded"})
        calls, ticks, sleeps = [], [0.0], []

        def monotonic():
            value = ticks[0]
            ticks[0] += 0.1
            return value

        def poll(*_args, timeout):
            calls.append(timeout)
            container_id = "a" * 32 if len(calls) == 1 else "b" * 32
            snapshot = collector.sanitize_snapshot({"captured_at": now, "worker": {"generation": 1, "pid": 2, "heartbeat_at": now}, "pg_stat_activity": [], "pg_metrics": {"wal_bytes": 1, "checkpoints_timed": 1, "checkpoints_requested": 1, "checkpoint_write_time_ms": 1, "checkpoint_sync_time_ms": 1, "buffers_checkpoint": 1, "buffers_clean": 1}, "host": {"memory_total_bytes": 1, "memory_available_bytes": 1, "swap_total_bytes": 0, "swap_free_bytes": 0, "memory_psi": {"some_avg10": 0, "full_avg10": 0}}, "containers": {"background": {"container_id": container_id, "started_at": now, "restart_count": 0, "oom_count": 0, "oom_killed": False, "working_set_bytes": 1, "file_bytes": 1, "cpu_periods": 5, "cpu_throttled_periods": 3, "cpu_throttled_usec": 100 if len(calls) == 1 else 1}, "extractor": {}, "postgres": {}}})
            return snapshot, [event1] if len(calls) == 1 else [event1, event2]

        with tempfile.TemporaryDirectory() as temporary, patch.object(collector.time, "monotonic", side_effect=monotonic), patch.object(collector.time, "sleep", side_effect=sleeps.append), patch.object(collector, "live_poll", side_effect=poll):
            count, _ = collector.collect_live(Path(temporary), "host", "background", "extractor", "db", "user", "aistack", "news", 1, 3, collector.MAX_OUTPUT_BYTES)
            files = sorted(Path(temporary).glob("incident-*/bundle-*.json"))
            second = json.loads(files[1].read_text(encoding="utf-8")) if len(files) > 1 else None
        self.assertEqual(count, 2)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(0 < timeout <= 1 for timeout in calls))
        self.assertTrue(all(0 <= delay <= 1 for delay in sleeps))
        self.assertIsNone(second["current_snapshot"]["containers"]["background"]["cpu_throttled_usec_delta"])


if __name__ == "__main__":
    unittest.main()
