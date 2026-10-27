#!/usr/bin/env python3
"""Run a disposable Docker acceptance gate for the consolidated Go background runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from scripts.run_go_web_fixture_gate import GoWebGate, WEB_BACKEND_TOKEN, SCHEDULED_TOKEN
    from scripts.run_go_ingestion_fixture_gate import (
        DB_PASSWORD, TOKEN as INGESTION_TOKEN, sanitize_fixture_log,
    )
    from scripts.run_go_job_worker_fixture_gate import ARTICLE_AI_TOKEN, ARTICLE_PROCESSING_TOKEN
    from scripts.run_go_scheduler_fixture_gate import SCHEDULER_TOKEN
    from scripts.run_background_worker_fixture_gate import GateFailure
except ModuleNotFoundError:
    from run_go_web_fixture_gate import GoWebGate, WEB_BACKEND_TOKEN, SCHEDULED_TOKEN
    from run_go_ingestion_fixture_gate import (
        DB_PASSWORD, TOKEN as INGESTION_TOKEN, sanitize_fixture_log,
    )
    from run_go_job_worker_fixture_gate import ARTICLE_AI_TOKEN, ARTICLE_PROCESSING_TOKEN
    from run_go_scheduler_fixture_gate import SCHEDULER_TOKEN
    from run_background_worker_fixture_gate import GateFailure


DEFAULT_IMAGES = {
    "web": "evening-news-web:consolidation-fixture",
    "background": "evening-news-background:consolidation-fixture",
    "executor": "evening-news-executor:consolidation-fixture",
    "collector": "evening-news-collector:consolidation-fixture",
}
FIXTURE_OWNER_LABEL = "io.evening-news.background-consolidation-gate"
WORKER_DB_USER = "fixture_worker_runtime"
WORKER_DB_PASSWORD = "fixture-worker-only"
SCHEDULER_DB_USER = "fixture_scheduler_runtime"
SCHEDULER_DB_PASSWORD = "fixture-scheduler-db-only"
EXECUTION_DB_USER = "fixture_execution_runtime"
EXECUTION_DB_PASSWORD = "fixture-execution-only"
SCHEDULER_SCHEMA = "scheduler_fixture"
WORKER_DRAIN_SECONDS = 8


def parse_docker_top_pid_args(output: str) -> list[tuple[int, str]]:
    """Parse Docker Desktop/Linux `docker top -eo pid,args`, ignoring headers."""
    rows = []
    for line in output.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) == 2 and fields[0].isdigit():
            rows.append((int(fields[0]), fields[1].strip()))
    return rows


class ConsolidatedBackgroundGate(GoWebGate):
    """Reuse the full synthetic web/article flow with one composed background PID."""

    def __init__(self, images: dict[str, str], output_dir: Path, qa_hold_seconds: int = 0):
        super().__init__(output_dir)
        self.image_refs = dict(images)
        self.image = images["executor"]
        self.web_image = images["web"]
        self.background_image = images["background"]
        self.collector_image = images["collector"]
        self.qa_hold_seconds = qa_hold_seconds
        self.report.update(
            gate="go-consolidated-background-docker-fixture",
            image_tags=dict(images),
            topology={"web": 0, "background": 0, "executor": 0, "collector": 0},
            production_database_used=False,
            production_origins_used=False,
            real_ai_provider_used=False,
            real_messaging_used=False,
            external_network_egress=False,
            browser_tested=False,
            throughput_claimed=False,
            recovery_fault_injection="not_run",
        )
        self.report["image_tags"] = dict(images)

    def app_environment(self, role):
        env = super().app_environment(role)
        if role == "web":
            env["SCHEDULER_API_URL"] = "http://background:8080"
            env["SCHEDULER_API_TOKEN"] = SCHEDULER_TOKEN
        elif role == "executor":
            env["EXECUTION_SERVICE_URL"] = "http://background:8101"
            env["SCHEDULER_API_URL"] = "http://background:8080"
            env["SCHEDULER_API_TOKEN"] = SCHEDULER_TOKEN
            env["ARTICLE_INGESTION_URL"] = "http://background:8093"
            env["ARTICLE_INGESTION_TOKEN"] = INGESTION_TOKEN
            env["ARTICLE_PROCESSING_URL"] = "http://background:8094"
            env["ARTICLE_PROCESSING_TOKEN"] = ARTICLE_PROCESSING_TOKEN
            env["ARTICLE_AI_TOKEN"] = ARTICLE_AI_TOKEN
        return env

    def run_container(self, name, **kwargs):
        # The scheduler's frozen target URL resolves `web` on the internal
        # network. The inherited GoWebGate topology omitted this alias because
        # its former fake scheduler never made a real request to that target.
        if name == self.prefix + "-web":
            if not kwargs.get("alias"):
                kwargs["alias"] = "web"
            env = dict(kwargs.get("env") or {})
            trusted_hosts = [host.strip() for host in env.get("TRUSTED_HOSTS", "").split(",") if host.strip()]
            if "web" not in trusted_hosts:
                trusted_hosts.append("web")
            env["TRUSTED_HOSTS"] = ",".join(trusted_hosts)
            kwargs["env"] = env
        return super().run_container(name, **kwargs)

    def check(self, name, condition):
        if name == "explicit_python_migration_bootstrap" and not condition:
            self.capture_migration_bootstrap_diagnostics()
        return super().check(name, condition)

    def capture_migration_bootstrap_diagnostics(self):
        name = self.prefix + "-migrate"
        state_result = self.docker("inspect", "--format", "{{json .State}}", name, check=False)
        try:
            state = json.loads(state_result.stdout)
        except ValueError:
            state = {}
        logs = self.docker("logs", "--tail", "40", name, check=False)
        output_lines = sanitize_fixture_log((logs.stdout or "") + "\n" + (logs.stderr or ""))
        url_pattern = re.compile(r"(?i)(postgres(?:ql)?(?:\+\w+)?://)[^\s\"'<>]+")
        fixture_secrets = (
            DB_PASSWORD, INGESTION_TOKEN, self.collector_token, self.executor_token,
            ARTICLE_AI_TOKEN, ARTICLE_PROCESSING_TOKEN, SCHEDULER_TOKEN,
            WEB_BACKEND_TOKEN, SCHEDULED_TOKEN,
        )
        safe_lines = []
        for line in output_lines[-24:]:
            line = url_pattern.sub(r"\1[redacted]", line)
            for secret in fixture_secrets:
                line = line.replace(secret, "[redacted]")
            safe_lines.append(line[:400])
        pg_ready = self.docker(
            "exec", self.prefix + "-postgres", "pg_isready", "-U", "gate_user", "-d", "gate_db",
            check=False,
        )
        self.report["migration_bootstrap_diagnostics"] = {
            "database_scope": "disposable synthetic gate_db only",
            "postgres_ready_at_failure_capture": pg_ready.returncode == 0,
            "postgres_readiness_probe_status": pg_ready.returncode,
            "container_state": {
                "status": state.get("Status"),
                "exit_code": state.get("ExitCode"),
                "oom_killed": state.get("OOMKilled"),
                "error_present": bool(state.get("Error")),
            },
            "logs_sanitized": True,
            "log_excerpt": safe_lines,
        }

    def docker(self, *args, **kwargs):
        # The executor fixture needs one additional read-only probe mount. Keep
        # the existing backup-volume mount and all normal owner-label cleanup.
        if args and args[0] == "create" and "--name" in args:
            name = args[args.index("--name") + 1]
            if name == self.prefix + "-probe":
                mutable = list(args)
                image_index = next((i for i, part in enumerate(mutable)
                                    if part == "python:3.12-alpine"), None)
                if image_index is None:
                    raise GateFailure("fixture_probe_image_not_found")
                fixtures = Path(__file__).resolve().parents[1] / "tests/fixtures/background-worker"
                mutable[image_index:image_index] = [
                    "--mount", f"type=bind,src={fixtures},dst=/fixtures,readonly",
                ]
                args = tuple(mutable)
            if name == self.prefix + "-executor":
                mutable = list(args)
                image_ref = self.image_refs["executor"]
                image_index = next((i for i, part in enumerate(mutable) if part == image_ref), None)
                if image_index is None:
                    raise GateFailure("fixture_executor_image_not_found")
                fixture_path = Path(__file__).resolve().parents[1] / "tests/fixtures/go-background"
                mutable[image_index:image_index] = [
                    "--mount", f"type=bind,src={fixture_path},dst=/consolidated-fixtures,readonly",
                ]
                args = tuple(mutable)
        return super().docker(*args, **kwargs)

    def provision_consolidated_roles(self):
        """Create isolated fixture-only DB users for each Go component pool."""
        sql = f"""
        CREATE ROLE {WORKER_DB_USER} LOGIN PASSWORD '{WORKER_DB_PASSWORD}';
        CREATE ROLE {SCHEDULER_DB_USER} LOGIN PASSWORD '{SCHEDULER_DB_PASSWORD}';
        CREATE ROLE {EXECUTION_DB_USER} LOGIN PASSWORD '{EXECUTION_DB_PASSWORD}';
        GRANT CONNECT ON DATABASE gate_db TO {WORKER_DB_USER};
        GRANT CONNECT ON DATABASE gate_db TO {EXECUTION_DB_USER};
        GRANT USAGE ON SCHEMA public TO {WORKER_DB_USER};
        GRANT USAGE ON SCHEMA public TO {EXECUTION_DB_USER};
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
            public.background_jobs, public.background_worker_status, public.background_job_executions
            TO {WORKER_DB_USER};
        GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.background_jobs_queue_position_seq
            TO {WORKER_DB_USER};
        GRANT SELECT, UPDATE ON TABLE public.background_jobs, public.background_worker_status
            TO {EXECUTION_DB_USER};
        GRANT SELECT, INSERT, UPDATE ON TABLE public.background_job_executions
            TO {EXECUTION_DB_USER};
        GRANT SELECT, UPDATE ON TABLE public.schedule_settings, public.articles
            TO {EXECUTION_DB_USER};
        """
        result = self.docker("exec", self.prefix + "-postgres", "psql", "-v", "ON_ERROR_STOP=1",
                             "-U", "gate_user", "-d", "gate_db", "-c", sql, check=False)
        if result.returncode:
            raise GateFailure("consolidated_fixture_db_roles_not_provisioned")
        created_db = self.docker("exec", self.prefix + "-postgres", "psql", "-v", "ON_ERROR_STOP=1",
                                 "-U", "gate_user", "-d", "gate_db", "-c",
                                 f"CREATE DATABASE gate_scheduler OWNER {SCHEDULER_DB_USER}", check=False)
        if created_db.returncode:
            raise GateFailure("consolidated_fixture_scheduler_database_not_created")
        self.report["database_roles_provisioned"] = sorted((WORKER_DB_USER, EXECUTION_DB_USER, SCHEDULER_DB_USER,
                                                             "news_ingestion_runtime", "news_processing_runtime"))

    def background_environment(self):
        return {
            "WORKER_DATABASE_URL": f"postgres://{WORKER_DB_USER}:{WORKER_DB_PASSWORD}@postgres:5432/gate_db?sslmode=disable",
            "EXECUTION_DATABASE_URL": f"postgres://{EXECUTION_DB_USER}:{EXECUTION_DB_PASSWORD}@postgres:5432/gate_db?sslmode=disable",
            "EXECUTION_MAX_CONNECTIONS": "2",
            "DOMAIN_SERVICE_URL": "http://executor:8100",
            "JOB_EXECUTOR_LISTEN": ":8101",
            "SCHEDULER_DATABASE_URL": f"postgres://{SCHEDULER_DB_USER}:{SCHEDULER_DB_PASSWORD}@postgres:5432/gate_scheduler?sslmode=disable",
            "INGESTION_DATABASE_URL": f"postgres://news_ingestion_runtime:{DB_PASSWORD}@postgres:5432/gate_db?search_path=public&sslmode=disable",
            "PROCESSING_DATABASE_URL": f"postgres://news_processing_runtime:{DB_PASSWORD}@postgres:5432/gate_db?search_path=public&sslmode=disable",
            "SCHEDULER_DB_SCHEMA": SCHEDULER_SCHEMA,
            "SCHEDULER_API_TOKEN": SCHEDULER_TOKEN,
            "SCHEDULER_TARGET_ALIASES": "news-worker",
            "SCHEDULER_TARGET_NEWS_WORKER_URL": "http://web:8000/api/v1/internal/scheduled-jobs",
            "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": SCHEDULED_TOKEN,
            "SCHEDULER_ALLOW_HTTP_TARGETS": "true",
            "SCHEDULER_POLL_INTERVAL": "250ms",
            "SCHEDULER_DB_MAX_CONNS": "3",
            "SCHEDULER_LISTEN_ADDR": ":8080",
            "ARTICLE_INGESTION_TOKEN": INGESTION_TOKEN,
            "ARTICLE_INGESTION_LISTEN": ":8093",
            "ARTICLE_INGESTION_DB_MAX_CONNS": "3",
            "EXTRACTION_SERVICE_URL": "http://collector:8080",
            "EXTRACTION_SERVICE_TOKEN": self.collector_token,
            "EXTRACTION_SERVICE_NATIVE_BROWSER": "false",
            "JOB_EXECUTOR_URL": "http://background:8101",
            "JOB_EXECUTOR_TOKEN": self.executor_token,
            "SELECTOR_SERVICE_URL": "http://executor:8100",
            "ARTICLE_PROCESSING_LISTEN": ":8094",
            "ARTICLE_PROCESSING_TOKEN": ARTICLE_PROCESSING_TOKEN,
            "ARTICLE_PROCESSING_DB_MAX_CONNS": "3",
            "ARTICLE_AI_URL": "http://executor:8100",
            "ARTICLE_AI_TOKEN": ARTICLE_AI_TOKEN,
            "MAX_WORKERS": "1",
            "PROCESSING_BATCH_SIZE": "2",
            "BACKGROUND_HEALTH_LISTEN": ":8095",
            "BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS": "25",
            "BACKGROUND_WORKER_BACKOFF_MIN": "100ms",
            "BACKGROUND_WORKER_BACKOFF_MAX": "1s",
            "WORKER_POLL_INTERVAL_SECONDS": "1",
            "WORKER_SHUTDOWN_DRAIN_SECONDS": str(WORKER_DRAIN_SECONDS),
            "WORKER_MAX_JOB_ATTEMPTS": "3",
            "WORKER_JOB_RETENTION_DAYS": "7",
            "SSL_CERT_FILE": "/fixtures/fixture-ca.crt",
            "EXTRACTION_SERVICE_MAX_CONCURRENT": "1",
        }

    @property
    def collector_token(self):
        try:
            from scripts.run_go_ingestion_fixture_gate import COLLECTOR_TOKEN
        except ModuleNotFoundError:
            from run_go_ingestion_fixture_gate import COLLECTOR_TOKEN
        return COLLECTOR_TOKEN

    @property
    def executor_token(self):
        try:
            from scripts.run_go_ingestion_fixture_gate import EXECUTOR_TOKEN
        except ModuleNotFoundError:
            from run_go_ingestion_fixture_gate import EXECUTOR_TOKEN
        return EXECUTOR_TOKEN

    def start_background_components(self):
        # Start without either dependency, then bring up executor and web. The
        # background runtime must keep its listeners live while executor retries.
        self.provision_consolidated_roles()
        self.run_container(self.prefix + "-collector", command=[], alias="collector",
                           image=self.collector_image,
                           env={"LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "true",
                                "EXTRACTOR_MAX_CONCURRENT": "1", "EXTRACTOR_SERVICE_TOKEN": self.collector_token,
                                "SSL_CERT_FILE": "/fixtures/fixture-ca.crt"},
                           mounts=False, memory="192m")
        self.run_container(self.prefix + "-background", command=[], alias="background",
                           image=self.background_image, env=self.background_environment(),
                           mounts=False, memory="512m")
        self.run_container(self.prefix + "-probe", command=["sh", "-c", "sleep 86400"],
                           alias="probe", image="python:3.12-alpine",
                           env={"GATE_WEB_URL": "http://web:8000"}, memory="64m",
                           disable_healthcheck=True)
        self.wait_background_liveness()
        pre_executor_pid = self.docker("inspect", "--format", "{{.State.Pid}}", self.prefix + "-background").stdout.strip()
        self.report["checks"]["missing_executor_keeps_background_liveness"] = self.health_request("/healthz")[0] == 200
        self.report["checks"]["missing_executor_keeps_private_listeners_open"] = self.probe_status(
            "http://background:8093/healthz") == 200 and self.probe_status("http://background:8094/healthz") == 200
        self.report["checks"]["missing_executor_keeps_readiness_false"] = self.health_request("/readyz")[0] == 503
        if not all(self.report["checks"][key] for key in (
                "missing_executor_keeps_background_liveness",
                "missing_executor_keeps_private_listeners_open",
                "missing_executor_keeps_readiness_false")):
            raise GateFailure("background_did_not_wait_safely_for_executor")
        self.run_container(self.prefix + "-executor", command=[], alias="executor",
                           env=self.app_environment("executor"), memory="512m",
                           disable_healthcheck=True)
        self.wait_http(self.prefix + "-executor", "http://127.0.0.1:8100/internal/v1/status",
                       auth=self.executor_token)
        self.wait_background_ready()
        self.report["checks"]["temporary_executor_start_recovers_same_background_pid"] = (
            self.docker("inspect", "--format", "{{.State.Pid}}", self.prefix + "-background").stdout.strip()
            == pre_executor_pid)
        if not self.report["checks"]["temporary_executor_start_recovers_same_background_pid"]:
            raise GateFailure("background_process_restarted_when_executor_became_ready")
        self.report["checks"]["background_ready_before_web"] = True
        self.capture_consolidated_resources("idle_before_web")
        self.arm_outbox_while_web_is_unavailable()
        # The web service is started by the parent gate only after this returns.

    def wait_background_ready(self, timeout=100):
        deadline = time.monotonic() + timeout
        url = "http://background:8095/readyz"
        code = (
            "import urllib.request;u=" + repr(url) + ";"
            "r=urllib.request.urlopen(u,timeout=2);print(r.status)"
        )
        while time.monotonic() < deadline:
            result = self.docker("exec", self.prefix + "-executor", "python", "-c", code, check=False)
            if result.returncode == 0 and result.stdout.strip().endswith("200"):
                self.wait_background_healthcheck()
                return
            state = self.docker("inspect", "--format", "{{.State.Status}}", self.prefix + "-background",
                                check=False).stdout.strip()
            if state != "running":
                raise GateFailure("consolidated_background_exited_before_ready")
            time.sleep(1)
        raise GateFailure("consolidated_background_readiness_timeout")

    def wait_background_healthcheck(self, timeout=35):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.docker("inspect", "--format",
                                "{{if .State.Health}}{{.State.Health.Status}}{{end}}",
                                self.prefix + "-background", check=False).stdout.strip()
            if state == "healthy":
                self.report["checks"]["packaged_background_healthcheck_reaches_ready"] = True
                return
            if not state:
                raise GateFailure("background_image_healthcheck_missing")
            time.sleep(0.5)
        raise GateFailure("background_image_healthcheck_did_not_reach_healthy")

    def wait_background_liveness(self, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.probe_status("http://background:8095/healthz") == 200:
                return
            state = self.docker("inspect", "--format", "{{.State.Status}}", self.prefix + "-background",
                                check=False).stdout.strip()
            if state != "running":
                raise GateFailure("consolidated_background_exited_while_executor_unavailable")
            time.sleep(0.5)
        raise GateFailure("consolidated_background_liveness_timeout")

    def probe_status(self, url):
        code = (
            "import urllib.request,urllib.error\n"
            "u=" + repr(url) + "\n"
            "try:\n r=urllib.request.urlopen(u,timeout=2); print(r.status)\n"
            "except urllib.error.HTTPError as e:\n print(e.code)\n"
        )
        result = self.docker("exec", self.prefix + "-probe", "python", "-c", code, check=False)
        return int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip().isdigit() else 0

    def scheduler_request(self, method, path, payload=None, *, token=SCHEDULER_TOKEN):
        url = "http://background:8080" + path
        script = (
            "import json,urllib.request,urllib.error\n"
            "u=" + repr(url) + "\n"
            "m=" + repr(method) + "\n"
            "d=" + repr(json.dumps(payload, separators=(",", ":")) if payload is not None else "None") + "\n"
            "h={'Authorization':'Bearer '+" + repr(token) + ", 'Content-Type':'application/json'}\n"
            "q=urllib.request.Request(u,data=(d.encode() if d is not None else None),headers=h,method=m)\n"
            "try:\n r=urllib.request.urlopen(q,timeout=8); s=r.status; b=r.read()\n"
            "except urllib.error.HTTPError as e:\n s=e.code; b=e.read()\n"
            "print(json.dumps({'status':s,'body':json.loads(b) if b else None}))\n"
        )
        result = self.docker("exec", self.prefix + "-executor", "python", "-c", script, check=False)
        if result.returncode:
            raise GateFailure("consolidated_scheduler_request_failed")
        try:
            response = json.loads(result.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("consolidated_scheduler_response_invalid") from None
        return response["status"], response.get("body")

    def arm_outbox_while_web_is_unavailable(self):
        web_state = self.docker("inspect", self.prefix + "-web", check=False)
        if web_state.returncode == 0:
            raise GateFailure("web_started_before_background_outbox_fixture")
        status, control = self.scheduler_request("GET", "/v1/status")
        if status != 200 or not isinstance(control, dict):
            raise GateFailure("embedded_scheduler_status_unavailable_before_web")
        revision = control.get("control_revision")
        if type(revision) is not int or control.get("paused") is not False:
            raise GateFailure("embedded_scheduler_initial_control_state_invalid")
        paused_status, paused = self.scheduler_request("PUT", "/v1/control", {
            "expected_revision": revision, "paused": True,
        })
        if paused_status != 200:
            raise GateFailure("embedded_scheduler_could_not_pause_before_web_start")
        due = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat(timespec="microseconds").replace("+00:00", "Z")
        schedule = {"schedule_key": "fixture:web-unavailable", "enabled": True,
                    "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
                    "target_alias": "news-worker", "operation": "telegram_digest", "payload": {},
                    "next_due_at": due}
        imported, _ = self.scheduler_request("POST", "/v1/import", {
            "expected_control_revision": paused["revision"], "items": [schedule],
        })
        resumed, _ = self.scheduler_request("PUT", "/v1/control", {
            "expected_revision": paused["revision"], "paused": False,
        })
        if imported != 200 or resumed != 200:
            raise GateFailure("embedded_scheduler_could_not_arm_unavailable_web_occurrence")
        deadline = time.monotonic() + 20
        pending = None
        while time.monotonic() < deadline:
            code, body = self.scheduler_request("GET", "/v1/occurrences?limit=100")
            pending = next((item for item in (body or {}).get("occurrences", [])
                            if item.get("schedule_key") == "fixture:web-unavailable"), None)
            if pending and pending.get("status") == "pending" and pending.get("attempts", 0) >= 1:
                break
            time.sleep(0.25)
        still_no_web = self.docker("inspect", self.prefix + "-web", check=False).returncode != 0
        self.report["checks"]["web_unavailable_scheduler_dispatch_remains_pending"] = bool(
            pending and pending.get("status") == "pending" and pending.get("attempts", 0) >= 1 and still_no_web)
        self.report["pre_web_outbox"] = {
            "schedule_key": "fixture:web-unavailable",
            "status": pending.get("status") if pending else None,
            "attempts": pending.get("attempts") if pending else None,
            "web_container_absent": still_no_web,
        }
        if not self.report["checks"]["web_unavailable_scheduler_dispatch_remains_pending"]:
            raise GateFailure("embedded_scheduler_did_not_retain_outbox_while_web_was_unavailable")

    def verify_single_background_runtime(self):
        running = self.docker("ps", "-q", "--filter", f"label={self.owner_label}={self.owner}").stdout.splitlines()
        bg_id = self.docker("inspect", "--format", "{{.Id}}", self.prefix + "-background").stdout.strip()
        top_result = self.docker("top", self.prefix + "-background", "-eo", "pid,args")
        top_rows = parse_docker_top_pid_args(top_result.stdout)
        inspect = self.docker("inspect", "--format", "{{.Path}}|{{json .Args}}|{{.State.Pid}}|{{.RestartCount}}|{{.State.OOMKilled}}",
                              self.prefix + "-background").stdout.strip()
        fields = inspect.split("|", 4)
        host_pid = int(fields[2]) if len(fields) > 2 and fields[2].isdigit() else None
        expected_command = fields[0] if fields else ""
        matching_process = next((row for row in top_rows
                                 if host_pid is not None and row[0] == host_pid), None)
        if matching_process is None:
            matching_process = next((row for row in top_rows
                                     if row[1].split(None, 1)[0] == expected_command), None)
        init_command = matching_process[1] if matching_process else ""
        self.report["background_process"] = {
            "path": expected_command or None,
            "args": fields[1] if len(fields) > 1 else "[]",
            "host_pid": host_pid,
            "restart_count": int(fields[3]) if len(fields) > 3 and fields[3].isdigit() else None,
            "oom_killed": fields[4].lower() == "true" if len(fields) > 4 else None,
            "top_process_count": len(top_rows),
            "init_process_pid": matching_process[0] if matching_process else None,
            "init_process_matches_inspect_pid": bool(matching_process and host_pid == matching_process[0]),
            "init_process_command": init_command[:240] or None,
            "top_contains_news_background": "/news-background" in init_command,
        }
        self.report["topology"] = {"web": 1, "background": 1, "executor": 1, "collector": 1}
        product_names = {self.prefix + "-web", self.prefix + "-background",
                         self.prefix + "-executor", self.prefix + "-collector"}
        self.check("exactly_one_container_per_product_role", product_names.issubset(set(
            self.docker("ps", "--format", "{{.Names}}", "--filter",
                        f"label={self.owner_label}={self.owner}").stdout.splitlines())))
        self.check("one_background_runtime_pid1", bool(bg_id) and expected_command == "/news-background"
                   and host_pid is not None and host_pid > 0
                   and bool(matching_process) and "/news-background" in init_command
                   and (host_pid == matching_process[0] or len(top_rows) == 1))
        self.check("no_component_container_per_background_subsystem", not any(
            name.endswith(("-worker", "-scheduler", "-ingestion", "-processing"))
            for name in self.docker("ps", "--format", "{{.Names}}", "--filter",
                                    f"label={self.owner_label}={self.owner}").stdout.splitlines()))
        self.check("background_health_ready", self.health_request("/readyz")[0] == 200)
        self.check("background_health_liveness", self.health_request("/healthz")[0] == 200)
        missing_token_status, _ = self.execution_status(authenticated=False)
        status, body = self.execution_status(authenticated=True)
        exact_fields = isinstance(body, dict) and set(body) == {
            "executor_instance_id", "active_executions", "ready"
        }
        self.check("execution_status_requires_auth", missing_token_status == 401)
        self.check("execution_status_strict_ready_contract", status == 200 and exact_fields
                   and body.get("ready") is True
                   and isinstance(body.get("active_executions"), int)
                   and not isinstance(body.get("active_executions"), bool)
                   and body.get("active_executions") >= 0
                   and isinstance(body.get("executor_instance_id"), str))
        self.check("container_identity_and_pid_captured", self.report["background_process"]["host_pid"] is not None)
        return bg_id, self.report["background_process"]["host_pid"]

    def health_request(self, path):
        url = "http://background:8095" + path
        executor_state = self.docker("inspect", self.prefix + "-executor", check=False)
        probe_container = self.prefix + ("-executor" if executor_state.returncode == 0 else "-probe")
        code = (
            "import urllib.request,urllib.error\n"
            "u=" + repr(url) + "\n"
            "try:\n r=urllib.request.urlopen(u,timeout=2); print(r.status)\n"
            "except urllib.error.HTTPError as e:\n print(e.code)\n"
        )
        result = self.docker("exec", probe_container, "python", "-c", code, check=False)
        status = int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip().isdigit() else 0
        return status, {}

    def execution_status(self, *, authenticated):
        url = "http://background:8101/internal/v1/status"
        headers = {"Authorization": "Bearer " + self.executor_token} if authenticated else {}
        script = (
            "import json,urllib.request,urllib.error\n"
            "u=" + repr(url) + "\n"
            "h=" + repr(headers) + "\n"
            "q=urllib.request.Request(u,headers=h)\n"
            "try:\n r=urllib.request.urlopen(q,timeout=3); s=r.status; b=r.read()\n"
            "except urllib.error.HTTPError as e:\n s=e.code; b=e.read()\n"
            "try: body=json.loads(b) if b else None\n"
            "except Exception: body=None\n"
            "print(json.dumps({'status':s,'body':body}))\n"
        )
        result = self.docker("exec", self.prefix + "-executor", "python", "-c", script, check=False)
        if result.returncode:
            return 0, None
        try:
            response = json.loads(result.stdout.strip().splitlines()[-1])
            return int(response.get("status", 0)), response.get("body")
        except (ValueError, TypeError, IndexError, json.JSONDecodeError):
            return 0, None

    def control_fake_services(self, **changes):
        url = "http://fixture-services:8090/__gate/control"
        script = (
            "import json,urllib.request\n"
            "u=" + repr(url) + "\n"
            "d=" + repr(json.dumps(changes, separators=(",", ":"))) + "\n"
            "q=urllib.request.Request(u,data=d.encode(),headers={'Content-Type':'application/json'},method='POST')\n"
            "print(urllib.request.urlopen(q,timeout=5).status)\n"
        )
        result = self.docker("exec", self.prefix + "-executor", "python", "-c", script, check=False)
        if result.returncode or result.stdout.strip() != "200":
            raise GateFailure("fixture_provider_control_failed")

    def exec_json(self, container, *args, timeout=20):
        output = self.docker("exec", self.prefix + "-probe", "python", "/fixtures/probe.py",
                             *args, timeout=timeout).stdout.strip()
        try:
            return json.loads(output.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("fixture_api_probe_response_invalid") from None

    def wait_job_http(self, job_id, timeout=240):
        job = super().wait_job_http(job_id, timeout)
        self.report.setdefault("job_diagnostics", {})[job_id] = {
            "status": job.get("status"),
            "error_code": job.get("error_code") if isinstance(job.get("error_code"), str) else None,
            "error_type": job.get("error_type") if isinstance(job.get("error_type"), str) else None,
        }
        return job

    def pg_value(self, sql):
        result = self.docker("exec", self.prefix + "-postgres", "psql", "-U", "gate_user", "-d", "gate_db",
                             "-qAtc", sql, check=False)
        if result.returncode:
            raise GateFailure("fixture_database_probe_failed")
        return result.stdout.strip()

    def enqueue_article_reprocess(self, article_id, idempotency_key, *, force=True):
        if not str(article_id).isdigit() or not re.fullmatch(r"[a-zA-Z0-9-]{8,128}", idempotency_key):
            raise GateFailure("fixture_reprocess_parameters_invalid")
        status, body = self.request("POST", f"/api/v1/articles/{article_id}/reprocess",
                                    token=self.access_token, body={"force": force},
                                    headers={"Idempotency-Key": idempotency_key})
        job_id = body.get("job_id") if isinstance(body, dict) else None
        if status != 202 or not isinstance(job_id, str):
            raise GateFailure("fixture_reprocess_job_not_accepted")
        return job_id

    def create_fresh_sigterm_article(self):
        if not re.fullmatch(r"[0-9a-f]{12}", self.run_id):
            raise GateFailure("fixture_run_id_invalid_for_article_seed")
        source_id = self.pg_value("SELECT id FROM sources WHERE name='Go Web Gate RSS 1'")
        if not source_id.isdigit():
            raise GateFailure("fresh_sigterm_fixture_source_missing")
        marker = self.run_id
        title = f"Synthetic active SIGTERM fixture {marker}"
        url = f"https://rss.fixture.test/active-sigterm/{marker}"
        content = ("This is a synthetic fixture article created only for the disposable active shutdown test. "
                   "Its unique body ensures the article has not passed through prior processing. " * 10 + marker)
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

        def literal(value):
            return "'" + value.replace("'", "''") + "'"

        sql = (
            "INSERT INTO articles (source_id,title,url,content,fetched_at,processed,summary_processed,"
            "category_processed,ad_processed,processing_state,processing_attempts,hash_content,"
            "is_advertisement,ad_confidence,media_files) VALUES ("
            + source_id + "," + literal(title) + "," + literal(url) + "," + literal(content)
            + ",clock_timestamp() AT TIME ZONE 'UTC',false,false,false,false,'pending',0,"
            + literal(content_hash) + ",false,0,'[]'::jsonb) RETURNING id"
        )
        article_id = self.pg_value(sql)
        if not article_id.isdigit():
            raise GateFailure("fresh_sigterm_fixture_article_not_created")
        stage_state = self.pg_value(
            "SELECT processing_state || '|' || processed::text || '|' || summary_processed::text || '|' || "
            "category_processed::text || '|' || ad_processed::text FROM articles WHERE id=" + article_id
        )
        expected = "pending|false|false|false|false"
        self.check("active_sigterm_fixture_article_requires_ai", stage_state == expected)
        self.report["active_sigterm_fixture_article"] = {
            "article_id": int(article_id),
            "source_id": int(source_id),
            "synthetic_unique_content_hash": content_hash,
            "stage_state": stage_state,
        }
        return int(article_id)

    def wait_gemini_call(self, previous_count, timeout=90, job_id=None, phase="unspecified", article_id=None):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            counters = self.fake_counters_http().get("providers", {})
            count = counters.get("gemini", 0)
            if isinstance(count, int) and count > previous_count:
                return count
            time.sleep(0.25)
        current = self.fake_counters_http()
        job_status = None
        error_code = None
        error_type = None
        http_status = None
        if isinstance(job_id, str):
            http_status, job = self.request("GET", "/api/v1/jobs/" + job_id,
                                            token=self.access_token)
            if http_status == 200 and isinstance(job, dict):
                job_status = job.get("status")
                error_code = job.get("error_code") if isinstance(job.get("error_code"), str) else None
                error_type = job.get("error_type") if isinstance(job.get("error_type"), str) else None
        article_state = None
        article_error_code = None
        if isinstance(article_id, int) and article_id > 0:
            value = self.pg_value(
                "SELECT processing_state || '|' || processed::text || '|' || summary_processed::text || '|' || "
                "category_processed::text || '|' || ad_processed::text FROM articles WHERE id="
                + str(article_id)
            )
            if re.fullmatch(r"[a-z_]{1,32}\|(?:true|false)\|(?:true|false)\|(?:true|false)\|(?:true|false)", value):
                article_state = value
            raw_error_code = self.pg_value(
                "SELECT COALESCE(last_processing_error,'') FROM articles WHERE id=" + str(article_id)
            )
            if re.fullmatch(r"[a-z0-9_]{1,64}", raw_error_code):
                article_error_code = raw_error_code
        self.report.setdefault("blocked_ai_wait_diagnostics", {})[phase] = {
            "job_http_status": http_status,
            "job_status": job_status,
            "job_error_code": error_code,
            "job_error_type": error_type,
            "gemini_count_before": previous_count,
            "gemini_count_current": current.get("providers", {}).get("gemini"),
            "article_id": article_id,
            "article_stage_state": article_state,
            "article_last_processing_error": article_error_code,
        }
        raise GateFailure("fixture_job_did_not_reach_blocked_fake_ai")

    def wait_worker_generation(self, previous, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.pg_value("SELECT generation FROM background_worker_status WHERE id=1")
            if value.isdigit() and int(value) > previous:
                return int(value)
            time.sleep(0.5)
        raise GateFailure("worker_generation_did_not_advance_after_lease_loss")

    def wait_for_blocked_worker_heartbeat(self, timeout=15):
        deadline = time.monotonic() + timeout
        query = (
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname='gate_db' "
            f"AND usename='{WORKER_DB_USER}' AND wait_event_type='Lock')"
        )
        while time.monotonic() < deadline:
            if self.pg_value(query) == "t":
                return
            time.sleep(0.25)
        raise GateFailure("fixture_worker_heartbeat_did_not_wait_on_row_lock")

    def worker_lease_snapshot(self):
        value = self.pg_value(
            "SELECT generation::text || '|' || status || '|' || (owner_id IS NOT NULL)::text || '|' || "
            "COALESCE(round(EXTRACT(EPOCH FROM (clock_timestamp()-heartbeat_at))::numeric,3)::text,'') "
            "FROM background_worker_status WHERE id=1"
        )
        try:
            generation, status, owner_present, age = value.split("|", 3)
            return {
                "generation": int(generation),
                "status": status if re.fullmatch(r"[a-z_]{1,32}", status) else None,
                "owner_present": owner_present in {"t", "true"},
                "heartbeat_age_seconds": float(age) if age else None,
            }
        except (ValueError, TypeError):
            return {"generation": None, "status": None, "owner_present": None,
                    "heartbeat_age_seconds": None}

    def observe_worker_during_row_lock(self, holder, timeout=26):
        started = time.monotonic()
        deadline = started + timeout
        samples = []
        readiness_dropped = False
        while time.monotonic() < deadline and holder.poll() is None:
            readiness = self.health_request("/readyz")[0]
            lease = self.worker_lease_snapshot()
            readiness_dropped = readiness_dropped or readiness == 503
            samples.append({
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "readiness_http_status": readiness,
                **lease,
            })
            time.sleep(0.75)
        return samples[-32:], readiness_dropped

    def sanitized_worker_json_phases(self):
        result = self.docker("logs", "--since", "2m", self.prefix + "-background", check=False)
        phases = []
        details = re.compile(
            r"heartbeat stage=([a-z_]+) reason=([a-z_]+) mutex_wait_ms=(\d+) "
            r"begin_ms=(\d+) update_ms=(\d+) commit_ms=(\d+)"
        )
        for line in result.stdout.splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            message = record.get("msg")
            if not isinstance(message, str) or not any(word in message.lower() for word in ("worker", "lease", "heartbeat")):
                continue
            phase = {"msg": message[:120]}
            generation = record.get("generation")
            if type(generation) is int:
                phase["generation"] = generation
            error = record.get("error")
            match = details.search(error) if isinstance(error, str) else None
            if match:
                reason = match.group(2)
                if re.fullmatch(r"[a-z_]{1,32}", reason):
                    phase["reason"] = reason
                phase["timings_ms"] = {
                    "mutex_wait": int(match.group(3)),
                    "begin": int(match.group(4)),
                    "update": int(match.group(5)),
                    "commit": int(match.group(6)),
                }
            phases.append(phase)
        return phases[-20:]

    def record_worker_lease_fault_diagnostics(self, scenario, samples, holder):
        self.report.setdefault("worker_lease_fault_diagnostics", {})[scenario] = {
            "row_lock_samples": samples,
            "final_lease_snapshot": self.worker_lease_snapshot(),
            "sanitized_worker_json_phases": self.sanitized_worker_json_phases(),
            "row_lock_helper_exited": holder.poll() is not None,
        }

    def verify_lease_loss_recovery(self, background_id, background_pid):
        baseline = self.fake_counters_http().get("providers", {}).get("gemini", 0)
        self.control_fake_services(gemini_mode="block")
        article_id = self.pg_value("SELECT id FROM articles WHERE processed=true ORDER BY id LIMIT 1")
        job_id = self.enqueue_article_reprocess(article_id, "lease-recovery-fixture-" + self.run_id)
        self.wait_gemini_call(baseline, job_id=job_id, phase="worker_reprocess", article_id=article_id)
        before_lease = self.pg_value("SELECT generation::text || '|' || lock_backend_pid::text "
                                     "FROM background_worker_status WHERE id=1")
        try:
            generation_before, old_backend_pid = (int(value) for value in before_lease.split("|", 1))
        except (ValueError, TypeError):
            raise GateFailure("fixture_worker_lease_snapshot_invalid") from None
        command = ["docker", "exec", "--env", "WORKER_DATABASE_URL=" + self.background_environment()["WORKER_DATABASE_URL"],
                   self.prefix + "-executor", "python",
                   "/consolidated-fixtures/lock_worker_status.py", "22"]
        holder = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.wait_for_blocked_worker_heartbeat(timeout=30)
            row_lock_samples, ready_dropped = self.observe_worker_during_row_lock(holder)
            self.report["checks"]["worker_lease_loss_drops_runtime_readiness"] = ready_dropped
            if not ready_dropped:
                self.record_worker_lease_fault_diagnostics("worker_reprocess", row_lock_samples, holder)
                raise GateFailure("background_readiness_did_not_drop_during_worker_lease_loss")
        finally:
            try:
                holder.wait(timeout=30)
            except subprocess.TimeoutExpired:
                holder.kill()
                holder.wait(timeout=5)
                raise GateFailure("fixture_row_lock_helper_did_not_exit") from None
            stdout = holder.stdout.read() if holder.stdout else ""
            stderr = holder.stderr.read() if holder.stderr else ""
            if holder.returncode != 0 or "fixture_lock_acquired" not in stdout or stderr:
                raise GateFailure("fixture_row_lock_helper_failed")
        self.control_fake_services(gemini_mode="release")
        self.wait_background_ready(timeout=90)
        generation_after = self.wait_worker_generation(generation_before, timeout=90)
        after_lease = self.pg_value("SELECT generation::text || '|' || lock_backend_pid::text "
                                    "FROM background_worker_status WHERE id=1")
        try:
            _, new_backend_pid = (int(value) for value in after_lease.split("|", 1))
        except (ValueError, TypeError):
            raise GateFailure("fixture_recovered_worker_lease_snapshot_invalid") from None
        old_backend_released = self.pg_value(
            f"SELECT count(*) FROM pg_stat_activity WHERE pid={old_backend_pid}"
        ) == "0"
        current_id = self.docker("inspect", "--format", "{{.Id}}", self.prefix + "-background").stdout.strip()
        current_pid = self.docker("inspect", "--format", "{{.State.Pid}}", self.prefix + "-background").stdout.strip()
        component_codes = []
        for port, path in ((8080, "/healthz"), (8093, "/healthz"), (8094, "/healthz"),
                           (8095, "/healthz"), (8101, "/internal/v1/status")):
            url = f"http://background:{port}{path}"
            headers = "{'Authorization':'Bearer ' + " + repr(self.executor_token) + "} " if port == 8101 else "{} "
            code = ("import urllib.request;print(urllib.request.urlopen(urllib.request.Request(" + repr(url)
                    + ",headers=" + headers + "),timeout=3).status)")
            result = self.docker("exec", self.prefix + "-executor", "python", "-c", code, check=False)
            component_codes.append(int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip().isdigit() else 0)
        self.report["lease_recovery"] = {
            "generation_before": generation_before,
            "generation_after": generation_after,
            "same_container_id": current_id == background_id,
            "same_background_pid": current_pid == str(background_pid),
            "old_lease_backend_released": old_backend_released,
            "new_lease_backend_pid": new_backend_pid,
            "component_health_codes": component_codes,
        }
        self.check("lease_generation_changes_in_same_background_process",
                   generation_after > generation_before and current_id == background_id
                   and current_pid == str(background_pid)
                   and old_backend_released and new_backend_pid != old_backend_pid)
        self.check("all_background_listeners_remain_healthy_after_worker_recovery",
                   component_codes == [200, 200, 200, 200, 200])
        job = self.wait_job_http(job_id, timeout=180)
        self.check("blocked_fixture_job_finishes_after_lease_recovery", job.get("status") == "succeeded")
        self.report["recovery_fault_injection"] = "completed"

    def verify_ambiguous_delivery_recovery(self):
        """Lose the worker lease after fake Telegram accepts but before it replies."""
        self.control_fake_services(telegram_mode="accept_then_hold")
        before = self.fake_counters_http().get("providers", {}).get("telegram", 0)
        digest = self.exec_json(self.prefix + "-web", "enqueue-digest",
                                "consolidated-delivery-unknown-" + self.run_id)
        self.check("ambiguous_digest_job_accepted", digest.get("http_status") == 202)
        if digest.get("http_status") != 202 or not isinstance(digest.get("response"), dict):
            raise GateFailure("ambiguous_delivery_fixture_job_not_accepted")
        job_id = digest["response"].get("job_id")
        if not isinstance(job_id, str):
            raise GateFailure("ambiguous_delivery_fixture_job_id_missing")
        deadline = time.monotonic() + 60
        accepted_count = None
        while time.monotonic() < deadline:
            counters = self.fake_counters_http()
            count = counters.get("providers", {}).get("telegram", 0)
            if isinstance(count, int) and count > before:
                accepted_count = count
                break
            time.sleep(0.25)
        if accepted_count != before + 1:
            self.control_fake_services(telegram_mode="release")
            current_counters = self.fake_counters_http()
            status, current_job = self.request("GET", "/api/v1/jobs/" + job_id,
                                               token=self.access_token)
            self.report["ambiguous_delivery_wait_diagnostics"] = {
                "job_http_status": status,
                "job_status": current_job.get("status") if status == 200 and isinstance(current_job, dict) else None,
                "job_error_code": current_job.get("error_code") if status == 200 and isinstance(current_job, dict)
                    and isinstance(current_job.get("error_code"), str) else None,
                "job_error_type": current_job.get("error_type") if status == 200 and isinstance(current_job, dict)
                    and isinstance(current_job.get("error_type"), str) else None,
                "telegram_count_before": before,
                "telegram_count_current": current_counters.get("providers", {}).get("telegram"),
                "telegram_endpoint_counts": current_counters.get("telegram_endpoints", {}),
            }
            raise GateFailure("fake_telegram_did_not_accept_held_delivery")
        self.check("fake_telegram_accepted_delivery_before_reply", True)

        before_lease = self.pg_value("SELECT generation::text || '|' || lock_backend_pid::text "
                                     "FROM background_worker_status WHERE id=1")
        try:
            generation_before, old_backend_pid = (int(value) for value in before_lease.split("|", 1))
        except (ValueError, TypeError):
            self.control_fake_services(telegram_mode="release")
            raise GateFailure("ambiguous_delivery_worker_lease_snapshot_invalid") from None
        command = ["docker", "exec", "--env", "WORKER_DATABASE_URL=" + self.background_environment()["WORKER_DATABASE_URL"],
                   self.prefix + "-executor", "python",
                   "/consolidated-fixtures/lock_worker_status.py", "22"]
        holder = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.wait_for_blocked_worker_heartbeat(timeout=30)
            row_lock_samples, readiness_lost = self.observe_worker_during_row_lock(holder)
            if not readiness_lost:
                self.record_worker_lease_fault_diagnostics("ambiguous_delivery", row_lock_samples, holder)
            self.check("ambiguous_delivery_lease_loss_drops_readiness", readiness_lost)
        finally:
            try:
                holder.wait(timeout=30)
            except subprocess.TimeoutExpired:
                holder.kill()
                holder.wait(timeout=5)
                self.control_fake_services(telegram_mode="release")
                raise GateFailure("ambiguous_delivery_row_lock_helper_did_not_exit") from None
            stdout = holder.stdout.read() if holder.stdout else ""
            stderr = holder.stderr.read() if holder.stderr else ""
            if holder.returncode != 0 or "fixture_lock_acquired" not in stdout or stderr:
                self.control_fake_services(telegram_mode="release")
                raise GateFailure("ambiguous_delivery_row_lock_helper_failed")

        self.wait_background_ready(timeout=90)
        generation_after = self.wait_worker_generation(generation_before, timeout=90)
        lease = self.pg_value("SELECT generation::text || '|' || lock_backend_pid::text "
                              "FROM background_worker_status WHERE id=1")
        try:
            _, new_backend_pid = (int(value) for value in lease.split("|", 1))
        except (ValueError, TypeError):
            self.control_fake_services(telegram_mode="release")
            raise GateFailure("ambiguous_delivery_recovered_lease_snapshot_invalid") from None
        old_backend_released = self.pg_value(
            f"SELECT count(*) FROM pg_stat_activity WHERE pid={old_backend_pid}") == "0"
        self.check("ambiguous_delivery_old_worker_lease_replaced",
                   generation_after > generation_before and new_backend_pid != old_backend_pid
                   and old_backend_released)
        # The response is now released after the owner changed. A safe delivery
        # contract records uncertainty rather than issuing a second Telegram send.
        self.control_fake_services(telegram_mode="release")
        job = self.wait_job_http(job_id, timeout=90)
        time.sleep(2)
        after = self.fake_counters_http().get("providers", {}).get("telegram", 0)
        self.report["ambiguous_delivery"] = {
            "job_status": job.get("status"),
            "accepted_provider_calls": accepted_count - before,
            "provider_calls_after_release": after,
            "lease_generation_changed": generation_after > generation_before,
            "old_lease_backend_released": old_backend_released,
        }
        self.check("accepted_ambiguous_delivery_resolves_unknown_without_replay",
                   job.get("status") == "delivery_unknown" and after == accepted_count)

    def verify_bounded_sigterm(self):
        name = self.prefix + "-background"
        container_id = self.docker("inspect", "--format", "{{.Id}}", name).stdout.strip()
        started = time.monotonic()
        self.docker("stop", "--time", "30", name, timeout=40)
        idle_elapsed = time.monotonic() - started
        stopped = self.docker("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip()
        self.check("idle_sigterm_stops_background_cleanly_within_bound",
                   stopped == "0" and idle_elapsed < 30)
        self.docker("start", name)
        self.wait_background_ready(timeout=90)
        idle_restart_pid = self.docker("inspect", "--format", "{{.State.Pid}}", name).stdout.strip()
        self.check("idle_sigterm_restart_reuses_container_and_restarts_pid1",
                   self.docker("inspect", "--format", "{{.Id}}", name).stdout.strip() == container_id
                   and idle_restart_pid.isdigit())

        baseline = self.fake_counters_http().get("providers", {}).get("gemini", 0)
        self.control_fake_services(gemini_mode="block")
        article_id = self.create_fresh_sigterm_article()
        job_id = self.enqueue_article_reprocess(article_id, "active-term-fixture-" + self.run_id,
                                                force=False)
        self.wait_gemini_call(baseline, job_id=job_id, phase="active_sigterm", article_id=article_id)
        started = time.monotonic()
        self.docker("stop", "--time", "30", name, timeout=40)
        elapsed = time.monotonic() - started
        exit_code = self.docker("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip()
        self.control_fake_services(gemini_mode="release")
        self.check("active_sigterm_stops_background_within_shutdown_bound",
                   exit_code == "0" and elapsed < 30)
        self.docker("start", name)
        self.wait_background_ready(timeout=90)
        job = self.wait_job_http(job_id, timeout=180)
        self.check("active_sigterm_job_recovers_without_duplicate_job_identity",
                   job.get("status") == "succeeded" and job.get("job_id") == job_id)
        active_restart_pid = self.docker("inspect", "--format", "{{.State.Pid}}", name).stdout.strip()
        self.check("active_sigterm_restart_reuses_same_container",
                   self.docker("inspect", "--format", "{{.Id}}", name).stdout.strip() == container_id
                   and active_restart_pid.isdigit())
        self.report["sigterm"] = {"idle_stop_seconds": round(idle_elapsed, 3),
                                   "active_stop_seconds": round(elapsed, 3),
                                   "active_exit_code": exit_code,
                                   "intentional_container_starts_after_initial": 2,
                                   "container_id_unchanged": True,
                                   "final_background_pid": int(active_restart_pid)}

    def capture_consolidated_resources(self, label):
        names = [self.prefix + "-executor", self.prefix + "-collector"]
        for role in ("web", "background"):
            candidate = self.prefix + "-" + role
            if self.docker("inspect", candidate, check=False).returncode == 0:
                names.append(candidate)
        result = self.docker("stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}", *names,
                             check=False)
        rows = {}
        for line in result.stdout.splitlines():
            fields = line.split("\t")
            if len(fields) == 3:
                rows[fields[0].rsplit("-", 1)[-1]] = {"memory_usage": fields[1], "cpu_percent": fields[2]}
        self.report.setdefault("resource_snapshots", []).append({
            "label": label, "captured_at": datetime.now(timezone.utc).isoformat(),
            "roles": rows,
            "measurement": "point-in-time Docker stats; not peak or throughput data",
        })

    def verify_scheduler_embedded_in_runtime(self):
        missing_auth, _ = self.scheduler_request("GET", "/v1/status", token="")
        status, runtime = self.scheduler_request("GET", "/v1/status")
        self.check("scheduler_api_requires_bearer", missing_auth == 401)
        self.check("embedded_scheduler_ready", status == 200 and isinstance(runtime, dict))
        if status != 200:
            raise GateFailure("embedded_scheduler_status_unavailable")
        initial = runtime
        current_revision = initial.get("control_revision")
        paused, control = self.scheduler_request("PUT", "/v1/control", {
            "expected_revision": current_revision, "paused": True,
        })
        self.check("embedded_scheduler_pause_increments_revision", paused == 200
                   and control.get("paused") is True
                   and control.get("revision") == current_revision + 1)
        if paused != 200:
            raise GateFailure("embedded_scheduler_pause_failed")
        due = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(timespec="microseconds").replace("+00:00", "Z")
        schedule = {"schedule_key": "fixture:consolidated-due", "enabled": True,
                    "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
                    "target_alias": "news-worker", "operation": "news_processing", "payload": {},
                    "next_due_at": due}
        imported, import_body = self.scheduler_request("POST", "/v1/import", {
            "expected_control_revision": control["revision"], "items": [schedule],
        })
        replay_status, replay = self.scheduler_request("POST", "/v1/import", {
            "expected_control_revision": control["revision"], "items": [schedule],
        })
        get_status, fetched = self.scheduler_request("GET", "/v1/schedules/fixture:consolidated-due")
        self.check("paused_import_preserves_due_timestamp_and_is_idempotent", imported == 200
                   and replay_status == 200 and len(replay.get("unchanged", [])) == 1
                   and get_status == 200 and fetched.get("next_due_at") == due)
        resumed, resume_body = self.scheduler_request("PUT", "/v1/control", {
            "expected_revision": control["revision"], "paused": False,
        })
        self.check("embedded_scheduler_resume_revision", resumed == 200
                   and resume_body.get("paused") is False
                   and resume_body.get("revision") == control["revision"] + 1)
        # Exercise an actual due dispatch after web becomes available. Use a second,
        # near-future occurrence to keep the imported timestamp assertion separate.
        due_now = datetime.now(timezone.utc) + timedelta(seconds=3)
        dispatch_schedule = {**schedule, "schedule_key": "fixture:consolidated-dispatch",
                             "target_alias": "news-worker",
                             "next_due_at": due_now.isoformat(timespec="microseconds").replace("+00:00", "Z")}
        # Imports require pause. Preserve state while importing this second schedule.
        pause_status, paused_again = self.scheduler_request("PUT", "/v1/control", {
            "expected_revision": resume_body["revision"], "paused": True,
        })
        if pause_status != 200:
            raise GateFailure("embedded_scheduler_repause_failed")
        import_status, _ = self.scheduler_request("POST", "/v1/import", {
            "expected_control_revision": paused_again["revision"], "items": [dispatch_schedule],
        })
        resume_status, final_control = self.scheduler_request("PUT", "/v1/control", {
            "expected_revision": paused_again["revision"], "paused": False,
        })
        self.check("embedded_scheduler_dispatch_fixture_armed", import_status == 200 and resume_status == 200)
        deadline = time.monotonic() + 40
        occurrence = None
        occurrence_http_status = None
        last_occurrence = None
        while time.monotonic() < deadline:
            occurrence_http_status, body = self.scheduler_request("GET", "/v1/occurrences?limit=100")
            occurrence = next((item for item in (body or {}).get("occurrences", [])
                               if item.get("schedule_key") == "fixture:consolidated-dispatch"), None)
            if occurrence:
                last_occurrence = {
                    "status": occurrence.get("status"),
                    "attempts": occurrence.get("attempts"),
                    "blocked_reason": None,
                    "has_job_id": bool(occurrence.get("job_id")),
                    "target_alias": occurrence.get("target_alias"),
                }
            if occurrence and occurrence.get("status") in {"accepted", "blocked"}:
                break
            time.sleep(0.5)
        accepted = bool(occurrence and occurrence.get("status") == "accepted"
                        and occurrence.get("job_id") and occurrence.get("attempts", 0) >= 1)
        if not accepted:
            schedule_status, schedule = self.scheduler_request(
                "GET", "/v1/schedules/fixture:consolidated-dispatch")
            operational_status, operational = self.scheduler_request("GET", "/v1/status")
            # The public inventory intentionally omits retry error details. Read
            # only the bounded error code from this disposable scheduler DB.
            raw_error = self.docker(
                "exec", self.prefix + "-postgres", "psql", "-U", "gate_user", "-d", "gate_scheduler",
                "-Atc", "SELECT COALESCE(last_error_code,'') FROM " + SCHEDULER_SCHEMA
                + ".occurrences WHERE schedule_key='fixture:consolidated-dispatch' "
                "ORDER BY created_at DESC LIMIT 1", check=False,
            )
            error_code = raw_error.stdout.strip() if raw_error.returncode == 0 else ""
            if not re.fullmatch(r"[a-z0-9_]{1,64}", error_code):
                error_code = None
            if last_occurrence is not None:
                last_occurrence["blocked_reason"] = error_code
            self.report["due_dispatch_diagnostics"] = {
                "occurrence_http_status": occurrence_http_status,
                "last_occurrence": last_occurrence,
                "schedule_http_status": schedule_status,
                "schedule_enabled": schedule.get("enabled") if schedule_status == 200 and isinstance(schedule, dict) else None,
                "schedule_next_due_at": schedule.get("next_due_at") if schedule_status == 200 and isinstance(schedule, dict) else None,
                "schedule_target_alias": schedule.get("target_alias") if schedule_status == 200 and isinstance(schedule, dict) else None,
                "schedule_operation": schedule.get("operation") if schedule_status == 200 and isinstance(schedule, dict) else None,
                "operational_http_status": operational_status,
                "control_paused": operational.get("paused") if operational_status == 200 and isinstance(operational, dict) else None,
                "pending_outbox": operational.get("pending_outbox") if operational_status == 200 and isinstance(operational, dict) else None,
            }
        self.check("embedded_scheduler_due_occurrence_accepted", accepted)
        if not accepted:
            raise GateFailure("embedded_scheduler_due_occurrence_not_accepted")
        self.report["embedded_scheduler_occurrence"] = {
            "schedule_key": occurrence.get("schedule_key"), "status": occurrence.get("status"),
            "attempts": occurrence.get("attempts"), "has_job_id": bool(occurrence.get("job_id")),
        }
        _, all_occurrences = self.scheduler_request("GET", "/v1/occurrences?limit=100")
        retained = next((item for item in (all_occurrences or {}).get("occurrences", [])
                         if item.get("schedule_key") == "fixture:web-unavailable"), None)
        occurrence_id = retained.get("occurrence_id") if retained else None
        receipt_count = "0"
        if isinstance(occurrence_id, str) and re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", occurrence_id):
            receipt_count = self.pg_value(
                "SELECT count(*) FROM scheduler_occurrence_receipts WHERE occurrence_id='"
                + occurrence_id + "'::uuid"
            )
        self.report["pre_web_outbox"]["after_web"] = {
            "status": retained.get("status") if retained else None,
            "attempts": retained.get("attempts") if retained else None,
            "receipt_count": int(receipt_count) if receipt_count.isdigit() else 0,
        }
        self.check("web_unavailable_outbox_delivered_once_after_web_start",
                   retained is not None and retained.get("status") == "accepted" and receipt_count == "1")

    def verify_runtime_db_users(self):
        result = self.docker("exec", self.prefix + "-postgres", "psql", "-U", "gate_user", "-d", "gate_db",
                             "-Atc", "SELECT usename,datname,count(*) FROM pg_stat_activity "
                             "WHERE usename IN ('fixture_worker_runtime','fixture_execution_runtime','fixture_scheduler_runtime',"
                             "'news_ingestion_runtime','news_processing_runtime') "
                             "GROUP BY usename,datname ORDER BY usename,datname")
        sessions = {}
        for line in result.stdout.splitlines():
            fields = line.split("|")
            if len(fields) == 3 and fields[2].isdigit():
                sessions[fields[0]] = {"database": fields[1], "count": int(fields[2])}
        runtime_users = {WORKER_DB_USER, EXECUTION_DB_USER, SCHEDULER_DB_USER, "news_ingestion_runtime", "news_processing_runtime"}
        self.report["runtime_db_users"] = {user: sessions.get(user, {"database": None, "count": 0})
                                            for user in sorted(runtime_users)}
        self.report["runtime_db_pool_caps"] = {
            WORKER_DB_USER: 4, EXECUTION_DB_USER: 3, SCHEDULER_DB_USER: 3,
            "news_ingestion_runtime": 3, "news_processing_runtime": 3,
        }
        expected_database = {SCHEDULER_DB_USER: "gate_scheduler"}
        expected_database.update({user: "gate_db" for user in runtime_users if user != SCHEDULER_DB_USER})
        self.check("five_scoped_component_pools_use_distinct_users_and_databases", all(
            sessions.get(user, {}).get("count", 0) > 0
            and sessions[user].get("database") == expected_database[user]
            for user in runtime_users))
        self.check("execution_pool_stays_within_two_plus_guard_connections",
                   sessions.get(EXECUTION_DB_USER, {}).get("count", 0) <= 3)
        raw_env = self.docker("inspect", "--format", "{{json .Config.Env}}", self.prefix + "-background").stdout
        env_names = {entry.split("=", 1)[0] for entry in json.loads(raw_env)}
        scoped_names = {"WORKER_DATABASE_URL", "EXECUTION_DATABASE_URL", "SCHEDULER_DATABASE_URL", "INGESTION_DATABASE_URL",
                        "PROCESSING_DATABASE_URL"}
        self.report["background_database_env_names"] = sorted(scoped_names & env_names)
        self.check("background_uses_five_scoped_dsn_keys_without_generic_fallback",
                   scoped_names.issubset(env_names) and "DATABASE_URL" not in env_names)
        article_privileges = self.pg_value(
            "SELECT (has_table_privilege('" + WORKER_DB_USER + "','public.articles','SELECT') OR "
            "has_table_privilege('" + WORKER_DB_USER + "','public.articles','INSERT') OR "
            "has_table_privilege('" + WORKER_DB_USER + "','public.articles','UPDATE') OR "
            "has_table_privilege('" + WORKER_DB_USER + "','public.articles','DELETE'))::text"
        )
        self.report["worker_article_table_privileges"] = article_privileges
        self.check("worker_role_is_scoped_to_job_tables", article_privileges == "false")

    def run(self):
        super().run()
        bg_id, bg_pid = self.verify_single_background_runtime()
        self.verify_scheduler_embedded_in_runtime()
        self.verify_runtime_db_users()
        pre_recovery_identity = {
            "container_id": self.docker("inspect", "--format", "{{.Id}}", self.prefix + "-background").stdout.strip(),
            "pid": self.docker("inspect", "--format", "{{.State.Pid}}", self.prefix + "-background").stdout.strip(),
        }
        self.check("background_container_pid_stable_before_worker_only_recovery",
                   pre_recovery_identity["container_id"] == bg_id and pre_recovery_identity["pid"] == str(bg_pid))
        self.capture_consolidated_resources("load_after_web_pipeline")
        self.verify_ambiguous_delivery_recovery()
        self.verify_lease_loss_recovery(bg_id, bg_pid)
        self.capture_consolidated_resources("after_worker_recovery")
        self.verify_bounded_sigterm()
        revision_checks = {}
        for key in ("web", "background", "executor"):
            reference = self.image_refs[key]
            revision = self.docker("image", "inspect", "--format",
                                   '{{ index .Config.Labels "org.opencontainers.image.revision" }}', reference).stdout.strip()
            revision_checks[key] = revision
        self.report["image_revisions"] = revision_checks
        self.check("three_final_target_images_have_distinct_ids",
                   len({self.report["image_ids"][key] for key in ("web", "background", "executor")}) == 3)
        self.check("three_final_target_images_share_source_revision",
                   all(re.fullmatch(r"[0-9a-f]{40,64}", revision) for revision in revision_checks.values())
                   and len(set(revision_checks.values())) == 1)
        final_image_ids = {
            key: self.docker("image", "inspect", self.image_refs[key], "--format", "{{.Id}}").stdout.strip()
            for key in self.image_refs
        }
        self.report["final_image_ids"] = final_image_ids
        self.check("target_image_ids_unchanged_during_acceptance",
                   all(final_image_ids.get(key) == self.report["image_ids"].get(key)
                       for key in self.image_refs))
        final_runtime_image_id = self.docker("inspect", "--format", "{{.Image}}", self.prefix + "-background").stdout.strip()
        expected_runtime_image_id = self.report["image_ids"]["background"]
        self.report["background_container_identity_after_intentional_restarts"] = {
            "same_container_id": self.docker("inspect", "--format", "{{.Id}}", self.prefix + "-background").stdout.strip() == bg_id,
            "same_image_id": final_runtime_image_id == expected_runtime_image_id,
            "intentional_restart_count": self.report.get("sigterm", {}).get("intentional_container_starts_after_initial"),
        }
        self.check("planned_sigterm_restarts_preserve_background_container_and_image",
                   self.report["background_container_identity_after_intentional_restarts"]["same_container_id"]
                   and self.report["background_container_identity_after_intentional_restarts"]["same_image_id"]
                   and self.report["background_container_identity_after_intentional_restarts"]["intentional_restart_count"] == 2)
        self.capture_resource_snapshot()
        self.capture_consolidated_resources("final")
        self.report["resource_notes"] = "Docker stats are point-in-time snapshots; no peak, throughput, or production capacity claim."
        if self.qa_hold_seconds:
            time.sleep(min(self.qa_hold_seconds, 60))


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    for key, image in DEFAULT_IMAGES.items():
        parser.add_argument("--" + key.replace("_", "-") + "-image", dest=key + "_image", default=image,
                            help=f"Existing local {key} image. This gate never builds or publishes images.")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Directory for sanitized JSON acceptance evidence.")
    parser.add_argument("--qa-hold-seconds", type=int, default=0,
                        help="Keep the disposable stack available for up to 60 seconds after checks.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not shutil.which("docker"):
        parser.error("docker CLI is required")
    if not 0 <= args.qa_hold_seconds <= 60:
        parser.error("--qa-hold-seconds must be between 0 and 60")
    images = {key: getattr(args, key + "_image") for key in DEFAULT_IMAGES}
    gate = ConsolidatedBackgroundGate(images, args.output_dir, args.qa_hold_seconds)
    try:
        gate.run()
        gate.report["result"] = "passed"
    except Exception as exc:
        gate.report["result"] = "failed"
        gate.report["failure"] = str(exc)[:400] if isinstance(exc, GateFailure) else f"unexpected_{type(exc).__name__}"
    finally:
        try:
            gate.cleanup()
        except Exception:
            gate.report["cleanup_errors"].append("cleanup could not inspect owned fixture resources")
        fixture_temp = getattr(gate, "fixture_temp", None)
        if fixture_temp is not None:
            shutil.rmtree(fixture_temp, ignore_errors=True)
        gate.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        gate.output_dir.mkdir(parents=True, exist_ok=True)
        path = gate.output_dir / f"{gate.run_id}.json"
        path.write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
        print(path)
    return 0 if gate.report.get("result") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
