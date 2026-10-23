#!/usr/bin/env python3
"""Run an isolated Docker acceptance gate for the standalone Go scheduler."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "go-scheduler"
OWNER_LABEL = "io.evening-news.go-scheduler-gate"
ROLE_LABEL = "io.evening-news.gate-role"
SCHEDULER_TOKEN = "fixture-scheduler-only"
NEWS_TARGET_TOKEN = "fixture-news-target"
GENERIC_TOKEN = "fixture-generic-token"
NEWS_PASSWORD = "fixture-news-only"
SCHEDULER_PASSWORD = "fixture-scheduler-only"


class GateFailure(RuntimeError):
    pass


def framed_source_hash(paths: list[Path], root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(relative + b"\0")
        digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()


class DockerGate:
    def __init__(self, app_image: str, scheduler_image: str, output_dir: Path):
        self.app_image_ref = app_image
        self.scheduler_image_ref = scheduler_image
        self.output_dir = output_dir
        self.run_id = uuid.uuid4().hex[:12]
        self.owner = f"fixture-{self.run_id}"
        self.prefix = f"news-sched-gate-{self.run_id}"
        self.network = self.prefix + "-net"
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.report: dict = {
            "gate": "go-scheduler-docker-fixture",
            "run_id": self.run_id,
            "started_at": self.created_at,
            "network_internal": True,
            "real_external_services_used": False,
            "real_provider_credentials_used": False,
            "repository_mounts": False,
            "checks": {},
            "restarts": {"web": 0, "worker": 0, "scheduler": 0},
            "cleanup_errors": [],
        }
        self.pinned_images: dict[str, str] = {}
        self.container_names = {
            "postgres": self.prefix + "-postgres",
            "fake": self.prefix + "-fake",
            "scheduler": self.prefix + "-scheduler",
            "web": self.prefix + "-web",
            "worker": self.prefix + "-worker",
        }

    def docker(self, *args: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            detail = (result.stderr or result.stdout).strip().splitlines()
            safe = detail[-1] if detail else f"docker command exited {result.returncode}"
            raise GateFailure(f"docker_{args[0]}_failed: {safe[:240]}")
        return result

    def resolve_image(self, reference: str, key: str) -> str:
        if not reference or reference.startswith("-") or "\n" in reference:
            raise GateFailure(f"{key} image reference is invalid")
        result = self.docker("image", "inspect", reference, "--format", "{{.Id}} {{json .RepoDigests}}")
        parts = result.stdout.strip().split(" ", 1)
        if not parts or not parts[0].startswith("sha256:"):
            raise GateFailure(f"Could not resolve local {key} image to an immutable ID")
        self.pinned_images[key] = parts[0]
        digests = json.loads(parts[1]) if len(parts) > 1 else []
        self.report.setdefault("images", {})[key] = {
            "requested_ref": reference,
            "image_id": parts[0],
            "repo_digests": digests,
        }
        return parts[0]

    def app_source_hashes(self) -> dict[str, str]:
        source_paths = sorted(
            path.relative_to(ROOT).as_posix()
            for package in ("news_aggregator", "web")
            for path in (ROOT / package).rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
            and (package == "web" or path.suffix == ".py")
        )
        return {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in source_paths}

    def scheduler_source_hash(self) -> str:
        base = ROOT / "go-scheduler"
        if not base.is_dir():
            raise GateFailure("Go scheduler build context does not exist")
        paths = [base / "go.mod", base / "go.sum"] if (base / "go.sum").exists() else [base / "go.mod"]
        paths.extend(path for path in base.rglob("*.go") if path.is_file())
        migration_dir = base / "migrations"
        if migration_dir.exists():
            paths.extend(path for path in migration_dir.rglob("*") if path.is_file())
        return framed_source_hash(paths, base)

    def create_network(self) -> None:
        self.docker("network", "create", "--internal", "--label", f"{OWNER_LABEL}={self.owner}", self.network)
        self.report["network_name"] = self.network

    def create_container(self, name: str, image: str, command: list[str], *, role: str,
                         env: dict[str, str] | None = None, alias: str | None = None,
                         fixture_mount: bool = False, memory: str, cpus: str = "1",
                         pids_limit: int = 128, no_healthcheck: bool = False,
                         tmpfs: dict[str, str] | None = None) -> None:
        args = ["create", "--pull=never", "--name", name, "--network", self.network,
                "--label", f"{OWNER_LABEL}={self.owner}", "--label", f"{ROLE_LABEL}={role}",
                "--memory", memory, "--cpus", cpus, "--pids-limit", str(pids_limit)]
        if alias:
            args.extend(["--network-alias", alias])
        if fixture_mount:
            args.extend(["--mount", f"type=bind,src={FIXTURE_DIR},dst=/fixtures,readonly"])
        for destination, options in (tmpfs or {}).items():
            args.extend(["--tmpfs", f"{destination}:{options}"])
        if no_healthcheck:
            args.append("--no-healthcheck")
        for key, value in (env or {}).items():
            args.extend(["--env", f"{key}={value}"])
        args.extend([image, *command])
        self.docker(*args)
        self.docker("start", name)

    def app_environment(self, role: str) -> dict[str, str]:
        return {
            "DATABASE_URL": f"postgresql+asyncpg://news_role:{NEWS_PASSWORD}@postgres:5432/gate_db",
            "ADMIN_USERNAME": "fixture-admin",
            "ADMIN_PASSWORD": "fixture-admin-password",
            "JWT_SECRET": "fixture-only-jwt-signing-value",
            "GEMINI_API_KEY": "fixture-only-gemini-key",
            "GEMINI_API_ENDPOINT": "http://fixture-services:8090/v1/models",
            "SUMMARIZATION_MODEL": "gemini-2.0-flash",
            "CATEGORIZATION_MODEL": "gemini-2.0-flash",
            "DIGEST_MODEL": "gemini-2.0-flash",
            "TELEGRAM_TOKEN": "fixture-only-token",
            "TELEGRAM_CHAT_ID": "fixture-chat-id",
            "TELEGRAM_SERVICE_CHAT_ID": "fixture-service-chat-id",
            "TELEGRAM_RICH_MESSAGES_ENABLED": "false",
            "SITE_BASE_URL": "https://fixture.invalid",
            "BACKGROUND_EXECUTION_MODE": "worker",
            "SCHEDULE_DISPATCH_MODE": "service",
            "SCHEDULER_ENABLED": "true",
            "SCHEDULER_API_URL": "http://scheduler:8080",
            "SCHEDULER_API_TOKEN": SCHEDULER_TOKEN,
            "SCHEDULER_API_ALLOW_HTTP": "true",
            "SCHEDULED_JOBS_TOKEN": NEWS_TARGET_TOKEN,
            "SCHEDULER_GATE_ROLE": role,
            "WORKER_POLL_INTERVAL_SECONDS": "1",
            "WORKER_SHUTDOWN_DRAIN_SECONDS": "0",
            "SOURCE_MANAGER_MODE": "go",
            "TELEGRAM_SOURCE_MODE": "go",
            "PAGE_MONITOR_SOURCE_MODE": "go",
            "EXTRACTION_SERVICE_URL": "http://fixture-services:8090",
            "EXTRACTION_SERVICE_TOKEN": "fixture-only-token",
            "EXTRACTION_SERVICE_MODE": "go",
            "EXTRACTION_SERVICE_PYTHON_FALLBACK": "false",
            "BROWSER_WS_ENDPOINT": "",
            "ALLOW_CREATE_ALL": "true",
            "DEVELOPMENT": "true",
            "TRUST_FORWARD_AUTH": "false",
            "ALLOWED_ORIGINS": "",
            "TRUSTED_HOSTS": "localhost,127.0.0.1,web",
            "NEWS_LIMIT_MAX_ARTICLES": "2",
            "NEWS_LIMIT_PER_SOURCE": "2",
            "NEWS_LIMIT_ENABLED": "true",
            "MAX_WORKERS": "1",
            "PROCESSING_BATCH_SIZE": "2",
            "PYTHONPATH": "/fixtures:/app",
            "LOG_LEVEL": "WARNING",
            "CACHE_DIR": "/tmp/go-scheduler-gate-cache",
        }

    def probe(self, container: str, *args: str, timeout: int = 20) -> dict:
        output = self.docker("exec", container, "python", "/fixtures/probe.py", *args,
                             timeout=timeout).stdout.strip()
        try:
            return json.loads(output.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("fixture probe returned invalid JSON") from None

    def request(self, service: str, method: str, path: str, payload: dict | None = None,
                *, auth: str = "none", container: str | None = None,
                headers: dict[str, str] | None = None) -> tuple[int, dict]:
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True) if payload is not None else "-"
        args = ["request", service, method, path, auth, encoded]
        if headers:
            # Probe headers are only used for the fixed idempotency contract.
            raise GateFailure("custom headers are disabled in fixture probes")
        result = self.probe(container or self.container_names["web"], *args)
        return int(result.get("http_status", 0)), result.get("response", {})

    def fake_control(self, **changes: str) -> None:
        result = self.probe(self.container_names["web"], "fake-control", json.dumps(changes))
        if result.get("http_status") != 200:
            raise GateFailure("fake service control did not respond")

    def fake_counters(self) -> dict:
        result = self.probe(self.container_names["web"], "fake-counters")
        if result.get("http_status") != 200:
            raise GateFailure("fake service counters are unavailable")
        return result["response"]

    def wait_status(self, service: str, path: str, expected: int = 200, *, timeout: float = 90) -> dict:
        deadline = time.monotonic() + timeout
        last_status = 0
        while time.monotonic() < deadline:
            container = self.container_names.get(service)
            if container:
                running = self.docker("inspect", "--format", "{{.State.Running}}", container,
                                      check=False).stdout.strip().lower()
                if running == "false":
                    raise GateFailure(f"{service} container exited before {path} became ready")
            try:
                last_status, body = self.request(service, "GET", path,
                                                 auth="none" if path in {"/health", "/healthz", "/readyz", "/version"}
                                                 else "scheduler")
                if last_status == expected:
                    return body
            except (GateFailure, subprocess.TimeoutExpired):
                pass
            time.sleep(1)
        raise GateFailure(f"{service}{path} did not reach HTTP {expected}; last={last_status}")

    def safe_failure_diagnostics(self) -> dict:
        diagnostics = {}
        for role in ("web", "scheduler", "worker", "postgres", "fake"):
            name = self.container_names[role]
            state_result = self.docker("inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}",
                                       name, check=False)
            if state_result.returncode:
                continue
            state_parts = state_result.stdout.strip().split()
            entry = {"state": state_parts[0] if state_parts else "unknown"}
            if len(state_parts) > 1 and state_parts[1].isdigit():
                entry["exit_code"] = int(state_parts[1])
            log_result = self.docker("logs", "--tail", "250", name, check=False)
            logs = (log_result.stdout or "") + "\n" + (log_result.stderr or "")
            exceptions = set()
            frames = set()
            categories = set()
            init_errors = set()
            for line in logs.splitlines():
                if match := re.search(r"\b([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception))\b", line):
                    exceptions.add(match.group(1).split(".")[-1])
                if match := re.search(r'File "(/app/news_aggregator/[^"]+)", line (\d+)', line):
                    frames.add((match.group(1).removeprefix("/app/"), int(match.group(2))))
                lowered = line.lower()
                if "password authentication failed" in lowered:
                    categories.add("database_authentication_failed")
                if match := re.search(r"permission denied for schema ([a-z_][a-z0-9_]*)", lowered):
                    categories.add("permission_denied_schema:" + match.group(1))
                if match := re.search(r'permission denied for table "?([a-z_][a-z0-9_]*)"?', lowered):
                    categories.add("permission_denied_table:" + match.group(1))
                if match := re.search(r'relation "([a-z_][a-z0-9_]*)" does not exist', lowered):
                    categories.add("missing_relation:" + match.group(1))
                if "could not connect to server" in lowered or "connection refused" in lowered:
                    categories.add("database_connection_failed")
                if "no module named" in lowered:
                    categories.add("python_module_import_failed")
                if match := re.search(
                    r"scheduler initialization failed phase=([a-z_]+)(?: statement=(\d+))?(?: sqlstate=([0-9A-Z]{5}))?",
                    line,
                ):
                    init_errors.add((match.group(1), match.group(2), match.group(3)))
            entry["exception_types"] = sorted(exceptions)[-8:]
            entry["application_frames"] = [
                {"file": filename, "line": line_number}
                for filename, line_number in sorted(frames)[-12:]
            ]
            entry["error_categories"] = sorted(categories)
            if init_errors:
                entry["scheduler_init_errors"] = [
                    {"phase": phase, **({"statement": int(statement)} if statement else {}),
                     **({"sqlstate": sqlstate} if sqlstate else {})}
                    for phase, statement, sqlstate in sorted(init_errors)
                ]
            diagnostics[role] = entry
        return diagnostics

    def psql(self, user: str, password: str, sql: str, *, check: bool = True) -> subprocess.CompletedProcess:
        result = self.docker("exec", "--env", f"PGPASSWORD={password}", self.container_names["postgres"],
                             "psql", "-h", "127.0.0.1", "-U", user, "-d", "gate_db",
                             "-v", "ON_ERROR_STOP=1", "-At", "-c", sql, check=False)
        if check and result.returncode:
            raise GateFailure("isolated PostgreSQL fixture command failed")
        return result

    def bootstrap_roles(self) -> None:
        version = self.psql("fixture_admin", "fixture-admin-only", "SHOW server_version_num").stdout.strip()
        if not version.startswith("16"):
            raise GateFailure("fixture PostgreSQL server is not major version 16")
        self.report["postgres_server_version_num"] = version
        sql = """
        CREATE ROLE news_role LOGIN PASSWORD 'fixture-news-only';
        CREATE ROLE scheduler_role LOGIN PASSWORD 'fixture-scheduler-only';
        CREATE SCHEMA news AUTHORIZATION news_role;
        CREATE SCHEMA scheduler AUTHORIZATION scheduler_role;
        REVOKE ALL ON SCHEMA public FROM PUBLIC;
        GRANT CONNECT ON DATABASE gate_db TO news_role, scheduler_role;
        ALTER ROLE news_role IN DATABASE gate_db SET search_path TO news, public;
        ALTER ROLE scheduler_role IN DATABASE gate_db SET search_path TO scheduler;
        """
        self.psql("fixture_admin", "fixture-admin-only", sql)

    def wait_occurrence(self, key: str, *, statuses: set[str] | None = None,
                        attempts: int = 0, timeout: float = 120) -> dict:
        deadline = time.monotonic() + timeout
        last_seen = None
        while time.monotonic() < deadline:
            status, body = self.request("scheduler", "GET", "/v1/occurrences?limit=100", auth="scheduler")
            if status != 200:
                raise GateFailure("scheduler occurrence inventory request failed")
            occurrence = next((item for item in body.get("occurrences", [])
                               if item.get("schedule_key") == key), None)
            if occurrence is not None:
                last_seen = {
                    "status": occurrence.get("status"),
                    "attempts": occurrence.get("attempts"),
                    "has_job_id": bool(occurrence.get("job_id")),
                }
            if occurrence and occurrence.get("attempts", 0) >= attempts and (
                    statuses is None or occurrence.get("status") in statuses):
                return occurrence
            time.sleep(0.5)
        schedule_status, schedule = self.request(
            "scheduler", "GET", "/v1/schedules/" + key, auth="scheduler")
        status_status, operational = self.request("scheduler", "GET", "/v1/status", auth="scheduler")
        self.report.setdefault("occurrence_wait_diagnostics", {})[key] = {
            "last_seen": last_seen,
            "schedule_http_status": schedule_status,
            "schedule_enabled": schedule.get("enabled") if schedule_status == 200 else None,
            "schedule_next_due_at": schedule.get("next_due_at") if schedule_status == 200 else None,
            "operational_http_status": status_status,
            "pending_outbox": operational.get("pending_outbox") if status_status == 200 else None,
            "control_paused": operational.get("paused") if status_status == 200 else None,
        }
        raise GateFailure(f"occurrence {key} did not reach expected delivery state")

    def wait_job(self, job_id: str, *, states: set[str], timeout: float = 180) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status, body = self.request("web", "GET", f"/api/v1/jobs/{job_id}", auth="admin")
            if status == 200 and body.get("status") in states:
                return body
            if status not in {200, 0}:
                raise GateFailure("News job status request failed")
            time.sleep(0.5)
        raise GateFailure("News job did not reach the expected terminal state")

    def wait_worker(self, expected: str, *, timeout: float = 45) -> dict | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snapshot = self.probe(self.container_names["web"], "snapshot")
            worker = snapshot.get("worker")
            actual = worker.get("status", "stopped") if isinstance(worker, dict) else "stopped"
            if actual == expected:
                return worker
            time.sleep(0.5)
        return None

    @staticmethod
    def due_timestamp(seconds_ago: int = 3, microseconds: int = 728869) -> str:
        value = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
        value = value.replace(microsecond=microseconds)
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")

    def import_schedules(self, control_revision: int, items: list[dict]) -> tuple[int, dict]:
        return self.request("scheduler", "POST", "/v1/import", {
            "expected_control_revision": control_revision,
            "items": items,
        }, auth="scheduler")

    def control_revision(self) -> int:
        status, body = self.request("scheduler", "GET", "/v1/status", auth="scheduler")
        if status != 200 or not isinstance(body.get("control_revision"), int):
            raise GateFailure("could not read scheduler control revision")
        return body["control_revision"]

    def verify_database_isolation(self) -> None:
        scheduler_read_news = self.psql(
            "scheduler_role", SCHEDULER_PASSWORD,
            "SELECT count(*) FROM news.background_jobs", check=False)
        news_read_scheduler = self.psql(
            "news_role", NEWS_PASSWORD,
            "SELECT count(*) FROM scheduler.schedules", check=False)
        news_schema_usage = self.psql(
            "fixture_admin", "fixture-admin-only",
            "SELECT has_schema_privilege('scheduler_role','news','USAGE')", check=True)
        scheduler_schema_usage = self.psql(
            "fixture_admin", "fixture-admin-only",
            "SELECT has_schema_privilege('news_role','scheduler','USAGE')", check=True)
        checks = {
            "scheduler_cannot_read_news_schema": scheduler_read_news.returncode != 0,
            "news_cannot_read_scheduler_schema": news_read_scheduler.returncode != 0,
            "scheduler_role_has_no_news_schema_usage": news_schema_usage.stdout.strip() == "f",
            "news_role_has_no_scheduler_schema_usage": scheduler_schema_usage.stdout.strip() == "f",
        }
        self.report["checks"].update(checks)
        if not all(checks.values()):
            raise GateFailure("cross-service database privileges are not isolated")

    def assert_mount_and_network_boundaries(self, *, include_worker: bool = True) -> None:
        internal = self.docker("network", "inspect", "--format", "{{.Internal}}", self.network).stdout.strip()
        if internal.lower() != "true":
            raise GateFailure("fixture Docker network is not internal-only")
        self.report["checks"]["network_internal_only"] = True
        fixture_roles = ["web", "fake"]
        if include_worker:
            fixture_roles.append("worker")
        for role in fixture_roles:
            name = self.container_names[role]
            output = self.docker("inspect", "--format", "{{json .Mounts}}", name).stdout.strip()
            mounts = json.loads(output)
            if len(mounts) != 1 or Path(mounts[0]["Source"]).resolve() != FIXTURE_DIR.resolve():
                raise GateFailure(f"{role} has an unexpected filesystem mount")
            if mounts[0]["Destination"] != "/fixtures" or mounts[0]["RW"]:
                raise GateFailure(f"{role} fixture mount is not read-only")
        for role in ("scheduler", "postgres"):
            output = self.docker("inspect", "--format", "{{json .Mounts}}", self.container_names[role]).stdout.strip()
            mounts = json.loads(output)
            if role == "postgres":
                tmpfs = json.loads(self.docker(
                    "inspect", "--format", "{{json .HostConfig.Tmpfs}}", self.container_names[role]
                ).stdout.strip())
                if mounts or tmpfs != {
                    "/var/lib/postgresql/data": "rw,noexec,nosuid,size=192m"
                }:
                    raise GateFailure("postgres data is not on the bounded in-container tmpfs")
            elif mounts:
                raise GateFailure(f"{role} unexpectedly has a host filesystem mount")
        self.report["checks"]["fixture_only_read_only_mounts"] = True
        for name in self.container_names.values():
            output = self.docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", name).stdout.strip()
            ports = json.loads(output)
            if any(bindings for bindings in (ports or {}).values()):
                raise GateFailure("fixture container unexpectedly publishes a port")
        self.report["checks"]["no_published_ports"] = True

    def start_services(self) -> None:
        self.create_network()
        self.create_container(self.container_names["postgres"], self.pinned_images["postgres"], ["postgres"],
                             role="postgres", alias="postgres", memory="256m", no_healthcheck=True,
                             tmpfs={"/var/lib/postgresql/data": "rw,noexec,nosuid,size=192m"},
                             env={"POSTGRES_DB": "gate_db", "POSTGRES_USER": "fixture_admin",
                                  "POSTGRES_PASSWORD": "fixture-admin-only"})
        self.create_container(self.container_names["fake"], self.pinned_images["app"],
                             ["python", "/fixtures/fake_services.py"], role="fake",
                             alias="fixture-services", fixture_mount=True, memory="128m",
                             no_healthcheck=True)
        self.docker("exec", self.container_names["postgres"], "sh", "-lc",
                    "until pg_isready -h 127.0.0.1 -U fixture_admin -d gate_db >/dev/null 2>&1; do sleep 1; done",
                    timeout=90)
        self.bootstrap_roles()
        scheduler_env = {
            "SCHEDULER_DATABASE_URL": f"postgres://scheduler_role:{SCHEDULER_PASSWORD}@postgres:5432/gate_db?sslmode=disable",
            "SCHEDULER_DB_SCHEMA": "scheduler",
            "SCHEDULER_API_TOKEN": SCHEDULER_TOKEN,
            "SCHEDULER_TARGET_ALIASES": "news-worker,generic,blocked-consumer",
            "SCHEDULER_TARGET_NEWS_WORKER_URL": "http://web:8000/api/v1/internal/scheduled-jobs",
            "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": NEWS_TARGET_TOKEN,
            "SCHEDULER_TARGET_GENERIC_URL": "http://fixture-services:8091/commands",
            "SCHEDULER_TARGET_GENERIC_TOKEN": GENERIC_TOKEN,
            "SCHEDULER_TARGET_BLOCKED_CONSUMER_URL": "http://fixture-services:8091/commands",
            "SCHEDULER_TARGET_BLOCKED_CONSUMER_TOKEN": "fixture-wrong-token",
            "SCHEDULER_ALLOW_HTTP_TARGETS": "true",
            "SCHEDULER_START_PAUSED": "true",
            "SCHEDULER_POLL_INTERVAL": "250ms",
            "SCHEDULER_HTTP_TIMEOUT": "2s",
            "SCHEDULER_MAX_INFLIGHT": "2",
            "LISTEN_ADDR": ":8080",
        }
        self.create_container(self.container_names["scheduler"], self.pinned_images["scheduler"],
                             ["serve"], role="scheduler", alias="scheduler",
                             memory="128m", env=scheduler_env, no_healthcheck=True)
        web_env = self.app_environment("web")
        self.create_container(self.container_names["web"], self.pinned_images["app"],
                             ["uvicorn", "news_aggregator.main:app", "--host", "0.0.0.0", "--port", "8000"],
                             role="web", alias="web", fixture_mount=True, memory="256m", env=web_env,
                             no_healthcheck=True)
        self.wait_status("web", "/health")
        self.wait_status("scheduler", "/readyz")
        self.report["resource_limits"] = {
            "web_memory": "256m", "worker_memory": "512m", "scheduler_memory": "128m",
            "postgres_memory": "256m", "fake_services_memory": "128m",
            "cpus_per_container": 1, "pids_per_container": 128,
        }

    def verify_source_freshness(self) -> None:
        workspace_app_hashes = self.app_source_hashes()
        runtime_app_hashes = self.probe(self.container_names["web"], "runtime-app-hashes")
        app_image_id = self.docker("inspect", "--format", "{{.Image}}", self.container_names["web"]).stdout.strip()
        scheduler_image_id = self.docker("inspect", "--format", "{{.Image}}", self.container_names["scheduler"]).stdout.strip()
        version_status, version = self.request("scheduler", "GET", "/version", auth="none")
        expected_scheduler_hash = self.scheduler_source_hash()
        checks = {
            "web_runtime_source_matches_checkout": workspace_app_hashes == runtime_app_hashes,
            "web_container_uses_requested_immutable_image": app_image_id == self.pinned_images["app"],
            "scheduler_container_uses_requested_immutable_image": scheduler_image_id == self.pinned_images["scheduler"],
            "scheduler_build_id_matches_checkout": version_status == 200 and version.get("build_id") == expected_scheduler_hash,
        }
        self.report["runtime_freshness"] = {
            "workspace_app_files": workspace_app_hashes,
            "runtime_app_files": runtime_app_hashes,
            "expected_scheduler_build_id": expected_scheduler_hash,
            "runtime_scheduler_build_id": version.get("build_id"),
            "app_image_id": app_image_id,
            "scheduler_image_id": scheduler_image_id,
        }
        self.report["checks"].update(checks)
        if not all(checks.values()):
            raise GateFailure("runtime image/source freshness check failed")

    def direct_contract_checks(self) -> None:
        status, _ = self.request("scheduler", "GET", "/v1/status", auth="none")
        self.report["checks"]["scheduler_api_requires_bearer"] = status == 401
        if status != 401:
            raise GateFailure("scheduler API accepted a request without bearer authentication")
        status, _ = self.request("scheduler", "GET", "/v1/status", auth="target")
        self.report["checks"]["scheduler_rejects_wrong_bearer"] = status == 401
        ready = self.wait_status("scheduler", "/readyz")
        self.report["checks"]["scheduler_ready"] = ready.get("status") == "ready"

        status, created = self.request("scheduler", "PUT", "/v1/schedules/fixture:crud", {
            "expected_revision": 0, "enabled": False,
            "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 30},
            "target_alias": "generic", "operation": "fixture.crud", "payload": {},
        }, auth="scheduler")
        if status != 201 or created.get("revision") != 1:
            raise GateFailure("scheduler CRUD create did not return revision 1")
        status, listed = self.request("scheduler", "GET", "/v1/schedules?limit=100", auth="scheduler")
        status_get, got = self.request("scheduler", "GET", "/v1/schedules/fixture:crud", auth="scheduler")
        status_stale, _ = self.request("scheduler", "PUT", "/v1/schedules/fixture:crud", {
            "expected_revision": 0, "enabled": False,
            "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 30},
            "target_alias": "generic", "operation": "fixture.crud", "payload": {},
        }, auth="scheduler")
        status_update, updated = self.request("scheduler", "PUT", "/v1/schedules/fixture:crud", {
            "expected_revision": 1, "enabled": True,
            "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 30},
            "target_alias": "generic", "operation": "fixture.crud", "payload": {},
        }, auth="scheduler")
        status_delete, _ = self.request("scheduler", "DELETE", "/v1/schedules/fixture:crud",
                                        {"expected_revision": 2}, auth="scheduler")
        checks = {
            "scheduler_crud_list_get": status == 200 and status_get == 200 and
                any(item.get("schedule_key") == "fixture:crud" for item in listed.get("schedules", [])) and
                got.get("schedule_key") == "fixture:crud",
            "scheduler_revision_conflict": status_stale == 409,
            "scheduler_revision_update": status_update == 200 and updated.get("revision") == 2,
            "scheduler_crud_delete": status_delete == 204,
        }
        self.report["checks"].update(checks)
        if not all(checks.values()):
            raise GateFailure("scheduler CRUD or optimistic revision contract failed")

        # Exercise the legacy admin facade with the same dummy Basic credentials used by the fixture.
        facade_body = {
            "task_name": "news_processing", "enabled": False, "schedule_type": "interval",
            "hour": 0, "minute": 0, "weekdays": [], "timezone": "UTC",
            "task_config": {"interval_minutes": 30},
        }
        empty_facade_status, empty_facade = self.request(
            "web", "GET", "/api/v1/schedule/settings", auth="admin")
        facade_create_status, facade_created = self.request(
            "web", "POST", "/api/v1/schedule/settings", facade_body, auth="admin")
        facade_update_status, facade_updated = self.request(
            "web", "PUT", "/api/v1/schedule/settings/news_processing",
            {"enabled": False, "schedule_type": "interval", "task_config": {"interval_minutes": 60}},
            auth="admin")
        facade_delete_status, _ = self.request(
            "web", "DELETE", "/api/v1/schedule/settings/news_processing", auth="admin")
        facade_status, facade = self.request("web", "GET", "/api/v1/schedule/settings", auth="admin")
        status_projection, projection = self.request("web", "GET", "/api/v1/schedule/status", auth="admin")
        status_code, _ = self.request("web", "GET", "/api/v1/schedule/settings", auth="none")
        self.report["checks"]["news_schedule_facade_auth"] = status_code in {401, 403}
        self.report["checks"]["empty_facade_list_then_create_first_schedule"] = (
            empty_facade_status == 200 and empty_facade.get("settings") == []
            and facade_create_status == 200 and facade_created.get("success") is True
            and isinstance(facade_created.get("setting"), dict)
            and facade_created["setting"].get("next_run") is None
        )
        self.report["checks"]["news_schedule_facade_crud"] = (
            facade_create_status == 200 and facade_created.get("success") is True
            and facade_update_status == 200 and facade_updated.get("success") is True
            and facade_delete_status == 200
        )
        self.report["checks"]["news_schedule_status_projection"] = (
            status_projection == 200 and isinstance(projection, dict)
        )
        if (facade_status != 200 or status_code not in {401, 403}
                or not self.report["checks"]["empty_facade_list_then_create_first_schedule"]
                or not self.report["checks"]["news_schedule_facade_crud"]
                or not self.report["checks"]["news_schedule_status_projection"]):
            raise GateFailure("News scheduler facade compatibility/authentication failed")
        self.report["facade_initial_keys"] = sorted(
            item.get("task_name", "") for item in facade.get("settings", [])
            if isinstance(item, dict) and isinstance(item.get("task_name"), str)
        )

    def prepare_and_dispatch(self) -> dict[str, dict]:
        seed = self.docker("exec", self.container_names["web"], "python", "/fixtures/seed_fixture.py").stdout.strip()
        try:
            self.report["fixture_seed"] = json.loads(seed.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("synthetic News fixture data could not be seeded") from None
        self.verify_database_isolation()

        due = self.due_timestamp(seconds_ago=4, microseconds=728869)
        items = [
            {"schedule_key": "news:news_processing", "enabled": True,
             "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
             "target_alias": "news-worker", "operation": "news_processing", "payload": {}, "next_due_at": due},
            {"schedule_key": "fixture:generic", "enabled": True,
             "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
             "target_alias": "generic", "operation": "fixture.echo", "payload": {"message": "fixture-only"},
             "next_due_at": due},
            {"schedule_key": "fixture:blocked", "enabled": True,
             "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
             "target_alias": "blocked-consumer", "operation": "fixture.blocked", "payload": {},
             "next_due_at": due},
            {"schedule_key": "fixture:telegram", "enabled": True,
             "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
             "target_alias": "news-worker", "operation": "telegram_digest", "payload": {},
             "next_due_at": self.due_timestamp(seconds_ago=5, microseconds=317001)},
        ]
        status, imported = self.import_schedules(0, items)
        if status != 200 or len(imported.get("created", [])) != len(items):
            raise GateFailure("paused legacy schedule import did not create the fixture schedules")
        imported_again_status, imported_again = self.import_schedules(0, items)
        self.report["checks"]["import_is_idempotent"] = (
            imported_again_status == 200 and len(imported_again.get("unchanged", [])) == len(items)
        )
        self.report["checks"]["import_preserves_microseconds_while_paused"] = True
        for item in items:
            status, resource = self.request("scheduler", "GET", "/v1/schedules/" + item["schedule_key"], auth="scheduler")
            self.report["checks"]["import_preserves_microseconds_while_paused"] &= (
                status == 200 and resource.get("next_due_at") == item["next_due_at"]
            )
        if not self.report["checks"]["import_is_idempotent"] or not self.report["checks"]["import_preserves_microseconds_while_paused"]:
            raise GateFailure("schedule import was not idempotent or lost timestamp precision")

        self.fake_control(consumer_mode="malformed")
        status, control = self.request("scheduler", "PUT", "/v1/control",
                                       {"expected_revision": 0, "paused": False}, auth="scheduler")
        if status != 200 or control.get("paused") is not False:
            raise GateFailure("scheduler could not be resumed after import")

        # Leave the real News worker stopped long enough to observe receiver backpressure.
        news_pending = self.wait_occurrence("news:news_processing", statuses={"pending"}, attempts=1)
        worker_down = self.wait_worker("stopped", timeout=2)
        self.report["checks"]["worker_down_503_keeps_scheduler_outbox_pending"] = (
            news_pending.get("status") == "pending" and news_pending.get("attempts", 0) >= 1
            and worker_down is not None
        )
        generic_first = self.wait_occurrence("fixture:generic", statuses={"pending"}, attempts=1)
        self.report["checks"]["malformed_receipt_is_not_acknowledged"] = generic_first.get("status") == "pending"
        blocked = self.wait_occurrence("fixture:blocked", statuses={"blocked"}, attempts=1)
        self.report["checks"]["invalid_target_token_becomes_blocked"] = blocked.get("status") == "blocked"
        if not all(self.report["checks"][name] for name in (
                "worker_down_503_keeps_scheduler_outbox_pending", "malformed_receipt_is_not_acknowledged",
                "invalid_target_token_becomes_blocked")):
            raise GateFailure("scheduler retry or terminal blocked-state behavior failed")

        self.fake_control(consumer_mode="normal")
        self.create_container(self.container_names["worker"], self.pinned_images["app"],
                             ["python", "-m", "news_aggregator.worker"], role="worker",
                             fixture_mount=True, memory="512m", env=self.app_environment("worker"),
                             no_healthcheck=True)
        self.assert_mount_and_network_boundaries()
        if self.wait_worker("running") is None:
            raise GateFailure("fixture News worker did not acquire its durable lease")
        news_occurrence = self.wait_occurrence("news:news_processing", statuses={"accepted"}, attempts=2)
        generic_occurrence = self.wait_occurrence("fixture:generic", statuses={"accepted"}, attempts=2)
        telegram_occurrence = self.wait_occurrence("fixture:telegram", statuses={"accepted"}, attempts=1)
        self.report["checks"]["second_generic_consumer_accepted_opaque_operation"] = (
            generic_occurrence.get("job_id") is not None and
            generic_occurrence.get("status") == "accepted"
        )
        generic_counters = self.fake_counters().get("consumer", {})
        self.report["generic_consumer"] = generic_counters
        self.report["checks"]["generic_consumer_replay_deduplicated"] = (
            generic_counters.get("occurrences") == 1 and
            generic_counters.get("operations", {}).get("fixture.echo") == 1
        )
        if not self.report["checks"]["generic_consumer_replay_deduplicated"]:
            raise GateFailure("generic consumer retry created more than one logical operation")

        news_job_id = news_occurrence.get("job_id")
        if not news_job_id:
            raise GateFailure("accepted News occurrence did not return a durable job id")
        news_job = self.wait_job(news_job_id, states={"succeeded", "failed"})
        if news_job.get("status") != "succeeded":
            raise GateFailure("News fixture job failed")
        telegram_job_id = telegram_occurrence.get("job_id")
        if not telegram_job_id:
            raise GateFailure("accepted Telegram occurrence did not return a durable job id")
        telegram_job = self.wait_job(telegram_job_id, states={"succeeded", "failed"})
        if telegram_job.get("status") != "succeeded":
            raise GateFailure("synthetic Telegram digest job failed")
        self.report["checks"]["news_adapter_receipt_matches_completed_job"] = True
        self.report["news_job"] = {"job_id": news_job_id, "kind": news_job.get("kind"), "status": news_job.get("status")}
        self.report["news_occurrence"] = news_occurrence
        self.report["generic_occurrence"] = generic_occurrence
        self.report["telegram_occurrence"] = telegram_occurrence
        counters = self.fake_counters()
        providers = counters.get("providers", {})
        self.report["fake_provider_calls"] = providers
        self.report["checks"]["fake_go_and_gemini_exercised"] = (
            providers.get("source_collection", 0) > 0 and providers.get("gemini", 0) > 0
        )
        self.report["checks"]["fake_telegram_exercised"] = providers.get("telegram", 0) > 0
        if not self.report["checks"]["fake_go_and_gemini_exercised"]:
            raise GateFailure("scheduled News processing did not use both fake Go and Gemini endpoints")
        if not self.report["checks"]["fake_telegram_exercised"]:
            raise GateFailure("scheduled Telegram digest did not use the fake Telegram endpoint")
        self.report["checks"]["service_mode_does_not_write_legacy_schedule_rows"] = (
            not self.probe(self.container_names["web"], "snapshot").get("legacy_schedule_rows")
        )
        if not self.report["checks"]["service_mode_does_not_write_legacy_schedule_rows"]:
            raise GateFailure("service-mode worker wrote legacy schedule_settings rows")
        return {"news": news_occurrence, "generic": generic_occurrence, "telegram": telegram_occurrence}

    def verify_capacity_and_scheduler_restart(self) -> None:
        # Hold one worker operation in the fake AI endpoint, then fill the bounded News queue.
        self.fake_control(gemini_mode="block")
        before = self.fake_counters().get("providers", {}).get("gemini", 0)
        blocker = self.probe(self.container_names["web"], "enqueue-gemini-blocker")
        status, blocker_body = int(blocker.get("http_status", 0)), blocker.get("response", {})
        if status != 202 or not blocker_body.get("job_id"):
            raise GateFailure("could not enqueue isolated article-reprocess queue blocker")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            current = self.fake_counters().get("providers", {}).get("gemini", 0)
            if current > before:
                break
            time.sleep(0.5)
        else:
            self.report["checks"]["controlled_fake_gemini_backpressure"] = False
            raise GateFailure("synthetic worker did not enter the controlled fake Gemini wait")
        self.report["checks"]["controlled_fake_gemini_backpressure"] = True
        blocker_state = self.probe(self.container_names["web"], "snapshot")
        running_blockers = [job for job in blocker_state.get("jobs", [])
                            if job.get("id") == blocker_body.get("job_id")
                            and job.get("state") == "running"]
        if len(running_blockers) != 1:
            raise GateFailure("synthetic Gemini blocker was not holding the single worker slot")
        worker_status = blocker_state.get("worker") or {}
        heartbeat_at = worker_status.get("heartbeat_at")
        if not heartbeat_at:
            raise GateFailure("worker heartbeat missing before queue-capacity fixture")
        self.report["capacity_worker_state"] = {
            "status": worker_status.get("status"),
            "heartbeat_at": heartbeat_at,
            "worker_remained_running": True,
        }
        saturation_started = time.monotonic()
        try:
            batch = self.probe(self.container_names["web"], "saturate-batch", "32")
            accepted_count = int(batch.get("accepted", 0))
            self.report["saturation_batch"] = {"accepted": accepted_count,
                                                "response_count": len(batch.get("statuses", [])),
                                                "unique_job_ids": batch.get("unique_job_ids", 0),
                                                "job_ids": batch.get("job_ids", [])}
            if accepted_count != 32 or batch.get("unique_job_ids") != 32:
                raise GateFailure("could not fill the News queue to its configured pending-job cap")
            queue_snapshot = self.probe(self.container_names["web"], "snapshot")
            self.report["queue_at_capacity"] = queue_snapshot.get("job_counts", {})
            fixture_job_ids = set(batch.get("job_ids", []))
            fixture_rows = [
                {"id": row.get("id"), "kind": row.get("kind"), "state": row.get("state")}
                for row in queue_snapshot.get("jobs", []) if row.get("id") in fixture_job_ids
            ]
            self.report["capacity_fixture_jobs"] = fixture_rows
            self.report["checks"]["pending_jobs_at_configured_cap"] = (
                self.report["queue_at_capacity"].get("queued", 0) == 32
                and len(fixture_rows) == 32
                and len({row["id"] for row in fixture_rows}) == 32
                and all(row["kind"] == "article_reprocess" and row["state"] == "queued"
                        for row in fixture_rows)
            )
            if not self.report["checks"]["pending_jobs_at_configured_cap"]:
                raise GateFailure("fixture queue snapshot did not show exactly 32 pending jobs")
            # The active blocker keeps the one worker slot occupied while its independent
            # heartbeat task stays live. No container pause is needed, so the receiver's
            # worker-availability check remains meaningful during the 503 assertion.
            capacity_worker = queue_snapshot.get("worker") or {}
            if capacity_worker.get("status") != "running" or not capacity_worker.get("heartbeat_at"):
                raise GateFailure("worker lease was not live while the queue was full")
            heartbeat_age = self.heartbeat_age_seconds(capacity_worker["heartbeat_at"])
            if heartbeat_age > 20:
                raise GateFailure("worker lease was stale before scheduled queue-capacity delivery")
            self.report["capacity_worker_state"].update({
                "status_at_capacity": capacity_worker.get("status"),
                "heartbeat_at_capacity": capacity_worker.get("heartbeat_at"),
                "heartbeat_age_seconds_at_capacity": round(heartbeat_age, 3),
            })

            self.capture_resource_snapshot("active_news_queue_at_capacity")
            control_revision = self.control_revision()
            control_status, paused = self.request(
                "scheduler", "PUT", "/v1/control",
                {"expected_revision": control_revision, "paused": True}, auth="scheduler")
            if control_status != 200 or paused.get("paused") is not True:
                raise GateFailure("scheduler could not pause before adding the capacity fixture")
            expected_revision = int(paused.get("revision", 0))
            due = self.due_timestamp(seconds_ago=3, microseconds=449877)
            status, imported = self.import_schedules(expected_revision, [{
                "schedule_key": "fixture:queue-capacity", "enabled": True,
                "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
                "target_alias": "news-worker", "operation": "news_processing", "payload": {},
                "next_due_at": due,
            }])
            if status != 200 or imported.get("created") != ["fixture:queue-capacity"]:
                raise GateFailure("could not import bounded queue-capacity fixture")
            resume_status, resumed = self.request("scheduler", "PUT", "/v1/control",
                                                  {"expected_revision": expected_revision, "paused": False},
                                                  auth="scheduler")
            if resume_status != 200 or resumed.get("paused") is not False:
                raise GateFailure("scheduler could not resume capacity fixture")
            capacity = self.wait_occurrence(
                "fixture:queue-capacity", statuses={"pending"}, attempts=1, timeout=18)
            self.report["checks"]["full_news_queue_returns_retryable_503"] = capacity.get("status") == "pending"
            self.report["queue_capacity_attempts"] = capacity.get("attempts")
            if not self.report["checks"]["full_news_queue_returns_retryable_503"]:
                raise GateFailure("full News queue was incorrectly accepted or terminally blocked")
            after_attempt = self.probe(self.container_names["web"], "snapshot")
            attempted_worker = after_attempt.get("worker") or {}
            if attempted_worker.get("status") != "running" or not attempted_worker.get("heartbeat_at"):
                raise GateFailure("worker lease was not live after the receiver returned retryable 503")
            attempt_heartbeat_age = self.heartbeat_age_seconds(attempted_worker["heartbeat_at"])
            if attempt_heartbeat_age > 20:
                raise GateFailure("worker lease became stale before queue-capacity retry assertion")
            self.report["capacity_worker_state"].update({
                "heartbeat_at_after_503": attempted_worker.get("heartbeat_at"),
                "heartbeat_age_seconds_after_503": round(attempt_heartbeat_age, 3),
            })
            elapsed = time.monotonic() - saturation_started
            self.report["capacity_worker_state"]["elapsed_seconds"] = round(elapsed, 3)
            self.report["capacity_worker_state"]["pause_elapsed_seconds"] = 0
            self.report["capacity_worker_state"]["heartbeat_freshness_limit_seconds"] = 20

            self.docker("stop", self.container_names["scheduler"], timeout=30)
            self.docker("start", self.container_names["scheduler"])
            self.report["restarts"]["scheduler"] += 1
            self.wait_status("scheduler", "/readyz")
        finally:
            cleanup = self.probe(self.container_names["web"], "cleanup-saturation")
            self.report["saturation_cleanup"] = cleanup
            self.report["checks"]["only_fixture_queue_rows_removed"] = (
                cleanup.get("deleted_queued_fixture_jobs", 0) == 32
                and cleanup.get("deleted_fixture_articles", 0) == 32
            )
        if not self.report["checks"]["only_fixture_queue_rows_removed"]:
            raise GateFailure("fixture cleanup did not remove exactly the gate-owned queue rows and articles")
        self.fake_control(gemini_mode="release")
        self.wait_job(blocker_body["job_id"], states={"succeeded", "failed"})
        accepted = self.wait_occurrence("fixture:queue-capacity", statuses={"accepted"}, attempts=2, timeout=180)
        self.report["checks"]["pending_occurrence_survives_scheduler_restart"] = accepted.get("job_id") is not None
        if not self.report["checks"]["pending_occurrence_survives_scheduler_restart"]:
            raise GateFailure("pending capacity occurrence was lost across scheduler restart")
        capacity_job = self.wait_job(accepted["job_id"], states={"succeeded", "failed"}, timeout=180)
        self.report["checks"]["capacity_job_survives_retry"] = capacity_job.get("status") == "succeeded"
        if not self.report["checks"]["capacity_job_survives_retry"]:
            raise GateFailure("retried capacity occurrence did not complete through the News worker")
        self.report["capacity_occurrence"] = accepted

    def verify_slow_target_isolation(self) -> None:
        self.fake_control(consumer_mode="accept_then_hold")
        revision = self.control_revision()
        status, paused = self.request("scheduler", "PUT", "/v1/control",
                                      {"expected_revision": revision, "paused": True}, auth="scheduler")
        if status != 200 or paused.get("paused") is not True:
            raise GateFailure("could not pause scheduler before slow-target fixture")
        revision = paused["revision"]
        status, imported = self.import_schedules(revision, [{
            "schedule_key": "fixture:slow-target", "enabled": True,
            "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
            "target_alias": "generic", "operation": "fixture.slow", "payload": {},
            "next_due_at": self.due_timestamp(seconds_ago=3, microseconds=271991),
        }])
        if status != 200 or imported.get("created") != ["fixture:slow-target"]:
            raise GateFailure("could not import slow generic-target fixture")
        status, resumed = self.request("scheduler", "PUT", "/v1/control",
                                       {"expected_revision": revision, "paused": False}, auth="scheduler")
        if status != 200 or resumed.get("paused") is not False:
            raise GateFailure("could not resume slow generic-target fixture")
        slow = self.wait_occurrence("fixture:slow-target", statuses={"pending", "leased"}, attempts=1)
        self.capture_resource_snapshot("active_slow_target_inflight")

        # Keep the generic target's response held while a newly due News
        # command is generated and dispatched through a separate static alias.
        status, paused = self.request("scheduler", "PUT", "/v1/control",
                                      {"expected_revision": resumed["revision"], "paused": True}, auth="scheduler")
        if status != 200 or paused.get("paused") is not True:
            raise GateFailure("could not pause during slow-target delivery")
        revision = paused["revision"]
        status, imported = self.import_schedules(revision, [{
            "schedule_key": "news:independent-dispatch", "enabled": True,
            "calendar": {"type": "interval", "timezone": "UTC", "interval_minutes": 1440},
            "target_alias": "news-worker", "operation": "news_processing", "payload": {},
            "next_due_at": self.due_timestamp(seconds_ago=3, microseconds=883107),
        }])
        if status != 200 or imported.get("created") != ["news:independent-dispatch"]:
            raise GateFailure("could not import independent News dispatch fixture")
        status, resumed = self.request("scheduler", "PUT", "/v1/control",
                                       {"expected_revision": revision, "paused": False}, auth="scheduler")
        if status != 200 or resumed.get("paused") is not False:
            raise GateFailure("could not resume independent News dispatch fixture")

        news = self.wait_occurrence("news:independent-dispatch", statuses={"accepted"}, timeout=35)
        slow_while_news_accepted = self.wait_occurrence(
            "fixture:slow-target", statuses={"pending", "leased"}, attempts=1, timeout=10)
        self.report["checks"]["fast_alias_dispatches_while_slow_alias_is_in_flight"] = (
            news.get("status") == "accepted" and
            slow_while_news_accepted.get("status") in {"pending", "leased"}
        )
        if not self.report["checks"]["fast_alias_dispatches_while_slow_alias_is_in_flight"]:
            self.fake_control(consumer_mode="normal")
            raise GateFailure("slow generic target starved a due News target")

        self.fake_control(consumer_mode="normal")
        slow_accepted = self.wait_occurrence("fixture:slow-target", statuses={"accepted"}, attempts=2)
        self.report["checks"]["slow_alias_receipt_retry_is_idempotent"] = (
            slow_accepted.get("status") == "accepted" and
            self.fake_counters().get("consumer", {}).get("operations", {}).get("fixture.slow") == 1
        )
        if not self.report["checks"]["slow_alias_receipt_retry_is_idempotent"]:
            raise GateFailure("slow generic-target replay did not retain one logical operation")
        if news.get("job_id"):
            self.wait_job(news["job_id"], states={"succeeded", "failed"})
        self.report["slow_target_occurrence"] = slow_accepted
        self.report["independent_news_occurrence"] = news

    def verify_process_restarts_and_inventory(self, accepted: dict[str, dict]) -> None:
        # Restart web and worker after a completed job and confirm durable job/receipt links remain.
        self.docker("restart", self.container_names["web"], timeout=90)
        self.report["restarts"]["web"] += 1
        self.wait_status("web", "/health")
        job_id = accepted["news"].get("job_id")
        persisted_job = self.wait_job(job_id, states={"succeeded"}, timeout=45)
        self.report["checks"]["job_survives_web_restart"] = persisted_job.get("job_id") == job_id
        before_worker = self.probe(self.container_names["web"], "snapshot").get("worker") or {}
        before_generation = before_worker.get("generation", 0)
        self.docker("restart", self.container_names["worker"], timeout=90)
        self.report["restarts"]["worker"] += 1
        self.wait_status("web", "/health")
        worker_after = self.wait_worker("running")
        self.report["checks"]["worker_reacquires_lease_after_restart"] = (
            worker_after is not None and worker_after.get("generation", 0) > before_generation
        )
        persisted_again = self.wait_job(job_id, states={"succeeded"}, timeout=45)
        self.report["checks"]["job_survives_worker_restart"] = persisted_again.get("job_id") == job_id

        status, inventory = self.request("scheduler", "GET", "/v1/occurrences?limit=100", auth="scheduler")
        rows = inventory.get("occurrences", [])
        accepted_rows = [row for row in rows if row.get("status") == "accepted"]
        self.report["inventory"] = {
            "status": status,
            "total": len(rows),
            "accepted": len(accepted_rows),
            "pending_or_blocked": sum(row.get("status") in {"pending", "blocked", "leased"} for row in rows),
        }
        self.report["checks"]["rollback_inventory_contains_durable_receipts"] = (
            status == 200 and any(row.get("occurrence_id") == accepted["news"].get("occurrence_id")
                                  and row.get("job_id") == job_id for row in accepted_rows)
        )
        facade_status, facade = self.request("web", "GET", "/api/v1/schedule/settings", auth="admin")
        facade_keys = {
            row.get("task_name") for row in facade.get("settings", []) if isinstance(row, dict)
        } if isinstance(facade.get("settings"), list) else set()
        self.report["checks"]["rollback_facade_reads_imported_scheduler_config"] = (
            facade_status == 200 and "news_processing" in facade_keys
        )
        if not all(self.report["checks"].get(name) for name in (
                "job_survives_web_restart", "job_survives_worker_restart",
                "worker_reacquires_lease_after_restart",
                "rollback_inventory_contains_durable_receipts", "rollback_facade_reads_imported_scheduler_config")):
            raise GateFailure("job restart persistence or rollback inventory/facade smoke failed")

    @staticmethod
    def _bytes(value: str) -> int | None:
        match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*(B|kB|KB|MB|MiB|GB|GiB)\s*", value)
        if not match:
            return None
        amount = float(match.group(1))
        unit = match.group(2)
        scale = {"B": 1, "kB": 1000, "KB": 1000, "MB": 1000**2,
                 "MiB": 1024**2, "GB": 1000**3, "GiB": 1024**3}[unit]
        return int(amount * scale)

    @staticmethod
    def heartbeat_age_seconds(value: str) -> float:
        heartbeat = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - heartbeat).total_seconds())

    def capture_resource_snapshot(self, label: str) -> None:
        running_names = []
        for name in self.container_names.values():
            inspect = self.docker("inspect", "--format", "{{.State.Running}}", name, check=False)
            if inspect.stdout.strip().lower() == "true":
                running_names.append(name)
        result = self.docker("stats", "--no-stream", "--format",
                             "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}", *running_names, check=False)
        measurements = {}
        cpu_total = 0.0
        memory_used_total = 0
        memory_limit_total = 0
        for line in result.stdout.splitlines():
            parts = line.split("|", maxsplit=2)
            if len(parts) != 3:
                continue
            name, cpu_raw, memory_raw = parts
            cpu_match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)%\s*", cpu_raw)
            memory_parts = memory_raw.split("/", maxsplit=1)
            used = self._bytes(memory_parts[0]) if memory_parts else None
            limit = self._bytes(memory_parts[1]) if len(memory_parts) == 2 else None
            if cpu_match and used is not None and limit is not None:
                cpu = float(cpu_match.group(1))
                cpu_total += cpu
                memory_used_total += used
                memory_limit_total += limit
                measurements[name] = {"cpu_percent": cpu, "memory_used_bytes": used,
                                      "memory_limit_bytes": limit}
        states = {}
        for role, name in self.container_names.items():
            inspect = self.docker("inspect", "--format", "{{.RestartCount}} {{.State.OOMKilled}}", name,
                                  check=False)
            fields = inspect.stdout.strip().split()
            if len(fields) == 2:
                states[role] = {"restart_count": int(fields[0]) if fields[0].isdigit() else None,
                                "oom_killed": fields[1].lower() == "true"}
        snapshot = {
            "label": label,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "containers": measurements,
            "total_cpu_percent": round(cpu_total, 2),
            "total_memory_used_bytes": memory_used_total,
            "total_memory_limit_bytes": memory_limit_total,
            "container_states": states,
        }
        self.report.setdefault("resource_snapshots", []).append(snapshot)

    def capture_resources(self) -> None:
        self.capture_resource_snapshot("final")

    def run(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.resolve_image(self.app_image_ref, "app")
        self.resolve_image(self.scheduler_image_ref, "scheduler")
        self.resolve_image("postgres:16-alpine", "postgres")
        self.report["workspace_source_hashes"] = {
            "app": self.app_source_hashes(),
            "scheduler_build_id": self.scheduler_source_hash(),
        }
        self.start_services()
        self.verify_source_freshness()
        self.direct_contract_checks()
        self.capture_resource_snapshot("idle_before_dispatch")
        accepted = self.prepare_and_dispatch()
        self.verify_slow_target_isolation()
        self.verify_capacity_and_scheduler_restart()
        self.verify_process_restarts_and_inventory(accepted)
        self.capture_resources()
        false_checks = [name for name, value in self.report["checks"].items() if value is not True]
        if false_checks:
            raise GateFailure("acceptance checks failed: " + ", ".join(false_checks))
        self.report["result"] = "passed"

    def cleanup(self) -> None:
        owned = self.docker("ps", "-aq", "--filter", f"label={OWNER_LABEL}={self.owner}", check=False)
        for container_id in owned.stdout.splitlines():
            removed = self.docker("rm", "-fv", container_id, check=False, timeout=30)
            if removed.returncode:
                self.report["cleanup_errors"].append("owned fixture container cleanup failed")
        network = self.docker("network", "inspect", "--format", f'{{{{ index .Labels "{OWNER_LABEL}" }}}}',
                              self.network, check=False)
        if network.returncode == 0 and network.stdout.strip() == self.owner:
            removed = self.docker("network", "rm", self.network, check=False, timeout=30)
            if removed.returncode:
                self.report["cleanup_errors"].append("owned fixture network cleanup failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-image", required=True, help="Existing local News app image; never built or published here.")
    parser.add_argument("--scheduler-image", required=True, help="Existing local standalone scheduler image.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for a sanitized JSON gate report.")
    args = parser.parse_args()
    if not shutil.which("docker"):
        parser.error("docker CLI is required")
    gate = DockerGate(args.app_image, args.scheduler_image, args.output_dir)
    try:
        gate.run()
    except Exception as exc:
        gate.report["result"] = "failed"
        failure = str(exc) if isinstance(exc, GateFailure) else f"unexpected_{type(exc).__name__}"
        for secret in (SCHEDULER_TOKEN, NEWS_TARGET_TOKEN, GENERIC_TOKEN, NEWS_PASSWORD,
                       SCHEDULER_PASSWORD, "fixture-admin-password", "fixture-admin-only",
                       "fixture-only-jwt-signing-value", "fixture-only-gemini-key",
                       "fixture-only-token"):
            failure = failure.replace(secret, "[redacted]")
        gate.report["failure"] = failure[:400]
        gate.report["failure_diagnostics"] = gate.safe_failure_diagnostics()
    finally:
        try:
            gate.cleanup()
        except Exception:
            gate.report["cleanup_errors"].append("cleanup could not inspect owned fixture resources")
        gate.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report_path = gate.output_dir / f"{gate.run_id}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(gate.report, indent=2, sort_keys=True))
    return 0 if gate.report.get("result") == "passed" and not gate.report["cleanup_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
