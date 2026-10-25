#!/usr/bin/env python3
"""Run an isolated, internal-network Docker acceptance gate for the Python worker."""

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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "background-worker"
OWNER_LABEL = "io.evening-news.background-worker-gate"
class GateFailure(RuntimeError):
    pass


class DockerGate:
    def __init__(self, image: str, output_dir: Path):
        self.image = image
        self.output_dir = output_dir
        self.run_id = uuid.uuid4().hex[:12]
        self.owner = f"fixture-{self.run_id}"
        self.prefix = f"news-bg-gate-{self.run_id}"
        self.network = f"{self.prefix}-net"
        self.containers: list[str] = []
        self.report = {
            "gate": "background-worker-docker-fixture",
            "run_id": self.run_id,
            "image": image,
            "network_internal": True,
            "real_external_services_used": False,
            "checks": {},
            "provider_calls": {},
            "memory_mib": {},
            "explicit_restarts": {"web": 0, "worker": 0},
            "cleanup_errors": [],
        }

    def docker(self, *args: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        command = ["docker", *args]
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            detail = (result.stderr or result.stdout).strip().splitlines()
            summary = detail[-1] if detail else f"docker command exited {result.returncode}"
            raise GateFailure(summary[:400])
        return result

    def image_id(self) -> str:
        return self.docker("image", "inspect", self.image, "--format", "{{.Id}}").stdout.strip()

    def source_hashes(self, container: str | None = None) -> dict[str, str]:
        if container is None:
            source_paths = sorted(
                path.relative_to(ROOT).as_posix()
                for package in ("news_aggregator", "web")
                for path in (ROOT / package).rglob("*")
                if path.is_file()
                and "__pycache__" not in path.parts
                and (package == "web" or path.suffix == ".py")
            )
            return {
                path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                for path in source_paths
            }
        script = (
            "import hashlib,json; from pathlib import Path; root=Path('/app'); "
            "paths=sorted(str(p.relative_to(root)) for folder in ('news_aggregator','web') "
            "for p in (root/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts "
            "and (folder == 'web' or p.suffix == '.py')); "
            "print(json.dumps({p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths}))"
        )
        output = self.docker("exec", container, "python", "-c", script).stdout.strip()
        try:
            return json.loads(output.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("Could not read runtime source hashes from app image") from None

    def safe_app_trace(self, container: str) -> dict:
        log_result = self.docker("logs", "--tail", "400", container, check=False)
        logs = (log_result.stdout or "") + "\n" + (log_result.stderr or "")
        frames = []
        exceptions = []
        safe_details = []
        frame_pattern = re.compile(r'File "(/app/news_aggregator/[^\"]+)", line (\d+)')
        exception_pattern = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception))(?::|$)")
        missing_attribute_pattern = re.compile(
            r"^\s*AttributeError: (?:(?:module|type) '([^']+)'|\s*'([^']+)' object) has no attribute '([A-Za-z_][A-Za-z0-9_]*)'\s*$"
        )
        for line in logs.splitlines():
            frame = frame_pattern.search(line)
            if frame:
                frames.append({"file": frame.group(1).removeprefix("/app/"), "line": int(frame.group(2))})
            exc = exception_pattern.match(line)
            if exc:
                exceptions.append(exc.group(1).split(".")[-1])
            missing_attribute = missing_attribute_pattern.match(line)
            if missing_attribute:
                safe_details.append({
                    "kind": "missing_attribute",
                    "target_type": missing_attribute.group(1) or missing_attribute.group(2),
                    "attribute": missing_attribute.group(3),
                })
        return {
            "exception_types": exceptions[-5:],
            "application_frames": frames[-12:],
            "safe_details": safe_details[-3:],
        }

    def create_network(self) -> None:
        self.docker("network", "create", "--internal", "--label", f"{OWNER_LABEL}={self.owner}",
                    self.network)
        self.report["network_name"] = self.network

    def run_container(self, name: str, *, command: list[str], alias: str | None = None,
                      env: dict[str, str] | None = None, mounts: bool = True,
                      detached: bool = True, image: str | None = None,
                      memory: str | None = None, cpus: str = "1",
                      pids_limit: int = 128, health_cmd: str | None = None,
                      disable_healthcheck: bool = False) -> None:
        args = ["create", "--name", name, "--network", self.network,
                "--label", f"{OWNER_LABEL}={self.owner}", "--label", f"io.evening-news.gate-role={name.rsplit('-', 1)[-1]}"]
        args.extend(["--cpus", cpus, "--pids-limit", str(pids_limit)])
        if memory:
            args.extend(["--memory", memory])
        if health_cmd:
            args.extend(["--health-cmd", health_cmd, "--health-interval", "5s",
                         "--health-timeout", "5s", "--health-retries", "3",
                         "--health-start-period", "10s"])
        elif disable_healthcheck:
            args.append("--no-healthcheck")
        if alias:
            args.extend(["--network-alias", alias])
        if mounts:
            args.extend(["--mount", f"type=bind,src={FIXTURE_DIR},dst=/fixtures,readonly"])
        for key, value in (env or {}).items():
            args.extend(["--env", f"{key}={value}"])
        args.append(image or self.image)
        args.extend(command)
        container_id = self.docker(*args).stdout.strip()
        if not container_id:
            raise GateFailure(f"Docker did not return an ID for {name}")
        self.containers.append(name)
        if detached:
            self.docker("start", name)

    def app_environment(self, role: str) -> dict[str, str]:
        return {
            "DATABASE_URL": "postgresql+asyncpg://gate_user:fixture-only-password@postgres:5432/gate_db",
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
            "SITE_BASE_URL": "https://fixture.example.test",
            "BACKGROUND_EXECUTION_MODE": "worker",
            "SCHEDULER_ENABLED": "false",
            "WORKER_POLL_INTERVAL_SECONDS": "1",
            "WORKER_SHUTDOWN_DRAIN_SECONDS": "0",
            "SOURCE_MANAGER_MODE": "go",
            "TELEGRAM_SOURCE_MODE": "go",
            "PAGE_MONITOR_SOURCE_MODE": "go",
            "EXTRACTION_SERVICE_URL": "http://fixture-services:8090",
            "EXTRACTION_SERVICE_TOKEN": "fixture-only-token",
            "EXTRACTION_SERVICE_MODE": "go",
            "EXTRACTION_SERVICE_PYTHON_FALLBACK": "false",
            "ARTICLE_INGESTION_MODE": "go",
            "ARTICLE_INGESTION_URL": "http://fixture-services:8090",
            "ARTICLE_INGESTION_TOKEN": "fixture-only-article-ingestion-token",
            "SCHEDULER_API_URL": "http://fixture-services:8090",
            "SCHEDULER_API_TOKEN": "fixture-only-scheduler-token",
            "SCHEDULER_API_ALLOW_HTTP": "true",
            "SCHEDULED_JOBS_TOKEN": "fixture-only-scheduled-jobs-token",
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
            "BG_GATE_ROLE": role,
            "BG_GATE_TELEGRAM_URL": "http://fixture-services:8090/telegram/botFixtureOnlyToken",
            "PYTHONPATH": "/fixtures:/app",
            "LOG_LEVEL": "WARNING",
            "CACHE_DIR": "/tmp/gate-cache",
        }

    def exec_json(self, container: str, *args: str, timeout: int = 20) -> dict:
        output = self.docker("exec", container, "python", "/fixtures/probe.py", *args,
                             timeout=timeout).stdout.strip()
        try:
            return json.loads(output.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure(f"Invalid fixture probe result from {container}") from None

    def fake_control(self, **changes: str) -> dict:
        payload = json.dumps(changes)
        output = self.docker("exec", f"{self.prefix}-fake", "python", "/fixtures/probe.py",
                             "fake-control", payload).stdout.strip()
        try:
            return json.loads(output.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("Invalid fake service control response") from None

    def fake_counters(self) -> dict[str, int]:
        output = self.docker("exec", f"{self.prefix}-fake", "python", "/fixtures/probe.py",
                             "fake-counters").stdout.strip()
        try:
            result = json.loads(output.splitlines()[-1])
            return result["response"]
        except (ValueError, IndexError, KeyError):
            raise GateFailure("Invalid fake service counters") from None

    def wait_http_probe(self, action: str, *, timeout: float = 90) -> dict:
        deadline = time.monotonic() + timeout
        last_error = "web not ready"
        while time.monotonic() < deadline:
            try:
                result = self.exec_json(f"{self.prefix}-web", action)
                if result.get("http_status") == 200:
                    return result
                last_error = f"probe returned HTTP {result.get('http_status')}"
            except (GateFailure, subprocess.TimeoutExpired) as exc:
                last_error = str(exc)
            time.sleep(1)
        raise GateFailure(f"Timed out waiting for web probe {action}: {last_error}")

    def job(self, job_id: str) -> dict:
        result = self.exec_json(f"{self.prefix}-web", "job", job_id)
        if result.get("http_status") != 200:
            raise GateFailure(f"Job status endpoint returned HTTP {result.get('http_status')}")
        return result["response"]

    def record_job_diagnostics(self, job_id: str, current: dict | None = None) -> dict:
        current = current or self.job(job_id)
        diagnostics = {
            "job_status": current.get("status"),
            "job_error_code": current.get("error_code"),
            "worker_available": current.get("worker_available"),
        }
        result = current.get("result")
        if isinstance(result, dict):
            safe_metrics = ("status", "articles_fetched", "articles_processed", "articles_summarized",
                            "articles_categorized", "api_calls_made", "errors_count", "parts_sent",
                            "backup_bytes", "duration_seconds")
            diagnostics["job_result_metrics"] = {
                key: result[key] for key in safe_metrics if type(result.get(key)) in (bool, int, float, str)
            }
        worker_name = f"{self.prefix}-worker"
        if worker_name in self.containers:
            state = self.docker("inspect", "--format",
                                "{{.State.Status}}/{{.State.ExitCode}}", worker_name,
                                check=False).stdout.strip()
            health = self.docker("inspect", "--format", "{{.State.Health.Status}}",
                                 worker_name, check=False).stdout.strip()
            diagnostics.update({"worker_process": state, "worker_health": health})
        try:
            diagnostics["fake_calls"] = self.fake_counters()
        except (GateFailure, subprocess.TimeoutExpired):
            diagnostics["fake_calls"] = "unavailable"
        web_name = f"{self.prefix}-web"
        if web_name in self.containers:
            try:
                snapshot = self.exec_json(web_name, "snapshot").get("response", {})
                diagnostics["articles"] = snapshot.get("articles", [])
            except (GateFailure, subprocess.TimeoutExpired):
                diagnostics["articles"] = "unavailable"
        self.report["last_timeout_diagnostics"] = diagnostics
        return diagnostics

    def wait_job(self, job_id: str, states: set[str], *, timeout: float = 120) -> dict:
        deadline = time.monotonic() + timeout
        current = {}
        while time.monotonic() < deadline:
            current = self.job(job_id)
            if current.get("status") in states:
                return current
            terminal_states = {"succeeded", "failed", "delivery_unknown"}
            if current.get("status") in terminal_states:
                diagnostics = self.record_job_diagnostics(job_id, current)
                raise GateFailure(
                    f"Job reached terminal state outside expected states: {current.get('status')}; "
                    f"error_code={current.get('error_code')}, diagnostics={json.dumps(diagnostics, sort_keys=True)}"
                )
            time.sleep(1)
        diagnostics = self.record_job_diagnostics(job_id, current)
        raise GateFailure(
            f"Job did not reach an expected state ({','.join(sorted(states))}); "
            f"current={current.get('status')}, error_code={current.get('error_code')}, "
            f"worker={diagnostics.get('worker_process')}/{diagnostics.get('worker_health')}"
        )

    def wait_counter(self, name: str, expected: int, *, timeout: float = 60,
                     job_id: str | None = None) -> dict[str, int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            counters = self.fake_counters()
            if counters.get(name, 0) >= expected:
                return counters
            if job_id:
                current = self.job(job_id)
                if current.get("status") in {"succeeded", "failed", "delivery_unknown"}:
                    diagnostics = self.record_job_diagnostics(job_id, current)
                    raise GateFailure(
                        f"Job reached terminal state before fake {name} counter advanced: "
                        f"{current.get('status')}, error_code={current.get('error_code')}; "
                        f"diagnostics={json.dumps(diagnostics, sort_keys=True)}"
                    )
            time.sleep(0.5)
        if job_id:
            current = self.job(job_id)
            diagnostics = self.record_job_diagnostics(job_id, current)
            details = f"; diagnostics={json.dumps(diagnostics, sort_keys=True)}"
        else:
            details = ""
        raise GateFailure(f"Fake provider counter did not reach requested threshold: {name}{details}")

    def create_worker(self, *, suffix: str = "worker") -> str:
        name = f"{self.prefix}-{suffix}"
        self.run_container(name, command=["python", "-m", "news_aggregator.worker"],
                           env=self.app_environment("worker"), memory="512m",
                           health_cmd="python -m news_aggregator.worker --healthcheck")
        return name

    def wait_worker_healthy(self, name: str, *, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        status = "starting"
        while time.monotonic() < deadline:
            result = self.docker("inspect", "--format", "{{.State.Health.Status}}", name,
                                 check=False)
            if result.returncode == 0:
                status = result.stdout.strip()
                if status == "healthy":
                    return
                if status == "unhealthy":
                    raise GateFailure("Worker healthcheck reported unhealthy")
            time.sleep(1)
        raise GateFailure(f"Worker healthcheck did not become healthy (status={status})")

    def wait_container_exit(self, name: str, *, timeout: int = 30) -> int:
        result = self.docker("wait", name, timeout=timeout)
        try:
            return int(result.stdout.strip())
        except ValueError:
            raise GateFailure(f"Could not read exit status for {name}") from None

    def memory_mib(self, container: str) -> float:
        raw = self.docker("stats", "--no-stream", "--format", "{{.MemUsage}}", container).stdout.strip()
        used = raw.split("/", 1)[0].strip()
        match = re.fullmatch(r"([0-9.]+)\s*(B|KiB|MiB|GiB)", used)
        if not match:
            return 0.0
        value = float(match.group(1))
        factor = {"B": 1 / (1024 * 1024), "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}[match.group(2)]
        return round(value * factor, 2)

    def capture_memory(self, label: str) -> None:
        web = self.memory_mib(f"{self.prefix}-web")
        worker = self.memory_mib(f"{self.prefix}-worker")
        self.report["memory_mib"][label] = {"web": web, "worker": worker, "combined": round(web + worker, 2)}

    def assert_owned_mounts(self) -> None:
        for role in ("web", "worker"):
            name = f"{self.prefix}-{role}"
            output = self.docker("inspect", "--format", "{{json .Mounts}}", name).stdout
            try:
                mounts = json.loads(output)
            except ValueError:
                raise GateFailure(f"Could not inspect mounts for {role}") from None
            if len(mounts) != 1 or Path(mounts[0]["Source"]).resolve() != FIXTURE_DIR.resolve():
                raise GateFailure(f"{role} has an unexpected bind mount")
            if mounts[0]["Destination"] != "/fixtures" or mounts[0]["RW"]:
                raise GateFailure(f"{role} fixture mount is not read-only")

    def run(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report["image_id"] = self.image_id()
        self.create_network()
        self.run_container(f"{self.prefix}-postgres", command=["postgres"], alias="postgres",
                           env={"POSTGRES_DB": "gate_db", "POSTGRES_USER": "gate_user",
                                "POSTGRES_PASSWORD": "fixture-only-password"}, image="postgres:16-alpine",
                           mounts=False, memory="256m")
        self.run_container(f"{self.prefix}-fake", command=["python", "/fixtures/fake_services.py"],
                           alias="fixture-services", memory="128m", disable_healthcheck=True)
        self.docker("exec", "--env", "PGPASSWORD=fixture-only-password",
                    f"{self.prefix}-postgres", "sh", "-lc",
                    "until pg_isready -h 127.0.0.1 -U gate_user -d gate_db >/dev/null 2>&1; do sleep 1; done",
                    timeout=90)
        self.docker("exec", "--env", "PGPASSWORD=fixture-only-password",
                    f"{self.prefix}-postgres", "createdb", "-h", "127.0.0.1",
                    "-U", "gate_user", "empty_gate_db")

        # The worker must refuse to start when the additive queue schema is absent.
        schema_check_name = f"{self.prefix}-schema-check-worker"
        schema_environment = self.app_environment("worker")
        schema_environment["DATABASE_URL"] = (
            "postgresql+asyncpg://gate_user:fixture-only-password@postgres:5432/empty_gate_db"
        )
        self.run_container(schema_check_name, command=["python", "-m", "news_aggregator.worker"],
                           env=schema_environment, memory="512m", disable_healthcheck=True)
        schema_exit = self.wait_container_exit(schema_check_name, timeout=45)
        self.report["checks"]["worker_fails_closed_without_schema"] = schema_exit != 0
        if schema_exit == 0:
            raise GateFailure("Worker unexpectedly started without the required jobs schema")

        self.run_container(f"{self.prefix}-web", command=["uvicorn", "news_aggregator.main:app",
                                                           "--host", "0.0.0.0", "--port", "8000"],
                           alias="web", env=self.app_environment("web"), memory="512m")
        self.wait_http_probe("health")
        openapi = self.exec_json(f"{self.prefix}-web", "openapi-check")
        self.report["checks"]["public_reprocess_contract_in_openapi"] = (
            openapi.get("http_status") == 200
            and openapi.get("response", {}).get("auth_login") is True
            and openapi.get("response", {}).get("public_reprocess") is True
            and openapi.get("response", {}).get("api_reprocess") is True
        )
        if not self.report["checks"]["public_reprocess_contract_in_openapi"]:
            raise GateFailure("Live OpenAPI schema did not expose the expected public/login routes")
        login_probe = self.exec_json(f"{self.prefix}-web", "auth-login-check")
        login_response = login_probe.get("response", {})
        self.report["admin_login_diagnostic"] = {
            "http_status": login_response.get("http_status"),
            "response_fields": login_response.get("response_fields", []),
            "token_returned": login_response.get("token_returned") is True,
        }
        if login_response.get("http_status") != 200:
            self.report["admin_login_diagnostic"]["safe_exception"] = self.safe_app_trace(
                f"{self.prefix}-web"
            )
        workspace_hashes = self.source_hashes()
        image_hashes = self.source_hashes(f"{self.prefix}-web")
        self.report["runtime_source_hashes"] = {
            "workspace": workspace_hashes,
            "image": image_hashes,
        }
        self.report["checks"]["image_matches_runtime_checkout"] = workspace_hashes == image_hashes
        if not self.report["checks"]["image_matches_runtime_checkout"]:
            raise GateFailure("App image runtime source does not match the current checkout")
        self.docker("exec", f"{self.prefix}-web", "python", "/fixtures/seed_fixture.py", timeout=30)
        self.report["checks"]["unauthorized_job_status"] = self.exec_json(
            f"{self.prefix}-web", "unauthorized").get("http_status") == 401
        dashboard = self.exec_json(f"{self.prefix}-web", "dashboard")
        process_status = self.exec_json(f"{self.prefix}-web", "status")
        self.report["checks"]["persisted_dashboard_without_web_orchestrator"] = (
            dashboard.get("http_status") == 200
            and isinstance(dashboard.get("response", {}).get("queue_stats"), dict)
            and "worker_available" in dashboard["response"]["queue_stats"]
            and process_status.get("http_status") == 200
            and "worker_available" in process_status.get("response", {})
        )
        if not self.report["checks"]["persisted_dashboard_without_web_orchestrator"]:
            raise GateFailure("Worker-mode dashboard/status did not return persisted worker state")
        self.report["checks"]["source_not_found"] = self.exec_json(
            f"{self.prefix}-web", "source-not-found", "source-missing-key").get("http_status") == 404
        if not self.report["checks"]["unauthorized_job_status"]:
            raise GateFailure("Job status endpoint did not reject an unauthenticated request")
        if not self.report["checks"]["source_not_found"]:
            raise GateFailure("Source connection test did not preserve not-found behavior")
        telegram_test = self.exec_json(f"{self.prefix}-web", "telegram-test")
        self.report["checks"]["web_telegram_diagnostic_uses_fixture"] = (
            telegram_test.get("http_status") == 200
            and telegram_test.get("response", {}).get("success") is True
        )
        if not self.report["checks"]["web_telegram_diagnostic_uses_fixture"]:
            raise GateFailure("Web Telegram connection diagnostic did not use the fixture receiver")

        # Block fixture AI, accept one real pipeline job, and confirm idempotent acceptance.
        self.fake_control(ai="block")
        accepted = self.exec_json(f"{self.prefix}-web", "enqueue-process", "gate-process-request-1")
        if accepted.get("http_status") != 202:
            raise GateFailure(f"Processing request did not return HTTP 202: {accepted.get('http_status')}")
        process_job_id = accepted["response"]["job_id"]
        repeated = self.exec_json(f"{self.prefix}-web", "enqueue-process", "gate-process-request-1")
        if repeated.get("http_status") != 202 or repeated["response"].get("job_id") != process_job_id:
            raise GateFailure("Repeated idempotency key did not return the accepted job")
        conflict = self.exec_json(f"{self.prefix}-web", "enqueue-digest", "gate-process-request-1")
        self.report["checks"]["idempotency_replay"] = repeated["response"].get("job_id") == process_job_id
        self.report["checks"]["idempotency_conflict"] = conflict.get("http_status") == 409
        if not self.report["checks"]["idempotency_conflict"]:
            raise GateFailure("Reusing the idempotency key with a different job kind was not rejected")

        worker_name = self.create_worker()
        self.assert_owned_mounts()
        self.report["checks"]["fixture_mount_only_read_only"] = True
        time.sleep(2)
        web_image = self.docker("inspect", "--format", "{{.Image}}", f"{self.prefix}-web").stdout.strip()
        worker_image = self.docker("inspect", "--format", "{{.Image}}", worker_name).stdout.strip()
        self.report["checks"]["same_image_matches"] = web_image == worker_image == self.image_id()
        if not self.report["checks"]["same_image_matches"]:
            raise GateFailure("Web and worker are not running the same local image")
        self.report["resource_limits"] = {
            "postgres_memory": "256m", "web_memory": "512m", "worker_memory": "512m",
            "fixture_services_memory": "128m", "cpus_per_container": "1",
            "pids_per_container": 128,
        }
        self.report["actual_go_runtime_tested"] = False
        self.report["browser_tested"] = False
        self.report["throughput_claimed"] = False
        internal = self.docker("network", "inspect", "--format", "{{.Internal}}", self.network).stdout.strip()
        if internal.lower() != "true":
            raise GateFailure("Fixture Docker network is not internal-only")
        self.capture_memory("idle_before")
        # Ensure the owner is active in the AI provider before restarting web.
        counters = self.wait_counter("gemini", 1, job_id=process_job_id)
        if self.job(process_job_id).get("status") != "running":
            raise GateFailure("Processing job was not running while its synthetic AI call was blocked")
        self.capture_memory("active_ai_blocked")
        self.wait_worker_healthy(worker_name)
        self.report["checks"]["worker_healthcheck_healthy"] = True
        self.report["retry_deadline_advanced_for_fixture"] = False

        # An independently started worker must exit without claiming/doing work.
        duplicate_name = self.create_worker(suffix="second-worker")
        duplicate_exit = self.wait_container_exit(duplicate_name)
        self.report["checks"]["second_worker_refused"] = duplicate_exit == 2
        self.report["provider_calls"]["before_web_restart"] = counters
        if not self.report["checks"]["second_worker_refused"]:
            raise GateFailure("A second worker did not exit after the singleton lease was refused")

        self.docker("restart", f"{self.prefix}-web", timeout=90)
        self.report["explicit_restarts"]["web"] += 1
        self.wait_http_probe("health")
        self.report["checks"]["accepted_job_survived_web_restart"] = self.job(process_job_id).get("status") == "running"
        if not self.report["checks"]["accepted_job_survived_web_restart"]:
            raise GateFailure("Web restart disrupted the accepted running job")
        self.fake_control(ai="release")
        process_terminal = self.wait_job(process_job_id, {"succeeded", "failed"})
        if process_terminal.get("status") != "succeeded":
            raise GateFailure("Synthetic processing pipeline did not complete successfully")
        snapshot = self.exec_json(f"{self.prefix}-web", "snapshot")["response"]
        completed_articles = [article for article in snapshot["articles"]
                              if article["title"] == "Fixture technology article" and article["state"] == "completed"]
        if not completed_articles:
            raise GateFailure("Synthetic article was not persisted as completed")
        article_id = completed_articles[0]["id"]
        counters = self.fake_counters()
        self.report["provider_calls"]["after_processing"] = counters
        if not counters.get("source_collection") or not counters.get("extraction") or not counters.get("gemini"):
            raise GateFailure("Processing did not exercise all fake Go/extraction/Gemini services")
        self.report["checks"]["article_completed_in_database"] = True
        self.report["checks"]["manual_acceptance_no_scheduler_jobs"] = (
            snapshot["due_schedules"] > 0 and not any(job["scheduled"] for job in snapshot["jobs"])
        )
        if not self.report["checks"]["manual_acceptance_no_scheduler_jobs"]:
            raise GateFailure("Disabled scheduler consumed a due synthetic schedule")

        # Reprocess the actual stored article, block AI, kill worker, and prove takeover resumes the job.
        self.fake_control(ai="block")
        public_unauthorized = self.exec_json(
            f"{self.prefix}-web", "public-reprocess-unauthorized", str(article_id)
        )
        self.report["checks"]["public_reprocess_requires_admin_cookie"] = (
            public_unauthorized.get("http_status") == 401
        )
        if not self.report["checks"]["public_reprocess_requires_admin_cookie"]:
            raise GateFailure("Public article reprocess accepted a request without the admin cookie")
        reprocess = self.exec_json(f"{self.prefix}-web", "reprocess", str(article_id), "gate-reprocess-request")
        if reprocess.get("http_status") != 202:
            response_body = reprocess.get("response", {})
            detail = response_body.get("detail") if isinstance(response_body, dict) else None
            if isinstance(detail, str):
                detail = detail[:240]
            else:
                detail = None
            self.report["public_reprocess_rejection"] = {
                "http_status": reprocess.get("http_status"),
                "detail": detail,
                "response_fields": sorted(response_body) if isinstance(response_body, dict) else [],
            }
            self.report["public_reprocess_exception"] = self.safe_app_trace(f"{self.prefix}-web")
            raise GateFailure(
                f"Public article reprocess did not return HTTP 202 "
                f"(status={reprocess.get('http_status')}, detail={detail!r}, "
                f"exception_types={self.report['public_reprocess_exception']['exception_types']})"
            )
        reprocess_id = reprocess["response"]["job_id"]
        admin_flow = reprocess["response"].get("fixture_admin_flow", {})
        self.report["checks"]["public_reprocess_real_admin_cookie_flow"] = (
            admin_flow.get("admin_page_http_status") == 200
            and admin_flow.get("admin_cookie_received") is True
            and admin_flow.get("public_admin_check_http_status") == 404
            and admin_flow.get("public_admin_check_authorized") is True
        )
        if not self.report["checks"]["public_reprocess_real_admin_cookie_flow"]:
            raise GateFailure("Public reprocess did not use the real admin page cookie flow")
        before_kill = self.wait_counter("gemini", counters["gemini"] + 1, job_id=reprocess_id)
        if self.job(reprocess_id).get("status") != "running":
            raise GateFailure("Article reprocess was not running while its synthetic AI call was blocked")
        self.docker("kill", worker_name)
        self.report["checks"]["worker_killed_during_processing"] = True
        # Advance only the synthetic row's long retry deadline; preserve its attempts and stages.
        retry_advance = self.exec_json(f"{self.prefix}-web", "advance-retry", str(article_id))["response"]
        self.report["retry_deadline_advanced_for_fixture"] = bool(
            retry_advance.get("advanced")
            and retry_advance.get("attempts_preserved")
            and retry_advance.get("stages_preserved")
        )
        if not self.report["retry_deadline_advanced_for_fixture"]:
            raise GateFailure("Could not safely advance the synthetic article retry deadline")
        # Start a new owner. It must recover the running attempt and continue the same durable job.
        self.docker("start", worker_name)
        self.report["explicit_restarts"]["worker"] += 1
        self.wait_job(reprocess_id, {"running"}, timeout=60)
        deadline = time.monotonic() + 60
        recovered_attempt = False
        while time.monotonic() < deadline:
            snap = self.exec_json(f"{self.prefix}-web", "snapshot")["response"]
            row = next((job for job in snap["jobs"] if job["id"] == reprocess_id), None)
            if row and row["attempt_generation"] >= 2 and row["state"] == "running":
                recovered_attempt = True
                break
            time.sleep(0.5)
        if not recovered_attempt:
            raise GateFailure("Worker takeover did not claim the interrupted processing job again")
        recovered_provider_calls = self.wait_counter("gemini", before_kill["gemini"] + 1)
        self.fake_control(ai="release")
        if self.wait_job(reprocess_id, {"succeeded", "failed"}).get("status") != "succeeded":
            raise GateFailure("Recovered article reprocess did not succeed")
        self.report["checks"]["processing_recovered_after_worker_kill"] = True
        self.report["provider_calls"]["after_processing_recovery"] = recovered_provider_calls

        # A receiver accepts the digest request then withholds its response; kill the worker in that ambiguity window.
        self.fake_control(telegram="block")
        digest = self.exec_json(f"{self.prefix}-web", "enqueue-digest", "gate-delivery-ambiguous")
        if digest.get("http_status") != 202:
            raise GateFailure("Digest request did not return HTTP 202")
        digest_id = digest["response"]["job_id"]
        before_send = self.fake_counters().get("telegram", 0)
        after_send = self.wait_counter("telegram", before_send + 1, job_id=digest_id)
        if self.job(digest_id).get("status") != "running":
            raise GateFailure("Digest job was not running while its fixture delivery response was blocked")
        self.docker("kill", worker_name)
        self.docker("start", worker_name)
        self.report["explicit_restarts"]["worker"] += 1
        delivery_terminal = self.wait_job(digest_id, {"delivery_unknown", "succeeded", "failed"}, timeout=60)
        if delivery_terminal.get("status") != "delivery_unknown":
            raise GateFailure("Ambiguous delivery was not marked delivery_unknown")
        self.fake_control(telegram="release")
        time.sleep(3)
        final_counters = self.fake_counters()
        if final_counters.get("telegram", 0) != after_send.get("telegram", 0):
            raise GateFailure("Ambiguous digest was resent automatically after worker recovery")
        self.report["provider_calls"]["final"] = final_counters
        self.report["checks"]["ambiguous_delivery_not_resent"] = True

        self.capture_memory("idle_final")
        self.report["checks"]["scheduler_disabled"] = True
        self.report["checks"]["delivery_crash_recovered_without_replay"] = True
        false_checks = [name for name, value in self.report["checks"].items() if value is not True]
        if false_checks:
            raise GateFailure("Acceptance checks failed: " + ", ".join(false_checks))
        self.report["result"] = "passed"

    def cleanup(self) -> None:
        for name in reversed(self.containers):
            result = self.docker("inspect", "--format", f"{{{{ index .Config.Labels \"{OWNER_LABEL}\" }}}}",
                                 name, check=False)
            if result.returncode == 0 and result.stdout.strip() == self.owner:
                removed = self.docker("rm", "-f", name, check=False, timeout=30)
                if removed.returncode:
                    self.report["cleanup_errors"].append(f"container cleanup failed: {name}")
        network = self.docker("network", "inspect", "--format", f"{{{{ index .Labels \"{OWNER_LABEL}\" }}}}",
                              self.network, check=False)
        if network.returncode == 0 and network.stdout.strip() == self.owner:
            removed = self.docker("network", "rm", self.network, check=False, timeout=30)
            if removed.returncode:
                self.report["cleanup_errors"].append("network cleanup failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="evening-news-background-worker:test-20261023",
                        help="Existing local app image; the gate never builds or publishes images.")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "background-worker-gate",
                        help="Directory for a sanitized JSON acceptance report.")
    args = parser.parse_args()
    docker = shutil.which("docker")
    if not docker:
        parser.error("docker CLI is required")
    gate = DockerGate(args.image, args.output_dir)
    try:
        gate.run()
    except Exception as exc:
        gate.report["result"] = "failed"
        gate.report["failure"] = str(exc)[:500]
    finally:
        gate.cleanup()
        report_path = gate.output_dir / f"{gate.run_id}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(gate.report, indent=2, sort_keys=True))
    return 0 if gate.report.get("result") == "passed" and not gate.report["cleanup_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
