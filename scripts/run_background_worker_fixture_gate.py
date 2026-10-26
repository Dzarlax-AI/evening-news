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

try:
    from scripts.article_processing_source_inventory import article_processing_inventory_matches
except ModuleNotFoundError:
    from article_processing_source_inventory import article_processing_inventory_matches


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "background-worker"
OWNER_LABEL = "io.evening-news.background-worker-gate"
ARTICLE_PROCESSING_TOKEN = "fixture-only-article-processing-token"
ARTICLE_AI_TOKEN = "fixture-only-article-ai-token"
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
            "ARTICLE_PROCESSING_URL": "http://processing:8094",
            "ARTICLE_PROCESSING_TOKEN": ARTICLE_PROCESSING_TOKEN,
            "ARTICLE_AI_TOKEN": ARTICLE_AI_TOKEN,
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
        raise GateFailure("legacy Python public web fixture retired; use the Go web job-worker gate")

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
    parser.add_argument("--image", required=True, help="Existing private Python app/executor image.")
    parser.add_argument("--web-image", default="evening-news-web:components-20261026",
                        help="Existing Go web image; this entrypoint never builds images.")
    parser.add_argument("--worker-image", required=True, help="Existing Go job-worker image.")
    parser.add_argument("--processing-image", required=True, help="Existing Go processing image.")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "go-job-worker-fixture",
                        help="Directory for the Go web acceptance report.")
    args = parser.parse_args()
    if not shutil.which("docker"):
        parser.error("docker CLI is required")
    try:
        from scripts.run_go_job_worker_fixture_gate import GoJobWorkerGate
    except ModuleNotFoundError:
        from run_go_job_worker_fixture_gate import GoJobWorkerGate
    gate = GoJobWorkerGate(args.image, args.worker_image, args.output_dir,
                           args.processing_image, args.web_image)
    try:
        gate.run()
    except Exception as exc:
        failure = str(exc)
        for secret in (ARTICLE_PROCESSING_TOKEN, ARTICLE_AI_TOKEN,
                       "fixture-only-executor-token", "fixture-only-password",
                       "fixture-admin-password", "fixture-only-gemini-key"):
            failure = failure.replace(secret, "[redacted]")
        gate.report.update(result="failed", failure=failure[:500])
    finally:
        gate.cleanup()
        report_path = gate.output_dir / f"{gate.run_id}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(gate.report, indent=2, sort_keys=True))
    return 0 if gate.report.get("result") == "passed" and not gate.report["cleanup_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
