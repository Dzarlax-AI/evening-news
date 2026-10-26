#!/usr/bin/env python3
"""Exercise Go web, its durable worker, and the Python/Go article pipeline locally."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import re
import select
import signal
import socket
import socketserver
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

try:
    from scripts.run_go_ingestion_fixture_gate import (
        COLLECTOR_TOKEN, DB_PASSWORD, EXECUTOR_TOKEN, IngestionGate, ORIGIN_IP,
        TOKEN as INGESTION_TOKEN,
    )
    from scripts.run_background_worker_fixture_gate import GateFailure
except ModuleNotFoundError:
    from run_go_ingestion_fixture_gate import (
        COLLECTOR_TOKEN, DB_PASSWORD, EXECUTOR_TOKEN, IngestionGate, ORIGIN_IP,
        TOKEN as INGESTION_TOKEN,
    )
    from run_background_worker_fixture_gate import GateFailure


IMAGES = {
    "web": "evening-news-web:components-20261026",
    "app": "evening-news-web-app:components-20261026",
    "worker": "evening-news-job-worker:web-components-20261026",
    "collector": "evening-news-collector:web-components-20261026",
    "ingestion": "evening-news-ingestion:web-components-20261026",
    "processing": "evening-news-processing:web-components-20261026",
}
WEB_BACKEND_TOKEN = "fixture-only-web-backend-token"
SCHEDULED_TOKEN = "fixture-only-scheduled-jobs-token"
ADMIN_USER = "fixture-admin"
ADMIN_PASSWORD = "fixture-admin-password"
WEB_FIXTURES = Path(__file__).resolve().parents[1] / "tests/fixtures/go-web"
MAX_RELAY_CONNECTIONS = 8
RELAY_IDLE_TIMEOUT_SECONDS = 120
RELAY_COPY_CHUNK_BYTES = 64 * 1024
RELAY_START_TIMEOUT_SECONDS = 5


def docker_create_options(container_name: str, gate_web_name: str,
                          backup_volume: str) -> list[str]:
    """Return gate-only mounts; web access uses a loopback exec relay when needed."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", backup_volume):
        raise GateFailure("unsafe_fixture_volume_name")
    if container_name.endswith("-seed"):
        return ["--mount", f"type=bind,src={WEB_FIXTURES},dst=/web-fixtures,readonly"]
    if container_name.endswith("-fake"):
        scheduler_fixtures = Path(__file__).resolve().parents[1] / "tests/fixtures/go-scheduler"
        return ["--mount", f"type=bind,src={scheduler_fixtures},dst=/scheduler,readonly"]
    if container_name.endswith("-executor"):
        return ["--mount", f"type=volume,src={backup_volume},dst=/app/backups"]
    return []


class GoWebGate(IngestionGate):
    def __init__(self, output_dir: Path):
        super().__init__(IMAGES["app"], IMAGES["worker"], IMAGES["collector"],
                         IMAGES["ingestion"], output_dir, IMAGES["processing"])
        self.web_image = IMAGES["web"]
        self.report.update(
            gate="go-web-full-pipeline-fixture", actual_go_web_tested=False,
            actual_python_executor_tested=False, actual_go_job_worker_tested=False,
            actual_go_collector_tested=False, actual_go_ingestion_tested=False,
            actual_go_processing_tested=False, source_coverage={"rss": 4, "telegram": 1,
                                                                 "page_monitor": 3},
            production_database_used=False, production_origins_used=False,
            real_ai_provider_used=False, real_messaging_used=False,
            external_network_egress=False, browser_tested=False, throughput_claimed=False,
        )
        self.report["image_tags"] = dict(IMAGES)
        self.backup_volume = self.prefix + "-backups"
        self.host_port = None
        self.web_relay = None
        self.access_method = None
        self.report["checks"] = {}
        self.image_refs = dict(IMAGES)


    def docker(self, *args, **kwargs):
        # Keep every fixture container on the internal network without host ports.
        if args and args[0] == "create" and "--name" in args:
            name = args[args.index("--name") + 1]
            additions = docker_create_options(name, self.prefix + "-web", self.backup_volume)
            if additions:
                mutable = list(args)
                image_index = next((i for i, part in enumerate(mutable)
                                    if part in {*self.image_refs.values(), "postgres:17-alpine", "python:3.12-alpine"}), None)
                if image_index is None:
                    raise GateFailure("fixture_container_image_not_found")
                mutable[image_index:image_index] = additions
                args = tuple(mutable)
        return super().docker(*args, **kwargs)

    def cleanup(self):
        if self.web_relay is not None:
            self.web_relay.stop()
            self.web_relay = None
        super().cleanup()
        label = self.docker("volume", "inspect", "--format",
                            '{{ index .Labels "' + self.owner_label + '" }}',
                            self.backup_volume, check=False)
        if label.returncode == 0 and label.stdout.strip() == self.owner:
            self.docker("volume", "rm", self.backup_volume, check=False)

    @property
    def owner_label(self):
        try:
            from scripts.run_background_worker_fixture_gate import OWNER_LABEL
        except ModuleNotFoundError:
            from run_background_worker_fixture_gate import OWNER_LABEL
        return OWNER_LABEL

    def app_environment(self, role):
        if role == "web":
            return {
                "DATABASE_URL": f"postgres://gate_user:{DB_PASSWORD}@postgres:5432/gate_db",
                "ADMIN_USERNAME": ADMIN_USER,
                "ADMIN_PASSWORD": ADMIN_PASSWORD,
                "JWT_SECRET": "fixture-only-jwt-signing-value",
                "WEB_BACKEND_URL": "http://executor:8100",
                "WEB_BACKEND_TOKEN": WEB_BACKEND_TOKEN,
                "SCHEDULED_JOBS_TOKEN": SCHEDULED_TOKEN,
                "SCHEDULED_JOBS_RECEIPT_LIMIT": "100000",
                "SCHEDULER_API_URL": "http://fixture-services:8090",
                "SCHEDULER_API_TOKEN": "fixture-only-scheduler-token",
                "SCHEDULER_API_ALLOW_HTTP": "true",
                "SCHEDULE_DISPATCH_MODE": "service",
                "BACKGROUND_EXECUTION_MODE": "worker",
                "SITE_BASE_URL": "http://127.0.0.1",
                "WEB_LISTEN_ADDRESS": ":8000",
                "ALLOWED_ORIGINS": "http://127.0.0.1",
                "TRUSTED_HOSTS": "127.0.0.1,localhost",
                "TRUST_FORWARD_AUTH": "false",
                "TRUSTED_PROXY_CIDRS": "",
                "DEVELOPMENT": "true",
            }
        env = super().app_environment(role)
        env.update(
            DATABASE_URL=f"postgresql+asyncpg://gate_user:{DB_PASSWORD}@postgres:5432/gate_db",
            ADMIN_USERNAME=ADMIN_USER, ADMIN_PASSWORD=ADMIN_PASSWORD,
            JWT_SECRET="fixture-only-jwt-signing-value",
            WEB_BACKEND_URL="http://executor:8100",
            WEB_BACKEND_TOKEN=WEB_BACKEND_TOKEN,
            SCHEDULED_JOBS_TOKEN=SCHEDULED_TOKEN,
            SCHEDULER_API_URL="http://fixture-services:8090",
            SCHEDULER_API_TOKEN="fixture-only-scheduler-token",
            SCHEDULER_API_ALLOW_HTTP="true", SCHEDULE_DISPATCH_MODE="service",
            BACKGROUND_EXECUTION_MODE="worker", TRUSTED_HOSTS="localhost,127.0.0.1",
            ARTICLE_INGESTION_MODE="go", ARTICLE_INGESTION_URL="http://ingestion:8093",
            ARTICLE_INGESTION_TOKEN=INGESTION_TOKEN,
            ARTICLE_PROCESSING_URL="http://processing:8094",
            ARTICLE_PROCESSING_TOKEN="fixture-only-processing-token",
            ARTICLE_AI_TOKEN="fixture-only-ai-token", JOB_EXECUTOR_TOKEN=EXECUTOR_TOKEN,
            PYTHONPATH="/gate:/background:/app", PYTHONUNBUFFERED="1",
            SCHEDULER_GATE_ROLE=role,
        )
        if role == "executor":
            env["PYTHONPATH"] = "/scheduler:/gate:/background:/app"
        return env

    def run_container(self, name, *, command, alias=None, env=None, mounts=True, detached=True,
                      image=None, memory=None, cpus="1", pids_limit=128, health_cmd=None,
                      disable_healthcheck=False):
        # Parent setup's bounded Docker configuration remains in use. This
        # wrapper only adds a read-only fixture mount for Python gate probes.
        super().run_container(name, command=command, alias=alias, env=env, mounts=mounts,
                              detached=detached, image=image, memory=memory, cpus=cpus,
                              pids_limit=pids_limit, health_cmd=health_cmd,
                              disable_healthcheck=disable_healthcheck)

    def request(self, method, path, *, token=None, body=None, headers=None, timeout=15):
        request_headers = dict(headers or {})
        if token:
            request_headers["Authorization"] = "Bearer " + token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            request_headers.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(self.base_url + path, data=data,
                                     headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read(5 * 1024 * 1024)
                content_type = response.headers.get_content_type().lower()
                if content_type == "application/json" or content_type.endswith("+json"):
                    return response.status, json.loads(raw) if raw else None
                return response.status, raw.decode(response.headers.get_content_charset() or "utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read(64 * 1024)
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = None
            return exc.code, payload

    def request_bytes(self, method, path, data, *, token=None, headers=None, timeout=30):
        request_headers = dict(headers or {})
        if token:
            request_headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(self.base_url + path, data=data,
                                     headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.status, response.read(64 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read(64 * 1024)

    @property
    def base_url(self):
        return f"http://127.0.0.1:{self.host_port}"

    def check(self, name, condition):
        self.report["checks"][name] = bool(condition)
        if not condition:
            raise GateFailure("acceptance_check_failed:" + name)

    def wait_for_web_port(self):
        # Use a real binding only when Docker exposes an explicitly-loopback
        # mapping. Docker 29 may ignore internal-network publications; the
        # fallback relay reaches only this owned container's loopback listener.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            result = self.docker("port", self.prefix + "-web", "8000/tcp", check=False)
            if result.returncode == 0 and result.stdout.strip():
                binding = result.stdout.strip().splitlines()[0]
                host, port = binding.rsplit(":", 1)
                if host not in {"127.0.0.1", "127.0.0.1/32"}:
                    self.capture_web_port_diagnostics()
                    raise GateFailure("go_web_host_binding_not_loopback")
                self.host_port = int(port)
                self.access_method = "published_loopback"
                return
            time.sleep(.2)
        self.capture_web_port_diagnostics()
        self.web_relay = LoopbackDockerExecRelay(self, self.prefix + "-web")
        self.host_port = self.web_relay.port
        self.access_method = "docker_exec_loopback"
        self.report["loopback_relay"] = {"host": "127.0.0.1", "port": self.host_port,
                                         "container_destination": "127.0.0.1:8000",
                                         "max_connections": MAX_RELAY_CONNECTIONS,
                                         "idle_timeout_seconds": RELAY_IDLE_TIMEOUT_SECONDS}

    def wait_for_web_ready(self, timeout=60):
        deadline = time.monotonic() + timeout
        last_status = None
        while time.monotonic() < deadline:
            try:
                last_status, _ = self.request("GET", "/health", timeout=3)
                if last_status == 200:
                    return
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(.5)
        self.capture_web_port_diagnostics()
        raise GateFailure("go_web_health_timeout:" + str(last_status))

    def capture_web_port_diagnostics(self):
        name = self.prefix + "-web"
        fields = {
            "host_config_port_bindings": "{{json .HostConfig.PortBindings}}",
            "network_settings_ports": "{{json .NetworkSettings.Ports}}",
            "configured_exposed_ports": "{{json .Config.ExposedPorts}}",
        }
        diagnostics = {"publish_requested_loopback": True}
        for key, template in fields.items():
            result = self.docker("inspect", "--format", template, name, check=False)
            if result.returncode:
                diagnostics[key] = {"inspect_available": False}
                continue
            try:
                diagnostics[key] = json.loads(result.stdout)
            except ValueError:
                diagnostics[key] = {"inspect_available": False}
        self.report["web_port_diagnostics"] = diagnostics

    def wait_job_http(self, job_id, timeout=240):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            status, body = self.request("GET", "/api/v1/jobs/" + job_id,
                                        token=self.access_token)
            if status == 200 and isinstance(body, dict):
                last = body
                if body.get("status") in {"succeeded", "failed", "delivery_unknown"}:
                    return body
            time.sleep(1)
        raise GateFailure("go_web_job_completion_timeout:" + str(last and last.get("status")))

    def fake_counters_http(self):
        script = ("import json,urllib.request; print(json.dumps(json.load(urllib.request.urlopen("
                  "'http://fixture-services:8090/__gate/counters',timeout=5))))")
        result = self.docker("exec", self.prefix + "-executor", "python", "-c", script,
                             check=False)
        if result.returncode:
            return {"available": False}
        try:
            return json.loads(result.stdout.splitlines()[-1])
        except (ValueError, IndexError):
            return {"available": False}

    def assert_go_web_environment_safe(self):
        raw = self.docker("inspect", "--format", "{{json .Config.Env}}", self.prefix + "-web").stdout
        try:
            names = {item.split("=", 1)[0] for item in json.loads(raw)}
        except (ValueError, TypeError):
            raise GateFailure("go_web_environment_inspection_failed") from None
        forbidden = {
            "GEMINI_API_KEY", "GEMINI_API_ENDPOINT", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID",
            "TELEGRAM_SERVICE_CHAT_ID", "TELEGRAPH_ACCESS_TOKEN", "ARTICLE_AI_TOKEN",
            "EXTRACTION_SERVICE_TOKEN", "ARTICLE_INGESTION_TOKEN", "ARTICLE_PROCESSING_TOKEN",
            "JOB_EXECUTOR_TOKEN", "BROWSER_WS_ENDPOINT",
        }
        self.check("go_web_provider_secrets_absent", names.isdisjoint(forbidden))

    def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for name, image in self.image_refs.items():
            self.report.setdefault("image_ids", {})[name] = self.docker(
                "image", "inspect", image, "--format", "{{.Id}}").stdout.strip()
        if not shutil.which("openssl"):
            raise GateFailure("openssl_required_for_isolated_fixture_tls")
        self.fixture_temp = Path(tempfile.mkdtemp(prefix="go-web-gate-"))
        self.fixture_temp.chmod(0o755)
        cert = self.fixture_temp / "fixture-ca.crt"
        key = self.fixture_temp / "fixture-ca.key"
        san = f"DNS:t.me,DNS:rss.fixture.test,DNS:pm.fixture.test,IP:{ORIGIN_IP}"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=go-web-fixture.test",
                        "-addext", f"subjectAltName={san}", "-addext", "basicConstraints=critical,CA:TRUE",
                        "-addext", "keyUsage=critical,digitalSignature,keyCertSign,keyEncipherment",
                        "-addext", "extendedKeyUsage=serverAuth"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cert.chmod(0o444)
        key.chmod(0o444)
        self.create_network()
        self.docker("volume", "create", "--label", f"{self.owner_label}={self.owner}",
                    self.backup_volume)
        self.run_container(self.prefix + "-postgres", command=["postgres"], alias="postgres",
                           env={"POSTGRES_DB": "gate_db", "POSTGRES_USER": "gate_user",
                                "POSTGRES_PASSWORD": DB_PASSWORD}, image="postgres:17-alpine",
                           mounts=False, memory="384m")
        self.run_container(self.prefix + "-origin", command=["python3", "/gate/gate_origin.py",
                           "--cert", "/fixtures/fixture-ca.crt", "--key", "/fixtures/fixture-ca.key"],
                           image="python:3.12-alpine", memory="128m")
        self.run_container(self.prefix + "-fake", command=["python", "/scheduler/fake_services.py"],
                           alias="fixture-services", memory="128m", disable_healthcheck=True)
        self.docker("exec", self.prefix + "-postgres", "sh", "-c",
                    "until pg_isready -U gate_user -d gate_db >/dev/null 2>&1; do sleep 1; done", timeout=90)

        # Explicit bootstrap is a one-shot app-image process; the Go web never migrates.
        self.run_container(self.prefix + "-migrate", command=["python", "-m", "news_aggregator.migrate"],
                           env=self.app_environment("migrate"), memory="512m")
        migration_state = self.docker("wait", self.prefix + "-migrate", timeout=120)
        self.check("explicit_python_migration_bootstrap", migration_state.stdout.strip() == "0")
        self.provision_runtime_role()
        self.run_container(self.prefix + "-seed", command=["python", "/web-fixtures/seed_sources.py"],
                           env=self.app_environment("seed"), memory="256m")
        seed_state = self.docker("wait", self.prefix + "-seed", timeout=60)
        self.check("eight_synthetic_sources_seeded", seed_state.stdout.strip() == "0")

        self.start_background_components()

        self.run_container(self.prefix + "-web", command=[], image=self.web_image,
                           env={**self.app_environment("web"),
                                "WEB_LISTEN_ADDRESS": ":8000", "SITE_BASE_URL": "http://127.0.0.1",
                                "ALLOWED_ORIGINS": "http://127.0.0.1", "TRUSTED_HOSTS": "127.0.0.1,localhost"},
                           mounts=False, memory="384m")
        self.wait_for_web_port()
        self.wait_for_web_ready()
        self.assert_go_web_environment_safe()
        self.check("fixture_network_internal", self.docker("network", "inspect", "--format",
                   "{{.Internal}}", self.network).stdout.strip().lower() == "true")
        port_inspect = json.loads(self.docker("inspect", "--format",
            "{{json .NetworkSettings.Ports}}", self.prefix + "-web").stdout)
        web_bindings = port_inspect.get("8000/tcp") or []
        if self.access_method == "published_loopback":
            self.check("go_web_host_binding_loopback_only", bool(web_bindings)
                       and all(binding.get("HostIp") == "127.0.0.1" for binding in web_bindings))
        else:
            self.check("go_web_host_binding_loopback_only", not web_bindings
                       and self.web_relay.bind_host == "127.0.0.1")
        self.report["access_method"] = self.access_method
        self.check("no_other_container_host_ports", all(
            not any(bindings or [] for port, bindings in json.loads(self.docker(
                "inspect", "--format", "{{json .NetworkSettings.Ports}}", container).stdout).items())
            for container in self.containers if container != self.prefix + "-web"))
        self.check("go_web_health", self.request("GET", "/health")[0] == 200)
        basic = base64.b64encode(f"{ADMIN_USER}:{ADMIN_PASSWORD}".encode()).decode("ascii")
        login_status, login = self.request("POST", "/auth/login",
                                           headers={"Authorization": "Basic " + basic})
        self.check("go_web_auth_login", login_status == 200 and isinstance(login, dict)
                   and login.get("token_type") == "bearer")
        self.access_token = login["access_token"]
        me_status, me = self.request("GET", "/auth/me", token=self.access_token)
        self.check("go_web_authenticated_identity", me_status == 200 and me.get("username") == ADMIN_USER)
        self.check("go_web_admin_html", self.request("GET", "/admin/", headers={
            "Authorization": "Basic Zml4dHVyZS1hZG1pbjpmaXh0dXJlLWFkbWluLXBhc3N3b3Jk"})[0] == 200)

        # Read-after-write through the real Go API confirms PostgreSQL persistence.
        create_status, created_source = self.request("POST", "/api/v1/sources", token=self.access_token,
            body={"name": "Go Web API fixture source", "source_type": "rss",
                  "url": "https://rss.fixture.test/rss.xml?edition=one", "is_active": True,
                  "update_interval": 3600})
        source_id = created_source.get("id") if isinstance(created_source, dict) else None
        list_status, source_list = self.request("GET", "/api/v1/sources", token=self.access_token)
        listed_sources = source_list.get("sources", []) if isinstance(source_list, dict) else []
        self.report["source_crud_http"] = {
            "create_status": create_status, "list_status": list_status,
            "created_id": source_id,
            "created_response_fields": sorted(created_source) if isinstance(created_source, dict) else [],
            "list_response_fields": sorted(source_list) if isinstance(source_list, dict) else [],
        }
        self.check("go_web_source_crud_persisted", create_status in {200, 201}
                   and list_status == 200 and isinstance(listed_sources, list)
                   and any(item.get("id") == source_id for item in listed_sources))
        toggle_status, _ = self.request("PATCH", f"/api/v1/sources/{source_id}/toggle",
                                        token=self.access_token)
        list_status, source_list = self.request("GET", "/api/v1/sources", token=self.access_token)
        listed_sources = source_list.get("sources", []) if isinstance(source_list, dict) else []
        self.check("go_web_source_toggle_persisted_without_changing_fixture_count",
                   toggle_status == 200 and any(item.get("id") == source_id
                                                and item.get("is_active") is False
                                                for item in listed_sources))

        # The Go web queues durable work; the sole Go worker then runs the real
        # executor -> ingestion -> collector -> processing -> fake Gemini path.
        run_status, accepted = self.request("POST", "/api/v1/process/run", token=self.access_token,
                                             headers={"Idempotency-Key": "go-web-process-once"})
        self.report["process_acceptance_http"] = {"status": run_status, "response": accepted}
        self.check("go_web_process_job_accepted", run_status == 202 and isinstance(accepted, dict)
                   and isinstance(accepted.get("job_id"), str))
        job = self.wait_job_http(accepted["job_id"])
        self.check("go_web_job_pipeline_succeeded", job.get("status") == "succeeded")
        self.report["pipeline_job"] = {"status": job.get("status"), "job_id": accepted["job_id"]}
        source_summary, processed_articles = self.source_receipt_check(accepted["job_id"])
        self.report["source_receipts"] = source_summary
        self.report["processed_fixture_articles"] = processed_articles
        self.check("source_collection_results_eight", source_summary.get("valid") is True)
        self.check("go_processing_persisted_completed_articles", processed_articles > 0)
        # Scheduled occurrence ingress is idempotent even when the scheduler
        # retries delivery after an ambiguous response.
        occurrence = "00000000-0000-4000-8000-000000000071"
        envelope = {"version": 1, "occurrence_id": occurrence,
                    "schedule_key": "news:news_processing", "schedule_revision": 1,
                    "scheduled_for": "2026-10-26T10:00:00Z", "operation": "news_processing",
                    "payload": {}}
        first_status, first_occurrence = self.request("POST", "/api/v1/internal/scheduled-jobs",
            token=SCHEDULED_TOKEN, body=envelope, headers={"Idempotency-Key": occurrence})
        second_status, second_occurrence = self.request("POST", "/api/v1/internal/scheduled-jobs",
            token=SCHEDULED_TOKEN, body=envelope, headers={"Idempotency-Key": occurrence})
        receipts = self.docker("exec", self.prefix + "-postgres", "psql", "-U", "gate_user",
            "-d", "gate_db", "-Atc",
            "SELECT count(*) FROM scheduler_occurrence_receipts WHERE occurrence_id='" + occurrence + "'")
        self.check("scheduled_occurrence_accepted_once", first_status == 202 and second_status == 200
                   and first_occurrence.get("job_id") == second_occurrence.get("job_id")
                   and receipts.stdout.strip() == "1")

        # Queue-only worker mode explicitly keeps AI category analysis disabled.
        ai_status, _ = self.request("POST", "/api/v1/categories/ai-category-analysis",
                                    token=self.access_token, body={"limit": 1})
        self.check("queue_mode_category_analysis_stays_disabled", ai_status == 409)
        telegram_status, telegram_body = self.request("POST", "/api/v1/telegram/test",
                                                       token=self.access_token, body={})
        self.check("telegram_test_uses_fake_provider", telegram_status == 200
                   and isinstance(telegram_body, dict))
        self.check("private_backup_list", self.request("GET", "/api/v1/backup",
                   token=self.access_token)[0] == 200)
        schedule_status, schedule_body = self.request("GET", "/api/v1/backup/schedule",
                                                       token=self.access_token)
        self.check("private_backup_schedule_static_route", schedule_status == 200
                   and isinstance(schedule_body, dict) and schedule_body.get("frequency") == "daily")
        boundary = "go-web-fixture-boundary"
        file_name = "go-web-fixture.tar.gz"
        multipart = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n"
                     "Content-Type: application/gzip\r\n\r\n").encode() + b"fixture-backup-bytes" + f"\r\n--{boundary}--\r\n".encode()
        upload_status, _ = self.request_bytes("POST", "/api/v1/backup/upload", multipart,
            token=self.access_token, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        self.check("worker_backup_upload_stays_disabled", upload_status == 409)
        # Existing worker-mode API forbids uploads and restore. Seed only this
        # disposable fixture's backup volume to verify read-only streaming.
        self.docker("exec", self.prefix + "-executor", "python", "-c",
                    "from pathlib import Path; p=Path('backups'); p.mkdir(exist_ok=True); "
                    "(p/'go-web-fixture.tar.gz').write_bytes(b'fixture-backup-bytes')")
        download_status, downloaded = self.request_bytes("GET", "/api/v1/backup/download/" + file_name,
                                                          b"", token=self.access_token)
        self.check("private_backup_download_bytes", download_status == 200 and downloaded == b"fixture-backup-bytes")
        restore_status, _ = self.request("POST", "/api/v1/backup/restore?filename=" + file_name,
                                         token=self.access_token)
        self.check("worker_backup_restore_stays_disabled", restore_status == 409)
        cleanup_status, _ = self.request("POST", "/api/v1/backup/cleanup?keep_count=5",
                                          token=self.access_token)
        self.check("worker_backup_cleanup_stays_disabled", cleanup_status == 409)
        migration_status, migration_body = self.request("POST", "/api/v1/system/migrations/run",
                                                          token=self.access_token, body={})
        self.check("private_migration_route_reachable", migration_status == 200
                   and isinstance(migration_body, dict)
                   and migration_body.get("message") == "Migrations completed"
                   and not migration_body.get("result", {}).get("errors"))
        self.report["provider_calls"] = self.fake_counters_http()
        providers = self.report["provider_calls"].get("providers", {})
        self.check("fake_provider_called_by_pipeline", int(providers.get("gemini", 0)) > 0)
        self.report["actual_go_runtime_tested"] = True
        for key in ("actual_go_web_tested", "actual_python_executor_tested",
                    "actual_go_job_worker_tested", "actual_go_collector_tested",
                    "actual_go_ingestion_tested", "actual_go_processing_tested"):
            self.report[key] = True
        self.capture_resource_snapshot()
        self.report["report"] = "Go web full pipeline fixture"
        self.report["actual_pipeline_tested"] = job.get("status") == "succeeded"

    def start_background_components(self):
        """Start the legacy standalone component containers for this gate."""
        self.run_container(self.prefix + "-collector", command=[], alias="collector",
                           image=self.collector_image,
                           env={"LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "true",
                                "EXTRACTOR_MAX_CONCURRENT": "1", "EXTRACTOR_SERVICE_TOKEN": COLLECTOR_TOKEN,
                                "SSL_CERT_FILE": "/fixtures/fixture-ca.crt"},
                           mounts=False, memory="192m")
        self.run_container(self.prefix + "-executor", command=[],
                           alias="executor", env=self.app_environment("executor"), memory="512m",
                           disable_healthcheck=True)
        self.wait_http(self.prefix + "-executor", "http://127.0.0.1:8100/internal/v1/status",
                       auth=EXECUTOR_TOKEN)
        retired = self.docker("exec", self.prefix + "-executor", "python", "-c",
            "from pathlib import Path; paths=[Path('news_aggregator/main.py'),Path('news_aggregator/public.py')]; "
            "paths+=list(Path('news_aggregator/api').rglob('*.py'))+list(Path('web/templates').rglob('*.html')); "
            "print(any(p.is_file() for p in paths))")
        self.check("python_public_web_absent_from_executor_image", retired.stdout.strip() == "False")
        self.run_container(self.prefix + "-ingestion", command=[], alias="ingestion",
                           image=self.ingestion_image,
                           env={"ARTICLE_INGESTION_LISTEN": ":8093",
                                "DATABASE_URL": f"postgres://news_ingestion_runtime:{DB_PASSWORD}@postgres:5432/gate_db?search_path=public",
                                "ARTICLE_INGESTION_TOKEN": INGESTION_TOKEN,
                                "EXTRACTION_SERVICE_URL": "http://collector:8080",
                                "EXTRACTION_SERVICE_TOKEN": COLLECTOR_TOKEN,
                                "EXTRACTION_SERVICE_NATIVE_BROWSER": "false",
                                "JOB_EXECUTOR_URL": "http://executor:8100",
                                "JOB_EXECUTOR_TOKEN": EXECUTOR_TOKEN,
                                "SSL_CERT_FILE": "/fixtures/fixture-ca.crt"},
                           mounts=False, memory="256m",
                           health_cmd="wget -q -O /dev/null http://127.0.0.1:8093/healthz")
        self.run_container(self.prefix + "-processing", command=[], alias="processing",
                           image=self.processing_image,
                           env={"ARTICLE_PROCESSING_LISTEN": ":8094",
                                "DATABASE_URL": f"postgres://news_processing_runtime:{DB_PASSWORD}@postgres:5432/gate_db?search_path=public",
                                "ARTICLE_PROCESSING_TOKEN": "fixture-only-processing-token",
                                "ARTICLE_AI_URL": "http://executor:8100",
                                "ARTICLE_AI_TOKEN": "fixture-only-ai-token",
                                "EXTRACTION_SERVICE_URL": "http://collector:8080",
                                "EXTRACTION_SERVICE_TOKEN": COLLECTOR_TOKEN,
                                "EXTRACTION_SERVICE_NATIVE_BROWSER": "false",
                                "SSL_CERT_FILE": "/fixtures/fixture-ca.crt",
                                "MAX_WORKERS": "1", "PROCESSING_BATCH_SIZE": "2"},
                           mounts=False, memory="128m",
                           health_cmd="wget -q -O /dev/null http://127.0.0.1:8094/healthz")
        self.wait_ready(self.prefix + "-collector")
        self.wait_ready(self.prefix + "-processing")
        self.wait_ready(self.prefix + "-ingestion")
        self.run_container(self.prefix + "-worker", command=[], image=self.worker_image,
                           env={**self.app_environment("worker"), "JOB_EXECUTOR_URL": "http://executor:8100"},
                           mounts=False, memory="128m")
        self.wait_worker_healthy(self.prefix + "-worker")


    def source_receipt_check(self, job_id):
        if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", job_id):
            raise GateFailure("accepted_job_id_invalid")
        query = (
            "SELECT COALESCE(json_agg(json_build_object('source_type',sr.source_type,"
            "'state',sr.state,'error_code',sr.error_code)), '[]'::json) "
            "FROM article_ingestion_source_runs sr "
            "JOIN article_ingestion_runs ir ON ir.id=sr.run_id "
            "JOIN sources s ON s.id=sr.source_id "
            f"WHERE ir.job_id='{job_id}'::uuid AND s.name LIKE 'Go Web Gate %'"
        )
        result = self.docker("exec", self.prefix + "-postgres", "psql", "-U", "gate_user",
                             "-d", "gate_db", "-Atc", query)
        try:
            receipts = json.loads(result.stdout.strip())
        except ValueError:
            raise GateFailure("source_receipt_query_invalid") from None
        counts = {"rss": 0, "telegram": 0, "custom": 0}
        valid = len(receipts) == 8
        for receipt in receipts:
            source_type = receipt.get("source_type")
            if source_type not in counts:
                valid = False
                continue
            counts[source_type] += 1
            if receipt.get("state") not in {"succeeded", "partial"} or receipt.get("error_code") is not None:
                valid = False
        valid = valid and counts == {"rss": 4, "telegram": 1, "custom": 3}
        article_query = (
            "SELECT count(*) FROM articles a JOIN sources s ON s.id=a.source_id "
            "WHERE s.name LIKE 'Go Web Gate %' AND a.processed=true "
            "AND a.summary_processed=true AND a.category_processed=true"
        )
        article_result = self.docker("exec", self.prefix + "-postgres", "psql", "-U", "gate_user",
                                     "-d", "gate_db", "-Atc", article_query)
        try:
            processed = int(article_result.stdout.strip())
        except ValueError:
            raise GateFailure("processed_article_query_invalid") from None
        return {"valid": valid, "counts": counts,
                "states": sorted({item.get("state", "") for item in receipts}),
                "error_count": sum(item.get("error_code") is not None for item in receipts)}, processed

    def capture_resource_snapshot(self):
        super().capture_resource_snapshot()
        states = {}
        for name in self.containers:
            result = self.docker("inspect", "--format",
                '{"status":"{{.State.Status}}","exit_code":{{.State.ExitCode}},'
                '"oom_killed":{{.State.OOMKilled}},"restarts":{{.RestartCount}}}',
                name, check=False)
            try:
                states[name.rsplit("-", 1)[-1]] = json.loads(result.stdout)
            except ValueError:
                states[name.rsplit("-", 1)[-1]] = {"inspect_available": False}
        self.report["runtime_states"] = states


class _RelayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = MAX_RELAY_CONNECTIONS

    def __init__(self, address, owner):
        self.owner = owner
        self.slots = threading.BoundedSemaphore(MAX_RELAY_CONNECTIONS)
        super().__init__(address, _RelayHandler)


class _RelayHandler(socketserver.BaseRequestHandler):
    def handle(self):
        server = self.server
        if not server.slots.acquire(blocking=False):
            return
        child = None
        try:
            child = server.owner.exec_nc()
            client = self.request
            client.settimeout(RELAY_IDLE_TIMEOUT_SECONDS)
            child.stdout  # ensure pipe exists
            finished = threading.Event()

            def socket_to_child():
                try:
                    while not finished.is_set():
                        data = client.recv(RELAY_COPY_CHUNK_BYTES)
                        if not data:
                            try:
                                child.stdin.close()
                            except OSError:
                                pass
                            return
                        child.stdin.write(data)
                        child.stdin.flush()
                except (OSError, BrokenPipeError):
                    pass

            def child_to_socket():
                try:
                    while not finished.is_set():
                        data = child.stdout.read(RELAY_COPY_CHUNK_BYTES)
                        if not data:
                            return
                        client.sendall(data)
                except (OSError, BrokenPipeError):
                    pass

            upstream = threading.Thread(target=socket_to_child, daemon=True)
            downstream = threading.Thread(target=child_to_socket, daemon=True)
            upstream.start()
            downstream.start()
            deadline = time.monotonic() + RELAY_IDLE_TIMEOUT_SECONDS
            while time.monotonic() < deadline and child.poll() is None:
                if not upstream.is_alive() and not downstream.is_alive():
                    break
                upstream.join(timeout=.1)
                downstream.join(timeout=.1)
            finished.set()
        finally:
            if child is not None:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=2)
                for stream in (child.stdin, child.stdout):
                    try:
                        stream.close()
                    except (OSError, AttributeError):
                        pass
                server.owner.reap_child(child)
            server.slots.release()


class LoopbackDockerExecRelay:
    """Temporary loopback TCP access to one owned container's loopback listener."""

    bind_host = "127.0.0.1"

    def __init__(self, gate, container):
        if container != gate.prefix + "-web":
            raise GateFailure("relay_container_not_owned_web")
        self.gate = gate
        self.container = container
        self.children = set()
        self.children_lock = threading.Lock()
        self.stopping = False
        self._verify_container()
        self.server = _RelayServer((self.bind_host, 0), self)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def _verify_container(self):
        result = self.gate.docker("inspect", "--format",
            '{{ index .Config.Labels "' + self.gate.owner_label + '" }}',
            self.container, check=False)
        if result.returncode or result.stdout.strip() != self.gate.owner:
            raise GateFailure("relay_container_owner_mismatch")
        check = self.gate.docker("exec", self.container, "busybox", "nc", "--help",
                                 check=False, timeout=RELAY_START_TIMEOUT_SECONDS)
        if check.returncode not in (0, 1) or "nc" not in ((check.stdout or "") + (check.stderr or "")).lower():
            raise GateFailure("relay_busybox_nc_unavailable")

    def exec_nc(self):
        with self.children_lock:
            if self.stopping:
                raise GateFailure("relay_is_stopping")
        child = subprocess.Popen(
            ["docker", "exec", "-i", self.container, "busybox", "nc", "127.0.0.1", "8000"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            bufsize=0, close_fds=True,
        )
        with self.children_lock:
            if self.stopping:
                child.terminate()
                child.wait(timeout=2)
                raise GateFailure("relay_is_stopping")
            self.children.add(child)
        return child

    def reap_child(self, child):
        with self.children_lock:
            self.children.discard(child)

    def stop_children(self):
        with self.children_lock:
            self.stopping = True
            children = list(self.children)
        for child in children:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=2)

    def stop(self):
        self.stop_children()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def bounded_qa_hold(value):
    try:
        seconds = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("QA hold must be an integer from 0 through 300") from None
    if seconds < 0 or seconds > 300:
        raise argparse.ArgumentTypeError("QA hold must be an integer from 0 through 300")
    return seconds


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/go-web-fixture"))
    parser.add_argument("--qa-hold-seconds", type=bounded_qa_hold, default=0,
                        help="Keep the loopback Go web available for an interactive check after acceptance")
    args = parser.parse_args(argv)
    gate = GoWebGate(args.output.resolve())
    try:
        gate.run()
        if args.qa_hold_seconds:
            gate.report["phase"] = "qa_ready"
            gate.report["qa_url"] = gate.base_url
            args.output.mkdir(parents=True, exist_ok=True)
            (args.output / "report.json").write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
            print(f"QA_READY {gate.base_url} (synthetic fixture credentials only)", flush=True)
            time.sleep(args.qa_hold_seconds)
    except (GateFailure, subprocess.TimeoutExpired, OSError) as exc:
        gate.report["failure"] = str(exc)[:400]
        raise
    except Exception as exc:
        gate.report["failure"] = "unexpected_gate_error:" + type(exc).__name__
        raise
    finally:
        gate.capture_resource_snapshot()
        gate.cleanup()
        gate.report["cleanup_complete"] = not gate.report.get("cleanup_errors")
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / "report.json").write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
        if gate.fixture_temp:
            shutil.rmtree(gate.fixture_temp, ignore_errors=True)
    print(json.dumps(gate.report, sort_keys=True))
    return 0 if gate.report.get("actual_pipeline_tested") else 1


if __name__ == "__main__":
    raise SystemExit(main())
