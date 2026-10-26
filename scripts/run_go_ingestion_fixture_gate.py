#!/usr/bin/env python3
"""Run the real Go ingestion path against synthetic, isolated fixture services."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from run_go_job_worker_fixture_gate import GoJobWorkerGate
from run_background_worker_fixture_gate import DockerGate, GateFailure, OWNER_LABEL

ROOT = Path(__file__).resolve().parents[1]
GATE_FIXTURES = ROOT / "tests/fixtures/go-ingestion"
ORIGIN_FIXTURES = ROOT / "tests/fixtures/go-source-manager"
NETWORK = ipaddress.ip_network("11.203.254.240/28")
ORIGIN_IP = "11.203.254.243"
TOKEN = "fixture-only-ingestion-token"
COLLECTOR_TOKEN = "fixture-only-collector-token"
EXECUTOR_TOKEN = "fixture-only-executor-token"
DB_PASSWORD = "fixture-only-password"
FIXTURE_LOG_SECRET = re.compile(
    r"(?i)(password|token|secret|api[_-]?key)(\s*[=:]\s*)([^\s,;]+)"
)


def sanitize_fixture_log(text):
    return [FIXTURE_LOG_SECRET.sub(r"\1\2[redacted]", line)[:400]
            for line in (text or "").splitlines()[-40:]]


def has_host_port_binding(ports):
    """Return whether Docker inspect reports a host-side port binding.

    Exposed container ports commonly appear as ``{"8000/tcp": null}``; that
    does not publish a host port. Fail closed for unexpected inspect shapes.
    """
    if not isinstance(ports, dict):
        return True
    for bindings in ports.values():
        if bindings in (None, []):
            continue
        if not isinstance(bindings, list):
            return True
        for binding in bindings:
            if not isinstance(binding, dict):
                return True
            if binding.get("HostPort"):
                return True
    return False


class IngestionGate(GoJobWorkerGate):
    def __init__(self, app_image, worker_image, collector_image, ingestion_image, output_dir, processing_image):
        super().__init__(app_image, worker_image, output_dir, processing_image)
        self.collector_image = collector_image
        self.ingestion_image = ingestion_image
        self.processing_image = processing_image
        self.report.update(gate="go-article-ingestion-docker-fixture",
                           actual_go_ingestion_tested=False, actual_go_processing_tested=False, actual_go_collector_tested=False,
                           actual_python_executor_tested=False, browser_tested=False,
                           actual_go_runtime_tested=False, actual_go_job_worker_tested=False,
                           production_database_used=False, production_origins_used=False,
                           real_ai_provider_used=False, real_messaging_used=False,
                           external_network_egress=False)
        self.extra_hosts = ["rss.fixture.test", "pm.fixture.test", "t.me"]
        self.fixture_temp = None

    def docker(self, *args, **kwargs):
        return DockerGate.docker(self, *args, **kwargs)

    def cleanup(self):
        for name in reversed(self.containers):
            label = self.docker("inspect", "--format",
                                f'{{{{ index .Config.Labels "{OWNER_LABEL}" }}}}',
                                name, check=False)
            if label.returncode == 0 and label.stdout.strip() == self.owner:
                removed = self.docker("rm", "-f", "-v", name, check=False, timeout=30)
                if removed.returncode:
                    self.report["cleanup_errors"].append(
                        "owned_container_cleanup_failed:" + name.rsplit("-", 1)[-1])
        DockerGate.cleanup(self)

    def postgres_diagnostic(self, name):
        return self.container_diagnostic(name, "postgres")

    def container_diagnostic(self, name, role):
        state_result = self.docker("inspect", "--format", "{{json .State}}", name, check=False)
        try:
            raw_state = json.loads(state_result.stdout)
        except ValueError:
            raw_state = {}
        logs = self.docker("logs", "--tail", "40", name, check=False)
        return {
            "role": role,
            "status": raw_state.get("Status"),
            "exit_code": raw_state.get("ExitCode"),
            "oom_killed": raw_state.get("OOMKilled"),
            "docker_error_tail": sanitize_fixture_log(state_result.stderr),
            "log_stdout_tail": sanitize_fixture_log(logs.stdout),
            "log_stderr_tail": sanitize_fixture_log(logs.stderr),
        }

    def capture_resource_snapshot(self):
        snapshot = {}
        memory_mib = {}
        restart_counts = {}
        for name in self.containers:
            role = name.rsplit("-", 1)[-1]
            stats = self.docker("stats", "--no-stream", "--format", "{{.MemUsage}}\t{{.CPUPerc}}",
                                name, check=False)
            stats_line = (stats.stdout or "").strip().splitlines()
            memory_usage = cpu_percent = None
            if stats.returncode == 0 and stats_line:
                columns = stats_line[-1].split("\t", 1)
                if len(columns) == 2:
                    memory_usage, cpu_percent = columns
            restart = self.docker("inspect", "--format", "{{.RestartCount}}", name, check=False)
            restart_count = None
            try:
                if restart.returncode == 0:
                    restart_count = int(restart.stdout.strip())
            except ValueError:
                pass
            snapshot[role] = {"memory_usage": memory_usage, "cpu_percent": cpu_percent,
                              "restart_count": restart_count,
                              "stats_available": stats.returncode == 0}
            if memory_usage:
                match = re.match(r"([0-9.]+)\s*(B|KiB|MiB|GiB)", memory_usage)
                if match:
                    amount, unit = float(match.group(1)), match.group(2)
                    memory_mib[role] = round(amount / 1048576 if unit == "B" else
                                             amount / 1024 if unit == "KiB" else
                                             amount * 1024 if unit == "GiB" else amount, 2)
                else:
                    memory_mib[role] = None
            else:
                memory_mib[role] = None
            restart_counts[role] = restart_count
        self.report["resource_snapshot"] = {
            "measurement": "one docker stats snapshot immediately before cleanup; not a peak or load test",
            "roles": snapshot,
        }
        self.report["memory_mib"] = memory_mib
        self.report["docker_restart_counts"] = restart_counts

    def create_network(self):
        result = self.docker("network", "ls", "-q", "--filter", f"name=^{self.network}$", check=False)
        if result.stdout.strip():
            raise GateFailure("fixture_network_name_collision")
        existing = self.docker("network", "ls", "-q", check=False).stdout.splitlines()
        for network_id in existing:
            inspect = self.docker("network", "inspect", "--format", "{{json .IPAM.Config}}",
                                  network_id, check=False)
            try:
                configs = json.loads(inspect.stdout or "[]")
            except ValueError:
                configs = []
            for config in configs or []:
                subnet = config.get("Subnet")
                if subnet:
                    try:
                        if NETWORK.overlaps(ipaddress.ip_network(subnet, strict=False)):
                            raise GateFailure("fixture_subnet_overlaps_existing_docker_network")
                    except ValueError:
                        continue
        self.docker("network", "create", "--internal", "--driver", "bridge", "--subnet",
                    str(NETWORK), "--gateway", str(NETWORK.network_address + 1),
                    "--label", f"{OWNER_LABEL}={self.owner}", self.network)
        self.report["network_name"] = self.network

    def run_container(self, name, *, command, alias=None, env=None, mounts=True, detached=True,
                      image=None, memory=None, cpus="1", pids_limit=128, health_cmd=None,
                      disable_healthcheck=False):
        image = image or self.image
        args = ["create", "--name", name, "--pull=never", "--network", self.network,
                "--label", f"{OWNER_LABEL}={self.owner}",
                "--label", f"io.evening-news.gate-role={name.rsplit('-', 1)[-1]}",
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                "--cpus", cpus, "--pids-limit", str(pids_limit)]
        if name.endswith("-postgres"):
            args.extend(["--user", "70:70", "--tmpfs",
                         "/var/lib/postgresql/data:rw,size=512m,uid=70,gid=70,mode=0700"])
        if memory:
            args.extend(["--memory", memory, "--memory-swap", memory])
        if name.endswith("-origin"):
            args.extend(["--ip", ORIGIN_IP])
            args.extend(["--cap-add", "NET_BIND_SERVICE"])
            for host in self.extra_hosts:
                args.extend(["--network-alias", host])
        if health_cmd:
            args.extend(["--health-cmd", health_cmd, "--health-interval", "5s",
                         "--health-timeout", "5s", "--health-retries", "3",
                         "--health-start-period", "10s"])
        elif disable_healthcheck:
            args.append("--no-healthcheck")
        if alias:
            args.extend(["--network-alias", alias])
        if mounts:
            args.extend(["--mount", f"type=bind,src={GATE_FIXTURES},dst=/gate,readonly",
                         "--mount", f"type=bind,src={ROOT / 'tests/fixtures/background-worker'},dst=/background,readonly",
                         "--mount", f"type=bind,src={ORIGIN_FIXTURES},dst=/origin-fixtures,readonly"])
        if name.endswith(("-origin", "-collector", "-ingestion", "-processing")) and self.fixture_temp:
            args.extend(["--mount", f"type=bind,src={self.fixture_temp},dst=/fixtures,readonly"])
        for host in self.extra_hosts:
            args.extend(["--add-host", f"{host}:{ORIGIN_IP}"])
        for key, value in (env or {}).items():
            args.extend(["--env", f"{key}={value}"])
        args.extend([image, *command])
        self.docker(*args)
        self.containers.append(name)
        if detached:
            started = self.docker("start", name, check=False)
            if started.returncode:
                role = name.rsplit("-", 1)[-1]
                self.report["container_start_failure"] = {
                    "role": role,
                    "docker_stderr_tail": sanitize_fixture_log(started.stderr),
                    "docker_stdout_tail": sanitize_fixture_log(started.stdout),
                    "container": self.container_diagnostic(name, role),
                }
                raise GateFailure("fixture_container_start_failed:" + role)

    def app_environment(self, role):
        env = super().app_environment(role)
        env.update(
            ARTICLE_PROCESSING_URL="http://processing:8094",
            ARTICLE_PROCESSING_TOKEN="fixture-only-processing-token",
            ARTICLE_AI_TOKEN="fixture-only-ai-token",
            ARTICLE_INGESTION_MODE="go",
            ARTICLE_INGESTION_URL="http://ingestion:8093",
            ARTICLE_INGESTION_TOKEN=TOKEN,
            JOB_EXECUTOR_TOKEN=EXECUTOR_TOKEN,
            PYTHONPATH="/gate:/background:/app",
            NEWS_LIMIT_MAX_ARTICLES="6", NEWS_LIMIT_PER_SOURCE="2",
            NEWS_LIMIT_ENABLED="false", MAX_WORKERS="1", PROCESSING_BATCH_SIZE="2",
        )
        return env

    def private_probe(self, action, *arguments):
        output = self.docker("exec", f"{self.prefix}-web", "python", "/gate/gate_probe.py",
                             action, *arguments).stdout.strip()
        try:
            return json.loads(output.splitlines()[-1])
        except (ValueError, IndexError):
            raise GateFailure("fixture_probe_result_invalid") from None

    def check(self, name, condition):
        self.report["checks"][name] = bool(condition)
        if not condition:
            raise GateFailure("acceptance_check_failed:" + name)

    def wait_ready(self, name, *, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.docker("inspect", "--format", "{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{end}}",
                                name, check=False).stdout.strip()
            if state.endswith("/healthy") or state == "running/":
                return
            if state.startswith("exited/") or state.startswith("dead/"):
                if name.endswith("-postgres"):
                    self.report["fixture_postgres_diagnostic"] = self.postgres_diagnostic(name)
                raise GateFailure("fixture_container_exited:" + name.rsplit("-", 1)[-1])
            time.sleep(1)
        raise GateFailure("fixture_container_readiness_timeout:" + name.rsplit("-", 1)[-1])

    def wait_job(self, job_id, *, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.private_probe("job", job_id)
            if result.get("http_status") == 200:
                job = result.get("response", {})
                if job.get("status") in {"succeeded", "failed", "delivery_unknown"}:
                    return job
            time.sleep(1)
        self.capture_timeout_diagnostics(job_id)
        raise GateFailure("fixture_job_timeout")

    def capture_timeout_diagnostics(self, job_id):
        """Preserve sanitized evidence before fixture cleanup on a stuck job."""
        diagnostics = {"job_id": job_id}
        for key, action, args in (
            ("job", "job", (job_id,)),
            ("fixture_snapshot", "snapshot", ()),
        ):
            try:
                diagnostics[key] = self.private_probe(action, *args).get("response", {})
            except Exception as exc:
                diagnostics[key + "_failure_type"] = type(exc).__name__
        logs = {}
        for role in ("collector", "ingestion", "processing", "executor", "worker"):
            result = self.docker("logs", "--tail", "40", f"{self.prefix}-{role}", check=False)
            logs[role] = sanitize_fixture_log((result.stdout or "") + "\n" + (result.stderr or ""))
        diagnostics["sanitized_logs"] = logs
        self.report["timeout_diagnostics"] = diagnostics

    def seed(self):
        self.docker("exec", f"{self.prefix}-web", "python", "/gate/gate_seed.py", timeout=30)
        return self.private_probe("snapshot")

    def provision_runtime_role(self):
        source = (ROOT / "deploy/news-ingestion/provision-runtime-role.sql").read_text()
        if source.count("\\password news_ingestion_runtime") != 1:
            raise GateFailure("runtime_role_template_password_prompt_missing")
        source = source.replace("\\password news_ingestion_runtime",
                                "ALTER ROLE news_ingestion_runtime PASSWORD 'fixture-only-password';")
        path = self.fixture_temp / "provision-fixture.sql"
        path.write_text(source)
        self.docker("cp", str(path), f"{self.prefix}-postgres:/tmp/provision-fixture.sql")
        self.docker("exec", f"{self.prefix}-postgres", "psql", "-v", "ON_ERROR_STOP=1",
                    "-v", "news_schema=public", "-U", "gate_user", "-d", "gate_db",
                    "-f", "/tmp/provision-fixture.sql", timeout=30)
        processing = (ROOT / "deploy/news-processing/provision-runtime-role.sql").read_text()
        if processing.count("\\password news_processing_runtime") != 1:
            raise GateFailure("processing_role_password_prompt_missing")
        processing = processing.replace("\\password news_processing_runtime",
                                        "ALTER ROLE news_processing_runtime PASSWORD 'fixture-only-password';")
        path = self.fixture_temp / "provision-processing.sql"
        path.write_text(processing)
        self.docker("cp", str(path), f"{self.prefix}-postgres:/tmp/provision-processing.sql")
        self.docker("exec", f"{self.prefix}-postgres", "psql", "-v", "ON_ERROR_STOP=1",
                    "-v", "news_schema=public", "-U", "gate_user", "-d", "gate_db",
                    "-f", "/tmp/provision-processing.sql", timeout=30)


    def wait_http(self, name, url, *, timeout=90, auth=None):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            headers = {"Authorization": "Bearer " + auth} if auth else {}
            script = ("import urllib.request; r=urllib.request.Request(" + repr(url) + ",headers="
                      + repr(headers) + "); print(urllib.request.urlopen(r,timeout=2).status)")
            result = self.docker("exec", name, "python", "-c", script, check=False)
            if result.returncode == 0 and result.stdout.strip().endswith("200"):
                return
            state = self.docker("inspect", "--format", "{{.State.Status}}", name, check=False).stdout.strip()
            if state != "running":
                raise GateFailure("fixture_service_exited:" + name.rsplit("-", 1)[-1])
            time.sleep(1)
        raise GateFailure("fixture_http_readiness_timeout:" + name.rsplit("-", 1)[-1])

    def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for image in (self.image, self.worker_image, self.collector_image, self.ingestion_image, self.processing_image):
            self.docker("image", "inspect", image, "--format", "{{.Id}}")
        self.report["image_ids"] = {
            key: self.docker("image", "inspect", value, "--format", "{{.Id}}").stdout.strip()
            for key, value in (("app", self.image), ("worker", self.worker_image),
                               ("collector", self.collector_image), ("ingestion", self.ingestion_image),
                               ("processing", self.processing_image))
        }
        if not shutil.which("openssl"):
            raise GateFailure("openssl_required_for_fixture_tls")
        self.fixture_temp = Path(tempfile.mkdtemp(prefix="go-ingestion-gate-"))
        self.fixture_temp.chmod(0o755)
        cert = self.fixture_temp / "fixture-ca.crt"
        key = self.fixture_temp / "fixture-ca.key"
        san = f"DNS:t.me,DNS:rss.fixture.test,DNS:pm.fixture.test,IP:{ORIGIN_IP}"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=go-ingestion-fixture.test",
                        "-addext", f"subjectAltName={san}", "-addext", "basicConstraints=critical,CA:TRUE",
                        "-addext", "keyUsage=critical,digitalSignature,keyCertSign,keyEncipherment",
                        "-addext", "extendedKeyUsage=serverAuth"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cert.chmod(0o444); key.chmod(0o444)
        self.create_network()
        self.run_container(f"{self.prefix}-postgres", command=["postgres"], alias="postgres",
                           env={"POSTGRES_DB": "gate_db", "POSTGRES_USER": "gate_user",
                                "POSTGRES_PASSWORD": DB_PASSWORD}, image="postgres:17-alpine",
                           mounts=False, memory="384m", health_cmd="pg_isready -U gate_user -d gate_db")
        self.run_container(f"{self.prefix}-origin", command=["python3", "/gate/gate_origin.py",
                           "--cert", "/fixtures/fixture-ca.crt", "--key", "/fixtures/fixture-ca.key"],
                           image="python:3.12-alpine", memory="128m")
        self.run_container(f"{self.prefix}-fake", command=["python", "/gate/gate_services.py"],
                           alias="fixture-services", memory="128m", disable_healthcheck=True)
        self.wait_ready(f"{self.prefix}-postgres")
        self.wait_http(f"{self.prefix}-origin", "http://rss.fixture.test/rss.xml")
        env = self.app_environment("web")
        self.run_container(f"{self.prefix}-web", command=["uvicorn", "news_aggregator.main:app",
                           "--host", "0.0.0.0", "--port", "8000"], alias="web", env=env,
                           memory="768m")
        self.wait_http(f"{self.prefix}-web", "http://127.0.0.1:8000/health")
        self.docker("exec", f"{self.prefix}-web", "python", "/gate/gate_seed.py", timeout=30)
        self.provision_runtime_role()
        collector_env = {"LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "true",
                         "EXTRACTOR_MAX_CONCURRENT": "1", "EXTRACTOR_SERVICE_TOKEN": COLLECTOR_TOKEN,
                         "SSL_CERT_FILE": "/fixtures/fixture-ca.crt"}
        self.run_container(f"{self.prefix}-collector", command=[], alias="collector",
                           env=collector_env, image=self.collector_image, mounts=False,
                           memory="192m")
        self.run_container(f"{self.prefix}-executor", command=["python", "-m", "news_aggregator.executor"],
                           alias="executor", env=self.app_environment("worker"), memory="512m",
                           disable_healthcheck=True)
        self.wait_http(f"{self.prefix}-executor", "http://127.0.0.1:8100/internal/v1/status",
                       auth=EXECUTOR_TOKEN)
        self.run_container(f"{self.prefix}-ingestion", command=[], alias="ingestion", image=self.ingestion_image,
                           env={"ARTICLE_INGESTION_LISTEN": ":8093",
                                "DATABASE_URL": "postgres://news_ingestion_runtime:fixture-only-password@postgres:5432/gate_db?search_path=public",
                                "ARTICLE_INGESTION_TOKEN": TOKEN,
                                "EXTRACTION_SERVICE_URL": "http://collector:8080",
                                "EXTRACTION_SERVICE_TOKEN": COLLECTOR_TOKEN,
                                "EXTRACTION_SERVICE_NATIVE_BROWSER": "false",
                                "JOB_EXECUTOR_URL": "http://executor:8100",
                                "JOB_EXECUTOR_TOKEN": EXECUTOR_TOKEN,
                                "SSL_CERT_FILE": "/fixtures/fixture-ca.crt"}, mounts=False,
                           memory="256m", health_cmd="wget -q -O /dev/null http://127.0.0.1:8093/healthz")
        self.run_container(f"{self.prefix}-processing", command=[], alias="processing",
                           image=self.processing_image,
                           env={"ARTICLE_PROCESSING_LISTEN": ":8094",
                                "DATABASE_URL": "postgres://news_processing_runtime:fixture-only-password@postgres:5432/gate_db?search_path=public",
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
        self.wait_ready(f"{self.prefix}-processing")
        self.wait_ready(f"{self.prefix}-collector")
        self.wait_ready(f"{self.prefix}-ingestion")
        self.create_worker()
        self.wait_worker_healthy(f"{self.prefix}-worker")
        network = json.loads(self.docker("network", "inspect", self.network).stdout)[0]
        self.check("network_internal_only", network.get("Internal") is True)
        self.check("source_origin_is_fixture_ip", network["Containers"] and any(
            info.get("IPv4Address", "").split("/")[0] == ORIGIN_IP
            for info in network["Containers"].values()))
        self.check("no_host_ports_published", all(not has_host_port_binding(json.loads(
            self.docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", name).stdout))
            for name in self.containers))
        self.check("runtime_role_is_not_superuser", self.docker("exec", f"{self.prefix}-postgres", "psql",
                   "-U", "gate_user", "-d", "gate_db", "-Atc",
                   "SELECT NOT rolsuper FROM pg_roles WHERE rolname='news_ingestion_runtime'").stdout.strip() == "t")
        before = self.private_probe("snapshot")["response"]
        self.check("three_fixture_sources_seeded", len(before.get("source_ids", {})) == 3
                   and before.get("enabled_source_count") == 3)
        accepted = self.private_probe("enqueue")
        self.check("python_enqueued_job", accepted.get("http_status") in (200, 202)
                   and isinstance(accepted.get("response", {}).get("job_id"), str))
        job_id = accepted["response"]["job_id"]
        job = self.wait_job(job_id)
        if job.get("status") != "succeeded":
            self.capture_timeout_diagnostics(job_id)
        self.check("job_succeeded", job.get("status") == "succeeded")
        state = self.private_probe("snapshot").get("response", {})
        source_ids = set(before.get("source_ids", {}).values())
        source_runs = [row for row in state.get("source_runs", []) if row.get("source_id") in source_ids]
        by_type = {name: source_id for name, source_id in before["source_ids"].items()}
        self.check("go_receipts_for_all_source_types", len(source_runs) == 3
                   and all(row.get("state") == "succeeded" and row.get("saved", 0) >= 1 for row in source_runs))
        rss_source_id = by_type["Go Ingestion Gate RSS"]
        rss_receipt = next((row for row in source_runs if row.get("source_id") == rss_source_id), {})
        before_duplicate = before.get("duplicate_article") or {}
        updated_duplicate = state.get("duplicate_article") or {}
        self.report["existing_article_update"] = {
            "same_row": bool(before_duplicate.get("id"))
            and updated_duplicate.get("id") == before_duplicate.get("id"),
            "source_receipt_updated": rss_receipt.get("updated", 0),
            "content_replaced": updated_duplicate.get("content_replaced") is True,
            "hash_content_present": updated_duplicate.get("hash_content_present") is True,
        }
        self.check("runtime_duplicate_article_metadata_update",
                   rss_receipt.get("updated", 0) == 1
                   and self.report["existing_article_update"]["same_row"]
                   and self.report["existing_article_update"]["content_replaced"]
                   and self.report["existing_article_update"]["hash_content_present"])
        self.check("articles_persisted_for_each_type", all(
            any(article.get("source_id") == source_id for article in state.get("articles", []))
            for source_id in by_type.values()))
        self.check("run_receipt_succeeded", any(row.get("state") == "succeeded" for row in state.get("runs", [])))
        self.check("page_monitor_cursor_receipt", any(
            row.get("source_id") == by_type.get("Go Ingestion Gate Page Monitor")
            and row.get("snapshot_at") is not None for row in state.get("cursors", [])))
        page_monitor_id = by_type["Go Ingestion Gate Page Monitor"]
        page_monitor_articles = [article for article in state.get("articles", [])
                                 if article.get("source_id") == page_monitor_id]
        page_monitor_receipt = next((row for row in source_runs
                                     if row.get("source_id") == page_monitor_id), {})
        origin_counters = self.private_probe("origin-counters").get("response", {})
        counters = self.private_probe("fake-counters").get("response", {})
        self.report["page_monitor_acceptance"] = {
            "expected_ingested_title": "Learned Selector Gate Story",
            "expected_processed_title": "Synthetic ingestion fixture story",
            "original_title_seen_by_ai": int(counters.get("page_monitor_original_title", 0)) >= 1,
            "expected_url": "https://pm.fixture.test/news",
            "actual_titles": [article.get("title") for article in page_monitor_articles],
            "actual_urls": [article.get("url") for article in page_monitor_articles],
            "candidate_count": page_monitor_receipt.get("candidates", 0),
            "saved_count": page_monitor_receipt.get("saved", 0),
            "selector_study_recorded": bool(page_monitor_receipt.get("selector_study_sha256")),
            "study_calls": int(counters.get("selector_study", 0)),
            "origin_page_fetches": int(origin_counters.get("page_fetches", 0)),
        }
        self.check("learned_selector_reparsed_captured_page", len(page_monitor_articles) == 1
                   and page_monitor_articles[0].get("title") == "Synthetic ingestion fixture story"
                   and counters.get("page_monitor_original_title", 0) >= 1
                   # Link selectors are not part of the existing PageAnalysis schema;
                   # the collector therefore uses the configured source URL fallback.
                   and page_monitor_articles[0].get("url") == "https://pm.fixture.test/news"
                   and page_monitor_receipt.get("candidates") == 1
                   and page_monitor_receipt.get("saved") == 1
                   and re.fullmatch(r"[0-9a-f]{64}",
                                    page_monitor_receipt.get("selector_study_sha256") or "") is not None
                   and origin_counters.get("page_fetches") == 2)
        self.check("fake_ai_processed_articles", counters.get("gemini", 0) >= 2
                   and any(article.get("summary_processed") is True
                           for article in state.get("articles", [])))
        self.check("go_processing_receipt_succeeded", "succeeded" in state.get("processing_run_states", []))
        self.check("go_processing_items_recorded", bool(state.get("processing_item_states")))
        self.check("fake_selector_study_callback", counters.get("selector_study", 0) == 1)
        self.report["source_counts"] = {"types": 3, "receipts": len(source_runs),
                                         "persisted_articles": len(state.get("articles", []))}
        self.report["fake_provider_calls"] = {key: int(counters.get(key, 0))
                                               for key in ("gemini", "selector_study", "telegram")}
        self.report["fixture_container_roles"] = sorted(
            name.rsplit("-", 1)[-1] for name in self.containers)
        self.report.update(actual_go_processing_tested=True, actual_go_ingestion_tested=True, actual_go_collector_tested=True,
                           actual_python_executor_tested=True, actual_go_runtime_tested=True,
                           actual_go_job_worker_tested=True)
        self.report["result"] = "passed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-image", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--collector-image", required=True)
    parser.add_argument("--ingestion-image", required=True)
    parser.add_argument("--processing-image", required=True)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/go-ingestion-fixture")
    args = parser.parse_args()
    if not shutil.which("docker"):
        parser.error("docker CLI is required")
    gate = IngestionGate(args.app_image, args.worker_image, args.collector_image,
                         args.ingestion_image, args.output_dir, args.processing_image)
    try:
        gate.run()
    except Exception as exc:
        gate.report["result"] = "failed"
        gate.report["failure_type"] = type(exc).__name__
        gate.report["failure"] = sanitize_fixture_log(str(exc))[0] if str(exc) else type(exc).__name__
    except KeyboardInterrupt:
        gate.report["result"] = "failed"
        gate.report["failure_type"] = "KeyboardInterrupt"
        gate.report["failure"] = "fixture_gate_interrupted"
    finally:
        try:
            try:
                gate.capture_resource_snapshot()
            except BaseException as exc:
                gate.report["resource_snapshot_failure_type"] = type(exc).__name__
        finally:
            gate.cleanup()
            if gate.fixture_temp:
                shutil.rmtree(gate.fixture_temp, ignore_errors=True)
            gate.output_dir.mkdir(parents=True, exist_ok=True)
            report_path = gate.output_dir / f"{gate.run_id}.json"
            report_path.write_text(json.dumps(gate.report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(gate.report, indent=2, sort_keys=True))
    return 0 if gate.report.get("result") == "passed" and not gate.report["cleanup_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
