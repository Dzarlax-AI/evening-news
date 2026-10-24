#!/usr/bin/env python3
"""Exercise web + Go job owner + private Python executor on isolated fake services."""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from run_background_worker_fixture_gate import DockerGate, GateFailure, OWNER_LABEL, ROOT


class GoJobWorkerGate(DockerGate):
    def __init__(self, image, worker_image, output_dir):
        super().__init__(image, output_dir)
        self.worker_image = worker_image
        self.report.update(gate="go-job-worker-docker-fixture", go_worker_image=worker_image,
                           actual_go_runtime_tested=True, actual_go_job_worker_tested=True,
                           actual_go_collector_tested=False, browser_tested=False,
                           throughput_claimed=False)
        self.report["explicit_restarts"]["executor"] = 0
        self.backup_volume = self.prefix + "-backups"

    def app_environment(self, role):
        environment = super().app_environment(role)
        environment.update(JOB_EXECUTOR_TOKEN="fixture-only-executor-token",
                           JOB_EXECUTOR_HOST="0.0.0.0", JOB_EXECUTOR_PORT="8100")
        return environment

    def docker(self, *args, **kwargs):
        if args and args[0] == "create" and "--name" in args:
            name = args[args.index("--name") + 1]
            if self.worker_image in args and "--no-healthcheck" not in args:
                arguments = list(args)
                index = arguments.index(self.worker_image)
                arguments[index:index] = ["--health-interval", "3s", "--health-timeout", "5s",
                                          "--health-start-period", "3s", "--health-retries", "3"]
                args = tuple(arguments)
            if name == self.prefix + "-postgres":
                arguments = list(args)
                index = arguments.index("postgres:17-alpine")
                arguments[index:index] = ["--tmpfs", "/var/lib/postgresql/data:rw,size=512m"]
                args = tuple(arguments)
            if name in {self.prefix + "-web", self.prefix + "-executor"}:
                arguments = list(args)
                index = arguments.index(self.image)
                mount = "type=volume,src=" + self.backup_volume + ",dst=/app/backups"
                if name.endswith("-web"):
                    mount += ",readonly"
                arguments[index:index] = ["--mount", mount]
                args = tuple(arguments)
        return super().docker(*args, **kwargs)

    def cleanup(self):
        super().cleanup()
        label = self.docker("volume", "inspect", "--format",
                            '{{ index .Labels "' + OWNER_LABEL + '" }}', self.backup_volume,
                            check=False)
        if label.returncode == 0 and label.stdout.strip() == self.owner:
            removed = self.docker("volume", "rm", self.backup_volume, check=False)
            if removed.returncode:
                self.report["cleanup_errors"].append("backup volume cleanup failed")

    def create_worker(self, *, suffix="worker"):
        name = f"{self.prefix}-{suffix}"
        environment = self.app_environment("worker")
        environment["JOB_EXECUTOR_URL"] = "http://executor:8100"
        self.run_container(name, command=[], image=self.worker_image, env=environment,
                           mounts=False, memory="128m")
        return name

    def private_probe(self, action, *arguments):
        output = self.docker("exec", f"{self.prefix}-web", "python",
                             "/fixtures/go_worker_probe.py", action, *arguments).stdout.strip()
        return json.loads(output.splitlines()[-1])

    def check(self, name, condition):
        self.report["checks"][name] = bool(condition)
        if not condition:
            raise GateFailure("Acceptance check failed: " + name)

    def wait_executor(self, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                result = self.private_probe("status")
                if result.get("http_status") == 200 and result.get("response", {}).get("ready"):
                    return result["response"]
            except (GateFailure, ValueError):
                pass
            time.sleep(1)
        raise GateFailure("Private executor did not become ready")

    def capture_memory(self, label):
        roles = ("web", "worker", "executor", "postgres")
        names = [f"{self.prefix}-{role}" for role in roles]
        output = self.docker("stats", "--no-stream", "--format",
                             "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}", *names).stdout
        resources = {}
        for line in output.splitlines():
            name, memory, cpu = line.split("\t")
            role = name.rsplit("-", 1)[-1]
            match = re.fullmatch(r"([0-9.]+)\s*(B|KiB|MiB|GiB)", memory.split("/", 1)[0].strip())
            if not match:
                raise GateFailure("Could not parse fixture memory sample")
            factor = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}[match[2]]
            resources[role] = {"memory_mib": round(float(match[1]) * factor, 2),
                               "cpu_percent_sample": float(cpu.rstrip("%"))}
        output = self.docker("inspect", "--format",
            '{"name":"{{.Name}}","restarts":{{.RestartCount}},"oom_killed":{{.State.OOMKilled}}}',
            *names).stdout
        for line in output.splitlines():
            state = json.loads(line)
            role = state.pop("name").rsplit("-", 1)[-1]
            resources[role].update(state)
        measured = {role: resources[role]["memory_mib"] for role in ("web", "worker", "executor")}
        measured["combined"] = round(sum(measured.values()), 2)
        self.report["memory_mib"][label] = measured
        self.report.setdefault("resource_samples", {})[label] = resources

    def assert_fixture_mounts(self):
        for role in ("web", "executor", "worker"):
            raw = self.docker("inspect", "--format", "{{json .Mounts}}", f"{self.prefix}-{role}").stdout
            mounts = json.loads(raw)
            if role == "worker":
                self.check("worker_has_no_host_mounts", not mounts)
                continue
            fixture = [mount for mount in mounts if mount["Destination"] == "/fixtures"]
            backup = [mount for mount in mounts if mount["Destination"] == "/app/backups"]
            self.check(role + "_owned_mounts_only", len(mounts) == 2 and len(fixture) == len(backup) == 1
                       and fixture[0]["Type"] == "bind" and not fixture[0]["RW"]
                       and Path(fixture[0]["Source"]).resolve() == (ROOT / "tests/fixtures/background-worker").resolve()
                       and backup[0]["Type"] == "volume" and backup[0]["Name"] == self.backup_volume
                       and backup[0]["RW"] == (role == "executor"))

    def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report["image_id"] = self.image_id()
        self.report["worker_image_id"] = self.docker(
            "image", "inspect", self.worker_image, "--format", "{{.Id}}").stdout.strip()
        self.create_network()
        self.docker("volume", "create", "--label", f"{OWNER_LABEL}={self.owner}", self.backup_volume)
        self.run_container(f"{self.prefix}-postgres", command=["postgres"], alias="postgres",
                           env={"POSTGRES_DB": "gate_db", "POSTGRES_USER": "gate_user",
                                "POSTGRES_PASSWORD": "fixture-only-password"},
                           image="postgres:17-alpine", mounts=False, memory="256m")
        self.run_container(f"{self.prefix}-fake", command=["python", "/fixtures/fake_services.py"],
                           alias="fixture-services", memory="128m", disable_healthcheck=True)
        self.docker("exec", f"{self.prefix}-postgres", "sh", "-c",
                    "until pg_isready -U gate_user -d gate_db >/dev/null 2>&1; do sleep 1; done",
                    timeout=60)
        self.docker("exec", f"{self.prefix}-postgres", "createdb", "-U", "gate_user", "empty_gate_db")
        missing_environment = self.app_environment("worker")
        missing_environment.update(DATABASE_URL="postgresql://gate_user:fixture-only-password@postgres:5432/empty_gate_db",
                                   JOB_EXECUTOR_URL="http://executor:8100")
        missing_worker = f"{self.prefix}-schema-check-worker"
        self.run_container(missing_worker, command=[], image=self.worker_image, env=missing_environment,
                           mounts=False, memory="128m", disable_healthcheck=True)
        self.check("worker_fails_closed_without_schema", self.wait_container_exit(missing_worker) != 0)
        self.check("fixture_network_internal", self.docker("network", "inspect", "--format",
                   "{{.Internal}}", self.network).stdout.strip().lower() == "true")
        self.run_container(f"{self.prefix}-web", command=["uvicorn", "news_aggregator.main:app",
                           "--host", "0.0.0.0", "--port", "8000"], alias="web",
                           env=self.app_environment("web"), memory="512m")
        self.wait_http_probe("health")
        self.check("runtime_source_matches_checkout", self.source_hashes() == self.source_hashes(
            f"{self.prefix}-web"))
        login = self.exec_json(f"{self.prefix}-web", "admin-cookie-check")
        self.check("public_home_and_admin_login", self.private_probe("public-home")["http_status"] == 200
                   and login.get("http_status") == 200
                   and login.get("response", {}).get("admin_cookie_received") is True)
        # Preserve the previous gate's diagnostic-only coverage of the separate
        # token-login endpoint; the actual admin path uses its session cookie.
        self.report["admin_login_diagnostic"] = self.exec_json(
            f"{self.prefix}-web", "auth-login-check").get("response", {})
        openapi = self.exec_json(f"{self.prefix}-web", "openapi-check")
        self.check("public_reprocess_contract_in_openapi", openapi.get("http_status") == 200
                   and all(openapi.get("response", {}).get(key) is True
                           for key in ("auth_login", "public_reprocess", "api_reprocess")))
        self.docker("exec", f"{self.prefix}-web", "python", "/fixtures/seed_fixture.py")
        self.check("unauthorized_job_status_rejected", self.exec_json(
            f"{self.prefix}-web", "unauthorized").get("http_status") == 401)
        dashboard = self.exec_json(f"{self.prefix}-web", "dashboard")
        status = self.exec_json(f"{self.prefix}-web", "status")
        self.check("persisted_web_status_without_orchestrator", dashboard.get("http_status") == 200
                   and isinstance(dashboard.get("response", {}).get("queue_stats"), dict)
                   and "worker_available" in dashboard["response"]["queue_stats"]
                   and status.get("http_status") == 200
                   and "worker_available" in status.get("response", {}))
        self.check("source_not_found_preserved", self.exec_json(
            f"{self.prefix}-web", "source-not-found", "missing-source").get("http_status") == 404)
        telegram = self.exec_json(f"{self.prefix}-web", "telegram-test")
        self.check("web_telegram_diagnostic_uses_fake", telegram.get("http_status") == 200
                   and telegram.get("response", {}).get("success") is True)
        self.run_container(f"{self.prefix}-executor", command=["python", "-m", "news_aggregator.executor"],
                           alias="executor", env=self.app_environment("worker"), memory="512m",
                           disable_healthcheck=True)
        first_instance = self.wait_executor()["executor_instance_id"]
        duplicate_executor = f"{self.prefix}-second-executor"
        self.run_container(duplicate_executor, command=["python", "-m", "news_aggregator.executor"],
                           env=self.app_environment("worker"), memory="128m", disable_healthcheck=True)
        self.check("second_python_executor_refused", self.wait_container_exit(duplicate_executor) != 0)
        self.check("executor_requires_service_token", self.private_probe("unauthorized")["http_status"] == 401)
        legacy = f"{self.prefix}-legacy-owner"
        self.run_container(legacy, command=["python", "/fixtures/go_worker_probe.py", "legacy-hold"],
                           env=self.app_environment("worker"), memory="128m", disable_healthcheck=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            ready = self.docker("exec", legacy, "python", "-c",
                "from pathlib import Path; print(Path('/tmp/legacy-lease-ready').exists())").stdout.strip()
            if ready == "True":
                break
            time.sleep(0.5)
        self.check("python_lease_fixture_ready", ready == "True")
        blocked_worker = self.create_worker(suffix="blocked-worker")
        self.check("go_refuses_python_lease_owner", self.wait_container_exit(blocked_worker) == 2)
        self.docker("stop", "--time", "5", legacy)
        self.fake_control(ai="block")
        accepted = self.exec_json(f"{self.prefix}-web", "enqueue-process", "go-worker-processing")
        self.check("processing_accepted", accepted.get("http_status") == 202)
        job_id = accepted["response"]["job_id"]
        repeated = self.exec_json(f"{self.prefix}-web", "enqueue-process", "go-worker-processing")
        self.check("producer_idempotency_preserved", repeated.get("response", {}).get("job_id") == job_id)
        conflict = self.exec_json(f"{self.prefix}-web", "enqueue-digest", "go-worker-processing")
        self.check("producer_idempotency_conflict", conflict.get("http_status") == 409)
        worker = self.create_worker()
        self.assert_fixture_mounts()
        web_image = self.docker("inspect", "--format", "{{.Image}}", f"{self.prefix}-web").stdout.strip()
        executor_image = self.docker("inspect", "--format", "{{.Image}}", f"{self.prefix}-executor").stdout.strip()
        worker_image = self.docker("inspect", "--format", "{{.Image}}", worker).stdout.strip()
        self.check("matching_role_images", web_image == executor_image == self.report["image_id"]
                   and worker_image == self.report["worker_image_id"])
        self.wait_worker_healthy(worker)
        self.check("python_refuses_go_lease_owner", not self.private_probe("legacy-lease")["acquired"])
        self.wait_counter("gemini", 1, job_id=job_id)
        self.capture_memory("active_ai_blocked")
        receipt = self.private_probe("receipts")
        self.check("go_claim_and_python_receipt", receipt["worker"].get("lock_backend_pid") is not None
                   and any(row["job_id"] == job_id for row in receipt["receipts"]))
        second = self.create_worker(suffix="second-worker")
        self.check("second_owner_refused", self.wait_container_exit(second) == 2)
        self.docker("restart", f"{self.prefix}-web", timeout=90)
        self.report["explicit_restarts"]["web"] += 1
        self.wait_http_probe("health")
        self.check("job_survives_web_restart", self.job(job_id).get("status") == "running")
        self.fake_control(ai="release")
        completed = self.wait_job(job_id, {"succeeded"})
        self.check("successful_processing_has_no_error_code", completed.get("error_code") is None)
        snapshot = self.exec_json(f"{self.prefix}-web", "snapshot")["response"]
        articles = [row for row in snapshot["articles"] if row["state"] == "completed"]
        self.check("whole_python_pipeline_completed", bool(articles))
        article_id = articles[0]["id"]
        counters = self.fake_counters()
        self.check("whole_pipeline_used_fake_providers", all(counters.get(key, 0) > 0
                   for key in ("source_collection", "extraction", "gemini")))
        self.check("disabled_scheduler_did_not_dispatch", snapshot["due_schedules"] > 0
                   and not any(job["scheduled"] for job in snapshot["jobs"]))
        self.check("public_home_available", self.private_probe("public-home")["http_status"] == 200)
        self.check("public_reads_after_processing", self.exec_json(
            f"{self.prefix}-web", "dashboard").get("http_status") == 200)

        for kind in ("news_digest", "reprocess_failed", "summaries_generate", "source_test", "backup"):
            extra = self.private_probe("enqueue-kind", kind)
            terminal = self.wait_job(extra["job_id"], {"succeeded"})
            self.check("job_kind_" + kind, terminal.get("status") == "succeeded")
            self.check("job_kind_" + kind + "_no_error_code", terminal.get("error_code") is None)
        archive_count = self.docker("exec", f"{self.prefix}-web", "python", "-c",
            "from pathlib import Path; print(len(list(Path('/app/backups').glob('*.tar.gz'))))"
        ).stdout.strip()
        self.check("backup_visible_from_web_shared_storage", int(archive_count) >= 1)

        # Kill the executor during real fixture AI; Go remains alive and must
        # detect its new instance rather than replay an old receipt in Python.
        self.fake_control(ai="block")
        counters = self.fake_counters()
        reprocess = self.exec_json(f"{self.prefix}-web", "reprocess", str(article_id), "go-executor-crash")
        self.check("admin_cookie_reprocess_accepted", reprocess.get("http_status") == 202)
        reprocess_id = reprocess["response"]["job_id"]
        flow = reprocess["response"].get("fixture_admin_flow", {})
        self.check("public_reprocess_real_admin_cookie", flow.get("admin_page_http_status") == 200
                   and flow.get("admin_cookie_received") is True
                   and flow.get("public_admin_check_http_status") == 404
                   and flow.get("public_admin_check_authorized") is True)
        self.wait_counter("gemini", counters["gemini"] + 1, job_id=reprocess_id)
        self.docker("kill", f"{self.prefix}-executor")
        self.docker("start", f"{self.prefix}-executor")
        self.report["explicit_restarts"]["executor"] += 1
        self.check("executor_instance_changed", self.wait_executor()["executor_instance_id"] != first_instance)
        self.fake_control(ai="release")
        # A crashed article attempt may retain its existing retry deadline.
        # Advance only fixture time, preserving article attempts and stages.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self.exec_json(f"{self.prefix}-web", "advance-retry", str(article_id))
            current = self.job(reprocess_id)
            if current.get("status") in {"succeeded", "failed", "delivery_unknown"}:
                break
            time.sleep(1)
        self.wait_job(reprocess_id, {"succeeded"})
        attempts = [row["attempt_generation"] for row in self.private_probe("receipts")["receipts"]
                    if row["job_id"] == reprocess_id]
        self.check("go_recovered_executor_crash", len(set(attempts)) >= 2)

        # Kill only the Go owner during a real blocked article operation. Python
        # survives, so takeover must cancel/resolve it before a new attempt.
        self.fake_control(ai="block")
        counters = self.fake_counters()
        unauthorized = self.exec_json(f"{self.prefix}-web", "public-reprocess-unauthorized", str(article_id))
        self.check("public_reprocess_rejects_missing_cookie", unauthorized.get("http_status") == 401)
        interrupted = self.exec_json(f"{self.prefix}-web", "reprocess", str(article_id), "go-owner-crash")
        self.check("owner_crash_job_accepted", interrupted.get("http_status") == 202)
        interrupted_id = interrupted["response"]["job_id"]
        self.wait_counter("gemini", counters["gemini"] + 1, job_id=interrupted_id)
        self.docker("kill", worker)
        self.docker("start", worker)
        self.report["explicit_restarts"]["worker"] += 1
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            self.exec_json(f"{self.prefix}-web", "advance-retry", str(article_id))
            attempts = [row["attempt_generation"] for row in self.private_probe("receipts")["receipts"]
                        if row["job_id"] == interrupted_id]
            if attempts and max(attempts) >= 2:
                break
            time.sleep(1)
        self.check("owner_crash_reclaimed_same_job", attempts and max(attempts) >= 2)
        self.fake_control(ai="release")
        self.wait_job(interrupted_id, {"succeeded"})
        self.check("python_operation_recovered_after_go_kill", True)

        # A lost Go owner while Telegram has accepted a send is ambiguous.
        self.fake_control(telegram="block")
        before_send = self.fake_counters().get("telegram", 0)
        digest = self.exec_json(f"{self.prefix}-web", "enqueue-digest", "go-delivery-unknown")
        self.check("digest_accepted", digest.get("http_status") == 202)
        digest_id = digest["response"]["job_id"]
        after_send = self.wait_counter("telegram", before_send + 1, job_id=digest_id)
        self.docker("kill", worker)
        self.docker("start", worker)
        self.report["explicit_restarts"]["worker"] += 1
        self.wait_job(digest_id, {"delivery_unknown"}, timeout=60)
        self.fake_control(telegram="release")
        time.sleep(3)
        self.check("ambiguous_delivery_not_replayed", self.fake_counters().get("telegram", 0)
                   == after_send.get("telegram", 0))
        self.capture_memory("idle_final")
        self.report["provider_calls"]["final"] = self.fake_counters()
        for role in ("web", "worker", "executor", "postgres"):
            self.check(role + "_no_oom", not self.report["resource_samples"]["idle_final"][role]["oom_killed"])
        self.report["receipt_snapshot"] = self.private_probe("receipts")
        self.report["result"] = "passed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "go-job-worker")
    args = parser.parse_args()
    gate = GoJobWorkerGate(args.image, args.worker_image, args.output_dir)
    try:
        gate.run()
    except Exception as error:
        gate.report.update(result="failed", failure=str(error)[:500])
    finally:
        gate.cleanup()
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / f"{gate.run_id}.json").write_text(json.dumps(gate.report, indent=2) + "\n")
        print(json.dumps(gate.report, indent=2))
    return 0 if gate.report.get("result") == "passed" and not gate.report["cleanup_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
