#!/usr/bin/env python3
"""Compare native Go browser concurrency 1 vs 2 on an isolated Cloak fixture.

The gate uses a pinned Cloak image, pre-cached helper images with recorded
identities, and caller-supplied Linux Go binaries. It never publishes a container port or connects to external networks.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CLOAK_IMAGE = "cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4"
PYTHON_IMAGE = "python:3.12-alpine"
ALPINE_IMAGE = "alpine:3.21"
FIXTURE_IP = "11.203.254.242"
EGRESS_FIXTURE_IP = "11.203.254.243"
FIXTURE_NETWORK = "11.203.254.240/28"
OWNER_LABEL = "com.dzarlax.native-cloak-concurrency"
RUN_LABEL = "com.dzarlax.native-cloak-concurrency.run"
MAX_SECONDS = 240
NATIVE_DIAGNOSTIC_STAGES = {
    "readiness", "discovery", "websocket", "events", "create_context", "create_target",
    "attach_target", "configure", "navigate", "dom", "cleanup",
}
NATIVE_DIAGNOSTIC_REASONS = {
    "context_timeout", "context_cancelled", "event_overflow", "ownership_loss", "unsupported",
    "client_disconnected", "malformed_detach", "owned_detach", "target_crashed",
    "protocol_error", "transport_error", "operation_failed",
}
CDP_TIMEOUT_REASONS = {
    "cdp_timeout_browser_get_version", "cdp_timeout_target_create_browser_context",
    "cdp_timeout_target_create_target", "cdp_timeout_target_attach_to_target",
    "cdp_timeout_target_get_targets", "cdp_timeout_target_get_browser_contexts",
    "cdp_timeout_runtime_evaluate", "cdp_timeout_target_dispose_browser_context", "cdp_timeout_other",
}
STARTUP_FAILURES = {
    "native browser capability probe failed": "native_browser_capability_probe_failed",
    "experimental browser backend must be selected explicitly": "native_backend_not_selected",
    "native configuration forbidden in HTTP-only mode": "native_configuration_forbidden",
    "CHROME_ENDPOINT is forbidden in HTTP-only mode": "browser_endpoint_forbidden",
    "invalid CHROME_ENDPOINT": "browser_endpoint_invalid",
    "invalid EXTRACTOR_HTTP_ONLY": "http_only_config_invalid",
    "invalid EXTRACTOR_BROWSER_CONCURRENT": "browser_concurrency_config_invalid",
    "invalid EXTRACTOR_MAX_CONCURRENT": "worker_concurrency_config_invalid",
    "native readiness selector exceeds limit": "readiness_selector_invalid",
    "native backend supports browser concurrency one or two": "browser_concurrency_config_invalid",
    "native_browser_network_isolation_required": "native_network_isolation_missing",
    "native_browser_invalid_endpoint": "browser_endpoint_invalid",
    "native_browser_invalid_proxy_configuration": "native_proxy_configuration_invalid",
    "native_browser_invalid_concurrency": "browser_concurrency_config_invalid",
    "native_browser_invalid_waiters": "browser_waiter_config_invalid",
}


class GateError(RuntimeError):
    def __init__(self, code: str, *, phase: str = "setup"):
        self.code = code if re.fullmatch(r"[a-z0-9_]{1,64}", code) else "gate_failed"
        self.phase = phase if re.fullmatch(r"[a-z0-9_-]{1,48}", phase) else "unknown"
        super().__init__(self.code)


def _run(args: list[str], *, timeout: int = 30, input_text: str | None = None) -> str:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                                input=input_text, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise GateError("command_unavailable_or_timeout") from None
    if result.returncode != 0:
        error = result.stderr.lower()
        if "no such network" in error:
            raise GateError("network_not_found")
        if "no such container" in error or "no such object" in error:
            raise GateError("container_not_found")
        raise GateError("container_operation_failed")
    return result.stdout.strip()


def checked_binary(path: Path) -> Path:
    if path.is_symlink():
        raise GateError("candidate_binary_invalid")
    path = path.resolve(strict=True)
    info = path.stat()
    if not path.is_file() or path.is_symlink() or info.st_size <= 0 or not info.st_mode & 0o111:
        raise GateError("candidate_binary_invalid")
    return path


def memory_bytes(value: str) -> int | None:
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*(B|kB|KB|MB|GB|KiB|MiB|GiB)\s*/\s*.+", value)
    if not match:
        return None
    number, unit = float(match.group(1)), match.group(2)
    scale = {"B": 1, "kB": 1000, "KB": 1000, "MB": 1_000_000,
             "GB": 1_000_000_000, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[unit]
    return int(number * scale)


def validate_receipt(value: object, command_id: str) -> dict[str, Any]:
    if (not isinstance(value, dict) or set(value) - {"command_id", "ok", "result", "error"}
            or value.get("command_id") != command_id or type(value.get("ok")) is not bool):
        raise GateError("client_receipt_invalid")
    if value["ok"] is not True:
        code = value.get("error")
        if not isinstance(code, str) or not re.fullmatch(r"[a-z0-9_]{1,64}", code):
            code = "client_gate_failed"
        raise GateError(code)
    if not isinstance(value.get("result"), dict):
        raise GateError("client_receipt_invalid")
    return value["result"]


def classify_runtime_logs(worker_logs: str = "", observer_logs: str = "",
                          cloak_logs: str = "", cloak_exit_code: int | None = None) -> dict[str, list[dict[str, str]]]:
    worker: list[dict[str, str]] = []
    for line in worker_logs[-65536:].splitlines()[-200:]:
        match = re.search(r"native_browser_diagnostic stage=([a-z_]+) reason=([a-z0-9_-]+)", line)
        if match:
            stage, reason = match.groups()
            valid_reason = reason in NATIVE_DIAGNOSTIC_REASONS or re.fullmatch(r"protocol_error_-?[0-9]{1,6}", reason)
            if stage in NATIVE_DIAGNOSTIC_STAGES and valid_reason:
                item = {"kind": "native_browser_diagnostic", "stage": stage, "reason": reason}
                if item not in worker:
                    worker.append(item)
        for message, code in STARTUP_FAILURES.items():
            if message in line:
                item = {"kind": "startup_failure", "reason": code}
                if item not in worker:
                    worker.append(item)
    observer: list[dict[str, str]] = []
    for line in observer_logs[-65536:].splitlines()[-200:]:
        match = re.search(r"native_cloak_observer_failure type=([A-Za-z]+) reason=([a-z0-9_]+)", line)
        if not match:
            continue
        exception_type, reason = match.groups()
        if exception_type not in {"OSError", "RuntimeError", "TimeoutError", "ValueError", "HTTPError", "Exception"}:
            exception_type = "Exception"
        if reason not in {"observer_error", "client_gate_failed"} | CDP_TIMEOUT_REASONS | {
                "cdp_handshake_closed", "cdp_handshake_rejected", "cdp_handshake_invalid",
                "cdp_connection_closed", "cdp_message_too_large", "cdp_command_failed",
                "cdp_discovery_invalid", "peer_context_create_failed", "peer_target_create_failed",
                "peer_attach_failed", "peer_target_or_context_lost", "peer_session_probe_failed",
                "news_browser_resource_leak", "worker_response_invalid", "one_slot_overlap_detected",
                "two_slot_barrier_or_third_waiter_invalid", "one_slot_fixture_evidence_invalid",
                "two_slot_fixture_evidence_invalid", "fixture_arrival_timeout", "origin_state_timeout",
                "cancellation_isolation_failed", "upstream_error_classification_invalid",
                "synthetic_article_request_failed", "client_command_invalid"}:
            reason = "observer_error"
        observer.append({"exception_type": exception_type, "reason": reason})
    cloak: list[dict[str, str]] = []
    lower_cloak = cloak_logs[-65536:].lower()
    cloak_markers = (
        ("no space left on device", "cloak_storage_exhaustion"),
        ("/dev/shm", "cloak_shared_memory_failure"),
        ("shared memory", "cloak_shared_memory_failure"),
        ("trace/breakpoint trap", "cloak_sigtrap"),
        ("sigtrap", "cloak_sigtrap"),
        ("fatal:", "cloak_fatal"),
        ("check failed", "cloak_check_failed"),
    )
    for marker, code in cloak_markers:
        if marker in lower_cloak and not any(item["reason"] == code for item in cloak):
            cloak.append({"kind": "cloak_runtime_diagnostic", "reason": code})
    if cloak_exit_code == 133 and not any(item["reason"] == "cloak_sigtrap" for item in cloak):
        cloak.append({"kind": "container_exit", "reason": "cloak_sigtrap"})
    return {"worker": worker, "observer": observer, "cloak": cloak}


def _write_private(path: Path, value: object) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(4))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class DockerGate:
    def __init__(self, stage: Path, worker_binary: Path, egress_binary: Path, output: Path):
        self.stage = stage
        self.worker_binary = worker_binary
        self.egress_binary = egress_binary
        self.output = output
        self.run_id = secrets.token_hex(8)
        self.label = f"{RUN_LABEL}={self.run_id}"
        self.prefix = "news-cloak-concurrency-" + self.run_id
        self.containers: list[str] = []
        self.networks: list[str] = []
        self.started = time.monotonic()
        self.max_memory: dict[str, int] = {}
        self.cgroup_metrics: dict[str, dict[str, int | None]] = {}
        self.control_subnet = self._choose_control_subnet()
        self.phase = "setup"
        self.control_ip = lambda host: str(self.control_subnet.network_address + host)
        self.token = secrets.token_urlsafe(32)
        self.output.parent.mkdir(parents=True, exist_ok=True)

    def _choose_control_subnet(self) -> ipaddress.IPv4Network:
        third = secrets.randbelow(200) + 20
        return ipaddress.ip_network(f"172.29.{third}.0/24")

    def _docker(self, *args: str, timeout: int = 30) -> str:
        try:
            return _run(["docker", *args], timeout=timeout)
        except GateError as exc:
            raise GateError(exc.code, phase=self.phase) from None

    def _verify_container(self, name: str) -> None:
        labels = self._docker("inspect", "--format", '{{index .Config.Labels "' + RUN_LABEL + '"}}', name)
        if labels != self.run_id:
            raise GateError("container_ownership_mismatch")

    def _create_network(self, name: str, subnet: str) -> None:
        if name not in self.networks:
            self.networks.append(name)
        self._docker("network", "create", "--internal", "--subnet", subnet,
                     "--label", OWNER_LABEL + "=v1", "--label", self.label, name)

    def _run_container(self, suffix: str, image: str, *, network: str | None = None,
                       ip: str | None = None, command: list[str] = (), mounts: tuple[str, ...] = (),
                       env: tuple[str, ...] = (), memory: str, cpu: str = "0.5", pids: int = 64,
                       readonly: bool = True, extra: tuple[str, ...] = (),
                       network_mode: str | None = None, entrypoint: str | None = None,
                       tmp_size: int = 16) -> str:
        name = self.prefix + "-" + suffix
        if name not in self.containers:
            self.containers.append(name)
        args = ["run", "--pull=never", "-d", "--name", name, "--label", OWNER_LABEL + "=v1", "--label", self.label,
                "--restart=no", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                "--platform=linux/amd64", "--memory", memory, "--memory-swap", memory,
                "--cpus", cpu, "--pids-limit", str(pids), "--log-opt", "max-size=2m", "--log-opt", "max-file=1"]
        if network_mode:
            args += ["--network", network_mode]
        else:
            assert network
            args += ["--network", network, "--dns", "127.0.0.1"]
            if ip:
                args += ["--ip", ip]
        if readonly:
            args += ["--read-only", "--tmpfs", f"/tmp:rw,nosuid,nodev,noexec,size={tmp_size}m"]
        if entrypoint:
            args += ["--entrypoint", entrypoint]
        for mount in mounts:
            args += ["-v", mount]
        for item in env:
            args += ["-e", item]
        args += list(extra) + [image] + command
        self._docker(*args)
        self._verify_container(name)
        return name

    def _sample_memory(self, name: str) -> None:
        try:
            value = self._docker("stats", "--no-stream", "--format", "{{.MemUsage}}", name, timeout=8)
            current = memory_bytes(value)
            if current is not None:
                self.max_memory[name] = max(self.max_memory.get(name, 0), current)
        except GateError:
            return

    def _snapshot_cgroup(self, name: str, label: str | None = None) -> None:
        key = label or name.removeprefix(self.prefix + "-")
        names = ("memory.current", "memory.peak", "memory.stat", "memory.events", "pids.current")
        result: dict[str, int | None] = {"memory_current_bytes": None, "memory_peak_bytes": None,
            "memory_anon_bytes": None, "memory_file_bytes": None, "oom_events": None,
            "oom_kill_events": None, "pids_current": None}
        try:
            raw = self._docker("exec", name, "sh", "-c",
                "for f in memory.current memory.peak memory.stat memory.events pids.current; do "
                "printf '%s\\n' ===$f; cat /sys/fs/cgroup/$f 2>/dev/null || true; done", timeout=5)
            current = None
            for line in raw.splitlines():
                if line.startswith("==="):
                    current = line[3:]
                    continue
                fields = line.split()
                if current in {"memory.current", "memory.peak", "pids.current"} and fields:
                    number = int(fields[0])
                    result[{"memory.current": "memory_current_bytes", "memory.peak": "memory_peak_bytes",
                            "pids.current": "pids_current"}[current]] = number
                elif current == "memory.stat" and len(fields) == 2 and fields[0] in {"anon", "file"}:
                    result["memory_anon_bytes" if fields[0] == "anon" else "memory_file_bytes"] = int(fields[1])
                elif current == "memory.events" and len(fields) == 2 and fields[0] in {"oom", "oom_kill"}:
                    result["oom_events" if fields[0] == "oom" else "oom_kill_events"] = int(fields[1])
        except (GateError, ValueError):
            pass
        self.cgroup_metrics[key] = result
        if result["oom_events"] not in (None, 0) or result["oom_kill_events"] not in (None, 0):
            raise GateError("container_oom_event_detected", phase=self.phase)

    def _all_runtime_ok(self) -> None:
        if time.monotonic() - self.started > MAX_SECONDS:
            raise GateError("gate_deadline_exceeded", phase=self.phase)
        for name in self.containers:
            self._verify_container(name)
            state = self._docker("inspect", "--format", "{{.State.Running}} {{.RestartCount}} {{.State.OOMKilled}}", name)
            if state.split() != ["true", "0", "false"]:
                raise GateError("runtime_stopped_restarted_or_oom", phase=self.phase)
        try:
            usage = self._docker("stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}",
                                 *self.containers, timeout=8)
            by_name = {name: name for name in self.containers}
            for line in usage.splitlines():
                fields = line.split(maxsplit=1)
                if len(fields) == 2 and fields[0] in by_name:
                    current = memory_bytes(fields[1])
                    if current is not None:
                        name = by_name[fields[0]]
                        self.max_memory[name] = max(self.max_memory.get(name, 0), current)
        except GateError:
            pass

    def _wait_command(self, observer: str, action: str, timeout: int = 45) -> dict[str, Any]:
        command_id = secrets.token_hex(16)
        command_path = self.stage / "commands" / (command_id + ".json")
        result_path = self.stage / "results" / (command_id + ".json")
        _write_private(command_path, {"action": action})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if result_path.exists() and not result_path.is_symlink():
                try:
                    if result_path.stat().st_size > 4096 or result_path.stat().st_mode & 0o077:
                        raise ValueError
                    value = json.loads(result_path.read_text())
                except (OSError, ValueError):
                    raise GateError("client_receipt_invalid") from None
                result_path.unlink()
                return validate_receipt(value, command_id)
            self._all_runtime_ok()
            time.sleep(.1)
        raise GateError("client_gate_timeout")

    def _wait_worker(self, worker: str) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                self._docker("exec", worker, "/extract-service", "--healthcheck", timeout=5)
                return
            except GateError:
                self._all_runtime_ok()
                time.sleep(.25)
        raise GateError("worker_readiness_timeout")

    def _start_worker(self, browser_ip: str, gateway_ip: str, concurrency: int) -> str:
        name = self.prefix + "-worker"
        try:
            self._docker("inspect", name)
        except GateError:
            pass
        else:
            self._verify_container(name)
            self._docker("stop", "--time", "5", name, timeout=10)
            self._docker("rm", name)
            self.containers.remove(name)
        self._run_container("worker", ALPINE_IMAGE, network=self.prefix + "-control",
            ip=self.control_ip(3), command=["/extract-service"],
            mounts=(str(self.worker_binary) + ":/extract-service:ro",), memory="256m", cpu="1",
            pids=128, env=("LISTEN_ADDR=0.0.0.0:8080", "EXTRACTOR_HTTP_ONLY=false",
                "EXTRACTOR_MAX_CONCURRENT=5", f"EXTRACTOR_BROWSER_CONCURRENT={concurrency}",
                "EXTRACTOR_BROWSER_BACKEND=native", "EXTRACTOR_NATIVE_NETWORK_ISOLATED=true",
                "EXTRACTOR_NATIVE_READY_SELECTOR=[data-ready=\"true\"]",
                "EXTRACTOR_NATIVE_DIAGNOSTICS=synthetic-soak", "EXTRACTOR_SERVICE_TOKEN=" + self.token,
                "CHROME_ENDPOINT=http://" + browser_ip + ":9222",
                "EXTRACTOR_NATIVE_PROXY_URL=http://" + gateway_ip + ":8081",
                "EXTRACTOR_NATIVE_PROXY_TOKEN=" + self.token))
        worker = self.prefix + "-worker"
        self._wait_worker(worker)
        return worker

    def execute(self) -> dict[str, Any]:
        self.phase = "preflight"
        self._create_network(self.prefix + "-control", str(self.control_subnet))
        image_details = {}
        for image in (CLOAK_IMAGE, PYTHON_IMAGE, ALPINE_IMAGE):
            output = self._docker("image", "inspect", "--format",
                                  '{{.Os}}/{{.Architecture}} {{.Id}} {{join .RepoDigests ";"}}', image)
            fields = output.split(maxsplit=2)
            if len(fields) < 2:
                raise GateError("cached_image_identity_unavailable", phase=self.phase)
            image_details[image] = {"platform": fields[0], "image_id": fields[1],
                                    "repo_digests": fields[2].split(";") if len(fields) > 2 and fields[2] else []}
        images = {image: details["platform"] for image, details in image_details.items()}
        if images[CLOAK_IMAGE] != "linux/amd64":
            raise GateError("pinned_cloak_image_architecture_mismatch")
        if self.worker_binary == self.egress_binary:
            raise GateError("candidate_binaries_must_be_distinct")

        self.phase = "fixture_network"
        fixture_net = self.prefix + "-fixture"
        self._create_network(fixture_net, FIXTURE_NETWORK)
        fixture_dir = self.stage / "fixture"
        fixture_dir.mkdir(mode=0o700)
        os.chmod(fixture_dir, 0o700)
        (fixture_dir / "release").mkdir(mode=0o700)
        cert, key = self._make_certificate(fixture_dir)
        cloak_spki = _certificate_spki_sha256(cert)
        info = fixture_dir / "browser-info"
        info.mkdir(mode=0o700)

        self.phase = "browser_startup"
        cloak = self._run_container("cloak", CLOAK_IMAGE, network=self.prefix + "-control",
            ip=self.control_ip(2), command=["python", "/entry/launch.py", "--info-dir", "/info", "--spki", cloak_spki],
            mounts=(str(ROOT / "scripts/launch_shadow_cloak.py") + ":/entry/launch.py:ro",
                    str(ROOT / "scripts/shadow_x11_client.sh") + ":/shadow-bin/xdotool:ro",
                    str(ROOT / "scripts/shadow_x11_client.sh") + ":/shadow-bin/openbox:ro",
                    str(info) + ":/info:rw"), memory="768m", cpu="0.5", pids=256,
            readonly=True, entrypoint="/entrypoint.sh", tmp_size=256,
            extra=("--shm-size", "1g", "--tmpfs", "/run:rw,noexec,nosuid,size=32m",
                "--tmpfs", "/root/.config:rw,nosuid,nodev,size=16m",
                "--tmpfs", "/root/.cache:rw,nosuid,nodev,size=16m"), env=("CLOAKBROWSER_AUTO_UPDATE=false",
                "DISPLAY=unix/:99", "PATH=/shadow-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"))
        self._wait_cloak_launch(info / "launch-info.json")

        self.phase = "relay_startup"
        relay = self._run_container("relay", PYTHON_IMAGE, network_mode="container:" + cloak,
            command=["python", "/relay.py", "--controller-ip", self.control_ip(3), "--controller-ip", self.control_ip(4)],
            mounts=(str(ROOT / "scripts/shadow_cdp_relay.py") + ":/relay.py:ro",), memory="64m", cpu="0.25", pids=32)

        self.phase = "egress_startup"
        gateway_ip = self.control_ip(5)
        gateway = self._run_container("egress", ALPINE_IMAGE, network=self.prefix + "-control",
            ip=gateway_ip, command=["/extract-egress"], mounts=(str(self.egress_binary) + ":/extract-egress:ro",),
            memory="128m", cpu="0.25", pids=64,
            env=("EGRESS_LISTEN_ADDR=" + gateway_ip + ":8081", "EGRESS_TOKEN=" + self.token,
                 "EGRESS_MAX_CONCURRENT=4", "EGRESS_MAX_LIFETIME_SECONDS=35", "EGRESS_TEST_MODE=true",
                 "EGRESS_TEST_ALLOWLIST={\"native-fixture.test\":\"" + FIXTURE_IP + "\"}"))
        self._docker("network", "connect", "--ip", EGRESS_FIXTURE_IP, fixture_net, gateway)

        self.phase = "origin_startup"
        origin_script = ROOT / "tests/fixtures/native-cloak/concurrency_origin.py"
        origin = self._run_container("origin", PYTHON_IMAGE, network=fixture_net, ip=FIXTURE_IP,
            command=["python", "/origin.py", "--cert", "/fixture/cert.pem", "--key", "/fixture/key.pem",
                     "--state-dir", "/state", "--port", "443"],
            mounts=(str(origin_script) + ":/origin.py:ro", str(cert) + ":/fixture/cert.pem:ro",
                    str(key) + ":/fixture/key.pem:ro", str(self.stage / "fixture") + ":/state:rw"),
            memory="64m", cpu="0.25", pids=64)
        self._wait_fixture(origin)
        self._wait_egress(gateway)

        state_dir = self.stage / "fixture"
        command_dir = self.stage / "commands"
        result_dir = self.stage / "results"
        for directory in (command_dir, result_dir):
            directory.mkdir(mode=0o700)
        token_path = self.stage / "private-token"
        token_path.write_text(self.token + "\n")
        token_path.chmod(0o600)
        self.phase = "observer_startup"
        observer = self._run_container("observer", PYTHON_IMAGE, network=self.prefix + "-control",
            ip=self.control_ip(4), command=["python", "-u", "/client.py", "--state-dir", "/state",
                "--worker", self.control_ip(3) + ":8080", "--browser", self.control_ip(2) + ":9222",
                "--proxy", "http://" + gateway_ip + ":8081", "--token-file", "/token",
                "--command-dir", "/commands", "--result-dir", "/results"],
            mounts=(str(ROOT / "scripts/native_cloak_concurrency_client.py") + ":/client.py:ro",
                    str(state_dir) + ":/state:rw", str(command_dir) + ":/commands:rw",
                    str(result_dir) + ":/results:rw", str(token_path) + ":/token:ro"),
            memory="128m", cpu="0.5", pids=64)

        # Complete a foreign CDP handshake/probe before the worker starts so
        # relay readiness is a separate observable prerequisite.
        self.phase = "foreign_peer_startup"
        self._wait_peer_open(observer)
        # Same Cloak browser and peer CDP session across worker restarts.
        self.phase = "profile_one"
        worker = self._start_worker(self.control_ip(2), gateway_ip, 1)
        one = self._wait_command(observer, "profile_one")
        self._snapshot_cgroup(worker, "worker-concurrency-1")
        self._docker("stop", "--time", "5", worker, timeout=10)
        self.phase = "profile_two"
        worker = self._start_worker(self.control_ip(2), gateway_ip, 2)
        two = self._wait_command(observer, "profile_two")
        self.phase = "cancellation"
        cancelled = self._wait_command(observer, "cancellation", timeout=35)
        self.phase = "upstream_error"
        error = self._wait_command(observer, "upstream_error", timeout=35)
        self.phase = "foreign_peer_probe"
        peer = self._wait_command(observer, "probe")
        for name in tuple(self.containers):
            self._snapshot_cgroup(name)
        closed = self._wait_command(observer, "close")
        report = {
            "schema_version": 1,
            "state": "passed",
            "host_architecture": self._docker("info", "--format", "{{.Architecture}}"),
            "images": image_details,
            "candidate_binary_sha256": {
                "worker": _sha256_file(self.worker_binary), "egress": _sha256_file(self.egress_binary)},
            "resource_caps_bytes": {"cloak": 768 * 1024**2, "worker": 256 * 1024**2,
                                    "gateway": 128 * 1024**2, "origin": 64 * 1024**2,
                                    "observer": 128 * 1024**2},
            "browser_generation_reused_for_both_profiles": True,
            "profiles": {"one": one, "two": two},
            "cancellation": cancelled,
            "upstream_error": error,
            "foreign_peer": {"survived_all_news_cases": peer.get("peer_survived") is True,
                             "closed_cleanly": closed.get("peer_closed") is True},
            "sampled_max_memory_bytes": dict(self.max_memory),
            "container_cgroup_metrics": dict(self.cgroup_metrics),
            "resource_state": self._resource_state(),
            "network_scope": "isolated_internal_docker_networks_with_no_published_ports",
            "platform_note": ("linux/amd64 emulated on ARM64 host" if self._docker("info", "--format", "{{.Architecture}}") in {"aarch64", "arm64"}
                              else "linux/amd64 native host"),
            "acceptance_limit": "synthetic CDP compatibility; not proof of production nftables/relay isolation",
        }
        return report

    @staticmethod
    def _make_certificate(directory: Path) -> tuple[Path, Path]:
        cert, key = directory / "cert.pem", directory / "key.pem"
        _run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
              "-subj", "/CN=native-fixture.test", "-addext",
              "subjectAltName=DNS:native-fixture.test,IP:127.0.0.1",
              "-keyout", str(key), "-out", str(cert)], timeout=20)
        cert.chmod(0o644)
        key.chmod(0o600)
        return cert, key

    def _wait_cloak_launch(self, path: Path) -> None:
        deadline = time.monotonic() + 45
        expected = "715722e8605ae3ce81523c1218aba1ec89425786ab33ceaf99f8a6cb5e70e6e8"
        while time.monotonic() < deadline:
            self._all_runtime_ok()
            try:
                if path.is_file() and not path.is_symlink() and path.stat().st_size <= 16384:
                    value = json.loads(path.read_text())
                    if value.get("binary_sha256") != expected:
                        raise GateError("pinned_browser_binary_digest_mismatch")
                    return
            except (OSError, ValueError):
                pass
            time.sleep(.1)
        raise GateError("pinned_browser_launch_info_missing")

    def _wait_fixture(self, origin: str) -> None:
        script = ("import ssl,urllib.request; c=ssl.create_default_context(cafile='/fixture/cert.pem'); "
                  "urllib.request.urlopen('https://127.0.0.1/healthz',context=c,timeout=2).read()")
        self._wait_exec(origin, "python", "-c", script, error="fixture_readiness_timeout")

    def _wait_egress(self, gateway: str) -> None:
        self._wait_exec(gateway, "/extract-egress", "--healthcheck", error="egress_readiness_timeout")

    def _wait_peer_open(self, observer: str) -> None:
        deadline = time.monotonic() + 15
        path = self.stage / "results" / "peer-open.json"
        while time.monotonic() < deadline:
            self._all_runtime_ok()
            if path.is_file() and not path.is_symlink():
                return
            time.sleep(.1)
        raise GateError("foreign_cdp_client_unavailable")

    def _wait_exec(self, name: str, *args: str, error: str) -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                self._docker("exec", name, *args, timeout=5)
                return
            except GateError:
                self._all_runtime_ok()
                time.sleep(.2)
        raise GateError(error)

    def _resource_state(self) -> dict[str, dict[str, Any]]:
        result = {}
        for name in self.containers:
            self._verify_container(name)
            rows = self._docker("inspect", "--format",
                                "{{.State.Running}} {{.RestartCount}} {{.State.OOMKilled}} {{.State.ExitCode}}", name).split()
            result[name.removeprefix(self.prefix + "-")] = {
                "running": rows[0] == "true", "restart_count": int(rows[1]),
                "oom_killed": rows[2] == "true", "exit_code": int(rows[3])}
        return result

    def cleanup(self) -> list[str]:
        failures = []
        for name in reversed(self.containers):
            try:
                try:
                    self._verify_container(name)
                except GateError:
                    # Planned names may not have been created. If one exists without
                    # our run label, leave it untouched and make cleanup visible.
                    try:
                        self._docker("inspect", name)
                    except GateError as exc:
                        if exc.code == "container_not_found":
                            continue
                        failures.append("container_cleanup_failed")
                        continue
                    failures.append("container_ownership_mismatch")
                    continue
                self._docker("stop", "--time", "3", name, timeout=8)
                self._docker("rm", name, timeout=8)
            except GateError as exc:
                failures.append(exc.code if exc.code == "container_ownership_mismatch"
                                else "container_cleanup_failed")
        for name in reversed(self.networks):
            try:
                try:
                    labels = self._docker("network", "inspect", "--format",
                                          '{{index .Labels "' + RUN_LABEL + '"}}', name)
                except GateError:
                    try:
                        self._docker("network", "inspect", name)
                    except GateError as exc:
                        if exc.code == "network_not_found":
                            continue
                        failures.append("network_cleanup_failed")
                        continue
                    failures.append("network_ownership_mismatch")
                    continue
                if labels != self.run_id:
                    raise GateError("network_ownership_mismatch")
                self._docker("network", "rm", name, timeout=8)
            except GateError as exc:
                failures.append(exc.code if exc.code == "network_ownership_mismatch"
                                else "network_cleanup_failed")
        return failures

    def diagnostic_state(self) -> dict[str, dict[str, Any]]:
        state: dict[str, dict[str, Any]] = {}
        for name in self.containers:
            try:
                self._verify_container(name)
                rows = self._docker("inspect", "--format",
                                    "{{.State.Running}} {{.State.ExitCode}} {{.RestartCount}} {{.State.OOMKilled}}", name).split()
                if len(rows) == 4:
                    state[name.removeprefix(self.prefix + "-")] = {
                        "running": rows[0] == "true", "exit_code": int(rows[1]),
                        "restart_count": int(rows[2]), "oom_killed": rows[3] == "true"}
            except (GateError, ValueError):
                continue
        return state

    def _bounded_container_logs(self, name: str) -> str:
        try:
            result = subprocess.run(["docker", "logs", "--tail", "100", name], capture_output=True,
                                    text=True, timeout=5, check=False)
            return (result.stdout + "\n" + result.stderr)[-65536:]
        except (OSError, subprocess.TimeoutExpired):
            return ""

    def runtime_diagnostics(self) -> dict[str, list[dict[str, str]]]:
        logs: dict[str, str] = {"worker": "", "observer": "", "cloak": ""}
        for role in logs:
            name = self.prefix + "-" + role
            try:
                self._verify_container(name)
            except GateError:
                continue
            logs[role] = self._bounded_container_logs(name)
        cloak_state = self.diagnostic_state().get("cloak", {})
        exit_code = cloak_state.get("exit_code")
        return classify_runtime_logs(logs["worker"], logs["observer"], logs["cloak"], exit_code)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _certificate_spki_sha256(cert: Path) -> str:
    import base64
    public_key = _run(["openssl", "x509", "-in", str(cert), "-pubkey", "-noout"])
    spki = subprocess.run(["openssl", "pkey", "-pubin", "-outform", "DER"],
                          input=public_key.encode(), capture_output=True, check=True).stdout
    return base64.b64encode(hashlib.sha256(spki).digest()).decode()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-binary", type=Path, required=True, help="Prebuilt linux/amd64 extract-service binary")
    parser.add_argument("--egress-binary", type=Path, required=True, help="Prebuilt linux/amd64 extract-egress binary")
    parser.add_argument("--output", type=Path, required=True, help="Private JSON report path")
    parser.add_argument("--stage-parent", type=Path, default=Path(tempfile.gettempdir()))
    args = parser.parse_args()
    stage_parent = args.stage_parent.resolve()
    if not stage_parent.is_dir() or stage_parent.is_symlink():
        raise GateError("stage_parent_invalid")
    worker_binary, egress_binary = checked_binary(args.worker_binary), checked_binary(args.egress_binary)
    if args.output.exists() or args.output.is_symlink():
        raise GateError("output_must_be_new")
    stage = Path(tempfile.mkdtemp(prefix="native-cloak-concurrency-", dir=stage_parent))
    os.chmod(stage, 0o700)
    gate = DockerGate(stage, worker_binary, egress_binary, args.output.resolve())
    result: dict[str, Any] | None = None
    failure: GateError | None = None
    try:
        result = gate.execute()
    except GateError as exc:
        failure = GateError(exc.code, phase=gate.phase if exc.phase == "setup" else exc.phase)
    except (subprocess.CalledProcessError, OSError):
        failure = GateError("helper_operation_failed", phase=gate.phase)
    except Exception:
        failure = GateError("gate_unexpected_failure", phase=gate.phase)
    finally:
        runtime_state = gate.diagnostic_state()
        runtime_diagnostics = gate.runtime_diagnostics()
        try:
            cleanup_failures = gate.cleanup()
        except Exception:
            cleanup_failures = ["cleanup_unexpected_failure"]
        shutil.rmtree(stage, ignore_errors=True)
    if failure or cleanup_failures:
        report = {"schema_version": 1, "state": "failed",
                  "failure_code": failure.code if failure else "cleanup_failed",
                  "phase": failure.phase if failure else gate.phase,
                  "cleanup_failures": cleanup_failures,
                  "runtime_state": runtime_state,
                  "runtime_diagnostics": runtime_diagnostics,
                  "sampled_max_memory_bytes": dict(gate.max_memory),
                  "container_cgroup_metrics": dict(gate.cgroup_metrics)}
        _write_private(args.output.resolve(), report)
        return 1
    assert result is not None
    _write_private(args.output.resolve(), result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
