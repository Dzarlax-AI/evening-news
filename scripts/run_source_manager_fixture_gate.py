#!/usr/bin/env python3
"""Run the Linux/amd64 Go SourceManager against isolated synthetic origins.

The script creates only uniquely named, labeled Docker resources and removes
those exact resources in a finally block. It has no production endpoint or
configuration input. Build the Go binary separately and pass it with --binary.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FIXTURE_ORIGIN = ROOT / "tests/fixtures/go-source-manager/fixture_origin.py"
OWNER_LABEL = "com.dzarlax.source-fixture"
OWNER_VALUE = "20261023"
RUN_LABEL = "com.dzarlax.source-fixture.run"
ORIGIN_IP = "11.203.254.242"
WORKER_IP = "11.203.254.243"
SUBNET = ipaddress.ip_network("11.203.254.240/28")
PYTHON_IMAGE = "python:3.12-alpine"
WORKER_IMAGE = "alpine:3.21"
MEMORY_LIMIT = 256 * 1024 * 1024
COMMAND_FAILURES: list[dict[str, Any]] = []
DIRECT_CLIENT_SUMMARY: dict[str, Any] = {}
NORMALIZATION_DIAGNOSTICS: list[dict[str, Any]] = []


class GateError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class _DockerExecProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        proxy = self.server.proxy
        process = None
        try:
            request = self._read_request()
            process = subprocess.Popen(
                ["docker", "exec", "-i", proxy.container_id,
                 "busybox", "nc", "127.0.0.1", "8080"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                bufsize=0,
            )
            proxy.register(self.request, process)
            assert process.stdin is not None and process.stdout is not None
            view = memoryview(request)
            while view:
                written = os.write(process.stdin.fileno(), view)
                if written <= 0:
                    raise GateError("proxy_worker_write_failed")
                view = view[written:]
            # Keep nc's stdin open while the worker handles the complete
            # request. The injected Connection: close makes the worker close
            # its response side; closing stdin here would prematurely close
            # the TCP connection and cancel the Go request context.
            while True:
                data = os.read(process.stdout.fileno(), 65_536)
                if not data:
                    break
                self.request.sendall(data)
            process.wait(timeout=5)
        except (OSError, ValueError, subprocess.SubprocessError, GateError):
            return
        finally:
            try:
                self.request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.request.close()
            except OSError:
                pass
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            if process is not None:
                for stream in (process.stdin, process.stdout):
                    try:
                        if stream is not None:
                            stream.close()
                    except (OSError, ValueError):
                        pass
                proxy.unregister(self.request, process)

    def _read_request(self) -> bytes:
        data = bytearray()
        while b"\r\n\r\n" not in data:
            chunk = self.request.recv(16_384)
            if not chunk:
                raise GateError("proxy_incomplete_http_request")
            data.extend(chunk)
            if len(data) > 65_536:
                raise GateError("proxy_request_headers_too_large")
        split = data.index(b"\r\n\r\n") + 4
        header = bytes(data[:split])
        body = bytearray(data[split:])
        lines = header[:-4].split(b"\r\n")
        if not lines or len(lines[0].split()) != 3:
            raise GateError("proxy_invalid_http_request")
        content_length = 0
        normalized = [lines[0]]
        for line in lines[1:]:
            name, separator, value = line.partition(b":")
            if not separator:
                raise GateError("proxy_invalid_http_request")
            lowered = name.strip().lower()
            if lowered in {b"transfer-encoding", b"connection", b"proxy-connection"}:
                continue
            if lowered == b"content-length":
                try:
                    content_length = int(value.strip())
                except ValueError:
                    raise GateError("proxy_invalid_content_length") from None
                if content_length < 0 or content_length > 8 * 1024 * 1024:
                    raise GateError("proxy_request_too_large")
            normalized.append(line)
        while len(body) < content_length:
            chunk = self.request.recv(min(65_536, content_length - len(body)))
            if not chunk:
                raise GateError("proxy_incomplete_http_request")
            body.extend(chunk)
        if len(body) != content_length:
            raise GateError("proxy_multiple_requests_not_supported")
        normalized.append(b"Connection: close")
        return b"\r\n".join(normalized) + b"\r\n\r\n" + bytes(body)


class _DockerExecProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    block_on_close = False


class DockerExecTCPProxy:
    """Expose the worker API on loopback without publishing a container port."""

    def __init__(self, container_id: str):
        self.container_id = container_id
        self._lock = threading.Lock()
        self._clients: set[socket.socket] = set()
        self._processes: set[subprocess.Popen] = set()
        self._server = _DockerExecProxyServer(("127.0.0.1", 0), _DockerExecProxyHandler)
        self._server.proxy = self
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address[:2]
        self.endpoint = f"http://{host}:{port}"
        self._closed = False

    def register(self, client: socket.socket, process: subprocess.Popen) -> None:
        with self._lock:
            self._clients.add(client)
            self._processes.add(process)

    def unregister(self, client: socket.socket, process: subprocess.Popen) -> None:
        with self._lock:
            self._clients.discard(client)
            self._processes.discard(process)

    def close(self) -> bool:
        if self._closed:
            return True
        self._closed = True
        try:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=3)
        except OSError:
            return False
        with self._lock:
            clients = list(self._clients)
            processes = list(self._processes)
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
                client.close()
            except OSError:
                pass
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
        return not any(process.poll() is None for process in processes)


def command(args: list[str], *, check: bool = True, timeout: int = 60) -> str:
    try:
        result = subprocess.run(args, check=False, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        COMMAND_FAILURES.append({"program": args[0] if args else "unknown",
                                 "operation": args[1] if len(args) > 1 else "unknown",
                                 "failure": "unavailable_or_timeout"})
        raise GateError("command_unavailable") from None
    if check and result.returncode != 0:
        safe_suboperation = args[2] if len(args) > 2 and args[2] in {
            "pull", "inspect", "create", "run", "rm", "ps", "ls", "info", "exec",
        } else None
        if args[:2] == ["docker", "run"]:
            safe_suboperation = "container_create"
        diagnostic = result.stderr.lower()
        safe_class = next((kind for marker, kind in (
            ("cannot connect", "daemon_unreachable"),
            ("error during connect", "daemon_unreachable"),
            ("permission denied", "permission_denied"),
            ("unauthorized", "registry_auth_failed"),
            ("manifest unknown", "image_not_found"),
            ("no matching manifest", "platform_manifest_unavailable"),
            ("no such host", "registry_dns_failed"),
            ("network is unreachable", "registry_network_unreachable"),
            ("no such image", "image_not_found"),
            ("no space left", "storage_exhausted"),
            ("invalid reference format", "invalid_image_reference"),
            ("invalid mount config", "invalid_bind_mount"),
            ("exec format error", "image_or_binary_architecture_mismatch"),
            ("operation not permitted", "container_runtime_permission_denied"),
            ("not found in manifest", "image_platform_unavailable"),
            ("image operating system", "image_platform_mismatch"),
        ) if marker in diagnostic), "other_nonzero")
        COMMAND_FAILURES.append({"program": args[0] if args else "unknown",
                                 "operation": args[1] if len(args) > 1 else "unknown",
                                 "suboperation": safe_suboperation,
                                 "class": safe_class,
                                 "failure": "nonzero_exit",
                                 "exit_code": result.returncode})
        raise GateError("command_failed")
    return result.stdout.strip()


def docker_json(args: list[str]) -> Any:
    output = command(["docker", *args])
    try:
        return json.loads(output)
    except (ValueError, TypeError):
        raise GateError("docker_invalid_response") from None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_binary(binary: Path) -> dict[str, Any]:
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise GateError("binary_missing_or_not_executable")
    with binary.open("rb") as handle:
        header = handle.read(64)
    if len(header) < 64 or header[:4] != b"\x7fELF":
        raise GateError("binary_not_elf")
    elf_class = header[4]
    endian = "<" if header[5] == 1 else ">" if header[5] == 2 else None
    if endian is None or elf_class != 2 or struct.unpack(endian + "H", header[18:20])[0] != 62:
        raise GateError("binary_not_linux_amd64")
    program_header_offset = struct.unpack(endian + "Q", header[32:40])[0]
    program_header_size = struct.unpack(endian + "H", header[54:56])[0]
    program_header_count = struct.unpack(endian + "H", header[56:58])[0]
    if program_header_size < 4 or program_header_count > 4096:
        raise GateError("binary_elf_headers_invalid")
    with binary.open("rb") as handle:
        for index in range(program_header_count):
            handle.seek(program_header_offset + index * program_header_size)
            program_header = handle.read(program_header_size)
            if len(program_header) < 4:
                raise GateError("binary_elf_headers_invalid")
            if struct.unpack(endian + "I", program_header[:4])[0] == 3:  # PT_INTERP
                raise GateError("binary_not_static")
    return {"sha256": sha256_file(binary), "format": "static ELF64 Linux amd64"}


def docker_architecture() -> str:
    info = docker_json(["info", "--format", "{{json .}}"])
    architecture = info.get("Architecture")
    if architecture not in {"x86_64", "amd64", "aarch64", "arm64"} or info.get("OSType") != "linux":
        raise GateError("docker_engine_architecture_unsupported")
    return architecture


def assert_subnet_available() -> None:
    network_ids = command(["docker", "network", "ls", "-q"]).splitlines()
    if not network_ids:
        return
    networks = docker_json(["network", "inspect", *network_ids])
    for network in networks:
        for config in network.get("IPAM", {}).get("Config", []) or []:
            raw_subnet = config.get("Subnet")
            if not raw_subnet:
                continue
            try:
                existing = ipaddress.ip_network(raw_subnet, strict=False)
            except ValueError:
                raise GateError("docker_network_subnet_unreadable") from None
            if SUBNET.overlaps(existing):
                raise GateError("fixture_subnet_overlaps_docker_network")


def resolve_amd64_image(image: str) -> dict[str, str | None]:
    command(["docker", "pull", "--platform", "linux/amd64", image], timeout=300)
    selected = docker_json(["image", "inspect", "--platform", "linux/amd64", image])
    if not isinstance(selected, list) or not selected:
        raise GateError("docker_image_unavailable")
    platform_image = selected[0]
    if platform_image.get("Architecture") != "amd64" or platform_image.get("Os") != "linux":
        raise GateError("fixture_image_not_linux_amd64")
    image_id = platform_image.get("Id")
    if not isinstance(image_id, str) or not image_id.startswith("sha256:"):
        raise GateError("docker_image_digest_unavailable")
    repo_digests = platform_image.get("RepoDigests") or []
    repo_name = image.split(":", 1)[0].rsplit("/", 1)[-1]
    repo_digest = None
    for item in repo_digests:
        if (isinstance(item, str) and "@sha256:" in item
                and item.split("@", 1)[0].rsplit("/", 1)[-1] == repo_name):
            repo_digest = item
            break
    if not repo_digest:
        raise GateError("docker_repository_digest_unavailable")
    # Docker Desktop can inspect the selected config by platform but cannot
    # run that config ID directly. Use the immutable repository index digest
    # plus --platform, then verify the created container resolved to this ID.
    return {"reference": repo_digest, "platform_image_id": image_id,
            "repo_digest": repo_digest, "platform": "linux/amd64"}


def verify_container_image(container_state: dict[str, Any], image: dict[str, Any], name: str) -> None:
    reference = container_state.get("Image")
    if not isinstance(reference, str) or not reference.startswith("sha256:"):
        raise GateError(f"{name}_resolved_image_reference_invalid")
    resolved = docker_json(["image", "inspect", "--platform", "linux/amd64", reference])
    if not isinstance(resolved, list) or not resolved:
        raise GateError(f"{name}_resolved_image_unavailable")
    actual = resolved[0]
    if (actual.get("Id") != image["platform_image_id"]
            or actual.get("Os") != "linux" or actual.get("Architecture") != "amd64"
            or image["repo_digest"] not in (actual.get("RepoDigests") or [])):
        raise GateError(f"{name}_resolved_image_mismatch")


def go_source_sha256() -> dict[str, Any]:
    files = sorted((ROOT / "go").rglob("*.go"))
    digest = hashlib.sha256()
    for path in files:
        relative = path.relative_to(ROOT).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return {"sha256": digest.hexdigest(), "go_file_count": len(files)}


def run_id() -> str:
    return uuid.uuid4().hex[:12]


def labels(run: str) -> list[str]:
    return ["--label", f"{OWNER_LABEL}={OWNER_VALUE}", "--label", f"{RUN_LABEL}={run}"]


def inspect_owned(kind: str, resource_id: str, run: str) -> bool:
    if kind == "container":
        data = docker_json(["container", "inspect", resource_id])
        labels_data = data[0].get("Config", {}).get("Labels", {})
    else:
        data = docker_json(["network", "inspect", resource_id])
        labels_data = data[0].get("Labels", {})
    return labels_data.get(OWNER_LABEL) == OWNER_VALUE and labels_data.get(RUN_LABEL) == run


def create_network(name: str, run: str) -> str:
    existing = command(["docker", "network", "ls", "-q", "--filter", f"name=^{name}$"], check=False)
    if existing:
        raise GateError("resource_name_collision")
    out = command([
        "docker", "network", "create", "--internal", "--driver", "bridge",
        "--subnet", str(SUBNET), "--gateway", str(SUBNET.network_address + 1),
        *labels(run), name,
    ])
    return out


def create_container(args: list[str], name: str, run: str) -> str:
    existing = command(["docker", "ps", "-aq", "--filter", f"name=^{name}$"], check=False)
    if existing:
        raise GateError("resource_name_collision")
    container_id = command(["docker", "run", "-d", "--name", name, *labels(run), *args], timeout=120)
    return container_id


def generate_certificate(directory: Path) -> tuple[Path, Path]:
    cert = directory / "fixture-ca.crt"
    key = directory / "fixture-ca.key"
    san = f"DNS:t.me,DNS:rss.fixture.test,DNS:pm.fixture.test,IP:{ORIGIN_IP}"
    command([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=source-fixture.test",
        "-addext", f"subjectAltName={san}",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,digitalSignature,keyCertSign,keyEncipherment",
        "-addext", "extendedKeyUsage=serverAuth",
    ])
    cert.chmod(0o444)
    key.chmod(0o444)
    return cert, key


def wait_http(url: str, token: str | None = None, timeout: float = 45.0) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
            with urllib.request.urlopen(request, timeout=2) as response:
                body = response.read(32_768)
                if response.status == 200:
                    return json.loads(body) if body.startswith(b"{") else None
        except (OSError, urllib.error.URLError, ValueError):
            time.sleep(0.25)
    raise GateError("service_readiness_timeout")


def cgroup_value(container: str, filename: str) -> int | None:
    output = command(["docker", "exec", container, "cat", f"/sys/fs/cgroup/{filename}"], check=False)
    try:
        value = int(output.strip())
    except ValueError:
        return None
    return value if value >= 0 else None


def cgroup_key_values(container: str, filename: str, keys: set[str]) -> dict[str, int] | None:
    output = command(["docker", "exec", container, "cat", f"/sys/fs/cgroup/{filename}"], check=False)
    parsed: dict[str, int] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0] not in keys:
            continue
        try:
            value = int(parts[1])
        except ValueError:
            return None
        if value < 0:
            return None
        parsed[parts[0]] = value
    return parsed if keys.issubset(parsed) else None


def wait_fixture_origin(worker_id: str, timeout: float = 45.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "exec", worker_id, "busybox", "wget", "-q", "-T", "2", "-O", "-",
             "http://rss.fixture.test/healthz"],
            check=False, capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip() == "ok":
            return
        time.sleep(0.5)
    raise GateError("fixture_origin_readiness_timeout")


def docker_container_state(container_id: str) -> dict[str, Any]:
    data = docker_json(["container", "inspect", container_id])[0]
    state = data.get("State", {})
    if data.get("HostConfig", {}).get("Memory") != MEMORY_LIMIT:
        raise GateError("worker_memory_limit_mismatch")
    if data.get("HostConfig", {}).get("NanoCpus") != 1_000_000_000:
        raise GateError("worker_cpu_limit_mismatch")
    if state.get("OOMKilled"):
        raise GateError("worker_oom_killed")
    return {"oom_killed": bool(state.get("OOMKilled")), "status": state.get("Status")}


def report_stats(endpoint: str, token: str) -> dict[str, Any]:
    result = wait_http(endpoint + "/stats", token=token)
    if not isinstance(result, dict):
        raise GateError("stats_invalid")
    return {key: result.get(key) for key in ("completed", "inflight", "admission_inflight", "heap_bytes", "goroutines")}


def make_sources(endpoint: str) -> list[dict[str, Any]]:
    from news_aggregator.models import Source

    return [
        Source(name="Fixture same-name RSS", source_type="rss",
               url="http://rss.fixture.test/rss.xml?edition=one", enabled=True, config={"limit": 10}),
        Source(name="Fixture same-name RSS", source_type="rss",
               url="http://rss.fixture.test/rss.xml?edition=two", enabled=True, config={"limit": 10}),
        Source(name="Fixture Telegram", source_type="telegram",
               url="https://t.me/s/fixture_channel", enabled=True, config={"limit": 5}),
        Source(name="Fixture Page Monitor", source_type="custom",
               url="http://pm.fixture.test/news", enabled=True,
               config={"limit": 10, "max_articles_per_check": 10, "min_title_length": 10,
                       "article_selectors": ["article"], "title_selectors": ["h2"],
                       "link_selectors": ["a"], "date_selectors": ["time"],
                       "use_browser": False, "wait_for_js": False, "enable_ai_analysis": False}),
    ]


async def exercise_python_path(endpoint: str, token: str, cycles: int, db_path: Path) -> dict[str, Any]:
    from pydantic import SecretStr
    from sqlalchemy import func, select
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from unittest.mock import patch
    from news_aggregator.config import settings
    from news_aggregator.models import Article, Source
    from news_aggregator.services.source_manager import SourceManager
    from news_aggregator.sources.rss_source import RSSSource
    from news_aggregator.telegram.telegram_source import TelegramSource
    from news_aggregator.sources.page_monitor_adapter import PageMonitorAdapter
    from news_aggregator.sources.page_monitor_source import PageMonitorSource

    engine = create_engine(f"sqlite:///{db_path}")
    Source.__table__.create(engine)
    Article.__table__.create(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session:
        sources = make_sources(endpoint)
        session.add_all(sources)
        session.commit()
        for source in sources:
            session.refresh(source)
        if len({source.id for source in sources}) != 4:
            raise GateError("sqlite_source_ids_not_unique")

    original_values = {
        "source_manager_mode": settings.source_manager_mode,
        "telegram_source_mode": settings.telegram_source_mode,
        "page_monitor_source_mode": settings.page_monitor_source_mode,
        "extraction_service_url": settings.extraction_service_url,
        "extraction_service_token": settings.extraction_service_token,
        "extraction_service_total_budget_seconds": settings.extraction_service_total_budget_seconds,
        "extraction_service_native_browser": settings.extraction_service_native_browser,
    }
    settings.source_manager_mode = "go"
    settings.telegram_source_mode = "go"
    settings.page_monitor_source_mode = "go"
    settings.extraction_service_url = endpoint
    settings.extraction_service_token = SecretStr(token)
    settings.extraction_service_total_budget_seconds = 120.0
    settings.extraction_service_native_browser = False
    manager = SourceManager()
    original_normalize = manager._normalize_go_source_result

    async def record_normalization_error(source, result, *, deadline):
        try:
            return await original_normalize(source, result, deadline=deadline)
        except Exception as exc:
            type_error_text = str(exc).lower() if isinstance(exc, TypeError) else ""
            NORMALIZATION_DIAGNOSTICS.append({
                "source_id": getattr(source, "id", None),
                "source_type": getattr(source, "source_type", None),
                "exception_type": type(exc).__name__,
                "type_error_kind": ("unexpected_deadline_keyword"
                                    if "unexpected keyword" in type_error_text and "deadline" in type_error_text
                                    else "other_type_error" if type_error_text else None),
                "result_keys": sorted(result) if isinstance(result, dict) else [],
            })
            raise

    async def forbidden_python_fetch(*_args, **_kwargs):
        raise AssertionError("python_source_fetch_called")

    async def fixture_has_no_learned_selectors(instance):
        # Keep selector setup local and avoid touching the app's configured DB.
        instance._learned_structure_loaded = True
        instance.learned_container_selectors = []
        instance.learned_title_selectors = []
        instance.learned_link_selectors = []
        instance.learned_date_selectors = []

    counts = []
    source_counts = {}
    initial_source_counts = {}
    expected_urls_by_id = {}
    cycle_times = []

    class AsyncSQLiteSession:
        """Async facade over isolated sync SQLite for this DB-only save method."""

        def __init__(self, session):
            self.session = session

        async def execute(self, *args, **kwargs):
            return self.session.execute(*args, **kwargs)

        def add(self, value):
            self.session.add(value)

        async def commit(self):
            self.session.commit()

    try:
        with patch.object(manager, "_normalize_go_source_result", record_normalization_error), \
                patch.object(RSSSource, "fetch_articles", forbidden_python_fetch), \
                patch.object(TelegramSource, "fetch_articles", forbidden_python_fetch), \
                patch.object(PageMonitorAdapter, "fetch_articles", forbidden_python_fetch), \
                patch.object(PageMonitorSource, "_load_learned_structure", fixture_has_no_learned_selectors):
            for cycle in range(cycles):
                started = time.monotonic()
                raw = await manager.fetch_from_all_sources_no_db(sources, per_source_timeout=120)
                if set(raw) != {source.id for source in sources}:
                    raise GateError("source_manager_result_ids_mismatch")
                rss_ids = [source.id for source in sources[:2]]
                if any(len(raw[source_id]) < 2 for source_id in rss_ids):
                    raise GateError("rss_fixture_count_mismatch")
                if len(raw[sources[2].id]) < 1:
                    raise GateError("telegram_fixture_count_mismatch")
                if cycle == 0 and len(raw[sources[3].id]) < 2:
                    raise GateError("page_monitor_fixture_count_mismatch")
                rss_names = [article.source_name for source_id in rss_ids for article in raw[source_id]]
                if any(name != sources[0].name for name in rss_names):
                    raise GateError("rss_source_name_metadata_mismatch")
                urls_by_id = {
                    source_id: {article.url for article in raw[source_id]}
                    for source_id in rss_ids
                }
                if not urls_by_id[rss_ids[0]].isdisjoint(urls_by_id[rss_ids[1]]):
                    raise GateError("rss_source_url_correlation_mismatch")
                if cycle == 0:
                    expected_urls_by_id = {
                        source_id: {article.url for article in articles}
                        for source_id, articles in raw.items()
                    }

                with sessions() as sync_session:
                    session = AsyncSQLiteSession(sync_session)
                    saved = await manager.save_fetched_articles_with_sources(
                        raw, {source.id: source for source in sources}, session)
                    await session.commit()
                    total = sync_session.scalar(select(func.count(Article.id)))
                    rows = sync_session.execute(select(Article.id, Article.source_id, Article.url)).all()
                if set(saved) != {source.id for source in sources}:
                    raise GateError("saved_source_ids_mismatch")
                if len(rows) != int(total or 0):
                    raise GateError("sqlite_article_count_mismatch")
                actual_source_ids = {row.source_id for row in rows}
                if actual_source_ids != {source.id for source in sources}:
                    raise GateError("sqlite_source_relationship_mismatch")
                if len({row.url for row in rows}) != len(rows):
                    raise GateError("sqlite_article_url_dedup_mismatch")
                persisted_by_source = {}
                for row in rows:
                    persisted_by_source.setdefault(row.source_id, set()).add(row.url)
                if persisted_by_source != expected_urls_by_id:
                    raise GateError("sqlite_article_source_correlation_mismatch")
                if counts and total != counts[0]:
                    raise GateError("sqlite_repeat_cycle_not_idempotent")
                counts.append(int(total or 0))
                source_counts = {str(key): len(value) for key, value in raw.items()}
                if cycle == 0:
                    initial_source_counts = dict(source_counts)
                cycle_times.append(round(time.monotonic() - started, 4))
    finally:
        for name, value in original_values.items():
            setattr(settings, name, value)
        engine.dispose()

    return {
        "cycles": cycles,
        "source_types": 4,
        "source_ids_distinct": len({source.id for source in sources}) == 4,
        "duplicate_rss_names_preserved_by_id": True,
        "first_cycle_articles_by_source_id": initial_source_counts,
        "per_cycle_articles_by_source_id": source_counts,
        "sqlite_article_counts": counts,
        "repeat_persistence_idempotent": len(set(counts)) == 1,
        "cycle_wall_seconds": cycle_times,
        "python_source_fetch_fallback": "forbidden_and_not_called",
    }


def direct_client_check(endpoint: str, token: str) -> dict[str, Any]:
    from news_aggregator.sources.source_manager_service_client import SourceManagerServiceClient

    sources = [
        {"source_id": 901, "type": "rss", "source_name": "Fixture direct RSS",
         "url": "http://rss.fixture.test/rss.xml?edition=one", "enabled": True, "limit": 10},
        {"source_id": 904, "type": "rss", "source_name": "Fixture expected failure",
         "url": "http://rss.fixture.test/missing.xml", "enabled": True, "limit": 10},
        {"source_id": 902, "type": "telegram", "source_name": "Fixture direct Telegram",
         "url": "https://t.me/s/fixture_channel", "enabled": True, "limit": 5,
         "telegram": {"limit": 5}},
        {"source_id": 903, "type": "custom", "source_name": "Fixture direct monitor",
         "url": "http://pm.fixture.test/news", "enabled": True, "limit": 10,
         "page_monitor": {"use_browser": False, "wait_for_js": False, "limit": 10,
                           "min_title_length": 10, "article_selectors": ["article"],
                           "title_selectors": ["h2"], "link_selectors": ["a"],
                           "date_selectors": ["time"]}},
    ]
    response = asyncio.run(SourceManagerServiceClient(endpoint, token).collect(sources, timeout_seconds=60))
    results = response["results"]
    outcomes = {str(item["source_id"]): item["outcome"] for item in results}
    result_by_id = {str(item["source_id"]): item for item in results}
    counts = {
        str(item["source_id"]): len(item.get("rss_articles") or [])
        if item["type"] == "rss"
        else len(item.get("telegram", {}).get("articles", []))
        if item["type"] == "telegram"
        else len((item.get("page_monitor") or {}).get("items", []))
        for item in results
    }
    failures = {
        str(item["source_id"]): {
            "code": (item.get("error") or {}).get("code"),
            "retryable": (item.get("error") or {}).get("retryable"),
        }
        for item in results if item.get("error") is not None
    }
    DIRECT_CLIENT_SUMMARY.update({"outcomes": outcomes, "item_counts": counts,
                                  "finite_failures": failures})
    if outcomes != {"901": "success", "902": "success", "903": "success", "904": "failed"}:
        raise GateError("direct_client_outcome_mismatch")
    failure = result_by_id["904"].get("error")
    from news_aggregator.sources.source_manager_service_client import SourceManagerServiceClient
    if (not isinstance(failure, dict) or not SourceManagerServiceClient._finite_source_error_code(failure.get("code"))
            or type(failure.get("retryable")) is not bool):
        raise GateError("direct_client_failure_code_invalid")
    if counts["901"] < 2 or counts["902"] < 1 or counts["903"] < 2 or counts["904"] != 0:
        raise GateError("direct_client_fixture_count_mismatch")
    return {"source_ids": sorted(outcomes), "outcomes": outcomes, "item_counts": counts,
            "expected_failure_code": failure["code"], "expected_failure_retryable": failure["retryable"]}


def create_worker(network: str, name: str, run: str, image: str, binary: Path,
                  cert: Path, token: str) -> str:
    args = [
        "--platform", "linux/amd64", "--network", network, "--ip", WORKER_IP,
        "--add-host", f"rss.fixture.test:{ORIGIN_IP}",
        "--add-host", f"pm.fixture.test:{ORIGIN_IP}",
        "--add-host", f"t.me:{ORIGIN_IP}",
        "--memory", str(MEMORY_LIMIT), "--memory-swap", str(MEMORY_LIMIT),
        "--cpus", "1", "--pids-limit", "128", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--read-only",
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m",
        "--user", "65534:65534",
        "-e", "LISTEN_ADDR=:8080", "-e", "EXTRACTOR_HTTP_ONLY=true",
        "-e", "EXTRACTOR_MAX_CONCURRENT=1", "-e", f"EXTRACTOR_SERVICE_TOKEN={token}",
        "-e", "SSL_CERT_FILE=/fixtures/fixture-ca.crt",
        "-v", f"{binary}:/usr/local/bin/extract-service:ro",
        "-v", f"{cert}:/fixtures/fixture-ca.crt:ro",
        image, "/usr/local/bin/extract-service",
    ]
    return create_container(args, name, run)


def create_origin(network: str, name: str, run: str, image: str,
                  origin_script: Path, cert: Path, key: Path) -> str:
    args = [
        "--platform", "linux/amd64", "--network", network, "--ip", ORIGIN_IP,
        "--network-alias", "rss.fixture.test", "--network-alias", "pm.fixture.test",
        "--network-alias", "t.me", "--memory", str(128 * 1024 * 1024),
        "--memory-swap", str(128 * 1024 * 1024), "--cpus", "1",
        "--pids-limit", "64", "--cap-drop", "ALL", "--security-opt",
        "no-new-privileges", "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=8m",
        "--cap-add", "NET_BIND_SERVICE", "--user", "65534:65534",
        "-v", f"{origin_script}:/fixture_origin.py:ro",
        "-v", f"{cert}:/fixture-ca.crt:ro", "-v", f"{key}:/fixture-ca.key:ro",
        image, "python3", "/fixture_origin.py", "--cert", "/fixture-ca.crt", "--key", "/fixture-ca.key",
    ]
    return create_container(args, name, run)


def cleanup(resources: dict[str, str], run: str) -> bool:
    okay = True
    try:
        container_ids = set(resources.get("containers", []))
        container_ids.update(command([
            "docker", "ps", "-aq", "--no-trunc", "--filter", f"label={OWNER_LABEL}={OWNER_VALUE}",
            "--filter", f"label={RUN_LABEL}={run}",
        ]).splitlines())
        network_ids = set([resources["network"]] if resources.get("network") else [])
        network_ids.update(command([
            "docker", "network", "ls", "-q", "--no-trunc", "--filter", f"label={OWNER_LABEL}={OWNER_VALUE}",
            "--filter", f"label={RUN_LABEL}={run}",
        ]).splitlines())
    except GateError:
        return False
    ordered = [("container", item) for item in sorted(container_ids, reverse=True)]
    ordered.extend(("network", item) for item in sorted(network_ids))

    def is_missing(kind: str, resource_id: str, diagnostic: str) -> bool:
        diagnostic = diagnostic.lower()
        return resource_id.lower() in diagnostic and (
            "not found" in diagnostic or "no such" in diagnostic
        )

    for kind, resource_id in ordered:
        try:
            check = subprocess.run(["docker", kind, "inspect", resource_id], check=False,
                                   capture_output=True, text=True, timeout=20)
            if check.returncode != 0:
                if is_missing(kind, resource_id, check.stderr):
                    continue
                okay = False
                continue
            if not check.stdout:
                okay = False
                continue
            if not inspect_owned(kind, resource_id, run):
                okay = False
                continue
            command(["docker", "rm", "-f", resource_id] if kind == "container"
                    else ["docker", "network", "rm", resource_id], check=False)
            verify = subprocess.run(["docker", kind, "inspect", resource_id], check=False,
                                    capture_output=True, text=True, timeout=20)
            if verify.returncode == 0 or not is_missing(kind, resource_id, verify.stderr):
                okay = False
        except GateError:
            okay = False
    return okay


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="separately built static Linux/amd64 extract-service binary")
    parser.add_argument("--report", required=True, type=Path, help="JSON report path, preferably under /tmp")
    parser.add_argument("--cycles", type=int, default=6)
    args = parser.parse_args()
    report: dict[str, Any] = {"status": "failed", "scope": "local synthetic fixture only"}
    resources: dict[str, str] = {}
    proxy: DockerExecTCPProxy | None = None
    run = run_id()
    phase = "startup"
    started = time.monotonic()
    try:
        if args.cycles != 6:
            raise GateError("cycles_must_equal_six")
        binary = args.binary.resolve(strict=True)
        binary_report = verify_binary(binary)
        if not shutil.which("docker") or not shutil.which("openssl"):
            raise GateError("required_command_unavailable")
        phase = "docker_preflight"
        architecture = docker_architecture()
        source_sha = go_source_sha256()
        assert_subnet_available()
        phase = "resolve_images"
        images = {"origin": resolve_amd64_image(PYTHON_IMAGE),
                  "worker": resolve_amd64_image(WORKER_IMAGE)}
        name = f"rssfix-{run}"
        phase = "create_internal_network"
        network_id = create_network(name, run)
        resources["network"] = network_id
        if not inspect_owned("network", network_id, run):
            raise GateError("network_ownership_check_failed")
        token = secrets.token_urlsafe(32)
        with tempfile.TemporaryDirectory(prefix="rss-source-fixture-") as temporary:
            temporary_path = Path(temporary)
            temporary_path.chmod(0o755)
            phase = "create_synthetic_origin"
            cert, key = generate_certificate(temporary_path)
            origin_id = create_origin(network_id, name + "-origin", run,
                                      images["origin"]["reference"], FIXTURE_ORIGIN, cert, key)
            resources.setdefault("containers", []).append(origin_id)
            if not inspect_owned("container", origin_id, run):
                raise GateError("container_ownership_check_failed")
            phase = "create_go_worker_and_proxy"
            worker_id = create_worker(network_id, name + "-worker", run,
                                       images["worker"]["reference"], binary, cert, token)
            resources["containers"].append(worker_id)
            if not inspect_owned("container", worker_id, run):
                raise GateError("container_ownership_check_failed")
            proxy = DockerExecTCPProxy(worker_id)
            endpoint = proxy.endpoint
            wait_http(endpoint + "/health/ready", token=token)
            phase = "wait_synthetic_origin"
            wait_fixture_origin(worker_id)
            phase = "verify_container_isolation"
            origin_state = docker_json(["container", "inspect", origin_id])[0]
            verify_container_image(origin_state, images["origin"], "origin")
            origin_networks = origin_state.get("NetworkSettings", {}).get("Networks", {})
            worker_state = docker_json(["container", "inspect", worker_id])[0]
            verify_container_image(worker_state, images["worker"], "worker")
            worker_networks = worker_state.get("NetworkSettings", {}).get("Networks", {})
            network_name = docker_json(["network", "inspect", network_id])[0].get("Name")
            if set(origin_networks) != {network_name} or set(worker_networks) != {network_name}:
                raise GateError("fixture_container_network_scope_mismatch")
            if origin_networks[network_name].get("IPAddress") != ORIGIN_IP:
                raise GateError("origin_ip_assignment_mismatch")
            if worker_networks[network_name].get("IPAddress") != WORKER_IP:
                raise GateError("worker_ip_assignment_mismatch")
            allowed_network_modes = {network_name, network_id}
            if (origin_state.get("HostConfig", {}).get("NetworkMode") not in allowed_network_modes
                    or worker_state.get("HostConfig", {}).get("NetworkMode") not in allowed_network_modes):
                raise GateError("fixture_container_network_scope_mismatch")
            hosts = command(["docker", "exec", worker_id, "cat", "/etc/hosts"])
            for host in ("rss.fixture.test", "pm.fixture.test", "t.me"):
                mappings = [line.split() for line in hosts.splitlines() if host in line.split()]
                if not mappings or any(entry[0] != ORIGIN_IP for entry in mappings):
                    raise GateError("fixture_dns_mapping_mismatch")
            wait_http(endpoint + "/version", token=token)
            phase = "direct_source_manager_client"
            before = report_stats(endpoint, token)
            # The application modules also create their normal async DB engine
            # at import time. Point that engine at a dead local-only address;
            # the exercised save path below uses its own isolated SQLite file.
            os.environ["DATABASE_URL"] = "postgresql+asyncpg://fixture:fixture@127.0.0.1:1/fixture"
            client_result = direct_client_check(endpoint, token)
            db_path = temporary_path / "source-fixture.sqlite3"
            phase = "python_source_manager_sqlite_integration"
            python_result = asyncio.run(exercise_python_path(endpoint, token, args.cycles, db_path))
            phase = "measure_worker_resources"
            after = report_stats(endpoint, token)
            if after.get("completed", 0) - before.get("completed", 0) != args.cycles + 1:
                raise GateError("unexpected_go_batch_call_count")
            cgroup_current = cgroup_value(worker_id, "memory.current")
            cgroup_peak = cgroup_value(worker_id, "memory.peak")
            memory_events = cgroup_key_values(worker_id, "memory.events", {"max", "oom", "oom_kill"})
            memory_stat = cgroup_key_values(worker_id, "memory.stat", {"anon", "file"})
            state = docker_container_state(worker_id)
            if memory_events is None:
                raise GateError("worker_memory_events_unavailable")
            if memory_stat is None:
                raise GateError("worker_memory_stat_unavailable")
            if any(memory_events[key] != 0 for key in ("max", "oom", "oom_kill")):
                raise GateError("worker_memory_events_exceeded")
            if cgroup_peak is not None and cgroup_peak > MEMORY_LIMIT:
                raise GateError("worker_memory_peak_exceeded")
            if after.get("inflight") != 0 or after.get("admission_inflight") != 0:
                raise GateError("worker_not_idle_after_cycles")
            report.update({
                "status": "passed",
                "run_id": run,
                "binary": binary_report,
                "docker_architecture": architecture,
                "worker_platform": "linux/amd64",
                "go_source": source_sha,
                "images": images,
                "worker_limit_bytes": MEMORY_LIMIT,
                "fixture_origin_limit_bytes": 128 * 1024 * 1024,
                "worker_cgroup_memory_current_bytes": cgroup_current,
                "worker_cgroup_memory_peak_bytes": cgroup_peak,
                "worker_peak_available": cgroup_peak is not None,
                "worker_cgroup_memory_events": memory_events,
                "worker_cgroup_memory_stat": memory_stat,
                "worker_state": state,
                "go_stats_before": before,
                "go_stats_after": after,
                "direct_client": client_result,
                "python_source_manager_and_sqlite": python_result,
                "fixture_network": {"internal": True, "subnet": str(SUBNET),
                                    "origin_ip": ORIGIN_IP, "worker_ip": WORKER_IP,
                                    "container_networks": [network_name],
                                    "api_access": "127.0.0.1 TCP proxy via docker exec"},
            })
    except GateError as exc:
        report["error_code"] = exc.code
    except Exception as exc:
        report["error_code"] = "unexpected_harness_error"
        report["error_phase"] = phase
        report["error_type"] = type(exc).__name__
    finally:
        report["wall_seconds"] = round(time.monotonic() - started, 3)
        proxy_cleanup_verified = proxy.close() if proxy is not None else True
        report["proxy_cleanup_verified"] = proxy_cleanup_verified
        report["cleanup_verified"] = cleanup(resources, run) and proxy_cleanup_verified
        if DIRECT_CLIENT_SUMMARY:
            report["direct_client_observed"] = DIRECT_CLIENT_SUMMARY
        if NORMALIZATION_DIAGNOSTICS:
            report["normalization_diagnostics"] = NORMALIZATION_DIAGNOSTICS
        if COMMAND_FAILURES:
            report["command_failures"] = COMMAND_FAILURES[-5:]
        if not report["cleanup_verified"]:
            report["status"] = "failed"
            report["error_code"] = report.get("error_code", "cleanup_unverified")
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("status") == "passed" and report.get("cleanup_verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
