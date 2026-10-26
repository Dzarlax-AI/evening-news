"""Behavioral safety checks for the repeatable Go web Docker acceptance gate."""

import importlib.util
from email.message import Message
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/run_go_web_fixture_gate.py"


def load_gate_module():
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location("run_go_web_fixture_gate", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_docker_options_never_publish_ports_and_reject_unsafe_volume_names():
    gate = load_gate_module()
    web = gate.docker_create_options("gate-web", "gate-web", "news-gate-backups")
    assert web == []
    assert gate.docker_create_options("gate-worker", "gate-web", "news-gate-backups") == []
    assert gate.docker_create_options("gate-executor", "gate-web", "news-gate-backups") == [
        "--mount", "type=volume,src=news-gate-backups,dst=/app/backups"
    ]
    with pytest.raises(gate.GateFailure, match="unsafe_fixture_volume_name"):
        gate.docker_create_options("gate-web", "gate-web", "../../host-path")


def test_docker_create_keeps_web_unpublished(monkeypatch, tmp_path):
    gate_module = load_gate_module()
    gate = gate_module.GoWebGate(tmp_path)
    captured = []

    def docker_parent(_self, *args, **_kwargs):
        captured.extend(args)
        return subprocess.CompletedProcess(args, 0, stdout="container-id\n", stderr="")

    monkeypatch.setattr(gate_module.IngestionGate, "docker", docker_parent)
    image = gate_module.IMAGES["web"]
    gate.docker("create", "--name", gate.prefix + "-web", "--network", gate.network, image)
    assert captured.count("--publish") == 0


def test_go_web_environment_excludes_provider_credentials():
    gate = load_gate_module()
    instance = gate.GoWebGate(Path("/tmp/go-web-env-test"))
    names = set(instance.app_environment("web"))
    forbidden = {
        "GEMINI_API_KEY", "GEMINI_API_ENDPOINT", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID",
        "TELEGRAM_SERVICE_CHAT_ID", "TELEGRAPH_ACCESS_TOKEN", "ARTICLE_AI_TOKEN",
        "EXTRACTION_SERVICE_TOKEN", "ARTICLE_INGESTION_TOKEN", "ARTICLE_PROCESSING_TOKEN",
        "JOB_EXECUTOR_TOKEN", "BROWSER_WS_ENDPOINT",
    }
    assert names.isdisjoint(forbidden)
    assert {"DATABASE_URL", "ADMIN_PASSWORD", "JWT_SECRET", "WEB_BACKEND_TOKEN",
            "SCHEDULED_JOBS_TOKEN"}.issubset(names)


def test_runtime_env_check_and_provider_counter_response(monkeypatch, tmp_path):
    module = load_gate_module()
    instance = module.GoWebGate(tmp_path)
    def fake_docker(*args, **kwargs):
        output = ('["PATH=/usr/bin", "WEB_BACKEND_TOKEN=fixture-only"]'
                  if args[0] == "inspect" else '{"providers":{"gemini":3}}')
        return subprocess.CompletedProcess(args, 0, stdout=output, stderr="")
    monkeypatch.setattr(instance, "docker", fake_docker)
    instance.assert_go_web_environment_safe()
    assert instance.report["checks"]["go_web_provider_secrets_absent"]
    assert instance.fake_counters_http() == {"providers": {"gemini": 3}}


def test_cleanup_does_not_remove_resources_with_mismatched_owner_labels(monkeypatch, tmp_path):
    gate_module = load_gate_module()
    gate = gate_module.GoWebGate(tmp_path)
    gate.containers = [gate.prefix + "-web"]
    gate.network = gate.prefix + "-net"
    gate.backup_volume = gate.prefix + "-backups"
    gate.report["cleanup_errors"] = []
    calls = []

    def fake_docker(*args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="another-run\n", stderr="")

    monkeypatch.setattr(gate, "docker", fake_docker)
    gate.cleanup()
    assert not any(call and (call[0] == "rm" or call[:2] == ("network", "rm"))
                   for call in calls)
    assert not gate.report["cleanup_errors"]


def test_missing_image_fails_before_network_creation(monkeypatch, tmp_path):
    gate_module = load_gate_module()
    gate = gate_module.GoWebGate(tmp_path)
    calls = []

    def fake_docker(*args, check=True, **kwargs):
        calls.append(args)
        if args[:2] == ("image", "inspect"):
            raise gate_module.GateFailure("fixture_image_missing")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(gate, "docker", fake_docker)
    with pytest.raises(gate_module.GateFailure, match="fixture_image_missing"):
        gate.run()
    assert not any(call[:2] == ("network", "create") for call in calls)


def test_loopback_port_failure_diagnostics_capture_only_port_configuration(monkeypatch, tmp_path):
    gate_module = load_gate_module()
    gate = gate_module.GoWebGate(tmp_path)
    values = iter((
        '{"8000/tcp":[{"HostIp":"127.0.0.1","HostPort":""}]}',
        '{"8000/tcp":null}',
        '{"8000/tcp":{}}',
    ))

    def fake_docker(*args, **kwargs):
        return subprocess.CompletedProcess(args, 0, stdout=next(values), stderr="")

    monkeypatch.setattr(gate, "docker", fake_docker)
    gate.capture_web_port_diagnostics()
    diag = gate.report["web_port_diagnostics"]
    assert diag["publish_requested_loopback"] is True
    assert diag["host_config_port_bindings"]["8000/tcp"][0]["HostIp"] == "127.0.0.1"
    assert diag["network_settings_ports"]["8000/tcp"] is None
    assert diag["configured_exposed_ports"] == {"8000/tcp": {}}


def test_successful_html_and_json_http_responses_decode_by_content_type(monkeypatch):
    gate_module = load_gate_module()
    instance = gate_module.GoWebGate(Path("/tmp/go-web-request-test"))
    instance.host_port = 43210

    class Response:
        status = 200

        def __init__(self, content_type, body):
            self.headers = Message()
            self.headers["Content-Type"] = content_type
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _limit):
            return self.body

    replies = iter((Response("text/html; charset=utf-8", b"<html>fixture</html>"),
                    Response("application/json", b'{"ok":true}')))
    monkeypatch.setattr(gate_module.urllib.request, "urlopen", lambda *_args, **_kwargs: next(replies))
    assert instance.request("GET", "/")[1] == "<html>fixture</html>"
    assert instance.request("GET", "/api/status")[1] == {"ok": True}


def test_qa_hold_duration_is_bounded():
    gate = load_gate_module()
    assert gate.bounded_qa_hold("0") == 0
    assert gate.bounded_qa_hold("300") == 300
    for unsafe in ("-1", "301", "no"):
        with pytest.raises(gate.argparse.ArgumentTypeError):
            gate.bounded_qa_hold(unsafe)


def test_relay_refuses_container_without_current_gate_owner_label(monkeypatch, tmp_path):
    gate_module = load_gate_module()
    gate = gate_module.GoWebGate(tmp_path)

    def fake_docker(*_args, **_kwargs):
        return subprocess.CompletedProcess([], 0, stdout="different-owner\n", stderr="")

    monkeypatch.setattr(gate, "docker", fake_docker)
    with pytest.raises(gate_module.GateFailure, match="relay_container_owner_mismatch"):
        gate_module.LoopbackDockerExecRelay(gate, gate.prefix + "-web")


def test_relay_only_execs_fixed_loopback_target_and_keeps_secrets_out_of_argv(monkeypatch, tmp_path):
    gate_module = load_gate_module()
    gate = gate_module.GoWebGate(tmp_path)
    relay = gate_module.LoopbackDockerExecRelay.__new__(gate_module.LoopbackDockerExecRelay)
    relay.container = gate.prefix + "-web"
    relay.children = set()
    relay.children_lock = gate_module.threading.Lock()
    relay.stopping = False
    captured = {}

    class FakeProcess:
        pass

    def fake_popen(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setattr(gate_module.subprocess, "Popen", fake_popen)
    relay.exec_nc()
    assert captured["argv"] == ["docker", "exec", "-i", gate.prefix + "-web",
                                 "busybox", "nc", "127.0.0.1", "8000"]
    assert captured["kwargs"]["stderr"] is gate_module.subprocess.DEVNULL
    assert "env" not in captured["kwargs"]


def test_relay_terminates_and_reaps_exec_child_on_client_disconnect(tmp_path):
    gate_module = load_gate_module()
    stopped = []

    class Pipe:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

        def write(self, _data):
            pass

        def flush(self):
            pass

        def read(self, _size):
            return b""

    class Child:
        def __init__(self):
            self.stdin, self.stdout = Pipe(), Pipe()
            self.returncode = None

        def poll(self):
            return self.returncode

        def terminate(self):
            stopped.append("terminate")
            self.returncode = 0

        def wait(self, timeout=None):
            stopped.append("wait")
            return self.returncode

    class Owner:
        def __init__(self):
            self.child = Child()

        def exec_nc(self):
            return self.child

        def reap_child(self, _child):
            stopped.append("reaped")

    class Client:
        def settimeout(self, _timeout):
            pass

        def recv(self, _size):
            return b""

        def sendall(self, _data):
            pass

    server = type("Server", (), {})()
    server.owner = Owner()
    server.slots = gate_module.threading.BoundedSemaphore(1)
    handler = gate_module._RelayHandler.__new__(gate_module._RelayHandler)
    handler.server, handler.request = server, Client()
    handler.handle()
    assert stopped == ["terminate", "wait", "reaped"]
    assert server.owner.child.stdin.closed and server.owner.child.stdout.closed
