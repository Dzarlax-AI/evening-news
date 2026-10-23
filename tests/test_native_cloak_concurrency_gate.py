from __future__ import annotations

import importlib.util
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import threading
import time
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest


ROOT = Path(__file__).parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


gate = load_module("native_cloak_concurrency_gate", ROOT / "scripts/native_cloak_concurrency_gate.py")
client = load_module("native_cloak_concurrency_client", ROOT / "scripts/native_cloak_concurrency_client.py")
origin = load_module("native_cloak_concurrency_origin", ROOT / "tests/fixtures/native-cloak/concurrency_origin.py")


def test_memory_parser_handles_docker_units():
    assert gate.memory_bytes("12.5MiB / 768MiB") == int(12.5 * 1024 * 1024)
    assert gate.memory_bytes("1.25GB / 2GB") == 1_250_000_000
    assert gate.memory_bytes("invalid") is None


def test_candidate_binary_must_be_executable_regular_file_and_not_symlink(tmp_path):
    binary = tmp_path / "candidate"
    binary.write_bytes(b"binary")
    binary.chmod(0o700)
    assert gate.checked_binary(binary) == binary
    link = tmp_path / "candidate-link"
    link.symlink_to(binary)
    with pytest.raises(gate.GateError, match="candidate_binary_invalid"):
        gate.checked_binary(link)
    binary.chmod(0o600)
    with pytest.raises(gate.GateError, match="candidate_binary_invalid"):
        gate.checked_binary(binary)


def test_fixture_certificate_spki_hash_is_valid_base64_sha256(tmp_path):
    cert, _key = gate.DockerGate._make_certificate(tmp_path)
    value = gate._certificate_spki_sha256(cert)
    assert len(value) == 44
    assert value.endswith("=")


def test_receipt_contract_correlates_commands_and_preserves_finite_failure_code():
    command_id = "a" * 32
    assert gate.validate_receipt({"command_id": command_id, "ok": True, "result": {"limit": 2}},
                                 command_id) == {"limit": 2}
    with pytest.raises(gate.GateError, match="client_receipt_invalid"):
        gate.validate_receipt({"command_id": "b" * 32, "ok": True, "result": {}}, command_id)
    with pytest.raises(gate.GateError, match="news_browser_resource_leak"):
        gate.validate_receipt({"command_id": command_id, "ok": False,
                               "error": "news_browser_resource_leak"}, command_id)
    with pytest.raises(gate.GateError, match="client_gate_failed"):
        gate.validate_receipt({"command_id": command_id, "ok": False,
                               "error": "token=private"}, command_id)


def test_runtime_log_classifier_keeps_only_finite_safe_codes():
    logs = ("native_browser_diagnostic stage=readiness reason=protocol_error_-32603\n"
            "native_browser_diagnostic stage=unknown reason=https://host/?token=secret\n"
            "native browser capability probe failed for https://private/?token=secret\n"
            "native_browser_invalid_proxy_configuration token=private\n")
    observer = ("native_cloak_observer_failure type=RuntimeError reason=peer_session_probe_failed token=hidden\n"
                "native_cloak_observer_failure type=TimeoutError reason=cdp_timeout_browser_get_version\n")
    result = gate.classify_runtime_logs(logs, observer,
                                       "Chrome FATAL: SIGTRAP while handling private URL token=secret\n", 133)
    assert result == {
        "worker": [
            {"kind": "native_browser_diagnostic", "stage": "readiness", "reason": "protocol_error_-32603"},
            {"kind": "startup_failure", "reason": "native_browser_capability_probe_failed"},
            {"kind": "startup_failure", "reason": "native_proxy_configuration_invalid"},
        ],
        "observer": [
            {"exception_type": "RuntimeError", "reason": "peer_session_probe_failed"},
            {"exception_type": "TimeoutError", "reason": "cdp_timeout_browser_get_version"},
        ],
        "cloak": [
            {"kind": "cloak_runtime_diagnostic", "reason": "cloak_sigtrap"},
            {"kind": "cloak_runtime_diagnostic", "reason": "cloak_fatal"},
        ],
    }
    assert "secret" not in json.dumps(result)


def test_cdp_socket_timeout_preserves_type_and_maps_exact_method():
    class TimedOutSocket:
        def close(self):
            pass

    cdp = client.CDP.__new__(client.CDP)
    cdp.socket = TimedOutSocket()
    cdp.sequence = 0
    cdp._send = lambda _value: None
    cdp._receive = lambda: (_ for _ in ()).throw(TimeoutError("private endpoint token=secret"))
    with pytest.raises(TimeoutError, match="cdp_timeout_browser_get_version") as failure:
        cdp.command("Browser.getVersion")
    assert str(failure.value) == "cdp_timeout_browser_get_version"


def test_container_cleanup_refuses_a_foreign_name_without_stopping_or_removing_it():
    instance = gate.DockerGate.__new__(gate.DockerGate)
    instance.run_id = "our-run"
    instance.containers = ["same-name-foreign"]
    instance.networks = []
    calls = []

    def verify(name):
        raise gate.GateError("container_ownership_mismatch")

    def docker(*args, **kwargs):
        calls.append(args)
        if args[:2] == ("inspect", "same-name-foreign"):
            return "exists"
        raise AssertionError("foreign container must not be mutated")

    instance._verify_container = verify
    instance._docker = docker
    assert instance.cleanup() == ["container_ownership_mismatch"]
    assert calls == [("inspect", "same-name-foreign")]


def test_container_launch_never_pulls_and_registers_name_before_ambiguous_failure():
    instance = gate.DockerGate.__new__(gate.DockerGate)
    instance.prefix = "news-cloak-concurrency-test"
    instance.containers = []
    instance.label = "com.dzarlax.native-cloak-concurrency.run=test"
    commands = []

    def fail_ambiguous(*args, **kwargs):
        commands.append(args)
        raise gate.GateError("command_unavailable_or_timeout")

    instance._docker = fail_ambiguous
    with pytest.raises(gate.GateError):
        instance._run_container("cloak", "cloakhq/cloakbrowser:0.5.2", network="fixture",
                                command=["python", "launch.py"], memory="768m", tmp_size=256,
                                extra=("--shm-size", "1g", "--tmpfs", "/run:rw,noexec,nosuid,size=32m"))
    assert "news-cloak-concurrency-test-cloak" in instance.containers
    assert commands[0][:2] == ("run", "--pull=never")
    assert commands[0][commands[0].index("--dns") + 1] == "127.0.0.1"
    assert commands[0][commands[0].index("--memory") + 1] == "768m"
    assert "/tmp:rw,nosuid,nodev,noexec,size=256m" in commands[0]
    assert commands[0][commands[0].index("--shm-size") + 1] == "1g"
    assert "/run:rw,noexec,nosuid,size=32m" in commands[0]


def test_cgroup_oom_event_fails_gate_and_keeps_safe_resource_metrics():
    instance = gate.DockerGate.__new__(gate.DockerGate)
    instance.prefix = "news-cloak-concurrency-test"
    instance.phase = "profile_two"
    instance.cgroup_metrics = {}
    instance._docker = lambda *_args, **_kwargs: (
        "===memory.current\n1024\n===memory.peak\n2048\n===memory.stat\n"
        "anon 512\nfile 128\n===memory.events\noom 1\noom_kill 0\n===pids.current\n4")
    with pytest.raises(gate.GateError, match="container_oom_event_detected") as failure:
        instance._snapshot_cgroup("news-cloak-concurrency-test-worker")
    assert failure.value.phase == "profile_two"
    assert instance.cgroup_metrics["worker"]["memory_peak_bytes"] == 2048
    assert instance.cgroup_metrics["worker"]["oom_events"] == 1


def fixture_server(tmp_path):
    state = origin.FixtureState(tmp_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), origin.handler_for(state))
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return state, server, thread, f"http://127.0.0.1:{server.server_port}"


def wait_state(path: Path, predicate, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            snapshot = json.loads(path.read_text())
            if predicate(snapshot):
                return snapshot
        except (OSError, ValueError):
            pass
        time.sleep(0.01)
    raise AssertionError("fixture barrier did not reach expected state")


def test_fixture_routes_return_synthetic_article_and_typed_upstream_error(tmp_path):
    _state, server, thread, base = fixture_server(tmp_path)
    try:
        with pytest.raises(HTTPError) as error:
            urlopen(base + "/error/case", timeout=2)
        assert error.value.code == 503
        assert b"Synthetic upstream error" in error.value.read()
        with pytest.raises(HTTPError) as missing:
            urlopen(base + "/unexpected", timeout=2)
        assert missing.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_fixture_barrier_records_actual_max_overlap_and_releases_all_requests(tmp_path):
    _state, server, thread, base = fixture_server(tmp_path)
    replies = []
    failures = []

    def fetch(job):
        try:
            replies.append(urlopen(f"{base}/hold/barrier/{job}", timeout=5).read())
        except Exception as exc:  # collected for assertion in the test thread
            failures.append(type(exc).__name__)

    workers = [threading.Thread(target=fetch, args=(str(index),), daemon=True) for index in range(3)]
    try:
        for worker in workers:
            worker.start()
        state_path = tmp_path / "origin-state.json"
        snapshot = wait_state(state_path, lambda value: value["active"] == 3)
        assert snapshot["max_active"] == 3
        release = tmp_path / "release" / "barrier"
        release.parent.mkdir(mode=0o700)
        release.write_text("release\n")
        for worker in workers:
            worker.join(timeout=3)
        final = json.loads(state_path.read_text())
        assert not failures
        assert len(replies) == 3
        assert all(b'data-ready="true"' in body and len(body) > 200 for body in replies)
        assert final["active"] == 0
        assert final["max_active"] == 3
        assert final["completed"] == 3
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class FakeCDP:
    def __init__(self):
        self.contexts = {"peer-context"}
        self.targets = {"peer-target"}
        self.calls = []
        self.closed = False

    def command(self, method, params=None, session_id=None):
        self.calls.append((method, params or {}, session_id))
        if method == "Target.getTargets":
            return {"targetInfos": [{"targetId": value} for value in self.targets]}
        if method == "Target.getBrowserContexts":
            return {"browserContextIds": list(self.contexts)}
        if method == "Runtime.evaluate":
            return {"result": {"value": 2}}
        if method == "Target.disposeBrowserContext":
            self.contexts.discard(params["browserContextId"])
            return {}
        raise AssertionError(f"unexpected CDP command {method}")

    def close(self):
        self.closed = True


def test_foreign_cdp_session_survives_news_resource_check_and_closes_only_owned_context():
    peer = client.Client.__new__(client.Client)
    peer.cdp = FakeCDP()
    peer.target, peer.context, peer.session = "peer-target", "peer-context", "peer-session"
    peer.targets, peer.contexts = {"peer-target"}, {"peer-context"}
    peer.assert_clean_news_resources()
    assert peer.cdp.closed is False
    assert peer.cdp.contexts == {"peer-context"}
    assert any(call[0] == "Runtime.evaluate" and call[2] == "peer-session" for call in peer.cdp.calls)
    peer.close()
    assert peer.cdp.contexts == set()
    assert peer.cdp.closed is True
    assert [call[1] for call in peer.cdp.calls if call[0] == "Target.disposeBrowserContext"] == [
        {"browserContextId": "peer-context"}]


def test_foreign_cdp_session_reports_target_loss_without_mutating_any_context():
    peer = client.Client.__new__(client.Client)
    peer.cdp = FakeCDP()
    peer.target, peer.context, peer.session = "peer-target", "peer-context", "peer-session"
    peer.targets, peer.contexts = {"peer-target"}, {"peer-context"}
    peer.cdp.targets.clear()
    with pytest.raises(RuntimeError, match="peer_target_or_context_lost"):
        peer.assert_clean_news_resources()
    assert not any(call[0] == "Target.disposeBrowserContext" for call in peer.cdp.calls)


class FakeBootstrapCDP:
    def __init__(self, _endpoint):
        self.calls = []
        self.closed = False

    def command(self, method, params=None, session_id=None):
        self.calls.append((method, params or {}, session_id))
        if method == "Browser.getVersion":
            return {"product": "Chrome/fixture"}
        if method == "Target.createBrowserContext":
            return {"browserContextId": "peer-context"}
        if method == "Target.createTarget":
            return {"targetId": "peer-target"}
        if method == "Target.attachToTarget":
            return {"sessionId": "peer-session"}
        if method == "Target.getTargets":
            return {"targetInfos": [{"targetId": "peer-target"}]}
        if method == "Target.getBrowserContexts":
            return {"browserContextIds": ["peer-context"]}
        if method == "Runtime.evaluate":
            return {"result": {"value": 2}}
        if method == "Target.disposeBrowserContext":
            return {}
        raise AssertionError(f"unexpected CDP command {method}")

    def close(self):
        self.closed = True


def test_foreign_peer_uses_native_browser_context_proxy_and_dispose_contract(monkeypatch):
    cdp_instances = []

    def cdp_factory(endpoint):
        value = FakeBootstrapCDP(endpoint)
        cdp_instances.append(value)
        return value

    monkeypatch.setattr(client, "CDP", cdp_factory)
    monkeypatch.setattr(client.urllib.request, "urlopen", lambda *_args, **_kwargs: FakeResponse(
        json.dumps({"webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/browser/fixture"}).encode()))
    peer = client.Client(Path("/state"), "worker:8080", "127.0.0.1:9222", "token",
                         "http://gateway:8081")
    calls = cdp_instances[0].calls
    context = next(params for method, params, _session in calls
                   if method == "Target.createBrowserContext")
    assert context == {"disposeOnDetach": True, "proxyServer": "http://gateway:8081",
                       "proxyBypassList": "<-loopback>"}
    assert calls[0][0] == "Browser.getVersion"
    peer.close()


class FakeResponse:
    def __init__(self, body: bytes):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit=-1):
        return self.body


def test_worker_response_contract_accepts_valid_result_and_rejects_malformed_json(monkeypatch):
    instance = client.Client.__new__(client.Client)
    instance.worker, instance.token = "http://worker:8080", "test-token"

    def valid_response(request, **_kwargs):
        request_id = json.loads(request.data)["request_id"]
        body = json.dumps({"contract_version": "1", "request_id": request_id,
                           "outcome": "success", "result": {"content": "synthetic article"}}).encode()
        return FakeResponse(body)

    monkeypatch.setattr(client.urllib.request, "urlopen", valid_response)
    result = instance.request("job", "case", 1000)
    assert result["outcome"] == "success"
    assert result["content_chars"] == len("synthetic article")

    monkeypatch.setattr(client.urllib.request, "urlopen",
                        lambda *_args, **_kwargs: FakeResponse(b"not-json"))
    with pytest.raises(RuntimeError, match="worker_response_invalid"):
        instance.request("job", "case", 1000)
