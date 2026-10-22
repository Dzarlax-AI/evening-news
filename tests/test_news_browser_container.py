"""Focused validation for the shared Cloak browser container boundary."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
BROWSER_DIR = ROOT / "deploy" / "news-extractor" / "browser"
sys.path.insert(0, str(BROWSER_DIR))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


policy = load("news_browser_policy", BROWSER_DIR / "policy.py")
entrypoint = load("news_browser_entrypoint", BROWSER_DIR / "entrypoint.py")


CONTROLLERS = {"news_app": "10.55.0.3", "worker": "10.55.0.5", "readeck": "10.55.0.7"}


def test_controller_config_is_named_finite_and_private():
    assert policy.parse_controllers(json.dumps(CONTROLLERS)) == CONTROLLERS
    for value in (
        "[]",
        json.dumps({"news_app": "10.55.0.3"}),
        json.dumps({**CONTROLLERS, "other": "8.8.8.8"}),
        json.dumps({**CONTROLLERS, "other": "10.55.0.3"}),
        "not-json",
    ):
        with pytest.raises(ValueError):
            policy.parse_controllers(value)


def test_browser_policy_is_default_drop_and_only_admits_exact_peers():
    rendered = policy.render_policy("10.55.0.2", CONTROLLERS, "10.55.0.4")
    assert 'comment "policy-sha256:' in rendered
    assert "type filter hook input priority -150; policy drop" in rendered
    assert "type filter hook output priority -150; policy drop" in rendered
    assert "type filter hook forward priority -150; policy drop" in rendered
    assert "ip daddr 10.55.0.4 tcp dport 8081" in rendered
    assert "ip saddr { 10.55.0.3, 10.55.0.5, 10.55.0.7 } tcp dport 9222" in rendered
    assert "ip daddr { 10.55.0.3, 10.55.0.5, 10.55.0.7 } tcp sport 9222" in rendered
    assert "meta skuid 65534" in rendered and "dport 5100" in rendered
    assert "0.0.0.0/0" not in rendered and "tcp dport { 80, 443 }" not in rendered
    with pytest.raises(ValueError):
        policy.render_policy("8.8.8.8", CONTROLLERS, "10.55.0.4")
    with pytest.raises(ValueError):
        policy.render_policy("10.55.0.2", CONTROLLERS, "127.0.0.1")
    with pytest.raises(ValueError, match="distinct"):
        policy.render_policy("10.55.0.2", CONTROLLERS, "10.55.0.3")


def test_policy_readback_checks_hooks_comment_and_ignores_counter_drift():
    value = {"nftables": [
        {"table": {"name": policy.TABLE, "comment": "policy-sha256:" + "a" * 64, "handle": 1}},
        *({"chain": {"hook": hook, "policy": "drop", "handle": index}} for index, hook in enumerate(
            ("input", "output", "forward"), 2)),
        {"rule": {"handle": 10, "counter": {"packets": 1, "bytes": 20}}},
    ]}
    text = 'table inet news_native_guard { comment "policy-sha256:' + "a" * 64 + '" }'
    fingerprint = policy.stable_fingerprint(value)
    changed_counters = json.loads(json.dumps(value))
    changed_counters["nftables"][-1]["rule"]["counter"] = {"packets": 99, "bytes": 900}
    changed_counters["nftables"][-1]["rule"]["handle"] = 55
    assert policy.readback_has_drop_hooks(value)
    assert policy.readback_comment_matches(value, text, "policy-sha256:" + "a" * 64)
    assert policy.stable_fingerprint(changed_counters) == fingerprint
    changed_counters["nftables"][2]["chain"]["policy"] = "accept"
    assert not policy.readback_has_drop_hooks(changed_counters)


def test_single_interface_check_rejects_extra_address_before_policy(monkeypatch):
    class Result:
        stdout = json.dumps([
            {"ifname": "lo", "addr_info": [{"family": "inet", "local": "127.0.0.1"}]},
            {"ifname": "eth0", "addr_info": [{"family": "inet", "local": "10.55.0.2"}]},
            {"ifname": "eth1", "addr_info": [{"family": "inet", "local": "10.55.0.3"}]},
        ])

    monkeypatch.setattr(entrypoint, "_run", lambda *args, **kwargs: Result())
    with pytest.raises(entrypoint.StartupError, match="single_expected_ipv4_interface_required"):
        entrypoint._check_single_interface("10.55.0.2")


def test_policy_install_uses_one_atomic_replace_and_requires_readback(monkeypatch):
    rendered = policy.render_policy("10.55.0.2", CONTROLLERS, "10.55.0.4")
    expected_comment = rendered.split('comment "', 1)[1].split('"', 1)[0]
    readback = {"nftables": [
        {"table": {"name": policy.TABLE, "comment": expected_comment}},
        *({"chain": {"hook": hook, "policy": "drop"}}
          for hook in ("input", "output", "forward")),
        {"rule": {"expr": [{"counter": {"packets": 0, "bytes": 0}}, {"accept": None}]}}
    ]}
    calls = []

    def fake_run(args, *, input_text=None, **kwargs):
        calls.append((args, input_text))
        if args == [entrypoint.NFT, "-j", "list", "tables"]:
            inventory = {"nftables": [{"table": {"family": "inet", "name": policy.TABLE}}]}
            return type("Result", (), {"stdout": json.dumps(inventory)})()
        if args[:2] == [entrypoint.NFT, "-f"]:
            return type("Result", (), {"stdout": ""})()
        if "-j" in args:
            return type("Result", (), {"stdout": json.dumps(readback)})()
        return type("Result", (), {"stdout": f'table inet {policy.TABLE} comment "{expected_comment}"'})()

    monkeypatch.setattr(entrypoint, "_run", fake_run)
    fingerprint, installed = entrypoint._install_policy(rendered)
    assert calls[1][1].startswith(f"delete table inet {policy.TABLE}\ntable inet {policy.TABLE} {{")
    assert fingerprint == policy.stable_fingerprint(readback)
    assert installed == readback


def test_supervisor_waits_for_delayed_exec_and_child_readiness(monkeypatch):
    class Child:
        def __init__(self, pid):
            self.pid = pid

        def poll(self):
            return None

    children = (Child(101), Child(202))
    monkeypatch.setattr(entrypoint.os, "geteuid", lambda: 0)
    monkeypatch.setattr(entrypoint, "_assert_runtime_capabilities", lambda: None)
    monkeypatch.setattr(entrypoint, "_runtime_env", lambda: ("10.55.0.2", CONTROLLERS,
                                                                "10.55.0.4", 800))
    monkeypatch.setattr(entrypoint, "_check_single_interface", lambda _ip: None)
    monkeypatch.setattr(entrypoint, "_policy_text", lambda *_args: "table comment \"policy-sha256:" + "a" * 64 + "\"")
    monkeypatch.setattr(entrypoint, "_expected_fingerprint", lambda: "b" * 64)
    monkeypatch.setattr(entrypoint, "_clear_stale_launch_info", lambda: None)
    monkeypatch.setattr(entrypoint, "_prepare_relay_heartbeat_dir", lambda: None)
    monkeypatch.setattr(entrypoint, "_start_children", lambda _controllers: children)
    monkeypatch.setattr(entrypoint, "_proc_starttime", lambda _pid: 999)
    monkeypatch.setattr(entrypoint, "_write_state", lambda _value: None)
    monkeypatch.setattr(entrypoint, "_marker_present_and_consume", lambda: False)
    monkeypatch.setattr(entrypoint, "_relay_heartbeat_fresh", lambda _pid: True)
    monkeypatch.setattr(entrypoint, "_terminate", lambda _process: None)
    monkeypatch.setattr(entrypoint.time, "monotonic", lambda: 0)
    ready_checks = iter((False, True))
    monkeypatch.setattr(entrypoint, "_children_ready", lambda *_pids: next(ready_checks))
    handlers = {}
    monkeypatch.setattr(entrypoint.signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler))
    sleeps = 0

    def sleep(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            handlers[entrypoint.signal.SIGTERM](entrypoint.signal.SIGTERM, None)

    monkeypatch.setattr(entrypoint.time, "sleep", sleep)
    assert entrypoint.supervise() == 0
    assert sleeps == 2


def test_policy_readback_failure_stops_only_verified_runtime_supervisor(monkeypatch):
    state = {"version": 1, "chrome_pid": 101, "relay_pid": 202,
             "supervisor_pid": 303, "supervisor_starttime": 404,
             "policy_fingerprint": "a" * 64}
    monkeypatch.setattr(entrypoint, "_runtime_env", lambda: ("10.55.0.2", CONTROLLERS,
                                                               "10.55.0.4", 800))
    monkeypatch.setattr(entrypoint, "_check_single_interface", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(entrypoint, "_policy_text", lambda *_args: "policy")
    monkeypatch.setattr(entrypoint, "_read_state", lambda: state)
    monkeypatch.setattr(entrypoint, "_expected_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(entrypoint, "_verify_policy",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(
                            entrypoint.StartupError("nft_policy_changed")))
    monkeypatch.setattr(entrypoint, "_verified_supervisor_pid", lambda _state: 303)
    signals = []
    monkeypatch.setattr(entrypoint.os, "kill", lambda pid, sig: signals.append((pid, sig)))

    with pytest.raises(entrypoint.StartupError, match="nft_policy_readback_failed"):
        entrypoint.healthcheck()
    assert signals == [(303, entrypoint.signal.SIGTERM)]


def test_healthcheck_never_signals_unverified_or_pid1_supervisor(monkeypatch):
    state = {"supervisor_pid": 1}
    monkeypatch.setattr(entrypoint, "_verified_supervisor_pid",
                        lambda _state: (_ for _ in ()).throw(
                            entrypoint.StartupError("supervisor_identity_invalid")))
    monkeypatch.setattr(entrypoint.os, "kill", lambda *_args: pytest.fail("unverified kill"))
    with pytest.raises(entrypoint.StartupError, match="supervisor_identity_invalid"):
        entrypoint._request_supervisor_restart(state)
    with pytest.raises(entrypoint.StartupError, match="supervisor_identity_invalid"):
        entrypoint._verified_supervisor_pid(state)


def test_stale_relay_heartbeat_terminates_children_for_container_restart(monkeypatch, tmp_path):
    class Child:
        def __init__(self, pid):
            self.pid = pid

        def poll(self):
            return None

    chrome, relay_child = Child(101), Child(202)
    stopped = []
    monkeypatch.setattr(entrypoint.os, "geteuid", lambda: 0)
    monkeypatch.setattr(entrypoint, "_assert_runtime_capabilities", lambda: None)
    monkeypatch.setattr(entrypoint, "_runtime_env", lambda: ("10.55.0.2", CONTROLLERS,
                                                               "10.55.0.4", 800))
    monkeypatch.setattr(entrypoint, "_check_single_interface", lambda _ip: None)
    monkeypatch.setattr(entrypoint, "_policy_text", lambda *_args: "table comment \"policy-sha256:" + "a" * 64 + "\"")
    monkeypatch.setattr(entrypoint, "_expected_fingerprint", lambda: "b" * 64)
    monkeypatch.setattr(entrypoint, "_clear_stale_launch_info", lambda: None)
    monkeypatch.setattr(entrypoint, "_prepare_relay_heartbeat_dir", lambda: None)
    monkeypatch.setattr(entrypoint, "_start_children", lambda _controllers: (chrome, relay_child))
    monkeypatch.setattr(entrypoint, "_proc_starttime", lambda _pid: 999)
    monkeypatch.setattr(entrypoint, "_write_state", lambda _value: None)
    monkeypatch.setattr(entrypoint, "_marker_present_and_consume", lambda: False)
    monkeypatch.setattr(entrypoint, "_terminate", lambda child: stopped.append(child.pid))
    monkeypatch.setattr(entrypoint, "_children_ready", lambda *_pids: True)
    monkeypatch.setattr(entrypoint, "_process_identity", lambda *_args: True)
    monkeypatch.setattr(entrypoint, "_child_commands_match", lambda *_args: True)
    monkeypatch.setattr(entrypoint, "_fd_count", lambda _pid: 1)
    monkeypatch.setattr(entrypoint, "_listener_ready", lambda _pid: True)
    monkeypatch.setattr(entrypoint, "_probe_cdp_as_relay_user", lambda: True)
    monkeypatch.setattr(entrypoint, "_relay_heartbeat_fresh", lambda _pid: False)
    monkeypatch.setattr(entrypoint, "STATE_FILE", tmp_path / "runtime.json")
    monkeypatch.setattr(entrypoint.time, "sleep", lambda _seconds: None)

    with pytest.raises(entrypoint.StartupError, match="relay_heartbeat_stale"):
        entrypoint.supervise()
    assert stopped == [202, 101]


def test_relay_event_loop_heartbeat_is_fresh_then_stales(tmp_path, monkeypatch):
    relay_module = load("news_shadow_cdp_relay_heartbeat", ROOT / "scripts" / "shadow_cdp_relay.py")
    heartbeat_dir = tmp_path / "relay"
    heartbeat_dir.mkdir(mode=0o700)
    heartbeat_dir.chmod(0o733)
    heartbeat_file = heartbeat_dir / "heartbeat.json"
    monkeypatch.setattr(entrypoint, "RELAY_HEARTBEAT_DIR", heartbeat_dir)
    monkeypatch.setattr(entrypoint, "RELAY_HEARTBEAT_FILE", heartbeat_file)
    monkeypatch.setattr(entrypoint, "RELAY_UID", os.geteuid())
    monkeypatch.setattr(entrypoint, "RELAY_HEARTBEAT_DIRECTORY_UID", os.geteuid())
    monkeypatch.setattr(relay_module, "HEARTBEAT_DIRECTORY_UID", os.geteuid())
    monkeypatch.setattr(relay_module, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
    instance = relay_module.Relay("10.55.0.3", heartbeat_path=heartbeat_file)

    async def scenario():
        task = asyncio.create_task(instance.heartbeat())
        try:
            await asyncio.sleep(0.03)
            assert entrypoint._relay_heartbeat_fresh(os.getpid())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    old = relay_module.time.monotonic_ns() - (entrypoint.RELAY_HEARTBEAT_MAX_AGE_SECONDS + 1) * 1_000_000_000
    relay_module.write_heartbeat(heartbeat_file, os.getpid(), monotonic_ns=old)
    assert not entrypoint._relay_heartbeat_fresh(os.getpid())


def test_terminate_cleans_process_group_when_leader_already_exited(monkeypatch):
    class Exited:
        pid = 303

        def poll(self):
            return 1

        def wait(self, **_kwargs):
            raise AssertionError("an exited leader must not be waited on")

    sent = []
    monkeypatch.setattr(entrypoint.os, "killpg", lambda pid, sig: sent.append((pid, sig)))
    entrypoint._terminate(Exited())
    assert sent == [(303, entrypoint.signal.SIGTERM), (303, entrypoint.signal.SIGKILL)]


def test_launch_info_cleanup_rejects_symlinked_directory(tmp_path, monkeypatch):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "info"
    link.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(entrypoint, "INFO_DIR", link)
    with pytest.raises(entrypoint.StartupError, match="launch_info_directory_invalid"):
        entrypoint._clear_stale_launch_info()


def test_runtime_state_writer_accepts_private_bootstrap_directory(tmp_path, monkeypatch):
    state_dir = tmp_path / "news-browser"
    state_file = state_dir / "runtime.json"
    monkeypatch.setattr(entrypoint, "STATE_DIR", state_dir)
    monkeypatch.setattr(entrypoint, "STATE_FILE", state_file)
    monkeypatch.setattr(entrypoint.os, "geteuid", lambda: 0)

    # The test host is often not root. Preserve the real mode and directory
    # checks while modeling the root-owned container tmpfs entry.
    real_stat = Path.stat

    def root_owned_stat(path, *, follow_symlinks=True):
        result = real_stat(path, follow_symlinks=follow_symlinks)
        if path == state_dir:
            values = list(result)
            values[4] = 0  # st_uid
            return os.stat_result(values)
        return result

    monkeypatch.setattr(Path, "stat", root_owned_stat)
    value = {"version": 1, "chrome_pid": 101, "relay_pid": 202,
             "supervisor_pid": 303, "supervisor_starttime": 404,
             "policy_fingerprint": "a" * 64}
    entrypoint._write_state(value)

    assert state_dir.stat().st_mode & 0o077 == 0
    assert json.loads(state_file.read_text()) == value


def test_runtime_keeps_pinned_display_path_and_disables_updates_and_widevine(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    environment = entrypoint._child_environment()
    assert environment["DISPLAY"] == "unix/:99"
    assert environment["PATH"] == "/shadow-bin:/usr/bin:/bin"
    assert environment["CLOAKBROWSER_AUTO_UPDATE"] == "false"
    assert environment["CLOAKBROWSER_FETCH_WIDEVINE"] == "false"
    assert entrypoint._drop_privilege_args(65534, 65534)[1:5] == [
        "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--securebits=+noroot"
    ]


def test_cdp_health_probe_uses_relay_uid_on_chrome_loopback(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(entrypoint, "_run", fake_run)
    assert entrypoint._probe_cdp_as_relay_user()
    args, kwargs = calls[0]
    assert args[:6] == [entrypoint.SET_PRIV, "--bounding-set=-all", "--inh-caps=-all",
                        "--ambient-caps=-all", "--securebits=+noroot", "--no-new-privs"]
    assert "--reuid=65534" in args and "--regid=65534" in args
    assert "http://127.0.0.1:5100/json/version" in args[-1]
