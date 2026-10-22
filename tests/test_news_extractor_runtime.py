from __future__ import annotations

import copy
import configparser
import importlib.util
from pathlib import Path
import sys

import pytest


RUNTIME_DIR = Path(__file__).resolve().parents[1] / "deploy" / "news-extractor" / "runtime"
sys.path.insert(0, str(RUNTIME_DIR))
spec = importlib.util.spec_from_file_location("news_extractor_supervisor", RUNTIME_DIR / "supervisor.py")
runtime = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(runtime)

policy_spec = importlib.util.spec_from_file_location("news_extractor_runtime_policy", RUNTIME_DIR / "policy.py")
policy_module = importlib.util.module_from_spec(policy_spec)
assert policy_spec.loader is not None
policy_spec.loader.exec_module(policy_module)


def runtime_config(mode="isolated-test"):
    image = "ghcr.io/example/image@sha256:" + "a" * 64
    clients = {
        "news_app": {"label": "com.example.app", "value": "fixture-news", "image": image,
                     "control_ip": "10.44.0.10", "cdp_proxy_accepted": False},
        "worker": {"label": "com.example.worker", "value": "runtime-worker", "image": image,
                   "control_ip": "10.44.0.11", "cdp_proxy_accepted": False},
    }
    return {
        "schema_version": 1,
        "mode": mode,
        "owner_value": "news-test-owner1",
        "networks": {
            "control": {"name": "news-test-control", "subnet": "10.44.0.0/24", "gateway": "10.44.0.1"},
            "egress": {"name": "news-test-egress", "subnet": "10.45.0.0/24", "gateway": "10.45.0.1"},
        },
        "addresses": {"browser": "10.44.0.20", "gateway": "10.44.0.21",
                      "worker_egress": "10.45.0.20", "gateway_egress": "10.45.0.21"},
        "clients": clients,
        "disabled_clients": ([{"name": "personal_assistant", "id": "c" * 64,
                               "label": "com.example.assistant", "value": "fixture-assistant", "image": image}]
                             if mode == "production" else []),
        "images": {"anchor": image, "extractor": image, "cloak": image},
        "containers": {"legacy_browser_id": None if mode == "isolated-test" else "b" * 64,
                       "browser_anchor": "news-test-browser-anchor", "worker_anchor": "news-test-worker-anchor",
                       "gateway_anchor": "news-test-gateway-anchor", "cloak": "news-test-cloak",
                       "relay": "news-test-relay", "worker": "news-test-service", "gateway": "news-test-egress"},
        "resolver": "1.1.1.1",
        "paths": {"runtime_dir": "/opt/news-test", "cloak_launch": "/opt/news-test/launch.py",
                  "relay_script": "/opt/news-test/relay.py", "browser_info_dir": "/var/lib/news-test/info",
                  "gateway_env_file": "/etc/news-test/gateway.env", "worker_env_file": "/etc/news-test/worker.env",
                  "browser_control_volume": "news-test-control", "resolv_conf": "/etc/news-test/resolv.conf"},
    }


def test_isolated_fixture_validates_and_plan_does_not_read_secret_values():
    config = runtime.validate_config(runtime_config())
    plan = runtime.render_plan(config)
    assert plan["mode"] == "isolated-test"
    assert plan["required_clients"]["news_app"]["expected_ip"] == "10.44.0.10"
    assert plan["secrets"] == {"gateway_env_file": "/etc/news-test/gateway.env",
                               "worker_env_file": "/etc/news-test/worker.env"}


def test_local_image_ids_are_test_only_and_match_resolved_docker_image_identity():
    local_id = "sha256:" + "f" * 64
    config = runtime_config()
    config["images"] = {name: local_id for name in config["images"]}
    for client in config["clients"].values():
        client["image"] = local_id
    assert runtime.validate_config(config)["images"]["anchor"] == local_id

    item = {"Image": local_id, "Config": {"Image": "loaded-candidate:local"}}
    assert runtime._container_image_matches(item, local_id, allow_local_id=True)
    item["Image"] = "sha256:" + "e" * 64
    assert not runtime._container_image_matches(item, local_id, allow_local_id=True)

    production = runtime_config("production")
    production["images"]["anchor"] = local_id
    with pytest.raises(runtime.RuntimeErrorSafe, match="invalid_immutable_image_anchor"):
        runtime.validate_config(production)


def test_registry_image_identity_still_uses_requested_immutable_reference():
    expected = "ghcr.io/example/image@sha256:" + "a" * 64
    item = {"Image": "sha256:" + "b" * 64, "Config": {"Image": expected}}
    assert runtime._container_image_matches(item, expected, allow_local_id=False)
    item["Config"]["Image"] = "ghcr.io/example/image:latest"
    assert not runtime._container_image_matches(item, expected, allow_local_id=False)


def test_production_requires_per_client_proxy_compatibility_and_legacy_id():
    config = runtime_config("production")
    with pytest.raises(runtime.RuntimeErrorSafe, match="client_proxy_compatibility_not_accepted_news_app"):
        runtime.validate_config(config, require_production=True)
    config["clients"]["news_app"]["cdp_proxy_accepted"] = True
    config["clients"]["worker"]["cdp_proxy_accepted"] = True
    config["disabled_clients"] = []
    with pytest.raises(runtime.RuntimeErrorSafe, match="disabled_personal_assistant_inventory_required"):
        runtime.validate_config(config, require_production=True)
    config["disabled_clients"] = [{"name": "personal_assistant", "id": "c" * 64,
                                   "label": "com.example.assistant", "value": "fixture-assistant",
                                   "image": "sha256:" + "d" * 64}]
    assert runtime.validate_config(config, require_production=True)
    assert runtime.validate_config(config, require_production=True)


def test_client_allowlist_is_finite_and_addresses_are_unique():
    config = runtime_config()
    config["clients"]["readeck"] = copy.deepcopy(config["clients"]["news_app"])
    with pytest.raises(runtime.RuntimeErrorSafe, match="duplicate_control_address"):
        runtime.validate_config(config)
    config = runtime_config()
    for index in range(7):
        item = copy.deepcopy(config["clients"]["news_app"])
        item["control_ip"] = f"10.44.0.{30 + index}"
        config["clients"][f"client_{index}"] = item
    with pytest.raises(runtime.RuntimeErrorSafe, match="exact_client_identities_required"):
        runtime.validate_config(config)


def test_policy_has_default_drop_and_exact_controller_rules_with_worker_response():
    config = runtime.validate_config(runtime_config())
    addresses = {"clients": {name: item["control_ip"] for name, item in config["clients"].items()},
                 "browser": config["addresses"]["browser"], "gateway": config["addresses"]["gateway"]}
    worker_rules = policy_module.policy("worker", addresses, config["resolver"], ["203.0.113.8"])
    assert "policy drop" in worker_rules
    assert "meta nfproto ipv6 counter drop" in worker_rules
    assert "ip daddr 10.44.0.10 tcp sport 8080 ct state established counter accept" in worker_rules
    assert "tcp dport { 80, 443 } ct state { new, established } counter accept" in worker_rules
    browser_rules = policy_module.policy("browser", addresses, config["resolver"], [])
    assert "ip saddr { 10.44.0.10, 10.44.0.11 } tcp dport 9222" in browser_rules
    assert "tcp dport 443" not in browser_rules


def test_policy_readback_fingerprint_ignores_counters_but_not_rule_changes():
    value = {"nftables": [{"rule": {"handle": 8, "expr": [{"accept": None}],
                                    "counter": {"packets": 5, "bytes": 10}}}]}
    same = copy.deepcopy(value)
    same["nftables"][0]["rule"]["handle"] = 99
    same["nftables"][0]["rule"]["counter"] = {"packets": 200, "bytes": 4096}
    changed = copy.deepcopy(same)
    changed["nftables"][0]["rule"]["expr"] = [{"drop": None}]
    assert runtime.Supervisor._policy_fingerprint(value) == runtime.Supervisor._policy_fingerprint(same)
    assert runtime.Supervisor._policy_fingerprint(value) != runtime.Supervisor._policy_fingerprint(changed)


def test_generated_policy_contains_sha256_policy_comment():
    config = runtime.validate_config(runtime_config())
    addresses = {"clients": {name: item["control_ip"] for name, item in config["clients"].items()},
                 "browser": config["addresses"]["browser"], "gateway": config["addresses"]["gateway"]}
    payload = policy_module.policy("gateway", addresses, config["resolver"], ["203.0.113.8"])
    assert __import__("re").search(r'comment "policy-sha256:[0-9a-f]{64}"', payload)
    assert len(payload) > 300


def test_install_roles_applies_all_policies_before_starting_any_child():
    supervisor = runtime.Supervisor.__new__(runtime.Supervisor)
    supervisor.config = runtime.validate_config(runtime_config())
    events = []
    supervisor.preflight = lambda: events.append("preflight")
    supervisor._ensure_network = lambda role, *, internal: events.append("network:" + role)
    supervisor._create_anchor = lambda role: events.append("anchor:" + role) or role + "-anchor"
    supervisor._attach_egress = lambda role, anchor: events.append("egress:" + role)
    supervisor._set_default_route = lambda role, anchor: events.append("route:" + role)
    supervisor._install_policy = lambda role, anchor: events.append("policy:" + role)
    supervisor._ensure_control_volume = lambda: events.append("volume")
    supervisor._launch_services = lambda anchors: events.append("children")
    supervisor._probe_roles = lambda: events.append("probes")
    supervisor.install_roles()
    assert events.index("policy:browser") < events.index("children")
    assert events.index("policy:gateway") < events.index("children")
    assert events.index("policy:worker") < events.index("children")
    assert events[-1] == "probes"


def test_partial_start_failure_always_runs_owned_cleanup():
    supervisor = runtime.Supervisor.__new__(runtime.Supervisor)
    events = []
    supervisor.stop_requested = False
    supervisor.install_roles = lambda: (_ for _ in ()).throw(runtime.RuntimeErrorSafe("fixture_start_failed"))
    supervisor.stop_owned = lambda: events.append("cleanup")
    with pytest.raises(runtime.RuntimeErrorSafe, match="fixture_start_failed"):
        supervisor.run()
    assert events == ["cleanup"]


def test_runtime_restarts_only_failed_http_role_but_rebuilds_browser_generation():
    supervisor = runtime.Supervisor.__new__(runtime.Supervisor)
    config = runtime.validate_config(runtime_config())
    supervisor.config = config
    names = config["containers"]
    supervisor._anchor_pid = lambda name, role: "123"
    supervisor._verify_default_route = lambda role, pid: None
    supervisor._verify_installed_policy = lambda role, pid: None
    running = {"cloak": True, "relay": True, "worker": True, "gateway": False}
    supervisor._exact_container = lambda name, role, allow_missing=False: {
        "State": {"Running": running[role]}}
    events = []
    supervisor._launch_services = lambda anchors, *, only=None: events.append(("start", only))
    supervisor._restart_browser_generation = lambda: events.append(("browser-generation", None))
    supervisor._verify_child = lambda *args: None
    supervisor._probe_roles = lambda: events.append(("probe", None))
    supervisor._verify_runtime_roles()
    assert events == [("start", {"gateway"}), ("probe", None)]
    running["relay"] = False
    events.clear()
    supervisor._verify_runtime_roles()
    assert events == [("browser-generation", None), ("probe", None)]


def test_disabled_assistant_must_remain_stopped_with_restart_disabled():
    supervisor = runtime.Supervisor.__new__(runtime.Supervisor)
    expected = runtime_config("production")["disabled_clients"][0]
    expected["image"] = "sha256:" + "d" * 64

    class FakeDocker:
        running = False
        restart = "no"
        image = expected["image"]
        container_id = expected["id"]
        label_value = expected["value"]

        def inspect(self, _target):
            return {"Id": self.container_id, "Config": {"Image": "legacy-image-name",
                                                     "Labels": {expected["label"]: self.label_value}},
                    "Image": self.image,
                    "State": {"Running": self.running},
                    "HostConfig": {"RestartPolicy": {"Name": self.restart}}}

    supervisor.docker = FakeDocker()
    supervisor._disabled_client_is_stopped(expected)
    supervisor.docker.container_id = "f" * 64
    with pytest.raises(runtime.RuntimeErrorSafe,
                       match="disabled_client_not_stopped_restart_disabled_personal_assistant"):
        supervisor._disabled_client_is_stopped(expected)
    supervisor.docker.container_id = expected["id"]
    supervisor.docker.label_value = "unexpected"
    with pytest.raises(runtime.RuntimeErrorSafe,
                       match="disabled_client_not_stopped_restart_disabled_personal_assistant"):
        supervisor._disabled_client_is_stopped(expected)
    supervisor.docker.label_value = expected["value"]
    supervisor.docker.image = "sha256:" + "e" * 64
    with pytest.raises(runtime.RuntimeErrorSafe,
                       match="disabled_client_not_stopped_restart_disabled_personal_assistant"):
        supervisor._disabled_client_is_stopped(expected)
    supervisor.docker.image = expected["image"]
    supervisor.docker.running = True
    with pytest.raises(runtime.RuntimeErrorSafe, match="disabled_client_not_stopped_restart_disabled_personal_assistant"):
        supervisor._disabled_client_is_stopped(expected)


def test_test_commands_cannot_target_production_config(monkeypatch, capsys):
    monkeypatch.setattr(runtime, "read_config", lambda *_args, **_kwargs: runtime_config("production"))
    assert runtime.main(["stop-owned-test", "--config", "/unused"]) == 2
    assert capsys.readouterr().err.strip() == "isolated_test_mode_required"


def test_systemd_unit_is_pulled_in_and_stopped_with_docker_restart():
    unit = configparser.ConfigParser(interpolation=None, strict=False)
    unit.read(RUNTIME_DIR / "news-extractor-runtime.service")
    assert "docker.service" in unit["Unit"]["After"]
    assert "docker.service" in unit["Unit"]["BindsTo"]
    assert "docker.service" in unit["Unit"]["PartOf"]
    assert {"multi-user.target", "docker.service"}.issubset(
        set(unit["Install"]["WantedBy"].split())
    )
    assert unit["Service"]["Restart"] == "on-failure"
    assert "stop-owned" in unit["Service"]["ExecStopPost"]


def test_listener_readback_matches_bind_port_state_and_uid():
    def row(host, port, uid):
        local = __import__("ipaddress").IPv4Address(host).packed[::-1].hex().upper()
        return f"0: {local}:{port:04X} 00000000:0000 0A 00:00 00:00 00000000 {uid} 0 123"

    assert runtime.Supervisor._listener_row_matches(row("127.0.0.1", 5100, 0), 5100, 0, "127.0.0.1")
    assert runtime.Supervisor._listener_row_matches(row("0.0.0.0", 9222, 65534), 9222, 65534, "0.0.0.0")
    assert not runtime.Supervisor._listener_row_matches(row("0.0.0.0", 9222, 0), 9222, 65534, "0.0.0.0")
    assert not runtime.Supervisor._listener_row_matches(row("127.0.0.1", 9222, 65534), 9222, 65534, "0.0.0.0")


def test_admitted_role_probe_retries_until_bounded_cold_start_is_ready(monkeypatch):
    supervisor = runtime.Supervisor.__new__(runtime.Supervisor)
    supervisor.config = runtime.validate_config(runtime_config())
    supervisor.docker = type("FakeDocker", (), {})()
    calls = []

    def call(*args, **kwargs):
        calls.append((args, kwargs))
        return "503\n200\n200" if len(calls) == 1 else "200\n200\n200"

    supervisor.docker.call = call
    supervisor._client = lambda name: {"Id": "app-id"}
    supervisor._check_process_uids = lambda *_args: None
    monkeypatch.setattr(runtime.time, "sleep", lambda _seconds: None)
    supervisor._probe_roles()
    assert len(calls) == 2
    assert calls[0][0][:3] == ("exec", "app-id", "python")
    assert calls[0][1]["timeout"] == 10


def test_admitted_role_probe_fails_closed_after_readiness_deadline(monkeypatch):
    supervisor = runtime.Supervisor.__new__(runtime.Supervisor)
    supervisor.config = runtime.validate_config(runtime_config())
    supervisor.docker = type("FakeDocker", (), {"call": lambda *_args, **_kwargs: "503\n503\n503"})()
    supervisor._client = lambda _name: {"Id": "app-id"}
    times = iter([0.0, 91.0])
    monkeypatch.setattr(runtime.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(runtime.time, "sleep", lambda _seconds: None)
    with pytest.raises(runtime.RuntimeErrorSafe, match="admitted_role_readiness_timeout"):
        supervisor._probe_roles()
