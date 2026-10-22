#!/usr/bin/env python3
"""Persistent root supervisor for the single shared native Cloak runtime.

This program is inert unless explicitly run with a root-owned production config.
It never pulls images and never mutates the existing app, database, Redis, or
Traefik containers. Production requires a prior canonical Compose handoff that
stops the legacy browser with restart policy `no` and attaches known clients to
the configured private control network.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
from typing import Any

try:
    from .policy import TABLE, policy
except ImportError:  # direct invocation from an installed runtime directory
    from policy import TABLE, policy

OWNER_LABEL = "com.dzarlax.news-extractor-runtime"
SCHEMA_VERSION = 1
ROLES = ("browser", "gateway", "worker")
REQUIRED_CLIENTS = {"news_app", "worker"}
MAX_CLIENTS = 8
SECRET_ENV_KEYS = {"EGRESS_TOKEN", "EXTRACTOR_SERVICE_TOKEN", "EXTRACTOR_NATIVE_PROXY_TOKEN"}
ANCHOR_SLEEP = "import time; time.sleep(10**9)"
PROBE = (
    "import sys,urllib.request; "
    "[(lambda r: print(r.status))(urllib.request.urlopen(u,timeout=2)) for u in sys.argv[1:]]"
)


class RuntimeErrorSafe(RuntimeError):
    """Finite safe error code; never includes process output or secret contents."""


def _fail(code: str) -> None:
    raise RuntimeErrorSafe(code)


def _require_digest(value: Any, name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", value):
        _fail("invalid_immutable_image_" + name)
    return value


def _require_image_reference(value: Any, name: str, *, allow_local_id: bool) -> str:
    if allow_local_id and isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        return value
    return _require_digest(value, name)


def _container_image_matches(item: dict[str, Any], expected: str, *, allow_local_id: bool) -> bool:
    """Match registry refs as requested; local IDs by Docker's resolved image ID."""
    if re.fullmatch(r"sha256:[0-9a-f]{64}", expected):
        return allow_local_id and item.get("Image") == expected
    return item.get("Config", {}).get("Image") == expected


def _private_ip(value: Any, name: str) -> ipaddress.IPv4Address:
    try:
        address = ipaddress.IPv4Address(value)
    except (ipaddress.AddressValueError, TypeError):
        _fail("invalid_ipv4_" + name)
    if not address.is_private or address.is_loopback or address.is_link_local or address.is_multicast:
        _fail("non_private_ipv4_" + name)
    return address


def _network(value: Any, name: str) -> tuple[ipaddress.IPv4Network, ipaddress.IPv4Address]:
    if not isinstance(value, dict) or set(value) != {"name", "subnet", "gateway"}:
        _fail("invalid_network_" + name)
    if not isinstance(value["name"], str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{1,62}", value["name"]):
        _fail("invalid_network_name_" + name)
    try:
        subnet = ipaddress.IPv4Network(value["subnet"], strict=True)
    except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, TypeError):
        _fail("invalid_network_subnet_" + name)
    gateway = _private_ip(value["gateway"], name + "_gateway")
    if gateway not in subnet:
        _fail("network_gateway_outside_subnet_" + name)
    return subnet, gateway


def validate_config(config: Any, *, require_production: bool = False) -> dict[str, Any]:
    if not isinstance(config, dict) or config.get("schema_version") != SCHEMA_VERSION:
        _fail("unsupported_config_schema")
    if config.get("mode") not in ("production", "isolated-test"):
        _fail("invalid_runtime_mode")
    if require_production and config["mode"] != "production":
        _fail("production_mode_required")

    networks = config.get("networks")
    if not isinstance(networks, dict) or set(networks) != {"control", "egress"}:
        _fail("invalid_network_set")
    control_subnet, control_gateway = _network(networks["control"], "control")
    egress_subnet, egress_gateway = _network(networks["egress"], "egress")
    if control_subnet.overlaps(egress_subnet) or networks["control"]["name"] == networks["egress"]["name"]:
        _fail("overlapping_runtime_networks")

    addresses = config.get("addresses")
    if not isinstance(addresses, dict) or set(addresses) != {
        "browser", "gateway", "worker_egress", "gateway_egress"
    }:
        _fail("invalid_address_set")
    clients = config.get("clients")
    if (not isinstance(clients, dict) or not REQUIRED_CLIENTS <= set(clients)
            or len(clients) > MAX_CLIENTS or not clients):
        _fail("exact_client_identities_required")
    for client, item in clients.items():
        if not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", client) or not isinstance(item, dict):
            _fail("invalid_client_name_or_identity")
        if set(item) != {"label", "value", "image", "control_ip", "cdp_proxy_accepted"}:
            _fail("invalid_client_identity_" + client)
        if not isinstance(item["label"], str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", item["label"]):
            _fail("invalid_client_label_" + client)
        if not isinstance(item["value"], str) or not item["value"] or len(item["value"]) > 128:
            _fail("invalid_client_label_value_" + client)
        _require_image_reference(item["image"], "client_" + client,
                                 allow_local_id=config["mode"] == "isolated-test" and not require_production)
        if type(item["cdp_proxy_accepted"]) is not bool:
            _fail("client_compatibility_decision_required_" + client)
        if require_production and item["cdp_proxy_accepted"] is not True:
            _fail("client_proxy_compatibility_not_accepted_" + client)
        address = _private_ip(item["control_ip"], client)
        if address not in control_subnet or address in (control_gateway, control_subnet.network_address,
                                                        control_subnet.broadcast_address):
            _fail("control_address_outside_subnet_" + client)
    disabled = config.get("disabled_clients", [])
    if not isinstance(disabled, list) or len(disabled) > 8:
        _fail("disabled_client_inventory_invalid")
    disabled_names = set()
    for item in disabled:
        if not isinstance(item, dict) or set(item) != {"name", "id", "label", "value", "image"}:
            _fail("disabled_client_identity_invalid")
        if (not isinstance(item["name"], str) or not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", item["name"])
                or item["name"] in disabled_names or item["name"] in clients):
            _fail("disabled_client_name_invalid")
        disabled_names.add(item["name"])
        if not isinstance(item["id"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["id"]):
            _fail("disabled_client_exact_id_required_" + item["name"])
        if not isinstance(item["label"], str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", item["label"]):
            _fail("disabled_client_label_invalid_" + item["name"])
        if not isinstance(item["value"], str) or not item["value"] or len(item["value"]) > 128:
            _fail("disabled_client_label_value_invalid_" + item["name"])
        # Disabled inventory is never launched or pulled; Docker may report
        # only its immutable local image ID for an existing stopped container.
        _require_image_reference(item["image"], "disabled_client_" + item["name"],
                                 allow_local_id=True)
    if require_production and "personal_assistant" not in disabled_names:
        _fail("disabled_personal_assistant_inventory_required")
    for role in ("browser", "gateway"):
        address = _private_ip(addresses[role], role)
        if address not in control_subnet or address in (control_gateway, control_subnet.network_address,
                                                        control_subnet.broadcast_address):
            _fail("control_address_outside_subnet_" + role)
    for name in ("worker_egress", "gateway_egress"):
        address = _private_ip(addresses[name], name)
        if address not in egress_subnet or address in (egress_gateway, egress_subnet.network_address,
                                                        egress_subnet.broadcast_address):
            _fail("egress_address_outside_subnet_" + name)
    control_ips = [item["control_ip"] for item in clients.values()] + [addresses["browser"], addresses["gateway"]]
    if len(set(control_ips)) != len(control_ips):
        _fail("duplicate_control_address")
    if addresses["worker_egress"] == addresses["gateway_egress"]:
        _fail("duplicate_egress_address")

    images = config.get("images")
    if not isinstance(images, dict) or set(images) != {"anchor", "extractor", "cloak"}:
        _fail("invalid_image_set")
    for name in images:
        _require_image_reference(images[name], name,
                                 allow_local_id=config["mode"] == "isolated-test" and not require_production)

    names = config.get("containers")
    if not isinstance(names, dict) or set(names) != {
        "legacy_browser_id", "browser_anchor", "worker_anchor", "gateway_anchor",
        "cloak", "relay", "worker", "gateway",
    }:
        _fail("invalid_container_set")
    if config["mode"] == "production":
        if not isinstance(names["legacy_browser_id"], str) or not re.fullmatch(r"[0-9a-f]{64}", names["legacy_browser_id"]):
            _fail("legacy_browser_exact_id_required")
    elif names["legacy_browser_id"] not in (None, ""):
        _fail("isolated_test_must_not_name_production_browser")
    for name, value in names.items():
        if name == "legacy_browser_id":
            continue
        if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{2,62}", value):
            _fail("invalid_container_name_" + name)
    if len(set(value for key, value in names.items() if key != "legacy_browser_id")) != 7:
        _fail("duplicate_runtime_container_name")

    try:
        resolver = ipaddress.IPv4Address(config.get("resolver"))
    except (ipaddress.AddressValueError, TypeError):
        _fail("invalid_public_resolver")
    if not resolver.is_global:
        _fail("resolver_must_be_public_ipv4")

    paths = config.get("paths")
    if not isinstance(paths, dict) or set(paths) != {
        "runtime_dir", "cloak_launch", "relay_script", "browser_info_dir",
        "gateway_env_file", "worker_env_file", "browser_control_volume", "resolv_conf"
    }:
        _fail("invalid_runtime_paths")
    for key in ("runtime_dir", "cloak_launch", "relay_script", "browser_info_dir",
                "gateway_env_file", "worker_env_file", "browser_control_volume", "resolv_conf"):
        if not isinstance(paths[key], str) or not paths[key] or "\n" in paths[key] or "\r" in paths[key]:
            _fail("invalid_path_" + key)
    for key in ("cloak_launch", "relay_script", "browser_info_dir", "gateway_env_file", "worker_env_file", "resolv_conf"):
        if not Path(paths[key]).is_absolute():
            _fail("absolute_path_required_" + key)
    return config


def read_config(path: Path, *, require_root: bool = False,
                require_production: bool = False) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 128 * 1024:
            _fail("config_file_invalid")
        info = path.stat()
        if require_root and (info.st_uid != 0 or info.st_mode & 0o077):
            _fail("config_file_permissions_invalid")
        return validate_config(json.loads(path.read_text()), require_production=require_production)
    except (OSError, json.JSONDecodeError):
        _fail("config_file_invalid")


def _safe_file(path: Path, *, secret: bool = False) -> None:
    try:
        info = path.stat()
    except OSError:
        _fail("required_runtime_file_missing")
    if (path.is_symlink() or not path.is_file() or info.st_uid != 0
            or info.st_mode & (0o022 if secret else 0o022)
            or (secret and info.st_mode & 0o077)):
        _fail("required_runtime_file_permissions_invalid")


def _safe_directory(path: Path) -> None:
    try:
        info = path.stat()
    except OSError:
        _fail("required_runtime_directory_missing")
    if path.is_symlink() or not path.is_dir() or info.st_uid != 0 or info.st_mode & 0o022:
        _fail("required_runtime_directory_permissions_invalid")


def render_plan(config: dict[str, Any]) -> dict[str, Any]:
    """Return a non-secret operation summary; no env-file contents are read."""
    validate_config(config)
    names = config["containers"]
    return {
        "mode": config["mode"],
        "networks": [config["networks"][name]["name"] for name in ("control", "egress")],
        "required_clients": {
            name: {"expected_ip": config["clients"][name]["control_ip"],
                  "label": config["clients"][name]["label"],
                  "expected_image": config["clients"][name]["image"],
                  "cdp_proxy_accepted": config["clients"][name]["cdp_proxy_accepted"]}
            for name in config["clients"]
        },
        "legacy_browser_id": names["legacy_browser_id"],
        "containers": {key: value for key, value in names.items() if key != "legacy_browser_id"},
        "images": dict(config["images"]),
        "secrets": {"gateway_env_file": config["paths"]["gateway_env_file"],
                    "worker_env_file": config["paths"]["worker_env_file"]},
        "start_order": ["anchors", "policies_readback", "gateway", "cloak", "relay", "worker", "role_probes"],
        "network_policy_roles": ["browser", "gateway", "worker"],
    }


class Docker:
    def __init__(self, executable: str = "/usr/bin/docker"):
        self.executable = executable

    def call(self, *args: str, input_text: str | None = None, timeout: int = 20) -> str:
        try:
            result = subprocess.run([self.executable, *args], input=input_text, capture_output=True,
                                    text=True, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired):
            _fail("docker_command_unavailable")
        if result.returncode != 0:
            _fail("docker_operation_failed")
        return result.stdout.strip()

    def inspect(self, target: str) -> dict[str, Any]:
        try:
            rows = json.loads(self.call("inspect", target))
            if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
                _fail("docker_inspect_invalid")
            return rows[0]
        except json.JSONDecodeError:
            _fail("docker_inspect_invalid")


class Supervisor:
    def __init__(self, config: dict[str, Any], docker: Docker | None = None, *, require_production: bool = True):
        self.config = validate_config(config, require_production=require_production)
        self.docker = docker or Docker()
        self.owner_value = config.get("owner_value")
        if not isinstance(self.owner_value, str) or not re.fullmatch(r"news-[a-z0-9-]{8,40}", self.owner_value):
            _fail("invalid_owner_value")
        self.stop_requested = False
        self.children_started: list[str] = []
        self.anchors_started: list[str] = []
        self.policy_fingerprints: dict[str, str] = {}
        self._shutdown_signal = None

    def _label(self, extra: str = "") -> list[str]:
        result = ["--label", f"{OWNER_LABEL}={self.owner_value}"]
        if extra:
            result += ["--label", f"{OWNER_LABEL}.role={extra}"]
        return result

    def _exact_container(self, name: str, role: str, *, allow_missing: bool = False) -> dict[str, Any] | None:
        result = subprocess.run([self.docker.executable, "inspect", name], capture_output=True,
                                text=True, timeout=15, check=False)
        if result.returncode:
            if allow_missing:
                return None
            _fail("owned_container_missing")
        try:
            rows = json.loads(result.stdout)
            item = rows[0]
        except (json.JSONDecodeError, IndexError, TypeError):
            _fail("docker_inspect_invalid")
        labels = item.get("Config", {}).get("Labels") or {}
        if labels.get(OWNER_LABEL) != self.owner_value or labels.get(OWNER_LABEL + ".role") != role:
            _fail("container_ownership_mismatch")
        return item

    def _legacy_browser_is_stopped(self) -> None:
        legacy_id = self.config["containers"]["legacy_browser_id"]
        item = self.docker.inspect(legacy_id)
        host = item.get("HostConfig") or {}
        if item.get("Id") != legacy_id or item.get("State", {}).get("Running") is not False:
            _fail("legacy_shared_browser_not_stopped")
        if host.get("RestartPolicy", {}).get("Name") != "no":
            _fail("legacy_shared_browser_restart_not_disabled")

    def _client(self, role: str) -> dict[str, Any]:
        expected = self.config["clients"][role]
        if role == "worker":
            _fail("worker_is_runtime_owned")
        rows = self.docker.call("ps", "-aq", "--no-trunc", "--filter",
                                f"label={expected['label']}={expected['value']}").splitlines()
        rows = [row for row in rows if row]
        if len(rows) != 1:
            _fail("client_identity_not_unique_" + role)
        item = self.docker.inspect(rows[0])
        config = item.get("Config") or {}
        if (item.get("Id") != rows[0]
                or not _container_image_matches(item, expected["image"],
                                                allow_local_id=self.config["mode"] == "isolated-test")):
            _fail("client_image_identity_mismatch_" + role)
        if item.get("HostConfig", {}).get("NetworkMode") == "host":
            _fail("client_host_network_denied_" + role)
        nets = (item.get("NetworkSettings") or {}).get("Networks") or {}
        network_name = self.config["networks"]["control"]["name"]
        if (network_name not in nets or nets[network_name].get("IPAddress") != expected["control_ip"]
                or item.get("State", {}).get("Running") is not True):
            _fail("client_control_identity_mismatch_" + role)
        return item

    def _disabled_client_is_stopped(self, expected: dict[str, Any]) -> None:
        item = self.docker.inspect(expected["id"])
        config = item.get("Config") or {}
        labels = config.get("Labels") or {}
        restart = (item.get("HostConfig") or {}).get("RestartPolicy", {}).get("Name")
        if (item.get("Id") != expected["id"]
                or not _container_image_matches(item, expected["image"], allow_local_id=True)
                or labels.get(expected["label"]) != expected["value"]
                or item.get("State", {}).get("Running") is not False or restart != "no"):
            _fail("disabled_client_not_stopped_restart_disabled_" + expected["name"])

    def preflight(self) -> None:
        if os.geteuid() != 0:
            _fail("root_required")
        if self.config["mode"] == "production":
            self._legacy_browser_is_stopped()
            for client in self.config["disabled_clients"]:
                self._disabled_client_is_stopped(client)
        for role in self.config["clients"]:
            if role != "worker":
                self._client(role)
        paths = self.config["paths"]
        for key in ("cloak_launch", "relay_script", "resolv_conf"):
            _safe_file(Path(paths[key]))
        _safe_directory(Path(paths["browser_info_dir"]))
        try:
            resolver_lines = [line.split() for line in Path(paths["resolv_conf"]).read_text().splitlines()
                              if line.strip() and not line.lstrip().startswith("#")]
        except OSError:
            _fail("resolver_config_unavailable")
        if resolver_lines != [["nameserver", self.config["resolver"]]]:
            _fail("resolver_config_mismatch")
        for key in ("gateway_env_file", "worker_env_file"):
            secret = Path(paths[key])
            _safe_file(secret, secret=True)
            contents = secret.read_text()
            required = ("EGRESS_TOKEN",) if key == "gateway_env_file" else (
                "EXTRACTOR_SERVICE_TOKEN", "EXTRACTOR_NATIVE_PROXY_TOKEN")
            env_values = {}
            for line in contents.splitlines():
                if not line or line.lstrip().startswith("#"):
                    continue
                name, separator, value = line.partition("=")
                if not separator or not name or "\n" in value or "\r" in value:
                    _fail("runtime_secret_file_invalid")
                env_values[name] = value
            if any(not env_values.get(key_name) for key_name in required):
                _fail("runtime_secret_file_incomplete")

    def _ensure_network(self, key: str, *, internal: bool) -> None:
        cfg = self.config["networks"][key]
        name = cfg["name"]
        result = subprocess.run([self.docker.executable, "network", "inspect", name], capture_output=True,
                                text=True, timeout=15, check=False)
        if result.returncode:
            self.docker.call("network", "create", "--driver", "bridge", "--subnet", cfg["subnet"],
                             "--gateway", cfg["gateway"], "--label", f"{OWNER_LABEL}={self.owner_value}",
                             "--internal" if internal else "--attachable", name)
            result = subprocess.run([self.docker.executable, "network", "inspect", name], capture_output=True,
                                    text=True, timeout=15, check=False)
        try:
            item = json.loads(result.stdout)[0]
        except (json.JSONDecodeError, IndexError, TypeError):
            _fail("runtime_network_inspect_failed")
        if item.get("Name") != name:
            _fail("runtime_network_identity_mismatch")
        labels = item.get("Labels") or {}
        ipam = item.get("IPAM", {}).get("Config") or []
        if (labels.get(OWNER_LABEL) != self.owner_value or len(ipam) != 1
                or ipam[0].get("Subnet") != cfg["subnet"] or ipam[0].get("Gateway") != cfg["gateway"]
                or bool(item.get("Internal")) != internal or item.get("EnableIPv6") is True):
            _fail("runtime_network_configuration_mismatch")

    def _ensure_control_volume(self) -> None:
        name = self.config["paths"]["browser_control_volume"]
        result = subprocess.run([self.docker.executable, "volume", "inspect", name], capture_output=True,
                                text=True, timeout=15, check=False)
        if result.returncode:
            self.docker.call("volume", "create", "--label", f"{OWNER_LABEL}={self.owner_value}", name)
            result = subprocess.run([self.docker.executable, "volume", "inspect", name], capture_output=True,
                                    text=True, timeout=15, check=False)
        try:
            volume = json.loads(result.stdout)[0]
        except (json.JSONDecodeError, IndexError, TypeError):
            _fail("browser_control_volume_inspect_failed")
        if ((volume.get("Labels") or {}).get(OWNER_LABEL) != self.owner_value
                or volume.get("Name") != name or not isinstance(volume.get("Mountpoint"), str)
                or not Path(volume["Mountpoint"]).is_absolute()):
            _fail("browser_control_volume_identity_mismatch")

    def _create_anchor(self, role: str) -> str:
        names = self.config["containers"]
        anchor_key = role + "_anchor"
        name = names[anchor_key]
        existing = self._exact_container(name, role + "_anchor", allow_missing=True)
        if existing:
            if existing.get("State", {}).get("Running") is True:
                return name
            # A previous service-manager stop leaves restart=no anchors behind.
            # Remove only the exact, label-owned stopped container before reuse.
            self.docker.call("rm", name)
        network = self.config["networks"]["control"]["name"]
        address = (self.config["addresses"][role] if role in ("browser", "gateway")
                   else self.config["clients"]["worker"]["control_ip"])
        args = ["run", "--pull=never", "-d", "--name", name, *self._label(role + "_anchor"), "--restart=no",
                "--network", network, "--ip", address, "--cap-drop=ALL",
                "--security-opt=no-new-privileges:true", "--read-only",
                "--tmpfs", "/tmp:rw,nosuid,nodev,size=16m", "--entrypoint", "python",
                self.config["images"]["anchor"], "-c", ANCHOR_SLEEP]
        self.docker.call(*args)
        self.anchors_started.append(name)
        return name

    def _attach_egress(self, role: str, anchor: str) -> None:
        net = self.config["networks"]["egress"]["name"]
        address = self.config["addresses"][role + "_egress"]
        nets = (self._exact_container(anchor, role + "_anchor") or {}).get("NetworkSettings", {}).get("Networks", {})
        if net not in nets:
            self.docker.call("network", "connect", "--ip", address, net, anchor)
            nets = (self._exact_container(anchor, role + "_anchor") or {}).get("NetworkSettings", {}).get("Networks", {})
        if net not in nets or nets[net].get("IPAddress") != address:
            _fail("egress_network_identity_mismatch_" + role)

    def _anchor_pid(self, name: str, anchor_role: str) -> str:
        item = self._exact_container(name, anchor_role)
        if item is None or item.get("State", {}).get("Running") is not True:
            _fail("owned_anchor_unavailable")
        host_pid = item.get("State", {}).get("Pid")
        if type(host_pid) is not int or host_pid <= 1:
            _fail("owned_anchor_pid_invalid")
        role = anchor_role.removesuffix("_anchor")
        expected_ip = (self.config["addresses"][role] if role in ("browser", "gateway")
                       else self.config["clients"]["worker"]["control_ip"])
        host = item.get("HostConfig") or {}
        control_name = self.config["networks"]["control"]["name"]
        networks = (item.get("NetworkSettings") or {}).get("Networks") or {}
        if (not _container_image_matches(item, self.config["images"]["anchor"],
                                         allow_local_id=self.config["mode"] == "isolated-test")
                or host.get("RestartPolicy", {}).get("Name") != "no"
                or host.get("NetworkMode") != control_name
                or host.get("Privileged") is True
                or host.get("ReadonlyRootfs") is not True
                or "no-new-privileges:true" not in (host.get("SecurityOpt") or [])
                or "ALL" not in (host.get("CapDrop") or [])
                or host.get("PortBindings") or control_name not in networks
                or networks[control_name].get("IPAddress") != expected_ip):
            _fail("owned_anchor_identity_mismatch_" + role)
        egress_name = self.config["networks"]["egress"]["name"]
        if role in ("worker", "gateway"):
            expected_egress = self.config["addresses"][role + "_egress"]
            if egress_name not in networks or networks[egress_name].get("IPAddress") != expected_egress:
                _fail("owned_anchor_egress_identity_mismatch_" + role)
        elif egress_name in networks:
            _fail("browser_anchor_egress_forbidden")
        if os.stat(f"/proc/{host_pid}/ns/net").st_ino == os.stat("/proc/1/ns/net").st_ino:
            _fail("anchor_uses_host_network_namespace")
        return str(host_pid)

    def _set_default_route(self, role: str, name: str) -> None:
        pid = self._anchor_pid(name, role + "_anchor")
        ip_json = subprocess.run(["nsenter", "-t", pid, "-n", "--", "ip", "-j", "-4", "address"],
                                 capture_output=True, text=True, timeout=10, check=False)
        try:
            rows = json.loads(ip_json.stdout)
        except json.JSONDecodeError:
            _fail("namespace_address_readback_failed")
        target = self.config["addresses"][role + "_egress"]
        device = None
        for interface in rows:
            if any(info.get("local") == target for info in interface.get("addr_info", [])):
                device = interface["ifname"]
                break
        if not device:
            _fail("egress_interface_identity_mismatch_" + role)
        gateway = self.config["networks"]["egress"]["gateway"]
        try:
            subprocess.run(["nsenter", "-t", pid, "-n", "--", "ip", "route", "replace", "default",
                            "via", gateway, "dev", device], capture_output=True, text=True,
                           timeout=10, check=True)
            routes = json.loads(subprocess.run(["nsenter", "-t", pid, "-n", "--", "ip", "-j", "-4", "route"],
                                               capture_output=True, text=True, timeout=10, check=True).stdout)
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError):
            _fail("egress_route_configuration_failed_" + role)
        defaults = [route for route in routes if route.get("dst") == "default"]
        if len(defaults) != 1 or defaults[0].get("gateway") != gateway or defaults[0].get("dev") != device:
            _fail("egress_route_readback_mismatch_" + role)

    def _install_policy(self, role: str, anchor: str) -> None:
        config = self.config
        addresses = {
            "clients": {name: item["control_ip"] for name, item in config["clients"].items()},
            "browser": config["addresses"]["browser"],
            "gateway": config["addresses"]["gateway"],
        }
        host_ip_result = subprocess.run(["ip", "-j", "-4", "address", "show"], capture_output=True,
                                        text=True, timeout=10, check=False)
        if host_ip_result.returncode != 0:
            _fail("host_address_inventory_failed")
        try:
            host_ips = [entry["local"] for interface in json.loads(host_ip_result.stdout)
                        for entry in interface.get("addr_info", []) if entry.get("scope") == "global"]
        except (json.JSONDecodeError, KeyError):
            _fail("host_address_inventory_failed")
        if not host_ips:
            _fail("host_address_inventory_empty")
        payload = policy(role, addresses, config["resolver"], host_ips)
        expected_hash = re.search(r'comment "policy-sha256:([0-9a-f]{64})"', payload)
        if expected_hash is None:
            _fail("nft_policy_expected_hash_missing_" + role)
        pid = self._anchor_pid(anchor, role + "_anchor")
        try:
            subprocess.run(["nsenter", "-t", pid, "-n", "--", "nft", "-f", "-"], input=payload,
                           capture_output=True, text=True, timeout=15, check=True)
            readback = subprocess.run(["nsenter", "-t", pid, "-n", "--", "nft", "-j", "list", "table",
                                       "inet", TABLE], capture_output=True, text=True, timeout=10, check=True)
            value = json.loads(readback.stdout)
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError):
            _fail("nft_policy_install_or_readback_failed_" + role)
        tables = [item.get("table") for item in value.get("nftables", []) if isinstance(item, dict) and item.get("table")]
        table = next((item for item in tables if item.get("name") == TABLE), None)
        comment = (table or {}).get("comment", "")
        if comment != "policy-sha256:" + expected_hash.group(1) or not self._readback_has_drop_hooks(value):
            _fail("nft_policy_readback_invalid_" + role)
        self.policy_fingerprints[role] = self._policy_fingerprint(value)

    @staticmethod
    def _readback_has_drop_hooks(value: dict[str, Any]) -> bool:
        hooks = set()
        for item in value.get("nftables", []):
            if not isinstance(item, dict) or "chain" not in item:
                continue
            chain = item["chain"]
            if chain.get("hook") in ("input", "output", "forward") and chain.get("policy") == "drop":
                hooks.add(chain["hook"])
        return hooks == {"input", "output", "forward"}

    @staticmethod
    def _policy_fingerprint(value: dict[str, Any]) -> str:
        def stable(item: Any) -> Any:
            if isinstance(item, dict):
                return {key: stable(child) for key, child in sorted(item.items())
                        if key not in {"handle", "packets", "bytes"}}
            if isinstance(item, list):
                return [stable(child) for child in item]
            return item
        return hashlib.sha256(json.dumps(stable(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def install_roles(self) -> None:
        self.preflight()
        self._ensure_network("control", internal=True)
        self._ensure_network("egress", internal=False)
        anchors = {role: self._create_anchor(role) for role in ROLES}
        for role in ("gateway", "worker"):
            self._attach_egress(role, anchors[role])
            self._set_default_route(role, anchors[role])
        for role in ROLES:
            self._install_policy(role, anchors[role])
        self._ensure_control_volume()
        self._launch_services(anchors)
        self._probe_roles()

    def _run_args(self, role: str, image: str, anchor: str, entrypoint: str, command: list[str],
                  *, env: dict[str, str] | None = None, env_file: str | None = None,
                  mounts: list[str] | None = None, user: str | None = None, shm: str | None = None,
                  memory: str, cpus: str, pids: int) -> str:
        name = self.config["containers"][role]
        existing = self._exact_container(name, role, allow_missing=True)
        if existing:
            if existing.get("State", {}).get("Running") is True:
                _fail("owned_service_already_running")
            self.docker.call("rm", name)
        args = ["run", "--pull=never", "-d", "--name", name, *self._label(role), "--restart=no", "--network",
                "container:" + anchor, "--no-healthcheck", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=32m", "--memory", memory,
                "--cpus", cpus, "--pids-limit", str(pids), "--log-opt", "max-size=10m", "--log-opt", "max-file=2"]
        if user:
            args += ["--user", user]
        if shm:
            args += ["--shm-size", shm]
        if env_file:
            args += ["--env-file", env_file]
        for key, value in (env or {}).items():
            if key in SECRET_ENV_KEYS or "TOKEN" in key or "SECRET" in key:
                _fail("secret_environment_value_forbidden")
            args += ["--env", key + "=" + value]
        for mount in mounts or []:
            args += ["--mount", mount]
        args += ["--entrypoint", entrypoint, image, *command]
        self.docker.call(*args, timeout=60)
        self.children_started.append(name)
        self._verify_child(name, role, anchor, image)
        return name

    def _verify_child(self, name: str, role: str, anchor: str, image: str) -> None:
        item = self._exact_container(name, role)
        host = item.get("HostConfig") or {}
        if (not _container_image_matches(item, image,
                                         allow_local_id=self.config["mode"] == "isolated-test")
                or host.get("RestartPolicy", {}).get("Name") != "no"
                or host.get("NetworkMode") != "container:" + anchor
                or host.get("CapAdd") not in (None, [])
                or "ALL" not in (host.get("CapDrop") or [])
                or host.get("Privileged") is True
                or host.get("ReadonlyRootfs") is not True
                or "no-new-privileges:true" not in (host.get("SecurityOpt") or [])
                or host.get("PortBindings")):
            _fail("owned_child_security_configuration_mismatch_" + role)
        host_pid = item.get("State", {}).get("Pid")
        if type(host_pid) is not int or host_pid <= 1:
            _fail("owned_child_pid_invalid_" + role)
        anchor_name = next((key for key, value in self.config["containers"].items()
                            if key.endswith("_anchor") and value == anchor), None)
        if anchor_name is None:
            _fail("owned_anchor_identity_missing")
        anchor_pid = self._anchor_pid(anchor, anchor_name)
        if os.stat(f"/proc/{host_pid}/ns/net").st_ino != os.stat(f"/proc/{anchor_pid}/ns/net").st_ino:
            _fail("owned_child_namespace_mismatch_" + role)

    def _launch_services(self, anchors: dict[str, str], *, only: set[str] | None = None) -> None:
        c, images, paths, addresses = self.config["containers"], self.config["images"], self.config["paths"], self.config["addresses"]
        selected = only or {"gateway", "cloak", "relay", "worker"}
        if "gateway" in selected:
            self._run_args("gateway", images["extractor"], anchors["gateway"], "/extract-egress",
                           [], env={"EGRESS_LISTEN_ADDR": addresses["gateway"] + ":8081",
                                   "EGRESS_MAX_CONCURRENT": "4"}, env_file=paths["gateway_env_file"],
                           mounts=["type=bind,src=" + paths["resolv_conf"] + ",dst=/etc/resolv.conf,readonly"],
                           memory="128m", cpus="0.25", pids=64)
        if "cloak" in selected:
            self._clear_launch_info()
            self._run_args("cloak", images["cloak"], anchors["browser"], "/entrypoint.sh",
                       ["python", "/entry/launch.py", "--info-dir", "/info"],
                       env={"CLOAKBROWSER_AUTO_UPDATE": "false", "DISPLAY": "unix/:99"},
                       mounts=[
                           "type=bind,src=" + paths["cloak_launch"] + ",dst=/entry/launch.py,readonly",
                           "type=bind,src=" + paths["browser_info_dir"] + ",dst=/info",
                           "type=volume,src=" + paths["browser_control_volume"] + ",dst=/browser-control",
                       ], shm="1g", memory="768m", cpus="0.5", pids=256)
            self._wait_listener(anchors["browser"], 5100, 0)
        if "relay" in selected:
            relay_args = ["/entry/relay.py"]
            for item in self.config["clients"].values():
                relay_args += ["--controller-ip", item["control_ip"]]
            self._run_args("relay", images["anchor"], anchors["browser"], "python", relay_args,
                       mounts=["type=bind,src=" + paths["relay_script"] + ",dst=/entry/relay.py,readonly"],
                       user="65534:65534", memory="32m", cpus="0.25", pids=64)
            # The worker's native startup probe uses this exact relay. Do not
            # start it until the unprivileged relay has bound its admitted port.
            self._wait_listener(anchors["browser"], 9222, 65534, host="0.0.0.0")
        if "worker" in selected:
            self._run_args("worker", images["extractor"], anchors["worker"], "/extract-service", [],
                       env={"LISTEN_ADDR": self.config["clients"]["worker"]["control_ip"] + ":8080",
                            "EXTRACTOR_HTTP_ONLY": "false",
                            "EXTRACTOR_BROWSER_BACKEND": "native",
                            "CHROME_ENDPOINT": "http://" + addresses["browser"] + ":9222",
                            "EXTRACTOR_NATIVE_PROXY_URL": "http://" + addresses["gateway"] + ":8081",
                            "EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true",
                            "EXTRACTOR_MAX_CONCURRENT": "1"},
                       env_file=paths["worker_env_file"],
                       mounts=["type=bind,src=" + paths["resolv_conf"] + ",dst=/etc/resolv.conf,readonly"],
                       memory="256m", cpus="1", pids=128)

    def _clear_launch_info(self) -> None:
        root = Path(self.config["paths"]["browser_info_dir"])
        if root.is_symlink() or not root.is_dir():
            _fail("browser_info_directory_invalid")
        for name in ("launch-info.json", "launch-info.json.tmp"):
            path = root / name
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                _fail("browser_launch_info_ownership_invalid")
            path.unlink()

    @staticmethod
    def _listener_row_matches(row: str, port: int, uid: int, host: str) -> bool:
        fields = row.split()
        if len(fields) < 8:
            return False
        address = ipaddress.IPv4Address(host).packed[::-1].hex().upper()
        return fields[1] == address + ":" + format(port, "04X") and fields[3] == "0A" and fields[7] == str(uid)

    def _wait_listener(self, anchor: str, port: int, uid: int, *, host: str = "127.0.0.1") -> None:
        for _ in range(60):
            pid = self._anchor_pid(anchor, "browser_anchor")
            try:
                rows = Path(f"/proc/{pid}/net/tcp").read_text().splitlines()[1:]
            except OSError:
                _fail("browser_listener_readback_failed")
            if any(self._listener_row_matches(row, port, uid, host) for row in rows):
                return
            time.sleep(1)
        _fail("browser_loopback_listener_unavailable")

    def _probe_roles(self) -> None:
        app_id = self._client("news_app")["Id"]
        browser_url = "http://" + self.config["addresses"]["browser"] + ":9222/json/version"
        gateway_url = "http://" + self.config["addresses"]["gateway"] + ":8081/health/ready"
        worker_url = "http://" + self.config["clients"]["worker"]["control_ip"] + ":8080/health/ready"
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                output = self.docker.call("exec", app_id, "python", "-c", PROBE,
                                          browser_url, gateway_url, worker_url, timeout=10)
                if output.splitlines() == ["200", "200", "200"]:
                    break
            except RuntimeErrorSafe:
                # Cold HTTP listeners or the worker's native CDP startup probe
                # can refuse connections briefly; retry only these bounded
                # read-only readiness requests.
                pass
            time.sleep(1)
        else:
            _fail("admitted_role_readiness_timeout")
        names = self.config["containers"]
        self._check_process_uids(names["cloak"], 0)
        self._check_process_uids(names["relay"], 65534)

    def _check_process_uids(self, name: str, expected: int) -> None:
        rows = self.docker.call("top", name, "-eo", "uid,pid,comm").splitlines()[1:]
        if not rows or any(row.split()[0] != str(expected) for row in rows):
            _fail("owned_process_uid_boundary_mismatch")

    def _stop_named(self, name: str, role: str, timeout: int = 10) -> None:
        item = self._exact_container(name, role, allow_missing=True)
        if item is None or item.get("State", {}).get("Running") is not True:
            return
        self.docker.call("stop", "--time", str(timeout), name, timeout=timeout + 5)

    def stop_owned(self) -> None:
        names = self.config["containers"]
        errors = []
        for key, role in (("worker", "worker"), ("relay", "relay"), ("cloak", "cloak"),
                          ("gateway", "gateway")):
            try:
                self._stop_named(names[key], role, 12 if key in ("worker", "gateway") else 8)
            except RuntimeErrorSafe as exc:
                errors.append(str(exc))
        for key, role in (("worker_anchor", "worker_anchor"), ("gateway_anchor", "gateway_anchor"),
                          ("browser_anchor", "browser_anchor")):
            try:
                self._stop_named(names[key], role, 3)
            except RuntimeErrorSafe as exc:
                errors.append(str(exc))
        if errors:
            _fail("owned_cleanup_incomplete")

    def run(self) -> None:
        try:
            self.install_roles()
            while not self.stop_requested:
                if self.config["mode"] == "production":
                    self._legacy_browser_is_stopped()
                    for client in self.config["disabled_clients"]:
                        self._disabled_client_is_stopped(client)
                for role in self.config["clients"]:
                    if role != "worker":
                        self._client(role)
                self._verify_runtime_roles()
                self._check_browser_fd_limit()
                if self._consume_restart_marker():
                    self._restart_browser_generation()
                time.sleep(5)
        finally:
            self.stop_owned()

    def _verify_runtime_roles(self) -> None:
        names = self.config["containers"]
        for role in ROLES:
            anchor_role = role + "_anchor"
            anchor = names[anchor_role]
            anchor_pid = self._anchor_pid(anchor, anchor_role)
            if role in ("worker", "gateway"):
                self._verify_default_route(role, anchor_pid)
            self._verify_installed_policy(role, anchor_pid)
        for key, role in (("cloak", "cloak"), ("relay", "relay"), ("worker", "worker"),
                          ("gateway", "gateway")):
            item = self._exact_container(names[key], role, allow_missing=True)
            if item is None or item.get("State", {}).get("Running") is not True:
                if role in ("cloak", "relay"):
                    self._restart_browser_generation()
                    break
                self._launch_services({name: names[name + "_anchor"] for name in ROLES}, only={role})
                item = self._exact_container(names[key], role)
            anchor_role = "browser" if role in ("cloak", "relay") else role
            image_role = "cloak" if role == "cloak" else ("anchor" if role == "relay" else "extractor")
            self._verify_child(names[key], role, names[anchor_role + "_anchor"], self.config["images"][image_role])
        self._probe_roles()

    def _verify_default_route(self, role: str, pid: str) -> None:
        try:
            routes = json.loads(subprocess.run(["nsenter", "-t", pid, "-n", "--", "ip", "-j", "-4", "route"],
                                               capture_output=True, text=True, timeout=10, check=True).stdout)
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError):
            _fail("egress_route_readback_failed_" + role)
        expected_gateway = self.config["networks"]["egress"]["gateway"]
        defaults = [route for route in routes if route.get("dst") == "default"]
        if len(defaults) != 1 or defaults[0].get("gateway") != expected_gateway:
            _fail("egress_route_readback_mismatch_" + role)

    def _verify_installed_policy(self, role: str, pid: str) -> None:
        config = self.config
        addresses = {"clients": {name: item["control_ip"] for name, item in config["clients"].items()},
                     "browser": config["addresses"]["browser"], "gateway": config["addresses"]["gateway"]}
        host_result = subprocess.run(["ip", "-j", "-4", "address", "show"], capture_output=True,
                                     text=True, timeout=10, check=False)
        if host_result.returncode != 0:
            _fail("host_address_inventory_failed")
        try:
            host_ips = [entry["local"] for interface in json.loads(host_result.stdout)
                        for entry in interface.get("addr_info", []) if entry.get("scope") == "global"]
            if not host_ips:
                _fail("host_address_inventory_empty")
            expected = re.search(r'comment "policy-sha256:([0-9a-f]{64})"',
                                 policy(role, addresses, config["resolver"], host_ips))
            result = subprocess.run(["nsenter", "-t", pid, "-n", "--", "nft", "-j", "list", "table",
                                     "inet", TABLE], capture_output=True, text=True, timeout=10, check=True)
            value = json.loads(result.stdout)
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError,
                ValueError, TypeError):
            _fail("nft_policy_runtime_readback_failed_" + role)
        table = next((item.get("table") for item in value.get("nftables", [])
                      if isinstance(item, dict) and isinstance(item.get("table"), dict)
                      and item["table"].get("name") == TABLE), None)
        expected_comment = "policy-sha256:" + expected.group(1) if expected else None
        if (table is None or table.get("comment") != expected_comment or not self._readback_has_drop_hooks(value)
                or self.policy_fingerprints.get(role) != self._policy_fingerprint(value)):
            _fail("nft_policy_runtime_changed_" + role)

    def _check_browser_fd_limit(self) -> None:
        name = self.config["containers"]["cloak"]
        item = self._exact_container(name, "cloak")
        host_pid = (item or {}).get("State", {}).get("Pid")
        if type(host_pid) is not int or host_pid <= 1:
            _fail("cloak_process_missing")
        try:
            count = len(list(Path(f"/proc/{host_pid}/fd").iterdir()))
        except OSError:
            _fail("cloak_fd_inventory_unavailable")
        if count >= int(self.config.get("browser_fd_threshold", 800)):
            _fail("cloak_fd_threshold_exceeded")

    def _consume_restart_marker(self) -> bool:
        volume = self.config["paths"]["browser_control_volume"]
        item = self.docker.inspect(volume)
        if ((item.get("Labels") or {}).get(OWNER_LABEL) != self.owner_value or item.get("Name") != volume):
            _fail("browser_control_volume_identity_mismatch")
        root = Path(item.get("Mountpoint", ""))
        if not root.is_absolute() or root.is_symlink():
            _fail("browser_control_volume_invalid")
        try:
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError:
            _fail("browser_control_volume_unavailable")
        try:
            try:
                info = os.stat("restart.request", dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or info.st_size > 4096:
                _fail("browser_restart_marker_invalid")
            os.unlink("restart.request", dir_fd=directory)
            return True
        finally:
            os.close(directory)

    def _restart_browser_generation(self) -> None:
        names, images, paths = self.config["containers"], self.config["images"], self.config["paths"]
        self._stop_named(names["worker"], "worker", 12)
        self._stop_named(names["relay"], "relay", 8)
        self._stop_named(names["cloak"], "cloak", 10)
        for key, role in (("worker", "worker"), ("relay", "relay"), ("cloak", "cloak")):
            item = self._exact_container(names[key], role, allow_missing=True)
            if item and item.get("State", {}).get("Running") is not True:
                self.docker.call("rm", names[key])
        anchors = {role: names[role + "_anchor"] for role in ROLES}
        self._launch_browser_relay_worker(anchors)

    def _launch_browser_relay_worker(self, anchors: dict[str, str]) -> None:
        # Keep the already-running gateway intact during browser-generation recovery.
        self._launch_services(anchors, only={"cloak", "relay", "worker"})
        self._probe_roles()

    def signal(self, signum: int, _frame: Any) -> None:
        self.stop_requested = True
        self._shutdown_signal = signum


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "plan", "supervise", "stop-owned",
                                              "supervise-test", "stop-owned-test"))
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        production_command = args.command in ("supervise", "stop-owned")
        runtime_command = args.command in ("supervise", "stop-owned", "supervise-test", "stop-owned-test")
        config = read_config(args.config, require_root=runtime_command,
                             require_production=production_command)
        if args.command in ("supervise-test", "stop-owned-test") and config["mode"] != "isolated-test":
            _fail("isolated_test_mode_required")
        if args.command == "validate":
            print("configuration_valid")
            return 0
        if args.command == "plan":
            print(json.dumps(render_plan(config), sort_keys=True, indent=2))
            return 0
        supervisor = Supervisor(config, require_production=production_command)
        if args.command in ("stop-owned", "stop-owned-test"):
            supervisor.stop_owned()
            return 0
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, supervisor.signal)
        supervisor.run()
        return 0
    except RuntimeErrorSafe as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
