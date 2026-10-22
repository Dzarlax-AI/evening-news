"""Default-deny nftables policy for the shared Cloak browser container."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from typing import Any

TABLE = "news_native_guard"
REQUIRED_CONTROLLERS = {"news_app", "worker"}
MAX_CONTROLLERS = 8


def private_ipv4(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an exact private IPv4 literal")
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        raise ValueError(f"{label} must be an exact private IPv4 literal") from None
    if (address.version != 4 or not address.is_private or address.is_loopback
            or address.is_link_local or address.is_multicast or address.is_unspecified):
        raise ValueError(f"{label} must be an exact private IPv4 literal")
    return str(address)


def parse_controllers(value: str) -> dict[str, str]:
    """Parse a named, finite set of private controller IPv4 addresses."""
    try:
        raw = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        raise ValueError("CLOAK_CONTROLLER_IPS must be a JSON object") from None
    if (not isinstance(raw, dict) or not REQUIRED_CONTROLLERS <= set(raw)
            or len(raw) > MAX_CONTROLLERS):
        raise ValueError("controller set must include news_app and worker and contain at most 8 clients")
    result: dict[str, str] = {}
    for name, address in raw.items():
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", name):
            raise ValueError("controller names must be simple identifiers")
        result[name] = private_ipv4(address, "controller address")
    if len(set(result.values())) != len(result):
        raise ValueError("controller addresses must be unique")
    return result


def render_policy(self_ip: str, controller_ips: dict[str, str], proxy_ip: str) -> str:
    """Render role-specific rules with exact peers and default-drop hooks."""
    own = private_ipv4(self_ip, "CLOAK_SELF_IP")
    proxy = private_ipv4(proxy_ip, "CLOAK_PROXY_IP")
    if (not isinstance(controller_ips, dict) or not REQUIRED_CONTROLLERS <= set(controller_ips)
            or not 2 <= len(controller_ips) <= MAX_CONTROLLERS):
        raise ValueError("controller set must include news_app and worker and contain at most 8 clients")
    controllers = {
        name: private_ipv4(address, "controller address") for name, address in controller_ips.items()
    }
    if any(not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", name)
           for name in controllers):
        raise ValueError("controller names must be simple identifiers")
    if len(set(controllers.values())) != len(controllers):
        raise ValueError("controller addresses must be unique")
    if own in controllers.values() or proxy in controllers.values() or own == proxy:
        raise ValueError("browser, proxy, and controller addresses must be distinct")
    peers = "{ " + ", ".join(controllers.values()) + " }"

    output = [
        "meta nfproto ipv6 counter drop",
        "ct state invalid counter drop",
        f"ip daddr {proxy} tcp dport 8081 ct state {{ new, established }} counter accept",
        f"ip daddr {peers} tcp sport 9222 ct state established counter accept",
        'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept',
        'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept',
    ]
    incoming = [
        "meta nfproto ipv6 counter drop",
        "ct state invalid counter drop",
        f"ip saddr {peers} tcp dport 9222 ct state {{ new, established }} counter accept",
        f"ip saddr {proxy} tcp sport 8081 ct state established counter accept",
        'iifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept',
        'iifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept',
    ]
    forward = ["counter drop"]
    digest = hashlib.sha256(("\n".join(output + incoming + forward) + "\n").encode()).hexdigest()
    chains = []
    for name, rules in (("output", output), ("input", incoming)):
        body = "\n".join(f"    {rule}" for rule in rules)
        chains.append(f"chain {name} {{\n    type filter hook {name} priority -150; policy drop;\n{body}\n    counter drop\n}}")
    chains.append("chain forward {\n    type filter hook forward priority -150; policy drop;\n    counter drop\n}")
    return (f'table inet {TABLE} {{\n  comment "policy-sha256:{digest}"\n'
            + "\n".join(chains) + "\n}\n")


def stable_fingerprint(value: dict[str, Any]) -> str:
    """Hash nft JSON while ignoring kernel handles and changing counters."""
    def stable(item: Any) -> Any:
        if isinstance(item, dict):
            return {key: stable(child) for key, child in sorted(item.items())
                    if key not in {"handle", "packets", "bytes"}}
        if isinstance(item, list):
            return [stable(child) for child in item]
        return item

    return hashlib.sha256(json.dumps(stable(value), sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def readback_has_drop_hooks(value: dict[str, Any]) -> bool:
    hooks = set()
    for item in value.get("nftables", []):
        if not isinstance(item, dict) or not isinstance(item.get("chain"), dict):
            continue
        chain = item["chain"]
        if chain.get("hook") in {"input", "output", "forward"} and chain.get("policy") == "drop":
            hooks.add(chain["hook"])
    return hooks == {"input", "output", "forward"}


def readback_comment_matches(value: dict[str, Any], text: str, expected: str) -> bool:
    tables = [item["table"] for item in value.get("nftables", [])
              if isinstance(item, dict) and isinstance(item.get("table"), dict)
              and item["table"].get("name") == TABLE]
    if len(tables) != 1:
        return False
    comments = re.findall(r'\bcomment\s+"(policy-sha256:[0-9a-f]{64})"', text)
    comment = tables[0].get("comment")
    if comment is not None:
        return comment == expected and comments == [expected]
    return comments == [expected]
