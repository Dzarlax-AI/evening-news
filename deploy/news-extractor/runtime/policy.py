"""Default-deny nftables policies for the persistent native browser roles."""

from __future__ import annotations

import hashlib
import ipaddress

TABLE = "news_native_guard"
SPECIAL4 = (
    "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
    "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
    "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24",
    "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
)


def _ip(value: str) -> str:
    address = ipaddress.ip_address(value)
    if address.version != 4 or not address.is_private:
        raise ValueError("runtime addresses must be private IPv4 literals")
    return str(address)


def _resolver_ip(value: str) -> str:
    address = ipaddress.ip_address(value)
    if address.version != 4 or not address.is_global:
        raise ValueError("resolver must be one exact public IPv4 literal")
    return str(address)


def _global_host_drops(host_ips: list[str]) -> list[str]:
    output = list(SPECIAL4)
    for value in host_ips:
        address = ipaddress.ip_address(value)
        if address.version == 4 and address.is_global:
            output.append(f"{address}/32")
    return output


def policy(role: str, addresses: dict[str, str], resolver: str,
           host_ips: list[str] | None = None) -> str:
    """Render a role policy; no CIDR or wildcard peer admission is accepted."""
    if not isinstance(addresses, dict) or set(addresses) != {"clients", "browser", "gateway"}:
        raise ValueError("role address set is incomplete")
    raw_clients = addresses["clients"]
    if (not isinstance(raw_clients, dict) or not 1 <= len(raw_clients) <= 8
            or not {"news_app", "worker"} <= set(raw_clients)):
        raise ValueError("runtime client list is incomplete or unbounded")
    clients_by_name = {name: _ip(value) for name, value in raw_clients.items()}
    if len(set(clients_by_name.values())) != len(clients_by_name):
        raise ValueError("duplicate runtime client address")
    peer = {name: _ip(addresses[name]) for name in ("browser", "gateway")}
    clients = "{ " + ", ".join(clients_by_name.values()) + " }"
    resolver = _resolver_ip(resolver)
    drops = _global_host_drops(host_ips or [])
    blocked = "{ " + ", ".join(drops) + " }"

    output = ["meta nfproto ipv6 counter drop", "ct state invalid counter drop"]
    incoming = ["meta nfproto ipv6 counter drop", "ct state invalid counter drop"]
    if role == "browser":
        controllers = clients
        output += [
            f"ip daddr {peer['gateway']} tcp dport 8081 ct state {{ new, established }} counter accept",
            f"ip daddr {controllers} tcp sport 9222 ct state established counter accept",
            "meta skuid 65534 oifname \"lo\" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept",
            "meta skuid 0 oifname \"lo\" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept",
        ]
        incoming += [
            f"ip saddr {controllers} tcp dport 9222 ct state {{ new, established }} counter accept",
            f"ip saddr {peer['gateway']} tcp sport 8081 ct state established counter accept",
            "iifname \"lo\" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept",
            "iifname \"lo\" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept",
        ]
    elif role == "worker":
        output += [
            f"ip daddr {peer['browser']} tcp dport 9222 ct state {{ new, established }} counter accept",
            f"ip daddr {peer['gateway']} tcp dport 8081 ct state {{ new, established }} counter accept",
            f"ip daddr {clients_by_name['news_app']} tcp sport 8080 ct state established counter accept",
            f"ip daddr {resolver} udp dport 53 counter accept",
            f"ip daddr {resolver} tcp dport 53 counter accept",
            f"ip daddr {blocked} counter drop",
            "tcp dport { 80, 443 } ct state { new, established } counter accept",
        ]
        incoming += [
            f"ip saddr {clients_by_name['news_app']} tcp dport 8080 ct state {{ new, established }} counter accept",
            f"ip saddr {peer['browser']} tcp sport 9222 ct state established counter accept",
            f"ip saddr {peer['gateway']} tcp sport 8081 ct state established counter accept",
            f"ip saddr {resolver} udp sport 53 ct state established counter accept",
            f"ip saddr {resolver} tcp sport 53 ct state established counter accept",
            f"ip saddr {blocked} counter drop",
            "tcp sport { 80, 443 } ct state established counter accept",
        ]
    elif role == "gateway":
        allowed_clients = "{ " + ", ".join((*clients_by_name.values(), peer["browser"])) + " }"
        output += [
            f"ip daddr {allowed_clients} tcp sport 8081 ct state established counter accept",
            f"ip daddr {resolver} udp dport 53 counter accept",
            f"ip daddr {resolver} tcp dport 53 counter accept",
            f"ip daddr {blocked} counter drop",
            "tcp dport 443 ct state { new, established } counter accept",
        ]
        incoming += [
            f"ip saddr {allowed_clients} tcp dport 8081 ct state {{ new, established }} counter accept",
            f"ip saddr {resolver} udp sport 53 ct state established counter accept",
            f"ip saddr {resolver} tcp sport 53 ct state established counter accept",
            f"ip saddr {blocked} counter drop",
            "tcp sport 443 ct state established counter accept",
        ]
    else:
        raise ValueError("unknown runtime role")

    forward = ["counter drop"]
    digest = hashlib.sha256(("\n".join(output + incoming + forward) + "\n").encode()).hexdigest()
    chains = []
    for name, rules in (("output", output), ("input", incoming)):
        body = "\n".join(f"    {rule}" for rule in rules)
        chains.append(f"chain {name} {{\n    type filter hook {name} priority -150; policy drop;\n{body}\n    counter drop\n}}")
    chains.append("chain forward {\n    type filter hook forward priority -150; policy drop;\n    counter drop\n}")
    return f'table inet {TABLE} {{\n  comment "policy-sha256:{digest}"\n' + "\n".join(chains) + "\n}\n"

