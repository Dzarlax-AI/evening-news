"""Bounded, peer-restricted CDP relay; synthetic mode is explicitly opt-in."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
from pathlib import Path
import secrets
import stat
import signal
import time
from collections.abc import Awaitable, Iterable

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 9222
TARGET_HOST = "127.0.0.1"
TARGET_PORT = 5100
MAX_CONNECTIONS = 4
BUFFER_BYTES = 65536
IDLE_SECONDS = 120
CONNECT_SECONDS = 2
LIFETIME_SECONDS = 7200
MAX_HTTP_HEADER = 16 * 1024
MAX_WS_FRAME = 1024 * 1024
MAX_WS_MESSAGE = 1024 * 1024
MAX_TRACKED_IDS = 64
MAX_PROTOCOL_ID_BYTES = 256
ARM_NAME = "overload-once.json"
MAX_CONTROLLERS = 8
HEARTBEAT_INTERVAL_SECONDS = 2
HEARTBEAT_DIRECTORY_UID = 0


def private_controller_ip(value: str) -> str:
    try:
        address = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        raise ValueError("Controller address must be an exact private IPv4 literal") from None
    if not address.is_private or address.is_loopback or address.is_link_local or address.is_multicast:
        raise ValueError("Controller address must be an exact private IPv4 literal")
    return str(address)


def private_controller_ips(values: str | Iterable[str], *, allow_loopback: bool = False) -> frozenset[str]:
    """Validate a finite exact-IP controller allowlist (never CIDRs)."""
    if isinstance(values, str):
        values = (values,)
    parsed = []
    for value in values:
        if allow_loopback:
            try:
                address = ipaddress.IPv4Address(value)
            except ipaddress.AddressValueError:
                raise ValueError("Controller address must be an exact IPv4 literal") from None
            if not (address.is_private or address.is_loopback) or address.is_link_local or address.is_multicast:
                raise ValueError("Controller address must be an exact local IPv4 literal")
            parsed.append(str(address))
        else:
            parsed.append(private_controller_ip(value))
    result = frozenset(parsed)
    if not result or len(result) > MAX_CONTROLLERS:
        raise ValueError("Controller allowlist must contain 1 to 8 exact private IPv4 addresses")
    return result


async def copy_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        data = await asyncio.wait_for(reader.read(BUFFER_BYTES), timeout=IDLE_SECONDS)
        if not data:
            if writer.can_write_eof():
                writer.write_eof()
                await writer.drain()
            return
        writer.write(data)
        await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)


async def close_writer(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, ConnectionError, RuntimeError):
        pass


async def read_http_header(reader: asyncio.StreamReader) -> bytes:
    value = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=IDLE_SECONDS)
    if len(value) > MAX_HTTP_HEADER:
        raise ValueError("http_header_too_large")
    return value


def is_websocket_upgrade(header: bytes) -> bool:
    lines = header.lower().split(b"\r\n")
    connection_tokens = {token.strip() for line in lines[1:] if line.startswith(b"connection:")
                         for token in line.split(b":", 1)[1].split(b",")}
    return (bool(lines) and lines[0].startswith(b"get ")
            and any(line == b"upgrade: websocket" for line in lines[1:])
            and b"upgrade" in connection_tokens)


def strip_websocket_extensions(header: bytes) -> bytes:
    """Remove only WebSocket extension offers unsupported by the frame inspector."""
    kept: list[bytes] = []
    skip_continuation = False
    for line in header.split(b"\r\n"):
        if skip_continuation and line.startswith((b" ", b"\t")):
            continue
        skip_continuation = False
        name, separator, _value = line.partition(b":")
        if separator and name.strip().lower() == b"sec-websocket-extensions":
            skip_continuation = True
            continue
        kept.append(line)
    return b"\r\n".join(kept)


async def _read_frame(reader: asyncio.StreamReader, *, expect_mask: bool) -> tuple[bytes, int, bool, bytes]:
    head = await asyncio.wait_for(reader.readexactly(2), timeout=IDLE_SECONDS)
    first, second = head
    fin = bool(first & 0x80)
    opcode = first & 0x0F
    masked = bool(second & 0x80)
    if first & 0x70 or opcode in (3, 4, 5, 6, 7, 11, 12, 13, 14, 15) or masked != expect_mask:
        raise ValueError("invalid_websocket_frame")
    size = second & 0x7F
    raw = bytearray(head)
    if size == 126:
        ext = await reader.readexactly(2)
        raw.extend(ext)
        size = int.from_bytes(ext, "big")
        if size < 126:
            raise ValueError("noncanonical_frame_length")
    elif size == 127:
        ext = await reader.readexactly(8)
        raw.extend(ext)
        size = int.from_bytes(ext, "big")
        if size < 65536 or size >> 63:
            raise ValueError("invalid_frame_length")
    if size > MAX_WS_FRAME or (opcode >= 8 and (not fin or size > 125)):
        raise ValueError("websocket_frame_too_large")
    mask = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(size)
    raw.extend(mask)
    raw.extend(payload)
    decoded = bytes(byte ^ mask[index & 3] for index, byte in enumerate(payload)) if masked else payload
    return bytes(raw), opcode, fin, decoded


async def read_frame(reader: asyncio.StreamReader, *, expect_mask: bool) -> tuple[bytes, int, bool, bytes]:
    return await asyncio.wait_for(_read_frame(reader, expect_mask=expect_mask), timeout=IDLE_SECONDS)


def encode_server_text(value: bytes) -> bytes:
    length = len(value)
    if length <= 125:
        return bytes((0x81, length)) + value
    if length <= 65535:
        return b"\x81\x7e" + length.to_bytes(2, "big") + value
    return b"\x81\x7f" + length.to_bytes(8, "big") + value


def notification(session_id: str) -> bytes:
    value = json.dumps({"method": "Network.dataReceived", "sessionId": session_id,
                        "params": {"requestId": "synthetic-zero", "timestamp": 0,
                                   "dataLength": 0, "encodedDataLength": 0}},
                       separators=(",", ":")).encode()
    return encode_server_text(value)


def _safe_json(data: bytes) -> dict | None:
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


class SyntheticState:
    """Ephemeral command correlation and privacy-preserving result counters."""

    def __init__(self, arm_dir: Path | None, report: Path | None):
        self.arm_dir = arm_dir
        self.report = report
        self.armed = False
        self.consumed = False
        self.arm_initialized = False
        self.contexts: set[str] = set()
        self.targets: set[str] = set()
        self.target_contexts: dict[str, str] = {}
        self.sessions: dict[str, str] = {}
        self.cleanup_contexts_requested: set[str] = set()
        self.cleanup_targets_requested: set[str] = set()
        self.cleanup_contexts_replied: set[str] = set()
        self.cleanup_targets_replied: set[str] = set()
        self.context_absent = 0
        self.target_absent = 0
        self.notifications_injected = 0
        self.barriers = 0
        self.reported = False

    def poll_arm(self) -> bool:
        if self.arm_dir is None or self.armed or self.consumed:
            return self.armed and not self.consumed
        path = self.arm_dir / ARM_NAME
        try:
            info = path.stat(follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
                    or info.st_size > 64 or path.is_symlink()):
                return False
            value = json.loads(path.read_text())
            if value != {"arm": "overload-once"}:
                return False
            if not self.arm_initialized:
                self.contexts.clear()
                self.targets.clear()
                self.target_contexts.clear()
                self.sessions.clear()
                self.cleanup_contexts_requested.clear()
                self.cleanup_targets_requested.clear()
                self.cleanup_contexts_replied.clear()
                self.cleanup_targets_replied.clear()
                self.context_absent = 0
                self.target_absent = 0
                self.notifications_injected = 0
                self.barriers = 0
                self.arm_initialized = True
            # The directory is mounted read-only into this unprivileged process.
            # The in-memory latch consumes the visible root-owned marker exactly once.
            self.armed = True
            return True
        except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
            return False

    def on_command(self, value: dict, pending: dict) -> None:
        command_id = value.get("id")
        method = value.get("method")
        if command_id is None or not isinstance(method, str):
            return
        if not (self.armed or self.consumed):
            return
        if command_id not in pending and len(pending) >= 512:
            raise ValueError("command_tracking_limit")
        meta = {"session_id": value.get("sessionId")}
        if method in ("Fetch.continueRequest", "Fetch.continueWithAuth"):
            meta["fetch"] = True
        if method == "Target.createBrowserContext":
            meta["kind"] = "create_context"
            pending[command_id] = meta
        elif method == "Target.createTarget":
            meta["kind"] = "create_target"
            meta["browser_context_id"] = (value.get("params") or {}).get("browserContextId")
            pending[command_id] = meta
        elif method == "Target.attachToTarget":
            target_id = (value.get("params") or {}).get("targetId")
            if target_id in self.targets and self.target_contexts.get(target_id) in self.contexts:
                meta.update(kind="attach_target", owned_id=target_id)
                pending[command_id] = meta
        elif method == "Target.disposeBrowserContext":
            context_id = (value.get("params") or {}).get("browserContextId")
            if context_id in self.contexts:
                meta.update(kind="cleanup_context", owned_id=context_id)
                sessions = [session for session, target in self.sessions.items()
                            if self.target_contexts.get(target) == context_id]
                if len(sessions) == 1:
                    meta["session_id"] = sessions[0]
                self.cleanup_contexts_requested.add(context_id)
                pending[command_id] = meta
        elif method == "Target.closeTarget":
            target_id = (value.get("params") or {}).get("targetId")
            if target_id in self.targets:
                meta.update(kind="cleanup_target", owned_id=target_id)
                self.cleanup_targets_requested.add(target_id)
                pending[command_id] = meta
        elif method in ("Target.getBrowserContexts", "Target.getTargets"):
            meta["kind"] = "list_contexts" if method.endswith("Contexts") else "list_targets"
            meta["barriers_at_issue"] = self.barriers
            meta["cleanup_ready_at_issue"] = self.cleanup_ready()
            pending[command_id] = meta
        elif meta.get("fetch"):
            pending[command_id] = meta

    def on_reply(self, value: dict, pending: dict) -> dict | None:
        command_id = value.get("id")
        item = pending.pop(command_id, None)
        if not item:
            return None
        if "error" in value:
            return item
        result = value.get("result") or {}
        kind = item.get("kind")
        if kind == "create_context" and isinstance(result.get("browserContextId"), str):
            if len(self.contexts) >= MAX_TRACKED_IDS or len(result["browserContextId"]) > MAX_PROTOCOL_ID_BYTES:
                raise ValueError("context_tracking_limit")
            self.contexts.add(result["browserContextId"])
        elif kind == "create_target" and isinstance(result.get("targetId"), str):
            if len(self.targets) >= MAX_TRACKED_IDS or len(result["targetId"]) > MAX_PROTOCOL_ID_BYTES:
                raise ValueError("target_tracking_limit")
            context_id = item.get("browser_context_id")
            if context_id not in self.contexts:
                raise ValueError("target_context_unowned")
            self.targets.add(result["targetId"])
            self.target_contexts[result["targetId"]] = context_id
        elif kind == "attach_target" and isinstance(result.get("sessionId"), str):
            session_id = result["sessionId"]
            if len(self.sessions) >= MAX_TRACKED_IDS or len(session_id) > MAX_PROTOCOL_ID_BYTES:
                raise ValueError("session_tracking_limit")
            self.sessions[session_id] = item["owned_id"]
        elif kind == "cleanup_context":
            self.cleanup_contexts_replied.add(item["owned_id"])
        elif kind == "cleanup_target":
            self.cleanup_targets_replied.add(item["owned_id"])
        elif kind == "list_contexts":
            current = set(result.get("browserContextIds", []))
            if (self.consumed and self.barriers > 0 and item.get("barriers_at_issue", 0) >= self.barriers
                    and item.get("cleanup_ready_at_issue")):
                self.context_absent += int(not (self.contexts & current))
        elif kind == "list_targets":
            current = {row.get("targetId") for row in result.get("targetInfos", []) if isinstance(row, dict)}
            if (self.consumed and self.barriers > 0 and item.get("barriers_at_issue", 0) >= self.barriers
                    and item.get("cleanup_ready_at_issue")):
                self.target_absent += int(not (self.targets & current))
        return item

    def cleanup_ready(self) -> bool:
        context_cleanup = (len(self.contexts) == 1 and len(self.targets) == 1
                           and self.cleanup_contexts_requested >= self.contexts
                           and self.cleanup_contexts_replied >= self.contexts)
        target_fallback = (not self.contexts and len(self.targets) == 1
                           and self.cleanup_targets_requested >= self.targets
                           and self.cleanup_targets_replied >= self.targets)
        return bool(context_cleanup or target_fallback)

    def on_event(self, value: dict) -> None:
        if not (self.armed or self.consumed):
            return
        if value.get("method") != "Target.attachedToTarget":
            return
        params = value.get("params") or {}
        target_info = params.get("targetInfo") or {}
        session_id = params.get("sessionId") or value.get("sessionId")
        target_id = target_info.get("targetId")
        if (isinstance(session_id, str) and target_id in self.targets
                and self.target_contexts.get(target_id) in self.contexts):
            if len(self.sessions) >= MAX_TRACKED_IDS or len(session_id) > MAX_PROTOCOL_ID_BYTES:
                raise ValueError("session_tracking_limit")
            self.sessions[session_id] = target_id

    def reserve_barrier(self, item: dict) -> bool:
        """Latch the one-shot before yielding so concurrent replies cannot duplicate it."""
        if (item.get("kind") != "cleanup_context"
                or item.get("owned_id") not in self.contexts
                or item.get("owned_id") not in self.cleanup_contexts_replied
                or not self.armed or self.consumed
                or not isinstance(item.get("session_id"), str)
                or item.get("session_id") not in self.sessions):
            return False
        self.consumed = True
        self.armed = False
        return True

    def write_report(self) -> None:
        if self.report is None or not self.consumed:
            return
        context_cleanup_confirmed = (bool(self.contexts)
                                     and self.cleanup_contexts_requested >= self.contexts
                                     and self.cleanup_contexts_replied >= self.contexts)
        target_cleanup_fallback = (not self.contexts and bool(self.targets)
                                   and self.cleanup_targets_requested >= self.targets
                                   and self.cleanup_targets_replied >= self.targets)
        cleanup_confirmed = (bool(self.contexts) and bool(self.targets)
                             and (context_cleanup_confirmed or target_cleanup_fallback)
                             and self.context_absent > 0 and self.target_absent > 0)
        value = {"schema_version": 1, "arm_consumed": self.consumed,
                 "overload_barriers": self.barriers, "notifications_injected": self.notifications_injected,
                 "owned_context_allocations": len(self.contexts), "owned_target_allocations": len(self.targets),
                 "cleanup_requests": len(self.cleanup_contexts_requested) + len(self.cleanup_targets_requested),
                 "cleanup_replies": len(self.cleanup_contexts_replied) + len(self.cleanup_targets_replied),
                 "contexts_absent_confirmations": self.context_absent,
                 "targets_absent_confirmations": self.target_absent,
                 "cleanup_confirmed": cleanup_confirmed}
        self.report.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temp = self.report.with_name(self.report.name + ".tmp-" + secrets.token_hex(4))
        fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, self.report)


async def websocket_copy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *,
                        client_side: bool, state: SyntheticState, pending: dict) -> None:
    """Forward frames intact, while correlating complete bounded messages in memory."""
    fragment_opcode: int | None = None
    fragments = bytearray()
    deferred_frames: list[bytes] = []
    deferred_bytes = 0
    while True:
        if client_side:
            state.poll_arm()
        raw, opcode, fin, payload = await read_frame(reader, expect_mask=client_side)
        message: bytes | None = None
        if opcode < 8:
            if opcode == 0:
                if fragment_opcode is None:
                    raise ValueError("unexpected_continuation")
                fragments.extend(payload)
            else:
                if fragment_opcode is not None:
                    raise ValueError("nested_fragment")
                fragment_opcode = opcode
                fragments = bytearray(payload)
            if len(fragments) > MAX_WS_MESSAGE:
                raise ValueError("websocket_message_too_large")
            deferred_frames.append(raw)
            deferred_bytes += len(raw)
            if len(deferred_frames) > 4096 or deferred_bytes > MAX_WS_MESSAGE + 65536:
                raise ValueError("websocket_frame_count_limit")
            if fin:
                if fragment_opcode == 1:
                    message = bytes(fragments)
                elif fragment_opcode == 2:
                    raise ValueError("binary_cdp_message_forbidden")
                fragment_opcode = None
                fragments.clear()
        if opcode >= 8:
            if fragment_opcode is not None:
                deferred_frames.append(raw)
                deferred_bytes += len(raw)
                if len(deferred_frames) > 4096 or deferred_bytes > MAX_WS_MESSAGE + 65536:
                    raise ValueError("websocket_frame_count_limit")
            else:
                writer.write(raw)
                await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)
        if opcode == 8:
            if client_side:
                for frame in deferred_frames:
                    writer.write(frame)
                deferred_frames.clear()
                await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)
            return
        value = _safe_json(message) if message is not None else None
        if message is not None and value is None:
            raise ValueError("invalid_cdp_json")
        if client_side and value is not None:
            state.poll_arm()
            state.on_command(value, pending)
            for frame in deferred_frames:
                writer.write(frame)
            deferred_frames.clear()
            deferred_bytes = 0
            await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)
        elif not client_side and value is not None:
            item = state.on_reply(value, pending)
            state.on_event(value)
            # During owned-context disposal the native consumer has stopped.
            # Hold the cleanup reply until the fixed burst has filled its queue;
            # this tests actual overflow even with nonblocking event RPCs.
            if item and state.reserve_barrier(item):
                for _ in range(257):
                    writer.write(notification(item["session_id"]))
                await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)
                state.notifications_injected += 257
                state.barriers += 1
            for frame in deferred_frames:
                writer.write(frame)
            deferred_frames.clear()
            deferred_bytes = 0
            await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)
            state.write_report()
        elif fin and deferred_frames:
            for frame in deferred_frames:
                writer.write(frame)
            deferred_frames.clear()
            deferred_bytes = 0
            await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)


class Relay:
    def __init__(self, controller_ip: str | Iterable[str], target_host: str = TARGET_HOST,
                 target_port: int = TARGET_PORT, *, arm_dir: Path | None = None,
                 report: Path | None = None, diagnostics_report: Path | None = None,
                 heartbeat_path: Path | None = None,
                 allow_loopback_test_peer: bool = False):
        self.controller_ips = private_controller_ips(controller_ip, allow_loopback=allow_loopback_test_peer)
        # Keep the historical attribute for single-controller in-process callers.
        self.controller_ip = next(iter(self.controller_ips)) if len(self.controller_ips) == 1 else None
        self.target_host = target_host
        self.target_port = target_port
        self.active = 0
        self.tasks: set[asyncio.Task] = set()
        self.synthetic = arm_dir is not None
        self.state = SyntheticState(arm_dir, report) if self.synthetic else None
        self.diagnostics_report = diagnostics_report if self.synthetic else None
        self.heartbeat_path = heartbeat_path if heartbeat_path is not None and not self.synthetic else None
        self.diagnostic_counts = {
            name: 0 for name in (
                'accepted', 'peer_rejected', 'capacity_rejected', 'connect_attempted',
                'connected', 'connect_timeout', 'connect_error', 'protocol_error',
                'stream_timeout', 'transport_error', 'relay_error', 'websocket_upgrade',
                'websocket_rejected', 'closed',
            )
        }
        self.peak_active = 0
        self.connect_ms_total = 0
        self.connect_ms_max = 0
        self.connection_ms_total = 0
        self.connection_ms_max = 0
        self.loop_lag_samples = 0
        self.loop_lag_max_ms = 0

    async def heartbeat(self) -> None:
        """Refresh a private liveness file only while this asyncio loop is responsive."""
        while True:
            write_heartbeat(self.heartbeat_path, os.getpid())
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

    def _write_diagnostics(self) -> None:
        path = self.diagnostics_report
        if path is None:
            return
        value = {
            'schema_version': 1,
            'active': self.active,
            'peak_active': self.peak_active,
            'counters': dict(self.diagnostic_counts),
            'timing_ms': {
                'connect_total': self.connect_ms_total,
                'connect_max': self.connect_ms_max,
                'connection_total': self.connection_ms_total,
                'connection_max': self.connection_ms_max,
            },
            'tcp_listener_counters': self._tcp_listener_counters(),
            'tcp_connection_counters': self._tcp_connection_counters(),
            'event_loop_lag_ms': {'samples': self.loop_lag_samples, 'max': self.loop_lag_max_ms},
        }
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            temp = path.with_name(path.name + '.tmp-' + secrets.token_hex(4))
            fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, 'w') as stream:
                json.dump(value, stream, separators=(',', ':'))
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, path)
        except OSError:
            # Diagnostics never change relay forwarding or failure behavior.
            return

    @staticmethod
    def _tcp_listener_counters() -> dict[str, int] | None:
        names = ('ListenOverflows', 'ListenDrops', 'TCPReqQFullDrop')
        try:
            rows = Path('/proc/net/netstat').read_text().splitlines()
            for index, header in enumerate(rows[:-1]):
                if not header.startswith('TcpExt:') or not rows[index + 1].startswith('TcpExt:'):
                    continue
                keys = header.split()[1:]
                values = rows[index + 1].split()[1:]
                if len(keys) != len(values):
                    return None
                counters = dict(zip(keys, values))
                result = {name: int(counters[name]) for name in names}
                if any(value < 0 for value in result.values()):
                    return None
                return result
        except (OSError, ValueError, KeyError):
            return None
        return None

    @staticmethod
    def _tcp_connection_counters() -> dict[str, int] | None:
        names = ('ActiveOpens', 'AttemptFails', 'RetransSegs', 'InErrs', 'OutRsts')
        try:
            rows = Path('/proc/net/snmp').read_text().splitlines()
            for index, header in enumerate(rows[:-1]):
                if not header.startswith('Tcp:') or not rows[index + 1].startswith('Tcp:'):
                    continue
                keys = header.split()[1:]
                values = rows[index + 1].split()[1:]
                if len(keys) != len(values):
                    return None
                counters = dict(zip(keys, values))
                result = {name: int(counters[name]) for name in names}
                if any(value < 0 for value in result.values()):
                    return None
                return result
        except (OSError, ValueError, KeyError):
            return None
        return None

    def _count(self, name: str) -> None:
        self.diagnostic_counts[name] += 1

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(0, int((time.monotonic() - started) * 1000))

    def _record_timing(self, name: str, duration: int) -> None:
        total = name + '_ms_total'
        maximum = name + '_ms_max'
        setattr(self, total, getattr(self, total) + duration)
        setattr(self, maximum, max(getattr(self, maximum), duration))

    def _write_report(self) -> None:
        self._write_diagnostics()
        if self.state:
            self.state.write_report()

    async def handle(self, client_reader: asyncio.StreamReader,
                     client_writer: asyncio.StreamWriter) -> None:
        connection_started = time.monotonic()
        peer = client_writer.get_extra_info("peername")
        if not peer or peer[0] not in self.controller_ips:
            self._count('peer_rejected')
            client_writer.close()
            await client_writer.wait_closed()
            self._write_diagnostics()
            return
        if self.active >= MAX_CONNECTIONS:
            self._count('capacity_rejected')
            client_writer.close()
            await client_writer.wait_closed()
            self._write_diagnostics()
            return
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        self._count('accepted')
        target_writer = None
        connect_classified = False
        try:
            self._count('connect_attempted')
            connect_started = time.monotonic()
            connect = (asyncio.open_connection(self.target_host, self.target_port,
                                               limit=MAX_HTTP_HEADER + 1)
                       if self.synthetic else asyncio.open_connection(self.target_host, self.target_port))
            try:
                target_reader, target_writer = await asyncio.wait_for(connect, timeout=CONNECT_SECONDS)
            except asyncio.TimeoutError:
                connect_classified = True
                self._count('connect_timeout')
                raise
            except (OSError, ConnectionError):
                connect_classified = True
                self._count('connect_error')
                raise
            finally:
                elapsed = self._elapsed_ms(connect_started)
                self._record_timing('connect', elapsed)
            self._count('connected')
            if self.synthetic:
                await self._synthetic_connection(client_reader, client_writer, target_reader, target_writer)
            else:
                forward = asyncio.create_task(copy_stream(client_reader, target_writer))
                reverse = asyncio.create_task(copy_stream(target_reader, client_writer))
                self.tasks.update((forward, reverse))
                try:
                    done, pending = await asyncio.wait((forward, reverse), return_when=asyncio.FIRST_COMPLETED)
                    for completed in done:
                        completed.result()
                    if pending:
                        await asyncio.wait_for(asyncio.gather(*pending), timeout=IDLE_SECONDS)
                finally:
                    for task in (forward, reverse):
                        if not task.done(): task.cancel()
                        self.tasks.discard(task)
                    await asyncio.gather(forward, reverse, return_exceptions=True)
        except ValueError:
            self._count('protocol_error')
        except asyncio.TimeoutError:
            if not connect_classified:
                self._count('stream_timeout')
        except (OSError, ConnectionError, asyncio.IncompleteReadError):
            self._count('transport_error')
        except RuntimeError:
            self._count('relay_error')
        finally:
            for task in tuple(self.tasks):
                if task.done():
                    self.tasks.discard(task)
            try:
                if target_writer is not None:
                    await close_writer(target_writer)
            finally:
                await close_writer(client_writer)
                self.active -= 1
                self._count('closed')
                elapsed = self._elapsed_ms(connection_started)
                self._record_timing('connection', elapsed)
                self._write_diagnostics()

    async def sample_event_loop_lag(self, interval: float = 0.25) -> None:
        loop = asyncio.get_running_loop()
        next_tick = loop.time() + interval
        while True:
            await asyncio.sleep(max(0.0, next_tick - loop.time()))
            now = loop.time()
            lag_ms = max(0, int((now - next_tick) * 1000))
            self.loop_lag_samples += 1
            self.loop_lag_max_ms = max(self.loop_lag_max_ms, lag_ms)
            next_tick = max(next_tick + interval, now + interval)

    async def _synthetic_connection(self, client_reader, client_writer, target_reader, target_writer):
        request = await read_http_header(client_reader)
        # websockets clients commonly offer permessage-deflate. This relay
        # inspects bounded, uncompressed CDP frames and rejects RSV bits, so
        # synthetic mode must not negotiate extensions it cannot decode.
        target_writer.write(strip_websocket_extensions(request) if self.synthetic else request)
        await target_writer.drain()
        if not is_websocket_upgrade(request):
            response = await read_http_header(target_reader)
            client_writer.write(response)
            await client_writer.drain()
            # CDP discovery has no body for the endpoints used by the worker. Keep
            # normal TCP forwarding for this connection after the bounded headers.
            forward = asyncio.create_task(copy_stream(client_reader, target_writer))
            reverse = asyncio.create_task(copy_stream(target_reader, client_writer))
            try:
                done, _ = await asyncio.wait((forward, reverse), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                for task in (forward, reverse):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(forward, reverse, return_exceptions=True)
            return
        self._count('websocket_upgrade')
        response = await read_http_header(target_reader)
        client_writer.write(response)
        await client_writer.drain()
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            self._count('websocket_rejected')
            return
        pending = {}
        to_server = asyncio.create_task(websocket_copy(client_reader, target_writer,
                                                       client_side=True, state=self.state, pending=pending))
        to_client = asyncio.create_task(websocket_copy(target_reader, client_writer,
                                                       client_side=False, state=self.state, pending=pending))
        try:
            done, pending = await asyncio.wait((to_server, to_client), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in (to_server, to_client):
                if not task.done(): task.cancel()
            await asyncio.gather(to_server, to_client, return_exceptions=True)


async def serve(controller_ip: str | Iterable[str], arm_dir: Path | None = None, report: Path | None = None,
               diagnostics_report: Path | None = None, *, persistent: bool = False,
               heartbeat_path: Path | None = None) -> None:
    relay = Relay(private_controller_ips(controller_ip), arm_dir=arm_dir, report=report,
                  diagnostics_report=diagnostics_report, heartbeat_path=heartbeat_path)
    server = await asyncio.start_server(relay.handle, LISTEN_HOST, LISTEN_PORT,
                                        limit=MAX_HTTP_HEADER + 1, backlog=MAX_CONNECTIONS)
    loop = asyncio.get_running_loop()
    stopped = loop.create_future()
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, lambda: not stopped.done() and stopped.set_result(None))
        except (NotImplementedError, RuntimeError):
            pass
    serving = asyncio.create_task(server.serve_forever())
    lag_sampler = (asyncio.create_task(relay.sample_event_loop_lag())
                   if relay.diagnostics_report is not None else None)
    heartbeat_task = (asyncio.create_task(relay.heartbeat()) if relay.heartbeat_path is not None else None)
    try:
        await wait_for_shutdown(stopped, persistent=persistent)
    finally:
        server.close()
        await server.wait_closed()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
        if lag_sampler is not None:
            lag_sampler.cancel()
            await asyncio.gather(lag_sampler, return_exceptions=True)
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
    for task in tuple(relay.tasks):
        task.cancel()
    if relay.tasks:
        await asyncio.gather(*relay.tasks, return_exceptions=True)
    relay._write_report()


async def wait_for_shutdown(stopped: Awaitable[object], *, persistent: bool = False) -> None:
    """Keep production relays alive; preserve the bounded experiment default."""
    if persistent:
        await stopped
        return
    try:
        await asyncio.wait_for(stopped, timeout=LIFETIME_SECONDS)
    except asyncio.TimeoutError:
        pass


def write_heartbeat(path: Path | None, pid: int, *, monotonic_ns: int | None = None) -> None:
    """Atomically publish a bounded heartbeat without following links."""
    if path is None:
        return
    parent_fd = open_heartbeat_directory(path.parent)
    temporary = f".{path.name}.{pid}.{secrets.token_hex(6)}.tmp"
    try:
        parent = os.fstat(parent_fd)
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != HEARTBEAT_DIRECTORY_UID
                or stat.S_IMODE(parent.st_mode) != 0o733):
            raise OSError("heartbeat_directory_invalid")
        try:
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            current = None
        if current is not None and (
                not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                or current.st_uid != os.geteuid() or current.st_mode & 0o022):
            raise OSError("heartbeat_file_invalid")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_NONBLOCK,
                     0o644, dir_fd=parent_fd)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.geteuid() or info.st_mode & 0o022):
                raise OSError("heartbeat_file_invalid")
            os.fchmod(fd, 0o644)
            payload = json.dumps({"pid": pid, "monotonic_ns": time.monotonic_ns()
                                  if monotonic_ns is None else monotonic_ns},
                                 separators=(",", ":")).encode("ascii")
            remaining = memoryview(payload)
            while remaining:
                remaining = remaining[os.write(fd, remaining):]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    except BaseException:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(parent_fd)


def open_heartbeat_directory(path: Path) -> int:
    """Open the 0733 heartbeat directory without requiring directory read access."""
    flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    return os.open(path, flags)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--controller-ip", action="append", required=True,
                        help="one exact private IPv4 controller (repeat for additional known clients)")
    parser.add_argument("--synthetic-arm-dir", type=Path)
    parser.add_argument("--synthetic-report", type=Path)
    parser.add_argument("--synthetic-diagnostics-report", type=Path)
    parser.add_argument("--persistent", action="store_true",
                        help="serve until SIGTERM/SIGINT instead of the bounded experiment lifetime")
    parser.add_argument("--heartbeat-path", type=Path,
                        help="owner-only event-loop heartbeat path for the persistent container relay")
    args = parser.parse_args(argv)
    if bool(args.synthetic_arm_dir) != bool(args.synthetic_report):
        parser.error("synthetic arm and report paths must be provided together")
    if args.synthetic_diagnostics_report and not args.synthetic_arm_dir:
        parser.error("synthetic diagnostics require synthetic mode")
    if args.persistent and args.synthetic_arm_dir:
        parser.error("persistent mode cannot be combined with synthetic mode")
    if args.heartbeat_path and (not args.persistent or args.synthetic_arm_dir):
        parser.error("heartbeat requires persistent production mode")
    try:
        controllers = private_controller_ips(args.controller_ip)
    except ValueError as exc:
        parser.error(str(exc))
    asyncio.run(serve(controllers, args.synthetic_arm_dir, args.synthetic_report,
                      args.synthetic_diagnostics_report, persistent=args.persistent,
                      heartbeat_path=args.heartbeat_path))


if __name__ == "__main__":
    main()
