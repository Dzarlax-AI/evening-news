"""Isolated launcher argument and CDP relay boundary checks."""

import asyncio
import base64
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


launcher = load("launch_shadow_cloak", "scripts/launch_shadow_cloak.py")
relay = load("shadow_cdp_relay", "scripts/shadow_cdp_relay.py")


def test_relay_persistent_waits_for_signal_but_default_lifetime_stays_bounded(monkeypatch):
    async def scenario():
        monkeypatch.setattr(relay, "LIFETIME_SECONDS", 0)
        stopped = asyncio.Event()
        await relay.wait_for_shutdown(stopped.wait())
        assert not stopped.is_set()

        persistent_stop = asyncio.Event()
        asyncio.get_running_loop().call_soon(persistent_stop.set)
        await relay.wait_for_shutdown(persistent_stop.wait(), persistent=True)
        assert persistent_stop.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize(("extra", "expected"), [([], False), (["--persistent"], True)])
def test_relay_cli_forwards_persistent_mode(monkeypatch, extra, expected):
    captured = []

    def capture(coroutine):
        captured.append(coroutine)

    monkeypatch.setattr(relay.asyncio, "run", capture)
    monkeypatch.setattr(sys, "argv", ["shadow_cdp_relay.py", "--controller-ip", "10.55.0.3", *extra])
    relay.main()
    assert len(captured) == 1
    assert captured[0].cr_frame.f_locals["persistent"] is expected
    captured[0].close()


def test_relay_cli_rejects_persistent_synthetic_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", ["shadow_cdp_relay.py", "--controller-ip", "10.55.0.3",
                                       "--persistent", "--synthetic-arm-dir", str(tmp_path),
                                       "--synthetic-report", str(tmp_path / "report.json")])
    with pytest.raises(SystemExit) as error:
        relay.main()
    assert error.value.code == 2


def test_relay_cli_forwards_heartbeat_only_for_persistent_mode(monkeypatch, tmp_path):
    captured = []
    monkeypatch.setattr(relay.asyncio, "run", captured.append)
    heartbeat = tmp_path / "heartbeat.json"
    monkeypatch.setattr(sys, "argv", ["shadow_cdp_relay.py", "--controller-ip", "10.55.0.3",
                                       "--persistent", "--heartbeat-path", str(heartbeat)])
    relay.main()
    assert len(captured) == 1
    assert captured[0].cr_frame.f_locals["heartbeat_path"] == heartbeat
    captured[0].close()

    monkeypatch.setattr(sys, "argv", ["shadow_cdp_relay.py", "--controller-ip", "10.55.0.3",
                                       "--heartbeat-path", str(heartbeat)])
    with pytest.raises(SystemExit) as error:
        relay.main()
    assert error.value.code == 2


def test_heartbeat_publishes_complete_file_by_atomic_replace(tmp_path, monkeypatch):
    directory = tmp_path / "relay"
    directory.mkdir(mode=0o700)
    directory.chmod(0o733)
    heartbeat = directory / "heartbeat.json"
    old = b'{"pid":1,"monotonic_ns":1}'
    heartbeat.write_bytes(old)
    heartbeat.chmod(0o644)
    monkeypatch.setattr(relay, "HEARTBEAT_DIRECTORY_UID", os.geteuid())

    real_replace = os.replace
    observations = []

    def check_then_replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
        # Until the single rename, readers still see the prior complete record.
        assert heartbeat.read_bytes() == old
        temp = directory / src
        payload = json.loads(temp.read_text())
        assert payload == {"pid": 4321, "monotonic_ns": 987654321}
        observations.append(payload)
        return real_replace(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(relay.os, "replace", check_then_replace)
    relay.write_heartbeat(heartbeat, 4321, monotonic_ns=987654321)

    assert observations == [{"pid": 4321, "monotonic_ns": 987654321}]
    assert json.loads(heartbeat.read_text()) == observations[0]
    assert sorted(item.name for item in directory.iterdir()) == ["heartbeat.json"]


def test_heartbeat_opens_search_only_directory_without_read_permission(tmp_path, monkeypatch):
    directory = tmp_path / "relay"
    directory.mkdir(mode=0o700)
    directory.chmod(0o733)
    monkeypatch.setattr(relay, "HEARTBEAT_DIRECTORY_UID", os.geteuid())
    flags_seen = []
    real_open = os.open

    def capture_open(path, flags, *args, **kwargs):
        if Path(path) == directory:
            flags_seen.append(flags)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(relay.os, "open", capture_open)
    fd = relay.open_heartbeat_directory(directory)
    try:
        info = os.fstat(fd)
        assert stat.S_ISDIR(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o733
        # Linux uses O_PATH so uid 65534 can traverse this directory without
        # acquiring the directory-read permission that mode 0733 omits.
        if hasattr(os, "O_PATH"):
            assert flags_seen and flags_seen[0] & os.O_PATH
    finally:
        os.close(fd)


def test_launcher_uses_only_pinned_args_and_fixed_debug_binding(tmp_path):
    binary = tmp_path / "chrome"
    binary.write_bytes(b"pinned browser fixture")
    calls = []

    def build_args(**kwargs):
        calls.append(kwargs)
        return ["--headless=new", "--fixed-cloak-flag"]

    definitions = {"ensure_binary": lambda: str(binary),
                   "BASE_CHROME_ARGS": ["--no-first-run"], "build_args": build_args}
    digest = base64.b64encode(b"x" * 32).decode()
    selected, argv = launcher.launch_spec(definitions, digest)
    assert selected == binary
    assert calls == [{"stealth_args": True, "extra_args": ["--fingerprint=57292"],
                      "timezone": None, "locale": None, "headless": True}]
    assert argv == ["--no-first-run", "--headless=new", "--fixed-cloak-flag",
                    f"--ignore-certificate-errors-spki-list={digest}",
                    "--remote-debugging-port=5100", "--remote-debugging-address=127.0.0.1",
                    "--user-data-dir=/tmp/native-shadow-browser"]
    output = launcher.write_launch_info(tmp_path, selected, argv)
    info = json.loads(output.read_text())
    assert info["args"] == argv
    assert len(info["binary_sha256"]) == 64
    with pytest.raises(FileExistsError):
        launcher.write_launch_info(tmp_path, selected, argv)


def test_launcher_rejects_conflicting_pinned_args_and_bad_spki(tmp_path):
    binary = tmp_path / "chrome"
    binary.write_bytes(b"x")
    base = {"ensure_binary": lambda: str(binary), "BASE_CHROME_ARGS": [],
            "build_args": lambda **kwargs: ["--remote-debugging-port=9222"]}
    with pytest.raises(RuntimeError, match="conflict"):
        launcher.launch_spec(base)
    base["build_args"] = lambda **kwargs: []
    with pytest.raises(ValueError, match="SPKI"):
        launcher.launch_spec(base, "not-base64")
    assert not any("spki" in arg for arg in launcher.launch_spec(base)[1])


@pytest.mark.parametrize("address", ["127.0.0.1", "8.8.8.8", "169.254.1.1", "localhost", "::1"])
def test_controller_must_be_private_ipv4(address):
    with pytest.raises(ValueError):
        relay.private_controller_ip(address)
    assert relay.private_controller_ip("10.55.0.3") == "10.55.0.3"


def test_relay_denies_wrong_peer_and_connection_cap_without_target_dial(monkeypatch):
    async def scenario():
        calls = []

        async def dial(*args, **kwargs):
            calls.append(True)
            raise AssertionError("target dial not allowed")

        monkeypatch.setattr(relay.asyncio, "open_connection", dial)

        class FakeWriter:
            def __init__(self, peer):
                self.peer = peer
                self.closed = False

            def get_extra_info(self, name):
                return (self.peer, 1234) if name == "peername" else None

            def close(self):
                self.closed = True

            async def wait_closed(self):
                pass

        boundary = relay.Relay("10.55.0.3")
        wrong = FakeWriter("10.55.0.4")
        await boundary.handle(asyncio.StreamReader(), wrong)
        assert wrong.closed
        boundary.active = relay.MAX_CONNECTIONS
        full = FakeWriter("10.55.0.3")
        await boundary.handle(asyncio.StreamReader(), full)
        assert full.closed
        assert not calls

    asyncio.run(scenario())


def test_relay_forwards_both_directions_and_drains():
    async def scenario():
        async def target(reader, writer):
            data = await reader.read(100)
            writer.write(b"reply:" + data)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        target_server = await asyncio.start_server(target, "127.0.0.1", 0)
        target_port = target_server.sockets[0].getsockname()[1]
        boundary = relay.Relay("127.0.0.1", target_port=target_port, allow_loopback_test_peer=True)
        front = await asyncio.start_server(boundary.handle, "127.0.0.1", 0)
        front_port = front.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", front_port)
            writer.write(b"hello")
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(100), timeout=2) == b"reply:hello"
            writer.close()
            await writer.wait_closed()
            for _ in range(20):
                if boundary.active == 0:
                    break
                await asyncio.sleep(0.01)
            assert boundary.active == 0
        finally:
            front.close()
            target_server.close()
            await front.wait_closed()
            await target_server.wait_closed()

    asyncio.run(scenario())


def test_relay_active_count_resets_when_target_close_raises(monkeypatch):
    async def scenario():
        class Writer:
            def __init__(self, peer, fail=False):
                self.peer = peer
                self.fail = fail
                self.closed = False

            def get_extra_info(self, name):
                return (self.peer, 1234) if name == "peername" else None

            def can_write_eof(self):
                return False

            def close(self):
                self.closed = True

            async def wait_closed(self):
                if self.fail:
                    raise OSError("simulated reset")

        async def dial(*args):
            reader = asyncio.StreamReader()
            reader.feed_eof()
            return reader, target

        target = Writer("127.0.0.1", fail=True)
        client = Writer("10.55.0.3")
        reader = asyncio.StreamReader()
        reader.feed_eof()
        monkeypatch.setattr(relay.asyncio, "open_connection", dial)
        boundary = relay.Relay("10.55.0.3")
        await boundary.handle(reader, client)
        assert target.closed and client.closed
        assert boundary.active == 0

    asyncio.run(scenario())


def test_relay_target_connect_timeout_releases_slot(monkeypatch):
    async def scenario():
        class Writer:
            def __init__(self):
                self.closed = False

            def get_extra_info(self, name):
                return ("10.55.0.3", 1234) if name == "peername" else None

            def close(self):
                self.closed = True

            async def wait_closed(self):
                pass

        async def blocked_dial(*args):
            await asyncio.Event().wait()

        monkeypatch.setattr(relay.asyncio, "open_connection", blocked_dial)
        monkeypatch.setattr(relay, "CONNECT_SECONDS", 0.01)
        boundary = relay.Relay("10.55.0.3")
        writer = Writer()
        await asyncio.wait_for(boundary.handle(asyncio.StreamReader(), writer), timeout=0.5)
        assert writer.closed
        assert boundary.active == 0

    asyncio.run(scenario())
