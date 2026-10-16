"""Isolated launcher argument and CDP relay boundary checks."""

import asyncio
import base64
import importlib.util
import json
from pathlib import Path
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
        boundary = relay.Relay("127.0.0.1", target_port=target_port)
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
