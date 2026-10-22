from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from scripts import shadow_cdp_relay as relay


def masked_frame(opcode: int, payload: bytes, *, fin: bool = True, mask: bytes = b"abcd") -> bytes:
    first = (0x80 if fin else 0) | opcode
    size = len(payload)
    if size < 126:
        header = bytes((first, 0x80 | size))
    elif size <= 65535:
        header = bytes((first, 0x80 | 126)) + size.to_bytes(2, "big")
    else:
        header = bytes((first, 0x80 | 127)) + size.to_bytes(8, "big")
    return header + mask + bytes(byte ^ mask[index & 3] for index, byte in enumerate(payload))


async def write_upgrade(writer: asyncio.StreamWriter) -> None:
    writer.write(b"GET /devtools/page/test HTTP/1.1\r\nHost: fixture\r\nUpgrade: websocket\r\n"
                 b"Connection: keep-alive, Upgrade\r\n\r\n")
    await writer.drain()


class RelayTests(unittest.IsolatedAsyncioTestCase):
    def test_private_ip_gate(self):
        self.assertEqual(relay.private_controller_ip("10.4.0.3"), "10.4.0.3")
        for value in ("127.0.0.1", "8.8.8.8", "::1", "localhost"):
            with self.assertRaises(ValueError):
                relay.private_controller_ip(value)

    def test_frame_length_and_direction_are_rejected(self):
        async def scenario():
            reader = asyncio.StreamReader()
            reader.feed_data(b"\x81\xff\x00\x00\x00\x00\x00\x10\x00\x01")
            with self.assertRaises(ValueError):
                await relay.read_frame(reader, expect_mask=True)
            reader = asyncio.StreamReader()
            reader.feed_data(b"\x81\x01x")
            with self.assertRaises(ValueError):
                await relay.read_frame(reader, expect_mask=True)
            reader = asyncio.StreamReader()
            compressed = masked_frame(1, b"x")
            reader.feed_data(bytes((compressed[0] | 0x40,)) + compressed[1:])
            with self.assertRaises(ValueError):
                await relay.read_frame(reader, expect_mask=True)
        asyncio.run(scenario())

    def test_extension_stripping_preserves_other_handshake_headers(self):
        header = (b"GET /devtools/browser/test HTTP/1.1\r\nHost: local\r\n"
                  b"Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n"
                  b"Sec-WebSocket-Protocol: cdp\r\n\r\n")
        stripped = relay.strip_websocket_extensions(header)
        self.assertNotIn(b"Sec-WebSocket-Extensions", stripped)
        self.assertIn(b"Host: local", stripped)
        self.assertIn(b"Sec-WebSocket-Protocol: cdp", stripped)

    async def test_default_websockets_client_works_without_negotiated_compression(self):
        from websockets.asyncio.client import connect
        from websockets.asyncio.server import serve

        observed = {"extension_offer_removed": False}
        large_result = {"id": 1, "result": {"payload": "x" * 200_000}}

        async def fixture(connection):
            offer = connection.request.headers.get("Sec-WebSocket-Extensions")
            observed["extension_offer_removed"] = offer is None
            command = json.loads(await connection.recv())
            self.assertEqual(command, {"id": 1, "method": "Target.getBrowserContexts"})
            await connection.send(json.dumps(large_result, separators=(",", ":")))

        async with serve(fixture, "127.0.0.1", 0) as upstream:
            upstream_port = upstream.sockets[0].getsockname()[1]
            target = relay.Relay("127.0.0.1", "127.0.0.1", upstream_port,
                                 arm_dir=Path(tempfile.gettempdir()))
            front = await asyncio.start_server(target.handle, "127.0.0.1", 0)
            front_port = front.sockets[0].getsockname()[1]
            try:
                async with connect(f"ws://127.0.0.1:{front_port}/devtools/browser/test",
                                   proxy=None, open_timeout=3, close_timeout=1) as client:
                    # websockets defaults to offering permessage-deflate. The relay
                    # strips that offer, so the frame inspector sees ordinary frames.
                    self.assertIn("permessage-deflate", client.request.headers.get(
                        "Sec-WebSocket-Extensions", ""))
                    self.assertEqual(client.protocol.extensions, [])
                    await client.send(json.dumps(
                        {"id": 1, "method": "Target.getBrowserContexts"}, separators=(",", ":")))
                    response = json.loads(await asyncio.wait_for(client.recv(), timeout=3))
                    self.assertEqual(response, large_result)
                self.assertTrue(observed["extension_offer_removed"])
            finally:
                front.close()
                await front.wait_closed()

    async def test_fragment_ping_and_close_are_forwarded(self):
        client_in = asyncio.StreamReader()

        class CaptureWriter:
            def __init__(self): self.data = bytearray()
            def write(self, value): self.data.extend(value)
            async def drain(self): pass

        writer = CaptureWriter()
        state = relay.SyntheticState(None, None)
        client_in.feed_data(masked_frame(1, b'{"method":', fin=False))
        client_in.feed_data(masked_frame(9, b"p"))
        client_in.feed_data(masked_frame(0, b'"x"}', fin=True))
        client_in.feed_data(masked_frame(8, b""))
        await relay.websocket_copy(client_in, writer, client_side=True, state=state, pending={})
        self.assertEqual(bytes(writer.data), masked_frame(1, b'{"method":', fin=False)
                         + masked_frame(9, b"p") + masked_frame(0, b'"x"}') + masked_frame(8, b""))

    async def test_synthetic_connect_timeout_writes_private_finite_diagnostics(self):
        class ClientWriter:
            def __init__(self):
                self.closed = False
            def get_extra_info(self, name):
                return ('10.0.0.8', 1234) if name == 'peername' else None
            def close(self):
                self.closed = True
            async def wait_closed(self):
                return None

        async def timeout(*_args, **_kwargs):
            raise asyncio.TimeoutError

        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / 'relay-diagnostics.json'
            relay_instance = relay.Relay(
                '10.0.0.8', 'secret.internal', 5100,
                arm_dir=Path(temp) / 'arm', diagnostics_report=report,
            )
            client_writer = ClientWriter()
            with patch.object(relay.asyncio, 'open_connection', side_effect=timeout):
                await relay_instance.handle(asyncio.StreamReader(), client_writer)
            value = json.loads(report.read_text())
            self.assertEqual(value['schema_version'], 1)
            self.assertEqual(value['counters']['connect_attempted'], 1)
            self.assertEqual(value['counters']['connect_timeout'], 1)
            self.assertEqual(value['counters']['connected'], 0)
            self.assertEqual(value['counters']['closed'], 1)
            self.assertEqual(value['active'], 0)
            self.assertEqual(value['peak_active'], 1)
            self.assertEqual(value['event_loop_lag_ms'], {'samples': 0, 'max': 0})
            self.assertEqual(report.stat().st_mode & 0o777, 0o600)
            self.assertNotIn('secret.internal', report.read_text())

    def test_tcp_listener_metrics_keep_only_finite_kernel_counters(self):
        sample = ('TcpExt: SyncookiesSent ListenOverflows ListenDrops TCPReqQFullDrop Other\n'
                  'TcpExt: 2 3 4 5 99\n')
        with patch.object(relay.Path, 'read_text', return_value=sample):
            self.assertEqual(relay.Relay._tcp_listener_counters(), {
                'ListenOverflows': 3, 'ListenDrops': 4, 'TCPReqQFullDrop': 5,
            })

    def test_tcp_connection_metrics_keep_only_fixed_kernel_counters(self):
        sample = ('Ip: Forwarding DefaultTTL\nIp: 1 64\n'
                  'Tcp: RtoAlgorithm ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs '
                  'RetransSegs InErrs OutRsts Other\n'
                  'Tcp: 1 11 12 13 14 15 16 17 18 19 20 999\n')
        with patch.object(relay.Path, 'read_text', return_value=sample):
            self.assertEqual(relay.Relay._tcp_connection_counters(), {
                'ActiveOpens': 11, 'AttemptFails': 13, 'RetransSegs': 18, 'InErrs': 19, 'OutRsts': 20,
            })

    async def test_synthetic_loop_lag_sampler_records_bounded_metrics(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / 'relay-diagnostics.json'
            instance = relay.Relay('10.0.0.8', arm_dir=Path(temp) / 'arm', diagnostics_report=report)
            sampler = asyncio.create_task(instance.sample_event_loop_lag(0.001))
            await asyncio.sleep(0.006)
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
            instance._write_diagnostics()
            metrics = json.loads(report.read_text())['event_loop_lag_ms']
            self.assertGreaterEqual(metrics['samples'], 1)
            self.assertGreaterEqual(metrics['max'], 0)

    async def test_barrier_injects_exact_257_before_owned_cleanup_reply_and_reports_no_ids(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            arm = root / "arm"
            arm.mkdir()
            report = root / "report.json"
            state = relay.SyntheticState(arm, report)

            class CaptureWriter:
                def __init__(self): self.data = bytearray()
                def write(self, value): self.data.extend(value)
                async def drain(self): pass

            # Command direction reads fragmented, masked client messages.
            command_id = 55421
            command = json.dumps({"id": command_id, "method": "Target.disposeBrowserContext",
                                  "params": {"browserContextId": "owned-context"}},
                                 separators=(",", ":")).encode()
            cut = len(command) // 2
            incoming = asyncio.StreamReader()
            incoming.feed_data(masked_frame(1, command[:cut], fin=False))
            incoming.feed_data(masked_frame(9, b"heartbeat"))
            incoming.feed_data(masked_frame(0, command[cut:]))
            incoming.feed_data(masked_frame(8, b""))
            arm_file = arm / relay.ARM_NAME
            arm_file.write_text('{"arm":"overload-once"}')
            os.chmod(arm_file, 0o600)
            with patch.object(Path, "stat", return_value=SimpleNamespace(
                    st_mode=0o100600, st_uid=0, st_size=24)):
                self.assertTrue(state.poll_arm())
            pending = {}
            state.on_command({"id": 1, "method": "Target.createBrowserContext"}, pending)
            state.on_reply({"id": 1, "result": {"browserContextId": "owned-context"}}, pending)
            state.on_command({"id": 2, "method": "Target.createTarget",
                              "params": {"browserContextId": "owned-context"}}, pending)
            state.on_reply({"id": 2, "result": {"targetId": "owned-target"}}, pending)
            state.on_command({"id": 3, "method": "Target.attachToTarget",
                              "params": {"targetId": "owned-target"}}, pending)
            state.on_reply({"id": 3, "result": {"sessionId": "SECRET-SESSION"}}, pending)
            self.assertTrue(arm_file.exists())
            client_capture = CaptureWriter()
            await relay.websocket_copy(incoming, client_capture, client_side=True, state=state, pending=pending)
            self.assertNotIn(b"SECRET-SESSION", client_capture.data)

            reply = asyncio.StreamReader()
            actual_reply = json.dumps({"id": command_id, "result": {}}, separators=(",", ":")).encode()
            reply.feed_data(relay.encode_server_text(actual_reply))
            reply.feed_data(b"\x88\x00")
            server_capture = CaptureWriter()
            await relay.websocket_copy(reply, server_capture, client_side=False, state=state, pending=pending)

            parsed = []
            stream = asyncio.StreamReader()
            stream.feed_data(server_capture.data)
            while stream._buffer:
                _, opcode, _, payload = await relay.read_frame(stream, expect_mask=False)
                if opcode == 1:
                    parsed.append(json.loads(payload))
            self.assertEqual(state.notifications_injected, 257)
            self.assertEqual(sum(item.get("method") == "Network.dataReceived" for item in parsed), 257)
            self.assertEqual(parsed[-1], {"id": command_id, "result": {}})
            self.assertEqual(parsed[0]["sessionId"], "SECRET-SESSION")
            state.report = report
            state.write_report()
            output = report.read_text()
            self.assertNotIn("SECRET-SESSION", output)
            self.assertNotIn(str(command_id), output)
            self.assertEqual(json.loads(output)["notifications_injected"], 257)

    async def test_peer_restriction_closes_untrusted_connection(self):
        class FakeWriter:
            closed = False
            def get_extra_info(self, key): return ("10.0.0.9", 1234) if key == "peername" else None
            def close(self): self.closed = True
            async def wait_closed(self): pass
        target = relay.Relay("10.0.0.8")
        writer = FakeWriter()
        await target.handle(asyncio.StreamReader(), writer)
        self.assertTrue(writer.closed)
        self.assertEqual(target.active, 0)

    def test_overload_barrier_requires_successful_owned_cleanup(self):
        state = relay.SyntheticState(None, None)
        state.armed = True
        pending = {}
        state.on_command({"id": 1, "method": "Target.createBrowserContext"}, pending)
        state.on_reply({"id": 1, "result": {"browserContextId": "owned"}}, pending)
        state.on_command({"id": 2, "method": "Target.createTarget",
                          "params": {"browserContextId": "owned"}}, pending)
        state.on_reply({"id": 2, "result": {"targetId": "target"}}, pending)
        state.on_command({"id": 3, "method": "Target.attachToTarget",
                          "params": {"targetId": "target"}}, pending)
        state.on_reply({"id": 3, "result": {"sessionId": "session"}}, pending)
        state.on_command({"id": 4, "method": "Fetch.continueRequest", "sessionId": "session"}, pending)
        fetch_reply = state.on_reply({"id": 4, "result": {}}, pending)
        self.assertFalse(state.reserve_barrier(fetch_reply))
        state.on_command({"id": 5, "method": "Target.disposeBrowserContext",
                          "params": {"browserContextId": "foreign"}}, pending)
        self.assertIsNone(state.on_reply({"id": 5, "result": {}}, pending))
        state.on_command({"id": 6, "method": "Target.disposeBrowserContext",
                          "params": {"browserContextId": "owned"}}, pending)
        failed = state.on_reply({"id": 6, "error": {"code": -1}}, pending)
        self.assertFalse(state.reserve_barrier(failed))
        state.on_command({"id": 7, "method": "Target.disposeBrowserContext",
                          "params": {"browserContextId": "owned"}}, pending)
        successful = state.on_reply({"id": 7, "result": {}}, pending)
        self.assertTrue(state.reserve_barrier(successful))
        self.assertFalse(state.reserve_barrier(successful))

    def test_cleanup_receipt_requires_owned_ids_and_post_barrier_queries(self):
        state = relay.SyntheticState(None, None)
        state.armed = True
        pending = {}
        state.on_command({"id": 1, "method": "Target.createBrowserContext"}, pending)
        state.on_reply({"id": 1, "result": {"browserContextId": "ctx-owned"}}, pending)
        state.on_command({"id": 2, "method": "Target.createTarget",
                          "params": {"browserContextId": "ctx-owned"}}, pending)
        state.on_reply({"id": 2, "result": {"targetId": "target-owned"}}, pending)

        # Results from lookups issued before overload cannot establish cleanup.
        state.on_command({"id": 3, "method": "Target.getBrowserContexts"}, pending)
        state.on_command({"id": 4, "method": "Target.getTargets"}, pending)
        state.consumed = True
        state.barriers = 1
        state.on_command({"id": 5, "method": "Target.disposeBrowserContext",
                          "params": {"browserContextId": "ctx-owned"}}, pending)
        state.on_reply({"id": 5, "result": {}}, pending)
        state.on_reply({"id": 3, "result": {"browserContextIds": []}}, pending)
        state.on_reply({"id": 4, "result": {"targetInfos": []}}, pending)
        self.assertEqual(state.context_absent, 0)
        self.assertEqual(state.target_absent, 0)

        state.on_command({"id": 7, "method": "Target.getBrowserContexts"}, pending)
        state.on_command({"id": 8, "method": "Target.getTargets"}, pending)
        state.on_reply({"id": 7, "result": {"browserContextIds": []}}, pending)
        state.on_reply({"id": 8, "result": {"targetInfos": []}}, pending)
        self.assertEqual(state.context_absent, 1)
        self.assertEqual(state.target_absent, 1)
        self.assertTrue(state.cleanup_contexts_replied >= state.contexts)
        with tempfile.TemporaryDirectory() as temp:
            state.report = Path(temp)/"metrics.json"
            state.write_report()
            report = state.report.read_text()
            value = json.loads(report)
            self.assertTrue(value["cleanup_confirmed"])
            self.assertEqual(value["cleanup_requests"], 1)
            self.assertEqual(value["cleanup_replies"], 1)
            self.assertNotIn("ctx-owned", report)
            self.assertNotIn("target-owned", report)

    def test_cdp_command_ids_can_repeat_on_separate_connections(self):
        state = relay.SyntheticState(None, None)
        state.armed = True
        first_pending, second_pending = {}, {}
        state.on_command({"id": 1, "method": "Target.createBrowserContext"}, first_pending)
        state.on_reply({"id": 1, "result": {"browserContextId": "ctx-one"}}, first_pending)
        state.on_command({"id": 1, "method": "Target.createBrowserContext"}, second_pending)
        state.on_reply({"id": 1, "result": {"browserContextId": "ctx-two"}}, second_pending)
        self.assertEqual(state.contexts, {"ctx-one", "ctx-two"})

    async def test_loopback_discovery_and_websocket_barrier(self):
        async def fixture(reader, writer):
            try:
                header = await relay.read_http_header(reader)
                if not relay.is_websocket_upgrade(header):
                    body = b'{"Browser":"fixture"}'
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode()
                                 + b"\r\nConnection: close\r\n\r\n" + body)
                    await writer.drain()
                    writer.write_eof()
                    return
                writer.write(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                             b"Connection: Upgrade\r\n\r\n")
                await writer.drain()
                for _ in range(4):
                    _, opcode, _, payload = await relay.read_frame(reader, expect_mask=True)
                    command = json.loads(payload)
                    replies = {
                        "Target.createBrowserContext": {"browserContextId": "ctx-1"},
                        "Target.createTarget": {"targetId": "target-1"},
                        "Target.attachToTarget": {"sessionId": "session-1"},
                    }
                    if command["method"] == "Target.disposeBrowserContext":
                        self.assertEqual(command["params"]["browserContextId"], "ctx-1")
                        reply_result = {}
                    else:
                        reply_result = replies[command["method"]]
                    writer.write(relay.encode_server_text(json.dumps(
                        {"id": command["id"], "result": reply_result}, separators=(",", ":")).encode()))
                    await writer.drain()
                _, opcode, _, _ = await relay.read_frame(reader, expect_mask=True)
                self.assertEqual(opcode, 8)
                writer.write(b"\x88\x00")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        fixture_server = await asyncio.start_server(fixture, "127.0.0.1", 0)
        fixture_port = fixture_server.sockets[0].getsockname()[1]
        with tempfile.TemporaryDirectory() as temp:
            target = relay.Relay("127.0.0.1", "127.0.0.1", fixture_port,
                                 arm_dir=Path(temp), report=Path(temp)/"report.json")
            target.state.armed = True
            front = await asyncio.start_server(target.handle, "127.0.0.1", 0)
            front_port = front.sockets[0].getsockname()[1]
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", front_port)
                writer.write(b"GET /json/version HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n")
                await writer.drain()
                discovery = await asyncio.wait_for(reader.read(), timeout=2)
                self.assertIn(b"{\"Browser\":\"fixture\"}", discovery)
                writer.close()
                await writer.wait_closed()

                reader, writer = await asyncio.open_connection("127.0.0.1", front_port)
                await write_upgrade(writer)
                response = await asyncio.wait_for(relay.read_http_header(reader), timeout=2)
                self.assertIn(b"101 Switching Protocols", response)
                commands = [
                    {"id": 17, "method": "Target.createBrowserContext"},
                    {"id": 18, "method": "Target.createTarget",
                     "params": {"browserContextId": "ctx-1"}},
                    {"id": 19, "method": "Target.attachToTarget",
                     "params": {"targetId": "target-1"}},
                ]
                expected = [{"browserContextId": "ctx-1"}, {"targetId": "target-1"},
                            {"sessionId": "session-1"}]
                for command, reply_result in zip(commands, expected):
                    writer.write(masked_frame(1, json.dumps(command, separators=(",", ":")).encode()))
                    await writer.drain()
                    _, opcode, _, payload = await asyncio.wait_for(
                        relay.read_frame(reader, expect_mask=False), timeout=3)
                    self.assertEqual(json.loads(payload), {"id": command["id"], "result": reply_result})
                message = json.dumps({"id": 20, "method": "Target.disposeBrowserContext",
                                      "params": {"browserContextId": "ctx-1"}}, separators=(",", ":")).encode()
                writer.write(masked_frame(1, message))
                await writer.drain()
                frames = []
                for _ in range(258):
                    _, opcode, _, payload = await asyncio.wait_for(
                        relay.read_frame(reader, expect_mask=False), timeout=3)
                    if opcode == 1:
                        frames.append(json.loads(payload))
                self.assertEqual(sum(row.get("method") == "Network.dataReceived" for row in frames), 257)
                self.assertEqual(frames[-1], {"id": 20, "result": {}})
                writer.write(masked_frame(8, b""))
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.assertEqual(target.state.notifications_injected, 257)
            finally:
                front.close()
                fixture_server.close()
                await front.wait_closed()
                await fixture_server.wait_closed()


if __name__ == "__main__":
    unittest.main()
