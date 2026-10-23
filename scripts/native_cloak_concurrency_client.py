"""Private command-loop for the isolated native Cloak concurrency harness."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import struct
import threading
import time
import urllib.error
import urllib.request
import uuid


CLIENT_ERROR_CODES = {
    "cdp_handshake_closed", "cdp_handshake_rejected", "cdp_handshake_invalid",
    "cdp_connection_closed", "cdp_message_too_large", "cdp_command_failed",
    "cdp_discovery_invalid", "peer_context_create_failed", "peer_target_create_failed",
    "peer_attach_failed", "peer_target_or_context_lost", "peer_session_probe_failed",
    "news_browser_resource_leak", "worker_response_invalid", "one_slot_overlap_detected",
    "two_slot_barrier_or_third_waiter_invalid", "one_slot_fixture_evidence_invalid",
    "two_slot_fixture_evidence_invalid", "fixture_arrival_timeout", "origin_state_timeout",
    "cancellation_isolation_failed", "upstream_error_classification_invalid",
    "synthetic_article_request_failed", "client_command_invalid",
    "cdp_timeout_browser_get_version", "cdp_timeout_target_create_browser_context",
    "cdp_timeout_target_create_target", "cdp_timeout_target_attach_to_target",
    "cdp_timeout_target_get_targets", "cdp_timeout_target_get_browser_contexts",
    "cdp_timeout_runtime_evaluate", "cdp_timeout_target_dispose_browser_context",
    "cdp_timeout_other",
}
CDP_TIMEOUT_CODES = {
    "Browser.getVersion": "cdp_timeout_browser_get_version",
    "Target.createBrowserContext": "cdp_timeout_target_create_browser_context",
    "Target.createTarget": "cdp_timeout_target_create_target",
    "Target.attachToTarget": "cdp_timeout_target_attach_to_target",
    "Target.getTargets": "cdp_timeout_target_get_targets",
    "Target.getBrowserContexts": "cdp_timeout_target_get_browser_contexts",
    "Runtime.evaluate": "cdp_timeout_runtime_evaluate",
    "Target.disposeBrowserContext": "cdp_timeout_target_dispose_browser_context",
}


class CDP:
    def __init__(self, endpoint: str):
        parsed = endpoint.removeprefix("http://").split("/", 1)
        hostport = parsed[0]
        host, port = hostport.rsplit(":", 1)
        self.socket = socket.create_connection((host, int(port)), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        path = "/" + parsed[1] if len(parsed) == 2 else "/"
        request = (f"GET {path} HTTP/1.1\r\nHost: {hostport}\r\nUpgrade: websocket\r\n"
                   f"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n\r\n")
        self.socket.sendall(request.encode())
        response = bytearray()
        while b"\r\n\r\n" not in response and len(response) < 16384:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise RuntimeError("cdp_handshake_closed")
            response.extend(chunk)
        head = bytes(response).split(b"\r\n\r\n", 1)[0]
        if not head.startswith(b"HTTP/1.1 101"):
            raise RuntimeError("cdp_handshake_rejected")
        expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
        if b"sec-websocket-accept: " + expected.lower() not in head.lower():
            raise RuntimeError("cdp_handshake_invalid")
        self.sequence = 0

    def _read_exact(self, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            chunk = self.socket.recv(count - len(chunks))
            if not chunk:
                raise RuntimeError("cdp_connection_closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def _send(self, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        mask = os.urandom(4)
        size = len(payload)
        if size < 126:
            header = bytes((0x81, 0x80 | size))
        elif size < 65536:
            header = bytes((0x81, 0xFE)) + struct.pack("!H", size)
        else:
            header = bytes((0x81, 0xFF)) + struct.pack("!Q", size)
        encoded = bytes(byte ^ mask[index & 3] for index, byte in enumerate(payload))
        self.socket.sendall(header + mask + encoded)

    def _receive(self) -> dict:
        first, second = self._read_exact(2)
        opcode = first & 0x0F
        size = second & 0x7F
        if size == 126:
            size = struct.unpack("!H", self._read_exact(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self._read_exact(8))[0]
        if size > 2 * 1024 * 1024:
            raise RuntimeError("cdp_message_too_large")
        mask = self._read_exact(4) if second & 0x80 else b""
        payload = self._read_exact(size)
        if mask:
            payload = bytes(byte ^ mask[index & 3] for index, byte in enumerate(payload))
        if opcode == 8:
            raise RuntimeError("cdp_connection_closed")
        if opcode != 1:
            return {}
        value = json.loads(payload)
        return value if isinstance(value, dict) else {}

    def command(self, method: str, params: dict | None = None, session_id: str | None = None) -> dict:
        self.sequence += 1
        command_id = self.sequence
        value = {"id": command_id, "method": method, "params": params or {}}
        if session_id:
            value["sessionId"] = session_id
        try:
            self._send(value)
            while True:
                result = self._receive()
                if result.get("id") != command_id:
                    continue
                if "error" in result:
                    raise RuntimeError("cdp_command_failed")
                return result.get("result") or {}
        except TimeoutError:
            raise TimeoutError(CDP_TIMEOUT_CODES.get(method, "cdp_timeout_other")) from None

    def close(self) -> None:
        self.socket.close()


class Client:
    def __init__(self, state_dir: Path, worker: str, browser: str, token: str, proxy: str):
        self.state_dir = state_dir
        self.worker = worker if worker.startswith("http://") else "http://" + worker
        self.token = token
        self.proxy = proxy
        version = json.loads(urllib.request.urlopen(f"http://{browser}/json/version", timeout=4).read())
        websocket = version.get("webSocketDebuggerUrl")
        if not isinstance(websocket, str) or not websocket.startswith("ws://"):
            raise RuntimeError("cdp_discovery_invalid")
        path = websocket.split("/", 3)[-1]
        self.cdp = CDP("http://" + browser + "/" + path)
        version_info = self.cdp.command("Browser.getVersion")
        if not isinstance(version_info.get("product"), str):
            raise RuntimeError("cdp_discovery_invalid")
        context_params = {"disposeOnDetach": True, "proxyServer": self.proxy,
                          "proxyBypassList": "<-loopback>"}
        context = self.cdp.command("Target.createBrowserContext", context_params).get("browserContextId")
        if not isinstance(context, str):
            raise RuntimeError("peer_context_create_failed")
        target = self.cdp.command("Target.createTarget", {"url": "about:blank", "browserContextId": context}).get("targetId")
        if not isinstance(target, str):
            raise RuntimeError("peer_target_create_failed")
        session = self.cdp.command("Target.attachToTarget", {"targetId": target, "flatten": True}).get("sessionId")
        if not isinstance(session, str):
            raise RuntimeError("peer_attach_failed")
        self.context, self.target, self.session = context, target, session
        self.targets = self._target_ids()
        self.contexts = self._context_ids()
        self.peer_probe()

    def _target_ids(self) -> set[str]:
        rows = self.cdp.command("Target.getTargets").get("targetInfos", [])
        return {row["targetId"] for row in rows if isinstance(row, dict) and isinstance(row.get("targetId"), str)}

    def _context_ids(self) -> set[str]:
        return set(self.cdp.command("Target.getBrowserContexts").get("browserContextIds", []))

    def peer_probe(self) -> None:
        if self.target not in self._target_ids() or self.context not in self._context_ids():
            raise RuntimeError("peer_target_or_context_lost")
        result = self.cdp.command("Runtime.evaluate", {"expression": "1 + 1", "returnByValue": True}, self.session)
        if (result.get("result") or {}).get("value") != 2:
            raise RuntimeError("peer_session_probe_failed")

    def assert_clean_news_resources(self) -> None:
        # Check the foreign CDP session first so target/context loss retains its
        # specific failure code even when that loss also changes resource sets.
        self.peer_probe()
        current_targets = self._target_ids()
        current_contexts = self._context_ids()
        if current_targets != self.targets or current_contexts != self.contexts:
            raise RuntimeError("news_browser_resource_leak")

    def request(self, name: str, case: str, budget_ms: int, route: str | None = None) -> dict:
        url = f"https://native-fixture.test/{route or f'hold/{case}/{name}'}"
        request_id = "cloak-concurrency-" + uuid.uuid4().hex
        body = json.dumps({"request_id": request_id, "url": url, "source_type": "article",
                           "strategy": "browser_rendering", "use_browser": True,
                           "budget_ms": budget_ms}).encode()
        req = urllib.request.Request(self.worker.rstrip("/") + "/v1/extract", data=body,
                                     headers={"Content-Type": "application/json",
                                              "Authorization": "Bearer " + self.token})
        started = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=max(5, budget_ms / 1000 + 5)) as response:
                raw = response.read(1024 * 1024 + 1)
        except urllib.error.HTTPError as response:
            raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise RuntimeError("worker_response_invalid")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, ValueError):
            raise RuntimeError("worker_response_invalid") from None
        if (not isinstance(value, dict) or value.get("contract_version") != "1"
                or value.get("request_id") != request_id or not isinstance(value.get("outcome"), str)
                or value.get("outcome") not in {"success", "failed", "rejected"}):
            raise RuntimeError("worker_response_invalid")
        error = value.get("error")
        if error is not None and (not isinstance(error, dict)
                or not isinstance(error.get("code"), str)
                or not re.fullmatch(r"[a-z0-9_]{1,64}", error["code"])
                or (error.get("upstream_status") is not None and type(error["upstream_status"]) is not int)):
            raise RuntimeError("worker_response_invalid")
        result = value.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("worker_response_invalid")
        return {"name": name, "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                "outcome": value.get("outcome"), "error": (value.get("error") or {}).get("code"),
                "upstream_status": (value.get("error") or {}).get("upstream_status"),
                "content_chars": len((value.get("result") or {}).get("content") or "")}

    def origin_state(self) -> dict:
        path = self.state_dir / "origin-state.json"
        end = time.monotonic() + 20
        while time.monotonic() < end:
            try:
                if path.is_file() and not path.is_symlink():
                    value = json.loads(path.read_text())
                    if isinstance(value, dict):
                        return value
            except (OSError, ValueError):
                pass
            time.sleep(.025)
        raise RuntimeError("origin_state_timeout")

    def wait_arrivals(self, case: str, count: int) -> dict:
        end = time.monotonic() + 25
        while time.monotonic() < end:
            state = self.origin_state()
            rows = [row for row in state.get("arrivals", []) if row.get("case") == case]
            if len(rows) >= count:
                return state
            time.sleep(.025)
        raise RuntimeError("fixture_arrival_timeout")

    def release(self, case: str) -> None:
        path = self.state_dir / "release" / case
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write("release\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)

    def profile_one(self) -> dict:
        started = time.monotonic()
        with concurrent_pool(2) as pool:
            first = pool.submit(self.request, "one", "one-first", 25_000)
            self.wait_arrivals("one-first", 1)
            second = pool.submit(self.request, "two", "one-second", 25_000)
            time.sleep(.6)
            before = self.origin_state()
            count = sum(row.get("case", "").startswith("one-") for row in before.get("arrivals", []))
            if before.get("active") != 1 or count != 1:
                raise RuntimeError("one_slot_overlap_detected")
            self.release("one-first")
            self.wait_arrivals("one-second", 1)
            time.sleep(.6)
            self.release("one-second")
            results = [first.result(timeout=35), second.result(timeout=35)]
        self.assert_success(results)
        state = self.origin_state()
        if (state.get("max_active") != 1 or
                sum(row.get("case", "").startswith("one-") for row in state.get("arrivals", [])) != 2):
            raise RuntimeError("one_slot_fixture_evidence_invalid")
        self.assert_clean_news_resources()
        return {"limit": 1, "max_origin_active": state["max_active"],
                "comparable_pair_elapsed_ms": round(max(row["elapsed_ms"] for row in results), 1),
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1), "requests": results}

    def profile_two(self) -> dict:
        started = time.monotonic()
        with concurrent_pool(3) as pool:
            first = pool.submit(self.request, "one", "two", 25_000)
            second = pool.submit(self.request, "two", "two", 25_000)
            self.wait_arrivals("two", 2)
            third = pool.submit(self.request, "three", "two", 25_000)
            time.sleep(.6)
            before = self.origin_state()
            count = sum(row.get("case") == "two" for row in before.get("arrivals", []))
            if before.get("active") != 2 or count != 2:
                raise RuntimeError("two_slot_barrier_or_third_waiter_invalid")
            self.release("two")
            results = [first.result(timeout=35), second.result(timeout=35), third.result(timeout=35)]
        self.assert_success(results)
        state = self.origin_state()
        if state.get("max_active") != 2 or sum(row.get("case") == "two" for row in state.get("arrivals", [])) != 3:
            raise RuntimeError("two_slot_fixture_evidence_invalid")
        self.assert_clean_news_resources()
        return {"limit": 2, "max_origin_active": state["max_active"],
                "comparable_pair_elapsed_ms": round(max(row["elapsed_ms"] for row in results[:2]), 1),
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1), "requests": results}

    def cancellation(self) -> dict:
        with concurrent_pool(2) as pool:
            cancelled = pool.submit(self.request, "cancelled", "cancel", 6_000)
            survivor = pool.submit(self.request, "survivor", "cancel", 25_000)
            self.wait_arrivals("cancel", 2)
            cancelled_result = cancelled.result(timeout=12)
            self.release("cancel")
            survivor_result = survivor.result(timeout=30)
        if (cancelled_result["outcome"] not in {"failed", "rejected"}
                or cancelled_result["error"] not in {"native_browser_timeout", "cancelled"}
                or survivor_result["outcome"] != "success" or survivor_result["content_chars"] < 200):
            raise RuntimeError("cancellation_isolation_failed")
        self.assert_clean_news_resources()
        return {"cancelled": cancelled_result, "survivor": survivor_result,
                "foreign_peer_survived": True, "news_resources_clean": True}

    def upstream_error(self) -> dict:
        result = self.request("upstream-error", "error", 25_000, route="error/error")
        if result["outcome"] != "rejected" or result["error"] != "native_browser_http_rejected" or result["upstream_status"] != 503:
            raise RuntimeError("upstream_error_classification_invalid")
        self.assert_clean_news_resources()
        return {"request": result, "foreign_peer_survived": True, "news_resources_clean": True}

    @staticmethod
    def assert_success(results: list[dict]) -> None:
        if any(row["outcome"] != "success" or row["content_chars"] < 200 for row in results):
            raise RuntimeError("synthetic_article_request_failed")

    def close(self) -> None:
        try:
            self.cdp.command("Target.disposeBrowserContext", {"browserContextId": self.context})
        finally:
            self.cdp.close()


def concurrent_pool(size: int):
    from concurrent.futures import ThreadPoolExecutor
    return ThreadPoolExecutor(max_workers=size)


def _run_client() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--worker", required=True)
    parser.add_argument("--browser", required=True)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--command-dir", type=Path, required=True)
    parser.add_argument("--result-dir", type=Path, required=True)
    args = parser.parse_args()
    token = args.token_file.read_text().strip()
    client = Client(args.state_dir, args.worker, args.browser, token, args.proxy)
    args.result_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    (args.result_dir / "peer-open.json").write_text(json.dumps({"target": "held", "context": "held"}) + "\n")
    seen: set[str] = set()
    try:
        while True:
            commands = sorted(args.command_dir.glob("[0-9a-f]" * 32 + ".json"))
            pending = next((path for path in commands if path.name not in seen), None)
            if pending is None:
                time.sleep(.025)
                continue
            seen.add(pending.name)
            command = json.loads(pending.read_text())
            command_id = pending.stem
            pending.unlink()
            try:
                action = command.get("action")
                if action == "profile_one":
                    result = client.profile_one()
                elif action == "profile_two":
                    result = client.profile_two()
                elif action == "cancellation":
                    result = client.cancellation()
                elif action == "upstream_error":
                    result = client.upstream_error()
                elif action == "probe":
                    client.assert_clean_news_resources()
                    result = {"peer_survived": True}
                elif action == "close":
                    client.close()
                    result = {"peer_closed": True}
                    status = {"command_id": command_id, "ok": True, "result": result}
                    _write_receipt(args.result_dir / (command_id + ".json"), status)
                    return
                else:
                    raise RuntimeError("client_command_invalid")
                client.assert_clean_news_resources()
                status = {"command_id": command_id, "ok": True, "result": result}
            except Exception as exc:
                status = {"command_id": command_id, "ok": False,
                          "error": str(exc) if str(exc) in CLIENT_ERROR_CODES else "client_gate_failed"}
            _write_receipt(args.result_dir / (command_id + ".json"), status)
            if not status["ok"]:
                return
    finally:
        try:
            client.close()
        except Exception:
            pass


def main() -> None:
    try:
        _run_client()
    except Exception as exc:
        reason = str(exc) if str(exc) in CLIENT_ERROR_CODES else "observer_error"
        exception_type = type(exc).__name__
        if exception_type not in {"OSError", "RuntimeError", "TimeoutError", "ValueError", "HTTPError"}:
            exception_type = "Exception"
        print(f"native_cloak_observer_failure type={exception_type} reason={reason}", flush=True)
        raise SystemExit(1) from None


def _write_receipt(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


if __name__ == "__main__":
    main()
