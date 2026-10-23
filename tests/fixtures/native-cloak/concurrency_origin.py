#!/usr/bin/env python3
"""Small HTTPS-only origin for the isolated native Cloak concurrency gate."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ID = re.compile(r"[a-z0-9-]{1,32}\Z")
BODY = (
    "The synthetic harbor team compared a new water sensor with a marked staff gauge "
    "during several morning inspections. The replacement records short surges that the "
    "older hourly readings missed, stores observations while its radio link is down, and "
    "keeps each original timestamp when it uploads delayed measurements. Engineers will "
    "review calibration notes and maintenance time before deciding whether to repeat "
    "the trial near the ferry terminal. This fixture contains no publisher content."
)


class FixtureState:
    def __init__(self, directory: Path):
        self.directory = directory
        self.lock = threading.Lock()
        self.persist_lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.arrivals: list[dict[str, object]] = []
        self.completed = 0
        self.errors = 0

    def update(self, **values: object) -> None:
        with self.lock:
            for key, value in values.items():
                setattr(self, key, value)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self.persist_lock:
            with self.lock:
                snapshot = {
                    "schema_version": 1,
                    "active": self.active,
                    "max_active": self.max_active,
                    "arrivals": list(self.arrivals),
                    "completed": self.completed,
                    "errors": self.errors,
                }
            path = self.directory / "origin-state.json"
            temporary = self.directory / ("origin-state.json.tmp-" + secrets.token_hex(8))
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(snapshot, stream, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)

    def arrive(self, case: str, job: str) -> None:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.arrivals.append({"case": case, "job": job, "at": time.monotonic()})
        self.update()

    def finish(self, error: bool = False) -> None:
        with self.lock:
            self.active = max(0, self.active - 1)
            self.completed += 1
            self.errors += int(error)
        self.update()

    def released(self, case: str) -> bool:
        path = self.directory / "release" / case
        try:
            info = path.stat(follow_symlinks=False)
            return path.is_file() and not path.is_symlink() and info.st_size <= 32
        except OSError:
            return False


def handler_for(state: FixtureState):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NewsConcurrencyFixture/1"
        sys_version = ""

        def log_message(self, _format, *_args):
            return

        def do_GET(self):  # noqa: N802 - stdlib handler API
            parts = self.path.split("?", 1)[0].strip("/").split("/")
            if len(parts) == 3 and parts[0] == "hold" and all(ID.fullmatch(x) for x in parts[1:]):
                self._hold(parts[1], parts[2])
                return
            if len(parts) == 2 and parts[0] == "error" and ID.fullmatch(parts[1]):
                state.arrive(parts[1], "error")
                state.finish(error=True)
                self._send(503, self._html("Synthetic upstream error"))
                return
            if self.path == "/healthz":
                self._send(200, b"ok", "text/plain; charset=utf-8")
                return
            self._send(404, b"fixture route rejected", "text/plain; charset=utf-8")

        def _hold(self, case: str, job: str):
            state.arrive(case, job)
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline and not state.released(case):
                time.sleep(0.025)
            if not state.released(case):
                state.finish(error=True)
                self._send(504, b"fixture barrier timed out", "text/plain; charset=utf-8")
                return
            state.finish()
            self._send(200, self._html(f"Fixture {case} {job}"))

        @staticmethod
        def _html(title: str) -> bytes:
            return ("<!doctype html><html><head><title>" + title + "</title></head>"
                    "<body><main data-ready=\"true\"><article><h1>" + title +
                    "</h1><p>" + BODY + "</p></article></main></body></html>").encode()

        def _send(self, status: int, body: bytes, content_type="text/html; charset=utf-8"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", required=True, type=Path)
    parser.add_argument("--key", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--port", type=int, default=443)
    args = parser.parse_args()
    state = FixtureState(args.state_dir)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), handler_for(state))
    server.daemon_threads = True
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.cert, args.key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    state.update()
    server.serve_forever()


if __name__ == "__main__":
    main()
