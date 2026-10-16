"""Fixed HTTPS origin for the isolated native-shadow network gate.

This fixture must be used only with the controlled bootstrap TLS configuration.
It serves a fixed route set and records no URLs, bodies, or credential values.
"""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import ssl
import threading

MAX_PROOF_BYTES = 1024 * 1024
ORIGIN_HOST = "native-fixture.test"
ARTICLE_SOURCE = Path(__file__).resolve().parents[1] / "native-cloak" / "origin.py"


def article_text() -> str:
    spec = importlib.util.spec_from_file_location("native_cloak_origin_for_gate", ARTICLE_SOURCE)
    if spec is None or spec.loader is None:
        raise RuntimeError("Fixed article source is unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.ROUTES["/article-alpha"] + (
        "</p><p>The harbor office will keep the trial records with their calibration notes and maintenance log. "
        "Engineers will inspect any gap before using the series for planning. "
        "They will publish the next review only after the ferry terminal comparison has been checked by a second technician."
    )


def validate_trap_ip(value: str) -> str:
    try:
        address = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        raise ValueError("Trap address must be an exact private IPv4 literal") from None
    if not address.is_private or address.is_loopback or address.is_link_local or address.is_multicast:
        raise ValueError("Trap address must be an exact private IPv4 literal")
    return str(address)


@dataclass(frozen=True)
class Response:
    route_id: str
    status: int
    body: bytes
    content_type: str = "text/html; charset=utf-8"
    headers: tuple[tuple[str, str], ...] = ()


def route_response(path: str, trap_ip: str, cookie: str = "") -> Response:
    """Return only fixed content for exact route paths; never proxy a URL."""
    trap = f"https://{trap_ip}:9443/trap"
    trap_ws = f"wss://{trap_ip}:9443/trap"
    article = ("<!doctype html><meta charset=utf-8><title>Gate article</title>"
               '<article data-ready="true"><h1>Gate article</h1><p>'
               + article_text() + "</p></article>")
    if path == "/cookie-proof":
        ok = "native_cookie=present" in cookie
        return Response("cookie-proof", 200 if ok else 403,
                        b"cookie confirmed" if ok else b"cookie missing", "text/plain; charset=utf-8")
    if path == "/js-article":
        story = json.dumps(article_text())
        html = ("<!doctype html><meta charset=utf-8><title>Gate article</title>"
                "<main id=article></main><script>"
                "document.cookie='native_cookie=present; Path=/; SameSite=Lax';"
                f"fetch('https://{ORIGIN_HOST}/cookie-proof').then(r=>r.text()).then(t=>{{"
                f"document.querySelector('#article').innerHTML='<article data-ready=\"true\"><h1>Gate article</h1><p>'+{story}+'</p><p>'+t+'</p></article>';"
                "});</script>")
        return Response("js-article", 200, html.encode("utf-8"))
    if path == "/private-subrequest":
        return Response("private-subrequest", 200,
                        (article + f"<img src='{trap}' alt='trap'>").encode())
    if path == "/redirect-private":
        return Response("redirect-private", 302, b"", headers=(("Location", trap),))
    if path == "/iframe-private":
        return Response("iframe-private", 200,
                        (article + f"<iframe src='{trap}'></iframe>").encode())
    if path == "/worker-private":
        return Response("worker-private", 200,
                        (article + "<script>new Worker('/worker.js');</script>").encode())
    if path == "/worker.js":
        return Response("worker-script", 200,
                        f"fetch('{trap}').catch(()=>{{}});".encode(), "text/javascript; charset=utf-8")
    if path == "/popup-private":
        html = (article + "<button id=gate-popup onclick=\"window.open('"
                + trap + "','_blank')\">Open fixture popup</button>"
                + "<script>window.open('" + trap + "','_blank');</script>")
        return Response("popup-private", 200, html.encode())
    if path == "/websocket-private":
        return Response("websocket-private", 200,
                        (article + f"<script>new WebSocket('{trap_ws}');</script>").encode())
    if path == "/download":
        return Response("download", 200, b"fixed fixture download\n", "text/plain; charset=utf-8",
                        (("Content-Disposition", "attachment; filename=gate-fixture.txt"),))
    if path == "/origin-auth":
        return Response("origin-auth", 401, b"origin credentials refused", "text/plain; charset=utf-8",
                        (("WWW-Authenticate", 'Basic realm="native-gate"'),))
    return Response("not-found", 404, b"fixture route rejected", "text/plain; charset=utf-8")


def proxy_token_in_authorization(value: str, expected_token: str | None) -> bool:
    if not expected_token or not value:
        return False
    if expected_token in value:
        return True
    if value.lower().startswith("basic "):
        try:
            decoded = base64.b64decode(value[6:], validate=True).decode("utf-8", errors="ignore")
        except (ValueError, UnicodeError):
            return False
        return expected_token in decoded
    return False


class ProofLog:
    def __init__(self, path: Path):
        self._lock = threading.Lock()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        self._fd = os.open(path, flags, 0o600)
        if hasattr(os, "fchmod"):
            os.fchmod(self._fd, 0o600)
        else:
            os.chmod(path, 0o600)
        self._size = 0

    def append(self, row: dict[str, object]) -> None:
        data = (json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        with self._lock:
            if self._size + len(data) > MAX_PROOF_BYTES:
                raise RuntimeError("Proof log byte limit reached")
            os.write(self._fd, data)
            self._size += len(data)

    def close(self) -> None:
        os.close(self._fd)


def handler_for(trap_ip: str, proof: ProofLog, expected_proxy_token: str | None = None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NativeGateFixture/1"
        sys_version = ""

        def log_message(self, _format, *_args):
            return

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            response = route_response(path, trap_ip, self.headers.get("Cookie", ""))
            authorization = self.headers.get("Authorization", "")
            proxy_authorization = self.headers.get("Proxy-Authorization", "")
            proof.append({
                "route_id": response.route_id,
                "tls_version": self.connection.version(),
                "has_authorization": bool(authorization),
                "has_proxy_authorization": bool(proxy_authorization),
                "proxy_token_in_authorization": proxy_token_in_authorization(
                    authorization, expected_proxy_token),
            })
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            self.send_header("Cache-Control", "no-store")
            for name, value in response.headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(response.body)

    return Handler


def serve(cert: Path, key: Path, proof_output: Path, trap_ip: str, port: int = 443) -> None:
    trap_ip = validate_trap_ip(trap_ip)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert, key)
    proof = ProofLog(proof_output)
    server = ThreadingHTTPServer(("127.0.0.1", port),
                                 handler_for(trap_ip, proof, os.environ.get("EXPECTED_PROXY_TOKEN")))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        proof.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cert", type=Path, required=True)
    parser.add_argument("--key", type=Path, required=True)
    parser.add_argument("--proof-output", type=Path, required=True)
    parser.add_argument("--trap-ip", required=True)
    parser.add_argument("--port", type=int, default=443)
    args = parser.parse_args(argv)
    serve(args.cert, args.key, args.proof_output, args.trap_ip, args.port)


if __name__ == "__main__":
    main()
