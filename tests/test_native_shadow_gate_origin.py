"""The controlled TLS gate origin must remain fixed and proof-only."""

from io import BytesIO
import importlib.util
import json
import os
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).resolve().parent / "fixtures" / "native-shadow" / "gate_origin.py"
spec = importlib.util.spec_from_file_location("native_shadow_gate_origin", SCRIPT)
gate = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = gate
spec.loader.exec_module(gate)


def test_article_positive_path_has_varied_prose_and_cookie_ready_marker():
    assert len(gate.article_text().split()) > 500
    response = gate.route_response("/js-article", "10.55.0.2")
    body = response.body.decode()
    assert response.status == 200
    assert "https://native-fixture.test/cookie-proof" in body
    assert "native_cookie=present" in body
    assert 'data-ready=\\"true\\"' in body or 'data-ready="true"' in body
    assert "Fixture signal alpha" in body
    assert gate.route_response("/cookie-proof", "10.55.0.2", "native_cookie=present").status == 200
    assert gate.route_response("/cookie-proof", "10.55.0.2").status == 403


@pytest.mark.parametrize("route,marker", [
    ("/private-subrequest", b"<img src='https://10.55.0.2:9443/trap'"),
    ("/iframe-private", b"<iframe src='https://10.55.0.2:9443/trap'"),
    ("/worker-private", b"new Worker('/worker.js')"),
    ("/worker.js", b"fetch('https://10.55.0.2:9443/trap')"),
    ("/popup-private", b"window.open('https://10.55.0.2:9443/trap'"),
    ("/websocket-private", b"new WebSocket('wss://10.55.0.2:9443/trap')"),
])
def test_fixed_routes_trigger_private_trap(route, marker):
    response = gate.route_response(route, "10.55.0.2")
    assert response.status == 200
    assert marker in response.body
    if route != "/worker.js":
        assert b'data-ready="true"' in response.body
        assert b"Fixture signal alpha" in response.body


def test_popup_attempts_automatically_without_user_click():
    body = gate.route_response("/popup-private", "10.55.0.2").body
    assert b"<script>window.open('https://10.55.0.2:9443/trap'" in body


def test_wire_limit_route_is_fixed_and_exceeds_service_wire_cap():
    response = gate.route_response("/wire-limit", "10.55.0.2")
    assert response.route_id == "wire-limit"
    assert response.status == 200
    assert b"fetch('/wire-limit-body')" in response.body
    assert len(response.body) < 1024
    body = gate.route_response("/wire-limit-body", "10.55.0.2")
    assert len(body.body) > 5 * 1024 * 1024
    assert body.content_type == "application/octet-stream"
    response = gate.route_response("/renderer-stress", "10.55.0.2")
    assert response.route_id == "renderer-stress"
    assert len(response.body) > 5 * 1024 * 1024
    assert response.body.startswith(b"<!doctype html>")
    assert response.body.endswith(b"</p>")
    controlled = gate.route_response("/controlled-crash", "10.55.0.2")
    assert controlled.route_id == "controlled-crash"
    assert len(controlled.body) < 1024
    assert b"Waiting for the test-only crash command" in controlled.body
    assert b"data-ready" not in controlled.body


@pytest.mark.parametrize('route', ['/wire-limit-body', '/renderer-stress'])
def test_wire_limit_handler_streams_fixed_chunks(monkeypatch, route):
    response_sizes = []
    sleeps = []

    class Writer:
        def write(self, data):
            response_sizes.append(len(data))
        def flush(self):
            pass

    class Proof:
        def append(self, _row):
            pass

    handler = gate.handler_for("10.55.0.2", Proof())
    fake = SimpleNamespace(path=route, headers={},
        connection=SimpleNamespace(version=lambda: "TLSv1.3"), wfile=Writer(),
        send_response=lambda _status: None, send_header=lambda *_args: None,
        end_headers=lambda: None)
    monkeypatch.setattr(gate.time, "sleep", lambda delay: sleeps.append(delay))
    handler.do_GET(fake)
    assert response_sizes and max(response_sizes) <= 8 * 1024
    assert sum(response_sizes) > 5 * 1024 * 1024
    assert len(sleeps) == len(response_sizes)
    assert set(sleeps) == {0.005}


def test_redirect_download_auth_and_unknown_routes():
    redirect = gate.route_response("/redirect-private", "10.55.0.2")
    assert redirect.status == 302
    assert redirect.headers == (("Location", "https://10.55.0.2:9443/trap"),)
    assert gate.route_response("/download", "10.55.0.2").headers[0][0] == "Content-Disposition"
    auth = gate.route_response("/origin-auth", "10.55.0.2")
    assert auth.status == 401
    assert auth.headers[0][0] == "WWW-Authenticate"
    assert gate.route_response("/arbitrary", "10.55.0.2").status == 404


@pytest.mark.parametrize("address", ["127.0.0.1", "8.8.8.8", "169.254.1.1", "localhost", "::1"])
def test_trap_ip_must_be_private_literal(address):
    with pytest.raises(ValueError):
        gate.validate_trap_ip(address)


def test_proof_drops_header_values_and_stays_bounded(tmp_path, monkeypatch):
    path = tmp_path / "proof.jsonl"
    proof = gate.ProofLog(path)
    rows = []

    class CaptureProof:
        def append(self, row):
            rows.append(row)
            proof.append(row)

    handler = gate.handler_for("10.55.0.2", CaptureProof(), "proxy-secret")
    fake = SimpleNamespace(
        path="/origin-auth?secret=article-token",
        headers={"Authorization": "Basic cHJveHktc2VjcmV0", "Proxy-Authorization": "proxy-secret"},
        connection=SimpleNamespace(version=lambda: "TLSv1.3"), wfile=BytesIO(),
        send_response=lambda status: None, send_header=lambda *args: None,
        end_headers=lambda: None,
    )
    handler.do_GET(fake)
    proof.close()
    saved = path.read_text()
    assert len(rows) == 1
    assert rows[0] == {"route_id": "origin-auth", "tls_version": "TLSv1.3",
                       "has_authorization": True, "has_proxy_authorization": True,
                       "proxy_token_in_authorization": True}
    assert "proxy-secret" not in saved and "article-token" not in saved
    assert "origin-auth?" not in saved
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        gate.ProofLog(path)

    monkeypatch.setattr(gate, "MAX_PROOF_BYTES", 8)
    small = gate.ProofLog(tmp_path / "small.jsonl")
    with pytest.raises(RuntimeError, match="byte limit"):
        small.append({"route_id": "too-large"})
    small.close()
    assert (tmp_path / "small.jsonl").stat().st_size == 0
