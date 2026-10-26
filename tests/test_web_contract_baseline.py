"""Guard registered FastAPI schemas and deterministic HTTP behavior without lifespan."""
import asyncio
import json
import os
import base64
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fixture:fixture@127.0.0.1/fixture")
os.environ.setdefault("GEMINI_API_KEY", "fixture-gemini-key")
os.environ.setdefault("ADMIN_PASSWORD", "fixture-admin-password")
os.environ.setdefault("JWT_SECRET", "fixture-jwt-secret")

from news_aggregator.main import app
from news_aggregator.config import settings
from news_aggregator.security import create_jwt_token, SecurityLevel, JWT_SECRET_KEY
import jwt
from datetime import datetime, timedelta, timezone
from scripts.capture_web_contract import capture

FIXTURE = Path(__file__).parent / "fixtures/web-contract-v1/registered-routes-openapi.json"


def test_registered_routes_and_schemas_match_frozen_baseline():
    assert capture() == json.loads(FIXTURE.read_text())


async def request(path, method="GET", headers=None):
    messages = []
    scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
             "http_version": "1.1", "method": method, "scheme": "http", "path": path,
             "raw_path": path.encode(), "query_string": b"", "root_path": "",
             "headers": [(b"host", b"localhost"), *[(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]],
             "client": ("127.0.0.1", 12345), "server": ("localhost", 80)}
    delivered = False
    async def receive():
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}
    async def send(message):
        messages.append(message)
    try:
        await app(scope, receive, send)
    except Exception as error:
        # Starlette sends a 500 response and re-raises for server logging.
        if not any(m["type"] == "http.response.start" for m in messages):
            raise
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return start["status"], dict((k.decode().lower(), v.decode()) for k, v in start["headers"]), body


def test_live_http_health_auth_admin_cookie_and_redirect_contracts():
    async def check():
        status, _, body = await request("/health")
        assert (status, json.loads(body)) == (200, {"status": "healthy"})
        status, headers, body = await request("/api/v1/sources")
        assert status == 401
        assert json.loads(body) == {"detail": "Invalid or missing credentials"}
        assert headers["www-authenticate"] == "Bearer"
        status, headers, _ = await request("/admin/")
        assert status == 401 and headers["www-authenticate"] == "Basic"
        basic = base64.b64encode(f"{settings.admin_username}:{settings.admin_password}".encode()).decode()
        status, headers, body = await request("/admin/", headers={"authorization": f"Basic {basic}"})
        assert status == 200 and b"Evening News" in body
        cookie = headers["set-cookie"].lower()
        assert "admin_token=" in cookie and "httponly" in cookie and "samesite=lax" in cookie
        status, headers, body = await request("/auth/login", method="POST")
        assert status == 401 and json.loads(body) == {"detail": "Not authenticated"}
        status, headers, body = await request("/auth/login", method="POST", headers={"authorization": f"Basic {basic}"})
        assert status == 500 and body == b"Internal Server Error"  # legacy authenticate_admin_basic returns username str, login expects credentials object
        assert "set-cookie" not in headers
        for auth_headers in ({"authorization": f"Bearer {create_jwt_token(settings.admin_username)}"}, {"cookie": f"admin_token={create_jwt_token(settings.admin_username)}"}):
            status, _, body = await request("/auth/me", headers=auth_headers)
            assert status == 200 and json.loads(body)["username"] == settings.admin_username
        status, _, body = await request("/auth/me", headers={"authorization": f"Basic {basic}"})
        assert status == 500 and body == b"Internal Server Error"  # Basic fallback lacks exp required by /auth/me
        nonadmin = create_jwt_token("reader", SecurityLevel.API_READ)
        status, _, body = await request("/api/v1/sources", headers={"authorization": f"Bearer {nonadmin}"})
        assert status == 403 and json.loads(body) == {"detail": "Admin access required"}
        expired = jwt.encode({"sub": "admin", "level": "admin", "exp": datetime.now(timezone.utc) - timedelta(seconds=1)}, JWT_SECRET_KEY, algorithm="HS256")
        status, _, body = await request("/auth/me", headers={"authorization": f"Bearer {expired}"})
        assert status == 401 and json.loads(body) == {"detail": "Invalid or missing credentials"}
        status, headers, body = await request("/auth/me")
        assert status == 401 and json.loads(body) == {"detail": "Invalid or missing credentials"}
        status, headers, body = await request("/auth/logout", method="POST")
        assert status == 200 and json.loads(body)["message"].startswith("Logged out successfully")
        status, headers, _ = await request("/news")
        assert status == 301 and headers["location"] == "/"
        status, _, _ = await request("/health/")
        assert status == 404
    asyncio.run(check())
