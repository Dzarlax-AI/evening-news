"""Capture registered FastAPI route/schema baseline without running lifespan or I/O."""
import json
import os
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

# These values permit settings validation at import; they are synthetic and never used.
os.environ["DATABASE_URL"] = "postgresql+asyncpg://fixture:fixture@127.0.0.1/fixture"
os.environ["GEMINI_API_KEY"] = "fixture-gemini-key"
os.environ["ADMIN_PASSWORD"] = "fixture-admin-password"
os.environ["JWT_SECRET"] = "fixture-jwt-secret"

from news_aggregator.main import app  # noqa: E402


def capture() -> dict:
    schema = app.openapi()
    route_data = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = sorted(getattr(route, "methods", ()) or ())
        if not path:
            continue
        route_data.append({
            "path": path,
            "methods": methods,
            "name": getattr(route, "name", None),
            "response_class": getattr(getattr(route, "response_class", None), "__name__", None),
            "openapi": path in schema.get("paths", {}),
        })
    for path, operations in schema.get("paths", {}).items():
        route_data.append({"path": path, "methods": sorted(method.upper() for method in operations if method.lower() in {"get", "post", "put", "patch", "delete", "options", "head", "trace"}), "name": None, "response_class": None, "openapi": True})
    unique = {(item["path"], tuple(item["methods"])): item for item in route_data}
    return {"openapi": schema, "registered_routes": sorted(unique.values(), key=lambda x: (x["path"], x["methods"]))}


if __name__ == "__main__":
    destination = REPO / "tests/fixtures/web-contract-v1/registered-routes-openapi.json"
    destination.write_text(json.dumps(capture(), indent=2, sort_keys=True) + "\n")
    print(f"Captured {len(capture()['registered_routes'])} registered routes to {destination}")
