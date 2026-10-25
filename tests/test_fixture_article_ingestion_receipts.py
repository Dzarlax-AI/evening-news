"""Contract checks for fixture-only article ingestion receipt endpoints."""

import importlib.util
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import threading
import urllib.error
import urllib.request
import uuid

import pytest

from news_aggregator.services.article_ingestion_client import _run_id


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_SERVICES = (
    ROOT / "tests/fixtures/background-worker/fake_services.py",
    ROOT / "tests/fixtures/go-scheduler/fake_services.py",
)
TOKEN = "fixture-only-article-ingestion-token"


def _load_fixture_handler(path):
    spec = importlib.util.spec_from_file_location(f"fixture_services_{path.parent.name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _request(base, method, path, *, body=None, token=TOKEN):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as response:
        return response.code, json.loads(response.read())


@pytest.mark.parametrize("fixture_path", FIXTURE_SERVICES, ids=lambda path: path.parent.name)
def test_fixture_ingestion_receipts_are_authenticated_and_idempotent(fixture_path):
    module = _load_fixture_handler(fixture_path)
    with module.LOCK:
        module.COUNTERS["ingestion_stub_runs"] = 0
        module.INGESTION_RUNS.clear()
        if hasattr(module, "INGESTION_REQUESTS"):
            module.INGESTION_REQUESTS.clear()

    server = ThreadingHTTPServer(("127.0.0.1", 0), module.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        job_id = str(uuid.uuid4())
        envelope = {
            "version": 1,
            "job_id": job_id,
            "attempt_generation": 1,
            "owner_id": str(uuid.uuid4()),
            "generation": 2,
            "backend_pid": 12345,
            "executor_instance_id": str(uuid.uuid4()),
            "deadline_at": "2030-01-01T00:00:00Z",
        }
        status, unauthorized = _request(
            base, "POST", "/internal/v1/ingestion/runs", body=envelope, token="wrong-token"
        )
        assert status == 401
        assert unauthorized == {"error": "unauthorized"}

        status, receipt = _request(base, "POST", "/internal/v1/ingestion/runs", body=envelope)
        assert status == 200
        assert receipt["run_id"] == _run_id(job_id, 1)
        assert receipt["job_id"] == job_id
        assert receipt["attempt_generation"] == 1
        assert receipt["state"] == "succeeded"
        assert receipt["result"]["status"] == "success"

        status, polled = _request(base, "GET", f"/internal/v1/ingestion/runs/{receipt['run_id']}")
        assert status == 200
        assert polled == receipt

        status, replay = _request(base, "POST", "/internal/v1/ingestion/runs", body=envelope)
        assert status == 200
        assert replay == receipt

        changed = {**envelope, "deadline_at": "2030-01-01T00:01:00Z"}
        status, conflict = _request(base, "POST", "/internal/v1/ingestion/runs", body=changed)
        assert status == 409
        assert conflict == {"error": "idempotency_conflict"}

        invalid = {**envelope, "version": 2}
        status, rejected = _request(base, "POST", "/internal/v1/ingestion/runs", body=invalid)
        assert status == 422
        assert rejected == {"error": "invalid_request"}

        assert module.COUNTERS["ingestion_stub_runs"] == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
