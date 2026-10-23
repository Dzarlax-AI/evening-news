import importlib.util
import json
import asyncio
from argparse import Namespace
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "benchmark_native_cloak.py"
SPEC = importlib.util.spec_from_file_location("benchmark_native_cloak", SCRIPT)
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)
FIXTURES = ROOT / "tests" / "fixtures" / "native-cloak"


def test_checked_in_corpus_has_safe_ids_and_fixed_paths():
    corpus, digest = bench.load_corpus(FIXTURES / "corpus.json")
    assert len(corpus["cases"]) == 5
    assert len(digest) == 64
    assert all("url" not in case for case in corpus["cases"])


@pytest.mark.parametrize("origin", [
    "https://example.com", "http://native-fixture.test", "https://user@native-fixture.test",
    "https://native-fixture.test:8443", "https://native-fixture.test/path",
    "https://native-fixture", "https://localhost", "https://127.0.0.1",
])
def test_origin_rejects_non_fixture_or_non_443_destinations(origin):
    with pytest.raises(bench.BenchmarkError):
        bench.validate_origin(origin)


def test_default_origin_is_exact_dotted_fixture_host_accepted_by_go():
    default = bench.build_parser().parse_args(["--variant", "C", "--output", "report.json"]).origin
    assert default == "https://native-fixture.test"
    assert bench.validate_origin(default) == default
    assert "." in bench.urlsplit(default).hostname


def test_expected_success_requires_all_fixture_anchors():
    case = {"expected_state": "success", "expected_contains": ["anchor one", "anchor two"]}
    assert not bench.compare_case(case, {"outcome": "success", "content": "anchor one"})["passed"]
    assert bench.compare_case(case, {"outcome": "success", "content": "ANCHOR ONE and anchor two"})["passed"]


def test_expected_success_cannot_pass_when_both_backends_fail():
    case = {"expected_state": "success", "expected_contains": []}
    result = {"outcome": "success", "state": "failed", "content": ""}
    assert not bench.compare_case(case, result)["passed"]


def test_known_rejected_state_is_state_parity_only():
    case = {"expected_state": "rejected", "expected_contains": []}
    result = bench.compare_case(case, {"outcome": "rejected", "content": "challenge"})
    assert result["passed"]
    assert result["anchor_match"] is None


def test_challenge_fixture_requires_challenge_error_classification():
    case = {"expected_state": "rejected", "expected_contains": [],
            "expected_error": "native_browser_challenge"}
    assert bench.compare_case(case, {"outcome": "rejected", "error": "native_browser_challenge"})["passed"]
    assert not bench.compare_case(case, {"outcome": "rejected", "error": "public_url_denied"})["passed"]


def test_python_benchmark_variants_are_not_available():
    for variant in ("A", "B"):
        with pytest.raises(SystemExit):
            bench.build_parser().parse_args(["--variant", variant, "--output", "report.json"])


def test_native_readiness_gate_requires_each_go_timing_and_network_condition():
    gate = bench._native_ready_gate
    ready = dict(now=10.5, started=10.0, stable_since=10.25,
                 last_activity=10.25, active_requests=0, main_status=200)
    assert gate(**ready)
    for change in ({"now": 10.499}, {"stable_since": 10.251},
                   {"last_activity": 10.251}, {"active_requests": 1},
                   {"main_status": 0}, {"main_status": 302}):
        assert not gate(**{**ready, **change})


def test_native_service_capability_must_be_ready_and_native_with_selector():
    good = {
        "ready": True,
        "capabilities": {"browser": True, "native_browser": True, "native_ready_selector": True},
        "native_profile": {"ready_selector_sha256": bench.hashlib.sha256(bench.READY_SELECTOR.encode()).hexdigest()},
    }
    assert bench._validate_native_capability_payload(good)["native_ready_selector"]
    with pytest.raises(bench.NativeCapabilityUnavailable, match="native_capability_unavailable") as caught:
        bench._validate_native_capability_payload({
            "ready": True,
            "capabilities": {"browser": True, "native_browser": False, "native_ready_selector": True},
            "native_profile": good["native_profile"],
        })
    assert caught.value.diagnostic_code == "capability_native_browser_false"
    wrong_hash = {**good, "native_profile": {"ready_selector_sha256": "0" * 64}}
    with pytest.raises(bench.NativeCapabilityUnavailable, match="native_capability_unavailable") as caught:
        bench._validate_native_capability_payload(wrong_hash)
    assert caught.value.diagnostic_code == "selector_hash_mismatch"


@pytest.mark.parametrize("status,body,diagnostic", [
    (503, b"private error payload", "http_status_503"),
    (200, b"not-json", "invalid_json"),
])
def test_native_capability_probe_classifies_failure_without_retaining_body(
        monkeypatch, status, body, diagnostic):
    import aiohttp

    class Response:
        def __init__(self):
            self.status = status
            self.content = self

        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        async def read(self, _limit): return body

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        def get(self, *_args, **_kwargs): return Response()

    monkeypatch.setattr(aiohttp, "ClientSession", lambda **_kwargs: Session())
    with pytest.raises(bench.NativeCapabilityUnavailable, match="native_capability_unavailable") as caught:
        asyncio.run(bench._probe_native_capabilities("http://127.0.0.1:8080"))
    assert caught.value.diagnostic_code == diagnostic
    assert str(caught.value) == "native_capability_unavailable"
    assert body.decode() not in str(caught.value)


def test_non_200_readiness_keeps_only_typed_flags_and_hash_match(monkeypatch):
    import aiohttp

    secret = "private-token-and-body-text"
    expected_hash = bench.hashlib.sha256(bench.READY_SELECTOR.encode()).hexdigest()
    body = json.dumps({
        "ready": False,
        "capabilities": {"browser": True, "native_browser": False,
                         "native_ready_selector": True, "extra": secret},
        "native_profile": {"ready_selector_sha256": expected_hash, "extra": secret},
        "message": secret, "url": "https://private.example/secret",
    }).encode()
    calls = 0

    class Response:
        status = 503
        content = None

        def __init__(self): self.content = self
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        async def read(self, _limit): return body

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        def get(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            return Response()

    monkeypatch.setattr(aiohttp, "ClientSession", lambda **_kwargs: Session())
    with pytest.raises(bench.NativeCapabilityUnavailable) as caught:
        asyncio.run(bench._probe_native_capabilities("http://127.0.0.1:8080"))
    assert calls == 1
    assert caught.value.diagnostic_code == "http_status_503"
    assert caught.value.readiness_diagnostic == {
        "ready": False, "browser": True, "native_browser": False,
        "native_ready_selector": True, "selector_hash_matches": True,
    }
    assert secret not in repr(caught.value.readiness_diagnostic)
    assert "https://" not in repr(caught.value.readiness_diagnostic)


@pytest.mark.parametrize("body", [
    b"private malformed body",
    b'{"ready":"false","capabilities":{"browser":1,"native_browser":null},'
    b'"native_profile":{"ready_selector_sha256":"private"}}',
    b'{"ready":false}' + (b" " * 16384),
])
def test_non_200_malformed_readiness_diagnostic_is_omitted(monkeypatch, body):
    import aiohttp

    class Response:
        status = 503
        content = None
        def __init__(self): self.content = self
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        async def read(self, _limit): return body

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        def get(self, *_args, **_kwargs): return Response()

    monkeypatch.setattr(aiohttp, "ClientSession", lambda **_kwargs: Session())
    with pytest.raises(bench.NativeCapabilityUnavailable) as caught:
        asyncio.run(bench._probe_native_capabilities("http://127.0.0.1:8080"))
    assert caught.value.diagnostic_code == "http_status_503"
    assert caught.value.readiness_diagnostic is None


def test_missing_cgroup_data_fails_instead_of_reporting_zero(tmp_path):
    with pytest.raises(bench.BenchmarkError, match="cgroup_metric_unavailable"):
        bench.sample_cgroups({"worker": tmp_path})


def test_cgroup_sample_and_summary_use_real_values(tmp_path):
    values = {
        "memory.current": "1200\n", "memory.peak": "1800\n",
        "memory.stat": "anon 700\nfile 400\nshmem 100\n",
        "cpu.stat": "usage_usec 900\n", "pids.current": "4\n",
        "memory.events": "oom 0\noom_kill 0\n",
    }
    for name, value in values.items():
        (tmp_path / name).write_text(value, encoding="ascii")
    sample = bench.sample_cgroups({"worker": tmp_path})
    summary = bench.summarize_samples([{"elapsed_ms": 1, "cgroups": sample}])
    assert summary["cgroups"]["worker"]["memory_current_sampled_max_bytes"] == 1200
    assert summary["cgroups"]["worker"]["memory_lifetime_peak_bytes"] == 1800
    assert summary["cgroups"]["worker"]["memory_lifetime_peak_source"] == "kernel_memory.peak"
    assert summary["kernel_peak_available"] is True
    assert summary["cgroups"]["worker"]["memory_anon_last_bytes"] == 700
    assert summary["aggregate_sampled_memory_current_bytes_max"] == 1200


def test_missing_kernel_peak_reports_unavailable_without_losing_sampled_metrics(tmp_path):
    values = {
        "memory.current": "1200\n", "memory.stat": "anon 700\nfile 400\nshmem 100\n",
        "cpu.stat": "usage_usec 900\n", "pids.current": "4\n",
        "memory.events": "oom 0\noom_kill 0\n",
    }
    for name, value in values.items():
        (tmp_path / name).write_text(value, encoding="ascii")
    sample = bench.sample_cgroups({"worker": tmp_path})
    assert sample["worker"]["memory_lifetime_peak_bytes"] is None
    summary = bench.summarize_samples([{"elapsed_ms": 1, "cgroups": sample}])
    assert summary["cgroups"]["worker"]["memory_current_sampled_max_bytes"] == 1200
    assert summary["cgroups"]["worker"]["memory_lifetime_peak_bytes"] is None
    assert summary["cgroups"]["worker"]["memory_lifetime_peak_source"] == "unavailable_on_host"
    assert summary["kernel_peak_available"] is False


def test_cgroup_paths_reject_duplicates_and_ancestry(tmp_path):
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    with pytest.raises(bench.BenchmarkError, match="overlapping_cgroup_paths"):
        bench.parse_cgroups([f"app={parent}", f"worker={child}"])
    with pytest.raises(bench.BenchmarkError, match="invalid_cgroup_argument"):
        bench.parse_cgroups([f"app={parent}", f"app={child}"])


def test_fixture_origin_serves_javascript_cookie_redirect_slow_dom_and_challenge():
    """Exercise fixed route responses without opening a socket in unit tests."""
    origin_spec = importlib.util.spec_from_file_location("native_cloak_fixture_origin", FIXTURES / "origin.py")
    origin = importlib.util.module_from_spec(origin_spec)
    origin_spec.loader.exec_module(origin)
    assert all(len(story.replace("</p><p>", " ").split()) >= 400 for story in origin.ROUTES.values())
    handler_type = origin.handler_for(None)

    def request(path, cookie=""):
        handler = handler_type.__new__(handler_type)
        handler.command = "GET"
        handler.path = path
        handler.headers = type("Headers", (), {"get": lambda _self, key, default="": cookie if key == "Cookie" else default})()
        captured = {}
        handler._send = lambda status, body, content_type="text/html; charset=utf-8", headers=(): captured.update(
            status=status, body=body, content_type=content_type, headers=headers)
        handler._proof = lambda _route: None
        handler.do_GET()
        return captured

    redirect = request("/redirect")
    assert dict(redirect["headers"])["Location"] == "/article-beta"
    js_page = request("/js-article")["body"]
    assert b"fetch('/cookie-proof')" in js_page
    assert request("/cookie-proof", "native_cookie=present")["body"] == b"cookie confirmed"
    challenge = request("/challenge")["body"]
    assert b'id="challenge-form"' in challenge
    assert b'data-ready="true"' in challenge
    slow = request("/slow")["body"]
    assert b"Fixture signal gamma" in slow
    huge = request("/huge-dom")["body"]
    assert b"Fixture signal delta" in huge and len(huge) < 5 * 1024 * 1024

def test_report_summary_does_not_copy_sensitive_fields():
    case = {"id": "safe-case", "expected_state": "success", "expected_contains": ["anchor"]}
    result = {"outcome": "success", "content": "anchor secret url https://private.example/path", "token": "secret"}
    check = bench.compare_case(case, result)
    assert set(check) == {"passed", "expected_state", "state_match", "anchor_match", "expected_error_match"}
    assert "https://private.example/path" not in json.dumps(check)


def test_go_runner_emits_redacted_report_and_marks_expected_anchor_mismatch(tmp_path, monkeypatch):
    corpus = {"version": 1, "cases": [{"id": "safe-case", "path": "/fixture", "expected_contains": ["real anchor"], "expected_state": "success"}]}
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps(corpus), encoding="utf-8")
    worker_path, cloak_path, gateway_path = tmp_path / "worker", tmp_path / "cloak", tmp_path / "gateway"
    for path in (worker_path, cloak_path, gateway_path):
        path.mkdir()
    cgroups = {"worker": worker_path, "cloak": cloak_path, "gateway": gateway_path}
    monkeypatch.setattr(bench, "sample_cgroups", lambda _paths: {
        name: {"memory_current_bytes": 100, "memory_anon_bytes": 60, "memory_file_bytes": 30,
               "memory_lifetime_peak_bytes": 120, "memory_shmem_bytes": 10, "cpu_usage_usec": 1, "pids": 2,
               "oom_events": 0, "oom_kill_events": 0} for name in cgroups
    })

    async def fake_service(_url, _endpoint, _token):
        return {"outcome": "success", "state": "extracted", "content_chars": 10,
                "html_bytes": 100, "method": "browser_rendering", "selector_present": True,
                "content": "a different result"}

    monkeypatch.setattr(bench, "_run_service_case", fake_service)
    monkeypatch.setattr(bench, "_probe_native_capabilities", lambda _endpoint: asyncio.sleep(0, result={"ready": True}))
    monkeypatch.setenv("EXTRACTOR_TEST_TOKEN", "synthetic-service-token")
    args = Namespace(variant="C", origin="https://native-fixture.test", corpus=str(corpus_path),
                     cgroup=[f"worker={worker_path}", f"cloak={cloak_path}", f"gateway={gateway_path}"],
                     case=[], token_env="EXTRACTOR_TEST_TOKEN", endpoint="http://127.0.0.1:8080",
                     native_ready_selector=bench.READY_SELECTOR, cycles=1, idle_seconds=[])
    report = __import__("asyncio").run(bench.run(args))
    encoded = json.dumps(report)
    assert report["status"] == "mismatch"
    assert report["mismatch_count"] == 1
    assert "native-fixture.test/fixture" not in encoded
    assert "a different result" not in encoded
    assert report["repeat_scope"].startswith("single_invocation")
    assert report["measurement_scope"] == "variant_controller_and_measured_cgroups"
    assert report["latency_ms"]["p95_method"] == "nearest_rank"
