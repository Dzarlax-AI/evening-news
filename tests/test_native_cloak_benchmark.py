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
    default = bench.build_parser().parse_args(["--variant", "A", "--output", "report.json"]).origin
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
    assert bench._is_challenge_html('<title>Just a moment</title><script src="cf_chl_x"></script>')
    assert not bench._is_challenge_html("An article mentioning a challenge")


def test_proxy_credentials_are_scoped_to_exact_proxy_auth_challenge():
    proxy = "http://10.40.0.12:3128"
    assert bench.proxy_challenge_matches("Proxy", "http://10.40.0.12:3128", proxy)
    assert not bench.proxy_challenge_matches("Server", "http://10.40.0.12:3128", proxy)
    assert not bench.proxy_challenge_matches("Proxy", "https://native-fixture.test", proxy)
    assert not bench.proxy_challenge_matches("Proxy", "http://10.40.0.12:31280", proxy)


def test_pinned_nodriver_frame_tree_result_uses_frame_id_():
    from nodriver import cdp

    command = cdp.page.get_frame_tree()
    assert next(command)["method"] == "Page.getFrameTree"
    frame = {"id": "main-frame", "loaderId": "loader", "url": "about:blank",
             "domainAndRegistry": "", "securityOrigin": "null", "mimeType": "text/html",
             "secureContextType": "Secure", "crossOriginIsolatedContextType": "NotIsolated",
             "gatedAPIFeatures": []}
    with pytest.raises(StopIteration) as result:
        command.send({"frameTree": {"frame": frame}})
    tree = result.value.value
    assert tree.frame.id_ == "main-frame"
    assert not hasattr(tree, "frame_tree")


def test_real_nodriver_request_event_keeps_frame_and_request_maps_separate():
    from nodriver import cdp

    event = object.__new__(cdp.network.RequestWillBeSent)
    event.request_id = "request-1"
    event.type_ = cdp.network.ResourceType.DOCUMENT
    event.frame_id = "main-frame"
    state = {"request_count": 0, "requests": {}, "request_frames": {}, "frames": {"main-frame"}}
    assert bench._record_request_event(state, event) is False
    assert state["requests"] == {"request-1": True}
    assert state["request_frames"] == {"request-1": "main-frame"}
    assert state["frames"] == {"main-frame"}


def test_native_readiness_gate_requires_each_go_timing_and_network_condition():
    gate = bench._native_ready_gate
    ready = dict(now=10.5, started=10.0, stable_since=10.25,
                 last_activity=10.25, active_requests=0, main_status=200)
    assert gate(**ready)
    for change in ({"now": 10.499}, {"stable_since": 10.251},
                   {"last_activity": 10.251}, {"active_requests": 1},
                   {"main_status": 0}, {"main_status": 302}):
        assert not gate(**{**ready, **change})


def test_disposable_context_cleanup_stops_then_disposes_then_retires_transport():
    calls = []

    class Tab:
        def __init__(self):
            self.__dict__["_benchmark_download_handler"] = lambda _event: None

        async def send(self, command):
            calls.append(command.gi_code.co_name)

    tab = Tab()
    sentinel = object()

    class Browser:
        def __init__(self):
            self._browser_pool_tabs = {id(tab): tab}
            self._targets = [tab, sentinel]

        async def send(self, command):
            calls.append(command.gi_code.co_name)

        def remove_handler(self, _event_type, callback):
            assert callback is handler
            calls.append("remove_download_handler")

    browser = Browser()

    class Pool:
        _managed_connections = {id(tab): tab}

        async def _close_transport(self, connection):
            assert connection is tab
            calls.append("local_transport_retire")

    handler = tab.__dict__["_benchmark_download_handler"]
    asyncio.run(bench._dispose_context_and_retire(Pool(), browser, "owned-context", tab))
    assert calls == ["remove_download_handler", "stop_loading", "dispose_browser_context", "local_transport_retire"]
    assert "_benchmark_download_handler" not in tab.__dict__
    assert browser._targets == [sentinel]
    assert browser._browser_pool_tabs == {}
    assert Pool._managed_connections == {}


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

    # Run the real production parser against the controlled, captured fixture HTML.
    from bs4 import BeautifulSoup
    from news_aggregator.extraction.extraction_utils import ExtractionUtils
    from news_aggregator.extraction.html_processor import HTMLProcessor
    processor = HTMLProcessor(ExtractionUtils())
    for path, anchor in (("/article-beta", "Fixture signal beta"), ("/slow", "Fixture signal gamma")):
        parsed_html = request(path)["body"].decode("utf-8")
        content, selector = processor.extract_by_enhanced_selectors(BeautifulSoup(parsed_html, "html.parser"))
        assert selector == "article"
        assert content and len(content) > 400 and anchor in content


def test_production_extraction_helpers_construct_without_database_access(monkeypatch):
    """The parser baseline uses production helpers but must not touch persistence."""
    from news_aggregator.extraction.extraction_utils import ExtractionUtils
    from news_aggregator.extraction.html_processor import HTMLProcessor
    from news_aggregator.extraction.date_extractor import DateExtractor
    from news_aggregator.extraction.metadata_extractor import MetadataExtractor
    from news_aggregator.extraction.extraction_strategies import ExtractionStrategies
    from bs4 import BeautifulSoup

    def forbidden_connect(*_args, **_kwargs):
        raise AssertionError("benchmark parser attempted database access")

    try:
        from news_aggregator.database import get_db
    except ImportError:
        get_db = None
    if get_db is not None and hasattr(get_db, "engine"):
        monkeypatch.setattr(get_db.engine, "connect", forbidden_connect)
    utils = ExtractionUtils()
    processor = HTMLProcessor(utils)
    strategy = ExtractionStrategies(utils, processor, DateExtractor(utils), MetadataExtractor(utils))
    assert strategy.html_processor is processor
    fixture_spec = importlib.util.spec_from_file_location("native_cloak_db_free_fixture", FIXTURES / "origin.py")
    fixture = importlib.util.module_from_spec(fixture_spec)
    fixture_spec.loader.exec_module(fixture)
    captured = "<article><p>" + fixture.ROUTES["/article-beta"] + "</p></article>"
    content, selector = processor.extract_by_enhanced_selectors(BeautifulSoup(captured, "html.parser"))
    assert selector == "article" and content and len(content) > 400


def test_report_summary_does_not_copy_sensitive_fields():
    case = {"id": "safe-case", "expected_state": "success", "expected_contains": ["anchor"]}
    result = {"outcome": "success", "content": "anchor secret url https://private.example/path", "token": "secret"}
    check = bench.compare_case(case, result)
    assert set(check) == {"passed", "expected_state", "state_match", "anchor_match", "expected_error_match"}
    assert "https://private.example/path" not in json.dumps(check)


def test_runner_emits_redacted_report_and_marks_expected_anchor_mismatch(tmp_path, monkeypatch):
    corpus = {"version": 1, "cases": [{"id": "safe-case", "path": "/fixture", "expected_contains": ["real anchor"], "expected_state": "success"}]}
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps(corpus), encoding="utf-8")
    app_path, cloak_path = tmp_path / "app", tmp_path / "cloak"
    app_path.mkdir()
    cloak_path.mkdir()
    cgroups = {"app": app_path, "cloak": cloak_path}
    monkeypatch.setattr(bench, "sample_cgroups", lambda _paths: {
        name: {"memory_current_bytes": 100, "memory_anon_bytes": 60, "memory_file_bytes": 30,
               "memory_lifetime_peak_bytes": 120, "memory_shmem_bytes": 10, "cpu_usage_usec": 1, "pids": 2,
               "oom_events": 0, "oom_kill_events": 0} for name in cgroups
    })

    async def fake_python(_variant, _url, _proxy, _token):
        return {"outcome": "success", "state": "extracted", "content_chars": 10,
                "html_bytes": 100, "method": "browser_rendering", "selector_present": True,
                "content": "a different result"}

    monkeypatch.setattr(bench, "_run_python_case", fake_python)
    args = Namespace(variant="A", origin="https://native-fixture.test", corpus=str(corpus_path),
                     cgroup=[f"app={app_path}", f"cloak={cloak_path}"], case=[], token_env=None,
                     proxy_token_env=None, proxy=None, endpoint=None, cycles=1, idle_seconds=[])
    report = __import__("asyncio").run(bench.run(args))
    encoded = json.dumps(report)
    assert report["status"] == "mismatch"
    assert report["mismatch_count"] == 1
    assert "native-fixture.test/fixture" not in encoded
    assert "a different result" not in encoded
    assert report["repeat_scope"].startswith("single_invocation")
    assert report["measurement_scope"] == "variant_controller_and_measured_cgroups"
    assert report["latency_ms"]["p95_method"] == "nearest_rank"
