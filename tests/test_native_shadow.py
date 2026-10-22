"""Safety and accounting checks for the isolated live-shadow controller."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import run_native_shadow as shadow


def test_standalone_help_imports_sibling_benchmark_with_other_scripts_package(tmp_path):
    app = tmp_path / "app"
    other_scripts = app / "scripts"
    other_scripts.mkdir(parents=True)
    (other_scripts / "__init__.py").write_text("")
    (other_scripts / "benchmark_native_cloak.py").write_text("raise RuntimeError('wrong benchmark package')\n")
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join((str(app), str(root)))}
    completed = subprocess.run([sys.executable, str(root / "scripts" / "run_native_shadow.py"), "--help"],
                               env=env, cwd=tmp_path, capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0, completed.stderr
    assert "--variant" in completed.stdout


def _case(number: int, host: str = "news.example.com") -> dict:
    return {"id": f"case-{number:04d}", "article_id": number, "source_type": "rss",
            "url": f"https://{host}/article-{number}", "title": "Private source title",
            "archived_content": "Archive source text", "published_at": "2026-10-15T00:00:00Z",
            "media_files": [], "image_url": None}


def _corpus(path: Path, cases: list[dict]) -> Path:
    path.write_text(json.dumps({"version": 1, "cases": cases}))
    path.chmod(0o600)
    return path


def _cgroups(base: Path) -> list[str]:
    flags = []
    for name in ("app", "cloak", "worker", "gateway"):
        path = base / name
        path.mkdir(parents=True)
        for filename, data in {
            "memory.current": "1024\n", "memory.stat": "anon 512\nfile 256\nshmem 128\n",
            "cpu.stat": "usage_usec 120\n", "pids.current": "2\n",
            "memory.events": "oom 0\noom_kill 0\n"}.items():
            (path / filename).write_text(data)
        flags.append(f"{name}={path}")
    return flags


def test_public_url_policy_and_corpus_bounds(tmp_path):
    allowed = "https://www.example.com/article"
    assert shadow._public_https_url(allowed, main=True)
    assert shadow._public_https_url(allowed + "?asset=1")
    for url in ("http://www.example.com/a", "https://www.example.com:8443/a",
                "https://u:p@www.example.com/a", "https://localhost/a",
                "https://127.0.0.1/a", "https://one/a", "https://www.example.com/a#frag"):
        assert not shadow._public_https_url(url, main=True)
    assert not shadow._public_https_url(allowed + "?token=secret", main=True)
    path = _corpus(tmp_path / "corpus.json", [_case(1), _case(2)])
    cases, digest = shadow.load_corpus(path)
    assert len(cases) == 2 and len(digest) == 64
    with pytest.raises(shadow.ShadowError, match="invalid_corpus"):
        shadow.load_corpus(_corpus(path, [_case(1)] * 2))
    with pytest.raises(shadow.ShadowError, match="invalid_corpus"):
        shadow.load_corpus(_corpus(path, [_case(i) for i in range(1, 12)]))


def test_retry_after_parsing_is_bounded():
    assert shadow.bench._retry_after_seconds("30") == 30
    assert shadow.bench._retry_after_seconds("999999") == 86400
    assert shadow.bench._retry_after_seconds("not a date") is None


def test_missing_cgroup_metric_is_fatal(tmp_path):
    flags = _cgroups(tmp_path)
    (tmp_path / "worker" / "memory.events").unlink()
    groups = shadow.bench.parse_cgroups(flags)
    with pytest.raises(shadow.bench.BenchmarkError, match="cgroup_metric_unavailable"):
        shadow.bench.sample_cgroups(groups)


def test_shadow_cgroup_roles_extend_strict_benchmark_names(tmp_path):
    cloak = tmp_path / "cloak"
    relay = tmp_path / "relay"
    child = cloak / "child"
    child.mkdir(parents=True)
    relay.mkdir()
    with pytest.raises(shadow.bench.BenchmarkError, match="invalid_cgroup_argument"):
        shadow.bench.parse_cgroups([f"relay={relay}"])
    parsed = shadow.bench.parse_cgroups(
        [f"cloak={cloak}", f"relay={relay}"], allowed_names=shadow.SHADOW_CGROUP_NAMES)
    assert set(parsed) == {"cloak", "relay"}
    with pytest.raises(shadow.bench.BenchmarkError, match="invalid_cgroup_argument"):
        shadow.bench.parse_cgroups([f"relay={relay}", f"relay={cloak}"],
                                   allowed_names=shadow.SHADOW_CGROUP_NAMES)
    with pytest.raises(shadow.bench.BenchmarkError, match="overlapping_cgroup_paths"):
        shadow.bench.parse_cgroups([f"cloak={cloak}", f"relay={child}"],
                                   allowed_names=shadow.SHADOW_CGROUP_NAMES)


def test_oom_delta_stops_variant(tmp_path):
    groups = shadow.bench.parse_cgroups(_cgroups(tmp_path))
    baseline = shadow.bench.sample_cgroups(groups)
    sampler = shadow.bench.Sampler(groups)
    sampler.baseline = baseline
    (tmp_path / "cloak" / "memory.events").write_text("oom 1\noom_kill 0\n")
    with pytest.raises(shadow.ShadowError, match="shadow_oom"):
        shadow._check_telemetry(sampler, groups)


def test_private_capture_limit_and_safe_row(tmp_path, monkeypatch):
    output = tmp_path / "out"
    shadow._prepare_output(output)
    raw = output / "raw-C-case-0001.json"
    shadow._write_private(raw, {"content": "Private story body"}, output_dir=output)
    if os.name == "posix":
        assert raw.stat().st_mode & 0o077 == 0
    monkeypatch.setattr(shadow, "MAX_OUTPUT_BYTES", raw.stat().st_size)
    with pytest.raises(shadow.ShadowError, match="capture_limit"):
        shadow._write_private(output / "report-C.json", {"new": "data"}, output_dir=output)
    result = {"outcome": "success", "state": "extracted", "content": "Private story body",
              "method": "browser_rendering", "error": "http://secret.example/path?token=abc",
              "final_url": "https://secret.example/path", "title": "Private source title"}
    row = shadow._case_row(_case(1), result, "C", 42)
    rendered = json.dumps(row)
    assert "Private" not in rendered and "secret.example" not in rendered and "token" not in rendered
    assert row["error"] == "other" and row["final_url_changed"]


@pytest.mark.asyncio
async def test_429_skips_host_without_retry_and_ledger_prevents_duplicate(tmp_path, monkeypatch):
    corpus = _corpus(tmp_path / "corpus.json", [_case(1), _case(2), _case(3, "other.example.com")])
    output = tmp_path / "output"
    calls = []

    async def ready(_endpoint):
        return {"ready": True, "browser": True, "native_browser": True, "native_ready_selector": False}

    async def extract(url, _endpoint, _token):
        calls.append(url)
        if len(calls) == 1:
            return {"outcome": "rejected", "state": "failed", "error": "upstream_rate_limited",
                    "upstream_status": 429, "retry_after_seconds": 30, "content": "", "method": "none"}
        return {"outcome": "success", "state": "extracted", "content": "Fresh body", "method": "browser_rendering"}

    monkeypatch.setattr(shadow, "_readiness", ready)
    monkeypatch.setattr(shadow, "_run_c", extract)
    monkeypatch.setattr(shadow, "NAVIGATION_INTERVAL_SECONDS", 0)
    monkeypatch.setenv("SERVICE_TOKEN", "token-longer-than-sixteen")
    args = argparse.Namespace(variant="C", corpus=str(corpus), output_dir=str(output), case=[],
                              cgroup=_cgroups(tmp_path / "metrics"), endpoint_c="http://news-native-shadow-service:8080",
                              proxy_b=None, token_env="SERVICE_TOKEN", proxy_token_env="PROXY_TOKEN")
    report = await shadow.run(args)
    assert report["status"] == "complete"
    assert [row["id"] for row in report["rows"]] == ["case-0001", "case-0003"]
    assert report["skipped_rate_limited_ids"] == ["case-0002"]
    assert report["rows"][0]["retry_after_seconds"] == 30
    assert len(calls) == 2
    assert "Private source title" not in (output / "report-C.json").read_text()
    assert "Private source title" in (output / "raw-C-case-0001.json").read_text()
    second = await shadow.run(args)
    assert [row["id"] for row in second["rows"]] == ["case-0001", "case-0003"]
    assert second["skipped_rate_limited_ids"] == ["case-0002"]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_fatal_error_keeps_partial_case_and_attempt_ledger(tmp_path, monkeypatch):
    corpus = _corpus(tmp_path / "corpus.json", [_case(1)])
    output = tmp_path / "output"

    async def ready(_endpoint):
        return {"ready": True, "browser": True, "native_browser": True, "native_ready_selector": False}

    async def failed(_url, _endpoint, _token):
        raise shadow.ShadowError("native_service_unavailable")

    monkeypatch.setattr(shadow, "_readiness", ready)
    monkeypatch.setattr(shadow, "_run_c", failed)
    monkeypatch.setenv("SERVICE_TOKEN", "token-longer-than-sixteen")
    args = argparse.Namespace(variant="C", corpus=str(corpus), output_dir=str(output), case=[],
                              cgroup=_cgroups(tmp_path / "metrics"),
                              endpoint_c="http://news-native-shadow-service:8080", proxy_b=None,
                              token_env="SERVICE_TOKEN", proxy_token_env="PROXY_TOKEN")
    report = await shadow.run(args)
    assert report["status"] == "incomplete"
    assert report["fatal_error"] == "native_service_unavailable"
    assert report["rows"][0]["id"] == "case-0001"
    assert (output / "raw-C-case-0001.json").exists()
    assert json.loads((output / "ledger.json").read_text())["attempted"]["C"] == ["case-0001"]
    assert "Private source title" not in (output / "report-C.json").read_text()
    assert "Private source title" in (output / "raw-C-case-0001.json").read_text()
    with pytest.raises(shadow.ShadowError, match="invalid_prior_report"):
        await shadow.run(args)


def _b_args(tmp_path, monkeypatch):
    corpus = _corpus(tmp_path / "corpus.json", [_case(1)])
    monkeypatch.setenv("BROWSER_WS_ENDPOINT", "http://172.20.0.2:9222")
    monkeypatch.setenv("PROXY_TOKEN", "proxy-token-longer-than-sixteen")
    return argparse.Namespace(variant="B", corpus=str(corpus), output_dir=str(tmp_path / "output"),
                              case=[], cgroup=_cgroups(tmp_path / "metrics"), endpoint_c=None,
                              proxy_b="http://gateway:8081", token_env="SERVICE_TOKEN",
                              proxy_token_env="PROXY_TOKEN")


@pytest.mark.asyncio
@pytest.mark.parametrize("change_at,expected_rows", [(2, 0), (3, 1)])
async def test_b_discovery_change_stops_before_or_after_job(tmp_path, monkeypatch, change_at, expected_rows):
    args = _b_args(tmp_path, monkeypatch)
    probes = []
    calls = []

    async def discovery(_endpoint):
        probes.append(True)
        return "identity-b" if len(probes) >= change_at else "identity-a"

    async def extraction(_url, _proxy, _token):
        calls.append(True)
        return {"outcome": "success", "state": "extracted", "content": "Article body",
                "method": "browser_rendering"}

    monkeypatch.setattr(shadow, "_probe_browser_discovery", discovery)
    monkeypatch.setattr(shadow, "_run_b", extraction)
    report = await shadow.run(args)
    assert report["status"] == "incomplete"
    assert report["fatal_error"] == "browser_generation_changed"
    assert len(report["rows"]) == expected_rows
    assert len(calls) == expected_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [shadow.BrowserUnavailableError("lost"), ConnectionError("lost"), OSError("lost")])
async def test_b_transport_failure_is_fatal_with_partial_capture(tmp_path, monkeypatch, failure):
    args = _b_args(tmp_path, monkeypatch)

    async def discovery(_endpoint):
        return "identity-a"

    async def extraction(_url, _proxy, _token):
        raise failure

    monkeypatch.setattr(shadow, "_probe_browser_discovery", discovery)
    monkeypatch.setattr(shadow, "_run_b", extraction)
    report = await shadow.run(args)
    assert report["status"] == "incomplete"
    assert report["fatal_error"] == "browser_transport_unavailable"
    assert report["rows"][0]["error"] == "browser_transport_unavailable"
    assert (Path(args.output_dir) / "raw-B-case-0001.json").exists()


def test_b_discovery_endpoint_must_be_numeric_private(monkeypatch):
    monkeypatch.setenv("BROWSER_WS_ENDPOINT", "http://browser.example.com:9222")
    with pytest.raises(shadow.ShadowError, match="invalid_browser_discovery_endpoint"):
        shadow._browser_discovery_endpoint()
    monkeypatch.setenv("BROWSER_WS_ENDPOINT", "http://8.8.8.8:9222")
    with pytest.raises(shadow.ShadowError, match="invalid_browser_discovery_endpoint"):
        shadow._browser_discovery_endpoint()


@pytest.mark.asyncio
async def test_readiness_requires_no_fixture_selector(monkeypatch):
    # Capability payload validation is tested through a minimal fake aiohttp session.
    class Response:
        status = 200
        content = None
        def __init__(self):
            self.content = self
        async def read(self, _limit):
            return b'{"ready":true,"capabilities":{"browser":true,"native_browser":true,"native_ready_selector":true}}'
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
    class Session:
        def get(self, *_args, **_kwargs):
            return Response()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
    import aiohttp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda **_kwargs: Session())
    with pytest.raises(shadow.ShadowError, match="native_readiness_lost"):
        await shadow._readiness("http://service:8080")


@pytest.mark.asyncio
async def test_b_uses_public_policy_generic_readiness_and_python_metadata(monkeypatch):
    seen = {}

    async def page(_pool, _parser, url, _proxy, _token, **kwargs):
        seen.update(kwargs)
        assert url == "https://news.example.com/story"
        return {"outcome": "success", "state": "extracted", "method": "browser_rendering",
                "content": "Article body", "final_url": url,
                "rendered_html": '<html><head><meta property="og:title" content="Current story">'
                                 '<meta property="og:image" content="/cover.jpg"></head>'
                                 '<body><article><p>Article body</p></article></body></html>'}

    monkeypatch.setattr(shadow.bench, "_run_native_policy_page", page)
    result = await shadow._run_b("https://news.example.com/story", "http://gateway:8080", "long-secret")
    assert seen["url_policy"]("https://news.example.com/img.jpg?size=2")
    assert not seen["url_policy"]("https://localhost/admin")
    assert "data-ready" not in seen["ready_expression"]
    assert seen["capture_raw"] is True
    assert result["title"] == "Current story"
    assert result["image_url"] == "https://news.example.com/cover.jpg"


@pytest.mark.asyncio
async def test_c_client_deadline_stops_variant_before_another_dispatch(monkeypatch):
    from news_aggregator.extraction.service_client import ExtractionServiceClient, ExtractionServiceError

    async def expired(*args,**kwargs):
        raise ExtractionServiceError('deadline_exceeded', True)

    monkeypatch.setattr(ExtractionServiceClient,'extract',expired)
    with pytest.raises(shadow.ShadowError,match='native_job_deadline_cleanup_unconfirmed'):
        await shadow._run_c('https://news.example.com/story','http://127.0.0.1:8080','test-token')


@pytest.mark.asyncio
async def test_c_outer_deadline_records_partial_case_and_stops(tmp_path,monkeypatch):
    import asyncio
    corpus=_corpus(tmp_path/'corpus.json',[_case(1),_case(2)])
    calls=[]
    async def ready(_endpoint):
        return {'ready':True,'browser':True,'native_browser':True,'native_ready_selector':False}
    async def blocked(*args):
        calls.append(True)
        await asyncio.sleep(60)
    monkeypatch.setattr(shadow,'_readiness',ready)
    monkeypatch.setattr(shadow,'_run_c',blocked)
    monkeypatch.setattr(shadow,'MAX_JOB_SECONDS',.01)
    monkeypatch.setenv('SERVICE_TOKEN','token-longer-than-sixteen')
    args=argparse.Namespace(variant='C',corpus=str(corpus),output_dir=str(tmp_path/'output'),case=[],
        cgroup=_cgroups(tmp_path/'metrics'),endpoint_c='http://news-native-shadow-service:8080',
        proxy_b=None,token_env='SERVICE_TOKEN',proxy_token_env='PROXY_TOKEN')
    report=await shadow.run(args)
    assert len(calls)==1
    assert report['status']=='incomplete'
    assert report['fatal_error']=='native_job_deadline_cleanup_unconfirmed'
    assert [row['id'] for row in report['rows']]==['case-0001']


@pytest.mark.asyncio
@pytest.mark.parametrize('ready', [True, False])
async def test_owned_crash_is_one_failed_job_only_after_readiness(monkeypatch, ready):
    from news_aggregator.extraction.service_client import ExtractionServiceClient
    calls = []
    async def crashed(*args, **kwargs):
        calls.append('extract')
        return {'outcome': 'failed', 'result': {'state': 'failed', 'content': ''},
                'error': {'code': 'native_browser_target_crashed', 'retryable': False}}
    async def check(_endpoint):
        calls.append('ready')
        if not ready:
            raise shadow.ShadowError('native_readiness_lost')
    monkeypatch.setattr(ExtractionServiceClient, 'extract', crashed)
    monkeypatch.setattr(shadow, '_readiness', check)
    if ready:
        result = await shadow._run_c('https://news.example.com/story', 'http://127.0.0.1:8080', 'test-token')
        assert result['error'] == 'native_browser_target_crashed'
        assert result['outcome'] == 'failed' and not result['content']
    else:
        with pytest.raises(shadow.ShadowError, match='native_readiness_lost'):
            await shadow._run_c('https://news.example.com/story', 'http://127.0.0.1:8080', 'test-token')
    assert calls == ['extract', 'ready']


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', [{'outcome': 'success'}, {'content': 'partial'}, {'retryable': True}])
async def test_crash_response_never_accepts_output_or_replay(monkeypatch, bad):
    from news_aggregator.extraction.service_client import ExtractionServiceClient
    async def invalid(*args, **kwargs):
        return {'outcome': bad.get('outcome', 'failed'),
                'result': {'state': 'failed', 'content': bad.get('content', '')},
                'error': {'code': 'native_browser_target_crashed', 'retryable': bad.get('retryable', False)}}
    monkeypatch.setattr(ExtractionServiceClient, 'extract', invalid)
    with pytest.raises(shadow.ShadowError, match='native_browser_unavailable'):
        await shadow._run_c('https://news.example.com/story', 'http://127.0.0.1:8080', 'test-token')
