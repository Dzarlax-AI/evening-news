"""The independent comparator only sends captured, bounded inputs and emits safe summaries."""

import importlib.util
import json
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "compare_extractor_service.py"
SPEC = importlib.util.spec_from_file_location("compare_extractor_service", SCRIPT)
compare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare)


def article_case(expected=None, **input_fields):
    return {"id": "captured.article", "source_type": "article", "expected": expected or {
        "state": "extracted", "contains": ["capture"]}, "input": {
        "url": "https://fixture.example.test/article", **input_fields}}


def test_rejects_mixed_sources_and_unsupported_fixture_injection():
    with pytest.raises(ValueError, match="unsupported_or_mixed_source_type"):
        compare._select_cases({"cases": [article_case(strategy="readability", html="<p>capture</p>"), {
            "id": "rss.item", "source_type": "rss", "input": {"strategy": "auto"}}]}, [])
    with pytest.raises(ValueError, match="unsupported_fixture_input_field"):
        compare._validate_case(article_case(strategy="readability", html="<p>capture</p>",
                                            rendered_html="must not be accepted"))


def test_rejects_nested_source_type_due_to_top_level_contract():
    with pytest.raises(ValueError, match="unsupported_fixture_input_field"):
        compare._validate_case(article_case(strategy="readability", html="<p>capture</p>",
                                            source_type="web"))


def test_rejects_unrecognized_fixture_expectation_fields():
    case = article_case(strategy="readability", html="<p>capture</p>")
    case["expected"]["unverified"] = "must not be silently ignored"
    with pytest.raises(ValueError, match="unsupported_fixture_expectation_field"):
        compare._validate_case(case)


def test_rejects_auto_pipeline_to_prevent_alternate_live_fetches():
    with pytest.raises(ValueError, match="strategy_not_supported_for_offline_http_replay"):
        compare._validate_case(article_case(strategy="auto", html="<article>capture</article>"))


def test_rejects_missing_or_oversized_captured_payload():
    with pytest.raises(ValueError, match="exactly_one_captured"):
        compare._validate_case(article_case(strategy="readability"))
    with pytest.raises(ValueError, match="exceeds_5_mib"):
        compare._validate_case(article_case(strategy="readability", html="x" * (5 * 1024 * 1024 + 1)))


@pytest.mark.parametrize("html", ["", "  \n\t"])
def test_rejects_empty_html_that_could_trigger_a_go_fetch(html):
    with pytest.raises(ValueError, match="captured_html_must_not_be_empty"):
        compare._validate_case(article_case(strategy="readability", html=html))


@pytest.mark.parametrize("html", [
    '<html><form id="challenge-form"></form></html>',
    "<html><div id='cf-challenge-running'></div></html>",
    "<html><title>Just a moment...</title><script>cf_chl_opt = {}</script></html>",
])
def test_go_challenge_predicate_matches_explicit_interstitial_forms(html):
    assert compare._is_go_challenge_html(html) is True
    assert compare._is_go_challenge_html(
        "<article><p>This report discusses challenge-form markup and cf_chl_ tokens.</p></article>") is False


@pytest.mark.asyncio
@pytest.mark.parametrize("html", [
    '<html><form id="challenge-form"></form></html>',
    "<html><div id='cf-challenge-running'></div></html>",
    "<html><title>Just a moment...</title><script>cf_chl_opt = {}</script></html>",
])
async def test_challenge_capture_is_rejected_before_any_http_request(html, tmp_path, monkeypatch):
    article = article_case(strategy="readability", html=html)
    corpus_path = tmp_path / "challenge-corpus.json"
    corpus_path.write_text(json.dumps({"version": 1, "cases": [article]}), encoding="utf-8")
    monkeypatch.setenv("EXTRACTOR_TEST_TOKEN", "test-token-never-reported")
    requests = []

    async def extract(request):
        requests.append(request)
        return web.json_response({})

    app = web.Application()
    app.router.add_post("/v1/extract", extract)
    async with TestServer(app) as server:
        args = type("Args", (), {"cycles": 1, "token_env": "EXTRACTOR_TEST_TOKEN",
                                  "corpus": str(corpus_path), "case": [],
                                  "endpoint": str(server.make_url("/")).rstrip("/"),
                                  "output": str(tmp_path / "challenge-report.json")})()
        with pytest.raises(ValueError, match="captured_html_is_challenge_interstitial"):
            await compare.run(args)

    assert requests == []


@pytest.mark.parametrize("case_id", ["https://example.test/?token=abc", "article-token-secret", "x" * 97])
def test_rejects_unsafe_or_unbounded_case_ids(case_id):
    case = article_case(strategy="readability", html="<p>capture</p>")
    case["id"] = case_id
    with pytest.raises(ValueError, match="case_id_must_be_bounded_safe_identifier"):
        compare._validate_case(case)


@pytest.mark.asyncio
async def test_real_http_client_sends_capture_without_browser_and_report_is_redacted(tmp_path, monkeypatch):
    secret_url = "https://fixture.example.test/article?private=fixture-query-secret"
    article = article_case(expected={"state": "extracted", "contains": ["Captured article text"]},
                          url=secret_url, strategy="source_content",
                          content="Captured article text with distinct details.")
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps({"version": 1, "cases": [article]}), encoding="utf-8")
    output_path = tmp_path / "report.json"
    monkeypatch.setenv("EXTRACTOR_TEST_TOKEN", "test-token-never-reported")
    requests = []

    async def extract(request):
        assert request.headers["Authorization"] == "Bearer test-token-never-reported"
        payload = await request.json()
        requests.append(payload)
        assert payload["url"] == secret_url
        assert payload["use_browser"] is False
        assert payload["strategy"] == "source_content"
        assert payload["content"] == "Captured article text with distinct details."
        assert "html" not in payload
        return web.json_response({
            "contract_version": "1", "build_version": "test", "request_id": payload["request_id"],
            "outcome": "success", "original_url": secret_url, "final_url": secret_url,
            "result": {"url": secret_url, "content": payload["content"], "method_used": "source_content",
                       "title": "", "publication_date": "", "author": "", "description": "",
                       "image_url": "", "media": []},
            "observations": [], "needs_selector_study": False,
        })

    app = web.Application()
    app.router.add_post("/v1/extract", extract)
    async with TestServer(app) as server:
        args = type("Args", (), {"cycles": 1, "token_env": "EXTRACTOR_TEST_TOKEN",
                                  "corpus": str(corpus_path), "case": [],
                                  "endpoint": str(server.make_url("/" )).rstrip("/"),
                                  "output": str(output_path)})()
        assert await compare.run(args) == 0

    report_text = output_path.read_text(encoding="utf-8")
    report = json.loads(report_text)
    assert len(requests) == 1
    assert report["case_ids"] == ["captured.article"]
    row = report["records"][0]
    assert row["expected_match"] is True
    assert row["state"] == "extracted"
    assert row["content_chars"] == len("Captured article text with distinct details.")
    assert row["classification"] == "matched"
    assert row["mismatch"] is False
    for private in (secret_url, "fixture-query-secret", "Captured article text", "test-token-never-reported"):
        assert private not in report_text


def test_same_rejected_outcome_is_explicit_and_not_an_improvement():
    payload = {"outcome": "rejected", "result": {"state": "failed", "content": ""}}
    assert compare._service_state(payload) == "failed"
    assert compare._expected_matches({"state": "failed"}, payload["result"], "failed") is True


def test_service_failure_cannot_satisfy_a_successful_fixture_expectation():
    expected = {"state": "extracted", "contains": ["required source sentence"]}
    result = {"state": "failed", "content": ""}
    assert compare._expected_matches(expected, result, result["state"]) is False


@pytest.mark.parametrize(("expected", "result"), [
    ({"fields": {"title": "Expected headline"}}, {"title": "Wrong headline"}),
    ({"method": "readability"}, {"method_used": "enhanced_selectors"}),
    ({"error": "quality_rejected"}, {"error": "invalid_html"}),
    ({"trace_contains": ["readability"]}, {"trace": ["enhanced_selectors"]}),
])
def test_nonempty_fixture_expectations_cannot_be_ignored(expected, result):
    expected = {"state": "failed", **expected}
    assert compare._expected_matches(expected, {"content": "", **result}, "failed") is False
