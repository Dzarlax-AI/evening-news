"""Check the independent extractor HTTP service against frozen captured-input expectations.

Requests contain bounded captured HTML/content and disable browser rendering.
Reports include outcome and match flags, never article text, URLs, or tokens.
"""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from news_aggregator.extraction.service_client import ExtractionServiceClient


MAX_CORPUS_BYTES = 64 * 1024 * 1024
MAX_CASES = 200
MAX_CYCLES = 6
MAX_HTML_BYTES = 5 * 1024 * 1024
MAX_TEXT_CHARS = 100000
MAX_BUDGET_SECONDS = 30
MAX_CASE_ID_CHARS = 96
SUPPORTED_SOURCE_TYPES = {"article", "web"}
# Only direct captured-input strategies are accepted, so the service cannot
# fetch alternate URLs as part of this offline contract gate.
SUPPORTED_STRATEGIES = {
    "source_content", "enhanced_selectors", "readability", "json_ld",
    "open_graph", "meta_description", "enhanced_heuristics", "fallback_extraction",
}
ALLOWED_INPUT_FIELDS = {"url", "html", "content", "title", "strategy"}
EXPECTED_STATES = {"extracted", "failed", "resource_limit"}


def _load_corpus(path):
    corpus_path = Path(path)
    if corpus_path.stat().st_size > MAX_CORPUS_BYTES:
        raise ValueError("corpus_exceeds_64_mib")
    raw = corpus_path.read_bytes()
    corpus = json.loads(raw)
    cases = corpus.get("cases") if isinstance(corpus, dict) else None
    if not isinstance(corpus, dict) or corpus.get("version") != 1 or not isinstance(cases, list) or not cases:
        raise ValueError("expected_nonempty_version_1_corpus")
    if len(cases) > MAX_CASES:
        raise ValueError("corpus_exceeds_200_cases")
    ids = [case.get("id") for case in cases if isinstance(case, dict)]
    if len(ids) != len(cases) or any(not _safe_case_id(case_id) for case_id in ids):
        raise ValueError("case_id_must_be_bounded_safe_identifier")
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate_case_id")
    return corpus, hashlib.sha256(raw).hexdigest()


def _safe_case_id(case_id):
    return (isinstance(case_id, str)
            and re.fullmatch(r"[A-Za-z0-9._-]{1,96}", case_id) is not None
            and re.search(r"token|secret|api[_-]?key|bearer|https?://", case_id, re.IGNORECASE) is None)


def _is_go_challenge_html(value):
    lowered = value.lower()
    return (any(marker in lowered for marker in (
        'id="challenge-form"', "id='challenge-form'",
        'id="cf-challenge-running"', "id='cf-challenge-running'",
    )) or ("<title>just a moment" in lowered and "cf_chl_" in lowered))


def _select_cases(corpus, case_ids):
    cases = corpus["cases"]
    if case_ids:
        if len(set(case_ids)) != len(case_ids):
            raise ValueError("duplicate_case_selection")
        by_id = {case["id"]: case for case in cases}
        missing = sorted(set(case_ids) - by_id.keys())
        if missing:
            raise ValueError("unknown_case_id")
        cases = [by_id[case_id] for case_id in case_ids]
    if not cases:
        raise ValueError("no_selected_cases")
    for case in cases:
        _validate_case(case)
    return cases


def _validate_case(case):
    case_id = case.get("id", "")
    if not _safe_case_id(case_id):
        raise ValueError("case_id_must_be_bounded_safe_identifier")
    source_type = case.get("source_type", "article")
    if not isinstance(source_type, str) or source_type not in SUPPORTED_SOURCE_TYPES:
        raise ValueError("unsupported_or_mixed_source_type")
    data = case.get("input")
    if not isinstance(data, dict):
        raise ValueError("case_input_must_be_object")
    if set(data) - ALLOWED_INPUT_FIELDS:
        raise ValueError("unsupported_fixture_input_field")
    strategy = data.get("strategy", "auto")
    if not isinstance(strategy, str) or strategy not in SUPPORTED_STRATEGIES:
        raise ValueError("strategy_not_supported_for_offline_http_replay")
    html, content = data.get("html"), data.get("content")
    if (html is None) == (content is None):
        raise ValueError("case_requires_exactly_one_captured_html_or_content_input")
    if not isinstance(html if html is not None else content, str):
        raise ValueError("captured_input_must_be_text")
    if html is not None and strategy == "source_content":
        raise ValueError("source_content_requires_captured_content")
    if content is not None and strategy != "source_content":
        raise ValueError("captured_content_requires_source_content_strategy")
    if len((html if html is not None else content).encode("utf-8")) > MAX_HTML_BYTES:
        raise ValueError("captured_input_exceeds_5_mib")
    if html is not None and not html.strip():
        raise ValueError("captured_html_must_not_be_empty")
    if html is not None and _is_go_challenge_html(html):
        raise ValueError("captured_html_is_challenge_interstitial")
    if "title" in data and not isinstance(data["title"], str):
        raise ValueError("title_must_be_text")
    expected = case.get("expected")
    if not isinstance(expected, dict) or expected.get("state") not in EXPECTED_STATES:
        raise ValueError("case_requires_supported_expected_state")
    allowed_expectations = {
        "state", "contains", "not_contains", "fields", "method", "error", "trace_contains",
    }
    if set(expected) - allowed_expectations:
        raise ValueError("unsupported_fixture_expectation_field")
    if "fields" in expected and not isinstance(expected["fields"], dict):
        raise ValueError("expected_fields_must_be_object")
    if "method" in expected and not isinstance(expected["method"], str):
        raise ValueError("expected_method_must_be_text")
    if "error" in expected and not isinstance(expected["error"], str):
        raise ValueError("expected_error_must_be_text")
    if "trace_contains" in expected and (
            not isinstance(expected["trace_contains"], list)
            or any(not isinstance(value, str) for value in expected["trace_contains"])):
        raise ValueError("expected_trace_contains_must_be_text_list")
    for key in ("contains", "not_contains"):
        values = expected.get(key, [])
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            raise ValueError("expected_anchors_must_be_text_lists")
    url = data.get("url")
    if not isinstance(url, str) or not re.fullmatch(r"https?://[^\s/?#]+(?:/[^\s]*)?", url):
        raise ValueError("case_requires_http_url")


def _projected_match(expected, actual):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and _projected_match(value, actual[key]) for key, value in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(expected) == len(actual) and all(
            _projected_match(left, right) for left, right in zip(expected, actual))
    if expected is None and actual == "":
        actual = None
    if actual is None and expected == "":
        expected = None
    return expected == actual


def _expected_matches(expected, result, state):
    content = result.get("content") or ""
    normalized = " ".join(content.split())
    fields = expected.get("fields", {})
    method = result.get("method_used") or result.get("method") or ""
    return (state == expected["state"]
            and all(" ".join(value.split()) in normalized for value in expected.get("contains", []))
            and all(" ".join(value.split()) not in normalized for value in expected.get("not_contains", []))
            and all(key in result and _projected_match(value, result[key]) for key, value in fields.items())
            and ("method" not in expected or expected["method"] == method)
            and (not expected.get("error")
                 or expected["error"] in (result.get("error") or ""))
            and all(value in (result.get("trace") or [])
                    for value in expected.get("trace_contains", [])))


def _service_state(payload):
    state = payload["result"].get("state")
    if state is None:
        state = "extracted" if payload.get("outcome") in {"success", "partial"} else "failed"
    if state not in EXPECTED_STATES:
        raise ValueError("service_result_requires_supported_state")
    return state


async def run(args):
    if not 1 <= args.cycles <= MAX_CYCLES:
        raise ValueError("cycles_must_be_between_1_and_6")
    token = os.environ.get(args.token_env)
    if not token:
        raise ValueError("missing_token_environment_variable")
    corpus, corpus_sha = _load_corpus(args.corpus)
    cases = _select_cases(corpus, args.case)
    client = ExtractionServiceClient(args.endpoint, token, max_bytes=MAX_HTML_BYTES,
                                     max_chars=MAX_TEXT_CHARS)
    rows = []
    for cycle in range(1, args.cycles + 1):
        for case in cases:
            data = case["input"]
            request_data = {key: data[key] for key in ("url", "html", "content", "title", "strategy") if key in data}
            payload = await client.extract(
                request_data["url"], timeout_seconds=MAX_BUDGET_SECONDS,
                html=request_data.get("html"), content=request_data.get("content"),
                title=request_data.get("title"), strategy=request_data.get("strategy", "auto"),
                source_type=case.get("source_type", "article"), use_browser=False,
            )
            expected = case["expected"]
            state = _service_state(payload)
            matched = _expected_matches(expected, payload["result"], state)
            row = {"id": case["id"], "cycle": cycle, "state": state,
                   "outcome": payload["outcome"],
                   "method": payload["result"].get("method_used") or payload["result"].get("method") or "",
                   "content_chars": min(len(payload["result"].get("content") or ""), MAX_TEXT_CHARS),
                   "expected_match": matched, "mismatch": not matched,
                   "classification": "matched" if matched else "expected_mismatch"}
            rows.append(row)
    report = {
        "version": 1,
        "corpus_sha256": corpus_sha,
        "case_ids": [case["id"] for case in cases],
        "cycles": args.cycles,
        "records": rows,
        "mismatches": sum(row["mismatch"] for row in rows),
        "scope": "captured input checked against frozen expectations; independent HTTP service; browser disabled",
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return int(report["mismatches"] > 0)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--case", action="append", default=[], help="Explicit case ID; repeat to select several")
    parser.add_argument("--token-env", default="EXTRACTOR_TEST_TOKEN",
                        help="Environment variable containing the service token")
    args = parser.parse_args(argv)
    try:
        return asyncio.run(run(args))
    except Exception as exc:
        # Do not print exception details from the HTTP client, which could
        # include input URLs or service response material.
        print(json.dumps({"error": type(exc).__name__, "code": "comparison_failed"}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
