"""Contract checks for the frozen Python article-processing v1 corpus."""

import json
from pathlib import Path

import pytest


CORPUS = Path(__file__).parent / "fixtures" / "article-processing-v1"
REQUIRED_CASES = {
    "partial-summary-survives-category-failure",
    "busy-refunds-attempt-and-sanitizes-retry-after",
    "numeric-string-retry-after-is-parsed",
    "missing-summary-is-retryable-without-invented-fields",
    "ad-verdict-preserves-valid-category-without-summary",
    "already-processed-ad-is-terminal-without-provider-call",
    "smart-filter-skip-is-terminal-without-fabricated-summary",
    "scientific-title-survives-unavailable-extraction",
    "improved-extraction-metadata-survives-following-ai-error",
    "forced-refresh-challenge-is-retryable-and-not-persisted",
    "genuine-failure-exhausts-on-final-claim",
    "confirmed-ad-is-terminal-rejection",
    "challenge-content-never-reaches-provider",
    "unicode-codepoint-limit-rejects-oversize",
    "category-normalization-preserves-alignment-and-labels",
    "source-type-path-uses-ai-and-normalizes-analysis",
    "force-refresh-analyzes-fresh-body-and-keeps-archive",
}


def _fixtures():
    return [json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(CORPUS.glob("*.json"))]


@pytest.mark.parametrize("fixture", _fixtures(), ids=lambda item: item["case"])
def test_golden_fixture_has_complete_v1_shape_and_frozen_result(fixture):
    assert fixture["version"] == 1
    assert fixture["scenario"]["pipeline"] in {
        "analyze", "summary_by_source_type", "force_refresh",
    }
    assert {"outcome", "persistence", "stats", "calls"} <= fixture["expected"].keys()
    assert {"state", "error", "retry_after_seconds", "fields"} <= fixture["expected"]["outcome"].keys()
    assert {"processing_state", "processing_attempts", "next_attempt_after_seconds",
            "last_processing_error", "processed"} <= fixture["expected"]["persistence"].keys()
    assert fixture["expected"]["outcome"]["state"] in {
        "completed", "rejected", "retry_wait",
    }


def test_golden_corpus_covers_required_policy_edges_and_synthetic_data_only():
    fixtures = _fixtures()
    assert len(fixtures) >= len(REQUIRED_CASES)
    assert REQUIRED_CASES <= {fixture["case"] for fixture in fixtures}
    for fixture in fixtures:
        url = fixture["article"].get("url", "")
        assert (url.startswith("https://example.test/") or url.startswith("synthetic://")
                or url == "https://arxiv.org/abs/synthetic-115")
        assert "expected" in fixture and fixture["expected"]
        assert fixture["scenario"].get("attempts", 1) >= 1


def test_frozen_retry_and_force_extraction_semantics_are_explicit():
    fixtures = {fixture["case"]: fixture for fixture in _fixtures()}
    busy = fixtures["busy-refunds-attempt-and-sanitizes-retry-after"]["expected"]
    assert busy["persistence"]["processing_attempts"] == 2
    assert busy["persistence"]["next_attempt_after_seconds"] == 60

    numeric_retry = fixtures["numeric-string-retry-after-is-parsed"]["expected"]
    assert numeric_retry["persistence"]["next_attempt_after_seconds"] == 3600

    exhausted = fixtures["genuine-failure-exhausts-on-final-claim"]["expected"]
    assert exhausted["outcome"]["state"] == "retry_wait"
    assert exhausted["persistence"]["processing_state"] == "exhausted"

    forced = fixtures["force-refresh-analyzes-fresh-body-and-keeps-archive"]
    assert forced["expected"]["calls"][-1]["content"] == forced["scenario"]["extracted"]["content"]
    assert "content" not in forced["expected"]["outcome"]["fields"]
