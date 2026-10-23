"""Keep the parity harness honest about source semantics and offline operation."""

import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "compare_extraction_replays.py"
SPEC = importlib.util.spec_from_file_location("go_extraction_replay", SCRIPT)
replay_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay_module)


def test_frozen_goldens_cover_the_complete_go_corpus_with_pinned_inputs():
    cases = replay_module.load_corpus(ROOT / "tests/fixtures/go-parity/manifest.json")
    goldens = replay_module.load_goldens(ROOT / "docs/extraction-golden-cases.json", cases)
    assert len(cases) == 72
    assert set(goldens) == set(cases)
    assert all(row["case_sha256"] == replay_module.fingerprint(cases[case_id])
               for case_id, row in goldens.items())


def test_go_replay_rejects_missing_or_extra_case_cycle_records():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "extracted", "contains": ["required"]}}}
    valid = {("fixture", 1): {"passed": True, "failures": [],
                               "result": {"state": "extracted", "content": "required text"}}}
    with pytest.raises(ValueError, match="case_or_cycle_set_mismatch"):
        replay_module.check_replay({}, cases, goldens, 1)
    with pytest.raises(ValueError, match="case_or_cycle_set_mismatch"):
        replay_module.check_replay({**valid, ("unexpected", 1): valid[("fixture", 1)]}, cases, goldens, 1)


def test_go_replay_requires_success_state_and_every_frozen_content_anchor():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "extracted", "contains": ["required anchor"]}}}
    record = {"passed": True, "failures": [],
              "result": {"state": "extracted", "content": "Only a different sentence."}}
    report = replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)
    assert report["gate"] == "requires_review"
    assert report["golden_failures"] == [{"id": "fixture", "cycle": 1}]
    record["result"]["content"] = "required anchor appears here"
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "passed"


def test_media_projection_preserves_ids_and_source_metadata():
    expected = [{"type": "contact", "url": "#contact_random", "contact_data": {"phone": "123"},
                 "source": "contact"}]
    assert replay_module.projected_match(expected, expected.copy())
    changed = [{**expected[0], "source": "wrong"}]
    assert not replay_module.projected_match(expected, changed)


def test_go_replay_normalizes_content_whitespace_only_for_source_anchors():
    assert replay_module.canonical_content("  Independent\n\tmeasurements  ") == "Independent measurements"
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "extracted", "contains": ["independent measurements"]}}}
    record = {"passed": True, "failures": [],
              "result": {"state": "extracted", "content": "Independent\n measurements"}}
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "requires_review"


def test_go_golden_projection_checks_exact_metadata_and_media_fields():
    expected = {"title": "River report", "media": [{"type": "photo", "url": "https://img.example/x.jpg"}]}
    actual = {"title": "River report", "media": [{"type": "photo", "url": "https://img.example/x.jpg",
                                                     "alt_text": "Caption"}]}
    assert replay_module.projected_match(expected, actual)
    assert not replay_module.projected_match({"media": [{"type": "photo", "url": "wrong"}]}, actual)


def test_go_replay_failed_expectation_is_checked_as_a_real_outcome():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "failed", "not_contains": ["partial body"]}}}
    record = {"passed": True, "failures": [], "result": {"state": "failed", "content": ""}}
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "passed"
    record["result"]["content"] = "partial body captured"
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "requires_review"


def test_go_golden_requires_media_even_when_text_is_empty():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {
        "state": "extracted", "media_contains": [{"type": "photo", "url": "https://cdn.example/photo.jpg"}]}}}
    record = {"passed": True, "failures": [],
              "result": {"state": "extracted", "content": "", "media": []}}
    report = replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)
    assert report["golden_failures"] == [{"id": "fixture", "cycle": 1}]
    record["result"]["media"] = [{"type": "photo", "url": "https://cdn.example/photo.jpg"}]
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "passed"


def test_go_replay_pass_flag_cannot_override_a_failed_golden_check():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "extracted", "contains": ["complete result"]}}}
    record = {"passed": True, "failures": [], "result": {"state": "failed", "content": ""}}
    report = replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)
    assert report["gate"] == "requires_review"
    record["passed"] = False
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "requires_review"


def test_golden_case_hash_detects_manifest_drift():
    cases = {"fixture": {"id": "fixture", "input": {"content": "original"}}}
    golden = {"version": 1, "cases": [{"id": "fixture",
        "case_sha256": replay_module.fingerprint(cases["fixture"]), "evidence": "fixture contract",
        "expected": {"state": "extracted", "contains": ["original"]}}]}
    path = ROOT / "tests/fixtures/go-parity/manifest.json"
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        corpus_path = Path(directory) / "corpus.json"
        golden_path = Path(directory) / "golden.json"
        corpus_path.write_text(json.dumps({"version": 1, "cases": list(cases.values())}), encoding="utf-8")
        golden_path.write_text(json.dumps(golden), encoding="utf-8")
        loaded = replay_module.load_corpus(corpus_path)
        assert set(replay_module.load_goldens(golden_path, loaded)) == {"fixture"}
        loaded["fixture"]["input"]["content"] = "changed"
        with pytest.raises(ValueError, match="golden_case_hash_mismatch"):
            replay_module.load_goldens(golden_path, loaded)


def test_media_attachment_does_not_erase_a_frozen_rejected_body_state():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "failed", "media_contains": [{"type": "video"}]}}}
    record = {"passed": True, "failures": [],
              "result": {"state": "extracted", "content": "", "media": [{"type": "video"}]}}
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "requires_review"


def test_go_golden_requires_nested_metadata_fields_to_match():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {
        "state": "extracted", "fields": {"author": {"name": "Verified author"}}}}}
    record = {"passed": True, "failures": [], "result": {
        "state": "extracted", "content": "Article body", "author": {"name": "Other author"}}}
    report = replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)
    assert report["golden_failures"] == [{"id": "fixture", "cycle": 1}]


def test_go_replay_resource_limit_remains_a_distinct_expected_state():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "resource_limit", "fields": {"error": "resource_limit"}}}}
    record = {"passed": True, "failures": [], "result": {
        "state": "resource_limit", "content": "", "error": "resource_limit"}}
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "passed"
    record["result"]["state"] = "failed"
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "requires_review"


def test_source_content_golden_requires_the_original_retained_text():
    cases = {"fixture": {"id": "fixture"}}
    goldens = {"fixture": {"expected": {"state": "extracted", "contains": ["Original stored source text"]}}}
    record = {"passed": True, "failures": [],
              "result": {"state": "extracted", "content": "Different generated summary."}}
    assert replay_module.check_replay({("fixture", 1): record}, cases, goldens, 1)["gate"] == "requires_review"


def test_go_replay_preserves_source_specific_presentation_limits(tmp_path):
    body = "Original stored source paragraph. " * 400
    cases = []
    for source_type in ("article", "rss", "telegram", "custom"):
        cases.append({"id": f"retention.{source_type}", "source_type": source_type,
                      "source_name": "Synthetic retained source", "methods": ["source_content"],
                      "provenance": {"kind": "synthetic", "derived_from": ["go/extractor/types.go"]},
                      "input": {"url": "https://fixture.test/source", "strategy": "source_content",
                                "content": body},
                      "expected": {"state": "extracted", "method": "source_content",
                                   "contains": ["Original stored source paragraph."]}})
    corpus_path = tmp_path / "retention-corpus.json"
    replay_path = tmp_path / "retention-replay.jsonl"
    corpus_path.write_text(json.dumps({"version": 1, "cases": cases}), encoding="utf-8")
    subprocess.run(["go", "run", "./cmd/extract-replay", "-corpus", str(corpus_path),
                    "-cycles", "1", "-workers", "2", "-output", str(replay_path)],
                   cwd=ROOT / "go", check=True, capture_output=True, text=True, timeout=120)
    records = replay_module.load_replay(replay_path)
    assert len(records) == 4
    for source_type in ("article", "rss", "telegram", "custom"):
        result = records[(f"retention.{source_type}", 1)]["result"]
        assert result["state"] == "extracted"
        if source_type == "article":
            assert result["truncated"] is True
            assert len(result["content"]) <= 8000
            assert result["truncation_policy"] == "sentence_or_word_8000"
        else:
            assert result["content"] == body


def reviewed_example():
    case = {"id": "frozen.case", "input": {"content": "Captured source"}}
    golden = {"id": case["id"], "case_sha256": replay_module.fingerprint(case),
              "evidence": "Pinned structural fixture",
              "expected": {"state": "extracted", "contains": ["Required introduction", "Complete body"],
                           "not_contains": ["Menus."]}}
    records = {(case["id"], 1): {"passed": True, "failures": [],
                                 "result": {"state": "extracted",
                                            "content": "Required introduction. Complete body."}}}
    return case, records, golden


def test_frozen_source_expectations_accept_only_the_complete_reviewed_result():
    case, records, golden = reviewed_example()
    assert replay_module.golden_matches(golden["expected"], records[(case["id"], 1)]["result"])
    records[(case["id"], 1)]["result"]["content"] = "Complete body."
    assert not replay_module.golden_matches(golden["expected"], records[(case["id"], 1)]["result"])


def test_changing_source_invalidates_its_frozen_golden_pin(tmp_path):
    case, _records, golden = reviewed_example()
    corpus_path = tmp_path / "corpus.json"
    golden_path = tmp_path / "golden.json"
    corpus_path.write_text(json.dumps({"version": 1, "cases": [case]}), encoding="utf-8")
    golden_path.write_text(json.dumps({"version": 1, "cases": [golden]}), encoding="utf-8")
    cases = replay_module.load_corpus(corpus_path)
    assert replay_module.load_goldens(golden_path, cases)
    case["input"]["content"] = "Changed captured source"
    corpus_path.write_text(json.dumps({"version": 1, "cases": [case]}), encoding="utf-8")
    with pytest.raises(ValueError, match="golden_case_hash_mismatch"):
        replay_module.load_goldens(golden_path, replay_module.load_corpus(corpus_path))


def test_go_assertion_failure_cannot_be_waived_by_matching_golden():
    case, records, golden = reviewed_example()
    record = records[(case["id"], 1)]
    record.update(passed=False, failures=["source expectation failed"])
    assert replay_module.golden_matches(golden["expected"], record["result"])
    report = replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 1)
    assert report["golden_failures"] == [{"id": case["id"], "cycle": 1}]


def test_golden_loader_rejects_unknown_case_ids_and_extra_fields(tmp_path):
    case, _records, golden = reviewed_example()
    corpus_path = tmp_path / "corpus.json"
    golden_path = tmp_path / "golden.json"
    corpus_path.write_text(json.dumps({"version": 1, "cases": [case]}), encoding="utf-8")
    cases = replay_module.load_corpus(corpus_path)
    golden["id"] = "not-in-corpus"
    golden_path.write_text(json.dumps({"version": 1, "cases": [golden]}), encoding="utf-8")
    with pytest.raises(ValueError, match="cover_every_corpus_case"):
        replay_module.load_goldens(golden_path, cases)
    golden["id"] = case["id"]
    golden["allow_any"] = True
    golden_path.write_text(json.dumps({"version": 1, "cases": [golden]}), encoding="utf-8")
    with pytest.raises(ValueError, match="exact_fields"):
        replay_module.load_goldens(golden_path, cases)


def test_each_go_replay_cycle_must_pass_independently():
    case, records, golden = reviewed_example()
    records[(case["id"], 2)] = {"passed": False, "failures": ["fixture failure"],
                                "result": records[(case["id"], 1)]["result"]}
    report = replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 2)
    assert report["record_count"] == 2
    assert report["golden_failures"] == [{"id": case["id"], "cycle": 2}]


def test_equal_incomplete_replays_cannot_omit_a_corpus_case():
    case, records, golden = reviewed_example()
    with pytest.raises(ValueError, match="case_or_cycle_set_mismatch"):
        replay_module.check_replay(records, {case["id"]: case, "missing": {"id": "missing"}},
                                   {case["id"]: golden, "missing": golden}, 1)


def test_equal_replays_cannot_silently_omit_required_cycles():
    case, records, golden = reviewed_example()
    with pytest.raises(ValueError, match="case_or_cycle_set_mismatch"):
        replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 2)


def test_source_golden_runs_even_if_both_implementations_agree():
    case, records, golden = reviewed_example()
    report = replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 1)
    assert report["gate"] == "passed"
    golden["expected"]["contains"].append("Missing source introduction")
    report = replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 1)
    assert report["golden_failures"] and report["gate"] == "requires_review"


def test_source_golden_cannot_omit_a_corpus_case(tmp_path):
    case, _records, _golden = reviewed_example()
    corpus_path = tmp_path / "corpus.json"
    golden_path = tmp_path / "golden.json"
    corpus_path.write_text(json.dumps({"version": 1, "cases": [case]}), encoding="utf-8")
    golden_path.write_text(json.dumps({"version": 1, "cases": []}), encoding="utf-8")
    cases = replay_module.load_corpus(corpus_path)
    with pytest.raises(ValueError, match="goldens_must_cover_every_corpus_case"):
        replay_module.load_goldens(golden_path, cases)


def test_source_golden_requires_inline_media_even_when_both_outputs_omit_it():
    case, records, golden = reviewed_example()
    golden["expected"]["media_contains"] = [{"type": "image", "url": "https://source.test/photo.jpg"}]
    report = replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 1)
    assert report["golden_failures"]
    records[(case["id"], 1)]["result"]["media"] = [
        {"type": "image", "url": "https://source.test/photo.jpg", "alt_text": "Independent observation"}]
    assert not replay_module.check_replay(records, {case["id"]: case}, {case["id"]: golden}, 1)["golden_failures"]
