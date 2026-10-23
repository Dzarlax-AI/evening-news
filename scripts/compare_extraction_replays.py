"""Check the Go extractor replay against the frozen, source-backed corpus."""

import argparse
import hashlib
import json
from pathlib import Path
import re


def fingerprint(value):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_content(value):
    return re.sub(r"\s+", " ", value or "").strip()


def projected_match(expected, actual):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and projected_match(value, actual[key]) for key, value in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(expected) == len(actual) and all(
            projected_match(left, right) for left, right in zip(expected, actual))
    return expected == actual


def load_corpus(path):
    corpus = json.loads(Path(path).read_text(encoding="utf-8"))
    cases = corpus.get("cases") if isinstance(corpus, dict) else None
    if not isinstance(cases, list) or not cases or corpus.get("version") != 1:
        raise ValueError("expected_nonempty_version_1_corpus")
    indexed = {}
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str) or not case["id"]:
            raise ValueError("corpus_cases_require_ids")
        if case["id"] in indexed:
            raise ValueError("duplicate_corpus_case_id")
        indexed[case["id"]] = case
    return indexed


def load_goldens(path, cases):
    policy = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = policy.get("cases") if isinstance(policy, dict) else None
    if not isinstance(rows, list) or policy.get("version") != 1:
        raise ValueError("expected_version_1_golden_cases")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"id", "case_sha256", "evidence", "expected"}:
            raise ValueError("goldens_require_exact_fields")
        if row["id"] in result:
            raise ValueError("duplicate_golden_case_id")
        if row["id"] not in cases:
            # The frozen policy also contains retired-live fixtures retained
            # for historical review. Only current corpus rows enter this gate.
            continue
        if row["case_sha256"] != fingerprint(cases[row["id"]]):
            raise ValueError("golden_case_hash_mismatch")
        if not isinstance(row["evidence"], str) or not row["evidence"].strip():
            raise ValueError("golden_requires_source_evidence")
        expected = row["expected"]
        if (not isinstance(expected, dict)
                or set(expected) - {"state", "contains", "not_contains", "fields", "media_contains"}
                or expected.get("state") not in {"extracted", "failed", "resource_limit"}):
            raise ValueError("golden_requires_supported_expected_result")
        for key in ("contains", "not_contains"):
            values = expected.get(key, [])
            if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
                raise ValueError("golden_content_anchors_must_be_nonempty_text")
        if not isinstance(expected.get("fields", {}), dict):
            raise ValueError("golden_fields_must_be_object")
        media = expected.get("media_contains", [])
        if not isinstance(media, list) or any(not isinstance(item, dict) or not item for item in media):
            raise ValueError("golden_media_contains_must_be_nonempty_object_list")
        if expected["state"] == "extracted" and not any(
                expected.get(key) for key in ("contains", "fields", "media_contains")):
            raise ValueError("successful_golden_requires_assertions")
        result[row["id"]] = row
    if set(cases) - set(result):
        raise ValueError("goldens_must_cover_every_corpus_case")
    return result


def load_replay(path):
    rows = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        case_id = record.get("case_id", record.get("id"))
        cycle = record.get("cycle")
        if not isinstance(case_id, str) or not isinstance(cycle, int) or isinstance(cycle, bool) or cycle < 1:
            raise ValueError("replay_requires_case_id_and_positive_cycle")
        key = (case_id, cycle)
        if key in rows:
            raise ValueError("duplicate_go_replay_record")
        rows[key] = record
    if not rows:
        raise ValueError("go_replay_must_not_be_empty")
    return rows


def golden_matches(expected, result):
    content = canonical_content(result.get("content"))
    return (result.get("state") == expected["state"]
            and all(canonical_content(anchor) in content for anchor in expected.get("contains", []))
            and all(canonical_content(anchor) not in content for anchor in expected.get("not_contains", []))
            and all(key in result and projected_match(value, result[key])
                    for key, value in expected.get("fields", {}).items())
            and all(any(projected_match(value, item) for item in result.get("media", []) or [])
                    for value in expected.get("media_contains", [])))


def check_replay(records, cases, goldens, cycles):
    if cycles < 1:
        raise ValueError("cycles_must_be_positive")
    expected = {(case_id, cycle) for case_id in cases for cycle in range(1, cycles + 1)}
    if records.keys() != expected:
        raise ValueError("go_replay_case_or_cycle_set_mismatch")
    failures = []
    for (case_id, cycle), record in sorted(records.items()):
        result = record.get("result")
        golden = goldens[case_id]
        if (not isinstance(result, dict) or record.get("passed") is not True
                or record.get("failures") or not golden_matches(golden["expected"], result)):
            failures.append({"id": case_id, "cycle": cycle})
    return {"case_count": len(cases), "cycles": cycles, "record_count": len(records),
            "golden_failures": failures,
            "gate": "passed" if not failures else "requires_review",
            "scope": "offline Go extractor replay checked against frozen source-backed expectations"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", required=True, help="Go extract-replay JSONL output")
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--cycles", type=int, required=True)
    parser.add_argument("--golden", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    cases = load_corpus(args.corpus)
    goldens = load_goldens(args.golden, cases)
    report = check_replay(load_replay(args.go), cases, goldens, args.cycles)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"case_count": report["case_count"], "cycles": report["cycles"],
                      "record_count": report["record_count"],
                      "golden_failures": len(report["golden_failures"])}))
    return int(bool(report["golden_failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
