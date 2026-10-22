"""Compare every replay case, including fields absent from fixture assertions.

Nonzero exit means review is required, not that either implementation is right.
This deliberately makes no total-system memory or quality claim.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re


def load(path):
    rows = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        key = (value["id"], value["cycle"])
        if key in rows:
            raise ValueError(f"Duplicate replay result {key}")
        rows[key] = value
    if not rows:
        raise ValueError("Replay must not be empty")
    return rows


def canonical(field, value):
    if field == "media":
        items = []
        for item in value or []:
            item = dict(item)
            for key in ("thumbnail", "alt_text", "title", "duration"):
                if item.get(key) in {None, ""}:
                    item.pop(key, None)
            if item.get("type") in {"poll", "location", "contact"}:
                item.pop("url", None)
            items.append(item)
        return items
    if value is None:
        return ""
    if field == "content":
        return re.sub(r"\s+", " ", value).strip()
    if field == "publication_date" and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()
        except ValueError:
            return value
    return value


def fingerprint(value):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_reviews(path):
    policy = json.loads(Path(path).read_text(encoding="utf-8"))
    if set(policy) != {"version", "reviews"} or policy["version"] != 1 or not isinstance(policy["reviews"], list):
        raise ValueError("Expected version 1 with explicit reviews")
    reviews, seen = [], set()
    required = {"kind", "case_id", "case_sha256", "python_sha256", "go_sha256", "classification", "reason", "evidence", "golden"}
    for review in policy["reviews"]:
        keys = required | ({"field"} if review.get("kind") == "difference" else set())
        if set(review) != keys or review["kind"] not in {"difference", "baseline_failure"}:
            raise ValueError("Review requires exact fields and a supported kind")
        if review["classification"] not in {"equivalent", "improvement", "baseline_defect"}:
            raise ValueError("Unknown review classification")
        if review["kind"] == "difference" and review["field"] not in {"state", "content", "url", "title", "publication_date", "author", "description", "image_url", "media"}:
            raise ValueError("Unknown reviewed field")
        for key in ("case_id", "reason", "evidence"):
            if not isinstance(review[key], str) or not review[key].strip() or "*" in review["case_id"]:
                raise ValueError("Review requires a specific case and source evidence")
        for key in ("case_sha256", "python_sha256", "go_sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", review[key]):
                raise ValueError("Review requires exact SHA-256 pins")
        golden = review["golden"]
        if not isinstance(golden, dict) or set(golden) - {"state", "contains", "not_contains", "fields"} or golden.get("state") not in {"extracted", "failed", "resource_limit"}:
            raise ValueError("Review requires an explicit golden outcome")
        for key in ("contains", "not_contains"):
            if not isinstance(golden.get(key, []), list) or any(not isinstance(v, str) or not v.strip() for v in golden.get(key, [])):
                raise ValueError("Golden anchors must be nonempty strings")
        if not isinstance(golden.get("fields", {}), dict):
            raise ValueError("Golden fields must be an object")
        if golden["state"] == "extracted" and not golden.get("contains") and not golden.get("fields"):
            raise ValueError("Successful golden outcomes require source anchors or fields")
        identity = (review["case_id"], review["kind"], review.get("field"))
        if identity in seen:
            raise ValueError("Duplicate review identity")
        seen.add(identity)
        reviews.append(review)
    return reviews


def golden_matches(golden, result):
    def projected(expected, actual):
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(k in actual and projected(v, actual[k]) for k, v in expected.items())
        if isinstance(expected, list):
            return isinstance(actual, list) and len(expected) == len(actual) and all(projected(a, b) for a, b in zip(expected, actual))
        return expected == actual
    content = canonical("content", result.get("content"))
    return (result.get("state") == golden["state"]
            and all(canonical("content", v) in content for v in golden.get("contains", []))
            and all(canonical("content", v) not in content for v in golden.get("not_contains", []))
            and projected(golden.get("fields", {}), result)
            and all(any(projected(required, item) for item in result.get("media", []) or [])
                    for required in golden.get("media_contains", [])))


def load_goldens(path):
    policy = json.loads(Path(path).read_text(encoding="utf-8"))
    if set(policy) != {"version", "cases"} or policy["version"] != 1 or not isinstance(policy["cases"], list):
        raise ValueError("Expected version-1 golden cases")
    result = {}
    for case in policy["cases"]:
        if set(case) != {"id", "case_sha256", "evidence", "expected"} or case["id"] in result:
            raise ValueError("Golden cases require unique exact fields")
        if (not isinstance(case["id"], str) or not case["id"].strip() or "*" in case["id"]
                or not isinstance(case["evidence"], str) or not case["evidence"].strip()
                or not re.fullmatch(r"[0-9a-f]{64}", case["case_sha256"])):
            raise ValueError("Golden cases require source evidence and input pins")
        expected = case["expected"]
        if not isinstance(expected, dict) or set(expected) - {"state", "contains", "not_contains", "fields", "media_contains"} or expected.get("state") not in {"extracted", "failed", "resource_limit"}:
            raise ValueError("Invalid golden expectation")
        if not isinstance(expected.get("fields", {}), dict):
            raise ValueError("Golden fields must be an object")
        if not isinstance(expected.get("media_contains", []), list) or any(not isinstance(item, dict) or not item for item in expected.get("media_contains", [])):
            raise ValueError("Required media must be nonempty object projections")
        for key in ("contains", "not_contains"):
            if not isinstance(expected.get(key, []), list) or any(not isinstance(v, str) or not v.strip() for v in expected.get(key, [])):
                raise ValueError("Golden anchors must be nonempty strings")
        if expected["state"] == "extracted" and not expected.get("contains") and not expected.get("fields") and not expected.get("media_contains"):
            raise ValueError("Successful golden outcomes require source anchors or fields")
        result[case["id"]] = case
    return result


def compare(left, right, reviews=None, cases=None, expected_cycles=None, goldens=None):
    if not left or not right:
        raise ValueError("Replay must not be empty")
    if left.keys() != right.keys():
        raise ValueError(f"Different case/cycle sets: left-only={sorted(left.keys()-right.keys())}, "
                         f"right-only={sorted(right.keys()-left.keys())}")
    if cases is not None and {key[0] for key in left} != set(cases):
        raise ValueError("Observed case IDs do not match the supplied corpus")
    if cases is not None or expected_cycles is not None:
        if expected_cycles is not None and expected_cycles < 1:
            raise ValueError("Expected cycles must be positive")
        cycles = {key[1] for key in left}
        if any(not isinstance(cycle, int) or isinstance(cycle, bool) or cycle < 1 for cycle in cycles):
            raise ValueError("Cycles must be positive integers")
        required = set(range(1, (expected_cycles if expected_cycles is not None else max(cycles)) + 1))
        for case_id in {key[0] for key in left}:
            if {key[1] for key in left if key[0] == case_id} != required:
                raise ValueError("Observed cycles do not match the required cycles for every case")
    if reviews and not cases:
        raise ValueError("Reviewed comparison requires the exact corpus")
    if goldens is not None and (not cases or not set(cases).issubset(goldens)):
        raise ValueError("Golden policy must cover every supplied corpus case")
    failures, differences, accepted, baseline_defects, golden_failures = [], [], [], [], []
    indexed = {(r["case_id"], r["kind"], r.get("field")): r for r in reviews or []}
    def reviewed(case_id, kind, field, av, bv, record):
        rule = indexed.get((case_id, kind, field))
        return (rule is not None and case_id in cases
                and rule["case_sha256"] == fingerprint(cases[case_id])
                and rule["python_sha256"] == fingerprint(av) and rule["go_sha256"] == fingerprint(bv)
                and record.get("passed") is True and not record.get("failures")
                and golden_matches(rule["golden"], record["result"]))
    for key in sorted(left):
        a, b = left[key], right[key]
        if goldens is not None:
            golden = goldens[key[0]]
            if (golden["case_sha256"] != fingerprint(cases[key[0]]) or b.get("passed") is not True
                    or b.get("failures") or not golden_matches(golden["expected"], b["result"])):
                golden_failures.append({"id": key[0], "cycle": key[1], "evidence": golden["evidence"]})
        if a.get("errors") or b.get("failures") or b.get("passed") is False:
            failure = {"id": key[0], "cycle": key[1], "left": a.get("errors"), "right": b.get("failures")}
            if a.get("errors") and reviewed(key[0], "baseline_failure", None, a["errors"], b["result"], b):
                baseline_defects.append({**failure, "review": indexed[(key[0], "baseline_failure", None)]})
            else:
                failures.append(failure)
        fields = ["state"]
        if a["result"].get("state") == b["result"].get("state") == "extracted":
            fields += ["content", "url", "title", "publication_date", "author", "description", "image_url", "media"]
        for field in fields:
            av, bv = a["result"].get(field), b["result"].get(field)
            if canonical(field, av) != canonical(field, bv):
                difference = {"id": key[0], "cycle": key[1], "field": field, "python": av, "go": bv}
                if reviewed(key[0], "difference", field, canonical(field, av), canonical(field, bv), b):
                    accepted.append({**difference, "review": indexed[(key[0], "difference", field)]})
                else:
                    differences.append(difference)
    return {"record_pairs": len(left), "assertion_failures": failures,
            "reviewed_differences": accepted, "reviewed_baseline_defects": baseline_defects,
            "golden_failures": golden_failures,
            "unreviewed_differences": differences, "decision_gate": "open",
            "comparison_gate": "passed" if not failures and not differences and not golden_failures else "requires_review",
            "scope": "offline component replay; source provenance is in the corpus; not deployed-system parity",
            "normalization": "content whitespace; aware/naive-UTC dates; null/empty metadata; empty optional media fields; poll/contact/location synthetic IDs only"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", required=True)
    parser.add_argument("--go", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--decisions", help="Source-reviewed, exact-hash-pinned decisions; never a wildcard allowlist")
    parser.add_argument("--corpus", help="Exact versioned input corpus required by reviewed decisions")
    parser.add_argument("--cycles", type=int, help="Required complete cycle count for every corpus case")
    parser.add_argument("--golden", help="Source-backed expectations checked even when both implementations agree")
    args = parser.parse_args()
    if (args.decisions or args.golden) and not args.corpus:
        parser.error("--decisions/--golden require --corpus")
    if args.decisions and not args.golden:
        parser.error("--decisions requires complete --golden source expectations")
    cases = None
    if args.corpus:
        corpus = json.loads(Path(args.corpus).read_text(encoding="utf-8"))
        cases = {c["id"]: c for c in corpus["cases"]}
        if corpus.get("version") != 1 or not cases or len(cases) != len(corpus["cases"]):
            raise ValueError("Expected unique nonempty version-1 corpus")
    report = compare(load(args.python), load(args.go), load_reviews(args.decisions) if args.decisions else None, cases, args.cycles, load_goldens(args.golden) if args.golden else None)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"record_pairs": report["record_pairs"],
                      "assertion_failures": len(report["assertion_failures"]),
                      "reviewed_differences": len(report["reviewed_differences"]),
                      "reviewed_baseline_defects": len(report["reviewed_baseline_defects"]),
                      "golden_failures": len(report["golden_failures"]),
                      "unreviewed_differences": len(report["unreviewed_differences"])}))
    return int(bool(report["assertion_failures"] or report["unreviewed_differences"] or report["golden_failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
