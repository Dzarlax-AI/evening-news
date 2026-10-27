"""Pure reference projections for the retired Python execution protocol.

These helpers characterize the frozen v1 oracle only; application runtime must
not import or serve the legacy execution protocol.
Baseline: commit 1443a32aeb53d73a3d27c69bae9c1a9c41369746.
"""

import hashlib
import json
import math
import uuid
from datetime import datetime, timezone


class InvalidEnvelope(ValueError):
    pass


def reject_duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidEnvelope("duplicate JSON field")
        result[key] = value
    return result


def parse_envelope(raw: bytes):
    if len(raw) > 8192:
        raise InvalidEnvelope("request body exceeds 8192 bytes")
    try:
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicate_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(
                              InvalidEnvelope("invalid JSON number")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEnvelope("request body must be a JSON object") from exc
    required = {"version", "job_id", "owner_id", "generation", "attempt_generation", "deadline_at"}
    if not isinstance(body, dict) or set(body) != required:
        raise InvalidEnvelope("request fields are invalid")
    if type(body["version"]) is not int or body["version"] != 1:
        raise InvalidEnvelope("unsupported envelope version")
    try:
        job_id = uuid.UUID(body["job_id"])
        owner_id = uuid.UUID(body["owner_id"])
    except (ValueError, TypeError, AttributeError) as exc:
        raise InvalidEnvelope("job or owner identifier is invalid") from exc
    if str(job_id) != body["job_id"] or str(owner_id) != body["owner_id"]:
        raise InvalidEnvelope("identifiers must use canonical UUID form")
    for field in ("generation", "attempt_generation"):
        if type(body[field]) is not int or body[field] <= 0 or body[field] > 2_147_483_647:
            raise InvalidEnvelope(f"{field} is invalid")
    if not isinstance(body["deadline_at"], str):
        raise InvalidEnvelope("deadline_at is invalid")
    try:
        deadline = datetime.fromisoformat(body["deadline_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEnvelope("deadline_at is invalid") from exc
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise InvalidEnvelope("deadline_at must include a timezone")
    deadline = deadline.astimezone(timezone.utc)
    canonical = {
        "version": 1, "job_id": str(job_id), "owner_id": str(owner_id),
        "generation": body["generation"], "attempt_generation": body["attempt_generation"],
        "deadline_at": body["deadline_at"],
    }
    digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {**canonical, "deadline": deadline}, digest


def receipt_response(receipt):
    deadline = receipt.deadline_at
    if deadline is not None and deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=timezone.utc)
    return {
        "job_id": str(receipt.job_id),
        "attempt_generation": receipt.attempt_generation,
        "owner_id": str(receipt.owner_id) if receipt.owner_id else None,
        "generation": receipt.worker_generation,
        "deadline_at": deadline.isoformat().replace("+00:00", "Z") if deadline else None,
        "executor_instance_id": str(receipt.executor_instance_id),
        "state": receipt.state,
        "accepted_at": receipt.accepted_at.isoformat() if receipt.accepted_at else None,
        "started_at": receipt.started_at.isoformat() if receipt.started_at else None,
        "finished_at": receipt.finished_at.isoformat() if receipt.finished_at else None,
        "result": receipt.result,
        "error_code": receipt.error_code,
    }


def bounded_result(kind, value):
    """Frozen Python v1 projection used as the cross-language oracle."""
    value = value if isinstance(value, dict) else {}
    result = {"status": "success"}
    status = value.get("status")
    article_statuses = {
        "success", "warning", "deferred", "dry_run", "completed", "rejected",
        "exhausted", "retry_wait", "settled", "incomplete", "busy",
    }
    explicit_status = status in article_statuses
    if explicit_status:
        result["status"] = status
    for name in ("success", "settled", "retryable"):
        if type(value.get(name)) is bool:
            result[name] = value[name]
    if ((value.get("success") is False or value.get("fatal_error") or value.get("errors") or value.get("error"))
            and (not explicit_status or status == "success")):
        result["status"] = "warning"
    if isinstance(value.get("errors"), list):
        result["errors_count"] = min(len(value["errors"]), 1_000_000_000)
    mappings = {
        "articles_fetched": ("articles_fetched", "total_fetched"),
        "articles_processed": ("articles_processed", "processed"),
        "articles_summarized": ("articles_summarized",),
        "articles_categorized": ("articles_categorized",),
        "articles_requeued": ("articles_requeued", "requeued_count"),
        "articles_rejected": ("articles_rejected",),
        "articles_retrying": ("articles_retrying",),
        "articles_exhausted": ("articles_exhausted",),
        "total_candidates": ("total_candidates",),
        "found_candidates": ("found_candidates",),
        "processed": ("processed",),
        "improved": ("improved",),
        "summaries_generated": ("summaries_generated",),
        "parts_sent": ("parts_sent",),
        "backup_bytes": ("backup_bytes",),
        "errors_count": ("errors_count", "error_count"),
        "duration_seconds": ("duration_seconds", "total_duration"),
        "requeued_count": ("articles_requeued", "requeued_count"),
    }
    for output, candidates in mappings.items():
        for candidate in candidates:
            metric = value.get(candidate)
            if (isinstance(metric, (int, float)) and not isinstance(metric, bool)
                    and (not isinstance(metric, float) or math.isfinite(metric))):
                bounded_metric = max(0, min(1_000_000_000, metric))
                if output == "errors_count":
                    result[output] = max(result.get(output, 0), bounded_metric)
                else:
                    result[output] = bounded_metric
                break
    if kind == "reprocess_failed":
        result["dry_run"] = bool(value.get("status") == "dry_run")
    if kind == "source_test":
        result["connection_ok"] = bool(value.get("success"))
        if not result["connection_ok"]:
            result["status"] = "warning"
    outcomes = value.get("article_outcomes")
    if isinstance(outcomes, list):
        safe_reasons = {
            "advertisement", "resource_limit", "category_mapping_failed", "missing_summary",
            "smart_filter_rejected", "extraction_challenge", "provider_timeout",
        }
        valid_outcomes = [
            {"article_id": item["article_id"], "state": item["state"],
             "reason": item.get("reason") if item.get("reason") in safe_reasons else "other"}
            for item in outcomes
            if isinstance(item, dict) and isinstance(item.get("article_id"), int)
            and item.get("state") in {"completed", "rejected", "exhausted", "pending", "retry_wait", "processing"}
        ]
        sample_limit = 20 if len(valid_outcomes) > 1 else 1
        result["article_outcomes"] = valid_outcomes[:sample_limit]
        prior_total = value.get("article_outcomes_total")
        if isinstance(prior_total, int) and not isinstance(prior_total, bool):
            total = max(len(valid_outcomes), min(100, max(0, prior_total)))
        else:
            total = len(valid_outcomes)
        result["article_outcomes_total"] = total
        result["article_outcomes_omitted"] = max(0, total - len(result["article_outcomes"]))
        if any(item["state"] != "completed" for item in valid_outcomes) and result["status"] == "success":
            result["status"] = "warning"
    if value.get("status") == "dry_run":
        result["status"] = "dry_run"
    return result
