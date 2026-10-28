"""Private, bounded real-origin Go native-browser capture runner.

Run only inside the isolated shadow stand after its network fixture gate. The
corpus and raw captures contain article data and must remain on the owner's VPS.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

if __package__:
    from scripts import benchmark_native_cloak as bench
else:
    # The mounted runner can be invoked as /bench/scripts/run_native_shadow.py
    # while PYTHONPATH points at /app, whose scripts package is unrelated.
    import benchmark_native_cloak as bench

MAX_CASES = 50
MAX_CORPUS_BYTES = 64 * 1024 * 1024
MAX_OUTPUT_BYTES = 256 * 1024 * 1024
MAX_JOB_SECONDS = 30
NAVIGATION_INTERVAL_SECONDS = 10
SAFE_CASE_ID = re.compile(r"^case-[0-9]{4}$")
SAFE_ERROR = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
SHADOW_CGROUP_NAMES = {"app", "worker", "observer", "cloak", "gateway", "relay",
                       "browser_anchor", "control_anchor", "gateway_anchor"}


class ShadowError(RuntimeError):
    """Safe code only; never include URL, token, exception message, or body."""


def _safe_code(value: Any) -> str:
    return value if isinstance(value, str) and SAFE_ERROR.fullmatch(value) else "other"


def _public_https_url(raw: str, *, main: bool = False) -> bool:
    try:
        u = urlsplit(raw)
        host = u.hostname or ""
        if (u.scheme != "https" or not host or not u.netloc or u.username or u.password
                or u.port not in (None, 443) or u.fragment or (main and u.query)
                or "." not in host or host.endswith(".") or ".." in host
                or any(ord(ch) < 33 for ch in raw) or len(raw) > 8192):
            return False
        lowered = host.lower()
        if (lowered == "localhost" or lowered.endswith((".localhost", ".local", ".internal", ".test", ".invalid"))
                or "%" in host):
            return False
        try:
            ipaddress.ip_address(host)
            return False
        except ValueError:
            return all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                       for label in lowered.split("."))
    except (TypeError, ValueError):
        return False


def load_corpus(path: Path) -> tuple[list[dict[str, Any]], str]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_CORPUS_BYTES:
            raise ShadowError("invalid_corpus")
        if os.name == "posix" and (path.stat().st_uid != os.geteuid()
                                   or path.stat().st_mode & 0o077):
            raise ShadowError("unsafe_corpus_permissions")
        raw = path.read_bytes()
        data = json.loads(raw)
    except (OSError, ValueError, UnicodeError):
        raise ShadowError("invalid_corpus") from None
    if (not isinstance(data, dict) or data.get("version") != 1
            or not isinstance(data.get("cases"), list) or not 1 <= len(data["cases"]) <= MAX_CASES):
        raise ShadowError("invalid_corpus")
    counts: Counter[str] = Counter()
    ids: set[str] = set()
    urls: set[str] = set()
    for case in data["cases"]:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str) or not SAFE_CASE_ID.fullmatch(case["id"]):
            raise ShadowError("invalid_corpus")
        if case["id"] in ids or not _public_https_url(case.get("url"), main=True) or case["url"] in urls:
            raise ShadowError("invalid_corpus")
        ids.add(case["id"])
        urls.add(case["url"])
        host = urlsplit(case["url"]).hostname.lower()
        counts[host] += 1
        if (counts[host] > 10 or case.get("source_type") not in ("rss", "custom")
                or not isinstance(case.get("article_id"), (int, str))
                or isinstance(case.get("article_id"), bool)
                or not isinstance(case.get("title"), (str, type(None)))
                or not isinstance(case.get("archived_content"), (str, type(None)))
                or len(case.get("archived_content") or "") > 100000
                or not isinstance(case.get("media_files"), (list, type(None)))
                or not isinstance(case.get("image_url"), (str, type(None)))
                or not isinstance(case.get("published_at"), (str, type(None)))):
            raise ShadowError("invalid_corpus")
    return data["cases"], hashlib.sha256(raw).hexdigest()


def _normalize(value: str | None) -> str:
    return " ".join((value or "").split()).casefold()


def _hash(value: str | None) -> str | None:
    normalized = _normalize(value)
    return hashlib.sha256(normalized.encode()).hexdigest() if normalized else None


def _prepare_output(directory: Path) -> None:
    if any(path.is_symlink() for path in (directory, *directory.parents)):
        raise ShadowError("unsafe_output_directory")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.name == "posix" and (directory.stat().st_uid != os.geteuid()
                               or directory.stat().st_mode & 0o077):
        raise ShadowError("unsafe_output_directory")


def _write_private(path: Path, data: Any, *, output_dir: Path) -> None:
    if path.is_symlink() or path.parent != output_dir:
        raise ShadowError("unsafe_output_path")
    encoded = (json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    existing = sum(p.stat().st_size for p in output_dir.iterdir() if p.is_file() and not p.is_symlink())
    old = path.stat().st_size if path.exists() else 0
    if existing - old + len(encoded) > MAX_OUTPUT_BYTES:
        raise ShadowError("capture_limit")
    temporary = output_dir / (path.name + ".tmp")
    if temporary.exists() or temporary.is_symlink():
        raise ShadowError("unsafe_output_path")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_ledger(path: Path, digest: str) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "corpus_sha256": digest, "attempted": {"C": []},
                "blocked_hosts": {"C": []},
                "last_navigation_epoch": {"C": None}}
    try:
        if path.is_symlink() or path.stat().st_size > 65536:
            raise ValueError
        data = json.loads(path.read_text())
        if (data.get("version") != 1 or data.get("corpus_sha256") != digest
                or not isinstance(data["attempted"]["C"], list)
                or not all(SAFE_CASE_ID.fullmatch(v) for v in data["attempted"]["C"])
                or not isinstance(data["blocked_hosts"]["C"], list)
                or not all(isinstance(v, str) and len(v) <= 253
                           for v in data["blocked_hosts"]["C"])):
            raise ValueError
        return data
    except (OSError, ValueError, KeyError, TypeError):
        raise ShadowError("invalid_ledger") from None


def _load_prior_report(path: Path, variant: str, digest: str, valid_ids: set[str]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        if path.is_symlink() or path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError
        report = json.loads(path.read_text())
        rows = report["rows"]
        if (report.get("schema_version") != 1 or report.get("variant") != variant
                or report.get("corpus_sha256") != digest or report.get("status") != "complete"
                or not isinstance(rows, list) or len(rows) > MAX_CASES
                or any(not isinstance(row, dict) or row.get("id") not in valid_ids for row in rows)
                or len({row["id"] for row in rows}) != len(rows)):
            raise ValueError
        return report
    except (OSError, ValueError, KeyError, TypeError):
        raise ShadowError("invalid_prior_report") from None


def _mark_attempt(ledger: dict[str, Any], variant: str, case_id: str, directory: Path) -> None:
    if case_id in ledger["attempted"][variant]:
        raise ShadowError("duplicate_navigation_denied")
    ledger["attempted"][variant].append(case_id)
    ledger["last_navigation_epoch"][variant] = time.time()
    _write_private(directory / "ledger.json", ledger, output_dir=directory)


async def _readiness(endpoint: str) -> dict[str, Any]:
    import aiohttp
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False) as session:
            async with session.get(endpoint.rstrip("/") + "/health/ready", allow_redirects=False) as response:
                raw = await response.content.read(16385)
                if response.status != 200 or len(raw) > 16384:
                    raise ShadowError("native_readiness_lost")
        payload = json.loads(raw)
        caps = payload.get("capabilities")
        if (payload.get("ready") is not True or not isinstance(caps, dict)
                or caps.get("browser") is not True or caps.get("native_browser") is not True
                or caps.get("native_ready_selector") is not False
                or not isinstance(payload.get("native_profile"), dict)
                or payload["native_profile"].get("ready_selector_sha256") != ""):
            raise ShadowError("native_readiness_lost")
        return {"ready": True, "browser": True, "native_browser": True,
                "native_ready_selector": False}
    except ShadowError:
        raise
    except Exception:
        raise ShadowError("native_readiness_lost") from None


async def _run_c(url: str, endpoint: str, token: str) -> dict[str, Any]:
    try:
        from scripts.native_browser_fixture_client import ExtractionServiceClient, ExtractionServiceError
    except ImportError:
        from native_browser_fixture_client import ExtractionServiceClient, ExtractionServiceError
    try:
        payload = await ExtractionServiceClient(endpoint, token).extract(
            url, timeout_seconds=MAX_JOB_SECONDS, strategy="browser_rendering",
            source_type="article", use_browser=True)
    except ExtractionServiceError as exc:
        if exc.code == "deadline_exceeded":
            # The server may still be disposing the owned context. End this
            # variant instead of dispatching another job into uncertain cleanup.
            raise ShadowError("native_job_deadline_cleanup_unconfirmed") from None
        if exc.code in {"unauthorized", "invalid_payload", "unavailable", "transport_error"}:
            raise ShadowError("native_service_unavailable") from None
        return {"outcome": "failed", "state": "failed", "error": exc.code,
                "retry_after_seconds": exc.retry_after_seconds,
                "upstream_status": 429 if exc.code == "busy" else None,
                "content": "", "method": "none"}
    result = payload["result"]
    error = payload.get("error") or {}
    if error.get("code") in {"native_browser_unavailable", "native_browser_busy"}:
        raise ShadowError("native_browser_unavailable")
    if error.get("code") == "native_browser_target_crashed":
        if (payload.get("outcome") != "failed" or result.get("state") != "failed"
                or any(result.get(key) for key in ("content", "title", "publication_date", "author", "description", "image_url", "media"))
                or error.get("retryable") is not False):
            raise ShadowError("native_browser_unavailable")
        await _readiness(endpoint)
    return {"outcome": payload["outcome"], "state": result.get("state"),
            "error": error.get("code"), "retry_after_seconds": error.get("retry_after_seconds"),
            "upstream_status": error.get("upstream_status"),
            "content": result.get("content") or "", "method": result.get("method") or result.get("method_used"),
            "final_url": payload.get("final_url") or result.get("extracted_from") or url,
            "title": result.get("title"), "publication_date": result.get("publication_date"),
            "author": result.get("author"),
            "description": result.get("description"), "image_url": result.get("image_url"),
            "media": result.get("media") or []}


def _case_row(case: dict[str, Any], result: dict[str, Any], variant: str, latency_ms: int) -> dict[str, Any]:
    content = result.get("content") or ""
    archive = case.get("archived_content") or ""
    final = result.get("final_url") or case["url"]
    return {"id": case["id"], "variant": variant,
            "outcome": result.get("outcome") if result.get("outcome") in ("success", "rejected", "failed", "needs_selector_study") else "failed",
            "state": bench._safe_state(result.get("state")),
            "error": _safe_code(result.get("error")) if result.get("error") else None,
            "upstream_status": result.get("upstream_status") if type(result.get("upstream_status")) is int else None,
            "retry_after_seconds": result.get("retry_after_seconds") if type(result.get("retry_after_seconds")) is int else None,
            "method": bench._safe_method(result.get("method")),
            "content_chars": len(content), "normalized_content_sha256": _hash(content),
            "historical_archive_match": bool(_normalize(content) and _normalize(content) == _normalize(archive)),
            "historical_archive_chars": len(archive),
            "title_present": bool(result.get("title")),
            "title_match_archive": bool(result.get("title") and _normalize(result.get("title")) == _normalize(case.get("title"))),
            "publication_date_present": bool(result.get("publication_date")),
            "publication_date_match_archive": bool(result.get("publication_date") and _normalize(result.get("publication_date")) == _normalize(case.get("published_at"))),
            "author_present": bool(result.get("author")),
            "description_present": bool(result.get("description")),
            "image_present": bool(result.get("image_url")),
            "image_match_archive": bool(result.get("image_url") and result.get("image_url") == case.get("image_url")),
            "media_count": len(result.get("media") or []),
            "historical_media_count": len(case.get("media_files") or []),
            "final_url_changed": final != case["url"], "latency_ms": latency_ms}


def _check_telemetry(sampler: bench.Sampler, cgroups: dict[str, Path]) -> dict[str, Any]:
    if sampler.error:
        raise ShadowError("cgroup_metric_unavailable")
    metrics = bench.sample_cgroups(cgroups)
    if sampler.baseline is None:
        raise ShadowError("cgroup_metric_unavailable")
    for name, values in metrics.items():
        baseline = sampler.baseline[name]
        if (values["oom_events"] > baseline["oom_events"]
                or values["oom_kill_events"] > baseline["oom_kill_events"]):
            raise ShadowError("shadow_oom")
    return metrics


async def run(args: argparse.Namespace) -> dict[str, Any]:
    cases, digest = load_corpus(Path(args.corpus))
    selected = set(args.case or [case["id"] for case in cases])
    if not selected.issubset({case["id"] for case in cases}):
        raise ShadowError("unknown_case_id")
    directory = Path(args.output_dir).absolute()
    _prepare_output(directory)
    ledger = _load_ledger(directory / "ledger.json", digest)
    prior = _load_prior_report(directory / "report-C.json", "C", digest,
                               {case["id"] for case in cases})
    pending = [c for c in cases if c["id"] in selected and c["id"] not in ledger["attempted"]["C"]]
    cgroups = bench.parse_cgroups(args.cgroup, allowed_names=SHADOW_CGROUP_NAMES)
    required = {"worker", "cloak", "gateway"}
    if not required.issubset(cgroups):
        raise ShadowError("required_cgroups_missing")
    token = os.environ.get(args.token_env, "")
    try:
        bench._validate_internal_endpoint(args.endpoint_c, "service_credentials_required")
    except bench.BenchmarkError:
        raise ShadowError("service_credentials_required") from None
    if len(token) < 16:
        raise ShadowError("service_credentials_required")
    caps = await _readiness(args.endpoint_c)
    browser_discovery_sha256 = None
    sampler = bench.Sampler(cgroups)
    await sampler.start()
    report = {"schema_version": 1, "variant": "C", "corpus_sha256": digest,
              "status": "incomplete", "readiness_profile": {
                  "document_complete": True, "minimum_navigation_ms": 500,
                  "dom_stable_ms": 250, "network_quiet_ms": 250,
                  "client_deadline_seconds": MAX_JOB_SECONDS,
                  "server_cleanup_max_seconds": 5,
                  "client_deadline_stops_variant": True,
                  "native_ready_selector_configured": False},
              "capabilities": caps,
              "browser_discovery_sha256": browser_discovery_sha256,
              "measurement_scope": "C Go worker, load-generator observer, dedicated Cloak, gateway, and named stand support cgroups",
              "measured_cgroup_names": sorted(cgroups),
              "selected_case_ids": [c["id"] for c in cases if c["id"] in selected or
                                    c["id"] in (prior or {}).get("selected_case_ids", [])],
              "skipped_prior_attempt_ids": [c["id"] for c in cases if c["id"] in selected and c["id"] not in {p["id"] for p in pending}],
              "attempted_case_ids": list(ledger["attempted"]["C"]),
              "rows": list((prior or {}).get("rows", [])),
              "skipped_rate_limited_ids": list((prior or {}).get("skipped_rate_limited_ids", [])),
              "fatal_error": None, "baseline_cgroups": sampler.baseline,
              "resource_metrics": None, "resource_runs": list((prior or {}).get("resource_runs", []))}
    blocked_hosts: set[str] = set(ledger["blocked_hosts"]["C"])
    try:
        for case in pending:
            host = urlsplit(case["url"]).hostname.lower()
            if host in blocked_hosts:
                if case["id"] not in report["skipped_rate_limited_ids"]:
                    report["skipped_rate_limited_ids"].append(case["id"])
                continue
            previous = ledger["last_navigation_epoch"].get("C")
            if previous is not None:
                await asyncio.sleep(max(0, NAVIGATION_INTERVAL_SECONDS - (time.time() - previous)))
            _check_telemetry(sampler, cgroups)
            await _readiness(args.endpoint_c)
            _mark_attempt(ledger, "C", case["id"], directory)
            report["attempted_case_ids"] = list(ledger["attempted"]["C"])
            started = time.monotonic()
            fatal_after_capture = None
            try:
                result = await asyncio.wait_for(_run_c(case["url"], args.endpoint_c, token), timeout=MAX_JOB_SECONDS)
            except ShadowError as exc:
                fatal_after_capture = str(exc)
                result = {"outcome": "failed", "state": "failed", "content": "", "method": "none",
                          "error": fatal_after_capture}
            except TimeoutError:
                fatal_after_capture = "native_job_deadline_cleanup_unconfirmed"
                result = {"outcome": "failed", "state": "failed", "content": "", "method": "none",
                          "error": fatal_after_capture or "native_browser_timeout"}
            except (ConnectionError, OSError):
                fatal_after_capture = "browser_transport_unavailable"
                result = {"outcome": "failed", "state": "failed", "content": "", "method": "none",
                          "error": fatal_after_capture}
            except Exception as exc:
                result = {"outcome": "failed", "state": "failed", "content": "", "method": "none",
                          "error": (_safe_code(str(exc)) if isinstance(exc, bench.BenchmarkError)
                                    else _safe_code(getattr(exc, "code", None)) if getattr(exc, "code", None)
                                    else "site_error")}
            latency = round((time.monotonic() - started) * 1000)
            row = _case_row(case, result, "C", latency)
            raw = {"id": case["id"], "variant": "C", "article_id": case["article_id"],
                   "source_type": case["source_type"], "origin_url": case["url"],
                   "archived_content": case.get("archived_content"),
                   "archived_title": case.get("title"), "archived_published_at": case.get("published_at"),
                   "archived_media_files": case.get("media_files"), "archived_image_url": case.get("image_url"),
                   "result": result}
            _write_private(directory / f"raw-C-{case['id']}.json", raw, output_dir=directory)
            report["rows"].append(row)
            _write_private(directory / "report-C.json", report, output_dir=directory)
            if fatal_after_capture:
                raise ShadowError(fatal_after_capture)
            if row["upstream_status"] == 429 or row["error"] in ("upstream_rate_limited", "busy"):
                blocked_hosts.add(host)
                ledger["blocked_hosts"]["C"] = sorted(blocked_hosts)
                _write_private(directory / "ledger.json", ledger, output_dir=directory)
            _check_telemetry(sampler, cgroups)
            await _readiness(args.endpoint_c)
            _write_private(directory / "report-C.json", report, output_dir=directory)
        report["status"] = "complete"
    except ShadowError as exc:
        report["fatal_error"] = str(exc)
    finally:
        try:
            await sampler.stop()
            _check_telemetry(sampler, cgroups)
            report["resource_metrics"] = bench.summarize_samples(sampler.samples)
            report["resource_runs"].append({"baseline_cgroups": sampler.baseline,
                                            "resource_metrics": report["resource_metrics"]})
        except (ShadowError, bench.BenchmarkError):
            report["status"] = "incomplete"
            report["fatal_error"] = report["fatal_error"] or "cgroup_metric_unavailable"
        _write_private(directory / "report-C.json", report, output_dir=directory)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("C",), required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--endpoint-c")
    parser.add_argument("--token-env", default="EXTRACTOR_TEST_TOKEN")
    parser.add_argument("--cgroup", action="append", default=[], metavar="NAME=/CGROUP/PATH")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if os.name == "posix" and os.geteuid() != 0:
        print("shadow failed: root_required", file=sys.stderr)
        return 2
    try:
        report = asyncio.run(run(args))
    except (ShadowError, bench.BenchmarkError) as exc:
        print(f"shadow failed: {_safe_code(str(exc))}", file=sys.stderr)
        return 2
    return 0 if report["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
