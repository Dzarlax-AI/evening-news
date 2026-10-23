#!/usr/bin/env python3
"""Bounded, opt-in Go native-browser benchmark controller.

The runner accepts only fixture route IDs from the checked-in corpus. It never
fetches an arbitrary user supplied URL, and reports no URLs, response text, or
credentials. A real run requires explicitly supplied cgroup-v2 files; absent
resource data is an error, never a synthetic zero.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import statistics
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "tests" / "fixtures" / "native-cloak" / "corpus.json"
MAX_CORPUS_BYTES = 1024 * 1024
MAX_CASES = 16
MAX_CYCLES = 6
MAX_IDLE = 60
SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
CGROUP_FILES = ("memory.current", "memory.stat", "cpu.stat", "pids.current", "memory.events")
READY_SELECTOR = '[data-ready="true"]'
MIN_NAVIGATION_MS = 500
DOM_STABLE_MS = 250
NETWORK_QUIET_MS = 250


class BenchmarkError(RuntimeError):
    pass


def _safe_readiness_snapshot(value: Any) -> dict[str, bool] | None:
    if not isinstance(value, dict):
        return None
    allowed = {"ready", "browser", "native_browser", "native_ready_selector", "selector_hash_matches"}
    snapshot = {key: value[key] for key in allowed if type(value.get(key)) is bool}
    return snapshot or None


def _safe_readiness_diagnostic(data: Any) -> dict[str, bool] | None:
    """Extract only bounded health flags; never retain the response or selector hash."""
    if not isinstance(data, dict):
        return None
    # A canonical snapshot may be passed through the exception boundary by the soak runner.
    if set(data).issubset({"ready", "browser", "native_browser", "native_ready_selector",
                          "selector_hash_matches"}):
        return _safe_readiness_snapshot(data)
    snapshot: dict[str, bool] = {}
    if type(data.get("ready")) is bool:
        snapshot["ready"] = data["ready"]
    capabilities = data.get("capabilities")
    if isinstance(capabilities, dict):
        for key in ("browser", "native_browser", "native_ready_selector"):
            if type(capabilities.get(key)) is bool:
                snapshot[key] = capabilities[key]
    native_profile = data.get("native_profile")
    selector_hash = native_profile.get("ready_selector_sha256") if isinstance(native_profile, dict) else None
    if isinstance(selector_hash, str) and re.fullmatch(r"[a-f0-9]{64}", selector_hash):
        expected = hashlib.sha256(READY_SELECTOR.encode("utf-8")).hexdigest()
        snapshot["selector_hash_matches"] = selector_hash == expected
    return _safe_readiness_snapshot(snapshot)


class NativeCapabilityUnavailable(BenchmarkError):
    """Stable public failure code with a finite, payload-free diagnostic reason."""

    def __init__(self, diagnostic_code: str, readiness_diagnostic: Any = None):
        allowed = {
            "response_invalid", "service_not_ready", "capabilities_missing",
            "native_profile_missing", "capability_browser_missing", "capability_browser_false",
            "capability_native_browser_missing", "capability_native_browser_false",
            "capability_native_ready_selector_missing", "capability_native_ready_selector_false",
            "selector_hash_mismatch", "http_status", "response_too_large", "invalid_json",
            "timeout", "transport_error", "probe_error",
        }
        super().__init__("native_capability_unavailable")
        safe_http_status = (isinstance(diagnostic_code, str)
                            and re.fullmatch(r"http_status_(?:[1-5][0-9]{2}|other)", diagnostic_code))
        self.diagnostic_code = (diagnostic_code if diagnostic_code in allowed or safe_http_status
                                else "probe_error")
        self.readiness_diagnostic = _safe_readiness_diagnostic(readiness_diagnostic)


def load_corpus(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise BenchmarkError("corpus_unavailable") from exc
    if len(raw) > MAX_CORPUS_BYTES:
        raise BenchmarkError("corpus_too_large")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeError):
        raise BenchmarkError("invalid_corpus") from None
    if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("cases"), list):
        raise BenchmarkError("invalid_corpus")
    cases = data["cases"]
    if not 1 <= len(cases) <= MAX_CASES:
        raise BenchmarkError("invalid_case_count")
    seen: set[str] = set()
    for case in cases:
        if not isinstance(case, dict) or not {"id", "path", "expected_contains", "expected_state"}.issubset(case) or set(case) - {
            "id", "path", "expected_contains", "expected_state", "expected_error"
        }:
            raise BenchmarkError("invalid_case_fields")
        ident, route = case["id"], case["path"]
        if not isinstance(ident, str) or not SAFE_ID.fullmatch(ident) or ident in seen:
            raise BenchmarkError("invalid_case_id")
        if not isinstance(route, str) or not re.fullmatch(r"/[a-z0-9_-]{1,48}", route):
            raise BenchmarkError("invalid_fixture_route")
        if case["expected_state"] not in {"success", "rejected"}:
            raise BenchmarkError("invalid_expected_state")
        if case.get("expected_error") is not None and (
            case["expected_error"] != "native_browser_challenge" or case["expected_state"] != "rejected"
        ):
            raise BenchmarkError("invalid_expected_error")
        anchors = case["expected_contains"]
        if not isinstance(anchors, list) or len(anchors) > 8 or any(
            not isinstance(x, str) or not 1 <= len(x) <= 100 for x in anchors
        ):
            raise BenchmarkError("invalid_expected_anchors")
        seen.add(ident)
    return data, digest


def validate_origin(origin: str) -> str:
    u = urlsplit(origin)
    if (u.scheme != "https" or u.hostname != "native-fixture.test"
            or u.username or u.password or u.path not in {"", "/"} or u.query or u.fragment
            or (u.port not in {None, 443})):
        raise BenchmarkError("invalid_fixture_origin")
    return f"https://{u.hostname}"


def _validate_internal_endpoint(raw: str | None, failure_code: str) -> str:
    if not raw:
        raise BenchmarkError(failure_code)
    u = urlsplit(raw)
    if (u.scheme not in {"http", "https"} or not u.hostname or u.username or u.password
            or u.query or u.fragment or u.path not in {"", "/"}):
        raise BenchmarkError(failure_code)
    # Service credentials and benchmark traffic stay on local/private test infrastructure.
    host = u.hostname.lower()
    if host not in {"extractor", "gateway", "localhost"}:
        try:
            import ipaddress
            address = ipaddress.ip_address(host)
        except ValueError:
            raise BenchmarkError(failure_code) from None
        if not (address.is_private or address.is_loopback or address.is_link_local):
            raise BenchmarkError(failure_code)
    return raw.rstrip("/")


def parse_cgroups(values: list[str], *, allowed_names: set[str] | None = None) -> dict[str, Path]:
    allowed = {"app", "worker", "cloak", "gateway"} if allowed_names is None else allowed_names
    out: dict[str, Path] = {}
    for item in values:
        name, sep, raw_path = item.partition("=")
        if not sep or name not in allowed or name in out:
            raise BenchmarkError("invalid_cgroup_argument")
        path = Path(raw_path)
        if not path.is_absolute():
            raise BenchmarkError("cgroup_path_must_be_absolute")
        try:
            path = path.resolve(strict=True)
        except OSError:
            raise BenchmarkError("cgroup_path_unavailable") from None
        if not path.is_dir():
            raise BenchmarkError("cgroup_path_unavailable")
        out[name] = path
    paths = list(out.values())
    identities: set[tuple[int, int]] = set()
    for index, path in enumerate(paths):
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if identity in identities:
            raise BenchmarkError("overlapping_cgroup_paths")
        identities.add(identity)
        if any(path == other or path in other.parents or other in path.parents for other in paths[index + 1:]):
            raise BenchmarkError("overlapping_cgroup_paths")
    return out


def _read_scalar(path: Path, filename: str) -> int:
    try:
        value = (path / filename).read_text(encoding="ascii").strip()
        if not value.isdigit():
            raise ValueError
        return int(value)
    except (OSError, ValueError):
        raise BenchmarkError("cgroup_metric_unavailable") from None


def _read_pairs(path: Path, filename: str, required: set[str]) -> dict[str, int]:
    try:
        lines = (path / filename).read_text(encoding="ascii").splitlines()
        values = {fields[0]: int(fields[1]) for line in lines
                  if len(fields := line.split()) == 2 and fields[1].isdigit()}
    except (OSError, ValueError):
        raise BenchmarkError("cgroup_metric_unavailable") from None
    if not required.issubset(values):
        raise BenchmarkError("cgroup_metric_unavailable")
    return values


def sample_cgroups(cgroups: dict[str, Path]) -> dict[str, Any]:
    if not cgroups:
        raise BenchmarkError("cgroup_metrics_required")
    snapshots = {}
    for name, path in cgroups.items():
        if not path.is_dir() or any(not (path / f).is_file() for f in CGROUP_FILES):
            raise BenchmarkError("cgroup_metric_unavailable")
        mem = _read_pairs(path, "memory.stat", {"anon", "file", "shmem"})
        cpu = _read_pairs(path, "cpu.stat", {"usage_usec"})
        events = _read_pairs(path, "memory.events", {"oom", "oom_kill"})
        peak_path = path / "memory.peak"
        snapshots[name] = {
            "memory_current_bytes": _read_scalar(path, "memory.current"),
            "memory_lifetime_peak_bytes": _read_scalar(path, "memory.peak") if peak_path.is_file() else None,
            "memory_anon_bytes": mem["anon"], "memory_file_bytes": mem["file"],
            "memory_shmem_bytes": mem["shmem"], "cpu_usage_usec": cpu["usage_usec"],
            "pids": _read_scalar(path, "pids.current"),
            "oom_events": events["oom"], "oom_kill_events": events["oom_kill"],
        }
    return snapshots


class Sampler:
    def __init__(self, cgroups: dict[str, Path], interval: float = 1.0):
        self.cgroups = cgroups
        self.interval = interval
        self.samples: list[dict[str, Any]] = []
        self.baseline: dict[str, Any] | None = None
        self.task: asyncio.Task | None = None
        self.error: Exception | None = None
        self.started_at: float | None = None

    def _elapsed_ms(self) -> int:
        if self.started_at is None:
            return 0
        return round((time.monotonic() - self.started_at) * 1000)

    async def _run(self) -> None:
        while True:
            try:
                self.samples.append({"elapsed_ms": self._elapsed_ms(),
                                     "cgroups": sample_cgroups(self.cgroups)})
            except Exception as exc:
                self.error = exc
                return
            await asyncio.sleep(self.interval)

    async def start(self) -> None:
        self.started_at = time.monotonic()
        self.baseline = sample_cgroups(self.cgroups)  # Fail before any work if telemetry is incomplete.
        self.samples.append({"elapsed_ms": 0, "phase": "baseline", "cgroups": self.baseline})
        self.task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.error:
            raise BenchmarkError("cgroup_metric_unavailable") from None


def _safe_error(exc: Exception) -> str:
    # Error strings from HTTP/CDP layers can embed URLs or headers. Emit only a class label.
    return type(exc).__name__[:48]


def _safe_method(value: Any) -> str:
    allowed = {"browser_rendering", "browser_heuristics", "readability", "none", "unknown"}
    return value if isinstance(value, str) and value in allowed else "other"


def _safe_state(value: Any) -> str:
    allowed = {"extracted", "failed", "unsupported", "resource_limit", "unknown"}
    return value if isinstance(value, str) and value in allowed else "other"


def _safe_error_code(value: Any) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,47}", value) else "other"


def _retry_after_seconds(value: Any) -> int | None:
    raw = str(value).strip() if value is not None else ""
    if raw.isdigit():
        return min(int(raw), 86400)
    try:
        target = parsedate_to_datetime(raw)
        if target.tzinfo is None:
            return None
        return min(max(0, math.ceil((target - datetime.now(timezone.utc)).total_seconds())), 86400)
    except (TypeError, ValueError, OverflowError):
        return None


def _native_ready_gate(*, now: float, started: float, stable_since: float,
                       last_activity: float, active_requests: int, main_status: int) -> bool:
    """Match the elapsed-time and network conditions in Go NativeBrowser.Fetch."""
    return (now - started >= MIN_NAVIGATION_MS / 1000
            and now - stable_since >= DOM_STABLE_MS / 1000
            and now - last_activity >= NETWORK_QUIET_MS / 1000
            and active_requests == 0 and main_status == 200)


async def _run_service_case(url: str, endpoint: str, token: str) -> dict[str, Any]:
    from news_aggregator.extraction.service_client import ExtractionServiceClient
    client = ExtractionServiceClient(endpoint, token)
    data = await client.extract(url, timeout_seconds=30, strategy="browser_rendering",
                                source_type="article", use_browser=True)
    result = data["result"]
    return {"outcome": data["outcome"], "state": result.get("state", "unknown"),
            "content_chars": len(result.get("content") or ""),
            "html_bytes": None, "method": _safe_method(result.get("method") or result.get("method_used") or "unknown"),
            "selector_present": bool(result.get("selector_used")), "content": result.get("content") or "",
            "error": _safe_error_code((data.get("error") or {}).get("code"))}


def _validate_native_capability_payload(data: Any) -> dict[str, Any]:
    caps = data.get("capabilities") if isinstance(data, dict) else None
    native_profile = data.get("native_profile") if isinstance(data, dict) else None
    selector_hash = hashlib.sha256(READY_SELECTOR.encode("utf-8")).hexdigest()
    if not isinstance(data, dict):
        raise NativeCapabilityUnavailable("response_invalid")
    if data.get("ready") is not True:
        raise NativeCapabilityUnavailable("service_not_ready")
    if not isinstance(caps, dict):
        raise NativeCapabilityUnavailable("capabilities_missing")
    for key in ("browser", "native_browser", "native_ready_selector"):
        if key not in caps:
            raise NativeCapabilityUnavailable("capability_" + key + "_missing")
        if caps[key] is not True:
            raise NativeCapabilityUnavailable("capability_" + key + "_false")
    if not isinstance(native_profile, dict):
        raise NativeCapabilityUnavailable("native_profile_missing")
    if native_profile.get("ready_selector_sha256") != selector_hash:
        raise NativeCapabilityUnavailable("selector_hash_mismatch")
    return {"ready": True, "browser": True, "native_browser": True,
            "native_ready_selector": True, "ready_selector_sha256": selector_hash,
            "expected_selector_matches_fixture": True}


async def _probe_native_capabilities(endpoint: str) -> dict[str, bool]:
    """Require live native readiness and selector capability before measuring C/D."""
    import aiohttp

    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
            async with session.get(endpoint.rstrip("/") + "/health/ready", allow_redirects=False) as response:
                raw = await response.content.read(16385)
                if response.status != 200:
                    status = response.status if type(response.status) is int and 100 <= response.status <= 599 else "other"
                    readiness = None
                    if len(raw) <= 16384:
                        try:
                            readiness = json.loads(raw)
                        except (ValueError, UnicodeError):
                            pass
                    raise NativeCapabilityUnavailable("http_status_" + str(status), readiness)
                if len(raw) > 16384:
                    raise NativeCapabilityUnavailable("response_too_large")
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeError):
            raise NativeCapabilityUnavailable("invalid_json") from None
        return _validate_native_capability_payload(data)
    except NativeCapabilityUnavailable:
        raise
    except asyncio.TimeoutError:
        raise NativeCapabilityUnavailable("timeout") from None
    except Exception as exc:
        try:
            import aiohttp
            if isinstance(exc, aiohttp.ClientError):
                diagnostic = "transport_error"
            else:
                diagnostic = "probe_error"
        except Exception:
            diagnostic = "probe_error"
        raise NativeCapabilityUnavailable(diagnostic) from None


def compare_case(case: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    state_match = result.get("outcome") == case["expected_state"]
    content = result.get("content", "")
    anchor_match = None
    if case["expected_state"] == "success":
        anchor_match = (bool(content.strip()) and result.get("state") != "failed"
                        and all(anchor.casefold() in content.casefold() for anchor in case["expected_contains"]))
        state_match = state_match and result.get("state") != "failed" and bool(content.strip())
    error_match = None
    if case.get("expected_error"):
        error_match = result.get("error") == case["expected_error"]
    ok = state_match and (anchor_match is not False) and (error_match is not False)
    return {"passed": bool(ok), "expected_state": case["expected_state"],
            "state_match": state_match, "anchor_match": anchor_match,
            "expected_error_match": error_match}


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        raise BenchmarkError("no_cgroup_samples")
    names = set(samples[0]["cgroups"])
    result = {"sample_count": len(samples), "cgroups": {}}
    for name in names:
        values = [s["cgroups"][name] for s in samples]
        kernel_peak_available = all(v["memory_lifetime_peak_bytes"] is not None for v in values)
        result["cgroups"][name] = {
            "memory_current_sampled_max_bytes": max(v["memory_current_bytes"] for v in values),
            "memory_current_last_bytes": values[-1]["memory_current_bytes"],
            "memory_lifetime_peak_bytes": (values[-1]["memory_lifetime_peak_bytes"]
                                           if kernel_peak_available else None),
            "memory_lifetime_peak_source": ("kernel_memory.peak" if kernel_peak_available
                                            else "unavailable_on_host"),
            "memory_anon_last_bytes": values[-1]["memory_anon_bytes"],
            "memory_file_last_bytes": values[-1]["memory_file_bytes"],
            "memory_shmem_last_bytes": values[-1]["memory_shmem_bytes"],
            "cpu_usage_usec_delta": values[-1]["cpu_usage_usec"] - values[0]["cpu_usage_usec"],
            "pids_last": values[-1]["pids"],
            "oom_events_delta": values[-1]["oom_events"] - values[0]["oom_events"],
            "oom_kill_events_delta": values[-1]["oom_kill_events"] - values[0]["oom_kill_events"],
        }
    result["aggregate_sampled_memory_current_bytes_max"] = max(
        sum(s["cgroups"][n]["memory_current_bytes"] for n in names) for s in samples)
    result["kernel_peak_available"] = all(
        group["memory_lifetime_peak_source"] == "kernel_memory.peak"
        for group in result["cgroups"].values())
    return result


async def run(args: argparse.Namespace) -> dict[str, Any]:
    origin = validate_origin(args.origin)
    corpus, corpus_digest = load_corpus(Path(args.corpus))
    cgroups = parse_cgroups(args.cgroup)
    required = {"C": {"worker", "cloak", "gateway"},
                "D": {"app", "worker", "cloak", "gateway"}}[args.variant]
    if not required.issubset(cgroups):
        raise BenchmarkError("required_cgroups_missing")
    cases = corpus["cases"]
    if args.case:
        selected = set(args.case)
        if not selected.issubset({c["id"] for c in cases}):
            raise BenchmarkError("unknown_case_id")
        cases = [c for c in cases if c["id"] in selected]
    token = os.environ.get(args.token_env) if args.token_env else None
    if not token:
        raise BenchmarkError("service_token_missing")
    args.endpoint = _validate_internal_endpoint(args.endpoint, "endpoint_required")
    if args.native_ready_selector != READY_SELECTOR:
        raise BenchmarkError("native_ready_selector_mismatch")
    native_capabilities = await _probe_native_capabilities(args.endpoint)
    sampler = Sampler(cgroups)
    await sampler.start()
    timings = []
    rows = []
    failures = 0
    warmup = {"case_id": cases[0]["id"]}
    try:
        warmup_start = time.perf_counter()
        warmup_url = origin + cases[0]["path"]
        try:
            warmup_result = await _run_service_case(warmup_url, args.endpoint, token)
            warmup.update({"outcome": warmup_result["outcome"], "state": _safe_state(warmup_result["state"]),
                           "method": _safe_method(warmup_result["method"])})
        except Exception as exc:
            warmup.update({"outcome": "failed", "state": "failed", "method": "none",
                           "error": _safe_error(exc)})
        warmup["duration_ms"] = round((time.perf_counter() - warmup_start) * 1000, 2)
        for cycle in range(1, args.cycles + 1):
            for case in cases:
                start = time.perf_counter()
                url = origin + case["path"]
                try:
                    result = await _run_service_case(url, args.endpoint, token)
                except Exception as exc:
                    result = {"outcome": "failed", "state": "failed", "content_chars": 0,
                              "html_bytes": None, "method": "none", "selector_present": False,
                              "content": "", "error": _safe_error(exc)}
                elapsed = (time.perf_counter() - start) * 1000
                check = compare_case(case, result)
                if not check["passed"]:
                    failures += 1
                timings.append(elapsed)
                rows.append({"case_id": case["id"], "cycle": cycle,
                             "outcome": result["outcome"], "state": _safe_state(result["state"]),
                             "error": _safe_error_code(result.get("error")), "method": _safe_method(result["method"]),
                             "duration_ms": round(elapsed, 2), "content_chars": result["content_chars"],
                             "html_bytes": result["html_bytes"], "selector_present": result["selector_present"],
                             "expectation": check})
            if args.idle_seconds:
                for seconds in args.idle_seconds:
                    await asyncio.sleep(seconds)
                    # An actual sample during each idle gate is mandatory.
                    sampler.samples.append({"elapsed_ms": sampler._elapsed_ms(),
                                            "cgroups": sample_cgroups(cgroups), "idle_seconds": seconds})
    finally:
        await sampler.stop()
    idle_samples = [{"idle_seconds": sample["idle_seconds"],
                     "cgroups": {name: {"memory_current_bytes": metrics["memory_current_bytes"],
                                        "pids": metrics["pids"]}
                                 for name, metrics in sample["cgroups"].items()}}
                    for sample in sampler.samples if "idle_seconds" in sample]
    return {"schema_version": 1, "variant": args.variant,
            "measurement_scope": ("controller_and_native_service_client; full_application_processing_not_measured"
                                  if args.variant == "D" else "variant_controller_and_measured_cgroups"),
            "readiness_profile": {"policy": "native_fixture_selector_and_network_gate",
                                      "selector": READY_SELECTOR,
                                      "minimum_navigation_ms": MIN_NAVIGATION_MS,
                                      "dom_stable_ms": DOM_STABLE_MS,
                                      "network_quiet_ms": NETWORK_QUIET_MS,
                                      "active_requests": 0,
                                      "main_document_status": 200,
                                      "go_profile_basis": "checked_in_source_not_live_attestation"},
            "corpus_sha256": corpus_digest,
            "case_ids": [c["id"] for c in cases], "cycles": args.cycles,
            "repeat_scope": "single_invocation; independent comparisons require fresh invocations",
            "ready_selector": READY_SELECTOR, "native_capabilities": native_capabilities,
            "baseline_cgroups": sampler.baseline,
            "warmup": warmup, "measure_interval_seconds": 1,
            "idle_samples": idle_samples, "outcomes": rows,
            "latency_ms": {"count": len(timings), "p50": round(statistics.median(timings), 2),
                           "p95": round(sorted(timings)[max(0, math.ceil(len(timings) * .95) - 1)], 2),
                           "p95_method": "nearest_rank"},
            "resource_metrics": summarize_samples(sampler.samples), "mismatch_count": failures,
            "status": "passed" if failures == 0 else "mismatch"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("C", "D"), required=True)
    parser.add_argument("--corpus", default=str(DEFAULT_CORPUS))
    parser.add_argument("--origin", default="https://native-fixture.test")
    parser.add_argument("--output", required=True)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--idle-seconds", type=lambda s: [int(x) for x in s.split(",")], default=[5, 30, 60])
    parser.add_argument("--cgroup", action="append", default=[], metavar="NAME=/CGROUP/PATH")
    parser.add_argument("--endpoint")
    parser.add_argument("--native-ready-selector", default=READY_SELECTOR,
                        help="must match the native extractor service's configured fixture readiness selector")
    parser.add_argument("--token-env", default="EXTRACTOR_TEST_TOKEN", help="name of environment variable containing API token")
    parser.add_argument("--serve-fixtures", action="store_true", help="run the fixed HTTPS fixture origin")
    parser.add_argument("--fixture-host", default="127.0.0.1")
    parser.add_argument("--fixture-port", type=int, default=443)
    parser.add_argument("--fixture-cert")
    parser.add_argument("--fixture-key")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.serve_fixtures:
        fixture_path = ROOT / "tests" / "fixtures" / "native-cloak" / "origin.py"
        import importlib.util
        spec = importlib.util.spec_from_file_location("native_cloak_fixture_origin", fixture_path)
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        if not args.fixture_cert or not args.fixture_key:
            raise SystemExit("--serve-fixtures requires --fixture-cert and --fixture-key")
        if args.fixture_host not in {"127.0.0.1", "::1", "0.0.0.0"}:
            raise SystemExit("fixture host must be loopback or isolated-container wildcard")
        fixture.serve(args.fixture_host, args.fixture_port, args.fixture_cert, args.fixture_key)
        return 0
    if not 1 <= args.cycles <= MAX_CYCLES:
        raise SystemExit("cycles outside supported bounds")
    if any(x < 0 or x > MAX_IDLE for x in args.idle_seconds):
        raise SystemExit("idle duration outside supported bounds")
    try:
        report = asyncio.run(run(args))
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except BenchmarkError as exc:
        diagnostic = getattr(exc, "diagnostic_code", None)
        readiness = getattr(exc, "readiness_diagnostic", None)
        suffix = f" diagnostic={diagnostic}" if isinstance(diagnostic, str) else ""
        if isinstance(readiness, dict):
            suffix += " readiness=" + json.dumps(readiness, sort_keys=True, separators=(",", ":"))
        print(f"benchmark failed: {exc}{suffix}", file=sys.stderr)
        return 2
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
