#!/usr/bin/env python3
"""Bounded, C-only native extractor soak driver. Writes redacted atomic reports."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import re
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_PATH = ROOT / "scripts" / "benchmark_native_cloak.py"
DEFAULT_CORPUS = ROOT / "tests" / "fixtures" / "native-cloak" / "corpus.json"
MIN_DURATION = 60
MAX_DURATION = 172800
MAX_CYCLES = 6
MAX_BATCHES = 2048
MAX_STORAGE_BYTES = 256 * 1024 * 1024
SUMMARY_RESERVE_BYTES = 64 * 1024
MAX_BATCH_BUDGET_SECONDS = 1800
MAX_BATCH_REPORT_BYTES = 1024 * 1024
SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
REPORT_NAME = re.compile(r"^batch-(\d{6})\.json$")
NATIVE_CAPABILITY_DIAGNOSTICS = frozenset({
    "response_invalid", "service_not_ready", "capabilities_missing",
    "native_profile_missing", "capability_browser_missing", "capability_browser_false",
    "capability_native_browser_missing", "capability_native_browser_false",
    "capability_native_ready_selector_missing", "capability_native_ready_selector_false",
    "selector_hash_mismatch", "http_status", "response_too_large", "invalid_json",
    "timeout", "transport_error", "probe_error",
})


def _load_benchmark():
    spec = importlib.util.spec_from_file_location("native_cloak_benchmark_for_soak", BENCHMARK_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("benchmark_runner_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


benchmark = _load_benchmark()


class SoakError(RuntimeError):
    pass


class SoakInterrupted(SoakError):
    pass


def _safe_error(exc: BaseException) -> str:
    if isinstance(exc, (SoakError, benchmark.BenchmarkError)):
        value = str(exc)
        if re.fullmatch(r"[a-z][a-z0-9_]{0,47}", value):
            return value
    return type(exc).__name__[:48]


def _safe_capability_diagnostic(exc: BaseException) -> str | None:
    value = getattr(exc, "diagnostic_code", None)
    if isinstance(value, str) and value in NATIVE_CAPABILITY_DIAGNOSTICS:
        return value
    if isinstance(value, str) and re.fullmatch(r"http_status_(?:[1-5][0-9]{2}|other)", value):
        return value
    return None


def _safe_readiness_diagnostic(exc: BaseException) -> dict[str, bool] | None:
    value = getattr(exc, "readiness_diagnostic", None)
    return benchmark._safe_readiness_diagnostic(value)


def _safe_case_ids(values: Any) -> list[str]:
    if not isinstance(values, list) or any(not isinstance(value, str) or not SAFE_ID.fullmatch(value)
                                           for value in values):
        raise SoakError("unsafe_case_ids")
    return values


def _redact_batch_report(report: dict[str, Any]) -> dict[str, Any]:
    """Keep only the benchmark's reviewed safe report fields; never serialize arbitrary payload keys."""
    if not isinstance(report, dict):
        raise SoakError("invalid_batch_report")
    allowed_top = (
        "schema_version", "variant", "measurement_scope", "corpus_sha256", "case_ids", "cycles",
        "repeat_scope", "ready_selector", "native_capabilities", "baseline_cgroups", "warmup",
        "measure_interval_seconds", "idle_samples", "outcomes", "latency_ms", "resource_metrics",
        "mismatch_count", "status", "readiness_diagnostic",
    )
    safe = {key: report[key] for key in allowed_top if key in report}
    safe["case_ids"] = _safe_case_ids(safe.get("case_ids"))
    safe["variant"] = "C"
    safe["ready_selector"] = benchmark.READY_SELECTOR
    safe["repeat_scope"] = "single_invocation; independent comparisons require fresh invocations"
    safe["measurement_scope"] = "C_worker_cloak_gateway_cgroups_only"
    safe["readiness_profile"] = _safe_readiness_profile(report.get("readiness_profile"))
    safe["cycles"] = _safe_int(report.get("cycles"))
    safe["schema_version"] = 1
    safe["measure_interval_seconds"] = _safe_number(report.get("measure_interval_seconds"))
    safe["status"] = report.get("status") if report.get("status") in {"passed", "mismatch"} else "failed"
    safe["corpus_sha256"] = report.get("corpus_sha256") if re.fullmatch(
        r"[a-f0-9]{64}", str(report.get("corpus_sha256", ""))) else None
    safe["warmup"] = _safe_outcome_row(report.get("warmup", {}), warmup=True)
    safe["outcomes"] = [_safe_outcome_row(row) for row in report.get("outcomes", [])
                        if isinstance(row, dict)]
    safe["baseline_cgroups"] = _safe_cgroups(report.get("baseline_cgroups", {}))
    safe["idle_samples"] = [{"idle_seconds": _safe_number(row.get("idle_seconds")),
                             "cgroups": _safe_cgroups(row.get("cgroups", {}))}
                            for row in report.get("idle_samples", []) if isinstance(row, dict)]
    safe["resource_metrics"] = _safe_resource_metrics(report.get("resource_metrics", {}))
    safe["native_capabilities"] = _safe_capabilities(report.get("native_capabilities"))
    safe["readiness_diagnostic"] = benchmark._safe_readiness_diagnostic(
        report.get("readiness_diagnostic"))
    safe["latency_ms"] = _safe_latency(report.get("latency_ms", {}))
    safe["mismatch_count"] = _safe_int(report.get("mismatch_count"))
    return safe


def _safe_int(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= 10**15 else None


def _safe_number(value: Any) -> int | float | None:
    return value if type(value) in {int, float} and 0 <= value <= 10**15 else None


def _safe_outcome_row(row: dict[str, Any], *, warmup: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if "case_id" in row and isinstance(row["case_id"], str) and SAFE_ID.fullmatch(row["case_id"]):
        out["case_id"] = row["case_id"]
    for key in ("cycle", "duration_ms", "content_chars", "html_bytes"):
        if key in row:
            out[key] = _safe_number(row[key])
    for key, allowed in (("outcome", {"success", "rejected", "failed"}),
                         ("state", {"extracted", "failed", "unsupported", "resource_limit", "unknown"}),
                         ("method", {"browser_rendering", "browser_heuristics", "readability",
                                     "enhanced_selectors", "learned_pattern_browser_rendering",
                                     "learned_pattern", "none", "unknown", "other"})):
        if isinstance(row.get(key), str) and row.get(key) in allowed:
            out[key] = row[key]
    error = row.get("error")
    if isinstance(error, str):
        out["error"] = error if re.fullmatch(r"[a-z][a-z0-9_]{0,47}", error) else "other"
    if type(row.get("selector_present")) is bool:
        out["selector_present"] = row["selector_present"]
    if isinstance(row.get("expectation"), dict):
        expectation = row["expectation"]
        out["expectation"] = {key: expectation[key] for key in
                              ("passed", "state_match", "anchor_match", "expected_error_match")
                              if type(expectation.get(key)) is bool}
        if isinstance(expectation.get("expected_state"), str) and expectation.get("expected_state") in {"success", "rejected"}:
            out["expectation"]["expected_state"] = expectation["expected_state"]
    return out


def _safe_readiness_profile(value: Any) -> dict[str, Any] | None:
    expected = {
        "policy": "native_fixture_selector_and_network_gate",
        "selector": benchmark.READY_SELECTOR,
        "minimum_navigation_ms": benchmark.MIN_NAVIGATION_MS,
        "dom_stable_ms": benchmark.DOM_STABLE_MS,
        "network_quiet_ms": benchmark.NETWORK_QUIET_MS,
        "active_requests": 0,
        "main_document_status": 200,
        "go_profile_basis": "checked_in_source_not_live_attestation",
    }
    if not isinstance(value, dict) or any(value.get(key) != item for key, item in expected.items()):
        return None
    return expected


_CGROUP_METRICS = {"memory_current_bytes", "memory_lifetime_peak_bytes", "memory_anon_bytes",
                   "memory_file_bytes", "memory_shmem_bytes", "cpu_usage_usec", "pids",
                   "oom_events", "oom_kill_events", "memory_current_sampled_max_bytes",
                   "memory_current_last_bytes", "cpu_usage_usec_delta", "pids_last",
                   "memory_anon_last_bytes", "memory_file_last_bytes", "memory_shmem_last_bytes",
                   "oom_events_delta", "oom_kill_events_delta"}


def _safe_cgroups(groups: Any) -> dict[str, Any]:
    if not isinstance(groups, dict):
        return {}
    safe = {}
    for name, metrics in groups.items():
        if name in {"app", "worker", "cloak", "gateway"} and isinstance(metrics, dict):
            safe[name] = {key: _safe_int(metrics[key]) for key in _CGROUP_METRICS if key in metrics}
            source = metrics.get("memory_lifetime_peak_source")
            if source in {"kernel_memory.peak", "unavailable_on_host"}:
                safe[name]["memory_lifetime_peak_source"] = source
    return safe


def _safe_resource_metrics(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {"sample_count": _safe_int(value.get("sample_count")),
            "cgroups": _safe_cgroups(value.get("cgroups")),
            "aggregate_sampled_memory_current_bytes_max": _safe_int(
                value.get("aggregate_sampled_memory_current_bytes_max")),
            "kernel_peak_available": value.get("kernel_peak_available") is True}


def _safe_capabilities(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    safe = {key: value[key] for key in ("ready", "browser", "native_browser", "native_ready_selector",
                                        "expected_selector_matches_fixture") if value.get(key) is True}
    selector_hash = value.get("ready_selector_sha256")
    if isinstance(selector_hash, str) and re.fullmatch(r"[a-f0-9]{64}", selector_hash):
        safe["ready_selector_sha256"] = selector_hash
    return safe


def _safe_latency(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {"count": _safe_int(value.get("count")), "p50": _safe_number(value.get("p50")),
            "p95": _safe_number(value.get("p95")),
            "p95_method": "nearest_rank" if value.get("p95_method") == "nearest_rank" else None}


def _output_files(out_dir: Path) -> tuple[list[Path], list[Path]]:
    reports: list[Path] = []
    summaries: list[Path] = []
    for entry in out_dir.iterdir():
        if entry.is_symlink() or not entry.is_file():
            raise SoakError("output_directory_not_dedicated")
        if REPORT_NAME.fullmatch(entry.name):
            reports.append(entry)
        elif entry.name == "summary.json":
            summaries.append(entry)
        else:
            raise SoakError("output_directory_not_dedicated")
    return reports, summaries


def _storage_bytes(out_dir: Path) -> int:
    reports, summaries = _output_files(out_dir)
    return sum(path.stat().st_size for path in (*reports, *summaries))


def _atomic_json(path: Path, data: dict[str, Any], *, max_bytes: int | None = None) -> int:
    encoded = (json.dumps(data, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode("utf-8")
    if max_bytes is not None and len(encoded) > max_bytes:
        raise SoakError("report_too_large")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".soak-tmp-", suffix=".json", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return len(encoded)


def _batch_budget_seconds(case_count: int, cycles: int, idle_seconds: list[int]) -> int:
    # Reserve startup health plus two bounded health probes per batch, all requests and idle gates.
    requests = 1 + case_count * cycles
    sleeps = cycles * sum(idle_seconds)
    return 5 + cycles * 10 + requests * 30 + sleeps + 30


def _benchmark_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        variant="C", corpus=args.corpus, origin=args.origin, output="", case=args.case,
        cycles=args.cycles, idle_seconds=args.idle_seconds, cgroup=args.cgroup,
        endpoint=args.endpoint, native_ready_selector=args.native_ready_selector,
        token_env=args.token_env, proxy_token_env=None, proxy=None,
    )


async def _await_interruptibly(awaitable: Awaitable[Any], stop_event: asyncio.Event) -> Any:
    work = asyncio.create_task(awaitable)
    interrupted = asyncio.create_task(stop_event.wait())
    done, _pending = await asyncio.wait({work, interrupted}, return_when=asyncio.FIRST_COMPLETED)
    if interrupted in done and stop_event.is_set():
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        raise SoakInterrupted("sigterm")
    interrupted.cancel()
    await asyncio.gather(interrupted, return_exceptions=True)
    return await work


def _install_sigterm(loop: asyncio.AbstractEventLoop, event: asyncio.Event):
    old_handler = None
    try:
        loop.add_signal_handler(signal.SIGTERM, event.set)
        return lambda: loop.remove_signal_handler(signal.SIGTERM)
    except (NotImplementedError, RuntimeError):
        old_handler = signal.getsignal(signal.SIGTERM)

        def on_signal(_signum, _frame):
            loop.call_soon_threadsafe(event.set)

        signal.signal(signal.SIGTERM, on_signal)

        def restore():
            signal.signal(signal.SIGTERM, old_handler)
        return restore


async def run(args: argparse.Namespace, *, batch_runner=None, health_probe=None,
              cgroup_sampler=None, monotonic: Callable[[], float] = time.monotonic,
              wallclock: Callable[[], float] = time.time,
              sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
              stop_event: asyncio.Event | None = None) -> dict[str, Any]:
    if args.variant != "C":
        raise SoakError("soak_variant_must_be_c")
    if not MIN_DURATION <= args.duration_seconds <= MAX_DURATION:
        raise SoakError("duration_out_of_bounds")
    if not 1 <= args.cycles <= MAX_CYCLES:
        raise SoakError("cycles_out_of_bounds")
    if any(type(value) is not int or value < 0 or value > 60 for value in args.idle_seconds):
        raise SoakError("idle_duration_out_of_bounds")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", args.token_env):
        raise SoakError("invalid_token_env_name")
    if not os.environ.get(args.token_env):
        raise SoakError("service_token_missing")
    try:
        args.origin = benchmark.validate_origin(args.origin)
        args.endpoint = benchmark._validate_internal_endpoint(args.endpoint, "endpoint_required")
    except benchmark.BenchmarkError as exc:
        raise SoakError(_safe_error(exc)) from None
    if args.native_ready_selector != benchmark.READY_SELECTOR:
        raise SoakError("native_ready_selector_mismatch")
    if args.output_dir.exists() and args.output_dir.is_symlink():
        raise SoakError("invalid_output_directory")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    reports, summaries = _output_files(args.output_dir)
    # This driver measures one uninterrupted lifetime. Prior output belongs to
    # another run; appending it would silently reset elapsed time after restart.
    if reports or summaries:
        raise SoakError("output_directory_not_empty")
    next_batch = 1

    bench_args = _benchmark_args(args)
    try:
        corpus, corpus_digest = benchmark.load_corpus(Path(args.corpus))
    except benchmark.BenchmarkError as exc:
        raise SoakError(_safe_error(exc)) from None
    case_ids = {case["id"] for case in corpus["cases"]}
    if any(not isinstance(value, str) or not SAFE_ID.fullmatch(value) for value in (args.case or [])):
        raise SoakError("invalid_case_id")
    selected_ids = set(args.case or [])
    if selected_ids - case_ids:
        raise SoakError("unknown_case_id")
    case_count = len(selected_ids) if selected_ids else len(corpus["cases"])
    expected_case_ids = [case["id"] for case in corpus["cases"]
                         if not selected_ids or case["id"] in selected_ids]
    if not case_count:
        raise SoakError("empty_case_selection")
    budget = _batch_budget_seconds(case_count, args.cycles, args.idle_seconds)
    if budget > MAX_BATCH_BUDGET_SECONDS:
        raise SoakError("batch_budget_exceeds_bound")

    batch_runner = batch_runner or benchmark.run
    health_probe = health_probe or benchmark._probe_native_capabilities
    cgroup_sampler = cgroup_sampler or benchmark.sample_cgroups
    cgroups = benchmark.parse_cgroups(args.cgroup)
    if not {"worker", "cloak", "gateway"}.issubset(cgroups):
        raise SoakError("required_cgroups_missing")
    started = monotonic()
    started_wall = wallclock()
    stop_event = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    restore_signal = _install_sigterm(loop, stop_event)
    last_oom_snapshot: dict[str, dict[str, int]] | None = None
    summary: dict[str, Any] = {
        "schema_version": 1, "state": "running", "variant": "C", "started_unix_seconds": int(started_wall),
        "run_policy": "uninterrupted_only",
        "requested_duration_seconds": args.duration_seconds, "cycles_per_batch": args.cycles,
        "batch_budget_seconds": budget, "case_ids": expected_case_ids,
        "completed_batches": 0,
        "batches_this_invocation": 0,
        "next_batch_index": next_batch, "last_batch_index": 0,
        "failure_code": None, "elapsed_seconds": 0,
    }

    async def persist_summary(state: str, failure_code: str | None = None):
        summary["state"] = state
        summary["failure_code"] = failure_code
        summary["elapsed_seconds"] = round(max(0.0, monotonic() - started), 2)
        summary["updated_unix_seconds"] = int(wallclock())
        current_bytes = _storage_bytes(args.output_dir)
        old_size = (args.output_dir / "summary.json").stat().st_size if (args.output_dir / "summary.json").exists() else 0
        if current_bytes - old_size + SUMMARY_RESERVE_BYTES > MAX_STORAGE_BYTES:
            raise SoakError("storage_limit_reached")
        _atomic_json(args.output_dir / "summary.json", summary, max_bytes=SUMMARY_RESERVE_BYTES)

    async def check_health() -> None:
        await health_probe(args.endpoint)

    async def read_cgroup_snapshot() -> tuple[dict[str, dict[str, int]], dict[str, Any]]:
        measured = cgroup_sampler(cgroups)
        if asyncio.iscoroutine(measured):
            measured = await measured
        snapshot: dict[str, dict[str, int]] = {}
        for name in ("worker", "cloak", "gateway"):
            metrics = measured.get(name)
            if not isinstance(metrics, dict) or any(type(metrics.get(key)) is not int or metrics[key] < 0
                                                    for key in ("oom_events", "oom_kill_events",
                                                                "memory_current_bytes", "pids")):
                raise SoakError("cgroup_metric_unavailable")
            snapshot[name] = {key: metrics[key] for key in ("oom_events", "oom_kill_events")}
        return snapshot, measured

    def check_oom_delta(previous: dict[str, dict[str, int]], current: dict[str, dict[str, int]]) -> None:
        for name, values in current.items():
            for key in ("oom_events", "oom_kill_events"):
                delta = values[key] - previous[name][key]
                if delta < 0:
                    raise SoakError("cgroup_metrics_reset")
                if delta > 0:
                    raise SoakError("oom_event_detected")

    async def final_idle() -> None:
        nonlocal last_oom_snapshot
        await check_health()
        current_oom, _initial_metrics = await read_cgroup_snapshot()
        if last_oom_snapshot is not None:
            check_oom_delta(last_oom_snapshot, current_oom)
        last_oom_snapshot = current_oom
        summary["final_idle"] = {"start_unix_seconds": int(wallclock()), "sample_count": 0,
                                  "last_sample": None}
        while monotonic() - started < args.duration_seconds:
            remaining = args.duration_seconds - (monotonic() - started)
            await _await_interruptibly(sleep(min(30, max(0.0, remaining))), stop_event)
            await check_health()
            current_oom, raw_metrics = await read_cgroup_snapshot()
            if last_oom_snapshot is not None:
                check_oom_delta(last_oom_snapshot, current_oom)
            deltas = {}
            for name, counts in current_oom.items():
                metrics = raw_metrics[name]
                previous = last_oom_snapshot[name] if last_oom_snapshot is not None else counts
                deltas[name] = {"memory_current_bytes": metrics["memory_current_bytes"],
                                "pids": metrics["pids"],
                                "oom_events_delta": counts["oom_events"] - previous["oom_events"],
                                "oom_kill_events_delta": counts["oom_kill_events"] - previous["oom_kill_events"]}
            summary["final_idle"]["sample_count"] += 1
            summary["final_idle"]["last_sample"] = {"elapsed_seconds": round(monotonic() - started, 2),
                                                       "cgroups": deltas}
            last_oom_snapshot = current_oom
            await persist_summary("running")

    failure_code = None
    state = "failed"
    try:
        await check_health()
        await persist_summary("running")
        last_oom_snapshot, _initial_metrics = await read_cgroup_snapshot()
        while monotonic() - started < args.duration_seconds:
            if stop_event.is_set():
                raise SoakInterrupted("sigterm")
            remaining = args.duration_seconds - (monotonic() - started)
            if remaining < budget:
                if summary["batches_this_invocation"] == 0:
                    raise SoakError("no_full_batch_completed")
                await final_idle()
                break
            await check_health()
            batch_index = next_batch
            batch_started = monotonic()
            try:
                report = await _await_interruptibly(batch_runner(bench_args), stop_event)
                safe_report = _redact_batch_report(report)
                safe_report["batch_index"] = batch_index
                safe_report["batch_elapsed_seconds"] = round(monotonic() - batch_started, 2)
            except SoakInterrupted:
                raise
            except Exception as exc:
                safe_report = {"schema_version": 1, "variant": "C", "batch_index": batch_index,
                               "status": "failed", "error": _safe_error(exc),
                               "batch_elapsed_seconds": round(monotonic() - batch_started, 2)}
                diagnostic = _safe_capability_diagnostic(exc)
                if diagnostic:
                    safe_report["error_diagnostic"] = diagnostic
                readiness = _safe_readiness_diagnostic(exc)
                if readiness:
                    safe_report["readiness_diagnostic"] = readiness
            encoded_size = len((json.dumps(safe_report, sort_keys=True, indent=2) + "\n").encode())
            current_bytes = _storage_bytes(args.output_dir)
            if current_bytes + encoded_size + SUMMARY_RESERVE_BYTES > MAX_STORAGE_BYTES:
                raise SoakError("storage_limit_reached")
            if batch_index > MAX_BATCHES:
                raise SoakError("batch_limit_reached")
            batch_path = args.output_dir / f"batch-{batch_index:06d}.json"
            if batch_path.exists():
                raise SoakError("batch_report_already_exists")
            _atomic_json(batch_path, safe_report, max_bytes=MAX_BATCH_REPORT_BYTES)
            summary["completed_batches"] += 1
            summary["batches_this_invocation"] += 1
            summary["last_batch_index"] = batch_index
            next_batch = batch_index + 1
            summary["next_batch_index"] = next_batch
            actual_pairs = {(row.get("case_id"), row.get("cycle")) for row in safe_report.get("outcomes", [])}
            expected_pairs = {(case_id, cycle) for cycle in range(1, args.cycles + 1)
                              for case_id in expected_case_ids}
            if (safe_report.get("case_ids") != expected_case_ids or safe_report.get("cycles") != args.cycles
                    or safe_report.get("corpus_sha256") != corpus_digest or actual_pairs != expected_pairs
                    or safe_report.get("readiness_profile") is None
                    or len(safe_report.get("outcomes", [])) != len(expected_pairs)
                    or any(row.get("expectation", {}).get("passed") is not True
                           for row in safe_report.get("outcomes", []))):
                raise SoakError("batch_contract_mismatch")
            if safe_report.get("status") != "passed" or safe_report.get("mismatch_count", 0) != 0:
                raise SoakError("batch_mismatch")
            resources = safe_report.get("resource_metrics", {}).get("cgroups", {})
            if safe_report.get("resource_metrics", {}).get("sample_count", 0) < 1:
                raise SoakError("cgroup_metric_unavailable")
            if not {"worker", "cloak", "gateway"}.issubset(resources):
                raise SoakError("cgroup_metric_unavailable")
            for metrics in resources.values():
                required_metrics = ("memory_current_sampled_max_bytes", "memory_current_last_bytes",
                                    "memory_anon_last_bytes", "memory_file_last_bytes",
                                    "memory_shmem_last_bytes", "cpu_usage_usec_delta", "pids_last",
                                    "oom_events_delta", "oom_kill_events_delta")
                if any(type(metrics.get(key)) is not int or metrics[key] < 0 for key in required_metrics):
                    raise SoakError("cgroup_metric_unavailable")
                if metrics["oom_events_delta"] < 0 or metrics["oom_kill_events_delta"] < 0:
                    raise SoakError("cgroup_metrics_reset")
                if metrics["oom_events_delta"] > 0 or metrics["oom_kill_events_delta"] > 0:
                    raise SoakError("oom_event_detected")
            current_oom, _current_metrics = await read_cgroup_snapshot()
            if last_oom_snapshot is not None:
                check_oom_delta(last_oom_snapshot, current_oom)
            last_oom_snapshot = current_oom
            if monotonic() - started < args.duration_seconds:
                await persist_summary("running")
        if monotonic() - started < args.duration_seconds:
            raise SoakError("duration_not_reached")
        state = "passed"
    except SoakInterrupted:
        state = "interrupted"
        failure_code = "sigterm"
        summary["signal"] = "SIGTERM"
    except Exception as exc:
        failure_code = _safe_error(exc) if isinstance(exc, (benchmark.BenchmarkError, SoakError)) else "benchmark_failed"
        diagnostic = _safe_capability_diagnostic(exc)
        if diagnostic:
            summary["failure_diagnostic"] = diagnostic
        readiness = _safe_readiness_diagnostic(exc)
        if readiness:
            summary["readiness_diagnostic"] = readiness
        state = "failed"
    finally:
        restore_signal()
        try:
            await persist_summary(state, failure_code)
        except Exception:
            if state != "interrupted":
                state = "failed"
                failure_code = "storage_limit_reached"
    result = dict(summary)
    if state != "passed":
        result["state"] = state
        result["failure_code"] = failure_code
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("C",), default="C")
    parser.add_argument("--duration-seconds", type=int, default=86400)
    parser.add_argument("--cycles", type=int, default=6)
    parser.add_argument("--idle-seconds", type=lambda text: [int(value) for value in text.split(",")],
                        default=[5, 30, 60])
    parser.add_argument("--corpus", default=str(DEFAULT_CORPUS))
    parser.add_argument("--origin", default="https://native-fixture.test")
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--cgroup", action="append", default=[], metavar="NAME=/CGROUP/PATH")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--native-ready-selector", default=benchmark.READY_SELECTOR)
    parser.add_argument("--token-env", default="EXTRACTOR_TEST_TOKEN")
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(run(args))
    except (SoakError, benchmark.BenchmarkError) as exc:
        print(f"soak failed: {_safe_error(exc)}", file=sys.stderr)
        return 2
    return 0 if result["state"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
