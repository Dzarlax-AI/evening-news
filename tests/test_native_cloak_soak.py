import asyncio
import importlib.util
import json
import os
from argparse import Namespace
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "soak_native_cloak.py"
SPEC = importlib.util.spec_from_file_location("soak_native_cloak_test", SCRIPT)
soak = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(soak)
CORPUS_SHA256 = soak.benchmark.load_corpus(soak.DEFAULT_CORPUS)[1]


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def wallclock(self):
        return 1_800_000_000 + self.value

    async def sleep(self, seconds):
        self.value += seconds


def _args(tmp_path, *, duration=60):
    return Namespace(
        variant="C", duration_seconds=duration, cycles=1, idle_seconds=[],
        corpus=str(ROOT / "tests" / "fixtures" / "native-cloak" / "corpus.json"),
        origin="https://native-fixture.test", case=["article-js-cookie"],
        cgroup=["worker=/cg/worker", "cloak=/cg/cloak", "gateway=/cg/gateway"],
        endpoint="http://extractor", native_ready_selector=soak.benchmark.READY_SELECTOR,
        token_env="SOAK_TEST_TOKEN", output_dir=tmp_path / "soak-report",
    )


def _metrics(oom=0):
    return {name: {"oom_events_delta": oom, "oom_kill_events_delta": 0,
                   "memory_current_sampled_max_bytes": 100, "memory_current_last_bytes": 90,
                   "memory_lifetime_peak_bytes": 150, "memory_anon_last_bytes": 60,
                   "memory_lifetime_peak_source": "kernel_memory.peak",
                   "memory_file_last_bytes": 30, "memory_shmem_last_bytes": 0,
                   "cpu_usage_usec_delta": 4, "pids_last": 2}
            for name in ("worker", "cloak", "gateway")}


def _batch_report(*, status="passed", mismatches=0, oom=0, sample_count=1):
    return {
        "schema_version": 1, "variant": "C", "measurement_scope": "internal",
        "readiness_profile": {
            "policy": "native_fixture_selector_and_network_gate",
            "selector": soak.benchmark.READY_SELECTOR,
            "minimum_navigation_ms": soak.benchmark.MIN_NAVIGATION_MS,
            "dom_stable_ms": soak.benchmark.DOM_STABLE_MS,
            "network_quiet_ms": soak.benchmark.NETWORK_QUIET_MS,
            "active_requests": 0, "main_document_status": 200,
            "go_profile_basis": "checked_in_source_not_live_attestation",
            "unreviewed_secret": "must not escape",
        },
        "corpus_sha256": CORPUS_SHA256, "case_ids": ["article-js-cookie"], "cycles": 1,
        "repeat_scope": "same-process", "ready_selector": "not trusted input",
        "native_capabilities": {"ready": True, "token": "must not escape"},
        "baseline_cgroups": _metrics(), "warmup": {"case_id": "article-js-cookie", "outcome": "success"},
        "idle_samples": [], "outcomes": [{"case_id": "article-js-cookie", "cycle": 1,
            "outcome": "success", "state": "extracted", "content": "private body", "url": "https://secret",
            "token": "must not escape", "expectation": {"passed": True, "state_match": True}}],
        "latency_ms": {"count": 1, "p50": 1.0, "p95": 1.0, "p95_method": "nearest_rank"},
        "resource_metrics": {"sample_count": sample_count, "cgroups": _metrics(oom),
            "aggregate_sampled_memory_current_bytes_max": 300, "kernel_peak_available": True},
        "mismatch_count": mismatches, "status": status,
        "unreviewed": {"response": "do not write"},
    }


def _patch_runtime(monkeypatch, clock, *, report=None, batch_error=None, cgroup_metrics=None):
    monkeypatch.setattr(soak, "_batch_budget_seconds", lambda *_args: 5)
    monkeypatch.setattr(soak.benchmark, "parse_cgroups", lambda _values: {
        "worker": Path("/cg/worker"), "cloak": Path("/cg/cloak"), "gateway": Path("/cg/gateway")})
    calls = {"batch": 0, "health": 0}

    async def runner(_args):
        calls["batch"] += 1
        clock.value += 57
        if batch_error:
            raise batch_error
        return report or _batch_report()

    async def health(_endpoint):
        calls["health"] += 1
        return {"ready": True}

    def sample(_cgroups):
        if cgroup_metrics is not None:
            return cgroup_metrics
        return {name: {"memory_current_bytes": 90, "pids": 2,
                       "oom_events": 0, "oom_kill_events": 0}
                for name in ("worker", "cloak", "gateway")}

    return runner, health, sample, calls


def _run(args, clock, runner, health, sample, *, stop_event=None):
    return asyncio.run(soak.run(args, batch_runner=runner, health_probe=health,
                                cgroup_sampler=sample, monotonic=clock.monotonic,
                                wallclock=clock.wallclock, sleep=clock.sleep,
                                stop_event=stop_event))


def test_duration_requires_full_batch_then_samples_final_idle_and_redacts(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, health, sample, calls = _patch_runtime(monkeypatch, clock)
    result = _run(_args(tmp_path), clock, runner, health, sample)

    assert result["state"] == "passed"
    assert result["run_policy"] == "uninterrupted_only"
    assert result["elapsed_seconds"] >= 60
    assert result["batches_this_invocation"] == 1
    assert result["final_idle"]["sample_count"] == 1
    assert calls["health"] >= 2
    batch = json.loads((tmp_path / "soak-report" / "batch-000001.json").read_text())
    encoded = json.dumps(batch)
    assert "private body" not in encoded and "https://secret" not in encoded
    assert "must not escape" not in encoded and "unreviewed" not in encoded
    assert batch["ready_selector"] == soak.benchmark.READY_SELECTOR
    assert batch["measurement_scope"] == "C_worker_cloak_gateway_cgroups_only"
    assert batch["readiness_profile"]["main_document_status"] == 200
    assert batch["resource_metrics"]["cgroups"]["worker"]["memory_lifetime_peak_source"] == "kernel_memory.peak"
    assert json.loads((tmp_path / "soak-report" / "summary.json").read_text())["state"] == "passed"


@pytest.mark.parametrize("report,expected", [
    (_batch_report(status="mismatch", mismatches=1), "batch_mismatch"),
    (_batch_report(oom=1), "oom_event_detected"),
    (_batch_report(sample_count=0), "cgroup_metric_unavailable"),
])
def test_batch_mismatch_oom_and_missing_metrics_fail_fast(tmp_path, monkeypatch, report, expected):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, health, sample, calls = _patch_runtime(monkeypatch, clock, report=report)
    result = _run(_args(tmp_path), clock, runner, health, sample)
    assert result["state"] == "failed"
    assert result["failure_code"] == expected
    assert calls["batch"] == 1
    assert json.loads((tmp_path / "soak-report" / "batch-000001.json").read_text())["status"] == report["status"]


def test_batch_exception_is_persisted_as_sanitized_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, health, sample, _calls = _patch_runtime(
        monkeypatch, clock, batch_error=soak.benchmark.BenchmarkError("native_capability_unavailable"))
    result = _run(_args(tmp_path), clock, runner, health, sample)
    assert result["state"] == "failed"
    batch = json.loads((tmp_path / "soak-report" / "batch-000001.json").read_text())
    assert batch["error"] == "native_capability_unavailable"
    assert "secret-token" not in json.dumps(batch) and "https://" not in json.dumps(batch)


def test_unhealthy_service_fails_before_starting_a_batch(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, _health, sample, calls = _patch_runtime(monkeypatch, clock)

    async def unhealthy(_endpoint):
        calls["health"] += 1
        raise soak.benchmark.BenchmarkError("native_capability_unavailable")

    result = _run(_args(tmp_path), clock, runner, unhealthy, sample)
    assert result["state"] == "failed"
    assert result["failure_code"] == "native_capability_unavailable"
    assert calls["batch"] == 0
    assert not list((tmp_path / "soak-report").glob("batch-*.json"))


def test_oom_during_final_idle_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, health, _sample, calls = _patch_runtime(monkeypatch, clock)
    snapshots = 0

    def sample(_cgroups):
        nonlocal snapshots
        snapshots += 1
        oom = 1 if snapshots > 1 else 0
        return {name: {"memory_current_bytes": 90, "pids": 2,
                       "oom_events": oom, "oom_kill_events": 0}
                for name in ("worker", "cloak", "gateway")}

    result = _run(_args(tmp_path), clock, runner, health, sample)
    assert result["state"] == "failed"
    assert result["failure_code"] == "oom_event_detected"
    assert calls["batch"] == 1


@pytest.mark.parametrize("existing_name", ["batch-000001.json", "summary.json"])
def test_prior_output_is_rejected_without_deleting_it(tmp_path, monkeypatch, existing_name):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, health, sample, _calls = _patch_runtime(monkeypatch, clock)
    out = tmp_path / "soak-report"
    out.mkdir()
    existing = out / existing_name
    existing.write_text('{"state":"historic"}\n')
    with pytest.raises(soak.SoakError, match="output_directory_not_empty"):
        _run(_args(tmp_path), clock, runner, health, sample)
    assert existing.read_text() == '{"state":"historic"}\n'
    assert not (out / "batch-000002.json").exists()


def test_storage_cap_fails_without_writing_batch(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    monkeypatch.setattr(soak, "MAX_STORAGE_BYTES", soak.SUMMARY_RESERVE_BYTES + 150)
    clock = FakeClock()
    runner, health, sample, _calls = _patch_runtime(monkeypatch, clock)
    result = _run(_args(tmp_path), clock, runner, health, sample)
    assert result["state"] == "failed"
    assert result["failure_code"] == "storage_limit_reached"
    assert not list((tmp_path / "soak-report").glob("batch-*.json"))


def test_duration_too_short_for_one_complete_batch_does_not_claim_pass(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    monkeypatch.setattr(soak, "_batch_budget_seconds", lambda *_args: 120)
    clock = FakeClock()
    runner, health, sample, calls = _patch_runtime(monkeypatch, clock)
    monkeypatch.setattr(soak, "_batch_budget_seconds", lambda *_args: 120)
    args = _args(tmp_path, duration=60)
    result = _run(args, clock, runner, health, sample)
    assert result["state"] == "failed"
    assert result["failure_code"] == "no_full_batch_completed"
    assert calls["batch"] == 0


def test_sigterm_records_interrupted_and_returns_nonzero_state(tmp_path, monkeypatch):
    monkeypatch.setenv("SOAK_TEST_TOKEN", "secret-token-that-is-not-reported")
    clock = FakeClock()
    runner, health, sample, calls = _patch_runtime(monkeypatch, clock)
    stop = asyncio.Event()
    stop.set()
    result = _run(_args(tmp_path), clock, runner, health, sample, stop_event=stop)
    assert result["state"] == "interrupted"
    assert result["failure_code"] == "sigterm"
    assert calls["batch"] == 0
    assert json.loads((tmp_path / "soak-report" / "summary.json").read_text())["state"] == "interrupted"


def test_cli_is_c_only_and_has_bounded_defaults():
    args = soak.build_parser().parse_args(["--endpoint", "http://extractor", "--output-dir", "reports"])
    assert args.variant == "C" and args.duration_seconds == 86400 and args.cycles == 6
    with pytest.raises(SystemExit):
        soak.build_parser().parse_args(["--variant", "B", "--endpoint", "http://extractor", "--output-dir", "reports"])


def test_default_full_batch_budget_fits_the_declared_30_minute_cap():
    budget = soak._batch_budget_seconds(5, 6, [5, 30, 60])
    assert budget == 1595 and budget <= soak.MAX_BATCH_BUDGET_SECONDS
    assert soak._batch_budget_seconds(16, 6, [5, 30, 60]) > soak.MAX_BATCH_BUDGET_SECONDS
    assert soak.MAX_BATCHES >= 1024
