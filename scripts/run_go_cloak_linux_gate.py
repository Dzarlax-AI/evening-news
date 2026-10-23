"""Run the bounded, synthetic Go/Cloak gate on the isolated Linux host."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

GO_IMAGE = 'golang:1.25.6-bookworm@sha256:f4490d7b261d73af4543c46ac6597d7d101b6e1755bcdd8c5159fda7046b6b3e'
GO_IMAGE_ID = 'sha256:5fa4affaa97b8f2ac6c9395a818712044c04d45197c9ebe645aa26322ef0e201'
APP_IMAGE = 'ghcr.io/dzarlax-ai/rss-summarizer@sha256:b6abbb428543e846d0310b86ff41236a6df028985a00ebfe4714357d22e67060'
APP_IMAGE_ID = 'sha256:990eea5e6c1ea30e68236cc11d7128ca904f707557dd1728aec01bda96d71bef'
CLOAK_IMAGE = 'cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4'
CLOAK_IMAGE_ID = 'sha256:1a9933043ea4b9639446e31feec79a3637b056fab0fa87d96d1e10263c4d69be'
MAX_STAGE_BYTES = 2 * 1024**3
MIN_DISK_START = 7 * 1024**3
MIN_DISK_ABORT = 5 * 1024**3
# Admission includes the candidate phase budget, 128 MiB supervisor allowance,
# and the fixed 1536 MiB production reserve.
GO_MEM_MIN = 2432 * 1024**2
BROWSER_MEM_MIN = 2432 * 1024**2
GO_CONTAINER_MEMORY = '768m'
BROWSER_WORKING_SET_BUDGET = 768 * 1024**2
RESERVE_MEM_MIN = 1536 * 1024**2
HOST_SUPERVISOR_RSS_LIMIT_KIB = 128 * 1024
MEMORY_STAT_KEYS = ('anon', 'file', 'shmem', 'kernel', 'slab_reclaimable',
                    'slab_unreclaimable', 'sock', 'inactive_file', 'active_file')
CGROUP_EVENT_KEYS = ('low', 'high', 'max', 'oom', 'oom_kill')
HOST_VMSTAT_KEYS = ('pgscan_direct', 'pgsteal_direct', 'pswpin', 'pswpout', 'allocstall')
PARENT_LABEL = 'linux-vps-gate='
BASE_COMMIT = 'c9441de4a6e0314a3c7073158ac960de11b9b4d3'
BROWSER_ROLE_SUFFIXES = frozenset({
    'browser-anchor', 'control-anchor', 'gateway-anchor', 'egress',
    'cloak', 'relay', 'observer', 'worker', 'origin',
})
BROWSER_CONTAINER_NAME = re.compile(
    r'news-native-shadow-[0-9a-f]{10}-(?P<role>' + '|'.join(sorted(BROWSER_ROLE_SUFFIXES)) + r')'
)
BINARY_MANIFEST_NAME = 'binary-manifest.json'
HASH_CHUNK_BYTES = 1024 * 1024
RUNTIME_BUNDLE_MANIFEST_SHA256 = 'd0c4375301832bc4ff12b25cbe0df43d899bd4f71f06a3ea67abd89c51d8c970'
EXPECTED_GO_INPUTS = 54
EXPECTED_GO_SUFFIX_COUNTS = {'.go': 52, '.mod': 1, '.sum': 1}
EXPECTED_RUNTIME_BINARIES = {'bin-soak/extract-service', 'bin-soak/extract-egress'}
QUALITY_CORPUS_SHA256 = '91fd95a3a91ad172bab7daa1cdd117f61a7ccc81bd7a55f6e82eea422ea0420d'
QUALITY_CASES = ('case-0001', 'case-0008', 'case-0034', 'case-0041', 'case-0050')
QUALITY_CORPUS_PATH = Path('/tmp/news-native-cloak.vh6lLSy2/shadow-private/corpus-run/corpus.json')
QUALITY_PRIVATE_OUTPUT_LIMIT = 256 * 1024**2
QUALITY_PHASES = (('C', 'c-smoke', ('case-0001',)),
                  ('C', 'c-quality', tuple(case for case in QUALITY_CASES if case != 'case-0001')))
NATIVE_STABILITY_CASE_IDS = (
    'article-js-cookie', 'redirect-article', 'slow-article', 'large-dom', 'challenge-page',
)
LIVE_QUALITY_ACTIVE_SECONDS = 22 * 60
LIVE_QUALITY_HARD_SECONDS = 24 * 60
LIVE_QUALITY_STAND_ACTIVE_SECONDS = 6 * 60
LIVE_QUALITY_STAND_HARD_SECONDS = 8 * 60
NATIVE_STABILITY_DURATION_SECONDS = 600
NATIVE_STABILITY_ACTIVE_SECONDS = 14 * 60
NATIVE_STABILITY_HARD_SECONDS = 16 * 60


class GateError(RuntimeError):
    pass


def safe_error_name(exc: BaseException) -> str:
    code = str(exc)
    if isinstance(exc, GateError) and re.fullmatch(r'[a-z0-9_]{1,80}', code):
        return code
    return type(exc).__name__


def command(args, *, timeout=8, check=True):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise GateError('command_failed_' + Path(args[0]).name)
    return result


def mem_available() -> int:
    rows = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(rows['MemAvailable'].split()[0]) * 1024


def memory_psi_full_avg10() -> float:
    for line in Path('/proc/pressure/memory').read_text().splitlines():
        if line.startswith('full '):
            fields = dict(part.split('=', 1) for part in line.split()[1:])
            return float(fields['avg10'])
    raise GateError('memory_psi_unavailable')


def tree_bytes(path: Path) -> int:
    total = 0
    for root, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            item = Path(root) / name
            if item.is_symlink():
                raise GateError('stage_symlink_forbidden')
            try:
                if item.is_file():
                    total += item.stat().st_size
                    if total > MAX_STAGE_BYTES:
                        return total
            except FileNotFoundError:
                # Go may replace cache and temporary files while the guard scans.
                continue
    return total


def _sha256_file(path: Path) -> str:
    """Hash provenance inputs in bounded memory rather than loading whole files."""
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        while True:
            chunk = source.read(HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_source_manifest(stage: Path) -> str:
    path = stage/'source-manifest.json'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
        raise GateError('source_manifest_missing_or_invalid')
    try:
        manifest = json.loads(path.read_text())
        rows = manifest['source_sha256']
    except (OSError, ValueError, KeyError, TypeError):
        raise GateError('source_manifest_invalid') from None
    if manifest.get('base_commit') != BASE_COMMIT or not isinstance(rows, dict) or not rows:
        raise GateError('source_manifest_base_mismatch')
    for name, expected in rows.items():
        if (not isinstance(name, str) or not name.startswith('benchmark/')
                or '..' in Path(name).parts or not re.fullmatch(r'[0-9a-f]{64}', str(expected))):
            raise GateError('source_manifest_entry_invalid')
        item = stage/name
        if item.is_symlink() or not item.is_file():
            raise GateError('source_manifest_source_missing')
        digest = _sha256_file(item)
        if digest != expected:
            raise GateError('source_manifest_hash_mismatch')
    return _sha256_file(path)


def verify_runtime_bundle(stage: Path) -> str:
    """Verify the exact staged Go inputs and previously built Linux binaries."""
    path = stage / BINARY_MANIFEST_NAME
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise GateError('runtime_bundle_manifest_missing_or_invalid')
    if _sha256_file(path) != RUNTIME_BUNDLE_MANIFEST_SHA256:
        raise GateError('runtime_bundle_manifest_hash_mismatch')
    try:
        manifest = json.loads(path.read_text())
        go_rows = manifest['go_source_sha256']
        binary_rows = manifest['binaries']
    except (OSError, ValueError, KeyError, TypeError):
        raise GateError('runtime_bundle_manifest_invalid') from None
    if manifest.get('base_commit') != BASE_COMMIT or not isinstance(go_rows, dict) or not isinstance(binary_rows, dict):
        raise GateError('runtime_bundle_base_mismatch')
    if len(go_rows) != EXPECTED_GO_INPUTS or set(binary_rows) != EXPECTED_RUNTIME_BINARIES:
        raise GateError('runtime_bundle_manifest_file_set_mismatch')
    if any(sum(Path(name).suffix == suffix for name in go_rows) != count
           for suffix, count in EXPECTED_GO_SUFFIX_COUNTS.items()):
        raise GateError('runtime_bundle_manifest_file_set_mismatch')

    source_path = stage / 'source-manifest.json'
    try:
        source_rows = json.loads(source_path.read_text())['source_sha256']
    except (OSError, ValueError, KeyError, TypeError):
        raise GateError('runtime_bundle_source_manifest_invalid') from None
    if {name: source_rows.get(name) for name in go_rows} != go_rows:
        raise GateError('runtime_bundle_source_manifest_mismatch')

    go_root = stage / 'benchmark' / 'go'
    if go_root.is_symlink() or not go_root.is_dir():
        raise GateError('runtime_bundle_source_directory_invalid')
    actual_inputs = set()
    for item in go_root.rglob('*'):
        if item.is_symlink():
            raise GateError('runtime_bundle_source_symlink_forbidden')
        if item.is_file() and item.suffix in EXPECTED_GO_SUFFIX_COUNTS:
            actual_inputs.add(item.relative_to(stage).as_posix())
    if actual_inputs != set(go_rows):
        raise GateError('runtime_bundle_staged_source_file_set_mismatch')
    for name, expected in go_rows.items():
        if (not isinstance(name, str) or not name.startswith('benchmark/go/')
                or '..' in Path(name).parts or Path(name).suffix not in EXPECTED_GO_SUFFIX_COUNTS
                or not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected)):
            raise GateError('runtime_bundle_source_entry_invalid')
        item = stage / name
        if item.is_symlink() or not item.is_file():
            raise GateError('runtime_bundle_source_missing')
        if _sha256_file(item) != expected:
            raise GateError('runtime_bundle_source_hash_mismatch')

    binary_root = stage / 'bin-soak'
    if binary_root.is_symlink() or not binary_root.is_dir():
        raise GateError('runtime_bundle_binary_directory_invalid')
    actual_binaries = {item.relative_to(stage).as_posix() for item in binary_root.iterdir()}
    if actual_binaries != EXPECTED_RUNTIME_BINARIES:
        raise GateError('runtime_bundle_binary_file_set_mismatch')
    for name, expected in binary_rows.items():
        if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
            raise GateError('runtime_bundle_binary_entry_invalid')
        item = stage / name
        if item.is_symlink() or not item.is_file() or not item.stat().st_mode & 0o111:
            raise GateError('runtime_bundle_binary_missing_or_not_executable')
        if _sha256_file(item) != expected:
            raise GateError('runtime_bundle_binary_hash_mismatch')
    return _sha256_file(path)


def proc_tree_rss_kib(pid: int) -> int:
    seen: set[int] = set()
    stack = [pid]
    total = 0
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        try:
            status = Path(f'/proc/{current}/status').read_text()
            line = next(row for row in status.splitlines() if row.startswith('VmRSS:'))
            total += int(line.split()[1])
            children = Path(f'/proc/{current}/task/{current}/children').read_text().split()
            stack.extend(int(value) for value in children)
        except (FileNotFoundError, ProcessLookupError, StopIteration, ValueError):
            continue
    return total


def proc_tree_rss_rows(pid: int, *, limit: int = 12) -> list[dict]:
    """Return bounded RSS metadata for this process and verified descendants only."""
    seen: set[int] = set()
    stack: list[tuple[int, int | None]] = [(pid, None)]
    rows: list[dict] = []
    while stack:
        current, expected_parent = stack.pop()
        if current in seen or current <= 1:
            continue
        seen.add(current)
        try:
            status = Path(f'/proc/{current}/status').read_text()
            fields = {}
            for line in status.splitlines():
                key, sep, value = line.partition(':')
                if sep and key in ('PPid', 'VmRSS'):
                    fields[key] = value.strip()
            if expected_parent is not None and int(fields['PPid']) != expected_parent:
                continue
            rss_fields = fields['VmRSS'].split()
            rss_kib = int(rss_fields[0]) if len(rss_fields) >= 2 and rss_fields[1] == 'kB' else 0
            comm_raw = Path(f'/proc/{current}/comm').read_text().splitlines()[0].strip()
            # comm is a kernel process label, never command arguments. Still
            # keep only a basename-like token and a small safe character set.
            comm_token = os.path.basename(comm_raw.split()[0]) if comm_raw else 'unknown'
            comm = re.sub(r'[^A-Za-z0-9_.-]', '_', comm_token)[:32] or 'unknown'
            rows.append({'pid': current, 'rss_kib': max(0, rss_kib), 'comm': comm})
            children = Path(f'/proc/{current}/task/{current}/children').read_text().split()
            stack.extend((int(child), current) for child in children)
        except (FileNotFoundError, ProcessLookupError, PermissionError, StopIteration,
                ValueError, IndexError, KeyError):
            if current == pid:
                raise
            continue
    return sorted(rows, key=lambda row: row['rss_kib'], reverse=True)[:max(1, limit)]


def production_cgroup(name: str) -> Path:
    pid = int(command(['docker', 'inspect', '--format', '{{.State.Pid}}', name]).stdout.strip())
    if pid <= 1:
        raise GateError('production_process_missing')
    path = next((row[3:] for row in Path(f'/proc/{pid}/cgroup').read_text().splitlines()
                 if row.startswith('0::')), None)
    if not path:
        raise GateError('cgroup_v2_path_missing')
    return Path('/sys/fs/cgroup' + path)


def production_fingerprint(name: str) -> list:
    result = command(['docker', 'inspect', '--format',
        '{{.Id}}|{{.Image}}|{{.State.StartedAt}}|{{.RestartCount}}|{{.State.Status}}|{{.State.OOMKilled}}', name])
    parts = result.stdout.strip().split('|')
    if len(parts) != 6 or parts[4] != 'running' or parts[5] != 'false':
        raise GateError('production_fingerprint_invalid')
    events = dict(line.split() for line in (production_cgroup(name)/'memory.events').read_text().splitlines())
    return parts + [int(events['oom']), int(events['oom_kill'])]


def stopped_web_fingerprint(expected_container_id: str) -> list:
    """Read the intentional maintenance stop state without requiring a live PID."""
    result = command(['docker', 'inspect', '--format',
        '{{.Id}}|{{.Image}}|{{.State.StartedAt}}|{{.RestartCount}}|{{.State.Status}}|{{.State.Running}}|{{.State.Pid}}|{{.State.ExitCode}}|{{.State.OOMKilled}}',
        'news_web'])
    parts = result.stdout.strip().split('|')
    if len(parts) != 9:
        raise GateError('maintenance_web_state_invalid')
    if parts[0] != expected_container_id or parts[1] != APP_IMAGE_ID:
        raise GateError('maintenance_web_container_identity_changed')
    if parts[5] == 'true' or parts[6] not in ('0', ''):
        raise GateError('maintenance_web_unexpectedly_running')
    if (parts[4] != 'exited' or parts[5] != 'false' or parts[6] != '0'
            or parts[7] not in ('0', '143') or parts[8] != 'false'):
        raise GateError('maintenance_web_stop_state_invalid')
    return parts


def validate_maintenance_baseline(stage: Path, stopped: list[str], expected_container_id: str) -> list[str]:
    path = stage/'maintenance-baseline.json'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4096:
        raise GateError('maintenance_baseline_missing_or_invalid')
    try:
        baseline = json.loads(path.read_text())
        running = baseline['news_web']
    except (OSError, ValueError, KeyError, TypeError):
        raise GateError('maintenance_baseline_invalid') from None
    if (not isinstance(running, list) or len(running) != 8
            or running[4] != 'running' or running[5] != 'false'
            or not isinstance(running[0], str) or not isinstance(running[1], str)
            or not isinstance(running[2], str) or not isinstance(running[3], str)
            or any(type(value) is not int or value < 0 for value in running[6:])):
        raise GateError('maintenance_baseline_invalid')
    if running[0] != expected_container_id or running[1] != APP_IMAGE_ID:
        raise GateError('maintenance_web_container_identity_changed')
    if (stopped[0] != running[0] or stopped[1] != running[1]
            or stopped[2] != running[2] or stopped[3] != running[3]):
        raise GateError('maintenance_web_baseline_mismatch')
    return running


def validate_maintenance_options(runtime_only: bool, news_web_stopped: bool,
                                 container_id: str | None, *, short_window=False,
                                 observe_resource_thresholds=False, live_quality=False,
                                 soak=False, long_synthetic_soak=False):
    if news_web_stopped and not runtime_only:
        raise GateError('maintenance_requires_runtime_only')
    if short_window and not runtime_only:
        raise GateError('short_window_requires_runtime_only')
    if observe_resource_thresholds and news_web_stopped:
        raise GateError('observe_resource_thresholds_conflicts_with_news_web_stopped')
    if news_web_stopped and short_window:
        raise GateError('short_window_conflicts_with_news_web_stopped')
    if observe_resource_thresholds and not runtime_only:
        raise GateError('observe_resource_thresholds_requires_runtime_only')
    if live_quality and (not runtime_only or news_web_stopped or short_window or soak
                         or long_synthetic_soak or not observe_resource_thresholds):
        raise GateError('live_quality_options_invalid')
    if long_synthetic_soak and (not runtime_only or news_web_stopped or short_window or soak
                                or live_quality or not observe_resource_thresholds):
        raise GateError('long_synthetic_soak_options_invalid')
    if observe_resource_thresholds and not short_window and not live_quality and not long_synthetic_soak:
        raise GateError('observe_resource_thresholds_requires_short_window')
    if news_web_stopped != bool(container_id):
        raise GateError('maintenance_container_id_required')
    if container_id and not re.fullmatch(r'[0-9a-f]{64}', container_id):
        raise GateError('maintenance_container_id_invalid')


def validate_worker_recovery_options(*, enabled, runtime_only, short_window,
                                     news_web_stopped, live_quality,
                                     long_synthetic_soak, observe_resource_thresholds):
    if enabled and (not runtime_only or not short_window or news_web_stopped
            or live_quality or long_synthetic_soak or observe_resource_thresholds):
        raise GateError('worker_recovery_gate_profile_invalid')


def worker_recovery_stand_args(stage: Path, deadline: int, label: str) -> list[str]:
    return ['python3', str(stage/'benchmark/scripts/native_shadow_stand.py'), '--stage', str(stage),
            '--bootstrap', '--gate-only', '--bounded-profile', '--supervisor-cleanup',
            '--worker-recovery-gate', '--deadline', str(deadline), '--supervisor-label', label]


def verify_quality_corpus(path: Path) -> str:
    """Verify the one approved private corpus before starting any container."""
    if path != QUALITY_CORPUS_PATH:
        raise GateError('quality_corpus_path_mismatch')
    try:
        if path.is_symlink() or not path.is_file():
            raise ValueError
        info = path.stat()
        parent = path.parent.stat()
        if (info.st_uid != os.geteuid() or info.st_mode & 0o077
                or not path.parent.is_dir() or path.parent.is_symlink()
                or parent.st_uid != os.geteuid() or parent.st_mode & 0o077
                or info.st_size > 64 * 1024**2):
            raise ValueError
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        data = json.loads(path.read_text())
        cases = data.get('cases') if isinstance(data, dict) and data.get('version') == 1 else None
        ids = [case.get('id') for case in cases] if isinstance(cases, list) else None
        if (not isinstance(ids, list) or len(ids) != 50 or len(set(ids)) != len(ids)
                or not set(QUALITY_CASES).issubset(ids)):
            raise ValueError
    except (OSError, ValueError, TypeError):
        raise GateError('quality_corpus_invalid') from None
    if digest != QUALITY_CORPUS_SHA256:
        raise GateError('quality_corpus_hash_mismatch')
    return digest


def prepare_quality_output(stage: Path) -> tuple[Path, Path]:
    """Create a new private capture directory and reject any prior ledger/history."""
    private = stage / 'shadow-private'
    if private.is_symlink():
        raise GateError('quality_private_directory_invalid')
    if not private.exists():
        private.mkdir(mode=0o700)
    if (not private.is_dir() or private.stat().st_uid != os.geteuid()
            or private.stat().st_mode & 0o077):
        raise GateError('quality_private_directory_invalid')
    captures = private / 'captures'
    if captures.exists() or captures.is_symlink():
        raise GateError('quality_captures_not_fresh')
    receipt = private / 'quality-gate-receipt'
    if receipt.exists() or receipt.is_symlink():
        raise GateError('quality_receipt_not_fresh')
    captures.mkdir(mode=0o700)
    ledger = captures / 'ledger.json'
    if ledger.exists() or ledger.is_symlink():
        raise GateError('quality_ledger_not_fresh')
    return private, captures


def validate_quality_capture_state(captures: Path, phase: str) -> dict:
    """Validate aggregate reports and the private attempt ledger without copying payloads."""
    expected = set(QUALITY_CASES)
    try:
        if captures.is_symlink() or not captures.is_dir() or captures.stat().st_mode & 0o077:
            raise ValueError
        if tree_bytes(captures) > QUALITY_PRIVATE_OUTPUT_LIMIT:
            raise GateError('quality_private_output_limit')
        ledger_path = captures / 'ledger.json'
        if ledger_path.is_symlink() or not ledger_path.is_file() or ledger_path.stat().st_size > 65536:
            raise ValueError
        ledger = json.loads(ledger_path.read_text())
        if (ledger.get('version') != 1 or ledger.get('corpus_sha256') != QUALITY_CORPUS_SHA256
                or not isinstance(ledger.get('attempted'), dict)
                or not isinstance(ledger.get('blocked_hosts'), dict)
                or not isinstance(ledger.get('last_navigation_epoch'), dict)):
            raise ValueError
        expected_attempts = {'C': []}
        for variant, _, case_ids in QUALITY_PHASES:
            expected_attempts[variant].extend(case_ids)
        if phase == 'c-smoke':
            expected_attempts = {'C': ['case-0001']}
        else:
            expected_attempts = {'C': list(QUALITY_CASES)}
        for variant in ('C',):
            attempted = ledger['attempted'].get(variant)
            if (not isinstance(attempted, list) or len(attempted) != len(set(attempted))
                    or attempted != expected_attempts[variant]):
                raise ValueError
            hosts = ledger['blocked_hosts'].get(variant)
            epoch = ledger['last_navigation_epoch'].get(variant)
            if (not isinstance(hosts, list) or not all(isinstance(host, str) and len(host) <= 253
                                                       for host in hosts)
                    or (epoch is not None and (type(epoch) not in (int, float) or epoch <= 0))):
                raise ValueError
        report_names = {'report-C.json'}
        reports = {}
        for name in report_names:
            path = captures / name
            variant = name[7]
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024**2:
                raise ValueError
            report = json.loads(path.read_text())
            rows = report.get('rows')
            row_ids = [row.get('id') for row in rows] if isinstance(rows, list) else None
            selected = (['case-0001'] if phase == 'c-smoke' else list(QUALITY_CASES))
            if (report.get('schema_version') != 1 or report.get('variant') != variant
                    or report.get('corpus_sha256') != QUALITY_CORPUS_SHA256
                    or report.get('status') != 'complete' or report.get('fatal_error') is not None
                    or report.get('selected_case_ids') != selected or row_ids != selected
                    or report.get('attempted_case_ids') != expected_attempts[variant]):
                raise ValueError
            reports[variant] = report
    except GateError:
        raise
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        raise GateError('quality_capture_validation_failed') from None
    outcome_counts = {}
    for report in reports.values():
        for row in report['rows']:
            outcome = row.get('outcome')
            if not isinstance(outcome, str) or len(outcome) > 40:
                raise GateError('quality_capture_validation_failed')
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
    return {'attempted_case_ids': {key: list(value) for key, value in ledger['attempted'].items()},
            'report_variants': sorted(reports), 'outcome_counts': outcome_counts}


class Supervisor:
    def __init__(self, stage: Path, label_value: str, started: float, baseline: dict,
                 *, maintenance_web_stopped=False, maintenance_container_id=None,
                 short_window=False, observe_resource_thresholds=False, live_quality=False,
                 long_synthetic_soak=False, worker_recovery_gate=False):
        self.stage = stage
        self.label_value = label_value
        self.started = started
        self.maintenance_web_stopped = maintenance_web_stopped
        self.maintenance_container_id = maintenance_container_id
        self.short_window = short_window
        self.observe_resource_thresholds = observe_resource_thresholds
        self.live_quality = live_quality
        self.long_synthetic_soak = long_synthetic_soak
        self.worker_recovery_gate = worker_recovery_gate
        self.deadline = started + (NATIVE_STABILITY_HARD_SECONDS if long_synthetic_soak else
                                   LIVE_QUALITY_HARD_SECONDS if live_quality else
                                   8 * 60 if maintenance_web_stopped or short_window else 30 * 60)
        self.baseline = baseline
        self.unhealthy = 0
        self.high_web_memory_since: float | None = None
        self.last_production = {}
        self.phases: list[dict] = []
        self.browser_phase = False
        self.last_guard_sample: dict | None = None
        self.first_guard_failure_sample: dict | None = None
        self.runtime_working_set_peak_bytes = 0
        self.runtime_container_sampled_peaks: dict[str, int] = {}
        self.runtime_container_memory_stats: dict[str, dict] = {}
        self.quality_captures: Path | None = None
        self.resource_threshold_bypass_counts = {
            'runtime_working_set_above_budget': 0,
            'memory_psi_pressure': 0,
        }
        self.memory_psi_full_avg10_sampled_peak = 0.0
        self.memory_psi_threshold_observations = 0
        self._runtime_diagnostic_identities = {}
        self._runtime_diagnostic_baselines = {}
        self._host_diagnostic_baseline = None
        self._diagnostic_sequence = 0
        self._diagnostic_recorded_for_guard = False
        self.diagnostic_samples: list[dict] = []

    @property
    def label_filter(self):
        return 'label=' + PARENT_LABEL + self.label_value

    def fingerprints(self) -> dict:
        web = (stopped_web_fingerprint(self.maintenance_container_id)
               if self.maintenance_web_stopped else production_fingerprint('news_web'))
        return {'news_web': web, 'infra-chrome': production_fingerprint('infra-chrome')}

    @staticmethod
    def cgroup_path(name: str) -> Path:
        return production_cgroup(name)

    def cgroup_oom_counters(self, name: str) -> list[int]:
        events = dict(line.split() for line in (self.cgroup_path(name)/'memory.events').read_text().splitlines())
        return [int(events['oom']), int(events['oom_kill'])]

    def web_memory_bytes(self) -> int:
        return int((self.cgroup_path('news_web')/'memory.current').read_text())

    def health_probe(self):
        result = subprocess.run(['curl', '--noproxy', '*', '--silent', '--show-error', '--output',
                                 '/dev/null', '--write-out', '%{http_code} %{time_total}', '--max-time', '1',
                                 'http://127.0.0.1:8005/health'], capture_output=True, text=True, timeout=2)
        try:
            status, elapsed = result.stdout.strip().split()
            elapsed = float(elapsed)
        except (ValueError, TypeError):
            status, elapsed = '000', 2.0
        return status, elapsed

    def owned_containers(self) -> list[str]:
        return command(['docker', 'ps', '-a', '--filter', self.label_filter,
                        '--format', '{{.Names}}']).stdout.splitlines()

    def _marked_recovery_worker(self, expected_phase=None):
        if not (self.worker_recovery_gate and expected_phase == 'native-worker-recovery-gate'):
            return None
        marker = self.stage/'.worker-recovery-active'
        try:
            fd = os.open(marker, os.O_RDONLY|os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                raw = stream.read(129)
            candidate = raw.decode('ascii') if stat.S_ISREG(info.st_mode) and info.st_size <= 128 else ''
            match = BROWSER_CONTAINER_NAME.fullmatch(candidate)
            if match and match.group('role') == 'worker':
                return candidate
        except (OSError, UnicodeDecodeError):
            pass
        return None

    def browser_working_set_bytes(self, *, expected_phase=None) -> tuple[int, dict[str, int]]:
        """Sum charged cgroup totals (not RSS) and capture a best-effort stat split."""
        self.runtime_container_memory_stats = {}
        self.runtime_container_diagnostics = {}
        try:
            names = self.owned_containers()
            if len(names) != len(set(names)) or any(not BROWSER_CONTAINER_NAME.fullmatch(name) for name in names):
                raise GateError('runtime_working_set_metric_unavailable')
            if not names:
                return 0, {}
            recovery_worker = self._marked_recovery_worker(expected_phase)
            inspected = command(['docker', 'inspect', '--format',
                '{{.Id}}|{{.Name}}|{{.State.Pid}}|{{index .Config.Labels "linux-vps-gate"}}|{{.State.Running}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.ExitCode}}', *names])
            rows = inspected.stdout.splitlines()
            if len(rows) != len(names):
                raise GateError('runtime_working_set_metric_unavailable')
            paths = []
            roles = set()
            for row in rows:
                container_id, name, pid_text, label, running, restarts, oom, exit_code = row.split('|')
                name = name.lstrip('/')
                match = BROWSER_CONTAINER_NAME.fullmatch(name)
                role = match.group('role') if match else None
                if (not re.fullmatch(r'[0-9a-f]{64}', container_id) or name not in names
                        or label != self.label_value or name in [entry[0] for entry in paths]
                        or role in roles):
                    raise GateError('runtime_working_set_metric_unavailable')
                roles.add(role)
                if int(pid_text) <= 1:
                    if (name == recovery_worker and running == 'false' and pid_text == '0'
                            and restarts == '0' and oom == 'false' and exit_code == '1'):
                        continue
                    raise GateError('runtime_working_set_metric_unavailable')
                if running != 'true':
                    raise GateError('runtime_working_set_metric_unavailable')
                pid = int(pid_text)
                cgroup = next((entry[3:] for entry in Path(f'/proc/{pid}/cgroup').read_text().splitlines()
                               if entry.startswith('0::')), None)
                if not cgroup or '..' in Path(cgroup).parts:
                    raise GateError('runtime_working_set_metric_unavailable')
                paths.append((role, pid, container_id, cgroup, Path('/sys/fs/cgroup') / cgroup.lstrip('/')))
            cgroups = [entry[4] for entry in paths]
            if any(left == right or left in right.parents or right in left.parents
                   for index, left in enumerate(cgroups) for right in cgroups[index + 1:]):
                raise GateError('runtime_working_set_metric_unavailable')
            total = 0
            per_container = {}
            memory_stats = {}
            runtime_diagnostics = {}
            for role, pid, generation, identity, cgroup in paths:
                value = (cgroup / 'memory.current').read_text().strip()
                if not value.isdecimal():
                    raise GateError('runtime_working_set_metric_unavailable')
                per_container[role] = int(value)
                total += per_container[role]
                try:
                    memory_stats[role] = self._memory_stat_diagnostic(cgroup / 'memory.stat')
                except Exception:
                    memory_stats[role] = {'status': 'unavailable',
                        'values_bytes': {key: None for key in MEMORY_STAT_KEYS}}
                try:
                    runtime_diagnostics[role] = self._cgroup_diagnostic(
                        role, pid, generation, identity, cgroup)
                except Exception:
                    runtime_diagnostics[role] = {'status': 'unavailable'}
            self.runtime_container_memory_stats = memory_stats
            self.runtime_container_diagnostics = runtime_diagnostics
            return total, per_container
        except GateError:
            raise
        except (OSError, ValueError, IndexError, StopIteration, subprocess.SubprocessError):
            raise GateError('runtime_working_set_metric_unavailable') from None

    @staticmethod
    def _read_numeric_file(path: Path, *, allow_max=False):
        try:
            value = path.read_text().strip()
        except (OSError, UnicodeError):
            return None, 'unavailable'
        if allow_max and value == 'max':
            return 'max', 'ok'
        if value.isdecimal():
            return int(value), 'ok'
        return None, 'malformed'

    @staticmethod
    def _pressure_diagnostic(path: Path) -> dict:
        result = {}
        try:
            lines = path.read_text().splitlines()
        except (OSError, UnicodeError):
            return {'status': 'unavailable', 'some': None, 'full': None}
        try:
            for line in lines:
                fields = line.split()
                if not fields or fields[0] not in ('some', 'full') or fields[0] in result:
                    raise ValueError
                values = dict(item.split('=', 1) for item in fields[1:])
                pressure = {}
                for key in ('avg10', 'avg60', 'avg300'):
                    number = float(values[key])
                    if not math.isfinite(number) or number < 0:
                        raise ValueError
                    pressure[key] = number
                total = values['total']
                if not total.isdecimal():
                    raise ValueError
                pressure['total'] = int(total)
                result[fields[0]] = pressure
        except (ValueError, KeyError, TypeError):
            return {'status': 'malformed', 'some': None, 'full': None}
        missing = [key for key in ('some', 'full') if key not in result]
        return {'status': 'partial' if missing else 'ok', 'some': result.get('some'),
                'full': result.get('full'), 'missing_keys': missing}

    def _cgroup_diagnostic(self, role: str, pid: int, generation: str,
                           identity: str, cgroup: Path) -> dict:
        same = self._runtime_diagnostic_identities.get(role) == (pid, generation, identity)
        previous = self._runtime_diagnostic_baselines.get(role) if same else None
        pressure = self._pressure_diagnostic(cgroup / 'memory.pressure')
        events, event_status = self._keyed_integer_diagnostic(cgroup / 'memory.events', CGROUP_EVENT_KEYS)
        counters = {}
        for key, filename, allow_max in (
                ('memory_current_bytes', 'memory.current', False),
                ('memory_peak_bytes', 'memory.peak', False),
                ('memory_max_bytes', 'memory.max', True),
                ('memory_swap_current_bytes', 'memory.swap.current', False),
                ('memory_swap_max_bytes', 'memory.swap.max', True),
                ('memory_high_bytes', 'memory.high', True)):
            counters[key], counters[key + '_status'] = self._read_numeric_file(cgroup / filename, allow_max=allow_max)
        numeric_totals = {}
        for kind in ('some', 'full'):
            total = (pressure.get(kind) or {}).get('total')
            if type(total) is int:
                numeric_totals['pressure_' + kind] = total
        numeric_totals.update({'event_' + key: value for key, value in events.items() if type(value) is int})
        deltas = {key: (value - previous[key] if previous and key in previous and value >= previous[key] else None)
                  for key, value in numeric_totals.items()}
        self._runtime_diagnostic_identities[role] = (pid, generation, identity)
        self._runtime_diagnostic_baselines[role] = numeric_totals
        return {'pressure': pressure, 'events': {'status': event_status, 'values': events},
                'limits': counters, 'deltas': deltas,
                'delta_status': 'observed' if previous else 'baseline'}

    @staticmethod
    def _keyed_integer_diagnostic(path: Path, keys: tuple[str, ...]):
        empty = {key: None for key in keys}
        try:
            lines = path.read_text().splitlines()
            values = {}
            for line in lines:
                fields = line.split()
                if len(fields) != 2 or not fields[1].isdecimal() or fields[0] in values:
                    raise ValueError
                values[fields[0]] = int(fields[1])
        except (OSError, UnicodeError):
            return empty, 'unavailable'
        except ValueError:
            return empty, 'malformed'
        selected = {key: values.get(key) for key in keys}
        missing = [key for key, value in selected.items() if value is None]
        return selected, 'partial' if missing else 'ok'

    def _host_pressure_diagnostic(self):
        pressure = self._pressure_diagnostic(Path('/proc/pressure/memory'))
        vmstat, vmstatus = self._keyed_integer_diagnostic(Path('/proc/vmstat'), HOST_VMSTAT_KEYS)
        current = {f'pressure_{kind}': (pressure.get(kind) or {}).get('total')
                   for kind in ('some', 'full')}
        current.update({'vmstat_' + key: value for key, value in vmstat.items()})
        previous = self._host_diagnostic_baseline
        deltas = {key: (value - previous[key] if previous and type(value) is int
                        and type(previous.get(key)) is int and value >= previous[key] else None)
                  for key, value in current.items()}
        self._host_diagnostic_baseline = current
        return {'sequence': self._diagnostic_sequence, 'pressure': pressure,
                'vmstat': {'status': vmstatus, 'values': vmstat}, 'deltas': deltas,
                'delta_status': 'observed' if previous else 'baseline'}

    def _record_diagnostic_sample(self, *, failure=False):
        try:
            if getattr(self, '_diagnostic_recorded_for_guard', False):
                if failure:
                    snapshot = dict(self.last_guard_sample)
                    if getattr(self, 'first_guard_failure_sample', None) is None:
                        self.first_guard_failure_sample = snapshot
                return
            self._diagnostic_sequence = getattr(self, '_diagnostic_sequence', 0) + 1
            self.last_guard_sample['diagnostics'] = {
                'host': self._host_pressure_diagnostic(),
                'runtime': getattr(self, 'runtime_container_diagnostics', {}),
            }
            self._diagnostic_recorded_for_guard = True
            samples = getattr(self, 'diagnostic_samples', None)
            if samples is None:
                samples = self.diagnostic_samples = []
            samples.append(dict(self.last_guard_sample))
            del self.diagnostic_samples[:-8]
            if failure and getattr(self, 'first_guard_failure_sample', None) is None:
                snapshot = dict(self.last_guard_sample)
                self.first_guard_failure_sample = snapshot
        except Exception:
            # Diagnostic collection is strictly best effort.
            try:
                self.last_guard_sample['diagnostics'] = {'status': 'unavailable'}
                self._diagnostic_recorded_for_guard = True
                samples = getattr(self, 'diagnostic_samples', None)
                if samples is None:
                    samples = self.diagnostic_samples = []
                samples.append(dict(self.last_guard_sample))
                del samples[:-8]
                if failure and getattr(self, 'first_guard_failure_sample', None) is None:
                    self.first_guard_failure_sample = dict(self.last_guard_sample)
            except Exception:
                pass

    @staticmethod
    def _memory_stat_diagnostic(path: Path) -> dict:
        """Return selected cgroup charge counters without affecting admission."""
        empty = {key: None for key in MEMORY_STAT_KEYS}
        try:
            text = path.read_text()
        except (OSError, UnicodeError):
            return {'status': 'unavailable', 'values_bytes': empty}
        parsed = {}
        try:
            for line in text.splitlines():
                fields = line.split()
                if len(fields) != 2 or not fields[1].isdecimal() or fields[0] in parsed:
                    raise ValueError
                parsed[fields[0]] = int(fields[1])
        except ValueError:
            return {'status': 'malformed', 'values_bytes': empty}
        values = {key: parsed.get(key) for key in MEMORY_STAT_KEYS}
        missing = [key for key, value in values.items() if value is None]
        return {'status': 'partial' if missing else 'ok',
                'values_bytes': values, 'missing_keys': missing}

    def guard(self, *, stage_size=False, expected_phase=None):
        now = time.monotonic()
        if (self.stage/'.watchdog-abort').exists():
            raise GateError('outer_watchdog_abort')
        if now >= self.deadline:
            if self.live_quality:
                raise GateError('global_live_quality_hard_deadline')
            if self.long_synthetic_soak:
                raise GateError('global_native_stability_hard_deadline')
            raise GateError('global_30_minute_deadline')
        if shutil.disk_usage('/tmp').free < MIN_DISK_ABORT:
            raise GateError('tmp_disk_below_abort_threshold')
        available = mem_available()
        pressure = memory_psi_full_avg10()
        self.memory_psi_full_avg10_sampled_peak = max(
            self.memory_psi_full_avg10_sampled_peak, pressure)
        psi_policy = 'observe_only' if self.worker_recovery_gate else 'fail_above_0_5'
        if self.worker_recovery_gate and pressure > 0.5:
            self.memory_psi_threshold_observations += 1
        if self.observe_resource_thresholds and pressure > 0.5:
            self.resource_threshold_bypass_counts['memory_psi_pressure'] += 1
        self.last_guard_sample = {
            'phase': expected_phase or ('synthetic-browser-gate' if self.browser_phase else 'preflight'),
            'mem_available_bytes': available,
            'memory_psi_full_avg10': pressure,
            'memory_psi_policy': psi_policy,
        }
        self._diagnostic_recorded_for_guard = False
        if self.browser_phase:
            self.last_guard_sample['runtime_working_set_budget_bytes'] = BROWSER_WORKING_SET_BUDGET
            self.last_guard_sample['runtime_working_set_bytes'] = None
            try:
                working_set, component_bytes = self.browser_working_set_bytes(
                    expected_phase=expected_phase)
            except GateError:
                self.last_guard_sample['runtime_working_set_metric_valid'] = False
                raise
            self.last_guard_sample['runtime_working_set_bytes'] = working_set
            self.last_guard_sample['runtime_working_set_container_count'] = len(component_bytes)
            self.last_guard_sample['runtime_working_set_metric_valid'] = True
            self.last_guard_sample['runtime_memory_stat_bytes'] = self.runtime_container_memory_stats
            self.last_guard_sample['runtime_diagnostics'] = getattr(self, 'runtime_container_diagnostics', {})
            self.runtime_working_set_peak_bytes = max(self.runtime_working_set_peak_bytes, working_set)
            for role, value in component_bytes.items():
                self.runtime_container_sampled_peaks[role] = max(
                    self.runtime_container_sampled_peaks.get(role, 0), value)
            if working_set > BROWSER_WORKING_SET_BUDGET:
                if self.observe_resource_thresholds:
                    self.resource_threshold_bypass_counts['runtime_working_set_above_budget'] += 1
                else:
                    self._record_diagnostic_sample(failure=True)
                    raise GateError('runtime_working_set_above_budget')
        self._record_diagnostic_sample()
        if available < RESERVE_MEM_MIN:
            raise GateError('host_memory_reserve_breached')
        if pressure > 0.5:
            if not self.observe_resource_thresholds and not self.worker_recovery_gate:
                self._record_diagnostic_sample(failure=True)
                raise GateError('memory_psi_pressure')
        if stage_size and tree_bytes(self.stage) > MAX_STAGE_BYTES:
            raise GateError('stage_size_limit')
        if (self.live_quality and self.quality_captures is not None
                and tree_bytes(self.quality_captures) > QUALITY_PRIVATE_OUTPUT_LIMIT):
            raise GateError('quality_private_output_limit')
        current = self.fingerprints()
        if current != self.baseline:
            raise GateError('production_fingerprint_changed')
        self.last_production = current
        if not self.maintenance_web_stopped:
            health_status, health_seconds = self.health_probe()
            if health_status not in ('200', '000'):
                raise GateError('production_health_non_200')
            if health_status != '200' or health_seconds > 1.0:
                self.unhealthy += 1
                if self.unhealthy >= 2:
                    raise GateError('production_health_failed_twice')
            else:
                self.unhealthy = 0
            memory = self.web_memory_bytes()
            if memory > 222 * 1024**2:
                if self.high_web_memory_since is None:
                    self.high_web_memory_since = now
                elif now - self.high_web_memory_since >= 10:
                    raise GateError('production_web_memory_above_limit')
            else:
                self.high_web_memory_since = None
        names = self.owned_containers()
        if names:
            recovery_worker = self._marked_recovery_worker(expected_phase)
            output = command(['docker', 'inspect', '--format',
                '{{.Name}}|{{.State.Running}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.Pid}}|{{.State.ExitCode}}|{{index .Config.Labels "linux-vps-gate"}}', *names])
            rows = output.stdout.splitlines()
            if len(rows) != len(names):
                raise GateError('owned_container_inspect_race')
            recovery_exit_seen = False
            for row in rows:
                name, running, restarts, oom, pid, exit_code, label = row.split('|')
                name = name.lstrip('/')
                if label != self.label_value or restarts != '0' or oom != 'false':
                    raise GateError('owned_container_stopped_restarted_or_oom')
                if running == 'true' and int(pid) > 1:
                    continue
                expected_name = self.label_value+'-'+expected_phase if expected_phase else None
                if (expected_phase in ('linux-race', 'linux-build') and name == expected_name
                        and running == 'false' and exit_code == '0' and int(pid) == 0):
                    continue
                match = BROWSER_CONTAINER_NAME.fullmatch(name)
                if (name == recovery_worker and match and match.group('role') == 'worker'
                        and running == 'false' and exit_code == '1' and int(pid) == 0):
                    if recovery_exit_seen:
                        raise GateError('owned_container_stopped_restarted_or_oom')
                    recovery_exit_seen = True
                    continue
                raise GateError('owned_container_stopped_restarted_or_oom')
        try:
            supervisor_rss_kib = proc_tree_rss_kib(os.getpid())
        except Exception:
            self.last_guard_sample['host_supervisor_rss_kib'] = None
            self.last_guard_sample['host_supervisor_rss_measurement_status'] = 'unavailable'
            raise GateError('host_supervisor_rss_measurement_unavailable') from None
        if type(supervisor_rss_kib) is not int or supervisor_rss_kib < 0:
            self.last_guard_sample['host_supervisor_rss_kib'] = None
            self.last_guard_sample['host_supervisor_rss_measurement_status'] = 'unavailable'
            raise GateError('host_supervisor_rss_measurement_unavailable')
        if supervisor_rss_kib > HOST_SUPERVISOR_RSS_LIMIT_KIB:
            self.last_guard_sample['host_supervisor_rss_kib'] = supervisor_rss_kib
            try:
                self.last_guard_sample['host_supervisor_rss_processes'] = proc_tree_rss_rows(os.getpid())
                self.last_guard_sample['host_supervisor_rss_processes_status'] = 'observed'
            except Exception:
                # The measured RSS still exceeds the unchanged limit; missing
                # attribution is diagnostic only and never converts it to pass.
                self.last_guard_sample['host_supervisor_rss_processes'] = []
                self.last_guard_sample['host_supervisor_rss_processes_status'] = 'unavailable'
            if self.first_guard_failure_sample is None:
                self.first_guard_failure_sample = dict(self.last_guard_sample)
            raise GateError('host_supervisor_rss_above_observed_limit')

    def run_command(self, args, *, phase: str, timeout: int, log_path: Path, stage_size=False):
        if shutil.disk_usage('/tmp').free < MIN_DISK_START:
            raise GateError('tmp_disk_below_start_threshold')
        self.guard(stage_size=stage_size, expected_phase=phase)
        log_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with log_path.open('wb') as log:
            process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            pgid_file = self.stage/'.active-command-pgid'
            pgid_file.write_text(str(process.pid)+'\n')
            phase_started = time.monotonic()
            try:
                while process.poll() is None:
                    self.guard(stage_size=stage_size, expected_phase=phase)
                    if time.monotonic() - phase_started > timeout:
                        raise GateError(phase + '_timeout')
                    time.sleep(1)
                if process.returncode:
                    raise GateError(phase + '_failed')
            except BaseException:
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        try: os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError: pass
                        process.wait()
                raise
            finally:
                pgid_file.unlink(missing_ok=True)
        self.phases.append({'phase': phase, 'seconds': round(time.monotonic() - phase_started, 1),
                            'exit_code': 0})

    def cleanup_label(self):
        cleanup_started = time.monotonic()
        deadline = (min(cleanup_started + 100, self.deadline)
                    if self.live_quality or self.long_synthetic_soak
                   else cleanup_started + 100)
        for name in self.owned_containers():
            if time.monotonic() >= deadline:
                raise GateError('cleanup_deadline')
            inspected = command(['docker', 'inspect', '--format', '{{index .Config.Labels "linux-vps-gate"}}', name], check=False)
            if inspected.returncode:
                # A container can exit and be removed between the owned-label
                # list and inspect. Skip it only after a successful exact-name
                # list confirms absence; a present object or Docker failure is
                # still a hard cleanup error.
                listed = command(['docker', 'ps', '-a', '--format', '{{.Names}}'], check=False)
                if listed.returncode:
                    raise GateError('cleanup_container_list_failed')
                if name in listed.stdout.splitlines():
                    raise GateError('cleanup_container_inspect_failed')
                continue
            value = inspected.stdout.strip()
            if value != self.label_value:
                raise GateError('cleanup_container_label_mismatch')
            removed = subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=6)
            if removed.returncode:
                listed = command(['docker', 'ps', '-a', '--format', '{{.Names}}'], check=False)
                if listed.returncode:
                    raise GateError('cleanup_container_list_failed')
                if name in listed.stdout.splitlines():
                    raise GateError('cleanup_container_remove_failed')
        network_list = command(['docker', 'network', 'ls', '--filter', self.label_filter,
                                '--format', '{{.Name}}'], check=False)
        if network_list.returncode:
            raise GateError('cleanup_network_list_failed')
        networks = network_list.stdout.splitlines()
        for name in networks:
            if time.monotonic() >= deadline:
                raise GateError('cleanup_deadline')
            inspected = command(['docker', 'network', 'inspect', '--format', '{{index .Labels "linux-vps-gate"}}', name], check=False)
            if inspected.returncode:
                listed = command(['docker', 'network', 'ls', '--format', '{{.Name}}'], check=False)
                if listed.returncode:
                    raise GateError('cleanup_network_list_failed')
                if name in listed.stdout.splitlines():
                    raise GateError('cleanup_network_inspect_failed')
                continue
            value = inspected.stdout.strip()
            if value != self.label_value:
                raise GateError('cleanup_network_label_mismatch')
            removed = subprocess.run(['docker', 'network', 'rm', name], capture_output=True, timeout=6)
            if removed.returncode:
                listed = command(['docker', 'network', 'ls', '--format', '{{.Name}}'], check=False)
                if listed.returncode:
                    raise GateError('cleanup_network_list_failed')
                if name in listed.stdout.splitlines():
                    raise GateError('cleanup_network_remove_failed')
        network_list = command(['docker', 'network', 'ls', '--filter', self.label_filter,
                                '--format', '{{.Name}}'], check=False)
        if network_list.returncode:
            raise GateError('cleanup_network_list_failed')
        if self.owned_containers() or network_list.stdout.splitlines():
            raise GateError('owned_resources_remain')


def ensure_stage(stage: Path, *, runtime_only=False):
    if stage.is_symlink() or not stage.is_dir():
        raise GateError('stage_missing_or_symlink')
    if stage.stat().st_mode & 0o777 != 0o700:
        raise GateError('stage_must_be_mode_0700')
    required = [stage/'benchmark/go/go.mod', stage/'benchmark/scripts/native_shadow_stand.py',
                stage/'benchmark/scripts/shadow_cdp_relay.py', stage/'bin-soak', stage/'tls']
    if runtime_only:
        required += [stage/BINARY_MANIFEST_NAME]
    else:
        required += [stage/'gopath/pkg/mod', stage/'go-cache', stage/'go-tmp']
    if any(not path.exists() for path in required):
        raise GateError('stage_layout_incomplete')
    for path in (stage/'bin-soak',) if runtime_only else (stage/'go-cache', stage/'go-tmp', stage/'bin-soak'):
        path.mkdir(mode=0o700, exist_ok=True)


def verify_pinned_images(*, runtime_only=False):
    images = [] if runtime_only else [(GO_IMAGE, GO_IMAGE_ID)]
    images.extend(((APP_IMAGE, APP_IMAGE_ID), (CLOAK_IMAGE, CLOAK_IMAGE_ID)))
    for image, expected in images:
        details = command(['docker', 'image', 'inspect', '--format', '{{.Id}}|{{.Os}}|{{.Architecture}}', image]).stdout.strip()
        if details != expected + '|linux|amd64':
            if image == GO_IMAGE:
                raise GateError('pinned_go_image_missing_or_changed')
            raise GateError('pinned_browser_image_missing_or_changed')


def run_go_checks(supervisor: Supervisor, stage: Path, *, runtime_only=False):
    if runtime_only:
        return
    run_go_phase(supervisor, stage, 'linux-race',
                 ['bash', '-ec', 'test -x "$(command -v gcc)" && go mod verify && go test -race -count=1 ./...'], 15*60)
    run_go_phase(supervisor, stage, 'linux-build', build_commands(), 3*60)


def run_go_phase(supervisor: Supervisor, stage: Path, phase: str, command_args: list[str], limit: int):
    name = supervisor.label_value + '-' + phase
    docker_args = ['docker', 'run', '--rm', '--name', name, '--label', supervisor.label_filter[6:],
        '--network', 'none', '--no-healthcheck', '--restart=no', '--read-only', '--cap-drop=ALL',
        '--security-opt=no-new-privileges:true', '--memory', GO_CONTAINER_MEMORY,
        '--memory-swap', GO_CONTAINER_MEMORY,
        '--cpus', '1', '--pids-limit', '128', '--tmpfs', '/tmp:rw,nosuid,nodev,size=32m',
        '--log-opt', 'max-size=10m', '--log-opt', 'max-file=2',
        '-v', str(stage/'benchmark/go')+':/src:ro', '-v', str(stage/'gopath')+':/gopath:rw',
        '-v', str(stage/'go-cache')+':/go-cache:rw', '-v', str(stage/'go-tmp')+':/go-tmp:rw',
        '-v', str(stage/'bin-soak')+':/out:rw', '-w', '/src',
        '-e', 'GOPATH=/gopath', '-e', 'GOMODCACHE=/gopath/pkg/mod', '-e', 'GOCACHE=/go-cache',
        '-e', 'GOTMPDIR=/go-tmp', '-e', 'TMPDIR=/go-tmp',
        '-e', 'GOPROXY=off', '-e', 'GOTOOLCHAIN=local',
        '-e', 'GOOS=linux', '-e', 'GOARCH=amd64', '-e', 'GOMAXPROCS=1',
        '-e', 'GOFLAGS=-p=1', '-e', 'CGO_ENABLED=1', GO_IMAGE, *command_args]
    supervisor.run_command(docker_args, phase=phase, timeout=limit,
                           log_path=stage/(phase+'.log'), stage_size=True)


def build_commands():
    shell = ('set -eu; test -x "$(command -v gcc)"; '
             'CGO_ENABLED=0 go build -trimpath -o /out/extract-service ./cmd/extract-service; '
             'CGO_ENABLED=0 go build -trimpath -o /out/extract-egress ./cmd/extract-egress; '
             'command -v readelf >/dev/null; '
             '! readelf -l /out/extract-service | grep -q INTERP; '
             '! readelf -l /out/extract-egress | grep -q INTERP')
    return ['bash', '-ec', shell]


def watchdog_cleanup(label_value: str, seconds: int = 100):
    label = PARENT_LABEL + label_value
    deadline = time.monotonic() + seconds
    try:
        names = subprocess.run(['docker', 'ps', '-a', '--filter', 'label='+label,
                                '--format', '{{.Names}}'], capture_output=True, text=True,
                               timeout=5).stdout.splitlines()
        for name in names:
            if time.monotonic() >= deadline:
                return
            check = subprocess.run(['docker', 'inspect', '--format',
                                    '{{index .Config.Labels "linux-vps-gate"}}', name],
                                   capture_output=True, text=True, timeout=5)
            if check.returncode == 0 and check.stdout.strip() == label_value:
                subprocess.run(['docker', 'rm', '-f', name],
                               capture_output=True, timeout=6)
        names = subprocess.run(['docker', 'network', 'ls', '--filter', 'label='+label,
                                '--format', '{{.Name}}'], capture_output=True, text=True,
                               timeout=5).stdout.splitlines()
        for name in names:
            if time.monotonic() >= deadline:
                return
            check = subprocess.run(['docker', 'network', 'inspect', '--format',
                                    '{{index .Labels "linux-vps-gate"}}', name],
                                   capture_output=True, text=True, timeout=5)
            if check.returncode == 0 and check.stdout.strip() == label_value:
                subprocess.run(['docker', 'network', 'rm', name], capture_output=True, timeout=6)
    except (OSError, subprocess.TimeoutExpired):
        return


def watchdog_deadlines(started: float, maintenance_profile=False,
                       live_quality=False, long_synthetic_soak=False) -> tuple[float, float]:
    if long_synthetic_soak:
        return started + NATIVE_STABILITY_ACTIVE_SECONDS, started + NATIVE_STABILITY_HARD_SECONDS
    if live_quality:
        return started + LIVE_QUALITY_ACTIVE_SECONDS, started + LIVE_QUALITY_HARD_SECONDS
    if maintenance_profile:
        return started + 6*60, started + 8*60
    return started + 28*60, started + 30*60


def active_command_pgid(stage: Path) -> int | None:
    """Accept only a live process in the recorded start_new_session group."""
    try:
        pid = int((stage/'.active-command-pgid').read_text().strip())
        if pid <= 1:
            return None
        for entry in Path('/proc').iterdir():
            if not entry.name.isdecimal():
                continue
            try:
                stat = (entry/'stat').read_text()
                fields = stat[stat.rfind(')')+2:].split()
                # After comm: state, ppid, pgrp, session. The runner creates
                # the command in a new session whose ID is its recorded PID.
                if len(fields) >= 4 and int(fields[2]) == pid and int(fields[3]) == pid:
                    return pid
            except (OSError, ValueError, IndexError):
                continue
        return None
    except (OSError, ValueError):
        return None


def stop_active_command_group(stage: Path, grace_seconds: float = 8):
    pgid = active_command_pgid(stage)
    if pgid is None:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError:
        return
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        except OSError:
            return
        time.sleep(0.1)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        pass


def outer_watchdog(stage: Path, label_value: str, parent_pid: int, started: float,
                   maintenance_profile=False, live_quality=False, long_synthetic_soak=False):
    abort_at, deadline = watchdog_deadlines(started, maintenance_profile, live_quality,
                                            long_synthetic_soak)
    abort = stage/'.watchdog-abort'
    label = PARENT_LABEL + label_value
    while time.monotonic() < abort_at:
        if (stage/'.watchdog-stop').exists():
            return
        try:
            os.kill(parent_pid, 0)
        except ProcessLookupError:
            stop_active_command_group(stage)
            watchdog_cleanup(label_value, max(1, int(deadline-time.monotonic())))
            return
        time.sleep(1)
    try:
        fd = os.open(abort, os.O_CREAT|os.O_WRONLY|os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as stream:
            stream.write('absolute_active_deadline\n')
    except OSError:
        pass
    stop_active_command_group(stage)
    # Diagnostic modes give the parent time to run its final guard before cleanup.
    try:
        os.kill(parent_pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    # Let the parent a few seconds to complete its final guard and exact-label
    # cleanup. If it hangs, reclaim within the reserved hard-deadline window.
    for _ in range(3):
        if (stage/'.watchdog-stop').exists():
            return
        try:
            os.kill(parent_pid, 0)
        except ProcessLookupError:
            stop_active_command_group(stage)
            watchdog_cleanup(label_value, max(1, int(deadline-time.monotonic())))
            return
        time.sleep(1)
    watchdog_cleanup(label_value, max(1, min(100, int(deadline-time.monotonic()))))
    while time.monotonic() < deadline:
        if (stage/'.watchdog-stop').exists():
            return
        try:
            os.kill(parent_pid, 0)
        except ProcessLookupError:
            stop_active_command_group(stage)
            watchdog_cleanup(label_value, max(1, int(deadline-time.monotonic())))
            return
        time.sleep(1)
    try:
        os.kill(parent_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    stop_active_command_group(stage)
    watchdog_cleanup(label_value, max(1, int(deadline-time.monotonic())))


def production_user_path():
    def fetch(url):
        request = urllib.request.Request(url, headers={'User-Agent': 'native-shadow-gate/1'})
        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                return response.status, response.read(2 * 1024 * 1024), response.geturl()
        except (urllib.error.URLError, TimeoutError, OSError):
            return 0, b'', ''
    import re
    from urllib.parse import urlsplit
    status, page, page_url = fetch('https://news.dzarlax.dev/')
    page_ok = (status == 200 and urlsplit(page_url).hostname == 'news.dzarlax.dev'
               and b'Evening News' in page
               and re.search(rb'id=["\']news-list["\']', page) is not None
               and re.search(rb'id=["\']feed-sentinel["\']', page) is not None)
    api_status, body, api_url = fetch('https://news.dzarlax.dev/api/public/feed?limit=1&offset=0')
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        value = {}
    rows = value.get('articles', []) if isinstance(value, dict) else []
    urls = [row.get('url', '') for row in rows if isinstance(row, dict)]
    api_ok = (api_status == 200 and urlsplit(api_url).hostname == 'news.dzarlax.dev'
              and isinstance(rows, list) and len(rows) == 1 and value.get('total') == 1
              and len(urls) == 1 and isinstance(urls[0], str) and 'db-error' not in urls[0].lower())
    return {'home_page_ok': page_ok, 'public_feed_ok': api_ok}


def stand_run_id(log_path: Path, expected_state: str) -> str:
    """Read only the small final status record emitted by a successful stand."""
    try:
        lines = log_path.read_text().splitlines()
        records = [json.loads(line) for line in lines if line.startswith('{')]
        record = records[-1]
        run_id = record.get('run_id')
        if (record.get('state') != expected_state
                or not isinstance(run_id, str)
                or not re.fullmatch(r'news-native-shadow-[0-9a-f]{10}', run_id)):
            raise ValueError
    except (OSError, ValueError, IndexError, TypeError, json.JSONDecodeError):
        raise GateError('stand_result_invalid') from None
    return run_id


def run_supervised_stand(supervisor: Supervisor, args: list[str], *, phase: str,
                         timeout: int, stage: Path, active_deadline: float):
    """Run a stand command, always trying the final guard before exact-label cleanup."""
    first_error = None
    final_guard_passed = False
    try:
        remaining = int(active_deadline - time.monotonic())
        if remaining <= 0:
            raise GateError('global_live_quality_active_deadline')
        supervisor.run_command(args, phase=phase, timeout=min(timeout, remaining),
                               log_path=stage/(phase+'.log'), stage_size=True)
    except BaseException as exc:
        first_error = exc
    try:
        supervisor.guard(stage_size=True, expected_phase=phase)
        final_guard_passed = True
    except BaseException as exc:
        if first_error is None:
            first_error = exc
    try:
        supervisor.cleanup_label()
    except BaseException as exc:
        if first_error is None:
            first_error = exc
    supervisor.browser_phase = False
    if first_error is not None:
        raise first_error
    return {'final_guard_passed': final_guard_passed,
            'owned_containers_remaining': len(supervisor.owned_containers()),
            'owned_networks_remaining': len(command([
                'docker', 'network', 'ls', '--filter', supervisor.label_filter,
                '--format', '{{.Name}}']).stdout.splitlines())}


def validate_stability_observation(stage: Path) -> dict:
    """Accept only the compact, fixed-workload result emitted by the stand."""
    run_id = stand_run_id(stage/'native-synthetic-stability.log', 'synthetic_stability_completed')
    observation_path = stage/run_id/'synthetic-soak-observation.json'
    manifest_path = stage/run_id/'manifest.json'
    try:
        if (manifest_path.is_symlink() or not manifest_path.is_file()
                or manifest_path.stat().st_size > 64 * 1024
                or observation_path.is_symlink() or not observation_path.is_file()
                or observation_path.stat().st_size > 64 * 1024):
            raise ValueError
        manifest = json.loads(manifest_path.read_text())
        value = json.loads(observation_path.read_text())
        if (manifest.get('state') != 'synthetic_stability_gate_passed'
                or manifest.get('bootstrap') is not True or manifest.get('prefix') != run_id
                or value.get('state') != 'synthetic_stability_completed'
                or value.get('duration_seconds') != NATIVE_STABILITY_DURATION_SECONDS
                or value.get('case_ids') != list(NATIVE_STABILITY_CASE_IDS)
                or type(value.get('completed_batches')) is not int
                or value['completed_batches'] < 1
                or value.get('container_generation_unchanged') is not True
                or value.get('browser_generation_unchanged') is not True
                or value.get('oom_counters_unchanged') is not True
                or not isinstance(value.get('batch_receipts'), list)
                or len(value['batch_receipts']) != value.get('completed_batches')
                or any(not isinstance(receipt, dict)
                       or receipt.get('batch_index') != index
                       or receipt.get('status') != 'passed'
                       or receipt.get('mismatch_count') != 0
                       or receipt.get('case_count') != len(NATIVE_STABILITY_CASE_IDS)
                       for index, receipt in enumerate(value['batch_receipts'], start=1))):
            raise ValueError
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        raise GateError('native_stability_result_invalid') from None
    # Keep summary output aggregate-only; the stand observation is private evidence.
    return {'run_id': run_id, 'duration_seconds': NATIVE_STABILITY_DURATION_SECONDS,
            'case_count': len(NATIVE_STABILITY_CASE_IDS),
            'completed_batches': value['completed_batches'],
            'container_generation_unchanged': True,
            'browser_generation_unchanged': True, 'oom_counters_unchanged': True}


def mint_quality_gate_receipt(stage: Path, run_id: str, parent_label: str,
                              source_hash: str, runtime_hash: str,
                              cleanup_state: dict) -> Path:
    """Create the only receipt accepted by live stands after gate and cleanup pass."""
    source_dir = stage / run_id
    receipt = stage / 'shadow-private' / 'quality-gate-receipt'
    try:
        if (source_dir.is_symlink() or not source_dir.is_dir() or receipt.exists()
                or receipt.is_symlink()):
            raise ValueError
        manifest_path = source_dir / 'manifest.json'
        trap_path = source_dir / 'browser-trap.json'
        if any(path.is_symlink() or not path.is_file() or path.stat().st_size > 64*1024
               for path in (manifest_path, trap_path)):
            raise ValueError
        manifest = json.loads(manifest_path.read_text())
        trap = json.loads(trap_path.read_text())
        if (manifest.get('state') != 'controlled_browser_gate_passed'
                or manifest.get('bootstrap') is not True or manifest.get('prefix') != run_id
                or trap.get('accepted') != 0):
            raise ValueError
        if (not cleanup_state.get('final_guard_passed')
                or cleanup_state.get('owned_containers_remaining') != 0
                or cleanup_state.get('owned_networks_remaining') != 0):
            raise ValueError
        if (not re.fullmatch(r'[0-9a-f]{64}', source_hash)
                or not re.fullmatch(r'[0-9a-f]{64}', runtime_hash)):
            raise ValueError
        receipt.mkdir(mode=0o700)
        os.chmod(receipt, 0o700)
        manifest = dict(manifest)
        manifest['state'] = 'cleaned'
        manifest['previous_phase'] = 'controlled_browser_gate_passed'
        cleanup = {'state': 'confirmed', 'parent_label': parent_label,
                   'owned_containers_remaining': 0, 'owned_networks_remaining': 0,
                   'final_guard_passed': True, 'source_manifest_sha256': source_hash,
                   'runtime_bundle_manifest_sha256': runtime_hash}
        for name, value in (('manifest.json', manifest), ('browser-trap.json', trap),
                            ('outer-cleanup.json', cleanup)):
            fd = os.open(receipt/name, os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
            with os.fdopen(fd, 'w') as stream:
                stream.write(json.dumps(value, indent=2)+'\n')
        return receipt
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        if receipt.is_dir() and not receipt.is_symlink():
            shutil.rmtree(receipt)
        raise GateError('quality_gate_receipt_invalid') from None


def run_live_quality(supervisor: Supervisor, stage: Path, *, corpus: Path,
                     captures: Path, receipt: Path, parent_label: str,
                     active_deadline: float, hard_deadline: float,
                     phase_records: list[dict]):
    """Execute the pinned Go smoke followed by the remaining Go quality cases."""
    phase_results = []
    for variant, phase_name, case_ids in QUALITY_PHASES:
        if time.monotonic() >= active_deadline:
            raise GateError('global_live_quality_active_deadline')
        phase_started = time.monotonic()
        phase_hard = min(phase_started + LIVE_QUALITY_STAND_HARD_SECONDS, hard_deadline)
        phase_deadline = min(phase_started + LIVE_QUALITY_STAND_ACTIVE_SECONDS, active_deadline)
        deadline_unix = int(time.time() + max(1, phase_hard-time.monotonic()))
        record = {'variant': variant, 'phase': phase_name, 'case_ids': list(case_ids),
                  'state': 'running'}
        phase_records.append(record)
        args = ['python3', str(stage/'benchmark/scripts/native_shadow_stand.py'),
                '--stage', str(stage), '--live', '--variant', variant,
                '--corpus', str(corpus), '--captures', str(captures),
                '--approved-gate', str(receipt), '--quality-profile',
                '--quality-phase', phase_name, '--bounded-profile', '--supervisor-cleanup',
                '--supervisor-label', parent_label, '--deadline', str(deadline_unix)]
        supervisor.browser_phase = True
        try:
            cleanup = run_supervised_stand(supervisor, args, phase='live-'+phase_name,
                                           timeout=LIVE_QUALITY_STAND_ACTIVE_SECONDS,
                                           stage=stage, active_deadline=phase_deadline)
            capture_state = validate_quality_capture_state(captures, phase_name)
        except BaseException as exc:
            record.update({'state': 'failed', 'error': safe_error_name(exc),
                           'elapsed_seconds': round(time.monotonic()-phase_started, 1)})
            raise
        record.update({'state': 'completed',
                       'elapsed_seconds': round(time.monotonic()-phase_started, 1),
                       'final_guard_passed': cleanup['final_guard_passed'],
                       'capture_state': capture_state})
        phase_results.append(record)
        if time.monotonic() > hard_deadline:
            raise GateError('global_live_quality_hard_deadline')
    return phase_results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', required=True, type=Path)
    parser.add_argument('--runtime-only', action='store_true',
                        help='run the synthetic browser gate with manifest-verified prebuilt Go binaries')
    parser.add_argument('--news-web-stopped', action='store_true',
                        help='maintenance profile: require the identified news_web container to be stopped')
    parser.add_argument('--short-window', action='store_true',
                        help='use the 6-minute active / 8-minute hard runtime-only window with production running')
    parser.add_argument('--observe-resource-thresholds', action='store_true',
                        help=('diagnostic only: observe PSI and aggregate working-set threshold exceedances '
                              'without aborting; requires --runtime-only and a bounded diagnostic profile'))
    parser.add_argument('--live-quality', action='store_true',
                        help='run the pinned five-case Go live quality batch within fixed watchdog limits')
    parser.add_argument('--long-synthetic-soak', action='store_true',
                        help='run the fixed 600-second native synthetic stability workload diagnostically')
    parser.add_argument('--worker-recovery-gate', action='store_true',
                        help='run the bounded synthetic fatal-worker recovery acceptance gate')
    parser.add_argument('--maintenance-container-id',
                        help='exact 64-hex news_web container ID captured before the authorized stop')
    parser.add_argument('--watchdog', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--maintenance-profile', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--label-value', help=argparse.SUPPRESS)
    parser.add_argument('--parent-pid', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--started-monotonic', type=float, help=argparse.SUPPRESS)
    args = parser.parse_args()
    stage = args.stage.resolve()
    validate_worker_recovery_options(enabled=args.worker_recovery_gate,
        runtime_only=args.runtime_only, short_window=args.short_window,
        news_web_stopped=args.news_web_stopped, live_quality=args.live_quality,
        long_synthetic_soak=args.long_synthetic_soak,
        observe_resource_thresholds=args.observe_resource_thresholds)
    if args.worker_recovery_gate and ((stage/'.worker-recovery-active').exists()
            or (stage/'.worker-recovery-active').is_symlink()):
        raise GateError('worker_recovery_marker_stale')
    validate_maintenance_options(args.runtime_only, args.news_web_stopped,
                                 args.maintenance_container_id, short_window=args.short_window,
                                 observe_resource_thresholds=args.observe_resource_thresholds,
                                 live_quality=args.live_quality,
                                 long_synthetic_soak=args.long_synthetic_soak)
    if args.watchdog:
        if (not args.label_value or not args.parent_pid or args.started_monotonic is None
                or not re.fullmatch(r'[0-9a-f]{16}', args.label_value)):
            raise GateError('watchdog_arguments_invalid')
        outer_watchdog(stage, args.label_value, args.parent_pid, args.started_monotonic,
                       maintenance_profile=args.maintenance_profile,
                       live_quality=args.live_quality,
                       long_synthetic_soak=args.long_synthetic_soak)
        return
    ensure_stage(stage, runtime_only=args.runtime_only)
    if shutil.disk_usage('/tmp').free < MIN_DISK_START:
        raise GateError('tmp_disk_below_start_threshold')
    if mem_available() < GO_MEM_MIN:
        raise GateError('host_memory_below_go_start_minimum')
    if shutil.disk_usage('/tmp').free < MIN_DISK_START:
        raise GateError('tmp_disk_below_start_threshold')
    verify_pinned_images(runtime_only=args.runtime_only)
    source_manifest_sha256=verify_source_manifest(stage)
    runtime_bundle_sha256 = verify_runtime_bundle(stage) if args.runtime_only else None
    quality_corpus_sha256 = None
    quality_private = quality_captures = None
    if args.live_quality:
        if stage == QUALITY_CORPUS_PATH.parents[2].resolve():
            raise GateError('quality_stage_reuse_forbidden')
        quality_corpus_sha256 = verify_quality_corpus(QUALITY_CORPUS_PATH)
    baseline = {'infra-chrome': production_fingerprint('infra-chrome')}
    if args.news_web_stopped:
        stopped = stopped_web_fingerprint(args.maintenance_container_id)
        validate_maintenance_baseline(stage, stopped, args.maintenance_container_id)
        baseline['news_web'] = stopped
    else:
        baseline['news_web'] = production_fingerprint('news_web')
        if command(['curl', '--noproxy', '*', '--silent', '--show-error', '--output', '/dev/null',
                    '--write-out', '%{http_code}', '--max-time', '1', 'http://127.0.0.1:8005/health']).stdout.strip() != '200':
            raise GateError('production_health_preflight_failed')
        preflight_path = production_user_path()
        if not all(preflight_path.values()):
            raise GateError('production_user_path_preflight_failed')

    if args.live_quality:
        quality_private, quality_captures = prepare_quality_output(stage)

    start = time.monotonic()
    label_value = secrets.token_hex(8)
    supervisor = Supervisor(stage, label_value, start, baseline,
                            maintenance_web_stopped=args.news_web_stopped,
                            maintenance_container_id=args.maintenance_container_id,
                            short_window=args.short_window,
                            observe_resource_thresholds=args.observe_resource_thresholds,
                            live_quality=args.live_quality,
                            long_synthetic_soak=args.long_synthetic_soak,
                            worker_recovery_gate=args.worker_recovery_gate)
    supervisor.quality_captures = quality_captures
    def interrupted(*_):
        raise GateError('runner_interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    (stage/'.watchdog-stop').unlink(missing_ok=True)
    (stage/'.watchdog-abort').unlink(missing_ok=True)
    watchdog = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--stage', str(stage),
                                 '--watchdog', '--label-value', label_value,
                                 '--parent-pid', str(os.getpid()), '--started-monotonic', str(start),
                                 *(('--runtime-only', '--observe-resource-thresholds', '--live-quality')
                                   if args.live_quality else ()),
                                 *(('--runtime-only', '--observe-resource-thresholds', '--long-synthetic-soak')
                                   if args.long_synthetic_soak else ()),
                                 *(('--maintenance-profile',)
                                   if not args.live_quality and not args.long_synthetic_soak
                                   and (args.news_web_stopped or args.short_window) else ())],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True)
    summary = {'schema_version': 1, 'state': 'running', 'phases': [],
               'source_manifest_sha256':source_manifest_sha256,
               'measurement_scope': ('native_synthetic_stability_600s' if args.long_synthetic_soak
                                     else 'live_quality_runtime_no_compilation' if args.live_quality
                                     else 'runtime_fixture_gate_no_compilation' if args.runtime_only
                                     else 'full_go_and_runtime_gate')}
    if args.news_web_stopped:
        summary.update({'maintenance_mode': True, 'maintenance_state_unchanged': False,
                        'maintenance_web_stopped_verified': False,
                        'infra_chrome_fingerprint_unchanged': False,
                        'service_user_path_checked_after_run': False})
    else:
        summary.update({'production_fingerprints_unchanged': False,
                        'home_page_ok': False, 'public_feed_ok': False})
    if args.short_window:
        summary['short_window'] = True
    if args.observe_resource_thresholds:
        summary['diagnostic_resource_observation'] = True
        summary['observe_resource_thresholds'] = True
    if args.live_quality:
        summary.update({'live_quality': True, 'quality_corpus_sha256': quality_corpus_sha256,
                        'quality_phases': [], 'quality_attempt_limit': 10,
                        'quality_output_limit_bytes': QUALITY_PRIVATE_OUTPUT_LIMIT,
                        'quality_claim_scope': 'diagnostic_only'})
    if args.long_synthetic_soak:
        summary.update({'long_synthetic_soak': True,
                        'stability_duration_seconds': NATIVE_STABILITY_DURATION_SECONDS,
                        'stability_claim_scope': 'diagnostic_only'})
    if args.worker_recovery_gate:
        summary.update({'worker_recovery_gate': True,
                        'memory_psi_policy': 'observe_only',
                        'memory_psi_threshold_observations': 0})
    if runtime_bundle_sha256 is not None:
        summary['runtime_bundle_manifest_sha256'] = runtime_bundle_sha256
    try:
        run_go_checks(supervisor, stage, runtime_only=args.runtime_only)
        supervisor.guard(stage_size=True)
        if mem_available() < BROWSER_MEM_MIN:
            raise GateError('host_memory_below_browser_start_minimum')
        active_deadline, hard_deadline = watchdog_deadlines(
            start, args.news_web_stopped or args.short_window, args.live_quality,
            args.long_synthetic_soak)
        deadline = int(time.time() + max(1, active_deadline-time.monotonic()))
        supervisor.browser_phase = True
        if args.live_quality:
            active_deadline = start + LIVE_QUALITY_ACTIVE_SECONDS
            hard_deadline = start + LIVE_QUALITY_HARD_SECONDS
            gate_deadline = int(time.time() + max(1, min(240, active_deadline-time.monotonic())))
            stand = ['python3', str(stage/'benchmark/scripts/native_shadow_stand.py'), '--stage', str(stage),
                     '--bootstrap', '--gate-only', '--bounded-profile', '--supervisor-cleanup',
                     '--deadline', str(gate_deadline),
                     '--supervisor-label', PARENT_LABEL+label_value]
            cleanup_state = run_supervised_stand(supervisor, stand,
                phase='synthetic-browser-gate', timeout=240, stage=stage,
                active_deadline=active_deadline)
            gate_run_id = stand_run_id(stage/'synthetic-browser-gate.log', 'controlled_gate_passed')
            # Recheck provenance after gate cleanup, before minting its receipt.
            if (verify_source_manifest(stage) != source_manifest_sha256
                    or verify_runtime_bundle(stage) != runtime_bundle_sha256):
                raise GateError('quality_gate_provenance_changed')
            receipt = mint_quality_gate_receipt(
                stage, gate_run_id, PARENT_LABEL+label_value,
                source_manifest_sha256, runtime_bundle_sha256, cleanup_state)
            summary['quality_phases'].append({'phase': 'synthetic-pre-gate',
                'state': 'passed', 'run_id': gate_run_id,
                'final_guard_passed': cleanup_state['final_guard_passed']})
            run_live_quality(
                supervisor, stage, corpus=QUALITY_CORPUS_PATH,
                captures=quality_captures, receipt=receipt,
                parent_label=PARENT_LABEL+label_value, active_deadline=active_deadline,
                hard_deadline=hard_deadline, phase_records=summary['quality_phases'])
            complete_capture_state = validate_quality_capture_state(quality_captures, 'c-quality')
            summary['quality_attempted_case_ids'] = complete_capture_state['attempted_case_ids']
            summary['quality_report_variants'] = complete_capture_state['report_variants']
            summary['quality_outcome_counts'] = complete_capture_state['outcome_counts']
        elif args.long_synthetic_soak:
            active_deadline = start + NATIVE_STABILITY_ACTIVE_SECONDS
            hard_deadline = start + NATIVE_STABILITY_HARD_SECONDS
            deadline = int(time.time() + max(1, active_deadline-time.monotonic()))
            stand = ['python3', str(stage/'benchmark/scripts/native_shadow_stand.py'), '--stage', str(stage),
                     '--bootstrap', '--gate-only', '--bounded-profile', '--supervisor-cleanup',
                     '--long-synthetic-soak', '--deadline', str(deadline),
                     '--supervisor-label', PARENT_LABEL+label_value]
            cleanup_state = run_supervised_stand(
                supervisor, stand, phase='native-synthetic-stability',
                timeout=NATIVE_STABILITY_ACTIVE_SECONDS, stage=stage,
                active_deadline=active_deadline)
            if (not cleanup_state.get('final_guard_passed')
                    or cleanup_state.get('owned_containers_remaining') != 0
                    or cleanup_state.get('owned_networks_remaining') != 0):
                raise GateError('native_stability_cleanup_unconfirmed')
            summary['native_stability'] = validate_stability_observation(stage)
        elif args.worker_recovery_gate:
            stand = worker_recovery_stand_args(stage, deadline, PARENT_LABEL+label_value)
            stand_timeout = max(1, int(active_deadline-time.monotonic()))
            supervisor.run_command(stand, phase='native-worker-recovery-gate', timeout=stand_timeout,
                                   log_path=stage/'stand.log', stage_size=True)
            # The stand leaves owned resources live until this final fail-closed check.
            supervisor.guard(stage_size=True)
            supervisor.cleanup_label()
        else:
            stand = ['python3', str(stage/'benchmark/scripts/native_shadow_stand.py'), '--stage', str(stage),
                     '--bootstrap', '--gate-only', '--bounded-profile', '--supervisor-cleanup',
                     '--deadline', str(deadline),
                     '--supervisor-label', PARENT_LABEL+label_value]
            stand_timeout = max(1, int(active_deadline-time.monotonic()))
            supervisor.run_command(stand, phase='synthetic-browser-gate', timeout=stand_timeout,
                                   log_path=stage/'stand.log', stage_size=True)
            supervisor.guard(stage_size=True, expected_phase='synthetic-browser-gate')
            supervisor.cleanup_label()
        final_fingerprints = supervisor.fingerprints()
        if args.news_web_stopped:
            summary['maintenance_web_stopped_verified'] = final_fingerprints['news_web'] == baseline['news_web']
            summary['infra_chrome_fingerprint_unchanged'] = final_fingerprints['infra-chrome'] == baseline['infra-chrome']
            summary['maintenance_state_unchanged'] = (summary['maintenance_web_stopped_verified']
                                                       and summary['infra_chrome_fingerprint_unchanged'])
        else:
            summary['production_fingerprints_unchanged'] = final_fingerprints == baseline
        if verify_source_manifest(stage)!=source_manifest_sha256:
            raise GateError('source_manifest_changed_after_run')
        summary['binaries_sha256']={name:_sha256_file(stage/'bin-soak'/name)
                                   for name in ('extract-service','extract-egress')}
        if args.runtime_only:
            verify_runtime_bundle(stage)
        if args.news_web_stopped:
            if not summary['maintenance_state_unchanged']:
                raise GateError('maintenance_state_changed_after_cleanup')
        else:
            summary.update(production_user_path())
            if not summary['production_fingerprints_unchanged'] or not summary['home_page_ok'] or not summary['public_feed_ok']:
                raise GateError('post_cleanup_production_verification_failed')
        # Live quality is diagnostic evidence and never an acceptance claim.
        summary['state'] = ('diagnostic_completed' if args.live_quality or args.long_synthetic_soak
                            or args.observe_resource_thresholds
                            else 'passed')
        if args.worker_recovery_gate:
            summary.update({'acceptance_scope': 'go_native_worker_recovery_with_memory_caps',
                            'strict_psi_capacity_accepted': False})
    except BaseException as exc:
        summary['state'] = 'failed'
        summary['error'] = str(exc) if isinstance(exc, GateError) else type(exc).__name__
        try:
            supervisor.cleanup_label()
        except Exception as cleanup_error:
            summary['cleanup_error'] = safe_error_name(cleanup_error)
        try:
            current = supervisor.fingerprints()
            if args.news_web_stopped:
                summary['maintenance_web_stopped_verified'] = current['news_web'] == baseline['news_web']
                summary['infra_chrome_fingerprint_unchanged'] = current['infra-chrome'] == baseline['infra-chrome']
                summary['maintenance_state_unchanged'] = (summary['maintenance_web_stopped_verified']
                                                           and summary['infra_chrome_fingerprint_unchanged'])
            else:
                summary['production_fingerprints_unchanged'] = current == baseline
                summary.update(production_user_path())
        except Exception:
            summary['production_postcheck_completed'] = False
        raise
    finally:
        try:
            (stage/'.watchdog-stop').write_text('done\n')
            watchdog.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            try:
                watchdog.terminate()
            except OSError:
                pass
        summary['phases'] = supervisor.phases
        summary['last_guard_sample'] = supervisor.last_guard_sample
        summary['first_guard_failure_sample'] = supervisor.first_guard_failure_sample
        summary['diagnostic_samples'] = getattr(supervisor, 'diagnostic_samples', [])
        summary['runtime_working_set_sampled_peak_bytes'] = supervisor.runtime_working_set_peak_bytes
        summary['memory_psi_full_avg10_sampled_peak'] = supervisor.memory_psi_full_avg10_sampled_peak
        if args.worker_recovery_gate:
            summary['memory_psi_policy'] = 'observe_only'
            summary['memory_psi_threshold_observations'] = getattr(
                supervisor, 'memory_psi_threshold_observations', 0)
        elif not args.observe_resource_thresholds:
            summary['memory_psi_policy'] = 'fail_above_0_5'
        if args.observe_resource_thresholds:
            summary['resource_threshold_bypass_counts'] = supervisor.resource_threshold_bypass_counts
        summary['runtime_container_sampled_peak_bytes'] = supervisor.runtime_container_sampled_peaks
        summary['elapsed_seconds'] = round(time.monotonic() - start, 1)
        report = stage/'summary.json'
        report.write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary, separators=(',', ':')))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        code = str(exc) if isinstance(exc, GateError) and re.fullmatch(r'[a-z0-9_]{1,80}', str(exc)) else type(exc).__name__
        print(json.dumps({'error': code}), file=sys.stderr)
        raise SystemExit(1)
