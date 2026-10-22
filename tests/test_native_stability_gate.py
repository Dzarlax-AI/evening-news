from __future__ import annotations

import json
import hashlib
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts import run_go_cloak_linux_gate as gate


def _completed_observation(stage: Path) -> None:
    run_id = 'news-native-shadow-0123456789'
    run_dir = stage / run_id
    run_dir.mkdir(mode=0o700)
    (run_dir / 'manifest.json').write_text(json.dumps({
        'state': 'synthetic_stability_gate_passed', 'bootstrap': True, 'prefix': run_id,
    }))
    (run_dir / 'synthetic-soak-observation.json').write_text(json.dumps({
        'state': 'synthetic_stability_completed',
        'duration_seconds': gate.NATIVE_STABILITY_DURATION_SECONDS,
        'case_ids': list(gate.NATIVE_STABILITY_CASE_IDS),
        'completed_batches': 1,
        'batch_receipts': [{'batch_index': 1, 'status': 'passed',
                            'mismatch_count': 0, 'case_count': 5}],
        'container_generation_unchanged': True,
        'browser_generation_unchanged': True,
        'oom_counters_unchanged': True,
    }))
    (stage / 'native-synthetic-stability.log').write_text(json.dumps({
        'state': 'synthetic_stability_completed', 'run_id': run_id,
    }) + '\n')


def test_long_synthetic_soak_limits_are_fixed_and_separate_from_other_profiles():
    start = 500.0
    assert gate.watchdog_deadlines(start, long_synthetic_soak=True) == (1340.0, 1460.0)
    assert gate.watchdog_deadlines(start, live_quality=True) == (
        start + gate.LIVE_QUALITY_ACTIVE_SECONDS, start + gate.LIVE_QUALITY_HARD_SECONDS)
    assert gate.watchdog_deadlines(start, maintenance_profile=True) == (start + 360, start + 480)
    assert gate.watchdog_deadlines(start) == (start + 1680, start + 1800)
    supervisor = gate.Supervisor(Path('/tmp/stability-test'), 'a' * 16, start, {},
                                 observe_resource_thresholds=True, long_synthetic_soak=True)
    assert supervisor.deadline == start + gate.NATIVE_STABILITY_HARD_SECONDS
    assert gate.NATIVE_STABILITY_DURATION_SECONDS == 600


def test_watchdog_cleans_exact_label_during_reserved_window_if_parent_hangs(tmp_path):
    now = [100.0]
    events = []

    def fake_kill(pid, sig):
        assert pid == 4321
        if sig == 0:
            return  # Parent remains alive until the hard deadline.
        events.append(('parent-signal', sig, now[0]))

    def fake_sleep(seconds):
        now[0] += seconds

    with patch.object(gate, 'watchdog_deadlines', return_value=(100.0, 110.0)), \
            patch.object(gate.time, 'monotonic', side_effect=lambda: now[0]), \
            patch.object(gate.time, 'sleep', side_effect=fake_sleep), \
            patch.object(gate.os, 'kill', side_effect=fake_kill), \
            patch.object(gate, 'stop_active_command_group'), \
            patch.object(gate, 'watchdog_cleanup',
                         side_effect=lambda label, timeout: events.append(
                             ('cleanup', label, timeout, now[0]))):
        gate.outer_watchdog(tmp_path, '0123456789abcdef', 4321, 100.0,
                            long_synthetic_soak=True)

    cleanups = [event for event in events if event[0] == 'cleanup']
    assert cleanups
    assert cleanups[0][1] == '0123456789abcdef'
    assert cleanups[0][3] < 110.0
    assert any(event[:2] == ('parent-signal', gate.signal.SIGKILL) for event in events)


def test_supervisor_rss_failure_records_value_and_bounded_owned_process_rows(tmp_path):
    assert gate.HOST_SUPERVISOR_RSS_LIMIT_KIB == 128*1024
    supervisor = gate.Supervisor(tmp_path, '0123456789abcdef', gate.time.monotonic(), {},
                                 observe_resource_thresholds=True,
                                 long_synthetic_soak=True)
    supervisor.deadline = gate.time.monotonic()+60
    supervisor.fingerprints = lambda: {}
    supervisor.health_probe = lambda: ('200', 0.2)
    supervisor.web_memory_bytes = lambda: 10
    supervisor.owned_containers = lambda: []
    safe_rows = [{'pid': 42, 'rss_kib': 140000, 'comm': 'python3'}]
    common = [
        patch.object(gate.shutil, 'disk_usage', return_value=SimpleNamespace(free=10*1024**3)),
        patch.object(gate, 'mem_available', return_value=4*1024**3),
        patch.object(gate, 'memory_psi_full_avg10', return_value=0.1),
        patch.object(gate, 'proc_tree_rss_kib', return_value=140000),
        patch.object(gate, 'proc_tree_rss_rows', return_value=safe_rows),
    ]
    with common[0], common[1], common[2], common[3], common[4]:
        with pytest.raises(gate.GateError, match='host_supervisor_rss_above_observed_limit'):
            supervisor.guard(expected_phase='native-synthetic-stability')
    assert supervisor.last_guard_sample['host_supervisor_rss_kib'] == 140000
    assert supervisor.last_guard_sample['host_supervisor_rss_processes'] == safe_rows
    assert supervisor.last_guard_sample['host_supervisor_rss_processes_status'] == 'observed'
    assert 'argv' not in json.dumps(supervisor.last_guard_sample)
    assert 'environment' not in json.dumps(supervisor.last_guard_sample)


def test_rss_attribution_failure_keeps_limit_fail_closed(tmp_path):
    assert gate.HOST_SUPERVISOR_RSS_LIMIT_KIB == 128*1024
    supervisor = gate.Supervisor(tmp_path, '0123456789abcdef', gate.time.monotonic(), {},
                                 observe_resource_thresholds=True,
                                 long_synthetic_soak=True)
    supervisor.deadline = gate.time.monotonic()+60
    supervisor.fingerprints = lambda: {}
    supervisor.health_probe = lambda: ('200', 0.2)
    supervisor.web_memory_bytes = lambda: 10
    supervisor.owned_containers = lambda: []
    common = [
        patch.object(gate.shutil, 'disk_usage', return_value=SimpleNamespace(free=10*1024**3)),
        patch.object(gate, 'mem_available', return_value=4*1024**3),
        patch.object(gate, 'memory_psi_full_avg10', return_value=0.1),
        patch.object(gate, 'proc_tree_rss_kib', return_value=140000),
        patch.object(gate, 'proc_tree_rss_rows', side_effect=OSError('private path unavailable')),
    ]
    with common[0], common[1], common[2], common[3], common[4]:
        with pytest.raises(gate.GateError, match='host_supervisor_rss_above_observed_limit'):
            supervisor.guard(expected_phase='native-synthetic-stability')
    assert supervisor.last_guard_sample['host_supervisor_rss_kib'] == 140000
    assert supervisor.last_guard_sample['host_supervisor_rss_processes'] == []
    assert supervisor.last_guard_sample['host_supervisor_rss_processes_status'] == 'unavailable'

    with common[0], common[1], common[2], \
            patch.object(gate, 'proc_tree_rss_kib', side_effect=OSError('read failed')):
        with pytest.raises(gate.GateError, match='host_supervisor_rss_measurement_unavailable'):
            supervisor.guard(expected_phase='native-synthetic-stability')
    assert supervisor.last_guard_sample['host_supervisor_rss_kib'] is None


def test_process_rss_rows_are_owned_descendants_and_sanitized_names(tmp_path, monkeypatch):
    contents = {
        '/proc/4321/status': 'Name:\tpython3\nPPid:\t1\nVmRSS:\t50000 kB\n',
        '/proc/4321/comm': 'python3 --api-key=PRIVATE\n',
        '/proc/4321/task/4321/children': '4322 4333',
        '/proc/4322/status': 'Name:\tdocker\nPPid:\t4321\nVmRSS:\t90000 kB\n',
        '/proc/4322/comm': '/usr/bin/docker\n',
        '/proc/4322/task/4322/children': '',
        '/proc/4333/status': 'Name:\tother\nPPid:\t9\nVmRSS:\t999999 kB\n',
    }
    monkeypatch.setattr(Path, 'read_text', lambda self, *args, **kwargs: contents[str(self)])
    rows = gate.proc_tree_rss_rows(4321)
    assert rows == [
        {'pid': 4322, 'rss_kib': 90000, 'comm': 'docker'},
        {'pid': 4321, 'rss_kib': 50000, 'comm': 'python3'},
    ]
    serialized = json.dumps(rows)
    assert 'PRIVATE' not in serialized
    assert '/usr/bin' not in serialized
    assert '--api-key' not in serialized


@pytest.mark.parametrize('changes, expected', [
    ({'runtime_only': False}, 'observe_resource_thresholds_requires_runtime_only'),
    ({'news_web_stopped': True}, 'observe_resource_thresholds_conflicts_with_news_web_stopped'),
    ({'short_window': True}, 'long_synthetic_soak_options_invalid'),
    ({'live_quality': True}, 'live_quality_options_invalid'),
    ({'soak': True}, 'long_synthetic_soak_options_invalid'),
    ({'observe_resource_thresholds': False}, 'long_synthetic_soak_options_invalid'),
])
def test_long_synthetic_soak_rejects_incompatible_options(changes, expected):
    options = {'runtime_only': True, 'news_web_stopped': False, 'container_id': None,
               'short_window': False, 'observe_resource_thresholds': True,
               'live_quality': False, 'long_synthetic_soak': True, 'soak': False}
    options.update(changes)
    with pytest.raises(gate.GateError, match=expected):
        gate.validate_maintenance_options(**options)


def test_long_synthetic_soak_cli_conflicts_fail_before_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(tmp_path),
        '--runtime-only', '--short-window', '--observe-resource-thresholds',
        '--long-synthetic-soak'])
    with patch.object(gate, 'ensure_stage') as ensure_stage:
        with pytest.raises(gate.GateError, match='long_synthetic_soak_options_invalid'):
            gate.main()
    ensure_stage.assert_not_called()


def test_long_synthetic_soak_cli_has_no_arbitrary_duration_option(tmp_path, monkeypatch):
    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(tmp_path), '--runtime-only',
        '--observe-resource-thresholds', '--long-synthetic-soak', '--duration-seconds', '30'])
    with patch.object(gate, 'ensure_stage') as ensure_stage:
        with pytest.raises(SystemExit):
            gate.main()
    ensure_stage.assert_not_called()


def test_live_quality_main_accepts_readonly_corpus_outside_fresh_stage(tmp_path, monkeypatch):
    stage = tmp_path/'fresh-output-stage'
    stage.mkdir(mode=0o700)
    stage.chmod(0o700)
    old_stage = tmp_path/'approved-corpus-stage'
    corpus = old_stage/'shadow-private'/'corpus-run'/'corpus.json'
    corpus.parent.mkdir(mode=0o700, parents=True)
    corpus.write_text(json.dumps({'version': 1, 'cases': [
        {'id': f'case-{index:04d}'} for index in range(1, 51)]}))
    corpus.chmod(0o600)
    assert corpus.parents[2].resolve() != stage.resolve()
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_PATH', corpus)
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_SHA256',
                        hashlib.sha256(corpus.read_bytes()).hexdigest())
    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(stage), '--runtime-only',
        '--observe-resource-thresholds', '--live-quality'])
    monkeypatch.setattr(gate.shutil, 'disk_usage', lambda _path: SimpleNamespace(free=20*1024**3))
    monkeypatch.setattr(gate, 'ensure_stage', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gate, 'mem_available', lambda: 8*1024**3)
    monkeypatch.setattr(gate, 'verify_pinned_images', lambda **_kwargs: None)
    monkeypatch.setattr(gate, 'verify_source_manifest', lambda _stage: 'a'*64)
    monkeypatch.setattr(gate, 'verify_runtime_bundle', lambda _stage: 'b'*64)
    calls = []
    real_verify_quality_corpus = gate.verify_quality_corpus
    def verify_fixed_corpus(path):
        calls.append(path)
        assert path == corpus
        assert path.resolve().is_relative_to(old_stage.resolve())
        assert path.resolve().is_relative_to(stage.resolve()) is False
        return real_verify_quality_corpus(path)
    # Preserve the real verifier while observing the path passed by main.
    monkeypatch.setattr(gate, 'verify_quality_corpus', verify_fixed_corpus)
    monkeypatch.setattr(gate, 'production_fingerprint', lambda role: {'role': role})
    monkeypatch.setattr(gate, 'production_user_path', lambda: {
        'home_page_ok': True, 'public_feed_ok': True})
    monkeypatch.setattr(gate, 'command', lambda *_args, **_kwargs:
        subprocess.CompletedProcess([], 0, '200', ''))

    class PreparationReached(Exception):
        pass

    real_prepare_quality_output = gate.prepare_quality_output
    def fresh_output_then_stop(fresh_stage):
        result = real_prepare_quality_output(fresh_stage)
        raise PreparationReached

    monkeypatch.setattr(gate, 'prepare_quality_output', fresh_output_then_stop)
    with pytest.raises(PreparationReached):
        gate.main()
    assert calls == [corpus]
    private = stage/'shadow-private'
    captures = private/'captures'
    assert private.is_dir() and private.stat().st_mode & 0o077 == 0
    assert captures.is_dir() and captures.stat().st_mode & 0o077 == 0
    assert not (old_stage/'shadow-private'/'captures').exists()


def test_stability_observation_requires_expected_state_duration_manifest_and_health(tmp_path):
    _completed_observation(tmp_path)
    result = gate.validate_stability_observation(tmp_path)
    assert result == {'run_id': 'news-native-shadow-0123456789', 'duration_seconds': 600,
                      'case_count': 5, 'completed_batches': 1,
                      'container_generation_unchanged': True,
                      'browser_generation_unchanged': True, 'oom_counters_unchanged': True}
    observation_path = tmp_path/result['run_id']/'synthetic-soak-observation.json'
    value = json.loads(observation_path.read_text())
    value['duration_seconds'] = 30
    observation_path.write_text(json.dumps(value))
    with pytest.raises(gate.GateError, match='native_stability_result_invalid'):
        gate.validate_stability_observation(tmp_path)


def test_full_main_runs_fixed_stability_stand_with_supervision_and_diagnostic_summary(
        tmp_path, monkeypatch):
    stage = tmp_path/'stage'
    stage.mkdir(mode=0o700)
    stage.chmod(0o700)
    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(stage), '--runtime-only',
        '--observe-resource-thresholds', '--long-synthetic-soak'])
    monkeypatch.setattr(gate.shutil, 'disk_usage', lambda _path: SimpleNamespace(free=20*1024**3))
    monkeypatch.setattr(gate, 'ensure_stage', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gate, 'mem_available', lambda: 8*1024**3)
    monkeypatch.setattr(gate, 'verify_pinned_images', lambda **_kwargs: None)
    monkeypatch.setattr(gate, 'verify_source_manifest', lambda _stage: 'a'*64)
    monkeypatch.setattr(gate, 'verify_runtime_bundle', lambda _stage: 'b'*64)
    monkeypatch.setattr(gate, 'production_fingerprint', lambda role: {'role': role})
    monkeypatch.setattr(gate, 'production_user_path', lambda: {
        'home_page_ok': True, 'public_feed_ok': True})
    def fake_go_checks(_supervisor, stage_arg, **_kwargs):
        binaries = stage_arg/'bin-soak'
        binaries.mkdir()
        for name in ('extract-service', 'extract-egress'):
            (binaries/name).write_bytes(b'test')
    monkeypatch.setattr(gate, 'run_go_checks', fake_go_checks)
    monkeypatch.setattr(gate, 'command', lambda *_args, **_kwargs:
        subprocess.CompletedProcess([], 0, '200', ''))
    events = []

    class FakeSupervisor:
        def __init__(self, stage_arg, label_value, started, baseline, **kwargs):
            self.stage = stage_arg
            self.label_value = label_value
            self.deadline = started + gate.NATIVE_STABILITY_HARD_SECONDS
            self.phases = []
            self.last_guard_sample = {'sampled': True}
            self.first_guard_failure_sample = None
            self.runtime_working_set_peak_bytes = 12
            self.memory_psi_full_avg10_sampled_peak = 0.1
            self.resource_threshold_bypass_counts = {'memory_psi_pressure': 0}
            self.runtime_container_sampled_peaks = {'worker': 123}

        def guard(self, **kwargs):
            events.append(('guard', kwargs.get('expected_phase')))

        def cleanup_label(self):
            events.append(('cleanup-label',))

        def fingerprints(self):
            return {'infra-chrome': {'role': 'infra-chrome'}, 'news_web': {'role': 'news_web'}}

    class FakeWatchdog:
        def wait(self, timeout):
            events.append(('watchdog-wait', timeout))
            return 0

        def terminate(self):
            events.append(('watchdog-terminate',))

    monkeypatch.setattr(gate, 'Supervisor', FakeSupervisor)
    watchdog_args = []
    def start_watchdog(args, **_kwargs):
        watchdog_args.extend(args)
        return FakeWatchdog()
    monkeypatch.setattr(gate.subprocess, 'Popen', start_watchdog)
    commands = []

    def supervised(_supervisor, args, **kwargs):
        commands.append((args, kwargs))
        events.extend([('stand', kwargs['phase']), ('guard', kwargs['phase']), ('cleanup',)])
        _completed_observation(stage)
        return {'final_guard_passed': True, 'owned_containers_remaining': 0,
                'owned_networks_remaining': 0}

    monkeypatch.setattr(gate, 'run_supervised_stand', supervised)
    with patch.object(gate.signal, 'signal'):
        gate.main()

    args, kwargs = commands[0]
    assert args[args.index('--long-synthetic-soak'):] == [
        '--long-synthetic-soak', '--deadline', args[args.index('--deadline')+1],
        '--supervisor-label', args[args.index('--supervisor-label')+1]]
    assert '--duration-seconds' not in args
    assert kwargs['phase'] == 'native-synthetic-stability'
    assert kwargs['timeout'] == gate.NATIVE_STABILITY_ACTIVE_SECONDS
    assert kwargs['active_deadline'] <= gate.time.monotonic()+gate.NATIVE_STABILITY_ACTIVE_SECONDS+1
    unix_deadline = int(args[args.index('--deadline')+1])
    assert unix_deadline <= int(__import__('time').time()+gate.NATIVE_STABILITY_ACTIVE_SECONDS+1)
    assert '--long-synthetic-soak' in watchdog_args
    assert '--observe-resource-thresholds' in watchdog_args
    assert '--runtime-only' in watchdog_args
    assert '--maintenance-profile' not in watchdog_args
    assert events.index(('stand', 'native-synthetic-stability')) < events.index(
        ('guard', 'native-synthetic-stability')) < events.index(('cleanup',))
    summary = json.loads((stage/'summary.json').read_text())
    assert summary['state'] == 'diagnostic_completed'
    assert summary['measurement_scope'] == 'native_synthetic_stability_600s'
    assert summary['stability_duration_seconds'] == 600
    assert summary['stability_claim_scope'] == 'diagnostic_only'
    assert summary['native_stability']['case_count'] == 5


def test_full_main_does_not_report_completion_when_stability_manifest_fails(tmp_path, monkeypatch):
    stage = tmp_path/'stage'
    stage.mkdir(mode=0o700)
    stage.chmod(0o700)
    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(stage), '--runtime-only',
        '--observe-resource-thresholds', '--long-synthetic-soak'])
    monkeypatch.setattr(gate.shutil, 'disk_usage', lambda _path: SimpleNamespace(free=20*1024**3))
    monkeypatch.setattr(gate, 'ensure_stage', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gate, 'mem_available', lambda: 8*1024**3)
    monkeypatch.setattr(gate, 'verify_pinned_images', lambda **_kwargs: None)
    monkeypatch.setattr(gate, 'verify_source_manifest', lambda _stage: 'a'*64)
    monkeypatch.setattr(gate, 'verify_runtime_bundle', lambda _stage: 'b'*64)
    monkeypatch.setattr(gate, 'production_fingerprint', lambda role: {'role': role})
    monkeypatch.setattr(gate, 'production_user_path', lambda: {
        'home_page_ok': True, 'public_feed_ok': True})
    def fake_go_checks(_supervisor, stage_arg, **_kwargs):
        binaries = stage_arg/'bin-soak'
        binaries.mkdir()
        for name in ('extract-service', 'extract-egress'):
            (binaries/name).write_bytes(b'test')
    monkeypatch.setattr(gate, 'run_go_checks', fake_go_checks)
    monkeypatch.setattr(gate, 'command', lambda *_args, **_kwargs:
        subprocess.CompletedProcess([], 0, '200', ''))

    class FakeSupervisor:
        def __init__(self, stage_arg, label_value, started, baseline, **kwargs):
            self.stage = stage_arg
            self.label_value = label_value
            self.deadline = started + gate.NATIVE_STABILITY_HARD_SECONDS
            self.phases = []
            self.last_guard_sample = None
            self.first_guard_failure_sample = None
            self.runtime_working_set_peak_bytes = 0
            self.memory_psi_full_avg10_sampled_peak = 0
            self.resource_threshold_bypass_counts = {}
            self.runtime_container_sampled_peaks = {}

        def guard(self, **kwargs):
            pass

        def cleanup_label(self):
            pass

        def cleanup_label(self):
            pass

        def fingerprints(self):
            return {'infra-chrome': {'role': 'infra-chrome'}, 'news_web': {'role': 'news_web'}}

    class FakeWatchdog:
        def wait(self, timeout):
            return 0

        def terminate(self):
            pass

    monkeypatch.setattr(gate, 'Supervisor', FakeSupervisor)
    monkeypatch.setattr(gate.subprocess, 'Popen', lambda *_args, **_kwargs: FakeWatchdog())

    def supervised(_supervisor, _args, **_kwargs):
        _completed_observation(stage)
        manifest = next(stage.glob('news-native-shadow-*/manifest.json'))
        manifest.write_text('{"state":"controlled_gate_passed"}')
        return {'final_guard_passed': True, 'owned_containers_remaining': 0,
                'owned_networks_remaining': 0}

    monkeypatch.setattr(gate, 'run_supervised_stand', supervised)
    with patch.object(gate.signal, 'signal'):
        with pytest.raises(gate.GateError, match='native_stability_result_invalid'):
            gate.main()
    summary = json.loads((stage/'summary.json').read_text())
    assert summary['state'] == 'failed'
    assert summary['error'] == 'native_stability_result_invalid'
