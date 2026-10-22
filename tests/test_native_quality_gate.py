from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from scripts import run_go_cloak_linux_gate as gate


def _report(variant: str, ids: list[str], attempted: list[str]) -> dict:
    return {
        'schema_version': 1,
        'variant': variant,
        'corpus_sha256': gate.QUALITY_CORPUS_SHA256,
        'status': 'complete',
        'fatal_error': None,
        'selected_case_ids': ids,
        'attempted_case_ids': attempted,
        'rows': [{'id': case_id, 'outcome': 'success'} for case_id in ids],
    }


def _write_capture_state(captures: Path, phase: str) -> None:
    if phase == 'c-smoke':
        attempted = {'B': [], 'C': ['case-0001']}
        reports = {'C': _report('C', ['case-0001'], attempted['C'])}
    elif phase == 'b-quality':
        attempted = {'B': list(gate.QUALITY_CASES), 'C': ['case-0001']}
        reports = {
            'C': _report('C', ['case-0001'], attempted['C']),
            'B': _report('B', list(gate.QUALITY_CASES), attempted['B']),
        }
    else:
        attempted = {'B': list(gate.QUALITY_CASES), 'C': list(gate.QUALITY_CASES)}
        reports = {
            'C': _report('C', list(gate.QUALITY_CASES), attempted['C']),
            'B': _report('B', list(gate.QUALITY_CASES), attempted['B']),
        }
    ledger = {
        'version': 1,
        'corpus_sha256': gate.QUALITY_CORPUS_SHA256,
        'attempted': attempted,
        'blocked_hosts': {'B': [], 'C': []},
        'last_navigation_epoch': {'B': None, 'C': 1790000000},
    }
    (captures / 'ledger.json').write_text(json.dumps(ledger))
    for variant, report in reports.items():
        (captures / f'report-{variant}.json').write_text(json.dumps(report))


def test_live_quality_uses_fixed_c_b_c_sequence_and_validates_each_ledger(tmp_path):
    captures = tmp_path / 'captures'
    captures.mkdir(mode=0o700)
    captures.chmod(0o700)
    stage = tmp_path / 'stage'
    stage.mkdir(mode=0o700)
    stage.chmod(0o700)
    supervisor = SimpleNamespace(browser_phase=False)
    commands = []

    def run_stand(_supervisor, args, **kwargs):
        phase = args[args.index('--quality-phase') + 1]
        commands.append((args, kwargs))
        _write_capture_state(captures, phase)
        return {'final_guard_passed': True, 'owned_containers_remaining': 0,
                'owned_networks_remaining': 0}

    records = []
    with patch.object(gate, 'run_supervised_stand', side_effect=run_stand):
        results = gate.run_live_quality(
            supervisor, stage, corpus=gate.QUALITY_CORPUS_PATH, captures=captures,
            receipt=tmp_path/'receipt', parent_label='linux-vps-gate=0123456789abcdef',
            active_deadline=gate.time.monotonic() + 1000,
            hard_deadline=gate.time.monotonic() + 1100, phase_records=records)

    assert [result['phase'] for result in results] == ['c-smoke', 'b-quality', 'c-quality']
    assert [result['variant'] for result in results] == ['C', 'B', 'C']
    assert [result['case_ids'] for result in results] == [
        ['case-0001'], list(gate.QUALITY_CASES), list(gate.QUALITY_CASES[1:])]
    assert [record['state'] for record in records] == ['completed'] * 3
    assert [args[args.index('--quality-phase') + 1] for args, _ in commands] == [
        'c-smoke', 'b-quality', 'c-quality']
    assert all('--live' in args and '--quality-profile' in args
               and '--bounded-profile' in args and '--supervisor-cleanup' in args
               for args, _ in commands)
    assert [kwargs['timeout'] for _, kwargs in commands] == [360, 360, 360]


def test_quality_corpus_pin_accepts_full_50_case_corpus(tmp_path, monkeypatch):
    ids = [f'case-{index:04d}' for index in range(1, 51)]
    corpus = tmp_path / 'corpus.json'
    corpus.write_text(json.dumps({'version': 1, 'cases': [{'id': case_id} for case_id in ids]}))
    corpus.chmod(0o600)
    digest = hashlib.sha256(corpus.read_bytes()).hexdigest()
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_PATH', corpus)
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_SHA256', digest)
    assert gate.verify_quality_corpus(corpus) == digest

    five_only = tmp_path / 'five.json'
    five_only.write_text(json.dumps({'version': 1, 'cases': [{'id': case_id} for case_id in gate.QUALITY_CASES]}))
    five_only.chmod(0o600)
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_PATH', five_only)
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_SHA256', hashlib.sha256(five_only.read_bytes()).hexdigest())
    with pytest.raises(gate.GateError, match='quality_corpus_invalid'):
        gate.verify_quality_corpus(five_only)


def test_quality_corpus_rejects_content_not_matching_fixed_hash(tmp_path, monkeypatch):
    corpus = tmp_path / 'corpus.json'
    corpus.write_text(json.dumps({'version': 1,
        'cases': [{'id': f'case-{index:04d}'} for index in range(1, 51)]}))
    corpus.chmod(0o600)
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_PATH', corpus)
    with pytest.raises(gate.GateError, match='quality_corpus_hash_mismatch'):
        gate.verify_quality_corpus(corpus)


@pytest.mark.parametrize('test_case', [
    ({'runtime_only': False}, 'observe_resource_thresholds_requires_runtime_only'),
    ({'news_web_stopped': True}, 'observe_resource_thresholds_conflicts_with_news_web_stopped'),
    ({'short_window': True}, 'live_quality_options_invalid'),
    ({'soak': True}, 'live_quality_options_invalid'),
    ({'observe_resource_thresholds': False}, 'live_quality_options_invalid'),
])
def test_live_quality_rejects_incompatible_modes(test_case):
    changes, expected_error = test_case
    options = {'runtime_only': True, 'news_web_stopped': False, 'container_id': None,
               'short_window': False, 'observe_resource_thresholds': True,
               'live_quality': True, 'soak': False}
    options.update(changes)
    with pytest.raises(gate.GateError, match=expected_error):
        gate.validate_maintenance_options(**options)


def test_live_quality_cli_rejects_mode_conflicts_before_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(tmp_path),
                                           '--runtime-only', '--short-window',
                                           '--observe-resource-thresholds', '--live-quality'])
    with patch.object(gate, 'ensure_stage') as ensure_stage:
        with pytest.raises(gate.GateError, match='live_quality_options_invalid'):
            gate.main()
    ensure_stage.assert_not_called()


def test_prepare_quality_output_rejects_stale_capture_history(tmp_path):
    private = tmp_path / 'shadow-private'
    private.mkdir(mode=0o700)
    private.chmod(0o700)
    captures = private / 'captures'
    captures.mkdir(mode=0o700)
    with pytest.raises(gate.GateError, match='quality_captures_not_fresh'):
        gate.prepare_quality_output(tmp_path)


def test_quality_capture_validation_rejects_duplicate_or_extra_attempt(tmp_path):
    captures = tmp_path / 'captures'
    captures.mkdir(mode=0o700)
    captures.chmod(0o700)
    _write_capture_state(captures, 'c-smoke')
    ledger = json.loads((captures / 'ledger.json').read_text())
    ledger['attempted']['C'].append('case-0001')
    (captures / 'ledger.json').write_text(json.dumps(ledger))
    with pytest.raises(gate.GateError, match='quality_capture_validation_failed'):
        gate.validate_quality_capture_state(captures, 'c-smoke')


def test_quality_capture_validation_enforces_256_mib_private_output_cap(tmp_path):
    captures = tmp_path / 'captures'
    captures.mkdir(mode=0o700)
    captures.chmod(0o700)
    with patch.object(gate, 'tree_bytes', return_value=gate.QUALITY_PRIVATE_OUTPUT_LIMIT+1):
        with pytest.raises(gate.GateError, match='quality_private_output_limit'):
            gate.validate_quality_capture_state(captures, 'c-smoke')


def test_receipt_is_minted_only_after_gate_guard_and_exact_cleanup(tmp_path):
    stage = tmp_path
    (stage / 'shadow-private').mkdir(mode=0o700)
    run_id = 'news-native-shadow-0123456789'
    source = stage / run_id
    source.mkdir(mode=0o700)
    (source / 'manifest.json').write_text(json.dumps({
        'state': 'controlled_browser_gate_passed', 'bootstrap': True, 'prefix': run_id,
        'previous_phase': 'runtime_ready'}))
    (source / 'browser-trap.json').write_text('{"accepted":0}')
    cleanup = {'final_guard_passed': True, 'owned_containers_remaining': 0,
               'owned_networks_remaining': 0}
    receipt = gate.mint_quality_gate_receipt(stage, run_id,
        'linux-vps-gate=0123456789abcdef', 'a' * 64, 'b' * 64, cleanup)
    manifest = json.loads((receipt/'manifest.json').read_text())
    assert manifest['state'] == 'cleaned'
    assert manifest['previous_phase'] == 'controlled_browser_gate_passed'
    assert manifest['bootstrap'] is True
    outer = json.loads((receipt/'outer-cleanup.json').read_text())
    assert outer == {'state': 'confirmed', 'parent_label': 'linux-vps-gate=0123456789abcdef',
                     'owned_containers_remaining': 0, 'owned_networks_remaining': 0,
                     'final_guard_passed': True, 'source_manifest_sha256': 'a' * 64,
                     'runtime_bundle_manifest_sha256': 'b' * 64}


def test_receipt_is_not_created_when_final_guard_or_cleanup_is_unconfirmed(tmp_path):
    (tmp_path/'shadow-private').mkdir(mode=0o700)
    run_id = 'news-native-shadow-0123456789'
    source = tmp_path/run_id
    source.mkdir(mode=0o700)
    (source/'manifest.json').write_text(json.dumps({
        'state': 'controlled_browser_gate_passed', 'bootstrap': True, 'prefix': run_id}))
    (source/'browser-trap.json').write_text('{"accepted":0}')
    with pytest.raises(gate.GateError, match='quality_gate_receipt_invalid'):
        gate.mint_quality_gate_receipt(tmp_path, run_id,
            'linux-vps-gate=0123456789abcdef', 'a'*64, 'b'*64,
            {'final_guard_passed': False, 'owned_containers_remaining': 0,
             'owned_networks_remaining': 0})
    assert not (tmp_path/'shadow-private'/'quality-gate-receipt').exists()


def test_supervised_stand_always_guards_before_exact_label_cleanup(tmp_path):
    events = []
    supervisor = SimpleNamespace(
        browser_phase=True,
        run_command=lambda *args, **kwargs: events.append(('run', kwargs['timeout'])),
        guard=lambda **kwargs: events.append(('guard', kwargs['expected_phase'])),
        cleanup_label=lambda: events.append(('cleanup',)),
        owned_containers=lambda: [], label_filter='label=linux-vps-gate=0123456789abcdef')
    network = subprocess.CompletedProcess([], 0, '', '')
    with patch.object(gate, 'command', return_value=network):
        result = gate.run_supervised_stand(
            supervisor, ['stand'], phase='live-c-smoke', timeout=360,
            stage=tmp_path, active_deadline=gate.time.monotonic()+30)
    assert [event[0] for event in events] == ['run', 'guard', 'cleanup']
    assert events[1][1] == 'live-c-smoke'
    assert events[0][1] < 360
    assert result['final_guard_passed'] is True


def test_supervised_stand_attempts_guard_and_cleanup_after_command_failure(tmp_path):
    events = []
    supervisor = SimpleNamespace(
        browser_phase=True,
        run_command=lambda *args, **kwargs: (_ for _ in ()).throw(gate.GateError('c_quality_timeout')),
        guard=lambda **kwargs: events.append('guard'),
        cleanup_label=lambda: events.append('cleanup'),
        owned_containers=lambda: [], label_filter='label=linux-vps-gate=0123456789abcdef')
    with pytest.raises(gate.GateError, match='c_quality_timeout'):
        gate.run_supervised_stand(supervisor, ['stand'], phase='live-c-quality', timeout=360,
                                  stage=tmp_path, active_deadline=gate.time.monotonic()+30)
    assert events == ['guard', 'cleanup']


def test_live_quality_global_active_deadline_stops_before_a_phase(tmp_path):
    supervisor = SimpleNamespace(browser_phase=False)
    records = []
    with patch.object(gate, 'run_supervised_stand') as run_stand:
        with pytest.raises(gate.GateError, match='global_live_quality_active_deadline'):
            gate.run_live_quality(
                supervisor, tmp_path, corpus=gate.QUALITY_CORPUS_PATH,
                captures=tmp_path, receipt=tmp_path/'receipt',
                parent_label='linux-vps-gate=0123456789abcdef',
                active_deadline=gate.time.monotonic()-1,
                hard_deadline=gate.time.monotonic()+30, phase_records=records)
    run_stand.assert_not_called()
    assert records == []


def test_live_quality_main_runs_pre_gate_and_c_b_c_before_diagnostic_summary(tmp_path, monkeypatch):
    stage = tmp_path / 'stage'
    stage.mkdir(mode=0o700)
    (stage / 'bin-soak').mkdir(mode=0o700)
    for name in ('extract-service', 'extract-egress'):
        (stage / 'bin-soak' / name).write_bytes(name.encode())
    baseline = {'news_web': ['web'], 'infra-chrome': ['chrome']}
    events = []

    class FakeSupervisor:
        def __init__(self, *_args, **_kwargs):
            self.phases = []
            self.last_guard_sample = None
            self.first_guard_failure_sample = None
            self.runtime_working_set_peak_bytes = 0
            self.memory_psi_full_avg10_sampled_peak = 0
            self.runtime_container_sampled_peaks = {}
            self.resource_threshold_bypass_counts = {'runtime_working_set_above_budget': 0,
                                                      'memory_psi_pressure': 0}
            self.browser_phase = False

        def guard(self, **_kwargs):
            events.append('guard-before-run')

        def fingerprints(self):
            return baseline

    def run_stand(_supervisor, args, **kwargs):
        events.append(kwargs['phase'])
        if kwargs['phase'] == 'synthetic-browser-gate':
            run_id = 'news-native-shadow-0123456789'
            run_dir = stage / run_id
            run_dir.mkdir(mode=0o700)
            (run_dir / 'manifest.json').write_text(json.dumps({
                'state': 'controlled_browser_gate_passed', 'bootstrap': True,
                'prefix': run_id, 'previous_phase': 'runtime_ready'}))
            (run_dir / 'browser-trap.json').write_text('{"accepted":0}')
            (stage / 'synthetic-browser-gate.log').write_text(json.dumps({
                'state': 'controlled_gate_passed', 'run_id': run_id}) + '\n')
        else:
            phase = args[args.index('--quality-phase') + 1]
            _write_capture_state(stage/'shadow-private'/'captures', phase)
        return {'final_guard_passed': True, 'owned_containers_remaining': 0,
                'owned_networks_remaining': 0}

    class FakeProcess:
        def wait(self, timeout=None):
            return 0

        def terminate(self):
            return None

    monkeypatch.setattr(gate.sys, 'argv', ['runner', '--stage', str(stage), '--runtime-only',
                                           '--observe-resource-thresholds', '--live-quality'])
    monkeypatch.setattr(gate, 'QUALITY_CORPUS_PATH',
                        tmp_path/'approved-corpus-stage'/'shadow-private'/'corpus-run'/'corpus.json')
    with patch.object(gate, 'ensure_stage'), \
         patch.object(gate.shutil, 'disk_usage', return_value=SimpleNamespace(free=10*1024**3)), \
         patch.object(gate, 'mem_available', return_value=4*1024**3), \
         patch.object(gate, 'verify_pinned_images'), \
         patch.object(gate, 'verify_source_manifest', side_effect=lambda *_: events.append('source-pin') or 'a'*64), \
         patch.object(gate, 'verify_runtime_bundle', side_effect=lambda *_: events.append('runtime-pin') or 'b'*64), \
         patch.object(gate, 'verify_quality_corpus', side_effect=lambda *_: events.append('corpus-pin') or gate.QUALITY_CORPUS_SHA256), \
         patch.object(gate, 'production_fingerprint', side_effect=lambda name: baseline[name]), \
         patch.object(gate, 'production_user_path', return_value={'home_page_ok': True, 'public_feed_ok': True}), \
         patch.object(gate, 'command', return_value=subprocess.CompletedProcess([], 0, '200', '')), \
         patch.object(gate, 'run_go_checks'), \
         patch.object(gate, 'Supervisor', FakeSupervisor), \
         patch.object(gate, 'run_supervised_stand', side_effect=run_stand), \
         patch.object(gate.signal, 'signal'), \
         patch.object(gate.subprocess, 'Popen', return_value=FakeProcess()), \
         patch('builtins.print'):
        gate.main()

    summary = json.loads((stage/'summary.json').read_text())
    assert summary['state'] == 'diagnostic_completed'
    assert summary['quality_claim_scope'] == 'diagnostic_only'
    assert [row.get('phase') for row in summary['quality_phases']] == [
        'synthetic-pre-gate', 'c-smoke', 'b-quality', 'c-quality']
    assert [row.get('phase') for row in summary['quality_phases'][1:]] == [
        'c-smoke', 'b-quality', 'c-quality']
    assert summary['quality_attempted_case_ids'] == {
        'B': list(gate.QUALITY_CASES), 'C': list(gate.QUALITY_CASES)}
    assert events.index('source-pin') < events.index('runtime-pin') < events.index('corpus-pin')
    assert events.index('corpus-pin') < events.index('synthetic-browser-gate')
    assert events.index('synthetic-browser-gate') < events.index('live-c-smoke')
    assert events.index('live-c-smoke') < events.index('live-b-quality') < events.index('live-c-quality')
    assert 'origin_url' not in json.dumps(summary)
