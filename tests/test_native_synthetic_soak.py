import json
from pathlib import Path

import pytest

from scripts import native_synthetic_soak as soak
from scripts import native_shadow_stand as stand_module


class FakeStand:
    prefix = 'news-native-shadow-0123456789'
    bootstrap = True
    bounded_profile = True
    synthetic_soak = True
    worker = prefix + '-worker'
    observer = prefix + '-observer'
    cloak = prefix + '-cloak'
    gateway = prefix + '-egress'
    relay = prefix + '-relay'

    def __init__(self, root):
        self.stage = root / 'stage'
        self.stage.mkdir()
        fixture = self.stage / 'benchmark/tests/fixtures/native-cloak'
        fixture.mkdir(parents=True)
        (fixture / 'origin.py').write_text('# fixed fixture')
        tls = self.stage / 'tls'
        tls.mkdir()
        (tls / 'cert.pem').write_text('cert')
        (tls / 'key.pem').write_text('key')
        self.run_dir = self.stage / self.prefix
        (self.run_dir / 'browser-info').mkdir(parents=True)
        (self.run_dir / 'browser-info/launch-info.json').write_text('{"binary_sha256":"fixed"}')
        self.containers = [self.prefix + '-' + suffix for suffix in
                           ('browser-anchor', 'control-anchor', 'gateway-anchor',
                            'egress', 'cloak', 'relay', 'observer', 'worker')]
        self.anchors = {'gateway': {'name': self.prefix + '-gateway-anchor'},
                        'browser': {'name': self.prefix + '-browser-anchor', 'ip': '172.22.0.2'}}
        self.origin = None
        self.events = []
        self.cgroup_reads = 0
        self.oom_after = False
        self.counter_reset = False
        self.generation_change = False
        self.fail_exec = False
        self.fail_diagnostic_collection = False
        self.fail_collection_on_failure = False
        self.log_output = ''
        self.warmup_override = None

    def run(self, suffix, image, args, **kwargs):
        self.events.append(('run', suffix, image, args, kwargs))
        self.origin = self.prefix + '-origin'
        self.containers.append(self.origin)
        return self.origin

    def verify_ns(self, name, anchor):
        self.events.append(('verify_ns', name, anchor))

    def wait_listener(self, role, port, uid):
        self.events.append(('wait_listener', role, port, uid))

    def browser_loopback_diagnostic_counters(self):
        return {name: 0 for name in soak.BROWSER_LOOPBACK_COUNTERS}

    def check_runtime(self):
        if self.fail_diagnostic_collection:
            raise RuntimeError('private runtime details')
        self.events.append(('check_runtime',))

    def cgroup(self, name):
        role = 'worker' if name == self.worker else 'cloak' if name == self.cloak else 'gateway'
        return '/host-cg/fake/' + role

    def docker(self, *args):
        if args[:2] == ('inspect', '--format'):
            rows = []
            for name in args[3:]:
                started = 'started-2' if self.generation_change and name.endswith('-cloak') else 'started-1'
                rows.append('|'.join(('/' + name, 'id-' + name, started, '0', 'false', 'true', self.prefix)))
            return '\n'.join(rows)
        if args[0] == 'exec':
            if args[-1].startswith('http://'):
                return 'c' * 64 + '\n'
            if 'cpu.stat' in args[4]:
                return json.dumps({'memory_current_bytes': 1000, 'oom_events': 0,
                                   'oom_kill_events': 0, 'pids': 4,
                                   'cpu_stat': {'usage_usec': 1, 'nr_periods': 2,
                                                'nr_throttled': 3, 'throttled_usec': 4}})
            self.cgroup_reads += 1
            values = {}
            for role in soak.CGROUP_ROLES:
                values[role] = {'memory_current_bytes': 1000 + self.cgroup_reads,
                                'oom_events': (1 if self.cgroup_reads == 1 and self.counter_reset else
                                               int(self.oom_after and self.cgroup_reads > 1)),
                                'oom_kill_events': 0, 'pids': 4}
            return json.dumps(values)
        raise AssertionError(f'unexpected docker command: {args[0]}')

    def diagnostic_logs(self, name):
        assert name == self.worker
        if self.fail_diagnostic_collection:
            raise RuntimeError('private log details')
        return self.log_output

    def monitored_exec(self, args):
        self.events.append(('monitored_exec', args))
        if self.fail_exec:
            self.log_output = (
                '2026/10/21 12:00:00 native_browser_diagnostic stage=events reason=protocol_error_-32603\n'
                'native_browser_diagnostic stage=bad reason=private-token\n'
                'private-token body https://secret.example\n')
            self.fail_diagnostic_collection = self.fail_collection_on_failure
            raise RuntimeError('private details must not escape')
        output = self.run_dir / 'soak'
        summary = {'schema_version': 1, 'state': 'passed', 'variant': 'C',
                   'requested_duration_seconds': 600, 'cycles_per_batch': 1,
                   'case_ids': list(soak.SOAK_CASE_IDS), 'batches_this_invocation': 1,
                   'completed_batches': 1, 'elapsed_seconds': 600, 'failure_code': None}
        (output / 'summary.json').write_text(json.dumps(summary))
        resources = {role: {'oom_events_delta': 0, 'oom_kill_events_delta': 0}
                     for role in soak.CGROUP_ROLES}
        outcomes = [{'case_id': case_id, 'cycle': 1, 'expectation': {'passed': True}}
                    for case_id in soak.SOAK_CASE_IDS]
        report = {'batch_index': 1, 'status': 'passed', 'variant': 'C', 'cycles': 1,
                  'case_ids': list(soak.SOAK_CASE_IDS), 'mismatch_count': 0,
                  'warmup': self.warmup_override or {
                      'case_id': soak.SOAK_CASE_IDS[0], 'outcome': 'success',
                      'state': 'extracted', 'method': 'browser_rendering', 'duration_ms': 42.0},
                  'readiness_profile': {
                      'policy': 'native_fixture_selector_and_network_gate',
                      'selector': '[data-ready="true"]', 'active_requests': 0,
                      'main_document_status': 200,
                      'go_profile_basis': 'checked_in_source_not_live_attestation'},
                  'outcomes': outcomes, 'resource_metrics': {'cgroups': resources}}
        (output / 'batch-000001.json').write_text(json.dumps(report))

    def wait_http(self, name, url):
        self.events.append(('wait_http', name, url))


def test_fixed_soak_command_starts_one_limited_origin_and_preserves_generation(tmp_path):
    stand = FakeStand(tmp_path)
    result = soak.run_synthetic_stability(stand)
    origin_run = next(event for event in stand.events if event[0] == 'run')
    _, suffix, image, origin_args, options = origin_run
    assert suffix == 'origin' and image == stand_module.APP
    assert soak.APP_IMAGE == stand_module.APP
    assert origin_args[:3] == ['/fixtures/native-cloak/origin.py', '--host', '127.0.0.1']
    assert options['network'] == 'container:' + stand.anchors['gateway']['name']
    assert options['memory'] == '128m' and options['cpu'] == '0.125' and options['pids'] == 64
    assert any(mount.endswith(':/fixtures/native-cloak:ro') for mount in options['mounts'])
    assert any(mount.endswith(':/tls:ro') for mount in options['mounts'])
    assert len([event for event in stand.events if event[0] == 'run']) == 1
    command = next(event[1] for event in stand.events if event[0] == 'monitored_exec')
    assert command[:3] == ['python', '/bench/scripts/soak_native_cloak.py', '--variant']
    assert '--duration-seconds' in command and command[command.index('--duration-seconds') + 1] == '600'
    assert '--cycles' in command and command[command.index('--cycles') + 1] == '1'
    assert '--idle-seconds' in command and command[command.index('--idle-seconds') + 1] == '5'
    assert command[command.index('--native-ready-selector') + 1] == '[data-ready="true"]'
    assert command[command.index('--endpoint') + 1] == 'http://127.0.0.1:8080'
    assert command[command.index('--origin') + 1] == 'https://native-fixture.test'
    assert [command[index + 1] for index, value in enumerate(command[:-1]) if value == '--case'] == list(soak.SOAK_CASE_IDS)
    assert all('DATABASE_URL' not in value and 'postgres' not in value for value in command)
    assert result['state'] == 'synthetic_stability_completed'
    assert result['completed_batches'] == 1 and result['oom_counters_unchanged'] is True
    assert result['container_generation_unchanged'] is True
    assert result['browser_generation_unchanged'] is True
    assert not any(event[0] == 'cleanup' for event in stand.events)


def test_failed_warmup_rejects_otherwise_passing_soak_batch(tmp_path):
    stand = FakeStand(tmp_path)
    stand.warmup_override = {
        'case_id': soak.SOAK_CASE_IDS[0], 'outcome': 'failed',
        'state': 'failed', 'method': 'none', 'duration_ms': 42.0,
    }
    with pytest.raises(soak.SoakPhaseError, match='soak_warmup_contract_failed'):
        soak.run_synthetic_stability(stand)


@pytest.mark.parametrize('profile', [
    {'bootstrap': False, 'bounded_profile': True, 'synthetic_soak': True},
    {'bootstrap': True, 'bounded_profile': False, 'synthetic_soak': True},
    {'bootstrap': True, 'bounded_profile': True, 'synthetic_soak': False},
])
def test_soak_requires_future_explicit_synthetic_bounded_profile(tmp_path, profile):
    stand = FakeStand(tmp_path)
    for key, value in profile.items():
        setattr(stand, key, value)
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_profile_required'):
        soak.run_synthetic_stability(stand)
    assert stand.origin is None


def test_oom_during_soak_fails_and_leaves_cleanup_to_outer_supervisor(tmp_path):
    stand = FakeStand(tmp_path)
    stand.oom_after = True
    with pytest.raises(soak.SoakPhaseError, match='soak_oom_detected'):
        soak.run_synthetic_stability(stand)
    assert stand.origin is not None
    assert not any(event[0] == 'cleanup' for event in stand.events)


def test_oom_counter_reset_fails_and_leaves_cleanup_to_outer_supervisor(tmp_path):
    stand = FakeStand(tmp_path)
    stand.counter_reset = True
    with pytest.raises(soak.SoakPhaseError, match='soak_cgroup_counter_reset'):
        soak.run_synthetic_stability(stand)
    assert stand.origin is not None
    assert not any(event[0] == 'cleanup' for event in stand.events)


def test_soak_failure_is_sanitized_and_does_not_remove_owned_resources(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed') as exc:
        soak.run_synthetic_stability(stand)
    assert 'private details' not in str(exc.value)
    assert stand.origin is not None
    assert not any(event[0] == 'cleanup' for event in stand.events)
    report_path = stand.run_dir / 'synthetic-failure-diagnostics.json'
    report = json.loads(report_path.read_text())
    encoded = json.dumps(report)
    assert report['diagnostics'] == [{'stage': 'events', 'reason': 'protocol_error_-32603'}]
    assert 'private-token' not in encoded and 'https://' not in encoded
    assert report_path.stat().st_mode & 0o777 == 0o600


def test_diagnostic_collection_failure_does_not_replace_soak_failure(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    stand.fail_collection_on_failure = True
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed') as caught:
        soak.run_synthetic_stability(stand)
    assert 'private details' not in str(caught.value)
    assert not any(event[0] == 'cleanup' for event in stand.events)


def test_failure_collector_includes_only_valid_relay_diagnostic_fields(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    relay_dir = stand.run_dir / 'relay-report'
    relay_dir.mkdir()
    counters = {name: 0 for name in soak.RELAY_DIAGNOSTIC_COUNTERS}
    counters.update(connect_attempted=3, connect_timeout=1, connected=2, closed=2)
    timing = {name: 0 for name in soak.RELAY_TIMING_FIELDS}
    timing.update(connect_total=2000, connect_max=2000)
    tcp = {name: value for name, value in zip(soak.TCP_LISTENER_COUNTERS, (1, 2, 3))}
    tcp_connections = {name: value for name, value in zip(soak.TCP_CONNECTION_COUNTERS, (4, 5, 6, 7, 8))}
    event_loop_lag = {'samples': 2400, 'max': 2010}
    (relay_dir / 'relay-diagnostics.json').write_text(json.dumps({
        'schema_version': 1, 'active': 0, 'peak_active': 1,
        'counters': counters, 'timing_ms': timing, 'tcp_listener_counters': tcp,
        'tcp_connection_counters': tcp_connections, 'event_loop_lag_ms': event_loop_lag,
        'endpoint': 'https://secret.example/private',
    }))
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed'):
        soak.run_synthetic_stability(stand)
    result = json.loads((stand.run_dir / 'synthetic-failure-diagnostics.json').read_text())
    encoded = json.dumps(result)
    assert result['relay']['counters']['connect_timeout'] == 1
    assert result['relay']['timing_ms']['connect_max'] == 2000
    assert result['relay']['tcp_listener_counters']['ListenOverflows'] == 1
    assert result['relay']['tcp_connection_counters']['AttemptFails'] == 5
    assert result['relay']['event_loop_lag_ms']['max'] == 2010
    assert set(result['browser_loopback_nft_counters']) == set(soak.BROWSER_LOOPBACK_COUNTERS)
    assert result['relay_cgroup']['cpu_stat']['nr_throttled'] == 3
    assert 'endpoint' not in result['relay']
    assert 'secret.example' not in encoded


def test_failure_collector_reports_accepted_handshake_counter_deltas_from_gate(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    baseline = {name: 0 for name in soak.BROWSER_LOOPBACK_COUNTERS}
    baseline['diag_loopback5100_fallback_syn'] = 1  # expected root-owned raw gate probe
    baseline['diag_loopback5100_accept_relay_uid_syn'] = 20
    baseline['diag_loopback5100_input_accept_syn'] = 20
    (stand.run_dir / 'raw-network-gate.json').write_text(json.dumps({
        'probes': [], 'traps': [], 'browser_loopback_nft_counters_after_gate': baseline,
    }))
    current = dict(baseline)
    current.update(diag_loopback5100_accept_relay_uid_syn=27,
                   diag_loopback5100_input_accept_syn=27,
                   diag_loopback5100_accept_cloak_uid_synack=7,
                   diag_loopback5100_input_accept_synack=7)
    stand.browser_loopback_diagnostic_counters = lambda: current
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed'):
        soak.run_synthetic_stability(stand)
    result = json.loads((stand.run_dir / 'synthetic-failure-diagnostics.json').read_text())
    delta = result['browser_loopback_nft_counter_delta']
    assert delta['baseline_after_raw_network_gate'] == baseline
    assert delta['current_at_failure'] == current
    assert delta['delta_since_raw_network_gate']['diag_loopback5100_accept_relay_uid_syn'] == 7
    assert delta['delta_since_raw_network_gate']['diag_loopback5100_input_accept_syn'] == 7
    assert delta['delta_since_raw_network_gate']['diag_loopback5100_accept_cloak_uid_synack'] == 7
    assert delta['delta_since_raw_network_gate']['diag_loopback5100_input_accept_synack'] == 7


def test_failure_collector_omits_loopback_delta_when_counter_reset(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    baseline = {name: 1 for name in soak.BROWSER_LOOPBACK_COUNTERS}
    (stand.run_dir / 'raw-network-gate.json').write_text(json.dumps({
        'browser_loopback_nft_counters_after_gate': baseline,
    }))
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed'):
        soak.run_synthetic_stability(stand)
    result = json.loads((stand.run_dir / 'synthetic-failure-diagnostics.json').read_text())
    assert 'browser_loopback_nft_counters' in result
    assert 'browser_loopback_nft_counter_delta' not in result


def test_failure_collector_rejects_malformed_loopback_counter_set(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    stand.browser_loopback_diagnostic_counters = lambda: {'unexpected': 4}
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed'):
        soak.run_synthetic_stability(stand)
    result = json.loads((stand.run_dir / 'synthetic-failure-diagnostics.json').read_text())
    assert 'browser_loopback_nft_counters' not in result


def test_failure_collector_omits_malformed_relay_diagnostics(tmp_path):
    stand = FakeStand(tmp_path)
    stand.fail_exec = True
    relay_dir = stand.run_dir / 'relay-report'
    relay_dir.mkdir()
    (relay_dir / 'relay-diagnostics.json').write_text(json.dumps({
        'schema_version': 1, 'active': True, 'peak_active': 1,
        'counters': {}, 'timing_ms': {}, 'tcp_listener_counters': None,
    }))
    with pytest.raises(soak.SoakPhaseError, match='synthetic_soak_failed'):
        soak.run_synthetic_stability(stand)
    result = json.loads((stand.run_dir / 'synthetic-failure-diagnostics.json').read_text())
    assert 'relay' not in result


def test_relay_diagnostic_snapshot_rejects_unknown_tcp_counter_fields(tmp_path):
    stand = FakeStand(tmp_path)
    relay_dir = stand.run_dir / 'relay-report'
    relay_dir.mkdir()
    counters = {name: 0 for name in soak.RELAY_DIAGNOSTIC_COUNTERS}
    timing = {name: 0 for name in soak.RELAY_TIMING_FIELDS}
    value = {
        'schema_version': 1, 'active': 0, 'peak_active': 0,
        'counters': counters, 'timing_ms': timing,
        'tcp_listener_counters': {name: 0 for name in soak.TCP_LISTENER_COUNTERS},
        'tcp_connection_counters': {'ActiveOpens': 1, 'private_field': 2},
    }
    (relay_dir / 'relay-diagnostics.json').write_text(json.dumps(value))
    assert soak._relay_diagnostic_snapshot(stand) is None


def test_soak_rejects_changed_browser_generation(tmp_path):
    stand = FakeStand(tmp_path)
    original = stand.docker
    inspect_count = 0
    def docker(*args):
        nonlocal inspect_count
        if args[:2] == ('inspect', '--format'):
            inspect_count += 1
            if inspect_count >= 2:
                stand.generation_change = True
        return original(*args)
    stand.docker = docker
    with pytest.raises(soak.SoakPhaseError, match='runtime_generation_changed'):
        soak.run_synthetic_stability(stand)

