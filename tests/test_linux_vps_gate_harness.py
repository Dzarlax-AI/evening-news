from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import hashlib
import json

from scripts import native_shadow_stand as stand_module
from scripts import run_go_cloak_linux_gate as gate


class BoundedProfileTests(unittest.TestCase):
    def test_worker_recovery_profile_is_short_runtime_only_and_synthetic(self):
        valid = dict(enabled=True, runtime_only=True, short_window=True,
                     news_web_stopped=False, live_quality=False,
                     long_synthetic_soak=False, observe_resource_thresholds=False)
        gate.validate_worker_recovery_options(**valid)
        for field, value in (('runtime_only', False), ('short_window', False),
                             ('news_web_stopped', True), ('live_quality', True),
                             ('long_synthetic_soak', True),
                             ('observe_resource_thresholds', True)):
            invalid = dict(valid)
            invalid[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                    gate.GateError, 'worker_recovery_gate_profile_invalid'):
                gate.validate_worker_recovery_options(**invalid)

    def test_worker_recovery_stand_uses_go_only_contract(self):
        stage = Path('/private/stage')
        args = gate.worker_recovery_stand_args(
            stage, 1234, 'linux-vps-gate=0123456789abcdef')
        self.assertIn('--worker-recovery-gate', args)
        self.assertNotIn('--go-worker-only', args)

    def test_only_marked_exact_owned_worker_exit_is_allowed_in_recovery_phase(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            label = '0123456789abcdef'
            baseline = {'news_web': ['web'], 'infra-chrome': ['chrome']}
            worker = 'news-native-shadow-0123456789-worker'
            supervisor = gate.Supervisor(stage, label, gate.time.monotonic(), baseline,
                                         worker_recovery_gate=True)
            supervisor.fingerprints = lambda: baseline
            supervisor.health_probe = lambda: ('200', .1)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: [worker]
            row = f'/{worker}|false|0|false|0|1|{label}'
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(gate, 'command', return_value=Mock(stdout=row)):
                with self.assertRaisesRegex(gate.GateError, 'owned_container_stopped_restarted_or_oom'):
                    supervisor.guard(expected_phase='native-worker-recovery-gate')
                marker = stage/'.worker-recovery-active'
                marker.write_text('news-native-shadow-1111111111-worker')
                with self.assertRaisesRegex(gate.GateError, 'owned_container_stopped_restarted_or_oom'):
                    supervisor.guard(expected_phase='native-worker-recovery-gate')
                marker.write_text(worker)
                supervisor.guard(expected_phase='native-worker-recovery-gate')
                for bad_row in (row.replace('|false|0|false|0|1|', '|false|1|false|0|1|'),
                                row.replace('|false|0|false|0|1|', '|false|0|true|0|1|'),
                                row.replace('|false|0|false|0|1|', '|true|0|false|0|1|')):
                    with patch.object(gate, 'command', return_value=Mock(stdout=bad_row)):
                        with self.assertRaisesRegex(gate.GateError,
                                'owned_container_stopped_restarted_or_oom'):
                            supervisor.guard(expected_phase='native-worker-recovery-gate')
                with self.assertRaisesRegex(gate.GateError, 'owned_container_stopped_restarted_or_oom'):
                    supervisor.guard()

    def test_sha256_file_reads_bounded_chunks_and_matches_standard_digest(self):
        payload = b'bounded hash input' * (gate.HASH_CHUNK_BYTES // 8) + b'end'
        requested = []

        class Reader:
            def __init__(self):
                self.offset = 0

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, size):
                requested.append(size)
                chunk = payload[self.offset:self.offset + size]
                self.offset += len(chunk)
                return chunk

        with patch.object(Path, 'open', return_value=Reader()):
            result = gate._sha256_file(Path('/bounded/provenance-input'))

        self.assertEqual(result, hashlib.sha256(payload).hexdigest())
        self.assertGreater(len(requested), 1)
        self.assertTrue(all(size == gate.HASH_CHUNK_BYTES for size in requested))

    def test_successful_stand_exit_gets_final_guard_before_cleanup_and_late_oom_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            stage.chmod(0o700)
            baseline = {'news_web': ['web'], 'infra-chrome': ['chrome']}
            supervisor = gate.Supervisor(stage, '0123456789abcdef', gate.time.monotonic(), baseline,
                                         short_window=True, observe_resource_thresholds=True)
            supervisor.fingerprints = lambda: baseline
            supervisor.health_probe = lambda: ('200', .1)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.browser_working_set_bytes = lambda **_kwargs: (
                100, {'cloak': 100})
            supervisor.owned_containers = Mock(side_effect=[[], [
                'news-native-shadow-0123456789-cloak']])
            supervisor.run_command = Mock(return_value=None)
            supervisor.cleanup_label = Mock()
            oom_row = ('/news-native-shadow-0123456789-cloak|true|0|true|123|0|'
                       '0123456789abcdef')

            def command(args, **_kwargs):
                if args[0] == 'curl':
                    return subprocess.CompletedProcess(args, 0, '200', '')
                if args[0] == 'docker' and args[1] == 'inspect':
                    return subprocess.CompletedProcess(args, 0, oom_row, '')
                self.fail(f'unexpected command: {args[0]}')

            with patch.object(gate.sys, 'argv', [
                    'runner', '--stage', str(stage), '--runtime-only', '--short-window',
                    '--observe-resource-thresholds']), \
                    patch.object(gate, 'ensure_stage'), \
                    patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(gate, 'verify_pinned_images'), \
                    patch.object(gate, 'verify_source_manifest', return_value='a'*64), \
                    patch.object(gate, 'verify_runtime_bundle', return_value='b'*64), \
                    patch.object(gate, 'production_fingerprint', side_effect=lambda name: baseline[name]), \
                    patch.object(gate, 'production_user_path', return_value={
                        'home_page_ok': True, 'public_feed_ok': True}), \
                    patch.object(gate, 'run_go_checks'), \
                    patch.object(gate, 'Supervisor', return_value=supervisor), \
                    patch.object(gate, 'command', side_effect=command), \
                    patch.object(gate.signal, 'signal'), \
                    patch.object(gate.subprocess, 'Popen') as popen, \
                    patch('builtins.print'):
                popen.return_value.wait.return_value = 0
                with self.assertRaisesRegex(gate.GateError, 'owned_container_stopped_restarted_or_oom'):
                    gate.main()

            supervisor.run_command.assert_called_once()
            command_args = supervisor.run_command.call_args.args[0]
            self.assertIn('--supervisor-cleanup', command_args)
            supervisor.cleanup_label.assert_called_once()
            summary = json.loads((stage/'summary.json').read_text())
            self.assertEqual(summary['state'], 'failed')
            self.assertEqual(summary['error'], 'owned_container_stopped_restarted_or_oom')

    def test_outer_watchdog_stops_owned_child_group_before_parent_death_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            events = []
            now = gate.time.monotonic()
            def killpg(pgid, sig):
                self.assertEqual(pgid, 1234)
                if sig == gate.signal.SIGTERM:
                    events.append('child_sigterm')
                    return
                if sig == 0:
                    raise ProcessLookupError
                self.fail(f'unexpected signal: {sig}')

            with patch.object(gate, 'watchdog_deadlines', return_value=(now+60, now+120)), \
                    patch.object(gate.os, 'kill', side_effect=ProcessLookupError), \
                    patch.object(gate, 'active_command_pgid', return_value=1234), \
                    patch.object(gate.os, 'killpg', side_effect=killpg), \
                    patch.object(gate, 'watchdog_cleanup', side_effect=lambda *_args: events.append('cleanup')):
                gate.outer_watchdog(stage, '0123456789abcdef', 4321, now)
            self.assertEqual(events, ['child_sigterm', 'cleanup'])

    def test_active_command_group_requires_recorded_session_leader(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            (stage/'.active-command-pgid').write_text('1234\n')
            stat = '1234 (python runner) S 1 1234 1234 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n'
            original_read_text = Path.read_text
            original_iterdir = Path.iterdir
            with patch.object(gate.Path, 'iterdir', autospec=True,
                              side_effect=lambda self: [Path('/proc/1234')] if self == Path('/proc')
                              else original_iterdir(self)), \
                    patch.object(gate.Path, 'read_text', autospec=True, side_effect=lambda self, *args, **kwargs:
                             stat if str(self) == '/proc/1234/stat' else original_read_text(self, *args, **kwargs)):
                self.assertEqual(gate.active_command_pgid(stage), 1234)
            other_group = stat.replace('S 1 1234 1234', 'S 1 5678 5678')
            with patch.object(gate.Path, 'iterdir', autospec=True,
                              side_effect=lambda self: [Path('/proc/1234')] if self == Path('/proc')
                              else original_iterdir(self)), \
                    patch.object(gate.Path, 'read_text', autospec=True, side_effect=lambda self, *args, **kwargs:
                             other_group if str(self) == '/proc/1234/stat' else original_read_text(self, *args, **kwargs)):
                self.assertIsNone(gate.active_command_pgid(stage))

    def test_stand_run_emits_equal_memory_swap_and_expected_security_caps(self):
        stand = object.__new__(stand_module.Stand)
        stand.prefix = 'test-run'
        stand.label = 'native-shadow-run=test-run'
        stand.bounded_profile = True
        stand.supervisor_label = 'linux-vps-gate=0123456789abcdef'
        stand.containers = []
        calls = []

        def docker(*args, **kwargs):
            calls.append(args)
            return 'test-run'

        stand.docker = docker
        name = stand.run('worker', 'image', [], network='private', memory='128m',
                         cpu='0.25', pids=128, entrypoint='worker')
        args = list(calls[0])
        self.assertEqual(name, 'test-run-worker')
        self.assertEqual(args[args.index('--memory')+1], '128m')
        self.assertEqual(args[args.index('--memory-swap')+1], '128m')
        self.assertEqual(args[args.index('--cpus')+1], '0.25')
        self.assertEqual(args[args.index('--pids-limit')+1], '128')
        self.assertIn('--cap-drop=ALL', args)
        self.assertIn('--read-only', args)
        self.assertIn('--no-healthcheck', args)
        self.assertIn('--restart=no', args)
        self.assertIn('--security-opt=no-new-privileges:true', args)
        self.assertIn('linux-vps-gate=0123456789abcdef', args)

    def test_bounded_stand_sets_phase_alarm_for_blocking_operations(self):
        stand = object.__new__(stand_module.Stand)
        stand.bounded_profile = True
        stand.deadline = stand_module.time.time()+90
        with patch.object(stand_module.signal, 'setitimer') as timer:
            stand.begin_phase('synthetic_network_gate', 240)
        self.assertEqual(stand.phase, 'synthetic_network_gate')
        self.assertLessEqual(stand.phase_deadline, stand.deadline)
        self.assertEqual(timer.call_args.args[0], stand_module.signal.ITIMER_REAL)
        self.assertLessEqual(timer.call_args.args[1], 90)

    def test_supervisor_cleanup_requires_bounded_bootstrap_gate_and_exact_label(self):
        valid = {
            'supervisor_cleanup': True, 'bounded_profile': True, 'bootstrap': True,
            'gate_only': True, 'live': False, 'variant': None, 'deadline': 1234,
            'supervisor_label': 'linux-vps-gate=0123456789abcdef',
        }
        stand_module.validate_supervisor_cleanup_options(**valid)
        invalid = (
            ({**valid, 'bounded_profile': False}, 'bounded_profile'),
            ({**valid, 'bootstrap': False}, 'bootstrap'),
            ({**valid, 'gate_only': False, 'live': True}, 'live'),
            ({**valid, 'supervisor_label': None}, 'missing_label'),
            ({**valid, 'supervisor_label': 'linux-vps-gate=bad!'}, 'invalid_label'),
        )
        for options, name in invalid:
            with self.subTest(name=name), self.assertRaisesRegex(
                    stand_module.StandError, 'supervisor_cleanup_requires_bounded_profile_and_label'):
                stand_module.validate_supervisor_cleanup_options(**options)

    def test_supervisor_managed_stand_leaves_resources_for_outer_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            stand = object.__new__(stand_module.Stand)
            stand.supervisor_cleanup = True
            stand.env_file = Path(temp)/'private.env'
            stand.env_file.write_text('private-token')
            stand.docker = Mock(side_effect=AssertionError('stand must not perform Docker teardown'))
            stand.write_manifest = Mock()
            stand.cleanup()
            self.assertFalse(stand.env_file.exists())
            stand.docker.assert_not_called()
            stand.write_manifest.assert_not_called()

    def test_stand_main_cleanup_ownership_on_success_and_failure(self):
        real_stand = stand_module.Stand
        for managed in (False, True):
            for fail_gate in (False, True):
                with self.subTest(managed=managed, fail_gate=fail_gate), \
                        tempfile.TemporaryDirectory() as temp:
                    stage = Path(temp)
                    stage.chmod(0o700)
                    created = []
                    docker_calls = []

                    def construct(*args, **kwargs):
                        instance = real_stand(*args, **kwargs)
                        created.append(instance)
                        return instance

                    def docker(args, **kwargs):
                        docker_calls.append(tuple(args[1:]))
                        if args[1:3] == ['inspect', '--format']:
                            return created[0].prefix
                        if args[1:4] == ['network', 'inspect', '--format']:
                            return created[0].prefix
                        if args[1:3] in (['ps', '-a'], ['network', 'ls']):
                            return ''
                        return ''

                    def setup(instance):
                        instance.containers.append(instance.prefix+'-cloak')
                        instance.networks.append(instance.prefix+'-private')

                    args = ['runner', '--stage', str(stage), '--bootstrap', '--gate-only',
                            '--bounded-profile', '--deadline', '1234']
                    if managed:
                        args += ['--supervisor-label', 'linux-vps-gate=0123456789abcdef',
                                 '--supervisor-cleanup']
                    gate_side_effect = (stand_module.StandError('synthetic_failure')
                                        if fail_gate else None)
                    with patch.object(stand_module.sys, 'argv', args), \
                            patch.object(stand_module, 'Stand', side_effect=construct), \
                            patch.object(real_stand, 'setup', setup), \
                            patch.object(real_stand, 'raw_network_gate'), \
                            patch.object(real_stand, 'start_runtime'), \
                            patch.object(real_stand, 'controlled_browser_gate',
                                         side_effect=gate_side_effect), \
                            patch.object(stand_module, 'command', side_effect=docker), \
                            patch.object(stand_module.signal, 'signal'), \
                            patch.object(stand_module.signal, 'setitimer'), \
                            patch('builtins.print'):
                        if fail_gate:
                            with self.assertRaisesRegex(stand_module.StandError, 'synthetic_failure'):
                                stand_module.main()
                        else:
                            stand_module.main()

                    self.assertEqual(len(created), 1)
                    self.assertFalse(created[0].env_file.exists())
                    rm_calls = [call for call in docker_calls if call[:2] == ('rm', '-f')]
                    if managed:
                        self.assertEqual(rm_calls, [])
                        self.assertTrue(created[0].containers)
                        self.assertFalse((created[0].run_dir/'manifest.json').exists())
                    else:
                        self.assertEqual(rm_calls, [('rm', '-f', created[0].prefix+'-cloak')])
                        manifest = json.loads((created[0].run_dir/'manifest.json').read_text())
                        self.assertEqual(manifest['state'], 'cleaned')

    def test_runner_guard_accepts_docker_inspect_label_value_without_key_prefix(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.2)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: ['0123456789abcdef-linux-race']
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(gate, 'command', return_value=Mock(stdout=
                        '/0123456789abcdef-linux-race|true|0|false|224|0|0123456789abcdef')):
                supervisor.guard(expected_phase='linux-race')
            self.assertEqual({key: supervisor.last_guard_sample[key] for key in (
                'phase', 'mem_available_bytes', 'memory_psi_full_avg10')}, {
                'phase': 'linux-race',
                'mem_available_bytes': 4*1024**3,
                'memory_psi_full_avg10': 0,
            })

    def test_first_rss_guard_failure_sample_survives_successful_cleanup_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.1)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: []
            process_rows = [{'pid': 10, 'comm': 'python3', 'rss_kib': 140000}]
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', side_effect=[140000, 100]), \
                    patch.object(gate, 'proc_tree_rss_rows', return_value=process_rows):
                with self.assertRaisesRegex(gate.GateError, 'host_supervisor_rss_above_observed_limit'):
                    supervisor.guard(expected_phase='live-c-smoke')
                supervisor.guard(expected_phase='live-c-smoke-cleanup')

            self.assertEqual(supervisor.first_guard_failure_sample['phase'], 'live-c-smoke')
            self.assertEqual(supervisor.first_guard_failure_sample['host_supervisor_rss_kib'], 140000)
            self.assertEqual(supervisor.first_guard_failure_sample['host_supervisor_rss_processes'], process_rows)
            self.assertEqual(supervisor.last_guard_sample['phase'], 'live-c-smoke-cleanup')
            self.assertNotIn('host_supervisor_rss_processes', supervisor.last_guard_sample)

    def test_runner_records_actual_pressure_sample_before_guard_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=2500*1024**2), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0.51):
                with self.assertRaisesRegex(gate.GateError, 'memory_psi_pressure'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertEqual({key: supervisor.last_guard_sample[key] for key in (
                'phase', 'mem_available_bytes', 'memory_psi_full_avg10')}, {
                'phase': 'synthetic-browser-gate',
                'mem_available_bytes': 2500*1024**2,
                'memory_psi_full_avg10': 0.51,
            })
            self.assertEqual(len(supervisor.diagnostic_samples), 1)

    def test_worker_recovery_profile_observes_psi_without_bypassing_memory_guards(self):
        with tempfile.TemporaryDirectory() as temp:
            baseline = {}
            supervisor = gate.Supervisor(
                Path(temp), '0123456789abcdef', 1, baseline,
                short_window=True, worker_recovery_gate=True)
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.fingerprints = lambda: baseline
            supervisor.health_probe = lambda: ('200', 0.1)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: []
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0.75), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100):
                supervisor.guard(expected_phase='native-worker-recovery-gate')
            self.assertEqual(supervisor.last_guard_sample['memory_psi_policy'], 'observe_only')
            self.assertEqual(supervisor.memory_psi_threshold_observations, 1)
            self.assertEqual(supervisor.memory_psi_full_avg10_sampled_peak, 0.75)

            supervisor.browser_phase = True
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0.75), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(supervisor, 'browser_working_set_bytes', return_value=(
                        gate.BROWSER_WORKING_SET_BUDGET+1, {'cloak': gate.BROWSER_WORKING_SET_BUDGET+1})):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_above_budget'):
                    supervisor.guard(expected_phase='native-worker-recovery-gate')
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=gate.RESERVE_MEM_MIN-1), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0.75), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100):
                with self.assertRaisesRegex(gate.GateError, 'host_memory_reserve_breached'):
                    supervisor.guard(expected_phase='native-worker-recovery-gate')

    def test_inspect_label_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.2)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: ['someone-elses-container']
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(gate, 'command', return_value=Mock(stdout=
                        '/someone-elses-container|true|0|false|224|0|different-label')):
                with self.assertRaisesRegex(gate.GateError, 'owned_container_stopped_restarted_or_oom'):
                    supervisor.guard(expected_phase='linux-race')

    def test_health_non_200_fails_immediately_and_slow_health_is_counted(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('503', 0.1)
            supervisor.web_memory_bytes = lambda: 10
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0):
                with self.assertRaisesRegex(gate.GateError, 'production_health_non_200'):
                    supervisor.guard()

    def test_build_requires_static_binary_and_gcc(self):
        command = gate.build_commands()[2]
        self.assertIn('command -v gcc', command)
        self.assertIn('CGO_ENABLED=0 go build', command)
        self.assertIn('readelf', command)
        self.assertIn('! readelf', command)

    def test_go_container_routes_compiler_and_test_tempdirs_to_writable_stage(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            supervisor = gate.Supervisor(stage, '0123456789abcdef', 1, {})
            supervisor.run_command = Mock()
            gate.run_go_phase(supervisor, stage, 'linux-race', ['go', 'test'], 900)
            docker_args = supervisor.run_command.call_args.args[0]
            self.assertIn('-e', docker_args)
            env_values = [docker_args[index+1] for index, value in enumerate(docker_args[:-1])
                          if value == '-e']
            self.assertIn('GOTMPDIR=/go-tmp', env_values)
            self.assertIn('TMPDIR=/go-tmp', env_values)
            self.assertIn('-v', docker_args)
            self.assertIn(str(stage/'go-tmp')+':/go-tmp:rw', docker_args)

    def test_go_and_browser_admission_limits_include_reserve_and_supervisor(self):
        self.assertEqual(gate.GO_MEM_MIN, 2432 * 1024**2)
        self.assertEqual(gate.BROWSER_MEM_MIN, 2432 * 1024**2)
        self.assertEqual(gate.BROWSER_WORKING_SET_BUDGET + 128*1024**2 + gate.RESERVE_MEM_MIN,
                         gate.BROWSER_MEM_MIN)

    def test_go_container_uses_768m_memory_and_equal_swap(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            supervisor = gate.Supervisor(stage, '0123456789abcdef', 1, {})
            supervisor.run_command = Mock()
            gate.run_go_phase(supervisor, stage, 'linux-race', ['go', 'test'], 900)
            docker_args = supervisor.run_command.call_args.args[0]
            self.assertEqual(docker_args[docker_args.index('--memory')+1], '768m')
            self.assertEqual(docker_args[docker_args.index('--memory-swap')+1], '768m')

    def test_browser_working_set_budget_is_inclusive_and_above_budget_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.browser_phase = True
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.2)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: []
            common_patches = [
                patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)),
                patch.object(gate, 'mem_available', return_value=2432*1024**2),
                patch.object(gate, 'memory_psi_full_avg10', return_value=0),
                patch.object(gate, 'proc_tree_rss_kib', return_value=100),
            ]
            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], \
                    patch.object(supervisor, 'browser_working_set_bytes', return_value=(
                        gate.BROWSER_WORKING_SET_BUDGET, {'cloak': gate.BROWSER_WORKING_SET_BUDGET})):
                supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertEqual(supervisor.last_guard_sample['runtime_working_set_bytes'],
                             gate.BROWSER_WORKING_SET_BUDGET)
            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], \
                    patch.object(supervisor, 'browser_working_set_bytes', return_value=(
                        gate.BROWSER_WORKING_SET_BUDGET+1, {'cloak': gate.BROWSER_WORKING_SET_BUDGET+1})):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_above_budget'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertEqual(supervisor.last_guard_sample['runtime_working_set_bytes'],
                             gate.BROWSER_WORKING_SET_BUDGET+1)

    def test_resource_observation_records_exceedances_but_keeps_other_guards(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(
                Path(temp), '0123456789abcdef', 1, {}, short_window=True,
                observe_resource_thresholds=True)
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.browser_phase = True
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.2)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: []
            common_patches = [
                patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)),
                patch.object(gate, 'mem_available', return_value=4*1024**3),
                patch.object(gate, 'memory_psi_full_avg10', return_value=0.75),
                patch.object(gate, 'proc_tree_rss_kib', return_value=100),
            ]
            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], \
                    patch.object(supervisor, 'browser_working_set_bytes', return_value=(
                        gate.BROWSER_WORKING_SET_BUDGET+1,
                        {'cloak': gate.BROWSER_WORKING_SET_BUDGET+1})):
                supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertEqual(supervisor.last_guard_sample['runtime_working_set_bytes'],
                             gate.BROWSER_WORKING_SET_BUDGET+1)
            self.assertEqual(supervisor.runtime_working_set_peak_bytes,
                             gate.BROWSER_WORKING_SET_BUDGET+1)
            self.assertEqual(supervisor.memory_psi_full_avg10_sampled_peak, 0.75)
            self.assertEqual(supervisor.resource_threshold_bypass_counts, {
                'runtime_working_set_above_budget': 1,
                'memory_psi_pressure': 1,
            })

            # A diagnostic observation still fails at the independent host-reserve guard.
            with common_patches[0], \
                    patch.object(gate, 'mem_available', return_value=gate.RESERVE_MEM_MIN-1), \
                    common_patches[2], common_patches[3], \
                    patch.object(supervisor, 'browser_working_set_bytes', return_value=(
                        gate.BROWSER_WORKING_SET_BUDGET+1, {'cloak': gate.BROWSER_WORKING_SET_BUDGET+1})):
                with self.assertRaisesRegex(gate.GateError, 'host_memory_reserve_breached'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')

            # It also leaves the production health guard intact.
            supervisor.health_probe = lambda: ('503', 0.2)
            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], \
                    patch.object(supervisor, 'browser_working_set_bytes', return_value=(
                        gate.BROWSER_WORKING_SET_BUDGET+1, {'cloak': gate.BROWSER_WORKING_SET_BUDGET+1})):
                with self.assertRaisesRegex(gate.GateError, 'production_health_non_200'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')

    def test_browser_working_set_missing_metric_fails_closed_and_records_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.browser_phase = True
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(supervisor, 'browser_working_set_bytes', side_effect=
                                 gate.GateError('runtime_working_set_metric_unavailable')):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_metric_unavailable'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertIsNone(supervisor.last_guard_sample['runtime_working_set_bytes'])
            self.assertFalse(supervisor.last_guard_sample['runtime_working_set_metric_valid'])

    def test_browser_metric_rejects_production_container_names(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.owned_containers = lambda: ['news_web', 'infra-chrome']
            with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_metric_unavailable'):
                supervisor.browser_working_set_bytes()

    def test_browser_metric_sums_two_owned_disjoint_cgroups_and_tracks_roles(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            names = [
                'news-native-shadow-abcdef0123-cloak',
                'news-native-shadow-abcdef0123-worker',
            ]
            supervisor.owned_containers = lambda: names
            reads = {
                '/proc/101/cgroup': '0::/docker/a\n',
                '/proc/102/cgroup': '0::/docker/b\n',
                '/sys/fs/cgroup/docker/a/memory.current': '300\n',
                '/sys/fs/cgroup/docker/b/memory.current': '400\n',
                '/sys/fs/cgroup/docker/a/memory.stat': '\n'.join(
                    f'{key} {index + 1}' for index, key in enumerate(gate.MEMORY_STAT_KEYS)) + '\n',
                '/sys/fs/cgroup/docker/b/memory.stat': 'anon 21\nfile 22\nshmem 23\nkernel 24\n'
                    'slab_reclaimable 25\nslab_unreclaimable 26\nsock 27\n'
                    'inactive_file 28\nactive_file 29\n',
            }
            original_read_text = Path.read_text
            def read_text(path, *args, **kwargs):
                if str(path) in reads:
                    return reads[str(path)]
                return original_read_text(path, *args, **kwargs)
            inspect = '\n'.join((
                'a'*64 + '|/' + names[0] + '|101|0123456789abcdef|true|0|false|0',
                'b'*64 + '|/' + names[1] + '|102|0123456789abcdef|true|0|false|0',
            ))
            with patch.object(gate, 'command', return_value=Mock(stdout=inspect)), \
                    patch.object(Path, 'read_text', read_text):
                total, roles = supervisor.browser_working_set_bytes()
            self.assertEqual(total, 700)
            self.assertEqual(roles, {'cloak': 300, 'worker': 400})
            self.assertEqual(supervisor.runtime_container_memory_stats['cloak'], {
                'status': 'ok',
                'values_bytes': {key: index + 1 for index, key in enumerate(gate.MEMORY_STAT_KEYS)},
                'missing_keys': [],
            })
            self.assertEqual(supervisor.runtime_container_memory_stats['worker']['values_bytes'], {
                key: value for key, value in zip(gate.MEMORY_STAT_KEYS, range(21, 30))
            })

    def test_browser_metric_allows_only_exact_marked_stopped_recovery_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            worker = 'news-native-shadow-0123456789-worker'
            supervisor = gate.Supervisor(stage, '0123456789abcdef', 1, {},
                                         worker_recovery_gate=True)
            supervisor.owned_containers = lambda: [worker]
            (stage/'.worker-recovery-active').write_text(worker)
            stopped = ('a'*64 + '|/' + worker + '|0|0123456789abcdef|false|0|false|1')
            with patch.object(gate, 'command', return_value=Mock(stdout=stopped)):
                self.assertEqual(supervisor.browser_working_set_bytes(
                    expected_phase='native-worker-recovery-gate'), (0, {}))

            stopped_cloak = stopped.replace(worker, worker.replace('-worker', '-cloak'))
            supervisor.owned_containers = lambda: [worker.replace('-worker', '-cloak')]
            with patch.object(gate, 'command', return_value=Mock(stdout=stopped_cloak)):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_metric_unavailable'):
                    supervisor.browser_working_set_bytes(
                        expected_phase='native-worker-recovery-gate')

    def test_memory_stat_diagnostic_marks_unavailable_and_malformed_without_raising(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            missing = gate.Supervisor._memory_stat_diagnostic(root/'missing')
            self.assertEqual(missing['status'], 'unavailable')
            self.assertTrue(all(value is None for value in missing['values_bytes'].values()))

            malformed_path = root/'memory.stat'
            malformed_path.write_text('anon 12\nfile not-a-number\n')
            malformed = gate.Supervisor._memory_stat_diagnostic(malformed_path)
            self.assertEqual(malformed['status'], 'malformed')
            self.assertTrue(all(value is None for value in malformed['values_bytes'].values()))

            partial_path = root/'partial-memory.stat'
            partial_path.write_text('anon 12\nfile 34\n')
            partial = gate.Supervisor._memory_stat_diagnostic(partial_path)
            self.assertEqual(partial['status'], 'partial')
            self.assertEqual(partial['values_bytes']['anon'], 12)
            self.assertEqual(partial['values_bytes']['file'], 34)
            self.assertIsNone(partial['values_bytes']['kernel'])
            self.assertIn('kernel', partial['missing_keys'])

    def test_pressure_diagnostic_reports_missing_and_malformed_without_raising(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertEqual(gate.Supervisor._pressure_diagnostic(root/'missing'), {
                'status': 'unavailable', 'some': None, 'full': None,
            })
            malformed = root/'pressure'
            malformed.write_text('some avg10=nan avg60=0.2 avg300=0.3 total=12\n')
            self.assertEqual(gate.Supervisor._pressure_diagnostic(malformed)['status'], 'malformed')
            malformed.write_text('some avg10=0.1 avg60=0.2 avg300=0.3 total=12\n')
            partial = gate.Supervisor._pressure_diagnostic(malformed)
            self.assertEqual(partial['status'], 'partial')
            self.assertEqual(partial['some']['total'], 12)
            self.assertIsNone(partial['full'])

    def test_cgroup_diagnostic_totals_are_interval_deltas_only_for_same_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            cgroup = Path(temp)
            pressure = cgroup/'memory.pressure'
            events = cgroup/'memory.events'
            pressure.write_text('some avg10=0.1 avg60=0.2 avg300=0.3 total=100\n'
                                'full avg10=0.0 avg60=0.0 avg300=0.0 total=20\n')
            events.write_text('low 0\nhigh 2\nmax 3\nome 0\nome_kill 0\n')
            for name, value in (('memory.current', '5'), ('memory.peak', '8'), ('memory.max', 'max'),
                                ('memory.swap.current', '1'), ('memory.swap.max', 'max'),
                                ('memory.high', 'max')):
                (cgroup/name).write_text(value)
            first = supervisor._cgroup_diagnostic('cloak', 123, 'a'*64, '/docker/a', cgroup)
            self.assertEqual(first['delta_status'], 'baseline')
            self.assertIsNone(first['deltas']['pressure_some'])
            self.assertEqual(first['pressure']['full']['total'], 20)
            self.assertEqual(first['events']['values']['high'], 2)
            self.assertEqual(first['limits']['memory_peak_bytes'], 8)
            pressure.write_text('some avg10=0.2 avg60=0.2 avg300=0.3 total=160\n'
                                'full avg10=0.0 avg60=0.0 avg300=0.0 total=35\n')
            events.write_text('low 0\nhigh 4\nmax 3\nome 0\nome_kill 0\n')
            second = supervisor._cgroup_diagnostic('cloak', 123, 'a'*64, '/docker/a', cgroup)
            self.assertEqual(second['delta_status'], 'observed')
            self.assertEqual(second['deltas']['pressure_some'], 60)
            self.assertEqual(second['deltas']['event_high'], 2)
            replaced = supervisor._cgroup_diagnostic('cloak', 124, 'b'*64, '/docker/b', cgroup)
            self.assertEqual(replaced['delta_status'], 'baseline')
            self.assertIsNone(replaced['deltas']['pressure_some'])

    def test_diagnostic_collection_failure_does_not_change_psi_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=2500*1024**2), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0.51), \
                    patch.object(supervisor, '_host_pressure_diagnostic', side_effect=OSError('diagnostic')):
                with self.assertRaisesRegex(gate.GateError, 'memory_psi_pressure'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertEqual(supervisor.last_guard_sample['memory_psi_full_avg10'], 0.51)
            self.assertEqual(supervisor.last_guard_sample['diagnostics']['status'], 'unavailable')
            self.assertEqual(len(supervisor.diagnostic_samples), 1)

    def test_guard_keeps_rolling_pre_failure_diagnostics_and_marks_actual_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.1)
            supervisor.web_memory_bytes = lambda: 10
            supervisor.owned_containers = lambda: []
            host_totals = iter((100, 130, 190))
            def host_diagnostic():
                total = next(host_totals)
                return {'sequence': total, 'pressure': {'full': {'total': total}},
                        'vmstat': {}, 'deltas': {}, 'delta_status': 'observed'}
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=2500*1024**2), \
                    patch.object(gate, 'memory_psi_full_avg10', side_effect=(0.1, 0.2, 0.51)), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(supervisor, '_host_pressure_diagnostic', side_effect=host_diagnostic):
                supervisor.guard(expected_phase='sample-one')
                supervisor.guard(expected_phase='sample-two')
                with self.assertRaisesRegex(gate.GateError, 'memory_psi_pressure'):
                    supervisor.guard(expected_phase='psi-failure')
            self.assertEqual(len(supervisor.diagnostic_samples), 3)
            self.assertEqual([sample['diagnostics']['host']['sequence']
                              for sample in supervisor.diagnostic_samples], [100, 130, 190])
            self.assertEqual(supervisor.first_guard_failure_sample['phase'], 'psi-failure')
            self.assertEqual(supervisor.first_guard_failure_sample['diagnostics']['host']['sequence'], 190)

    def test_guard_keeps_charged_total_limit_when_memory_stat_is_unavailable(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.deadline = gate.time.monotonic() + 100
            supervisor.browser_phase = True
            supervisor.runtime_container_memory_stats = {'cloak': {
                'status': 'unavailable',
                'values_bytes': {key: None for key in gate.MEMORY_STAT_KEYS},
            }}
            supervisor.fingerprints = lambda: {}
            supervisor.health_probe = lambda: ('200', 0.2)
            supervisor.web_memory_bytes = lambda: 10
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100), \
                    patch.object(supervisor, 'browser_working_set_bytes',
                                 return_value=(gate.BROWSER_WORKING_SET_BUDGET+1, {'cloak': gate.BROWSER_WORKING_SET_BUDGET+1})):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_above_budget'):
                    supervisor.guard(expected_phase='synthetic-browser-gate')
            self.assertEqual(supervisor.last_guard_sample['runtime_working_set_bytes'],
                             gate.BROWSER_WORKING_SET_BUDGET+1)
            self.assertEqual(supervisor.last_guard_sample['runtime_memory_stat_bytes']['cloak']['status'],
                             'unavailable')

    def test_browser_metric_fails_for_missing_cgroup_value_wrong_label_and_overlap(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            names = [
                'news-native-shadow-abcdef0123-cloak',
                'news-native-shadow-abcdef0123-worker',
            ]
            supervisor.owned_containers = lambda: names
            original_read_text = Path.read_text
            def read_text(path, *args, **kwargs):
                values = {
                    '/proc/101/cgroup': '0::/docker/a\n',
                    '/proc/102/cgroup': '0::/docker/b\n',
                }
                if str(path) in values:
                    return values[str(path)]
                if str(path).startswith('/sys/fs/cgroup/docker/'):
                    raise FileNotFoundError(str(path))
                return original_read_text(path, *args, **kwargs)
            inspect = '\n'.join((
                'a'*64 + '|/' + names[0] + '|101|0123456789abcdef|true|0|false|0',
                'b'*64 + '|/' + names[1] + '|102|0123456789abcdef|true|0|false|0',
            ))
            with patch.object(gate, 'command', return_value=Mock(stdout=inspect)), \
                    patch.object(Path, 'read_text', read_text):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_metric_unavailable'):
                    supervisor.browser_working_set_bytes()

            wrong_label = inspect.replace('0123456789abcdef', 'fedcba9876543210')
            with patch.object(gate, 'command', return_value=Mock(stdout=wrong_label)), \
                    patch.object(Path, 'read_text', read_text):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_metric_unavailable'):
                    supervisor.browser_working_set_bytes()

            overlap_reads = {
                '/proc/101/cgroup': '0::/docker/a\n',
                '/proc/102/cgroup': '0::/docker/a/child\n',
            }
            def overlap_read_text(path, *args, **kwargs):
                if str(path) in overlap_reads:
                    return overlap_reads[str(path)]
                return original_read_text(path, *args, **kwargs)
            with patch.object(gate, 'command', return_value=Mock(stdout=inspect)), \
                    patch.object(Path, 'read_text', overlap_read_text):
                with self.assertRaisesRegex(gate.GateError, 'runtime_working_set_metric_unavailable'):
                    supervisor.browser_working_set_bytes()

    def test_runtime_only_skips_go_phases_and_go_image_inspection(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            with patch.object(gate, 'run_go_phase') as run_phase:
                gate.run_go_checks(supervisor, Path(temp), runtime_only=True)
            run_phase.assert_not_called()
        calls = []
        def fake_command(args, **kwargs):
            calls.append(args[-1])
            image_id = gate.APP_IMAGE_ID if args[-1] == gate.APP_IMAGE else gate.CLOAK_IMAGE_ID
            return Mock(stdout=image_id+'|linux|amd64')
        with patch.object(gate, 'command', side_effect=fake_command):
            gate.verify_pinned_images(runtime_only=True)
        self.assertEqual(set(calls), {gate.APP_IMAGE, gate.CLOAK_IMAGE})
        self.assertNotIn(gate.GO_IMAGE, calls)

    def test_runtime_bundle_verifies_exact_source_and_binary_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            stage.chmod(0o700)
            source_rows = {}
            go_root = stage/'benchmark/go'
            go_root.mkdir(parents=True)
            source_names = [f'benchmark/go/source-{index:02d}.go' for index in range(52)]
            source_names += ['benchmark/go/go.mod', 'benchmark/go/go.sum']
            for name in source_names:
                item = stage/name
                item.write_text(name+'\n')
                source_rows[name] = hashlib.sha256(item.read_bytes()).hexdigest()
            (stage/'source-manifest.json').write_text(json.dumps({
                'base_commit': gate.BASE_COMMIT, 'source_sha256': source_rows,
            }))
            binaries = {}
            binary_root = stage/'bin-soak'
            binary_root.mkdir()
            for name in ('extract-service', 'extract-egress'):
                item = binary_root/name
                item.write_bytes(('binary-'+name).encode())
                item.chmod(0o700)
                binaries['bin-soak/'+name] = hashlib.sha256(item.read_bytes()).hexdigest()
            bundle = {
                'base_commit': gate.BASE_COMMIT,
                'go_source_sha256': source_rows,
                'binaries': binaries,
            }
            manifest_path = stage/gate.BINARY_MANIFEST_NAME
            manifest_path.write_text(json.dumps(bundle))
            bundle_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            with patch.object(gate, 'RUNTIME_BUNDLE_MANIFEST_SHA256', bundle_sha256):
                digest = gate.verify_runtime_bundle(stage)
            self.assertEqual(digest, hashlib.sha256((stage/gate.BINARY_MANIFEST_NAME).read_bytes()).hexdigest())
            (binary_root/'extract-service').write_bytes(b'mutated')
            with patch.object(gate, 'RUNTIME_BUNDLE_MANIFEST_SHA256', bundle_sha256):
                with self.assertRaisesRegex(gate.GateError, 'runtime_bundle_binary_hash_mismatch'):
                    gate.verify_runtime_bundle(stage)
            (stage/'benchmark/go/source-00.go').write_text('mutated source\n')
            with patch.object(gate, 'RUNTIME_BUNDLE_MANIFEST_SHA256', bundle_sha256):
                with self.assertRaisesRegex(gate.GateError, 'runtime_bundle_source_hash_mismatch'):
                    gate.verify_runtime_bundle(stage)

    def test_runtime_only_stage_does_not_require_go_caches(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            stage.chmod(0o700)
            for path in ('benchmark/go', 'benchmark/scripts', 'bin-soak', 'tls'):
                (stage/path).mkdir(parents=True)
            (stage/'benchmark/go/go.mod').write_text('module fixture\n')
            (stage/'benchmark/scripts/native_shadow_stand.py').write_text('')
            (stage/'benchmark/scripts/shadow_cdp_relay.py').write_text('')
            (stage/gate.BINARY_MANIFEST_NAME).write_text('{}')
            gate.ensure_stage(stage, runtime_only=True)
            self.assertFalse((stage/'gopath').exists())
            self.assertFalse((stage/'go-cache').exists())
            self.assertFalse((stage/'go-tmp').exists())

    def test_outer_watchdog_reserves_two_minutes_for_cleanup(self):
        started = 1234.5
        abort_at, hard_deadline = gate.watchdog_deadlines(started)
        self.assertEqual(abort_at-started, 28*60)
        self.assertEqual(hard_deadline-started, 30*60)

    def test_maintenance_watchdog_allows_six_minutes_active_and_two_for_cleanup(self):
        started = 1234.5
        active, hard = gate.watchdog_deadlines(started, maintenance_profile=True)
        self.assertEqual(active-started, 6*60)
        self.assertEqual(hard-started, 8*60)
        self.assertEqual(gate.watchdog_deadlines(started), (started+28*60, started+30*60))

    def test_maintenance_flags_require_runtime_mode_and_exact_container_id(self):
        container_id = 'a'*64
        gate.validate_maintenance_options(True, True, container_id)
        gate.validate_maintenance_options(True, False, None)
        gate.validate_maintenance_options(True, False, None, short_window=True)
        cases = [
            (False, True, container_id, 'maintenance_requires_runtime_only'),
            (True, True, None, 'maintenance_container_id_required'),
            (True, False, container_id, 'maintenance_container_id_required'),
            (True, True, 'short', 'maintenance_container_id_invalid'),
        ]
        for runtime_only, stopped, identity, expected in cases:
            with self.subTest(expected=expected), self.assertRaisesRegex(gate.GateError, expected):
                gate.validate_maintenance_options(runtime_only, stopped, identity)
        with self.assertRaisesRegex(gate.GateError, 'short_window_requires_runtime_only'):
            gate.validate_maintenance_options(False, False, None, short_window=True)
        with self.assertRaisesRegex(gate.GateError, 'short_window_conflicts_with_news_web_stopped'):
            gate.validate_maintenance_options(True, True, container_id, short_window=True)
        with self.assertRaisesRegex(gate.GateError, 'observe_resource_thresholds_requires_runtime_only'):
            gate.validate_maintenance_options(False, False, None, observe_resource_thresholds=True)
        with self.assertRaisesRegex(gate.GateError, 'observe_resource_thresholds_requires_short_window'):
            gate.validate_maintenance_options(True, False, None, observe_resource_thresholds=True)
        with self.assertRaisesRegex(gate.GateError, 'observe_resource_thresholds_conflicts_with_news_web_stopped'):
            gate.validate_maintenance_options(True, True, container_id, short_window=True,
                                              observe_resource_thresholds=True)
        with self.assertRaisesRegex(gate.GateError, 'observe_resource_thresholds_conflicts_with_news_web_stopped'):
            gate.validate_maintenance_options(True, True, container_id,
                                              observe_resource_thresholds=True)
        gate.validate_maintenance_options(True, False, None, short_window=True,
                                          observe_resource_thresholds=True)

    def test_short_window_changes_only_deadline_and_keeps_normal_guards(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            started = gate.time.monotonic()
            supervisor = gate.Supervisor(stage, '0123456789abcdef', started, {}, short_window=True)
            self.assertFalse(supervisor.maintenance_web_stopped)
            self.assertAlmostEqual(supervisor.deadline-started, 8*60, delta=1)
            baseline = {'news_web': ['web-running'], 'infra-chrome': ['chrome-running']}
            supervisor.baseline = baseline
            supervisor.fingerprints = lambda: baseline
            supervisor.owned_containers = lambda: []
            supervisor.health_probe = Mock(return_value=('200', .1))
            supervisor.web_memory_bytes = Mock(return_value=10)
            supervisor.deadline = gate.time.monotonic()+100
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100):
                supervisor.guard(expected_phase='synthetic-browser-gate')
            supervisor.health_probe.assert_called_once()
            supervisor.web_memory_bytes.assert_called_once()

    def test_stopped_web_fingerprint_requires_exact_live_container_identity_and_clean_exit(self):
        identity = 'a'*64
        good = '|'.join((identity, gate.APP_IMAGE_ID, '2026-10-17T00:00:00Z', '3',
                         'exited', 'false', '0', '143', 'false'))
        with patch.object(gate, 'command', return_value=Mock(stdout=good)):
            self.assertEqual(gate.stopped_web_fingerprint(identity), good.split('|'))
        for row, expected in (
            (good.replace('|exited|false|0|143|', '|running|true|42|0|'),
             'maintenance_web_unexpectedly_running'),
            (good.replace('|143|false', '|137|false'), 'maintenance_web_stop_state_invalid'),
            (good.replace('|143|false', '|143|true'), 'maintenance_web_stop_state_invalid'),
            (good.replace(identity, 'b'*64), 'maintenance_web_container_identity_changed'),
            (good.replace(gate.APP_IMAGE_ID, 'sha256:'+'b'*64),
             'maintenance_web_container_identity_changed'),
        ):
            with patch.object(gate, 'command', return_value=Mock(stdout=row)):
                with self.assertRaisesRegex(gate.GateError, expected):
                    gate.stopped_web_fingerprint(identity)

    def test_default_production_fingerprint_rejects_intentionally_stopped_web(self):
        stopped = '|'.join(('a'*64, gate.APP_IMAGE_ID, '2026-10-17T00:00:00Z', '0',
                            'exited', 'false'))
        with patch.object(gate, 'command', return_value=Mock(stdout=stopped)):
            with self.assertRaisesRegex(gate.GateError, 'production_fingerprint_invalid'):
                gate.production_fingerprint('news_web')

    def test_maintenance_baseline_binds_stopped_state_to_pre_stop_fingerprint(self):
        identity = 'a'*64
        running = [identity, gate.APP_IMAGE_ID, '2026-10-17T00:00:00Z', '3', 'running', 'false', 5, 2]
        stopped = [identity, gate.APP_IMAGE_ID, running[2], running[3],
                   'exited', 'false', '0', '143', 'false']
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            (stage/'maintenance-baseline.json').write_text(json.dumps({'news_web': running}))
            self.assertEqual(gate.validate_maintenance_baseline(stage, stopped, identity), running)
            (stage/'maintenance-baseline.json').write_text(json.dumps({'news_web': running[:-1]}))
            with self.assertRaisesRegex(gate.GateError, 'maintenance_baseline_invalid'):
                gate.validate_maintenance_baseline(stage, stopped, identity)
            (stage/'maintenance-baseline.json').write_text(json.dumps({'news_web': running}))
            changed = list(stopped)
            changed[2] = 'different-start-time'
            with self.assertRaisesRegex(gate.GateError, 'maintenance_web_baseline_mismatch'):
                gate.validate_maintenance_baseline(stage, changed, identity)
            for index, value, expected in (
                    (0, 'b'*64, 'maintenance_web_baseline_mismatch'),
                    (1, 'sha256:'+'b'*64, 'maintenance_web_baseline_mismatch'),
                    (3, '4', 'maintenance_web_baseline_mismatch')):
                changed = list(stopped)
                changed[index] = value
                with self.subTest(index=index), self.assertRaisesRegex(
                        gate.GateError, expected):
                    gate.validate_maintenance_baseline(stage, changed, identity)

    def test_maintenance_guard_skips_only_web_health_and_memory_checks(self):
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            baseline = {'news_web': ['web-stopped'], 'infra-chrome': ['chrome-running']}
            supervisor = gate.Supervisor(stage, '0123456789abcdef', 1, baseline,
                maintenance_web_stopped=True, maintenance_container_id='a'*64)
            supervisor.deadline = gate.time.monotonic()+100
            supervisor.fingerprints = lambda: baseline
            supervisor.owned_containers = lambda: []
            supervisor.health_probe = Mock(side_effect=AssertionError('web health must be skipped'))
            supervisor.web_memory_bytes = Mock(side_effect=AssertionError('web memory must be skipped'))
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(gate, 'proc_tree_rss_kib', return_value=100):
                supervisor.guard(expected_phase='synthetic-browser-gate')
            supervisor.health_probe.assert_not_called()
            supervisor.web_memory_bytes.assert_not_called()

    def test_maintenance_guard_rejects_early_web_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {},
                maintenance_web_stopped=True, maintenance_container_id='a'*64)
            supervisor.deadline = gate.time.monotonic()+100
            with patch.object(gate.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
                    patch.object(gate, 'mem_available', return_value=4*1024**3), \
                    patch.object(gate, 'memory_psi_full_avg10', return_value=0), \
                    patch.object(supervisor, 'fingerprints', side_effect=
                                 gate.GateError('maintenance_web_unexpectedly_running')):
                with self.assertRaisesRegex(gate.GateError, 'maintenance_web_unexpectedly_running'):
                    supervisor.guard()

    def test_detached_watchdog_cleanup_rechecks_exact_label_value(self):
        def result(stdout='', returncode=0):
            return subprocess.CompletedProcess([], returncode, stdout, '')

        calls = []
        def run(args, **kwargs):
            calls.append(args)
            if args[1:3] == ['ps', '-a']:
                return result('owned-container\n')
            if args[1:3] == ['inspect', '--format']:
                return result('0123456789abcdef\n')
            return result()

        with patch.object(gate.subprocess, 'run', side_effect=run):
            gate.watchdog_cleanup('0123456789abcdef', seconds=2)
        self.assertTrue(any(args[1:3] == ['rm', '-f'] for args in calls))
        self.assertFalse(any('-t' in args for args in calls))
        inspections = [args for args in calls if args[1:3] == ['inspect', '--format']]
        self.assertIn('owned-container', inspections[0])

        calls.clear()
        def foreign_run(args, **kwargs):
            calls.append(args)
            if args[1:3] == ['ps', '-a']:
                return result('foreign-container\n')
            if args[1:3] == ['inspect', '--format']:
                return result('different-label\n')
            return result()
        with patch.object(gate.subprocess, 'run', side_effect=foreign_run):
            gate.watchdog_cleanup('0123456789abcdef', seconds=2)
        self.assertFalse(any(args[1:3] == ['rm', '-f'] for args in calls))

    def test_supervisor_cleanup_uses_supported_rm_flags_and_exact_label(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            containers = ['owned-container']
            supervisor.owned_containers = lambda: list(containers)
            subprocess_calls = []
            network_lists = 0

            def command(args, *, timeout=8, check=True):
                nonlocal network_lists
                if args[1:3] == ['inspect', '--format']:
                    return subprocess.CompletedProcess(args, 0, '0123456789abcdef\n', '')
                if args[1:3] == ['network', 'inspect']:
                    return subprocess.CompletedProcess(args, 0, '0123456789abcdef\n', '')
                if args[1:3] == ['network', 'ls']:
                    network_lists += 1
                    output = 'owned-network\n' if network_lists == 1 else ''
                    return subprocess.CompletedProcess(args, 0, output, '')
                if args[1:3] == ['ps', '-a']:
                    return subprocess.CompletedProcess(args, 0, '', '')
                self.fail(f'unexpected command: {args}')

            def run(args, **kwargs):
                subprocess_calls.append(args)
                self.assertNotIn('-t', args)
                return subprocess.CompletedProcess(args, 0, '', '')

            with patch.object(gate, 'command', side_effect=command), \
                    patch.object(gate.subprocess, 'run', side_effect=run):
                # First owned-container query sees the one resource; the final
                # verification sees none after the valid rm command.
                query = iter((['owned-container'], []))
                supervisor.owned_containers = lambda: next(query, [])
                supervisor.cleanup_label()
            self.assertEqual(subprocess_calls, [['docker', 'rm', '-f', 'owned-container'],
                                                ['docker', 'network', 'rm', 'owned-network']])

    def test_supervisor_cleanup_skips_only_confirmed_absent_container_race(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.owned_containers = lambda: ['vanished-container']
            calls = []

            def command(args, *, timeout=8, check=True):
                calls.append(args)
                if args[1:3] == ['inspect', '--format']:
                    return subprocess.CompletedProcess(args, 1, '', 'not found')
                if args[1:3] == ['ps', '-a']:
                    return subprocess.CompletedProcess(args, 0, '', '')
                if args[1:3] == ['network', 'ls']:
                    return subprocess.CompletedProcess(args, 0, '', '')
                self.fail(f'unexpected command: {args}')

            with patch.object(gate, 'command', side_effect=command), \
                    patch.object(gate.subprocess, 'run') as run:
                query = iter((['vanished-container'], []))
                supervisor.owned_containers = lambda: next(query, [])
                supervisor.cleanup_label()
            run.assert_not_called()
            self.assertTrue(any(args[1:3] == ['ps', '-a'] for args in calls))

    def test_supervisor_cleanup_does_not_treat_present_inspect_failure_as_absence(self):
        with tempfile.TemporaryDirectory() as temp:
            supervisor = gate.Supervisor(Path(temp), '0123456789abcdef', 1, {})
            supervisor.owned_containers = lambda: ['still-present']

            def command(args, *, timeout=8, check=True):
                if args[1:3] == ['inspect', '--format']:
                    return subprocess.CompletedProcess(args, 1, '', 'inspect error')
                if args[1:3] == ['ps', '-a']:
                    return subprocess.CompletedProcess(args, 0, 'still-present\n', '')
                self.fail(f'unexpected command: {args}')

            with patch.object(gate, 'command', side_effect=command), \
                    patch.object(gate.subprocess, 'run') as run:
                with self.assertRaisesRegex(gate.GateError, 'cleanup_container_inspect_failed'):
                    supervisor.cleanup_label()
            run.assert_not_called()

    def test_stand_cleanup_uses_supported_rm_flags_and_preserves_ownership(self):
        with tempfile.TemporaryDirectory() as temp:
            stand = object.__new__(stand_module.Stand)
            stand.bounded_profile = True
            stand.supervisor_cleanup = False
            stand.prefix = 'owned-prefix'
            stand.label = 'native-shadow-run=owned-prefix'
            stand.supervisor_label = 'linux-vps-gate=0123456789abcdef'
            stand.containers = ['owned-prefix-worker']
            stand.networks = ['owned-prefix-private']
            stand.env_file = Path(temp)/'private.env'
            stand.env_file.write_text('temporary')
            stand.write_manifest = Mock()
            calls = []

            def docker(*args, **kwargs):
                calls.append(args)
                if args[:2] == ('inspect', '--format'):
                    return 'owned-prefix'
                if args[:3] == ('network', 'inspect', '--format'):
                    return 'owned-prefix'
                if args[:2] == ('ps', '-a') or args[:2] == ('network', 'ls'):
                    return ''
                return ''

            stand.docker = docker
            stand.cleanup()
            rm_calls = [args for args in calls if args[0] == 'rm']
            self.assertEqual(rm_calls, [('rm', '-f', 'owned-prefix-worker')])
            self.assertFalse(any('-t' in args for args in calls))

    def test_cleanup_error_summary_keeps_only_safe_gate_code(self):
        self.assertEqual(gate.safe_error_name(gate.GateError('owned_resources_remain')),
                         'owned_resources_remain')
        self.assertEqual(gate.safe_error_name(gate.GateError('token=secret')), 'GateError')
        self.assertEqual(gate.safe_error_name(ValueError('unsafe detail')), 'ValueError')


if __name__ == '__main__':
    unittest.main()
