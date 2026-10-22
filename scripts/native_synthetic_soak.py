"""Run and verify one isolated native-C browser stability soak."""

from __future__ import annotations

import hashlib
import ipaddress
import json
from pathlib import Path
import re


SOAK_CASE_IDS = (
    'article-js-cookie', 'redirect-article', 'slow-article', 'large-dom', 'challenge-page',
)
SOAK_DURATION_SECONDS = 600
SOAK_CYCLES = 1
SOAK_IDLE_SECONDS = (5,)
CGROUP_ROLES = ('worker', 'cloak', 'gateway')
APP_IMAGE = 'ghcr.io/dzarlax-ai/rss-summarizer@sha256:b6abbb428543e846d0310b86ff41236a6df028985a00ebfe4714357d22e67060'
NATIVE_DIAGNOSTIC_LINE = re.compile(
    r'^(?:[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} )?'
    r'native_browser_diagnostic stage=(readiness|discovery|websocket|events|create_context|create_target|'
    r'attach_target|configure|navigate|dom|cleanup) reason=(context_timeout|context_cancelled|'
    r'event_overflow|ownership_loss|client_disconnected|malformed_detach|owned_detach|target_crashed|'
    r'unsupported|protocol_error(?:_-?[0-9]{1,6})?|'
    r'transport_error|operation_failed)$')
RELAY_DIAGNOSTIC_COUNTERS = (
    'accepted', 'peer_rejected', 'capacity_rejected', 'connect_attempted', 'connected',
    'connect_timeout', 'connect_error', 'protocol_error', 'stream_timeout',
    'transport_error', 'relay_error', 'websocket_upgrade', 'websocket_rejected', 'closed',
)
RELAY_TIMING_FIELDS = ('connect_total', 'connect_max', 'connection_total', 'connection_max')
TCP_LISTENER_COUNTERS = ('ListenOverflows', 'ListenDrops', 'TCPReqQFullDrop')
TCP_CONNECTION_COUNTERS = ('ActiveOpens', 'AttemptFails', 'RetransSegs', 'InErrs', 'OutRsts')
BROWSER_LOOPBACK_COUNTERS = (
    'diag_loopback5100_invalid_syn', 'diag_loopback5100_invalid_synack',
    'diag_loopback5100_fallback_syn', 'diag_loopback5100_fallback_relay_uid_syn',
    'diag_loopback5100_fallback_synack', 'diag_loopback5100_fallback_cloak_uid_synack',
    'diag_loopback5100_accept_relay_uid_syn', 'diag_loopback5100_accept_cloak_uid_synack',
    'diag_loopback5100_input_invalid_syn', 'diag_loopback5100_input_invalid_synack',
    'diag_loopback5100_input_fallback_syn', 'diag_loopback5100_input_fallback_synack',
    'diag_loopback5100_input_accept_syn', 'diag_loopback5100_input_accept_synack',
)
RELAY_CPU_STAT_FIELDS = ('usage_usec', 'nr_periods', 'nr_throttled', 'throttled_usec')
MAX_RELAY_COUNTER = (1 << 63) - 1
CONTAINER_INSPECT_FORMAT = (
    '{{.Name}}|{{.Id}}|{{.State.StartedAt}}|{{.RestartCount}}|'
    '{{.State.OOMKilled}}|{{.State.Running}}|{{index .Config.Labels "native-shadow-run"}}'
)


def _valid_browser_loopback_counters(value) -> bool:
    return (isinstance(value, dict) and set(value) == set(BROWSER_LOOPBACK_COUNTERS)
            and all(type(value[key]) is int and 0 <= value[key] <= MAX_RELAY_COUNTER
                    for key in BROWSER_LOOPBACK_COUNTERS))


def _loopback_counter_baseline(run_dir: Path):
    """Read only the bounded, finite baseline captured after the raw network gate."""
    try:
        path = run_dir / 'raw-network-gate.json'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 32768:
            return None
        evidence = json.loads(path.read_text(encoding='utf-8'))
        counters = evidence.get('browser_loopback_nft_counters_after_gate')
        return dict(counters) if _valid_browser_loopback_counters(counters) else None
    except (OSError, ValueError, TypeError, AttributeError):
        return None


class SoakPhaseError(RuntimeError):
    """A finite, safe reason the synthetic stability phase did not complete."""


def _safe_cgroup_path(value: str) -> bool:
    return (isinstance(value, str) and value.startswith('/host-cg/')
            and '..' not in Path(value).parts
            and re.fullmatch(r'/host-cg/[A-Za-z0-9_./-]+', value) is not None)


def _runtime_snapshot(stand) -> dict:
    stand.check_runtime()
    names = list(stand.containers)
    if not names or len(names) != len(set(names)):
        raise SoakPhaseError('runtime_generation_unavailable')
    rows = stand.docker('inspect', '--format', CONTAINER_INSPECT_FORMAT, *names).splitlines()
    if len(rows) != len(names):
        raise SoakPhaseError('runtime_generation_unavailable')
    generations = {}
    for row in rows:
        fields = row.split('|')
        if len(fields) != 7:
            raise SoakPhaseError('runtime_generation_unavailable')
        name, ident, started, restarts, oom, running, owner = fields
        name = name.lstrip('/')
        if (name not in names or name in generations or not ident or not started
                or restarts != '0' or oom != 'false' or running != 'true'
                or owner != stand.prefix):
            raise SoakPhaseError('runtime_generation_changed')
        generations[name] = (ident, started)
    launch_info = Path(stand.run_dir) / 'browser-info' / 'launch-info.json'
    if launch_info.is_symlink() or not launch_info.is_file():
        raise SoakPhaseError('browser_generation_unavailable')
    browser_info_sha256 = hashlib.sha256(launch_info.read_bytes()).hexdigest()
    try:
        browser_ip = ipaddress.ip_address(stand.anchors['browser']['ip'])
    except (KeyError, ValueError, TypeError):
        raise SoakPhaseError('browser_generation_unavailable') from None
    if not browser_ip.is_private:
        raise SoakPhaseError('browser_generation_unavailable')
    script = '''import hashlib,json,re,sys,urllib.request,urllib.parse
endpoint=sys.argv[1]
with urllib.request.urlopen(endpoint,timeout=2) as response: value=json.load(response)
ws=value.get('webSocketDebuggerUrl','')
parsed=urllib.parse.urlsplit(ws)
match=re.fullmatch(r'/devtools/browser/([A-Za-z0-9-]{8,80})',parsed.path)
expected=urllib.parse.urlsplit(endpoint)
if parsed.scheme!='ws' or parsed.hostname!=expected.hostname or parsed.port!=9222 or not match: raise ValueError()
print(hashlib.sha256(match.group(1).encode()).hexdigest())
'''
    endpoint = 'http://' + str(browser_ip) + ':9222/json/version'
    try:
        cdp_generation_sha256 = stand.docker('exec', stand.observer, 'python', '-c', script, endpoint).strip()
    except Exception:
        raise SoakPhaseError('browser_generation_unavailable') from None
    if not re.fullmatch(r'[0-9a-f]{64}', cdp_generation_sha256):
        raise SoakPhaseError('browser_generation_unavailable')
    return {'container_count': len(generations), 'generations': generations,
            'browser_info_sha256': browser_info_sha256,
            'cdp_generation_sha256': cdp_generation_sha256}


def _cgroup_snapshot(stand) -> dict:
    groups = {'worker': stand.worker, 'cloak': stand.cloak, 'gateway': stand.gateway}
    paths = {role: stand.cgroup(name) for role, name in groups.items()}
    if set(paths) != set(CGROUP_ROLES) or any(not _safe_cgroup_path(path) for path in paths.values()):
        raise SoakPhaseError('cgroup_path_invalid')
    script = '''import json,pathlib,sys
out={}
for item in sys.argv[1:]:
 role,path=item.split('=',1); root=pathlib.Path(path)
 events=dict(line.split() for line in (root/'memory.events').read_text().splitlines())
 out[role]={'memory_current_bytes':int((root/'memory.current').read_text()),
            'oom_events':int(events['oom']),'oom_kill_events':int(events['oom_kill']),
            'pids':int((root/'pids.current').read_text())}
print(json.dumps(out,separators=(',',':')))
'''
    try:
        data = json.loads(stand.docker('exec', stand.observer, 'python', '-c', script,
                                       *[role + '=' + path for role, path in paths.items()]))
    except (OSError, ValueError, TypeError):
        raise SoakPhaseError('cgroup_snapshot_unavailable') from None
    if not isinstance(data, dict) or set(data) != set(CGROUP_ROLES):
        raise SoakPhaseError('cgroup_snapshot_unavailable')
    for values in data.values():
        if (not isinstance(values, dict) or any(type(values.get(key)) is not int or values[key] < 0
                                                  for key in ('memory_current_bytes', 'oom_events',
                                                              'oom_kill_events', 'pids'))):
            raise SoakPhaseError('cgroup_snapshot_unavailable')
    return data


def _relay_diagnostic_snapshot(stand) -> dict | None:
    path = Path(stand.run_dir) / 'relay-report' / 'relay-diagnostics.json'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024:
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if (not isinstance(value, dict) or type(value.get('schema_version')) is not int
            or value['schema_version'] != 1):
        return None
    active, peak = value.get('active'), value.get('peak_active')
    counters, timing = value.get('counters'), value.get('timing_ms')
    if (type(active) is not int or active < 0 or active > 4
            or type(peak) is not int or peak < active or peak > 4
            or not isinstance(counters, dict) or set(counters) != set(RELAY_DIAGNOSTIC_COUNTERS)
            or any(type(counters[key]) is not int or not 0 <= counters[key] <= MAX_RELAY_COUNTER
                   for key in RELAY_DIAGNOSTIC_COUNTERS)
            or not isinstance(timing, dict) or set(timing) != set(RELAY_TIMING_FIELDS)
            or any(type(timing[key]) is not int or not 0 <= timing[key] <= MAX_RELAY_COUNTER
                   for key in RELAY_TIMING_FIELDS)):
        return None
    tcp = value.get('tcp_listener_counters')
    if tcp is not None and (
            not isinstance(tcp, dict) or set(tcp) != set(TCP_LISTENER_COUNTERS)
            or any(type(tcp[key]) is not int or not 0 <= tcp[key] <= MAX_RELAY_COUNTER
                   for key in TCP_LISTENER_COUNTERS)):
        return None
    tcp_connections = value.get('tcp_connection_counters')
    if tcp_connections is not None and (
            not isinstance(tcp_connections, dict) or set(tcp_connections) != set(TCP_CONNECTION_COUNTERS)
            or any(type(tcp_connections[key]) is not int or not 0 <= tcp_connections[key] <= MAX_RELAY_COUNTER
                   for key in TCP_CONNECTION_COUNTERS)):
        return None
    event_loop = value.get('event_loop_lag_ms')
    if event_loop is not None and (
            not isinstance(event_loop, dict) or set(event_loop) != {'samples', 'max'}
            or any(type(event_loop[key]) is not int or not 0 <= event_loop[key] <= MAX_RELAY_COUNTER
                   for key in ('samples', 'max'))):
        return None
    return {
        'active': active,
        'peak_active': peak,
        'counters': {key: counters[key] for key in RELAY_DIAGNOSTIC_COUNTERS},
        'timing_ms': {key: timing[key] for key in RELAY_TIMING_FIELDS},
        'tcp_listener_counters': ({key: tcp[key] for key in TCP_LISTENER_COUNTERS}
                                  if tcp is not None else None),
        'tcp_connection_counters': ({key: tcp_connections[key] for key in TCP_CONNECTION_COUNTERS}
                                    if tcp_connections is not None else None),
        'event_loop_lag_ms': ({'samples': event_loop['samples'], 'max': event_loop['max']}
                              if event_loop is not None else None),
    }


def _relay_cgroup_snapshot(stand) -> dict | None:
    try:
        path = stand.cgroup(stand.relay)
        if not _safe_cgroup_path(path):
            return None
        script = '''import json,pathlib,sys
root=pathlib.Path(sys.argv[1]); cpu=dict(line.split() for line in (root/'cpu.stat').read_text().splitlines())
events=dict(line.split() for line in (root/'memory.events').read_text().splitlines())
out={'memory_current_bytes':int((root/'memory.current').read_text()),
     'oom_events':int(events['oom']),'oom_kill_events':int(events['oom_kill']),
     'pids':int((root/'pids.current').read_text()),
     'cpu_stat':{key:int(cpu[key]) for key in ('usage_usec','nr_periods','nr_throttled','throttled_usec')}}
print(json.dumps(out,separators=(',',':')))
'''
        data = json.loads(stand.docker('exec', stand.observer, 'python', '-c', script, path))
        cpu = data.get('cpu_stat') if isinstance(data, dict) else None
        if (not isinstance(data, dict) or set(data) != {'memory_current_bytes', 'oom_events',
                'oom_kill_events', 'pids', 'cpu_stat'} or not isinstance(cpu, dict)
                or set(cpu) != set(RELAY_CPU_STAT_FIELDS)
                or any(type(value) is not int or value < 0 for value in
                       [data[key] for key in ('memory_current_bytes', 'oom_events',
                                              'oom_kill_events', 'pids')] + list(cpu.values()))):
            return None
        return {'memory_current_bytes': data['memory_current_bytes'], 'oom_events': data['oom_events'],
                'oom_kill_events': data['oom_kill_events'], 'pids': data['pids'],
                'cpu_stat': {key: cpu[key] for key in RELAY_CPU_STAT_FIELDS}}
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None


def _collect_failure_diagnostics(stand) -> None:
    """Best-effort private collection; collection errors never replace the soak failure."""
    report = {'schema_version': 1, 'diagnostics': []}
    try:
        log_text = stand.diagnostic_logs(stand.worker)
        if isinstance(log_text, str):
            for line in log_text.splitlines()[-1000:]:
                match = NATIVE_DIAGNOSTIC_LINE.fullmatch(line)
                if match:
                    item = {'stage': match.group(1), 'reason': match.group(2)}
                    if item not in report['diagnostics']:
                        report['diagnostics'].append(item)
                    if len(report['diagnostics']) == 32:
                        break
    except Exception:
        pass
    try:
        runtime = _runtime_snapshot(stand)
        report['runtime_snapshot'] = {
            'container_count': runtime['container_count'],
            'browser_info_sha256': runtime['browser_info_sha256'],
            'cdp_generation_sha256': runtime['cdp_generation_sha256'],
        }
    except Exception:
        pass
    try:
        report['cgroups'] = _cgroup_snapshot(stand)
    except Exception:
        pass
    relay = _relay_diagnostic_snapshot(stand)
    if relay is not None:
        report['relay'] = relay
    try:
        counters = stand.browser_loopback_diagnostic_counters()
        if _valid_browser_loopback_counters(counters):
            report['browser_loopback_nft_counters'] = {
                key: counters[key] for key in BROWSER_LOOPBACK_COUNTERS
            }
            baseline = _loopback_counter_baseline(Path(stand.run_dir))
            if baseline is not None and all(counters[key] >= baseline[key]
                                            for key in BROWSER_LOOPBACK_COUNTERS):
                report['browser_loopback_nft_counter_delta'] = {
                    'baseline_after_raw_network_gate': baseline,
                    'current_at_failure': dict(counters),
                    'delta_since_raw_network_gate': {
                        key: counters[key] - baseline[key] for key in BROWSER_LOOPBACK_COUNTERS
                    },
                }
    except Exception:
        pass
    relay_cgroup = _relay_cgroup_snapshot(stand)
    if relay_cgroup is not None:
        report['relay_cgroup'] = relay_cgroup
    try:
        path = Path(stand.run_dir) / 'synthetic-failure-diagnostics.json'
        if path.is_symlink() or path.exists():
            return
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
        path.chmod(0o600)
    except Exception:
        pass


def _start_fixture_origin(stand) -> str:
    if getattr(stand, 'origin', None) or any(name.endswith('-origin') for name in stand.containers):
        raise SoakPhaseError('fixture_origin_already_started')
    fixture_dir = Path(stand.stage) / 'benchmark' / 'tests' / 'fixtures' / 'native-cloak'
    origin_source = fixture_dir / 'origin.py'
    cert = Path(stand.stage) / 'tls' / 'cert.pem'
    key = Path(stand.stage) / 'tls' / 'key.pem'
    if any(path.is_symlink() or not path.is_file() for path in (origin_source, cert, key)):
        raise SoakPhaseError('fixture_origin_assets_missing')
    gateway_anchor = stand.anchors['gateway']['name']
    origin = stand.run(
        'origin', APP_IMAGE,
        ['/fixtures/native-cloak/origin.py', '--host', '127.0.0.1', '--port', '443',
         '--cert', '/tls/cert.pem', '--key', '/tls/key.pem',
         '--proof-output', '/out/soak-origin-proof.jsonl'],
        network='container:' + gateway_anchor, memory='128m', cpu='0.125', pids=64,
        mounts=[str(fixture_dir) + ':/fixtures/native-cloak:ro',
                str(Path(stand.stage) / 'tls') + ':/tls:ro', str(stand.run_dir) + ':/out'],
        entrypoint='python')
    stand.origin = origin
    stand.verify_ns(origin, gateway_anchor)
    stand.wait_listener('gateway', 443, 0)
    return origin


def _validate_reports(output_dir: Path, summary: dict) -> list[dict]:
    if (summary.get('schema_version') != 1 or summary.get('state') != 'passed'
            or summary.get('variant') != 'C'
            or summary.get('requested_duration_seconds') != SOAK_DURATION_SECONDS
            or summary.get('cycles_per_batch') != SOAK_CYCLES
            or summary.get('case_ids') != list(SOAK_CASE_IDS)
            or type(summary.get('batches_this_invocation')) is not int
            or summary['batches_this_invocation'] < 1
            or type(summary.get('completed_batches')) is not int
            or summary['completed_batches'] < 1
            or summary['completed_batches'] != summary['batches_this_invocation']
            or type(summary.get('elapsed_seconds')) not in (int, float)
            or summary['elapsed_seconds'] < SOAK_DURATION_SECONDS
            or summary.get('failure_code') is not None):
        raise SoakPhaseError('soak_summary_contract_failed')
    entries = list(output_dir.iterdir())
    if any(item.is_symlink() or not item.is_file()
           or (item.name != 'summary.json' and not re.fullmatch(r'batch-[0-9]{6}\.json', item.name))
           for item in entries):
        raise SoakPhaseError('soak_output_directory_invalid')
    files = sorted(output_dir.glob('batch-*.json'))
    if len(files) != summary['batches_this_invocation']:
        raise SoakPhaseError('soak_batch_count_mismatch')
    reports = []
    for path in files:
        if path.is_symlink() or not re.fullmatch(r'batch-[0-9]{6}\.json', path.name):
            raise SoakPhaseError('soak_batch_report_invalid')
        try:
            report = json.loads(path.read_text())
        except (OSError, ValueError):
            raise SoakPhaseError('soak_batch_report_invalid') from None
        if not isinstance(report, dict):
            raise SoakPhaseError('soak_batch_report_invalid')
        warmup = report.get('warmup')
        if (not isinstance(warmup, dict) or warmup.get('case_id') != SOAK_CASE_IDS[0]
                or warmup.get('outcome') != 'success' or warmup.get('state') != 'extracted'):
            raise SoakPhaseError('soak_warmup_contract_failed')
        outcomes = report.get('outcomes') if isinstance(report, dict) else None
        if (report.get('status') != 'passed' or report.get('variant') != 'C'
                or report.get('cycles') != SOAK_CYCLES
                or report.get('case_ids') != list(SOAK_CASE_IDS)
                or report.get('mismatch_count') != 0 or not isinstance(outcomes, list)
                or len(outcomes) != len(SOAK_CASE_IDS)):
            raise SoakPhaseError('soak_batch_contract_failed')
        readiness = report.get('readiness_profile')
        if (not isinstance(readiness, dict)
                or readiness.get('policy') != 'native_fixture_selector_and_network_gate'
                or readiness.get('selector') != '[data-ready="true"]'
                or readiness.get('active_requests') != 0
                or readiness.get('main_document_status') != 200
                or readiness.get('go_profile_basis') != 'checked_in_source_not_live_attestation'):
            raise SoakPhaseError('soak_readiness_contract_failed')
        outcome_pairs = {(row.get('case_id'), row.get('cycle')) for row in outcomes
                         if isinstance(row, dict)}
        expected_pairs = {(case_id, 1) for case_id in SOAK_CASE_IDS}
        if outcome_pairs != expected_pairs or any(
                row.get('expectation', {}).get('passed') is not True for row in outcomes):
            raise SoakPhaseError('soak_batch_contract_failed')
        if report.get('batch_index') != len(reports) + 1:
            raise SoakPhaseError('soak_batch_order_invalid')
        resources = report.get('resource_metrics', {}).get('cgroups', {})
        if not isinstance(resources, dict) or not set(CGROUP_ROLES).issubset(resources):
            raise SoakPhaseError('soak_cgroup_contract_failed')
        for role in CGROUP_ROLES:
            metrics = resources[role]
            if (not isinstance(metrics, dict)
                    or any(type(metrics.get(key)) is not int or metrics[key] < 0
                           for key in ('oom_events_delta', 'oom_kill_events_delta'))):
                raise SoakPhaseError('soak_cgroup_contract_failed')
            if metrics['oom_events_delta'] or metrics['oom_kill_events_delta']:
                raise SoakPhaseError('soak_oom_detected')
        reports.append({'batch_index': report.get('batch_index'), 'status': 'passed',
                        'mismatch_count': 0, 'case_count': len(SOAK_CASE_IDS)})
    return reports


def run_synthetic_stability(stand) -> dict:
    """Run the fixed 10-minute C fixture soak; outer supervisor owns all cleanup."""
    if (getattr(stand, 'bootstrap', False) is not True
            or getattr(stand, 'bounded_profile', False) is not True
            or getattr(stand, 'synthetic_soak', False) is not True
            or not all(hasattr(stand, name) for name in ('worker', 'observer', 'cloak', 'gateway'))):
        raise SoakPhaseError('synthetic_soak_profile_required')

    try:
        _start_fixture_origin(stand)
        stand.check_runtime()
        generation_before = _runtime_snapshot(stand)
        cgroups_before = _cgroup_snapshot(stand)
    except SoakPhaseError:
        raise
    except Exception:
        raise SoakPhaseError('soak_baseline_unavailable') from None

    output_dir = Path(stand.run_dir) / 'soak'
    if output_dir.exists() or output_dir.is_symlink():
        raise SoakPhaseError('soak_output_directory_not_fresh')
    output_dir.mkdir(mode=0o700)
    cgroup_args = []
    for role in CGROUP_ROLES:
        container = {'worker': stand.worker, 'cloak': stand.cloak, 'gateway': stand.gateway}[role]
        cgroup_args += ['--cgroup', role + '=' + stand.cgroup(container)]
    args = ['python', '/bench/scripts/soak_native_cloak.py', '--variant', 'C',
            '--duration-seconds', str(SOAK_DURATION_SECONDS), '--cycles', str(SOAK_CYCLES),
            '--idle-seconds', '5', '--corpus', '/bench/tests/fixtures/native-cloak/corpus.json',
            '--origin', 'https://native-fixture.test', '--endpoint', 'http://127.0.0.1:8080',
            '--native-ready-selector', '[data-ready="true"]', '--output-dir', '/out/soak',
            *cgroup_args]
    for case_id in SOAK_CASE_IDS:
        args += ['--case', case_id]

    try:
        stand.monitored_exec(args)
        stand.check_runtime()
        generation_after = _runtime_snapshot(stand)
        cgroups_after = _cgroup_snapshot(stand)
        for role in CGROUP_ROLES:
            before, after = cgroups_before[role], cgroups_after[role]
            for key in ('oom_events', 'oom_kill_events'):
                delta = after[key] - before[key]
                if delta < 0:
                    raise SoakPhaseError('soak_cgroup_counter_reset')
                if delta:
                    raise SoakPhaseError('soak_oom_detected')
        if (generation_before['generations'] != generation_after['generations']
                or generation_before['browser_info_sha256'] != generation_after['browser_info_sha256']
                or generation_before['cdp_generation_sha256'] != generation_after['cdp_generation_sha256']):
            raise SoakPhaseError('runtime_generation_changed')
        summary_path = output_dir / 'summary.json'
        if summary_path.is_symlink() or not summary_path.is_file() or summary_path.stat().st_size > 64 * 1024:
            raise SoakPhaseError('soak_summary_missing_or_invalid')
        try:
            summary = json.loads(summary_path.read_text())
        except (OSError, ValueError):
            raise SoakPhaseError('soak_summary_invalid') from None
        reports = _validate_reports(output_dir, summary)
        stand.check_runtime()
        # The soak driver itself probes readiness through the final idle interval;
        # this final local check confirms the same worker/browser remain healthy.
        stand.wait_http(stand.observer, 'http://127.0.0.1:8080/health/ready')
        observation = {
            'schema_version': 1,
            'state': 'synthetic_stability_completed',
            'duration_seconds': SOAK_DURATION_SECONDS,
            'cycles_per_batch': SOAK_CYCLES,
            'case_ids': list(SOAK_CASE_IDS),
            'completed_batches': summary['batches_this_invocation'],
            'batch_receipts': reports,
            'container_count': generation_after['container_count'],
            'container_generation_unchanged': True,
            'browser_generation_unchanged': True,
            'cgroups_before': cgroups_before,
            'cgroups_after': cgroups_after,
            'oom_counters_unchanged': True,
        }
        (Path(stand.run_dir) / 'synthetic-soak-observation.json').write_text(
            json.dumps(observation, indent=2) + '\n')
        return observation
    except SoakPhaseError:
        _collect_failure_diagnostics(stand)
        raise
    except Exception:
        # Leave all exact-label resources for the outer fail-closed supervisor.
        _collect_failure_diagnostics(stand)
        raise SoakPhaseError('synthetic_soak_failed') from None
