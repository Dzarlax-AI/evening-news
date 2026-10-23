import ast
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import socket
import stat
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location('native_shadow_stand', Path(__file__).parents[1]/'scripts/native_shadow_stand.py')
stand = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stand)


def policy(role, bootstrap=False):
    return stand.nft_policy(role,'172.22.0.2','172.22.0.3','172.22.0.4',bootstrap=bootstrap,host_ips=['65.109.0.90'])


def test_observer_mounts_staged_python_extraction_sources_read_only(tmp_path):
    assert stand.observer_extraction_mounts(tmp_path) == [
        str(tmp_path/'benchmark/news_aggregator/extraction/service_client.py') +
        ':/app/news_aggregator/extraction/service_client.py:ro',
        str(tmp_path/'benchmark/news_aggregator/extraction/extraction_utils.py') +
        ':/app/news_aggregator/extraction/extraction_utils.py:ro',
    ]
    assert all(path.endswith(':ro') for path in stand.observer_extraction_mounts(tmp_path))


def test_go_native_browser_route_gate_runs_after_origin_is_ready(tmp_path):
    events=[]
    instance=object.__new__(stand.Stand)
    instance.stage=tmp_path
    instance.run_dir=tmp_path
    instance.anchors={'gateway':{'name':'gateway-test','ip':'172.22.0.4'},
                      'control':{'ip':'172.22.0.2'}}
    instance.app='pinned-app'
    instance.worker_recovery_gate=True
    instance.bounded_profile=True
    instance.observer='observer-test'
    def run(suffix,*args,**kwargs):
        assert suffix=='origin'
        assert kwargs['network']=='container:gateway-test'
        events.append('origin_started')
        return 'origin-test'
    instance.run=run
    instance.verify_ns=lambda *_args: events.append('origin_namespace_verified') or 123
    def wait_listener(role,port,uid):
        assert (role,port,uid)==('gateway',443,0)
        events.append('origin_listener_ready')
    instance.wait_listener=wait_listener
    generation='a'*64
    def docker(*args,**kwargs):
        assert args[:3]==('exec','observer-test','python')
        events.append('browser_generation_read')
        return json.dumps({'browser_generation':generation})
    instance.docker=docker
    def native_gate(_generation):
        events.append('native_route_gate')
        raise stand.StandError('stop_after_order_check')
    instance.production_core_extractor_native_gate=native_gate
    with pytest.raises(stand.StandError,match='stop_after_order_check'):
        instance._controlled_browser_routes()
    assert events==['origin_started','origin_namespace_verified','origin_listener_ready',
                    'browser_generation_read','native_route_gate']


def test_core_extractor_native_gate_uses_current_production_route_and_no_python_browser():
    script=stand.CORE_EXTRACTOR_NATIVE_GATE
    assert 'from news_aggregator.extraction.core_extractor import CoreExtractor' in script
    assert 'from news_aggregator.extraction.service_client import ExtractionServiceError' in script
    assert 'from news_aggregator.extraction.extraction_utils import ExtractionUtils' in script
    assert 'core.extract_article_content_with_metadata(' in script
    assert "'https://native-fixture.test/js-article'" in script
    assert "'https://native-fixture.test/origin-auth'" in script
    assert 'cookie confirmed' in script
    assert 'python_fallback_calls' in script and 'forbidden_python_fallback' in script
    assert 'asyncio.wait_for' in script
    assert 'news_aggregator.core.browser_pool' in script
    assert "os.open('/out/core-extractor-native.json'" in script
    assert 'print(' not in script and 'benchmark_native' not in script


def _core_native_receipt():
    receipt={key:True for key in stand.CORE_EXTRACTOR_NATIVE_RECEIPT_FLAGS}
    receipt.update({
        'state':'passed','publisher_auth_code':'upstream_auth_required',
        'python_fallback_calls':0,'success_record_count':1,'learned_pattern_lookups':2,
        'learning_backend':'in_memory','article_characters':512,
    })
    receipt['python_browser_imported']=False
    receipt['ai_db_telegram_imported']=False
    return receipt


def test_core_extractor_native_receipt_is_strict_and_sanitized(tmp_path):
    receipt=_core_native_receipt()
    assert stand.validate_core_extractor_native_receipt(receipt)==receipt
    receipt['article']='raw article text'
    with pytest.raises(stand.StandError,match='core_extractor_native_receipt_invalid'):
        stand.validate_core_extractor_native_receipt(receipt)

    failed={'state':'failed','phase':'positive_article','failure_type':'RuntimeError',
        'failure_code':'cookie_proof_missing','python_fallback_calls':0}
    path=tmp_path/'failed.json'
    path.write_text(json.dumps(failed)); path.chmod(0o600)
    with pytest.raises(stand.StandError,match='core_extractor_native_gate_failed'):
        stand.read_core_extractor_native_receipt(path)
    failed['diagnostic']='raw URL or exception'
    path.write_text(json.dumps(failed)); path.chmod(0o600)
    with pytest.raises(stand.StandError,match='core_extractor_native_receipt_invalid'):
        stand.read_core_extractor_native_receipt(path)


def test_core_extractor_native_gate_runs_only_in_worker_recovery_profile(tmp_path):
    mounts=stand.observer_core_extractor_mounts(tmp_path)
    assert mounts==[
        str(tmp_path/'benchmark/news_aggregator/config.py')+':/app/news_aggregator/config.py:ro',
        str(tmp_path/'benchmark/news_aggregator/extraction/core_extractor.py')+
        ':/app/news_aggregator/extraction/core_extractor.py:ro',
    ]
    assert stand.observer_extraction_mounts(tmp_path)==[
        str(tmp_path/'benchmark/news_aggregator/extraction/service_client.py')+
        ':/app/news_aggregator/extraction/service_client.py:ro',
        str(tmp_path/'benchmark/news_aggregator/extraction/extraction_utils.py')+
        ':/app/news_aggregator/extraction/extraction_utils.py:ro',
    ]


def test_core_extractor_native_gate_invocation_requires_worker_recovery_and_accepts_complete_receipt(
        tmp_path):
    instance=stand.Stand.__new__(stand.Stand)
    instance.run_dir=tmp_path
    instance.observer='owned-observer'
    instance.check_budget=lambda:None
    instance.check_runtime=lambda:None
    generation='a'*64
    calls=[]
    def docker(*args,**kwargs):
        calls.append((args,kwargs))
        path=tmp_path/'core-extractor-native.json'
        path.write_text(json.dumps(_core_native_receipt())+'\n')
        path.chmod(0o600)
        return ''
    instance.docker=docker
    receipt=stand.Stand.production_core_extractor_native_gate(instance,generation)
    assert receipt==_core_native_receipt()
    assert len(calls)==1
    assert calls[0][0][:4]==('exec','owned-observer','python','-c')
    assert calls[0][0][4]==stand.CORE_EXTRACTOR_NATIVE_GATE
    assert calls[0][0][5]==generation
    assert calls[0][1]['timeout']==75


def test_diagnostic_logs_verify_worker_and_capture_both_docker_streams(monkeypatch):
    instance = object.__new__(stand.Stand)
    instance.prefix = 'news-native-shadow-0123456789'
    instance.worker = instance.prefix + '-worker'
    instance.containers = [instance.worker]
    ownership_calls = []

    def docker(*args, **_kwargs):
        ownership_calls.append(args)
        return instance.prefix

    instance.docker = docker
    stderr_line = 'native_browser_diagnostic stage=dom reason=protocol_error'
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, stdout='stdout record', stderr=stderr_line)

    monkeypatch.setattr(stand.subprocess, 'run', run)
    logs = instance.diagnostic_logs(instance.worker)
    assert stderr_line in logs and 'stdout record' in logs
    assert ownership_calls == [('inspect', '--format', '{{index .Config.Labels "native-shadow-run"}}', instance.worker)]
    assert calls[0][0] == ['docker', 'logs', '--tail', '1000', instance.worker]
    with pytest.raises(stand.StandError, match='diagnostic_log_target_invalid'):
        instance.diagnostic_logs(instance.prefix + '-other')


def test_quality_failure_receipt_keeps_only_finite_worker_diagnostics(tmp_path):
    class FakeStand:
        worker = 'owned-worker'
        run_dir = tmp_path
        def diagnostic_logs(self, name):
            assert name == self.worker
            return ('native_browser_diagnostic stage=events reason=event_overflow\n'
                    'native_browser_diagnostic stage=dom reason=operation_failed\n'
                    'native_browser_diagnostic stage=secret.example reason=secret-token\n'
                    'page body secret-token\n')

    assert stand.collect_native_diagnostic_receipt(FakeStand())
    receipt_path = tmp_path / 'native-browser-diagnostics.json'
    assert receipt_path.stat().st_mode & 0o777 == 0o600
    value = json.loads(receipt_path.read_text())
    assert value == {'schema_version': 1, 'diagnostics': [
        {'stage': 'events', 'reason': 'event_overflow'},
        {'stage': 'dom', 'reason': 'operation_failed'},
    ]}
    assert 'secret-token' not in receipt_path.read_text()


def test_quality_receipt_preserves_new_finite_native_failure_reasons(tmp_path):
    reasons = ('client_disconnected', 'malformed_detach', 'owned_detach', 'target_crashed')

    class FakeStand:
        worker = 'owned-worker'
        run_dir = tmp_path
        def diagnostic_logs(self, _name):
            return ''.join(f'native_browser_diagnostic stage=events reason={reason}\n'
                           for reason in reasons)

    assert stand.collect_native_diagnostic_receipt(FakeStand())
    value = json.loads((tmp_path / 'native-browser-diagnostics.json').read_text())
    assert value['diagnostics'] == [
        {'stage': 'events', 'reason': reason} for reason in reasons
    ]


def test_quality_diagnostic_receipt_parses_only_bounded_log_tail(tmp_path):
    class FakeStand:
        worker = 'owned-worker'
        run_dir = tmp_path
        def diagnostic_logs(self, _name):
            return ('native_browser_diagnostic stage=events reason=event_overflow\n'
                    + ('x' * 262145)
                    + '\nnative_browser_diagnostic stage=dom reason=context_timeout\n')

    assert stand.collect_native_diagnostic_receipt(FakeStand())
    value = json.loads((tmp_path / 'native-browser-diagnostics.json').read_text())
    assert value['diagnostics'] == [{'stage': 'dom', 'reason': 'context_timeout'}]


@pytest.mark.parametrize('symlink', [False, True])
def test_quality_diagnostic_receipt_does_not_overwrite_existing_path(tmp_path, symlink):
    receipt = tmp_path / 'native-browser-diagnostics.json'
    target = tmp_path / 'private-target.json'
    target.write_text('keep')
    if symlink:
        receipt.symlink_to(target)
    else:
        receipt.write_text('existing')

    class FakeStand:
        worker = 'owned-worker'
        run_dir = tmp_path
        def diagnostic_logs(self, _name):
            return 'native_browser_diagnostic stage=events reason=event_overflow\n'

    assert not stand.collect_native_diagnostic_receipt(FakeStand())
    assert target.read_text() == 'keep'
    if symlink:
        assert receipt.is_symlink()
    else:
        assert receipt.read_text() == 'existing'


def test_quality_diagnostic_collection_failure_does_not_replace_original_failure(tmp_path):
    class FakeStand:
        worker = 'owned-worker'
        run_dir = tmp_path
        def diagnostic_logs(self, _name):
            raise stand.StandError('diagnostic_logs_unavailable')

    original = stand.StandError('native_browser_unavailable')
    with pytest.raises(stand.StandError, match='native_browser_unavailable'):
        try:
            raise original
        except stand.StandError:
            assert not stand.collect_native_diagnostic_receipt(FakeStand())
            raise
    assert not (tmp_path / 'native-browser-diagnostics.json').exists()


@pytest.mark.parametrize(('quality', 'soak', 'bounded_gate', 'expected'), [
    (True, False, False, 'live-quality'), (False, True, False, 'synthetic-soak'),
    (False, False, True, 'synthetic-soak'), (False, False, False, ''),
    (True, True, False, ''), (True, False, True, ''),
])
def test_native_diagnostics_mode_is_explicit(quality, soak, bounded_gate, expected):
    assert stand.native_diagnostics_mode(quality_profile=quality, synthetic_soak=soak,
                                        bounded_gate=bounded_gate) == expected


@pytest.mark.parametrize('role',['browser','control','gateway'])
def test_policy_is_default_deny_without_host_flush(role):
    text=policy(role)
    assert text.count('policy drop;') == 3
    assert 'flush' not in text
    assert 'priority -150' in text
    assert 'meta nfproto ipv6 counter drop' in text
    assert 'ct state invalid counter drop' in text


def test_browser_only_opens_proxy_and_replies_to_controller():
    text=policy('browser')
    output=text.split('chain output')[1].split('chain input')[0]
    assert 'ip daddr 172.22.0.4 tcp dport 8081 ct state { new, established }' in output
    assert 'tcp sport 9222 ct state established' in output
    assert 'dport 443' not in output
    assert 'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established }' in output
    assert 'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established' in output
    assert 'meta skuid 0 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100' not in output
    assert 'dport 53' not in output


def test_browser_loopback_diagnostics_are_counter_only_and_preserve_allowlist():
    text = policy('browser')
    output = text.split('chain output')[1].split('chain input')[0]
    input_chain = text.split('chain input')[1].split('chain forward')[0]
    names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if not name.startswith('diag_loopback5100_input_'))
    input_names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if name.startswith('diag_loopback5100_input_'))
    for name in names:
        rule = next(line for line in output.splitlines() if f'comment "{name}"' in line)
        assert 'counter' in rule
        if name.startswith('diag_loopback5100_accept_'):
            assert 'tcp flags & (syn | ack) ==' in rule
            assert ' accept' not in rule and ' drop' not in rule
            if name == 'diag_loopback5100_accept_relay_uid_syn':
                assert 'meta skuid 65534' in rule and rule.endswith('== syn counter comment "' + name + '"')
            else:
                assert 'meta skuid 0' in rule and rule.endswith('== (syn | ack) counter comment "' + name + '"')
        else:
            assert ' accept' not in rule and ' drop' not in rule
    assert output.index('diag_loopback5100_invalid_syn') < output.index('ct state invalid counter drop')
    assert output.index('diag_loopback5100_fallback_syn') < output.rindex('counter drop')
    for name in input_names:
        rule = next(line for line in input_chain.splitlines() if f'comment "{name}"' in line)
        assert 'counter' in rule
        if name.startswith('diag_loopback5100_input_accept_'):
            assert 'tcp flags & (syn | ack) ==' in rule
            assert ' accept' not in rule and ' drop' not in rule
        else:
            assert ' accept' not in rule and ' drop' not in rule
    assert input_chain.index('diag_loopback5100_input_invalid_syn') < input_chain.index('ct state invalid counter drop')
    assert input_chain.index('diag_loopback5100_input_fallback_syn') < input_chain.rindex('counter drop')
    assert 'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept' in output
    assert 'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept' in output
    output_lines = output.splitlines()
    input_lines = input_chain.splitlines()
    assert next(i for i, line in enumerate(output_lines) if 'diag_loopback5100_accept_relay_uid_syn' in line) < next(
        i for i, line in enumerate(output_lines) if line.startswith('meta skuid 65534') and 'ct state {' in line)
    assert next(i for i, line in enumerate(input_lines) if 'diag_loopback5100_input_accept_syn' in line) < next(
        i for i, line in enumerate(input_lines) if line.startswith('iifname "lo"') and 'ct state {' in line)


def test_browser_loopback_diagnostic_reader_exports_only_owned_comment_counters(monkeypatch):
    instance = object.__new__(stand.Stand)
    instance.anchors = {'browser': {'name': 'owned-browser'}}
    verified = []
    instance.verify_ns = lambda name, anchor: verified.append((name, anchor)) or '123'
    names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if not name.startswith('diag_loopback5100_input_'))
    input_names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if name.startswith('diag_loopback5100_input_'))
    all_names = stand.BROWSER_LOOPBACK_COUNTERS
    def nft_reply(args):
        selected = names if args[-1] == 'output' else input_names
        return json.dumps({'nftables': [{'rule': {'comment': name,
                                                    'expr': [{'counter': {'packets': i, 'bytes': 99}},
                                                             {'match': {'op': '==', 'left': 1, 'right': 1}}]}}
                                        for i, name in enumerate(selected)]})
    calls = []
    monkeypatch.setattr(stand, 'command', lambda args, **kwargs: calls.append(args) or nft_reply(args))
    assert instance.browser_loopback_diagnostic_counters() == dict(zip(all_names, list(range(8)) + list(range(6))))
    assert verified == [('owned-browser', 'owned-browser')]
    assert calls == [['nsenter', '-t', '123', '-n', '--', 'nft', '-j', 'list', 'chain', 'inet',
                      stand.TABLE, 'output'],
                     ['nsenter', '-t', '123', '-n', '--', 'nft', '-j', 'list', 'chain', 'inet',
                      stand.TABLE, 'input']]


@pytest.mark.parametrize('bad_rule', [
    {'comment': 'diag_loopback5100_unknown', 'expr': [{'counter': {'packets': 0}}]},
    {'comment': 'diag_loopback5100_invalid_syn', 'expr': []},
])
def test_browser_loopback_diagnostic_reader_rejects_unknown_or_malformed_rules(monkeypatch, bad_rule):
    instance = object.__new__(stand.Stand)
    instance.anchors = {'browser': {'name': 'owned-browser'}}
    instance.verify_ns = lambda *_args: '123'
    names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if not name.startswith('diag_loopback5100_input_'))
    input_names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if name.startswith('diag_loopback5100_input_'))
    def nft_reply(args):
        selected = names if args[-1] == 'output' else input_names
        rules = [{'comment': name, 'expr': [{'counter': {'packets': 0}}]} for name in selected]
        if (args[-1] == 'input') == str(bad_rule.get('comment', '')).startswith('diag_loopback5100_input_'):
            rules.append(bad_rule)
        return json.dumps({'nftables': [{'rule': rule} for rule in rules]})
    monkeypatch.setattr(stand, 'command', lambda args, **_kwargs: nft_reply(args))
    with pytest.raises(stand.StandError, match='browser_nft_diagnostics_unavailable'):
        instance.browser_loopback_diagnostic_counters()


def test_browser_loopback_diagnostic_reader_rejects_duplicate_rule_comments(monkeypatch):
    instance = object.__new__(stand.Stand)
    instance.anchors = {'browser': {'name': 'owned-browser'}}
    instance.verify_ns = lambda *_args: '123'
    names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if not name.startswith('diag_loopback5100_input_'))
    input_names = tuple(name for name in stand.BROWSER_LOOPBACK_COUNTERS if name.startswith('diag_loopback5100_input_'))
    def nft_reply(args):
        selected = names if args[-1] == 'output' else input_names
        rules = [{'comment': name, 'expr': [{'counter': {'packets': 0}}]} for name in selected]
        if args[-1] == 'output':
            rules.append({'comment': names[0], 'expr': [{'counter': {'packets': 1}}]})
        return json.dumps({'nftables': [{'rule': rule} for rule in rules]})
    monkeypatch.setattr(stand, 'command', lambda args, **_kwargs: nft_reply(args))
    with pytest.raises(stand.StandError, match='browser_nft_diagnostics_unavailable'):
        instance.browser_loopback_diagnostic_counters()


def test_public_gateway_has_no_fixture_loopback_exception():
    text=policy('gateway')
    assert '65.109.0.90/32' in text
    assert text.index('ip daddr { 0.0.0.0/8') < text.index('tcp dport 443 ct state')
    assert 'iifname "lo"' not in text
    assert 'oifname "lo"' not in text
    assert 'ip daddr 1.1.1.1 udp dport 53' in text
    assert '127.0.0.11' not in text
    assert 'oifname "lo"' in policy('gateway',True)
    bootstrap=policy('gateway',True)
    assert 'ip daddr 1.1.1.1' not in bootstrap
    assert '\ntcp dport 443 ct state' not in bootstrap


def test_control_cannot_fetch_origins_or_resolve_dns():
    text=policy('control')
    assert 'tcp dport 8080' in text
    assert 'tcp dport 9222' in text
    assert 'dport 443' not in text
    assert 'dport 53' not in text


def test_policy_rejects_injected_addresses_and_unknown_role():
    with pytest.raises(ValueError):
        stand.nft_policy('browser','172.22.0.2; accept','172.22.0.3','172.22.0.4')
    with pytest.raises(stand.StandError):
        policy('host')


def test_unroutable_public_destination_is_denied_without_faking_firewall_counter():
    assert stand.probe_denied({'connected':False,'errno':101},0,'1.1.1.1')
    assert not stand.probe_denied({'connected':True},0,'1.1.1.1')
    assert not stand.probe_denied({'connected':False,'errno':111},0,'172.22.0.3')
    assert stand.probe_denied({'connected':False,'errno':None},1,'172.22.0.3')


@pytest.mark.parametrize('fails',[False,True])
def test_export_is_capped_and_passes_only_database_secret_then_removes_it(tmp_path,monkeypatch,fails):
    monkeypatch.setattr(stand,'mem_available',lambda:3*1024**3)
    gate=tmp_path/'gate';gate.mkdir()
    (gate/'manifest.json').write_text(json.dumps({'state':'cleaned','previous_phase':'controlled_browser_gate_passed','bootstrap':True}))
    (gate/'browser-trap.json').write_text('{"accepted":0}')
    seen=[]
    def call(args,**kwargs):
        if args[:3]==['docker','inspect','news_web']:
            return json.dumps([{'Config':{'Image':stand.APP,'Env':['DATABASE_URL=postgresql://user:sentinel@db/news',
                                                                'TELEGRAM_TOKEN=never-copy']},
                                'State':{'StartedAt':'fixed'},'RestartCount':0}])
        seen.append(args)
        assert '--memory' in args and args[args.index('--memory')+1]=='384m'
        assert args[args.index('--network')+1]=='container:news_web'
        secret=Path(args[args.index('--env-file')+1])
        assert secret.read_text()=='DATABASE_URL=postgresql://user:sentinel@db/news\n'
        assert 'sentinel' not in ' '.join(args)
        if fails: raise stand.StandError('simulated_failure')
        output=tmp_path/'shadow-private/corpus-run';output.mkdir()
        (output/'manifest.json').write_text('{"case_count":1}')
        (output/'corpus.json').write_text('{}')
        return '{}'
    monkeypatch.setattr(stand,'command',call)
    monkeypatch.setattr(stand.subprocess,'run',lambda *a,**kw:SimpleNamespace(returncode=1))
    if fails:
        with pytest.raises(stand.StandError):stand.export_private_corpus(tmp_path,gate)
    else:
        corpus,manifest=stand.export_private_corpus(tmp_path,gate)
        assert corpus.exists() and manifest['case_count']==1
    assert len(seen)==1
    assert not (tmp_path/'shadow-private/export-db.env').exists()


def test_export_memory_preflight_precedes_production_inspection(tmp_path,monkeypatch):
    gate=tmp_path/'gate';gate.mkdir()
    (gate/'manifest.json').write_text(json.dumps({'state':'cleaned','previous_phase':'controlled_browser_gate_passed','bootstrap':True}))
    (gate/'browser-trap.json').write_text('{"accepted":0}')
    monkeypatch.setattr(stand,'mem_available',lambda:1024**3)
    monkeypatch.setattr(stand,'command',lambda *a,**kw:pytest.fail('production inspected before memory gate'))
    with pytest.raises(stand.StandError,match='insufficient_host_memory'):
        stand.export_private_corpus(tmp_path,gate)
    assert not (tmp_path/'shadow-private').exists()


def test_idle_offsets_and_oom_guard(tmp_path,monkeypatch):
    cg=tmp_path/'cg';cg.mkdir()
    (cg/'memory.events').write_text('oom 0\noom_kill 0\n')
    (cg/'memory.current').write_text('1024')
    (cg/'pids.current').write_text('4')
    (cg/'cpu.stat').write_text('usage_usec 10\n')
    instance=stand.Stand.__new__(stand.Stand)
    instance.run_dir=tmp_path
    instance.resource_groups=lambda variant:{'cloak':'owned'}
    instance.cgroup=lambda container:str(cg)
    instance.check_runtime=lambda:None
    clock=[0.0]
    monkeypatch.setattr(stand.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(stand.time,'sleep',lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    rows=instance.idle_gates('C','test')
    assert [row['idle_seconds'] for row in rows]==[5,30,60]
    assert clock[0]==60
    def oom_sleep(seconds):
        clock[0]+=seconds
        (cg/'memory.events').write_text('oom 1\noom_kill 0\n')
    monkeypatch.setattr(stand.time,'sleep',oom_sleep)
    with pytest.raises(stand.StandError,match='idle_oom'):
        instance.idle_gates('C','oom')


def test_first_public_smoke_requires_single_c_case(tmp_path):
    corpus=tmp_path/'corpus.json';corpus.write_text('{"version":1}')
    instance=stand.Stand.__new__(stand.Stand)
    instance.check_public_smoke('C',corpus,tmp_path,['case-0001'])
    for variant,ids in [('C',[]),('C',['case-0001','case-0002'])]:
        with pytest.raises(stand.StandError,match='first_public'):
            instance.check_public_smoke(variant,corpus,tmp_path,ids)


@pytest.mark.parametrize('change',[
    {'status':'incomplete'}, {'corpus_sha256':'stale'}, {'variant':'B'},
    {'schema_version':2}, {'fatal_error':'shadow_oom'}, {'rows':[]},
    {'rows':[{'outcome':'failed'}]}, {'rows':[None]},
])
def test_public_smoke_rejects_failed_incomplete_or_stale_report(tmp_path,change):
    import hashlib
    corpus=tmp_path/'corpus.json';corpus.write_bytes(b'{"version":1}')
    report={'schema_version':1,'variant':'C','status':'complete',
            'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest(),
            'fatal_error':None,'rows':[{'id':'case-0001','outcome':'success'}]}
    report.update(change)
    (tmp_path/'report-C.json').write_text(json.dumps(report))
    instance=stand.Stand.__new__(stand.Stand)
    for variant in ('C',):
        with pytest.raises(stand.StandError,match='first_public_native_tls'):
            instance.check_public_smoke(variant,corpus,tmp_path,['case-0002'])


def test_public_smoke_accepts_only_matching_complete_c_success(tmp_path):
    import hashlib
    corpus=tmp_path/'corpus.json';corpus.write_bytes(b'{"version":1}')
    report={'schema_version':1,'variant':'C','status':'complete',
            'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest(),
            'fatal_error':None,'rows':[{'id':'case-0001','outcome':'success'}]}
    prior=tmp_path/'report-C.json';prior.write_text(json.dumps(report))
    instance=stand.Stand.__new__(stand.Stand)
    instance.check_public_smoke('C',corpus,tmp_path)
    prior.write_text('{broken')
    with pytest.raises(stand.StandError,match='first_public_native_tls'):
        instance.check_public_smoke('C',corpus,tmp_path)


@pytest.mark.parametrize('error,status,allowed',[
    ('native_browser_http_rejected',403,True),
    ('native_browser_http_rejected',500,True),
    ('upstream_auth_required',401,True),
    ('upstream_rate_limited',429,True),
    ('native_browser_http_rejected',407,False),
    ('native_browser_http_rejected',None,False),
    ('native_browser_http_rejected','403',False),
    ('native_browser_http_rejected',True,False),
    ('native_browser_http_rejected',199,False),
    ('native_browser_http_rejected',600,False),
    ('native_browser_unavailable',403,False),
    ('transport_error',500,False),
    ('public_url_denied',403,False),
    ('native_job_deadline_cleanup_unconfirmed',None,False),
])
def test_public_smoke_requires_origin_tls_not_extraction_success(tmp_path,error,status,allowed):
    import hashlib
    corpus=tmp_path/'corpus.json';corpus.write_bytes(b'{"version":1}')
    report={'schema_version':1,'variant':'C','status':'complete',
            'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest(),
            'fatal_error':None,'rows':[{'id':'case-0001','outcome':'rejected',
                                      'error':error,'upstream_status':status}]}
    (tmp_path/'report-C.json').write_text(json.dumps(report))
    instance=stand.Stand.__new__(stand.Stand)
    if allowed:
        instance.check_public_smoke('C',corpus,tmp_path)
    else:
        with pytest.raises(stand.StandError,match='first_public_native_tls'):
            instance.check_public_smoke('C',corpus,tmp_path)


@pytest.mark.parametrize('phase,variant,cases', [
    ('c-smoke', 'C', ('case-0001',)),
    ('c-quality', 'C', tuple(case for case in stand.QUALITY_CASES if case != 'case-0001')),
])
def test_quality_phase_has_fixed_variant_and_case_allowlist(phase, variant, cases):
    stand.validate_quality_phase(variant, phase, ())
    stand.validate_quality_phase(variant, phase, cases)
    with pytest.raises(stand.StandError, match='quality_phase_selection_invalid'):
        stand.validate_quality_phase('B', phase, cases)
    with pytest.raises(stand.StandError, match='quality_phase_selection_invalid'):
        stand.validate_quality_phase(variant, phase, (*cases, 'case-9999'))


def test_quality_corpus_requires_pinned_hash_and_root_only_file(tmp_path, monkeypatch):
    corpus = tmp_path / 'corpus.json'
    corpus.write_bytes(b'pinned fixture')
    corpus.chmod(0o600)
    tmp_path.chmod(0o700)
    import hashlib
    monkeypatch.setattr(stand, 'QUALITY_CORPUS_SHA256', hashlib.sha256(b'pinned fixture').hexdigest())
    assert stand.validate_quality_corpus(corpus) == stand.QUALITY_CORPUS_SHA256
    corpus.write_bytes(b'changed fixture')
    with pytest.raises(stand.StandError, match='quality_corpus_hash_mismatch'):
        stand.validate_quality_corpus(corpus)
    corpus.unlink()
    target = tmp_path / 'target.json'
    target.write_bytes(b'pinned fixture')
    target.chmod(0o600)
    corpus.symlink_to(target)
    with pytest.raises(stand.StandError, match='quality_corpus_invalid'):
        stand.validate_quality_corpus(corpus)


def test_quality_receipt_requires_confirmed_outer_cleanup(tmp_path):
    parent_label = 'linux-vps-gate=0123456789abcdef'
    receipt = tmp_path / 'receipt'
    receipt.mkdir(mode=0o700)
    receipt.chmod(0o700)
    (receipt / 'manifest.json').write_text(json.dumps({
        'state': 'cleaned', 'previous_phase': 'controlled_browser_gate_passed',
        'bootstrap': True, 'prefix': 'news-native-shadow-0123456789'}))
    (receipt / 'browser-trap.json').write_text('{"accepted":0}')
    cleanup = {'state': 'confirmed', 'parent_label': parent_label,
               'owned_containers_remaining': 0, 'owned_networks_remaining': 0,
               'final_guard_passed': True, 'source_manifest_sha256': 'a' * 64,
               'runtime_bundle_manifest_sha256': 'b' * 64}
    cleanup_path = receipt / 'outer-cleanup.json'
    cleanup_path.write_text(json.dumps(cleanup))
    assert stand.validate_quality_gate_receipt(receipt, parent_label)['state'] == 'cleaned'
    cleanup['owned_containers_remaining'] = 1
    cleanup_path.write_text(json.dumps(cleanup))
    with pytest.raises(stand.StandError, match='controlled_gate_receipt_invalid'):
        stand.validate_quality_gate_receipt(receipt, parent_label)


def test_quality_cli_reaches_live_run_with_fixed_cases(tmp_path, monkeypatch):
    corpus = tmp_path / 'corpus.json'
    corpus.write_bytes(b'pinned fixture')
    corpus.chmod(0o600)
    tmp_path.chmod(0o700)
    import hashlib
    monkeypatch.setattr(stand, 'QUALITY_CORPUS_SHA256', hashlib.sha256(b'pinned fixture').hexdigest())
    captures = tmp_path / 'captures'
    captures.mkdir(mode=0o700)
    receipt = tmp_path / 'receipt'
    receipt.mkdir(mode=0o700)
    (receipt / 'manifest.json').write_text(json.dumps({
        'state': 'cleaned', 'previous_phase': 'controlled_browser_gate_passed',
        'bootstrap': True, 'prefix': 'news-native-shadow-0123456789'}))
    (receipt / 'browser-trap.json').write_text('{"accepted":0}')
    (receipt / 'outer-cleanup.json').write_text(json.dumps({
        'state': 'confirmed', 'parent_label': 'linux-vps-gate=0123456789abcdef',
        'owned_containers_remaining': 0, 'owned_networks_remaining': 0,
        'final_guard_passed': True, 'source_manifest_sha256': 'a' * 64,
        'runtime_bundle_manifest_sha256': 'b' * 64}))
    seen = {}
    class FakeStand:
        run_dir = tmp_path
        prefix = 'news-native-shadow-0123456789'
        phase = 'quality-test'
        def __init__(self, *args, **kwargs):
            seen['quality_profile'] = kwargs['quality_profile']
        def begin_phase(self, *args): seen['phase'] = args
        def setup(self): pass
        def raw_network_gate(self): pass
        def run_live(self, *args): seen['live_args'] = args
        def cleanup(self): pass
    monkeypatch.setattr(stand, 'Stand', FakeStand)
    monkeypatch.setattr(stand.signal, 'signal', lambda *_args: None)
    monkeypatch.setattr(stand.signal, 'setitimer', lambda *_args: None)
    monkeypatch.setattr(stand.sys, 'argv', [
        'native_shadow_stand.py', '--stage', str(tmp_path), '--live', '--variant', 'C',
        '--quality-profile', '--quality-phase', 'c-smoke', '--bounded-profile',
        '--supervisor-cleanup', '--supervisor-label', 'linux-vps-gate=0123456789abcdef',
        '--approved-gate', str(receipt), '--corpus', str(corpus), '--captures', str(captures),
        '--deadline', '1792519999'])
    stand.main()
    assert seen['quality_profile'] is True
    assert seen['phase'] == ('live_quality_c-smoke', 360)
    assert seen['live_args'] == ('C', corpus, captures, stand.QUALITY_PHASE_CASES['c-smoke'])


def test_quality_cli_rejects_missing_supervisor_cleanup_before_stand_creation(tmp_path, monkeypatch):
    monkeypatch.setattr(stand, 'Stand', lambda *_a, **_k: pytest.fail('stand created before option validation'))
    monkeypatch.setattr(stand.sys, 'argv', [
        'native_shadow_stand.py', '--stage', str(tmp_path), '--live', '--variant', 'C',
        '--quality-profile', '--quality-phase', 'c-smoke', '--bounded-profile'])
    with pytest.raises(stand.StandError, match='quality_profile_configuration_invalid'):
        stand.main()


def test_long_synthetic_soak_cli_runs_setup_runtime_soak_then_overload(tmp_path, monkeypatch):
    events = []
    class FakeStand:
        prefix = 'news-native-shadow-0123456789'
        run_dir = tmp_path
        phase = 'synthetic-test'
        def __init__(self, *args, **kwargs):
            events.append(('construct', kwargs['synthetic_soak']))
        def begin_phase(self, *args): events.append(('phase', *args))
        def setup(self): events.append(('setup',))
        def raw_network_gate(self): events.append(('raw_network_gate',))
        def start_runtime(self, variant): events.append(('start_runtime', variant))
        def synthetic_overload_gate(self): events.append(('overload',))
        def write_manifest(self, state): events.append(('manifest', state))
        def cleanup(self): events.append(('cleanup',))
    monkeypatch.setattr(stand, 'Stand', FakeStand)
    monkeypatch.setattr(stand, 'run_synthetic_stability', lambda _stand: events.append(('soak',)) or {'state':'ok'})
    monkeypatch.setattr(stand.signal, 'signal', lambda *_args: None)
    monkeypatch.setattr(stand.signal, 'setitimer', lambda *_args: None)
    monkeypatch.setattr(stand.sys, 'argv', [
        'native_shadow_stand.py', '--stage', str(tmp_path), '--bootstrap', '--gate-only',
        '--bounded-profile', '--long-synthetic-soak', '--supervisor-cleanup',
        '--supervisor-label', 'linux-vps-gate=0123456789abcdef', '--deadline',
        str(int(stand.time.time() + 14 * 60))])
    stand.main()
    labels = [event[0] for event in events]
    assert labels == ['construct', 'phase', 'setup', 'raw_network_gate', 'phase',
                      'start_runtime', 'phase', 'soak', 'overload', 'manifest', 'cleanup']
    assert events[0] == ('construct', True)
    assert ('start_runtime', 'C') in events
    assert ('phase', 'synthetic_stability_soak', 660) in events
    assert ('manifest', 'synthetic_stability_gate_passed') in events


def test_bounded_bootstrap_gate_failure_collects_finite_native_diagnostic(tmp_path, monkeypatch):
    class FakeStand:
        run_dir = tmp_path
        worker = 'owned-worker'
        def __init__(self, *_args, **_kwargs):
            self.phase = 'synthetic_browser_fixture'
        def begin_phase(self, *_args): pass
        def setup(self): pass
        def raw_network_gate(self): pass
        def start_runtime(self, *_args): pass
        def controlled_browser_gate(self): raise stand.StandError('positive_native_browser_gate_failed')
        def diagnostic_logs(self, name):
            assert name == self.worker
            return 'native_browser_diagnostic stage=events reason=target_crashed\n'
        def cleanup(self): pass

    monkeypatch.setattr(stand, 'Stand', FakeStand)
    monkeypatch.setattr(stand.signal, 'signal', lambda *_args: None)
    monkeypatch.setattr(stand.signal, 'setitimer', lambda *_args: None)
    monkeypatch.setattr(stand.sys, 'argv', [
        'native_shadow_stand.py', '--stage', str(tmp_path), '--bootstrap', '--gate-only',
        '--bounded-profile', '--supervisor-cleanup', '--supervisor-label',
        'linux-vps-gate=0123456789abcdef', '--deadline', str(int(stand.time.time() + 5 * 60))])
    with pytest.raises(stand.StandError, match='positive_native_browser_gate_failed'):
        stand.main()
    value = json.loads((tmp_path / 'native-browser-diagnostics.json').read_text())
    assert value['diagnostics'] == [{'stage': 'events', 'reason': 'target_crashed'}]


@pytest.mark.parametrize('change', [
    {'live': True}, {'variant': 'C'}, {'case_ids': ['case-0001']},
    {'quality_profile': True}, {'corpus': Path('/private/corpus')},
    {'captures': Path('/private/captures')}, {'approved_gate': Path('/private/gate')},
    {'supervisor_cleanup': False}, {'bounded_profile': False},
])
def test_long_synthetic_soak_rejects_other_profiles_before_resource_creation(change):
    options = {'enabled': True, 'bootstrap': True, 'gate_only': True, 'live': False,
               'bounded_profile': True, 'supervisor_cleanup': True,
               'supervisor_label': 'linux-vps-gate=0123456789abcdef',
               'deadline': stand.time.time() + 14 * 60, 'variant': None,
               'quality_profile': False, 'quality_phase': None, 'corpus': None,
               'captures': None, 'approved_gate': None, 'case_ids': []}
    options.update(change)
    with pytest.raises(stand.StandError, match='synthetic_soak_'):
        stand.validate_synthetic_soak_options(**options)


def test_synthetic_overload_gate_checks_barrier_readiness_and_post_taint_allocation(tmp_path):
    instance = stand.Stand.__new__(stand.Stand)
    instance.bounded_profile = True
    instance.run_dir = tmp_path
    instance.observer = 'owned-observer'
    instance.phase = 'stability'
    instance.begin_phase = lambda name, seconds: setattr(instance, 'phase', (name, seconds))
    (tmp_path/'relay-arm').mkdir()
    report = tmp_path/'relay-report'
    report.mkdir()
    counters = {'overload_barriers': 1, 'notifications_injected': 257,
                'cleanup_confirmed': True, 'owned_context_allocations': 2,
                'owned_target_allocations': 3}
    (report/'metrics.json').write_text(json.dumps(counters))
    calls = []
    def docker(*args, **kwargs):
        calls.append((args, kwargs))
        script = args[4]
        if 'health/ready' in script:
            return json.dumps({'status':503,'ready':False,'browser':False})
        if args[6] == 'synthetic-overload':
            return json.dumps({'status':200,'outcome':'rejected',
                               'error':'native_browser_unavailable','chars':0})
        if args[6] == 'synthetic-post-taint':
            return json.dumps({'status':503,'error':'unavailable'})
        raise AssertionError('unexpected request')
    instance.docker = docker
    result = instance.synthetic_overload_gate()
    assert result == {'overload_rejected':True, 'barrier_count':1,
                      'notifications_injected':257, 'cleanup_confirmed':True,
                      'post_taint_rejected':True}
    assert instance.phase == ('synthetic_overload', 120)
    assert len(calls) == 3
    assert json.loads((tmp_path/'overload.json').read_text())['error'] == 'native_browser_unavailable'


def test_worker_recovery_gate_requires_terminal_exit_and_keeps_browser_generation(tmp_path):
    instance=stand.Stand.__new__(stand.Stand)
    instance.stage=tmp_path
    instance.run_dir=tmp_path
    instance.observer='owned-observer'
    instance.worker='news-native-shadow-0123456789-worker'
    instance.cloak='owned-cloak'
    instance.prefix='news-native-shadow-0123456789'
    instance.supervisor_label='linux-vps-gate=0123456789abcdef'
    instance.begin_phase=lambda name,seconds: setattr(instance,'phase',(name,seconds))
    instance.check_budget=lambda: None
    instance.wait_http=lambda *_args: None
    (tmp_path/'relay-arm').mkdir()
    report=tmp_path/'relay-report'
    report.mkdir()
    metrics={'overload_barriers':1,'notifications_injected':257,'cleanup_confirmed':True,
             'owned_context_allocations':2,'owned_target_allocations':3}
    (report/'metrics.json').write_text(json.dumps(metrics))
    generation='a'*64
    calls=[]
    def docker(*args,**kwargs):
        calls.append(args)
        if args[0]=='inspect':
            if args[-1]=='owned-cloak':
                return 'cloak-id|true|0|false|'+instance.prefix+'|0123456789abcdef'
            return ('/'+instance.worker+'|false|0|1|0|false|'+instance.prefix+
                    '|0123456789abcdef')
        if args[0]=='start':
            return instance.worker
        if 'synthetic-overflow-terminal' in args:
            return json.dumps({'status':200,'outcome':'rejected',
                               'error':'native_browser_unavailable','chars':0})
        if 'synthetic-while-worker-stopped' in args:
            return json.dumps({'status':None,'error':'unavailable','chars':0,
                               'transport_unavailable':True})
        if 'health/ready' in args[-1]:
            return json.dumps({'status':200,'ready':True,'browser':True,
                               'native_browser':True})
        if 'synthetic-after-worker-restart' in args:
            return json.dumps({'outcome':'success','cookie_confirmed':True})
        if 'json/version' in args[-1]:
            return json.dumps({'browser_generation':generation})
        raise AssertionError(args)
    instance.docker=docker
    post='controlled request'
    receipt=instance.synthetic_worker_recovery_gate(post,generation)
    assert receipt['next_article_succeeded'] is True
    assert receipt['browser_generation_unchanged'] is True
    assert ('start',instance.worker) in calls
    assert json.loads((tmp_path/'worker-recovery.json').read_text())==receipt
    assert not (tmp_path/'.worker-recovery-active').exists()


def test_stopped_worker_request_script_reports_only_connection_refused(tmp_path):
    with socket.socket() as listener:
        listener.bind(('127.0.0.1',0))
        port=listener.getsockname()[1]
    script=stand.Stand.stopped_worker_request_script().replace(
        '127.0.0.1:8080',f'127.0.0.1:{port}')
    environment=dict(os.environ,EXTRACTOR_TEST_TOKEN='synthetic-test-token')
    result=subprocess.run([sys.executable,'-c',script,'js-article','synthetic-stopped-worker'],
                          capture_output=True,text=True,env=environment,timeout=5)
    assert result.returncode==0, result.stderr
    value=json.loads(result.stdout)
    assert value=={'status':None,'outcome':None,'error':'unavailable','chars':0,
                   'cookie_confirmed':False,'transport_unavailable':True}


def _controlled_gate_stand(tmp_path, monkeypatch, *, wire_ok=True, ready=True, crash_ok=True,
                           crash_ready=True, generation_changed=False):
    instance = stand.Stand.__new__(stand.Stand)
    instance.stage = tmp_path
    instance.run_dir = tmp_path
    instance.observer = 'owned-observer'
    instance.bounded_profile = True
    instance.anchors = {'gateway': {'name': 'owned-gateway', 'ip': '172.22.0.4'},
                        'control': {'name': 'owned-control', 'ip': '172.22.0.3'}}
    instance.verify_ns = lambda *_args: 'owned-pid'
    instance.wait_listener = lambda *args: events.append(('listener-ready', *args))
    instance.wait_http = lambda *_args: None
    instance.check_budget = lambda: None
    events = []
    generation_calls = 0

    def run(name, _image, _args, **_kwargs):
        events.append(('start-origin', name))
        (tmp_path / 'origin-proof.jsonl').write_text('')
        return 'owned-origin'

    instance.run = run

    def docker(*args, **_kwargs):
        nonlocal generation_calls
        if len(args) >= 5 and args[:4] == ('exec', '-d', instance.observer, 'python'):
            events.append(('controlled-crash-helper-started',))
            receipt=tmp_path/'controlled-crash.json'
            receipt.write_text(json.dumps({'owned_target_selected':True,'crash_requested':True})+'\n')
            receipt.chmod(0o600)
            return ''
        if len(args) == 5 and 'browser_generation' in args[4]:
            generation_calls += 1
            return json.dumps({'browser_generation': ('b' if generation_changed and generation_calls>1 else 'a')*64})
        if len(args) == 5 and 'health/ready' in args[4]:
            events.append(('ready',))
            if not ready or (not crash_ready and ('request','controlled-crash') in events):
                return json.dumps({'status': 503, 'ready': False,
                                   'capabilities': {'browser': False, 'native_browser': False}})
            return json.dumps({'status': 200, 'ready': True,
                               'capabilities': {'browser': True, 'native_browser': True}})
        route = args[5]
        events.append(('request', route))
        if route == 'js-article':
            return json.dumps({'status': 200, 'outcome': 'success', 'error': None,
                               'chars': 100, 'cookie_confirmed': True})
        if route == 'wire-limit':
            error = 'resource_limit' if wire_ok else 'native_browser_unavailable'
            return json.dumps({'status': 200, 'outcome': 'rejected', 'error': error,
                               'chars': 0, 'cookie_confirmed': False})
        if route == 'controlled-crash':
            return json.dumps({'status': 200, 'outcome': 'failed',
                               'error': 'native_browser_target_crashed' if crash_ok else 'native_browser_unavailable',
                               'chars': 0, 'cookie_confirmed': False})
        expected = {'redirect-private': 'native_browser_policy_denied',
                    'origin-auth': 'native_browser_http_rejected'}
        return json.dumps({'status': 200, 'outcome': 'rejected',
                           'error': expected.get(route, 'public_url_denied'),
                           'chars': 0, 'cookie_confirmed': False})

    instance.docker = docker
    instance.synthetic_overload_gate = lambda _post: events.append(('overload',))
    return instance, events


def test_controlled_gate_wire_limit_recovers_on_same_browser_before_overload(tmp_path, monkeypatch):
    instance, events = _controlled_gate_stand(tmp_path, monkeypatch)
    results = instance._controlled_browser_routes()
    requests = [event[1] for event in events if event[0] == 'request']
    assert requests[:3] == ['js-article', 'wire-limit', 'js-article']
    wire_index = events.index(('request', 'wire-limit'))
    ready_index = events.index(('ready',), wire_index + 1)
    recovery_index = events.index(('request', 'js-article'), wire_index + 1)
    assert wire_index < ready_index < recovery_index < events.index(('overload',))
    assert [result['case'] for result in results[:3]] == [
        'js-article', 'wire-limit', 'js-article-after-wire-limit']
    assert results[1]['error'] == 'resource_limit' and results[1]['chars'] == 0
    assert results[2]['outcome'] == 'success' and results[2]['cookie_confirmed'] is True


@pytest.mark.parametrize('worker_recovery_gate', [False, True])
def test_production_core_extractor_gate_is_scoped_to_worker_recovery_profile(
        tmp_path, monkeypatch, worker_recovery_gate):
    instance, events = _controlled_gate_stand(tmp_path, monkeypatch)
    instance.worker_recovery_gate=worker_recovery_gate
    instance.production_core_extractor_native_gate=lambda value: events.append(('core-native-gate',value))
    instance.synthetic_worker_recovery_gate=lambda *_args: events.append(('worker-recovery',))
    instance._controlled_browser_routes()
    gate_events=[event for event in events if event[0]=='core-native-gate']
    if worker_recovery_gate:
        assert gate_events==[('core-native-gate','a'*64)]
        listener_index=events.index(('listener-ready','gateway',443,0))
        assert listener_index<events.index(gate_events[0])
        assert events.index(gate_events[0])<next(i for i,event in enumerate(events)
                                                   if event[0]=='request')
        assert ('worker-recovery',) in events
    else:
        assert gate_events==[]
        assert ('overload',) in events


@pytest.mark.parametrize(('wire_ok', 'ready', 'error'), [
    (False, True, 'native_wire_limit_gate_failed'),
    (True, False, 'native_wire_limit_readiness_lost'),
])
def test_controlled_gate_does_not_dispatch_recovery_after_failed_resource_limit(
        tmp_path, monkeypatch, wire_ok, ready, error):
    instance, events = _controlled_gate_stand(tmp_path, monkeypatch, wire_ok=wire_ok, ready=ready)
    with pytest.raises(stand.StandError, match=error):
        instance._controlled_browser_routes()
    requests = [event[1] for event in events if event[0] == 'request']
    assert requests == ['js-article', 'wire-limit']
    assert ('overload',) not in events


@pytest.mark.parametrize('interpreter', [None, 'python', 'python3'])
@pytest.mark.parametrize('exit_code', [0, 7])
def test_detached_runner_wrapper_records_system_exit_and_private_output(tmp_path, exit_code, interpreter):
    command_id='a'*32
    script=tmp_path/'runner.py'
    script.write_text("print('private output')\nimport sys\nsys.exit("+str(exit_code)+")\n")
    command=tmp_path/('runner-command-'+command_id+'.json')
    argv=([interpreter] if interpreter else [])+[str(script),'--fixed']
    command.write_text(json.dumps({'command_id':command_id,'args':argv}))
    command.chmod(0o600)
    wrapper=stand.RUNNER_WRAPPER.replace("Path('/out')",'Path('+repr(str(tmp_path))+')')
    result=subprocess.run([sys.executable,'-c',wrapper,command_id],capture_output=True,text=True,timeout=5)
    assert result.returncode == 0
    assert not command.exists()
    receipt_path=tmp_path/('runner-status-'+command_id+'.json')
    assert stand.read_runner_receipt(receipt_path,command_id) == exit_code
    assert receipt_path.stat().st_mode & 0o777 == 0o600
    assert 'private output' in (tmp_path/'runner-private.log').read_text()


def test_detached_runner_wrapper_maps_exception_to_bounded_failure(tmp_path):
    command_id='b'*32
    script=tmp_path/'runner.py'
    script.write_text("raise RuntimeError('private failure')\n")
    command=tmp_path/('runner-command-'+command_id+'.json')
    command.write_text(json.dumps({'command_id':command_id,'args':[str(script)]}))
    command.chmod(0o600)
    wrapper=stand.RUNNER_WRAPPER.replace("Path('/out')",'Path('+repr(str(tmp_path))+')')
    result=subprocess.run([sys.executable,'-c',wrapper,command_id],capture_output=True,text=True,timeout=5)
    assert result.returncode == 0
    assert stand.read_runner_receipt(tmp_path/('runner-status-'+command_id+'.json'),command_id) == 1
    assert 'private failure' in (tmp_path/'runner-private.log').read_text()


@pytest.mark.parametrize('receipt_kind', ['stale', 'oversized', 'symlink'])
def test_detached_runner_receipt_fails_closed(tmp_path, receipt_kind):
    command_id='c'*32
    receipt=tmp_path/'receipt.json'
    if receipt_kind == 'stale':
        receipt.write_text(json.dumps({'command_id':'d'*32,'exit_code':0,'state':'complete'}))
        receipt.chmod(0o600)
    elif receipt_kind == 'oversized':
        receipt.write_text(' '*1025)
        receipt.chmod(0o600)
    else:
        target=tmp_path/'target.json'
        target.write_text(json.dumps({'command_id':command_id,'exit_code':0,'state':'complete'}))
        target.chmod(0o600)
        receipt.symlink_to(target)
    with pytest.raises(stand.StandError,match='runner_receipt_invalid'):
        stand.read_runner_receipt(receipt,command_id)


def test_monitored_exec_uses_short_detached_docker_command_and_receipt(tmp_path, monkeypatch):
    instance=stand.Stand.__new__(stand.Stand)
    instance.run_dir=tmp_path
    instance.observer='owned-observer'
    checks=[]
    instance.check_runtime=lambda: checks.append('checked')
    calls=[]
    def docker_start(args,**kwargs):
        calls.append((args,kwargs))
        command_id=args[-1]
        receipt=tmp_path/('runner-status-'+command_id+'.json')
        receipt.write_text(json.dumps({'command_id':command_id,'exit_code':0,'state':'complete'}))
        receipt.chmod(0o600)
        return subprocess.CompletedProcess(args,0,'','')
    monkeypatch.setattr(stand.subprocess,'run',docker_start)
    monkeypatch.setattr(stand.subprocess,'Popen',lambda *_a,**_k: pytest.fail('persistent host child used'))
    monkeypatch.setattr(stand.time,'sleep',lambda _seconds: None)
    instance.monitored_exec(['python','/bench/scripts/run_native_shadow.py','--case','case-0001'])
    assert calls[0][0][:4] == ['docker','exec','-d','owned-observer']
    assert calls[0][0][-2] == stand.RUNNER_WRAPPER
    assert calls[0][1]['timeout'] == 10
    assert len(checks) == 2
    assert not list(tmp_path.glob('runner-command-*.json'))


def test_monitored_exec_never_serializes_stand_token(tmp_path, monkeypatch):
    instance=stand.Stand.__new__(stand.Stand)
    instance.run_dir=tmp_path
    instance.observer='owned-observer'
    instance.token='sensitive-run-token'
    monkeypatch.setattr(stand.subprocess,'run',lambda *_a,**_k: pytest.fail('runner started with token'))
    with pytest.raises(stand.StandError,match='runner_arguments_invalid'):
        instance.monitored_exec(['python','/bench/run.py','--header','sensitive-run-token'])


def test_monitored_exec_propagates_deadline_when_detached_receipt_missing(tmp_path, monkeypatch):
    instance=stand.Stand.__new__(stand.Stand)
    instance.run_dir=tmp_path
    instance.observer='owned-observer'
    starts=[]
    monkeypatch.setattr(stand.subprocess,'run',lambda *a,**k: starts.append((a,k)) or
                        subprocess.CompletedProcess(a,0,'',''))
    monkeypatch.setattr(stand.time,'sleep',lambda _seconds: None)
    checks=[]
    def expired():
        checks.append(True)
        raise stand.StandError('runner_phase_timeout')
    instance.check_runtime=expired
    with pytest.raises(stand.StandError,match='runner_phase_timeout'):
        instance.monitored_exec(['python','/bench/scripts/run_native_shadow.py'])
    assert len(starts)==1 and starts[0][0][0][2]=='-d'
    assert not list(tmp_path.glob('runner-status-*.json'))
    assert not list(tmp_path.glob('runner-command-*.json'))


def test_owned_crash_requires_recovery_and_next_success_before_overload(tmp_path, monkeypatch):
    instance, events = _controlled_gate_stand(tmp_path, monkeypatch)
    results = instance._controlled_browser_routes()
    assert [row['case'] for row in results[:5]] == [
        'js-article', 'wire-limit', 'js-article-after-wire-limit',
        'controlled-crash', 'js-article-after-crash']
    assert results[3]['error'] == 'native_browser_target_crashed'
    proof = json.loads((tmp_path/'crash-recovery.json').read_text())
    assert all(proof[key] is True for key in ('owned_page_crash_observed', 'failed_output_empty',
        'readiness_recovered', 'browser_generation_unchanged', 'next_article_succeeded',
        'controlled_helper_receipt_verified'))
    assert proof['failed_article_retried'] is False
    helper_index=events.index(('controlled-crash-helper-started',))
    crash_index=events.index(('request', 'controlled-crash'))
    assert helper_index < crash_index
    assert events[-1] == ('overload',)


def test_controlled_crash_helper_selects_only_unique_owned_fixture_page():
    compile(stand.CONTROLLED_CRASH_HELPER, '<controlled-crash-helper>', 'exec')
    tree=ast.parse(stand.CONTROLLED_CRASH_HELPER)
    selector=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='select_target')
    namespace={}
    exec(compile(ast.Module(body=[selector],type_ignores=[]),'<controlled-crash-selector>','exec'),namespace)
    select=namespace['select_target']
    owned=['context-owned']
    fixture={'type':'page','url':'https://native-fixture.test/controlled-crash',
             'browserContextId':'context-owned','targetId':'target-owned'}
    assert select(owned,[fixture]) == ('selected','target-owned')
    assert select(owned,[dict(fixture,browserContextId='context-foreign')]) == ('target_waiting',None)
    assert select([], [fixture]) == ('target_waiting',None)
    assert select(owned,[fixture,dict(fixture,targetId='target-duplicate')]) == ('target_ambiguous',None)
    assert select(owned,[dict(fixture,type='worker')]) == ('target_waiting',None)
    assert 'range(100)' in stand.CONTROLLED_CRASH_HELPER
    assert 'asyncio.wait_for(run(),timeout=10)' in stand.CONTROLLED_CRASH_HELPER
    assert "'method':'Page.crash'" in stand.CONTROLLED_CRASH_HELPER


def test_controlled_crash_receipt_rejects_unbounded_or_unknown_results(tmp_path):
    path=tmp_path/'controlled-crash.json'
    path.write_text(json.dumps({'owned_target_selected':True,'crash_requested':True})+'\n')
    path.chmod(0o600)
    assert stand.read_controlled_crash_receipt(path) is True
    path.write_text(json.dumps({'failure_code':'target_ambiguous'})+'\n')
    with pytest.raises(stand.StandError,match='native_crash_helper_failed'):
        stand.read_controlled_crash_receipt(path)
    path.write_text(json.dumps({'failure_code':'attacker-secret-token'})+'\n')
    with pytest.raises(stand.StandError,match='native_crash_receipt_invalid'):
        stand.read_controlled_crash_receipt(path)
    path.write_text(json.dumps({'owned_target_selected':True,'crash_requested':True,'extra':'private'})+'\n')
    with pytest.raises(stand.StandError,match='native_crash_receipt_invalid'):
        stand.read_controlled_crash_receipt(path)


@pytest.mark.parametrize(('kwargs', 'error'), [
    ({'crash_ok': False}, 'native_crash_gate_failed'),
    ({'crash_ready': False}, 'native_crash_readiness_lost'),
    ({'generation_changed': True}, 'native_crash_generation_changed'),
])
def test_uncertain_crash_cannot_dispatch_next_article(tmp_path, monkeypatch, kwargs, error):
    instance, events = _controlled_gate_stand(tmp_path, monkeypatch, **kwargs)
    with pytest.raises(stand.StandError, match=error):
        instance._controlled_browser_routes()
    assert [event for event in events if event == ('request', 'js-article')] == [
        ('request', 'js-article'), ('request', 'js-article')]
    assert ('overload',) not in events
    assert not (tmp_path/'crash-recovery.json').exists()
