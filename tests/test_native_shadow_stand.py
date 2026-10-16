import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location('native_shadow_stand', Path(__file__).parents[1]/'scripts/native_shadow_stand.py')
stand = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stand)


def policy(role, bootstrap=False):
    return stand.nft_policy(role,'172.22.0.2','172.22.0.3','172.22.0.4',bootstrap=bootstrap,host_ips=['65.109.0.90'])


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


def test_first_public_smoke_requires_c_single_case_before_b(tmp_path):
    corpus=tmp_path/'corpus.json';corpus.write_text('{"version":1}')
    instance=stand.Stand.__new__(stand.Stand)
    instance.check_public_smoke('C',corpus,tmp_path,['case-0001'])
    for variant,ids in [('B',[]),('B',['case-0001']),('C',[]),('C',['case-0001','case-0002'])]:
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
    for variant in ('B','C'):
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
    for variant in ('B','C'):
        instance.check_public_smoke(variant,corpus,tmp_path)
    prior.write_text('{broken')
    with pytest.raises(stand.StandError,match='first_public_native_tls'):
        instance.check_public_smoke('B',corpus,tmp_path)


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
    for variant in ('B','C'):
        if allowed:
            instance.check_public_smoke(variant,corpus,tmp_path)
        else:
            with pytest.raises(stand.StandError,match='first_public_native_tls'):
                instance.check_public_smoke(variant,corpus,tmp_path)


@pytest.mark.parametrize('b_change',[
    None, {'corpus_sha256':'stale'}, {'fatal_error':'browser_transport_unavailable'},
    {'status':'incomplete'}, {'variant':'C'}, {'rows':[]},
    {'rows':[{'outcome':'failed','error':'transport_error','upstream_status':None}]},
])
def test_later_c_failure_only_permits_verified_b_continuation(tmp_path,b_change):
    import hashlib
    corpus=tmp_path/'corpus.json';corpus.write_bytes(b'{"version":1}')
    first={'id':'case-0001','outcome':'rejected','error':'native_browser_http_rejected',
           'upstream_status':403}
    base={'schema_version':1,'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest()}
    failed_c={**base,'variant':'C','status':'incomplete','fatal_error':'native_browser_unavailable',
              'rows':[first,{'id':'case-0008','outcome':'failed','error':'native_browser_unavailable'}]}
    c_path=tmp_path/'report-C.json';c_path.write_text(json.dumps(failed_c))
    b_path=tmp_path/'report-B.json'
    healthy_b={**base,'variant':'B','status':'complete','fatal_error':None,
               'rows':[first]+[{'id':f'case-{i:04d}','outcome':'success'} for i in range(2,6)]}
    if b_change is not None:
        healthy_b.update(b_change)
        b_path.write_text(json.dumps(healthy_b))
    instance=stand.Stand.__new__(stand.Stand)
    for variant in ('B','C'):
        with pytest.raises(stand.StandError,match='first_public_native_tls'):
            instance.check_public_smoke(variant,corpus,tmp_path)


def test_b_continues_after_later_c_failure_without_rewriting_evidence(tmp_path):
    import hashlib
    corpus=tmp_path/'corpus.json';corpus.write_bytes(b'{"version":1}')
    first={'id':'case-0001','outcome':'rejected','error':'native_browser_http_rejected',
           'upstream_status':403}
    base={'schema_version':1,'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest()}
    c_path=tmp_path/'report-C.json';b_path=tmp_path/'report-B.json'
    c_report={**base,'variant':'C','status':'incomplete','fatal_error':'native_browser_unavailable',
              'rows':[first,{'id':'case-0008','outcome':'failed','error':'native_browser_unavailable'}]}
    b_report={**base,'variant':'B','status':'complete','fatal_error':None,
              'rows':[first]+[{'id':f'case-{i:04d}','outcome':'success'} for i in range(2,6)]}
    c_path.write_text(json.dumps(c_report));b_path.write_text(json.dumps(b_report))
    original=(c_path.read_bytes(),b_path.read_bytes())
    instance=stand.Stand.__new__(stand.Stand)
    instance.check_public_smoke('B',corpus,tmp_path)
    with pytest.raises(stand.StandError,match='first_public_native_tls'):
        instance.check_public_smoke('C',corpus,tmp_path)
    assert original==(c_path.read_bytes(),b_path.read_bytes())
    for fatal in (None,''):
        c_report['fatal_error']=fatal
        c_path.write_text(json.dumps(c_report))
        with pytest.raises(stand.StandError,match='first_public_native_tls'):
            instance.check_public_smoke('B',corpus,tmp_path)
    c_report['fatal_error']='native_browser_unavailable'
    c_report['rows'][0]={**first,'error':'transport_error','upstream_status':None}
    c_path.write_text(json.dumps(c_report))
    for variant in ('B','C'):
        with pytest.raises(stand.StandError,match='first_public_native_tls'):
            instance.check_public_smoke(variant,corpus,tmp_path)
