"""Isolated native shadow namespaces; never modifies a host firewall or production service."""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time

APP = 'ghcr.io/dzarlax-ai/rss-summarizer@sha256:b6abbb428543e846d0310b86ff41236a6df028985a00ebfe4714357d22e67060'
CLOAK = 'cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4'
SPECIAL4 = ('0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8',
            '169.254.0.0/16', '172.16.0.0/12', '192.0.0.0/24', '192.0.2.0/24',
            '192.88.99.0/24', '192.168.0.0/16', '198.18.0.0/15', '198.51.100.0/24',
            '203.0.113.0/24', '224.0.0.0/4', '240.0.0.0/4')
TABLE = 'native_shadow_guard'


class StandError(RuntimeError):
    pass


def export_private_corpus(stage,gate_dir):
    """Use a capped one-shot reader, never execute a payload-heavy query in news_web's cgroup."""
    stage=Path(stage).resolve()
    gate=json.loads((Path(gate_dir)/'manifest.json').read_text())
    trap=json.loads((Path(gate_dir)/'browser-trap.json').read_text())
    if (gate.get('state')!='cleaned' or gate.get('previous_phase')!='controlled_browser_gate_passed'
            or gate.get('bootstrap') is not True
            or trap.get('accepted')!=0):
        raise StandError('export_network_gate_not_passed')
    if mem_available() < 2 * 1024**3:
        raise StandError('insufficient_host_memory')
    private=stage/'shadow-private'
    private.mkdir(mode=0o700,exist_ok=True)
    os.chmod(private,0o700)
    if (private/'corpus-run').exists():
        raise StandError('corpus_already_exported')
    inspected=json.loads(command(['docker','inspect','news_web']))[0]
    if inspected['Config']['Image']!=APP:
        raise StandError('production_image_changed')
    environment=dict(value.split('=',1) for value in inspected['Config']['Env'] if '=' in value)
    database_url=environment.get('DATABASE_URL')
    if not database_url or '\n' in database_url or '\r' in database_url:
        raise StandError('database_environment_missing_or_invalid')
    secret=private/'export-db.env'
    fd=os.open(secret,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as stream: stream.write('DATABASE_URL='+database_url+'\n')
    name='news-native-shadow-export-'+secrets.token_hex(5)
    label='native-shadow-export='+name
    try:
        result=command(['docker','run','--rm','--name',name,'--label',label,
                        '--network','container:news_web','--no-healthcheck','--restart=no',
                        '--read-only','--cap-drop=ALL','--security-opt=no-new-privileges:true',
                        '--memory','384m','--cpus','0.25','--pids-limit','32',
                        '--tmpfs','/tmp:rw,nosuid,nodev,size=16m',
                        '--env-file',str(secret),'-v',str(private)+':/private',
                        '-v',str(stage/'benchmark/scripts/export_native_shadow_corpus.py')+':/export.py:ro',
                        '--entrypoint','python',APP,'/export.py','--output-dir','/private/corpus-run'],timeout=90)
        manifest=json.loads((private/'corpus-run/manifest.json').read_text())
        (private/'production-export-baseline.json').write_text(json.dumps({
            'image':inspected['Config']['Image'],'started_at':inspected['State']['StartedAt'],
            'restart_count':inspected['RestartCount'],'read_only_export':True,
            'isolated_memory_limit_bytes':384*1024**2},indent=2)+'\n')
        if manifest['case_count']<1:
            raise StandError('no_eligible_public_urls')
        return private/'corpus-run/corpus.json',manifest
    finally:
        secret.unlink(missing_ok=True)
        current=subprocess.run(['docker','inspect','--format','{{index .Config.Labels "native-shadow-export"}}',name],
                               capture_output=True,text=True,timeout=10)
        if current.returncode==0 and current.stdout.strip()==name:
            command(['docker','rm','-f',name],timeout=15)


def command(args, *, data=None, timeout=30):
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise StandError('command_failed_' + Path(args[0]).name)
    return result.stdout.strip()


def checked_ip(value):
    ip = ipaddress.ip_address(value)
    if ip.version != 4:
        raise StandError('ipv4_required')
    return str(ip)


def nft_policy(role, browser, control, gateway, resolver='1.1.1.1', bootstrap=False, host_ips=()):
    browser, control, gateway, resolver = map(checked_ip, (browser, control, gateway, resolver))
    blocked = list(SPECIAL4) + [checked_ip(value) + '/32' for value in host_ips
                              if ipaddress.ip_address(value).is_global]
    common = 'meta nfproto ipv6 counter drop\nct state invalid counter drop\n'
    if role == 'browser':
        output = (f'ip daddr {gateway} tcp dport 8081 ct state {{ new, established }} counter accept\n'
                  f'ip daddr {control} tcp sport 9222 ct state established counter accept\n'
                  'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept\n'
                  'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept\n')
        incoming = (f'ip saddr {gateway} tcp sport 8081 ct state established counter accept\n'
                    f'ip saddr {control} tcp dport 9222 ct state {{ new, established }} counter accept\n'
                    'iifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } counter accept\n'
                    'iifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established counter accept\n')
    elif role == 'control':
        output = (f'ip daddr {browser} tcp dport 9222 ct state {{ new, established }} counter accept\n'
                  f'ip daddr {gateway} tcp dport 8081 ct state {{ new, established }} counter accept\n'
                  'ip daddr 127.0.0.1 tcp dport 8080 ct state { new, established } counter accept\n'
                  'ip saddr 127.0.0.1 tcp sport 8080 ct state established counter accept\n')
        incoming = (f'ip saddr {browser} tcp sport 9222 ct state established counter accept\n'
                    f'ip saddr {gateway} tcp sport 8081 ct state established counter accept\n'
                    'iifname "lo" ip saddr 127.0.0.1 tcp dport 8080 ct state { new, established } counter accept\n'
                    'iifname "lo" ip saddr 127.0.0.1 tcp sport 8080 ct state established counter accept\n')
    elif role == 'gateway':
        peers = f'{{ {browser}, {control} }}'
        output = f'ip daddr {peers} tcp sport 8081 ct state established counter accept\n'
        incoming = f'ip saddr {peers} tcp dport 8081 ct state {{ new, established }} counter accept\n'
        if bootstrap:
            output += ('oifname "lo" ip daddr 127.0.0.1 tcp dport 443 counter accept\n'
                       'oifname "lo" ip saddr 127.0.0.1 tcp sport 443 ct state established counter accept\n')
            incoming += ('iifname "lo" ip daddr 127.0.0.1 tcp dport 443 counter accept\n'
                         'iifname "lo" ip saddr 127.0.0.1 tcp sport 443 ct state established counter accept\n')
        else:
            output += (f'ip daddr {resolver} udp dport 53 counter accept\n'
                       f'ip daddr {resolver} tcp dport 53 counter accept\n'
                       'ip daddr { ' + ', '.join(blocked) + ' } counter drop\n'
                       'tcp dport 443 ct state { new, established } counter accept\n')
            incoming += (f'ip saddr {resolver} udp sport 53 ct state established counter accept\n'
                         f'ip saddr {resolver} tcp sport 53 ct state established counter accept\n'
                         'tcp sport 443 ct state established counter accept\n')
    else:
        raise StandError('invalid_role')
    return (f'table inet {TABLE} {{\nchain output {{\ntype filter hook output priority -150; policy drop;\n'
            + common + output + 'counter drop\n}\nchain input {\n'
            'type filter hook input priority -150; policy drop;\n' + common + incoming
            + 'counter drop\n}\nchain forward {\ntype filter hook forward priority -150; policy drop;\n}\n}\n')


def mem_available():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) * 1024


def probe_denied(result, delta, ip):
    address=ipaddress.ip_address(ip)
    no_route=result.get('errno') in (97,101) and (address.is_global or address.version==6)
    return not result['connected'] and (delta > 0 or no_route)


class Stand:
    def __init__(self, stage, *, bootstrap=False, deadline=None):
        self.stage = Path(stage).resolve()
        if not self.stage.is_dir() or self.stage.is_symlink():
            raise StandError('stage_missing')
        self.bootstrap = bootstrap
        self.prefix = 'news-native-shadow-' + secrets.token_hex(5)
        self.label = 'native-shadow-run=' + self.prefix
        self.containers = []
        self.networks = []
        self.anchors = {}
        self.started = time.monotonic()
        self.deadline = min(time.time()+7200,deadline) if deadline is not None else time.time()+7200
        self.phase = 'created'
        self.token = secrets.token_urlsafe(32)
        self.resolver = '1.1.1.1'
        self.run_dir = self.stage / self.prefix
        self.run_dir.mkdir(mode=0o700)
        os.chmod(self.run_dir, 0o700)
        self.resolv = self.run_dir / 'resolv.conf'
        self.resolv.write_text('nameserver ' + self.resolver + '\noptions timeout:2 attempts:1\n')
        os.chmod(self.resolv, 0o600)
        self.env_file = self.run_dir / 'private.env'
        self.env_file.write_text('EXTRACTOR_TEST_TOKEN=' + self.token + '\nEGRESS_TOKEN=' + self.token
                                 + '\nEXTRACTOR_SERVICE_TOKEN=' + self.token
                                 + '\nEXTRACTOR_NATIVE_PROXY_TOKEN=' + self.token + '\n')
        os.chmod(self.env_file, 0o600)

    def docker(self, *args, **kwargs):
        return command(['docker', *args], **kwargs)

    def owned(self, name):
        label = self.docker('inspect', '--format', '{{index .Config.Labels "native-shadow-run"}}', name)
        if label != self.prefix:
            raise StandError('resource_ownership_mismatch')

    def run(self, suffix, image, args, *, network, memory='32m', cpu='0.25', pids=64,
            mounts=(), env=(), entrypoint='sleep', readonly=True, extra=()):
        name = self.prefix + '-' + suffix
        command_args = ['run', '-d', '--name', name, '--label', self.label, '--network', network,
                        '--no-healthcheck', '--restart=no', '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
                        '--cpus', cpu, '--memory', memory, '--pids-limit', str(pids),
                        '--log-opt', 'max-size=10m', '--log-opt', 'max-file=2']
        if readonly:
            command_args += ['--read-only', '--tmpfs', '/tmp:rw,nosuid,nodev,size=32m']
        for value in mounts:
            command_args += ['-v', value]
        for value in env:
            command_args += ['-e', value]
        command_args += list(extra) + ['--entrypoint', entrypoint, image] + list(args)
        self.containers.append(name)
        self.docker(*command_args)
        self.owned(name)
        return name

    def verify_ns(self, name, anchor):
        self.owned(name)
        self.owned(anchor)
        def details(value):
            pid, restarts = self.docker('inspect', '--format', '{{.State.Pid}} {{.RestartCount}}', value).split()
            if int(pid) <= 1 or int(restarts) != 0:
                raise StandError('namespace_process_changed')
            return os.stat('/proc/' + pid + '/ns/net').st_ino, pid
        inode, pid = details(name)
        expected, _ = details(anchor)
        if inode != expected or inode == os.stat('/proc/1/ns/net').st_ino:
            raise StandError('namespace_identity_mismatch')
        return pid

    def ns(self, role, *args, data=None, timeout=30):
        anchor = self.anchors[role]['name']
        pid = self.verify_ns(anchor, anchor)
        return command(['nsenter', '-t', pid, '-n', '--', *args], data=data, timeout=timeout)

    def wait_listener(self,role,port,uid):
        for _ in range(60):
            pid=self.verify_ns(self.anchors[role]['name'],self.anchors[role]['name'])
            rows=Path('/proc/'+pid+'/net/tcp').read_text().splitlines()[1:]
            if any(row.split()[1]=='0100007F:'+format(port,'04X') and row.split()[3]=='0A'
                   and int(row.split()[7])==uid for row in rows):
                return
            self.check_budget()
            time.sleep(1)
        raise StandError('private_listener_not_ready')

    def setup(self):
        if mem_available() < 2 * 1024**3:
            raise StandError('insufficient_host_memory')
        for kind in (('private',) if self.bootstrap else ('private','egress')):
            name = self.prefix + '-' + kind
            args = ['network', 'create', '--label', self.label]
            if kind == 'private':
                args += ['--internal']
            self.networks.append(name)
            self.docker(*args, name)
        for role in ('browser', 'control', 'gateway'):
            network = self.networks[1] if role == 'gateway' and not self.bootstrap else self.networks[0]
            name = self.run(role + '-anchor', APP, ['7200'], network=network,
                            mounts=[str(self.resolv) + ':/etc/resolv.conf:ro'] if role == 'gateway' else [])
            if role == 'gateway' and not self.bootstrap:
                self.docker('network', 'connect', self.networks[0], name)
            raw = json.loads(self.docker('inspect', '--format', '{{json .NetworkSettings.Networks}}', name))
            ip = checked_ip(raw[self.networks[0]]['IPAddress'])
            self.anchors[role] = {'name': name, 'ip': ip}
        host = json.loads(command(['ip', '-j', '-4', 'address', 'show']))
        host_ips = [address['local'] for interface in host for address in interface.get('addr_info', [])]
        for role in self.anchors:
            policy = nft_policy(role, *(self.anchors[key]['ip'] for key in ('browser', 'control', 'gateway')),
                                self.resolver, self.bootstrap, host_ips)
            (self.run_dir / (role + '.nft')).write_text(policy)
            self.ns(role, 'nft', '-f', '-', data=policy)
        self.write_manifest('namespaces_guarded')

    def write_manifest(self, state):
        previous=self.phase
        self.phase=state
        value = {'state': state, 'previous_phase':previous, 'bootstrap': self.bootstrap, 'prefix': self.prefix,
                 'anchors': self.anchors, 'containers': self.containers, 'networks': self.networks,
                 'started_unix_seconds': int(time.time() - (time.monotonic()-self.started))}
        (self.run_dir / 'manifest.json').write_text(json.dumps(value, indent=2) + '\n')

    def cgroup(self, name):
        self.owned(name)
        pid = self.docker('inspect', '--format', '{{.State.Pid}}', name)
        path = next(line[3:] for line in Path('/proc/' + pid + '/cgroup').read_text().splitlines()
                    if line.startswith('0::'))
        return '/host-cg' + path

    def check_budget(self):
        if time.time() >= self.deadline or time.monotonic() - self.started > 7200:
            raise StandError('stand_deadline')
        if mem_available() < 1024**3:
            raise StandError('host_memory_watchdog')

    def check_runtime(self):
        self.check_budget()
        if not self.containers:
            raise StandError('runtime_empty')
        lines=self.docker('inspect','--format','{{.State.Running}} {{.RestartCount}} {{.State.OOMKilled}}',
                          *self.containers).splitlines()
        if len(lines)!=len(self.containers) or any(row.split()!=['true','0','false'] for row in lines):
            raise StandError('runtime_stopped_restarted_or_oom')

    def monitored_exec(self, args):
        """Watch host memory and every owned runtime process during long runner commands."""
        logfile=self.run_dir/'runner-private.log'
        with logfile.open('a') as log:
            proc=subprocess.Popen(['docker','exec',self.observer,*args],stdout=log,stderr=log)
            try:
                while proc.poll() is None:
                    self.check_runtime()
                    time.sleep(2)
                self.check_runtime()
                if proc.returncode:
                    raise StandError('runner_failed')
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try: proc.wait(timeout=3)
                    except subprocess.TimeoutExpired: proc.kill(); proc.wait()

    def resource_groups(self,variant):
        groups={'cloak':self.cloak,'gateway':self.gateway,'relay':self.relay,
                'browser_anchor':self.anchors['browser']['name'],
                'control_anchor':self.anchors['control']['name'],
                'gateway_anchor':self.anchors['gateway']['name']}
        if variant=='B': groups['app']=self.observer
        else: groups.update(worker=self.worker,observer=self.observer)
        return groups

    def idle_gates(self,variant,label):
        rows=[]
        groups=self.resource_groups(variant)
        initial={}
        for name,container in groups.items():
            path=Path(self.cgroup(container).replace('/host-cg','/sys/fs/cgroup',1))
            events=dict(line.split() for line in (path/'memory.events').read_text().splitlines())
            initial[name]=(int(events['oom']),int(events['oom_kill']))
        idle_started=time.monotonic()
        for seconds in (5,30,60):
            until=idle_started+seconds
            while time.monotonic()<until:
                self.check_runtime()
                time.sleep(min(2,max(0,until-time.monotonic())))
            self.check_runtime()
            row={'idle_seconds':seconds,'cgroups':{}}
            for name,container in groups.items():
                path=Path(self.cgroup(container).replace('/host-cg','/sys/fs/cgroup',1))
                events=dict(line.split() for line in (path/'memory.events').read_text().splitlines())
                if (int(events['oom']),int(events['oom_kill']))!=initial[name]:
                    raise StandError('idle_oom_or_counter_reset')
                cpu=dict(line.split() for line in (path/'cpu.stat').read_text().splitlines())
                row['cgroups'][name]={'memory_current_bytes':int((path/'memory.current').read_text()),
                                      'pids':int((path/'pids.current').read_text()),
                                      'cpu_usage_usec':int(cpu['usage_usec']),
                                      'oom_events':int(events['oom']),'oom_kill_events':int(events['oom_kill'])}
            rows.append(row)
            (self.run_dir/('idle-'+label+'.json')).write_text(json.dumps(rows,indent=2)+'\n')
        return rows

    def check_public_smoke(self,variant,corpus,captures,case_ids=()):
        import hashlib

        prior=captures/'report-C.json'
        if not prior.exists() and not prior.is_symlink():
            if variant!='C':
                raise StandError('first_public_native_tls_not_confirmed')
            if len(case_ids)!=1:
                raise StandError('first_public_request_must_be_single_case')
            return
        try:
            if corpus.stat().st_size>64*1024**2:
                raise ValueError
            digest=hashlib.sha256(corpus.read_bytes()).hexdigest()

            def read_report(path,expected_variant):
                if path.is_symlink() or not path.is_file() or path.stat().st_size>16*1024**2:
                    raise ValueError
                report=json.loads(path.read_text())
                if (not isinstance(report,dict) or report.get('schema_version')!=1
                        or report.get('variant')!=expected_variant
                        or report.get('corpus_sha256')!=digest
                        or not isinstance(report.get('rows'),list) or not report['rows']
                        or not isinstance(report['rows'][0],dict)):
                    raise ValueError
                first=report['rows'][0]
                status=first.get('upstream_status')
                # An origin HTTP rejection proves TLS independently of extraction.
                origin_response=(type(status) is int and 200<=status<=599 and status!=407
                                 and first.get('error') in {'native_browser_http_rejected',
                                     'upstream_auth_required','upstream_rate_limited'})
                if first.get('outcome')!='success' and not origin_response:
                    raise ValueError
                return report

            existing=read_report(prior,'C')
            if existing.get('status')=='complete' and existing.get('fatal_error') is None:
                return
            # A later C failure stops C. Only an already completed independent B
            # smoke can authorize B continuation; neither report is rewritten.
            if (variant!='B' or existing.get('status')!='incomplete' or len(existing['rows'])<2
                    or not isinstance(existing.get('fatal_error'),str) or not existing['fatal_error']):
                raise ValueError
            control=read_report(captures/'report-B.json','B')
            if control.get('status')!='complete' or control.get('fatal_error') is not None:
                raise ValueError
        except (OSError,ValueError,TypeError):
            raise StandError('first_public_native_tls_not_confirmed') from None

    def run_live(self,variant,corpus,captures,case_ids=()):
        if self.bootstrap:
            raise StandError('live_bootstrap_forbidden')
        corpus=Path(corpus)
        captures=Path(captures)
        if not corpus.is_file() or corpus.is_symlink() or not captures.is_dir() or captures.is_symlink():
            raise StandError('private_corpus_or_capture_missing')
        # These mounts are attached before any runner uses the input. No DB credentials are mounted.
        self.corpus=corpus
        self.captures=captures
        self.check_public_smoke(variant,corpus,captures,case_ids)
        self.start_runtime(variant)
        # Prove the gateway's exact resolver works; public-origin content is not fetched here.
        self.docker('exec',self.anchors['gateway']['name'],'python','-c',
                    "import socket; assert socket.getaddrinfo('example.com',443, socket.AF_INET)",timeout=10)
        args=['python','/bench/scripts/run_native_shadow.py','--variant',variant,
              '--corpus','/corpus/corpus.json','--output-dir','/captures']
        if variant=='C': args+=['--endpoint-c','http://127.0.0.1:8080']
        else: args+=['--proxy-b','http://'+self.anchors['gateway']['ip']+':8081']
        for role,container in self.resource_groups(variant).items():
            args+=['--cgroup',role+'='+self.cgroup(container)]
        for case_id in case_ids:
            if not re.fullmatch(r'case-[0-9]{4}',case_id):
                raise StandError('invalid_case_selection')
            args+=['--case',case_id]
        self.monitored_exec(args)
        self.idle_gates(variant,'after-run')
        self.write_manifest('live_batch_completed')

    def drop_packets(self, role):
        data = json.loads(self.ns(role, 'nft', '-j', 'list', 'chain', 'inet', TABLE, 'output'))
        return sum(expression['counter']['packets'] for item in data['nftables']
                   if 'rule' in item and any('drop' in term for term in item['rule']['expr'])
                   for expression in item['rule']['expr'] if 'counter' in expression)

    def raw_network_gate(self):
        """Unprivileged socket probes before browser startup, with drop counters and traps."""
        trap_code = '''import json,signal,socket,selectors,sys
s=selectors.DefaultSelector(); count=0
for port in map(int,sys.argv[1:]):
 sock=socket.socket(); sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 sock.bind(('0.0.0.0',port)); sock.listen(); sock.setblocking(False); s.register(sock,selectors.EVENT_READ)
print('ready',flush=True)
def end(*_):
 print(json.dumps({'accepted':count}),flush=True); raise SystemExit(0)
signal.signal(signal.SIGTERM,end)
while True:
 for key,_ in s.select(1):
  peer,_=key.fileobj.accept(); peer.close(); count+=1
'''
        traps = []
        reports = []
        try:
            for role, ports in [('browser',['9222','5100']),('control',['9443','8080']),('gateway',['443','9443'])]:
                pid = self.verify_ns(self.anchors[role]['name'], self.anchors[role]['name'])
                proc = subprocess.Popen(['nsenter','-t',pid,'-n','--','python3','-u','-c',trap_code,*ports],
                                        stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                traps.append((role,proc))
                if proc.stdout.readline().strip() != 'ready':
                    raise StandError('trap_start_failed')
            browser, control, gateway = (self.anchors[x]['ip'] for x in ('browser','control','gateway'))
            matrix = [('browser', gateway,443,'tcp'),('browser',control,9443,'tcp'),
                      ('browser',browser,9222,'tcp'),('browser','127.0.0.1',9222,'tcp'),
                      ('browser','127.0.0.1',5100,'tcp'),
                      ('browser','127.0.0.11',53,'udp'),('browser',gateway,443,'udp'),
                      ('browser','::1',9222,'tcp'),('control',gateway,443,'tcp'),
                      ('control','127.0.0.11',53,'udp'),('control','1.1.1.1',53,'udp'),
                      ('gateway',control,9443,'tcp'),('gateway','127.0.0.11',53,'udp'),
                      ('gateway','8.8.8.8',53,'udp')]
            probe = '''import json,socket,sys
ip,port,kind=sys.argv[1],int(sys.argv[2]),sys.argv[3]
sock=socket.socket(socket.AF_INET6 if ':' in ip else socket.AF_INET,
                   socket.SOCK_DGRAM if kind=='udp' else socket.SOCK_STREAM)
sock.settimeout(.4)
try:
 if kind=='udp': sock.sendto(b'bounded-shadow-gate',(ip,port)); sock.recv(1)
 else: sock.connect((ip,port))
 print(json.dumps({'connected':True}))
except OSError as exc: print(json.dumps({'connected':False,'errno':exc.errno}))
finally: sock.close()
'''
            for role, ip, port, kind in matrix:
                before = self.drop_packets(role)
                pid = self.verify_ns(self.anchors[role]['name'],self.anchors[role]['name'])
                # Browser runs UID 0 without capabilities; UID 65534 belongs only to the CDP relay.
                probe_uid = '0' if role=='browser' else '65533'
                result = json.loads(command(['nsenter','-t',pid,'-n','--setuid',probe_uid,'--setgid',probe_uid,
                                             '--','python3','-c',probe,ip,str(port),kind],timeout=5))
                delta = self.drop_packets(role) - before
                denied = probe_denied(result,delta,ip)
                reports.append({'role':role,'target_kind':('ipv6_loopback' if ':' in ip else
                                'loopback' if ip.startswith('127.') else 'controlled_or_dns_probe'),
                                'port':port,'transport':kind,'drop_packets_delta':delta,
                                'socket_errno':result.get('errno'),'denied':denied})
                if not denied:
                    raise StandError('namespace_bypass_gate_failed')
        finally:
            counts = []
            for role, proc in traps:
                proc.terminate()
                try:
                    stdout, _ = proc.communicate(timeout=5)
                    counts.append({'role':role,**json.loads(stdout.strip().splitlines()[-1])})
                except (ValueError,IndexError,subprocess.TimeoutExpired):
                    proc.kill(); proc.wait(); counts.append({'role':role,'accepted':None})
            evidence = {'probes':reports,'traps':counts}
            (self.run_dir/'raw-network-gate.json').write_text(json.dumps(evidence,indent=2)+'\n')
        if any(row['accepted'] != 0 for row in counts):
            raise StandError('trap_connection_observed')
        self.write_manifest('raw_network_gate_passed')
        return evidence

    def start_runtime(self, variant='C'):
        if variant not in ('B','C'):
            raise StandError('invalid_variant')
        browser, control, gateway = (self.anchors[x] for x in ('browser','control','gateway'))
        bin_mount = str(self.stage/'bin-soak') + ':/bin-native:ro'
        bench_mount = str(self.stage/'benchmark') + ':/bench:ro'
        env_mount = ['--env-file',str(self.env_file)]
        gateway_env = ['EGRESS_LISTEN_ADDR='+gateway['ip']+':8081', 'EGRESS_MAX_CONCURRENT=16']
        if self.bootstrap:
            gateway_env += ['EGRESS_TEST_MODE=true','EGRESS_TEST_ALLOWLIST={"native-fixture.test":"127.0.0.1"}']
        self.gateway = self.run('egress',APP,[],network='container:'+gateway['name'],memory='128m',
                                env=gateway_env,mounts=[bin_mount,str(self.resolv)+':/etc/resolv.conf:ro'],
                                entrypoint='/bin-native/extract-egress',extra=env_mount)
        self.verify_ns(self.gateway,gateway['name'])
        cloak_args = ['python','/entry/launch.py','--info-dir','/info']
        if self.bootstrap:
            public_key=command(['openssl','x509','-in',str(self.stage/'tls/cert.pem'),'-pubkey','-noout'])
            der=subprocess.run(['openssl','pkey','-pubin','-outform','DER'],input=public_key.encode(),capture_output=True,check=True).stdout
            import base64,hashlib
            cloak_args += ['--spki',base64.b64encode(hashlib.sha256(der).digest()).decode()]
        info=self.run_dir/'browser-info'
        info.mkdir(mode=0o700)
        self.cloak = self.run('cloak',CLOAK,cloak_args,network='container:'+browser['name'],memory='768m',
                              cpu='0.5',pids=256,entrypoint='/entrypoint.sh',
                              mounts=[str(self.stage/'benchmark/scripts/launch_shadow_cloak.py')+':/entry/launch.py:ro',
                                      str(self.stage/'benchmark/scripts/shadow_x11_client.sh')+':/shadow-bin/xdotool:ro',
                                      str(self.stage/'benchmark/scripts/shadow_x11_client.sh')+':/shadow-bin/openbox:ro',
                                      str(info)+':/info'],
                              env=['CLOAKBROWSER_AUTO_UPDATE=false','DISPLAY=unix/:99',
                                   'PATH=/shadow-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'],extra=['--shm-size','1g',
                                   '--tmpfs','/root/.config:rw,nosuid,nodev,size=16m',
                                   '--tmpfs','/root/.cache:rw,nosuid,nodev,size=16m'])
        self.verify_ns(self.cloak,browser['name'])
        self.wait_listener('browser',5100,0)
        self.relay=self.run('relay',APP,['/entry/relay.py','--controller-ip',control['ip']],
                            network='container:'+browser['name'],memory='32m',entrypoint='python',
                            mounts=[str(self.stage/'benchmark/scripts/shadow_cdp_relay.py')+':/entry/relay.py:ro'],
                            extra=['--user','65534:65534'])
        self.verify_ns(self.relay,browser['name'])
        chrome_endpoint = 'http://'+browser['ip']+':9222'
        self.observer = self.run('observer',APP,['7200'],network='container:'+control['name'],
            memory='384m' if variant=='B' else '256m',cpu='0.5' if variant=='B' else '0.25',pids=128,
            mounts=[bench_mount,bin_mount,str(self.stage/'benchmark/news_aggregator/extraction/service_client.py')+
                    ':/app/news_aggregator/extraction/service_client.py:ro',str(self.run_dir)+':/out',
                    '/sys/fs/cgroup:/host-cg:ro'] + ([str(self.corpus.parent)+':/corpus:ro',str(self.captures)+':/captures']
                    if hasattr(self,'corpus') else []),env=['PYTHONPATH=/app','BROWSER_WS_ENDPOINT='+chrome_endpoint,
                    'EXTRACTION_SERVICE_MODE=python','SCHEDULER_ENABLED=false'],extra=env_mount)
        self.verify_ns(self.observer,control['name'])
        self.wait_http(self.observer,chrome_endpoint+'/json/version')
        uid_rows=self.docker('top',self.cloak,'-eo','uid,pid,comm').splitlines()[1:]
        if not uid_rows or any(row.split()[0]!='0' for row in uid_rows):
            raise StandError('browser_uid_boundary_changed')
        relay_rows=self.docker('top',self.relay,'-eo','uid,pid,comm').splitlines()[1:]
        if not relay_rows or any(row.split()[0]!='65534' for row in relay_rows):
            raise StandError('relay_uid_boundary_changed')
        self.wait_http(self.observer,'http://'+gateway['ip']+':8081/health/ready')
        if variant == 'C':
            self.worker = self.run('worker',APP,[],network='container:'+control['name'],memory='384m',
                cpu='0.5',pids=128,mounts=[bin_mount],entrypoint='/bin-native/extract-service',extra=env_mount,
                env=['LISTEN_ADDR=127.0.0.1:8080','EXTRACTOR_HTTP_ONLY=false','EXTRACTOR_BROWSER_BACKEND=native',
                     'CHROME_ENDPOINT='+chrome_endpoint,'EXTRACTOR_NATIVE_PROXY_URL=http://'+gateway['ip']+':8081',
                     'EXTRACTOR_NATIVE_NETWORK_ISOLATED=true','EXTRACTOR_NATIVE_READY_SELECTOR='])
            self.verify_ns(self.worker,control['name'])
            self.wait_http(self.observer,'http://127.0.0.1:8080/health/ready')
        self.write_manifest('runtime_ready')

    def wait_http(self, name, url):
        component='chrome' if '/json/version' in url else 'gateway' if ':8081/' in url else 'service'
        self.write_manifest('waiting_'+component)
        for _ in range(40):
            if hasattr(self,'cloak'):
                running=self.docker('inspect','--format','{{.State.Running}}',self.cloak)
                if running!='true':
                    raise StandError('cloak_process_exited')
            if component=='service' and hasattr(self,'worker'):
                running=self.docker('inspect','--format','{{.State.Running}}',self.worker)
                if running!='true':
                    raise StandError('native_worker_startup_failed')
            result=subprocess.run(['docker','exec',name,'curl','--noproxy','*','--max-time','1','-fsS',url],
                                  capture_output=True,text=True,timeout=3)
            (self.run_dir/('readiness-'+component+'.json')).write_text(json.dumps({
                'exit_code':result.returncode,'stderr':result.stderr[:512],'response':result.stdout[:1024]})+'\n')
            if result.returncode==0:
                return result.stdout
            self.check_budget()
            time.sleep(1)
        raise StandError('runtime_readiness_failed')

    def controlled_browser_gate(self):
        trap_code='''import json,signal,socket
count=0
sock=socket.socket(); sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock.bind(('0.0.0.0',9443)); sock.listen(); sock.settimeout(.5)
print('ready',flush=True)
def end(*_):
 print(json.dumps({'accepted':count}),flush=True); raise SystemExit(0)
signal.signal(signal.SIGTERM,end)
while True:
 try: peer,_=sock.accept(); peer.close(); count+=1
 except socket.timeout: pass
'''
        pid=self.verify_ns(self.anchors['control']['name'],self.anchors['control']['name'])
        trap=subprocess.Popen(['nsenter','-t',pid,'-n','--','python3','-u','-c',trap_code],
                              stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        accepted=None
        try:
            if trap.stdout.readline().strip()!='ready':
                raise StandError('browser_trap_start_failed')
            result=self._controlled_browser_routes()
        finally:
            trap.terminate()
            try:
                stdout,_=trap.communicate(timeout=5)
                accepted=json.loads(stdout.strip().splitlines()[-1])['accepted']
            except (ValueError,IndexError,subprocess.TimeoutExpired):
                trap.kill();trap.wait()
            (self.run_dir/'browser-trap.json').write_text(json.dumps({'accepted':accepted})+'\n')
        if accepted!=0:
            raise StandError('browser_trap_connection_observed')
        self.write_manifest('controlled_browser_gate_passed')
        return result

    def _controlled_browser_routes(self):
        fixture_dir=self.stage/'benchmark/tests/fixtures'
        proof=self.run_dir/'origin-proof.jsonl'
        self.origin=self.run('origin',APP,['/fixtures/native-shadow/gate_origin.py',
            '--cert','/tls/cert.pem','--key','/tls/key.pem','--proof-output','/out/origin-proof.jsonl',
            '--trap-ip',self.anchors['control']['ip']],network='container:'+self.anchors['gateway']['name'],
            memory='128m',mounts=[str(fixture_dir)+':/fixtures:ro',str(self.stage/'tls')+':/tls:ro',
                                 str(self.run_dir)+':/out'],entrypoint='python')
        self.verify_ns(self.origin,self.anchors['gateway']['name'])
        self.wait_listener('gateway',443,0)
        post='''import json,os,sys,urllib.request
payload={'request_id':'gate-'+sys.argv[1],'url':'https://native-fixture.test/'+sys.argv[1],
         'strategy':'browser_rendering','source_type':'article','use_browser':True,'budget_ms':30000}
req=urllib.request.Request('http://127.0.0.1:8080/v1/extract',data=json.dumps(payload).encode(),
 headers={'Content-Type':'application/json','Authorization':'Bearer '+os.environ['EXTRACTOR_TEST_TOKEN']})
with urllib.request.urlopen(req,timeout=35) as response: value=json.load(response)
content=value.get('result',{}).get('content','') or ''
print(json.dumps({'outcome':value.get('outcome'),'error':(value.get('error') or {}).get('code'),
 'chars':len(content),'cookie_confirmed':'cookie confirmed' in content}))
'''
        results=[]
        for route in ['js-article','private-subrequest','redirect-private','iframe-private',
                      'worker-private','popup-private','websocket-private','download','origin-auth']:
            result=json.loads(self.docker('exec',self.observer,'python','-c',post,route,timeout=40))
            result['case']=route
            results.append(result)
            (self.run_dir/'browser-gate.json').write_text(json.dumps(results,indent=2)+'\n')
            if route=='js-article':
                if result['outcome']!='success' or not result['cookie_confirmed']:
                    raise StandError('positive_native_browser_gate_failed')
            else:
                expected={'native_browser_policy_denied','native_browser_unsupported','public_url_denied'}
                if route=='origin-auth':
                    expected={'upstream_auth_required','native_browser_http_rejected'}
                if result['outcome']=='success' or result['error'] not in expected:
                    raise StandError('adversarial_browser_gate_failed')
            self.wait_http(self.observer,'http://127.0.0.1:8080/health/ready')
            self.check_budget()
        # Origin credentials must never include proxy auth; no raw header values are stored.
        records=[json.loads(line) for line in proof.read_text().splitlines()]
        if any(row.get('has_authorization') or row.get('has_proxy_authorization') for row in records):
            raise StandError('origin_auth_leak')
        return results

    def cleanup(self):
        for name in reversed(self.containers):
            try:
                self.owned(name)
                logs = subprocess.run(['docker','logs','--tail','200',name],capture_output=True,text=True,timeout=10)
                # Container logs are private evidence, never forwarded to the shared report.
                (self.run_dir / (name + '.log')).write_text(logs.stdout + logs.stderr)
                self.docker('rm', '-f', name)
            except (StandError, subprocess.TimeoutExpired):
                continue
        for name in reversed(self.networks):
            try:
                label = self.docker('network', 'inspect', '--format', '{{index .Labels "native-shadow-run"}}', name)
                if label == self.prefix:
                    self.docker('network', 'rm', name)
            except StandError:
                continue
        self.env_file.unlink(missing_ok=True)
        remaining=self.docker('ps','-a','--filter','label='+self.label,'--format','{{.Names}}').splitlines()
        networks=self.docker('network','ls','--filter','label='+self.label,'--format','{{.Name}}').splitlines()
        if remaining or networks:
            (self.run_dir/'cleanup-incomplete.json').write_text(json.dumps({'containers':remaining,'networks':networks})+'\n')
            self.write_manifest('cleanup_incomplete')
            raise StandError('cleanup_incomplete')
        self.write_manifest('cleaned')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', required=True, type=Path)
    parser.add_argument('--bootstrap', action='store_true')
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--gate-only', action='store_true')
    mode.add_argument('--live', action='store_true')
    parser.add_argument('--variant',choices=['B','C'])
    parser.add_argument('--corpus',type=Path)
    parser.add_argument('--captures',type=Path)
    parser.add_argument('--case',action='append',default=[])
    parser.add_argument('--approved-gate',type=Path)
    parser.add_argument('--deadline',type=int)
    args = parser.parse_args()
    def interrupted(*_):
        raise StandError('stand_interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    if args.live:
        if args.bootstrap or not all([args.variant,args.corpus,args.captures,args.approved_gate,args.deadline]):
            raise StandError('live_configuration_missing')
        gate=json.loads((args.approved_gate/'manifest.json').read_text())
        trap=json.loads((args.approved_gate/'browser-trap.json').read_text())
        if (gate.get('state')!='cleaned' or gate.get('previous_phase')!='controlled_browser_gate_passed'
                or gate.get('bootstrap') is not True or trap.get('accepted')!=0):
            raise StandError('controlled_gate_not_passed')
    stand = Stand(args.stage, bootstrap=args.bootstrap,deadline=args.deadline)
    try:
        stand.setup()
        stand.raw_network_gate()
        if args.live:
            stand.run_live(args.variant,args.corpus,args.captures,args.case)
        elif args.bootstrap:
            stand.start_runtime()
            stand.controlled_browser_gate()
        print(json.dumps({'state':'live_batch_completed' if args.live else 'controlled_gate_passed'
                          if args.bootstrap else 'raw_network_gate_passed',
                          'run_id':stand.prefix}))
    except Exception as exc:
        code=str(exc) if isinstance(exc,StandError) else type(exc).__name__
        (stand.run_dir/'failure.json').write_text(json.dumps({'phase':stand.phase,'error':code})+'\n')
        raise
    finally:
        stand.cleanup()


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        code = str(exc) if isinstance(exc,StandError) and re.fullmatch(r'[a-z0-9_]{1,80}',str(exc)) else 'stand_failed'
        print(json.dumps({'error':type(exc).__name__,'code':code}),file=sys.stderr)
        raise SystemExit(1)
