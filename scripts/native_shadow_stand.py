"""Isolated native shadow namespaces; never modifies a host firewall or production service."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import sys
import tempfile
import time

try:
    from .native_synthetic_soak import (BROWSER_LOOPBACK_COUNTERS, NATIVE_DIAGNOSTIC_LINE,
                                        run_synthetic_stability)
except ImportError:
    try:
        from native_synthetic_soak import (BROWSER_LOOPBACK_COUNTERS, NATIVE_DIAGNOSTIC_LINE,
                                           run_synthetic_stability)
    except ImportError:
        from scripts.native_synthetic_soak import (BROWSER_LOOPBACK_COUNTERS,
                                                   NATIVE_DIAGNOSTIC_LINE, run_synthetic_stability)

APP = 'ghcr.io/dzarlax-ai/rss-summarizer@sha256:b6abbb428543e846d0310b86ff41236a6df028985a00ebfe4714357d22e67060'
CLOAK = 'cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4'
SPECIAL4 = ('0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8',
            '169.254.0.0/16', '172.16.0.0/12', '192.0.0.0/24', '192.0.2.0/24',
            '192.88.99.0/24', '192.168.0.0/16', '198.18.0.0/15', '198.51.100.0/24',
            '203.0.113.0/24', '224.0.0.0/4', '240.0.0.0/4')
TABLE = 'native_shadow_guard'
RUNNER_WRAPPER = r'''import contextlib,json,os,runpy,sys,tempfile,traceback
from pathlib import Path
command_id=sys.argv[1]
if len(command_id)!=32 or any(ch not in '0123456789abcdef' for ch in command_id):
    raise SystemExit(2)
base=Path('/out')
command_path=base/('runner-command-'+command_id+'.json')
status_path=base/('runner-status-'+command_id+'.json')
log_path=base/'runner-private.log'
exit_code=1
try:
    if command_path.is_symlink() or not command_path.is_file() or command_path.stat().st_size>65536:
        raise ValueError('invalid runner command file')
    command=json.loads(command_path.read_text())
    command_path.unlink()
    if not isinstance(command,dict) or command.get('command_id')!=command_id:
        raise ValueError('stale runner command file')
    args=command.get('args') if isinstance(command,dict) else None
    if (not isinstance(args,list) or not args or len(args)>256
            or any(not isinstance(arg,str) or len(arg)>8192 for arg in args)):
        raise ValueError('invalid runner arguments')
    if args[0] in ('python','python3'):
        args=args[1:]
    if not args:
        raise ValueError('missing runner script')
    script=Path(args[0])
    if not script.is_absolute() or not script.is_file() or script.is_symlink():
        raise ValueError('invalid runner script')
    sys.argv=args
    sys.path.insert(0,str(script.parent))
    log_fd=os.open(log_path,os.O_WRONLY|os.O_APPEND|os.O_CREAT|os.O_NOFOLLOW,0o600)
    with os.fdopen(log_fd,'a',encoding='utf-8') as log:
        os.dup2(log.fileno(),1); os.dup2(log.fileno(),2)
        with contextlib.redirect_stdout(log),contextlib.redirect_stderr(log):
            try:
                runpy.run_path(str(script),run_name='__main__')
                exit_code=0
            except SystemExit as exc:
                exit_code=exc.code if type(exc.code) is int and 0<=exc.code<=255 else (0 if exc.code is None else 1)
            except Exception:
                traceback.print_exc(file=log)
                exit_code=1
            except KeyboardInterrupt:
                exit_code=130
    receipt={'command_id':command_id,'exit_code':exit_code,'state':'complete'}
    fd,tmp_name=tempfile.mkstemp(prefix='.runner-status-',dir=base)
    try:
        os.fchmod(fd,0o600)
        with os.fdopen(fd,'w',encoding='utf-8') as out:
            json.dump(receipt,out,separators=(',',':'))
            out.write('\n'); out.flush(); os.fsync(out.fileno())
        os.replace(tmp_name,status_path)
    finally:
        try: os.unlink(tmp_name)
        except FileNotFoundError: pass
except Exception:
    try: command_path.unlink(missing_ok=True)
    except OSError: pass
    raise
'''


def observer_extraction_mounts(stage: Path) -> list[str]:
    """Mount the staged Python extraction paths under test into the observer."""
    extraction = stage / 'benchmark/news_aggregator/extraction'
    return [
        str(extraction / 'service_client.py') + ':/app/news_aggregator/extraction/service_client.py:ro',
        str(extraction / 'extraction_utils.py') + ':/app/news_aggregator/extraction/extraction_utils.py:ro',
    ]


def observer_core_extractor_mounts(stage: Path) -> list[str]:
    """Mount current config and production CoreExtractor for isolated routed tests."""
    return [
        str(stage / 'benchmark/news_aggregator/config.py') + ':/app/news_aggregator/config.py:ro',
        str(stage / 'benchmark/news_aggregator/extraction/core_extractor.py') +
        ':/app/news_aggregator/extraction/core_extractor.py:ro',
    ]


CORE_EXTRACTOR_NATIVE_RECEIPT_FLAGS = (
    'production_core_route','real_http_client','native_browser_requested','positive_article',
    'cookie_confirmed','metadata_present','publisher_auth_rejected','python_browser_imported',
    'ai_db_telegram_imported','browser_generation_unchanged')


def validate_core_extractor_native_receipt(value: object) -> dict:
    fields=set(CORE_EXTRACTOR_NATIVE_RECEIPT_FLAGS)|{
        'state','publisher_auth_code','python_fallback_calls','success_record_count',
        'learned_pattern_lookups','learning_backend','article_characters'}
    if not isinstance(value,dict) or set(value)!=fields or value.get('state')!='passed':
        raise StandError('core_extractor_native_receipt_invalid')
    for key in CORE_EXTRACTOR_NATIVE_RECEIPT_FLAGS:
        expected=False if key in {'python_browser_imported','ai_db_telegram_imported'} else True
        if value.get(key) is not expected:
            raise StandError('core_extractor_native_receipt_invalid')
    if (value.get('publisher_auth_code') not in {'upstream_auth_required','native_browser_http_rejected'}
            or value.get('learning_backend')!='in_memory'
            or type(value.get('python_fallback_calls')) is not int or value['python_fallback_calls']!=0
            or type(value.get('success_record_count')) is not int or value['success_record_count']!=1
            or type(value.get('learned_pattern_lookups')) is not int or value['learned_pattern_lookups']!=2
            or type(value.get('article_characters')) is not int
            or not 200<=value['article_characters']<=100000):
        raise StandError('core_extractor_native_receipt_invalid')
    return value


def read_core_extractor_native_receipt(path: Path) -> dict:
    try:
        if (path.is_symlink() or not path.is_file() or path.stat().st_size>2048
                or path.stat().st_mode & 0o077):
            raise ValueError
        value=json.loads(path.read_text())
    except (OSError,ValueError,TypeError):
        raise StandError('core_extractor_native_receipt_invalid') from None
    if isinstance(value,dict) and value.get('state')=='failed':
        failure_keys={'state','phase','failure_type','failure_code','python_fallback_calls'}
        if (set(value)==failure_keys
                and value.get('phase') in {'setup','production_imports','positive_article','publisher_auth_rejection'}
                and value.get('failure_type') in {'AssertionError','BrowserUnavailableError','ConnectionError',
                    'ExtractionServiceError','ImportError','KeyError','ModuleNotFoundError',
                    'RuntimeError','TimeoutError','ValueError','GateAbort','other'}
                and value.get('failure_code') in {'other_error','service_error','native_browser_timeout',
                    'native_browser_unavailable','native_browser_http_rejected','native_browser_no_content',
                    'native_browser_policy_denied','positive_article_validation_failed','cookie_proof_missing',
                    'publisher_auth_succeeded','publisher_auth_failure_unexpected',
                    'unexpected_fallback_or_learning_counts','unexpected_production_dependency_import',
                    'browser_generation_changed'}
                and type(value.get('python_fallback_calls')) is int
                and 0<=value['python_fallback_calls']<=1):
            raise StandError('core_extractor_native_gate_failed')
        raise StandError('core_extractor_native_receipt_invalid')
    return validate_core_extractor_native_receipt(value)


CORE_EXTRACTOR_NATIVE_GATE = r'''import asyncio,hashlib,json,os,sys,types,urllib.request

phase='setup'
failure_code='other_error'
failure_type='other'
python_fallback_calls=0
service_error_type=None

class GateAbort(Exception): pass

def write_receipt(value):
    raw=(json.dumps(value,separators=(',',':'))+'\n').encode()
    fd=os.open('/out/core-extractor-native.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        os.fchmod(fd,0o600); os.write(fd,raw); os.fsync(fd)
    finally: os.close(fd)

def safe_failure(exc):
    name=type(exc).__name__
    allowed={'AssertionError','BrowserUnavailableError','ConnectionError','ExtractionServiceError',
             'ImportError','KeyError','ModuleNotFoundError','RuntimeError','TimeoutError',
             'ValueError','GateAbort'}
    return name if name in allowed else 'other'

def generation():
    endpoint=os.environ.get('BROWSER_WS_ENDPOINT','').rstrip('/')+'/json/version'
    with urllib.request.urlopen(endpoint,timeout=2) as response:
        if response.status!=200: raise RuntimeError('discovery_unavailable')
        raw=response.read(16385)
    if len(raw)>16384: raise RuntimeError('discovery_unavailable')
    value=json.loads(raw)
    websocket=value.get('webSocketDebuggerUrl') if isinstance(value,dict) else None
    if not isinstance(websocket,str) or len(websocket)>2048: raise RuntimeError('discovery_unavailable')
    return hashlib.sha256(websocket.encode()).hexdigest()

async def run():
    global phase,failure_code,python_fallback_calls,service_error_type
    expected=sys.argv[1] if len(sys.argv)==2 else ''
    token=os.environ.get('EXTRACTOR_SERVICE_TOKEN','')
    if len(expected)!=64 or any(ch not in '0123456789abcdef' for ch in expected) or not token:
        raise RuntimeError('gate_configuration_invalid')
    os.environ.update({
        'EXTRACTION_SERVICE_MODE':'go','EXTRACTION_SERVICE_NATIVE_BROWSER':'true',
        'EXTRACTION_SERVICE_PYTHON_FALLBACK':'false',
        'EXTRACTION_SERVICE_URL':'http://127.0.0.1:8080',
        'EXTRACTION_SERVICE_TOKEN':token,
        'EXTRACTION_SERVICE_TIMEOUT_SECONDS':'30',
        'EXTRACTION_SERVICE_TOTAL_BUDGET_SECONDS':'30',
    })
    # Preseed only packages whose __init__ modules otherwise load the DB-backed
    # extraction facade or browser strategies. CoreExtractor and its service
    # client are imported from the mounted production files below.
    def package(name,path):
        module=types.ModuleType(name); module.__path__=[path]; module.__package__=name
        sys.modules[name]=module
    package('news_aggregator.extraction','/app/news_aggregator/extraction')
    package('news_aggregator.services','/app/news_aggregator/services')
    class Memory:
        def __init__(self): self.successes=0; self.pattern_reads=0
        async def get_successful_pattern(self,domain):
            self.pattern_reads+=1
            return {'selector':'article','method':'browser_rendering'}
        async def record_extraction_attempt(self,attempt): self.successes+=1
        async def degrade_pattern(self,*_args): pass
    memory=Memory()
    memory_module=types.ModuleType('news_aggregator.services.extraction_memory')
    class ExtractionAttempt:
        def __init__(self,**values): self.values=values
    async def get_memory(): return memory
    memory_module.ExtractionAttempt=ExtractionAttempt
    memory_module.get_extraction_memory=get_memory
    sys.modules[memory_module.__name__]=memory_module
    tracker=types.SimpleNamespace(update_domain_stats=lambda **_values:None)
    tracker_module=types.ModuleType('news_aggregator.services.domain_stability_tracker')
    async def get_tracker(): return tracker
    tracker_module.get_stability_tracker=get_tracker
    sys.modules[tracker_module.__name__]=tracker_module
    logger_module=types.ModuleType('news_aggregator.extraction.extraction_logger')
    logger_module.get_extraction_logger=lambda:None
    sys.modules[logger_module.__name__]=logger_module
    phase='production_imports'
    from news_aggregator.config import get_settings
    from news_aggregator.extraction.core_extractor import CoreExtractor
    from news_aggregator.extraction.extraction_utils import ExtractionUtils
    from news_aggregator.extraction.service_client import ExtractionServiceError
    service_error_type=ExtractionServiceError
    settings=get_settings()
    if (settings.extraction_service_mode!='go' or not settings.extraction_service_native_browser
            or settings.extraction_service_python_fallback
            or settings.extraction_service_url!='http://127.0.0.1:8080'):
        raise RuntimeError('native_route_configuration_invalid')
    forbidden=('news_aggregator.services.ai_client','news_aggregator.services.ai_extraction_optimizer',
        'news_aggregator.services.smart_filter','news_aggregator.database','news_aggregator.telegram')
    def forbidden_imports():
        return any(name in sys.modules for name in forbidden)
    def python_browser_imported():
        return any(name.startswith('news_aggregator.core.browser_pool') for name in sys.modules)
    if forbidden_imports() or python_browser_imported():
        raise RuntimeError('unexpected_production_dependency_import')
    async def unused_close(): pass
    core=CoreExtractor(ExtractionUtils(),None,types.SimpleNamespace(close_browser=unused_close))
    async def forbidden_python_fallback(*_args,**_kwargs):
        global python_fallback_calls
        python_fallback_calls+=1
        raise RuntimeError('python_fallback_invoked')
    core._extract_article_content_python=forbidden_python_fallback
    phase='positive_article'
    start_generation=generation()
    if start_generation!=expected: raise RuntimeError('browser_generation_changed')
    try:
        result=await asyncio.wait_for(core.extract_article_content_with_metadata(
            'https://native-fixture.test/js-article'),timeout=31)
    except ExtractionServiceError as exc:
        failure_code=exc.code if exc.code in {'native_browser_timeout','native_browser_unavailable',
            'native_browser_http_rejected','native_browser_no_content','native_browser_policy_denied'} else 'service_error'
        raise GateAbort() from None
    content=result.get('content') if isinstance(result,dict) else None
    title=result.get('title') if isinstance(result,dict) else None
    if not isinstance(content,str) or len(content)<200 or not isinstance(title,str) or not title.strip():
        failure_code='positive_article_validation_failed'; raise GateAbort()
    if 'cookie confirmed' not in content.casefold():
        failure_code='cookie_proof_missing'; raise GateAbort()
    phase='publisher_auth_rejection'
    publisher_auth_code=None
    try:
        await asyncio.wait_for(core.extract_article_content_with_metadata(
            'https://native-fixture.test/origin-auth'),timeout=31)
        failure_code='publisher_auth_succeeded'; raise GateAbort()
    except ExtractionServiceError as exc:
        if exc.code not in {'upstream_auth_required','native_browser_http_rejected'} or exc.retryable:
            failure_code='publisher_auth_failure_unexpected'; raise GateAbort() from None
        publisher_auth_code=exc.code
    if python_fallback_calls!=0 or memory.successes!=1 or memory.pattern_reads!=2:
        failure_code='unexpected_fallback_or_learning_counts'; raise GateAbort()
    if forbidden_imports() or python_browser_imported():
        failure_code='unexpected_production_dependency_import'; raise GateAbort()
    end_generation=generation()
    if end_generation!=expected:
        failure_code='browser_generation_changed'; raise GateAbort()
    return {
        'state':'passed','production_core_route':True,'real_http_client':True,
        'native_browser_requested':True,'positive_article':True,'cookie_confirmed':True,
        'metadata_present':True,'publisher_auth_rejected':True,
        'publisher_auth_code':publisher_auth_code,
        'python_fallback_calls':python_fallback_calls,
        'success_record_count':1,'learned_pattern_lookups':2,
        'learning_backend':'in_memory','python_browser_imported':False,
        'ai_db_telegram_imported':False,'browser_generation_unchanged':True,
        'article_characters':len(content),
    }

try:
    value=asyncio.run(run())
    write_receipt(value)
except BaseException as exc:
    failure_type=safe_failure(exc)
    code=failure_code if failure_code in {'other_error','service_error','native_browser_timeout',
        'native_browser_unavailable','native_browser_http_rejected','native_browser_no_content',
        'native_browser_policy_denied','positive_article_validation_failed','cookie_proof_missing',
        'publisher_auth_succeeded','publisher_auth_failure_unexpected',
        'unexpected_fallback_or_learning_counts','unexpected_production_dependency_import',
        'browser_generation_changed'} else 'other_error'
    try: write_receipt({'state':'failed','phase':phase,'failure_type':failure_type,'failure_code':code,
                        'python_fallback_calls':python_fallback_calls})
    except Exception: pass
'''


CONTROLLED_CRASH_HELPER = r'''import asyncio,ipaddress,json,os,stat
from urllib.parse import urlsplit
import aiohttp

class Failure(Exception):
    def __init__(self, code):
        self.code=code

def select_target(context_ids, infos):
    if not isinstance(context_ids,list):
        return 'contexts_invalid',None
    owned={item for item in context_ids if type(item) is str and item}
    if not isinstance(infos,list):
        return 'targets_invalid',None
    matches=[]
    for info in infos:
        if not isinstance(info,dict):
            continue
        if info.get('type')!='page' or info.get('url')!='https://native-fixture.test/controlled-crash':
            continue
        context_id=info.get('browserContextId')
        if type(context_id) is not str or context_id not in owned:
            continue
        target_id=info.get('targetId')
        if type(target_id) is not str or not target_id:
            return 'target_invalid',None
        matches.append(target_id)
    if len(matches)>1:
        return 'target_ambiguous',None
    if not matches:
        return 'target_waiting',None
    return 'selected',matches[0]

def write_receipt(value):
    raw=(json.dumps(value,separators=(',',':'))+'\n').encode()
    fd=os.open('/out/controlled-crash.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        os.fchmod(fd,0o600)
        os.write(fd,raw)
        os.fsync(fd)
    finally:
        os.close(fd)

async def command(ws, counter, method, params=None, session_id=None, timeout=1.0):
    counter+=1
    message={'id':counter,'method':method,'params':params or {}}
    if session_id:
        message['sessionId']=session_id
    await ws.send_json(message)
    deadline=asyncio.get_running_loop().time()+timeout
    while True:
        remaining=deadline-asyncio.get_running_loop().time()
        if remaining<=0:
            raise Failure('cdp_timeout')
        try:
            frame=await asyncio.wait_for(ws.receive(),remaining)
        except asyncio.TimeoutError:
            raise Failure('cdp_timeout') from None
        if frame.type!=aiohttp.WSMsgType.TEXT:
            raise Failure('websocket_closed')
        try:
            value=json.loads(frame.data)
        except (TypeError,ValueError):
            raise Failure('cdp_invalid_message') from None
        if isinstance(value,dict) and value.get('id')==counter:
            if 'error' in value:
                raise Failure('cdp_command_failed')
            result=value.get('result')
            if not isinstance(result,dict):
                raise Failure('cdp_invalid_result')
            return counter,result

async def run():
    endpoint=os.environ.get('BROWSER_WS_ENDPOINT','')
    parsed=urlsplit(endpoint)
    try:
        host=ipaddress.ip_address(parsed.hostname or '')
    except ValueError:
        raise Failure('endpoint_invalid') from None
    if (parsed.scheme!='http' or not host.is_private or host.is_loopback or host.version!=4
            or parsed.port!=9222 or parsed.username or parsed.password or parsed.path not in ('','/')
            or parsed.query or parsed.fragment):
        raise Failure('endpoint_invalid')
    timeout=aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(endpoint.rstrip('/')+'/json/version') as response:
                if response.status!=200:
                    raise Failure('browser_discovery_failed')
                raw=await response.content.read(65537)
        except Failure:
            raise
        except Exception:
            raise Failure('browser_discovery_failed') from None
        if len(raw)>65536:
            raise Failure('browser_discovery_invalid')
        try:
            version=json.loads(raw)
            ws_url=version['webSocketDebuggerUrl']
            ws_parsed=urlsplit(ws_url)
        except (TypeError,ValueError,KeyError):
            raise Failure('browser_discovery_invalid') from None
        if (ws_parsed.scheme!='ws' or ws_parsed.hostname!=parsed.hostname or ws_parsed.port!=9222
                or ws_parsed.username or ws_parsed.password or ws_parsed.query or ws_parsed.fragment):
            raise Failure('browser_discovery_invalid')
        try:
            async with session.ws_connect(ws_url,max_msg_size=65536,heartbeat=1) as ws:
                counter=0
                target_id=None
                for _ in range(100):
                    counter,contexts=await command(ws,counter,'Target.getBrowserContexts',timeout=1)
                    context_ids=contexts.get('browserContextIds')
                    if not isinstance(context_ids,list):
                        raise Failure('contexts_invalid')
                    counter,targets=await command(ws,counter,'Target.getTargets',timeout=.5)
                    state,target_id=select_target(context_ids,targets.get('targetInfos'))
                    if state=='selected':
                        break
                    if state!='target_waiting':
                        raise Failure(state)
                    await asyncio.sleep(.05)
                if target_id is None:
                    raise Failure('target_timeout')
                counter,attached=await command(ws,counter,'Target.attachToTarget',
                    {'targetId':target_id,'flatten':True},timeout=1)
                session_id=attached.get('sessionId')
                if type(session_id) is not str or not session_id:
                    raise Failure('attach_invalid')
                counter+=1
                crash_id=counter
                await ws.send_json({'id':crash_id,'sessionId':session_id,'method':'Page.crash','params':{}})
                deadline=asyncio.get_running_loop().time()+2
                while True:
                    remaining=deadline-asyncio.get_running_loop().time()
                    if remaining<=0:
                        raise Failure('crash_timeout')
                    try:
                        frame=await asyncio.wait_for(ws.receive(),remaining)
                    except asyncio.TimeoutError:
                        raise Failure('crash_timeout') from None
                    if frame.type!=aiohttp.WSMsgType.TEXT:
                        raise Failure('websocket_closed')
                    try:
                        value=json.loads(frame.data)
                    except (TypeError,ValueError):
                        raise Failure('cdp_invalid_message') from None
                    if not isinstance(value,dict):
                        continue
                    if (value.get('method')=='Inspector.targetCrashed'
                            and value.get('sessionId')==session_id):
                        return
                    if value.get('id')==crash_id:
                        if 'error' in value:
                            raise Failure('crash_command_failed')
                        return
        except Failure:
            raise
        except Exception:
            raise Failure('websocket_connect_failed') from None

receipt={'owned_target_selected':True,'crash_requested':True}
try:
    asyncio.run(asyncio.wait_for(run(),timeout=10))
except Failure as exc:
    receipt={'failure_code':exc.code}
except asyncio.TimeoutError:
    receipt={'failure_code':'helper_timeout'}
except Exception:
    receipt={'failure_code':'helper_failed'}
try:
    write_receipt(receipt)
except Exception:
    pass
'''

CONTROLLED_CRASH_FAILURE_CODES = frozenset({
    'endpoint_invalid','browser_discovery_failed','browser_discovery_invalid','cdp_timeout',
    'websocket_connect_failed','websocket_closed','cdp_invalid_message','cdp_command_failed','cdp_invalid_result',
    'contexts_invalid','targets_invalid','target_invalid','target_ambiguous','target_timeout',
    'attach_invalid','crash_timeout','crash_command_failed','helper_timeout','helper_failed',
})


def read_controlled_crash_receipt(path):
    try:
        info=path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_size>1024 or info.st_mode&0o777!=0o600:
            raise StandError('native_crash_receipt_invalid')
        value=json.loads(path.read_text(encoding='utf-8'))
    except StandError:
        raise
    except (OSError,ValueError,UnicodeError):
        raise StandError('native_crash_receipt_invalid') from None
    if (isinstance(value,dict) and set(value)=={'owned_target_selected','crash_requested'}
            and value.get('owned_target_selected') is True and value.get('crash_requested') is True):
        return True
    if (isinstance(value,dict) and set(value)=={'failure_code'}
            and type(value.get('failure_code')) is str
            and value.get('failure_code') in CONTROLLED_CRASH_FAILURE_CODES):
        raise StandError('native_crash_helper_failed')
    raise StandError('native_crash_receipt_invalid')


def collect_native_diagnostic_receipt(stand):
    """Persist only allowlisted native CDP stage/reason pairs from owned worker logs."""
    try:
        log_text = stand.diagnostic_logs(stand.worker)
        diagnostics = []
        if isinstance(log_text, str):
            for line in log_text[-262144:].splitlines()[-1000:]:
                match = NATIVE_DIAGNOSTIC_LINE.fullmatch(line)
                if match:
                    item = {'stage': match.group(1), 'reason': match.group(2)}
                    if item not in diagnostics:
                        diagnostics.append(item)
                    if len(diagnostics) == 32:
                        break
        receipt = stand.run_dir / 'native-browser-diagnostics.json'
        if receipt.exists() or receipt.is_symlink():
            return False
        fd = os.open(receipt, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(json.dumps({'schema_version': 1, 'diagnostics': diagnostics}) + '\n')
        return True
    except Exception:
        # Diagnostics must never replace the original quality failure.
        return False


def native_diagnostics_mode(*, quality_profile=False, synthetic_soak=False, bounded_gate=False):
    if quality_profile and not synthetic_soak and not bounded_gate:
        return 'live-quality'
    if (synthetic_soak or bounded_gate) and not quality_profile and not (synthetic_soak and bounded_gate):
        return 'synthetic-soak'
    return ''
QUALITY_CORPUS_SHA256 = '91fd95a3a91ad172bab7daa1cdd117f61a7ccc81bd7a55f6e82eea422ea0420d'
QUALITY_CASES = ('case-0001', 'case-0008', 'case-0034', 'case-0041', 'case-0050')
QUALITY_PHASE_CASES = {
    'c-smoke': ('case-0001',),
    'c-quality': tuple(case for case in QUALITY_CASES if case != 'case-0001'),
}

def validate_quality_phase(variant, phase, case_ids):
    if (phase not in QUALITY_PHASE_CASES or variant != 'C'
            or (case_ids and tuple(case_ids) != QUALITY_PHASE_CASES[phase])):
        raise StandError('quality_phase_selection_invalid')


def validate_synthetic_soak_options(*, enabled, bootstrap, gate_only, live, bounded_profile,
                                    supervisor_cleanup, supervisor_label, deadline, variant,
                                    quality_profile, quality_phase, corpus, captures,
                                    approved_gate, case_ids):
    if not enabled:
        return
    if (not bootstrap or not gate_only or live or not bounded_profile or not supervisor_cleanup
            or not supervisor_label or deadline is None or variant is not None or quality_profile
            or quality_phase is not None or corpus is not None or captures is not None
            or approved_gate is not None or case_ids):
        raise StandError('synthetic_soak_configuration_invalid')
    if not re.fullmatch(r'linux-vps-gate=[a-z0-9-]{8,48}', supervisor_label):
        raise StandError('synthetic_soak_configuration_invalid')
    now = time.time()
    if deadline <= now or deadline > now + 16 * 60:
        raise StandError('synthetic_soak_deadline_invalid')


class StandError(RuntimeError):
    pass


def read_runner_receipt(path, command_id):
    path = Path(path)
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_size > 1024 or info.st_mode & 0o077):
            raise ValueError
        with os.fdopen(fd, 'rb') as receipt_file:
            fd = None
            value = json.loads(receipt_file.read(1025))
    except (OSError, ValueError, TypeError):
        raise StandError('runner_receipt_invalid') from None
    finally:
        if fd is not None:
            os.close(fd)
    if (not isinstance(value, dict) or value.get('command_id') != command_id
            or value.get('state') != 'complete' or type(value.get('exit_code')) is not int
            or not 0 <= value['exit_code'] <= 255):
        raise StandError('runner_receipt_invalid')
    return value['exit_code']


def validate_quality_corpus(path):
    """Check the pinned private corpus before any stand resources are created."""
    path = Path(path)
    try:
        if path.is_symlink() or not path.is_file():
            raise ValueError
        info = path.stat()
        parent = path.parent.stat()
        if (info.st_uid != os.geteuid() or info.st_mode & 0o077
                or not path.parent.is_dir() or parent.st_uid != os.geteuid()
                or parent.st_mode & 0o077 or info.st_size > 64 * 1024**2):
            raise ValueError
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except (OSError, ValueError):
        raise StandError('quality_corpus_invalid') from None
    if digest != QUALITY_CORPUS_SHA256:
        raise StandError('quality_corpus_hash_mismatch')
    return digest


def validate_quality_captures(path):
    path = Path(path)
    try:
        info = path.stat()
    except OSError:
        raise StandError('quality_captures_invalid') from None
    if (path.is_symlink() or not path.is_dir() or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise StandError('quality_captures_invalid')


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
    base_common = 'meta nfproto ipv6 counter drop\n'
    common = base_common + 'ct state invalid counter drop\n'
    output_common = common
    input_common = common
    input_diag = ''
    if role == 'browser':
        invalid_diag = (
            'oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 tcp flags & (syn | ack) == syn '
            'ct state invalid counter comment "diag_loopback5100_invalid_syn"\n'
            'oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 tcp flags & (syn | ack) == (syn | ack) '
            'ct state invalid counter comment "diag_loopback5100_invalid_synack"\n')
        input_invalid_diag = (
            'iifname "lo" ip daddr 127.0.0.1 tcp dport 5100 tcp flags & (syn | ack) == syn '
            'ct state invalid counter comment "diag_loopback5100_input_invalid_syn"\n'
            'iifname "lo" ip saddr 127.0.0.1 tcp sport 5100 tcp flags & (syn | ack) == (syn | ack) '
            'ct state invalid counter comment "diag_loopback5100_input_invalid_synack"\n')
        fallback_diag = (
            'oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 tcp flags & (syn | ack) == syn '
            'counter comment "diag_loopback5100_fallback_syn"\n'
            'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 tcp flags & (syn | ack) == syn '
            'counter comment "diag_loopback5100_fallback_relay_uid_syn"\n'
            'oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 tcp flags & (syn | ack) == (syn | ack) '
            'counter comment "diag_loopback5100_fallback_synack"\n'
            'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 tcp flags & (syn | ack) == (syn | ack) '
            'counter comment "diag_loopback5100_fallback_cloak_uid_synack"\n')
        output_common = base_common + invalid_diag + 'ct state invalid counter drop\n'
        input_common = base_common + input_invalid_diag + 'ct state invalid counter drop\n'
        input_diag = (
            'iifname "lo" ip daddr 127.0.0.1 tcp dport 5100 tcp flags & (syn | ack) == syn '
            'counter comment "diag_loopback5100_input_fallback_syn"\n'
            'iifname "lo" ip saddr 127.0.0.1 tcp sport 5100 tcp flags & (syn | ack) == (syn | ack) '
            'counter comment "diag_loopback5100_input_fallback_synack"\n')
        accepted_diag = (
            'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 '
            'tcp flags & (syn | ack) == syn counter comment "diag_loopback5100_accept_relay_uid_syn"\n'
            'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 '
            'tcp flags & (syn | ack) == (syn | ack) counter comment "diag_loopback5100_accept_cloak_uid_synack"\n')
        input_accepted_diag = (
            'iifname "lo" ip daddr 127.0.0.1 tcp dport 5100 tcp flags & (syn | ack) == syn '
            'counter comment "diag_loopback5100_input_accept_syn"\n'
            'iifname "lo" ip saddr 127.0.0.1 tcp sport 5100 tcp flags & (syn | ack) == (syn | ack) '
            'counter comment "diag_loopback5100_input_accept_synack"\n')
        output = (f'ip daddr {gateway} tcp dport 8081 ct state {{ new, established }} counter accept\n'
                  f'ip daddr {control} tcp sport 9222 ct state established counter accept\n'
                  + accepted_diag +
                  'meta skuid 65534 oifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } '
                  'counter accept\n'
                  'meta skuid 0 oifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established '
                  'counter accept\n')
        incoming = (f'ip saddr {gateway} tcp sport 8081 ct state established counter accept\n'
                    f'ip saddr {control} tcp dport 9222 ct state {{ new, established }} counter accept\n'
                    + input_accepted_diag +
                    'iifname "lo" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } '
                    'counter accept\n'
                    'iifname "lo" ip saddr 127.0.0.1 tcp sport 5100 ct state established '
                    'counter accept\n')
    elif role == 'control':
        common += 'ct state invalid counter drop\n'
        output = (f'ip daddr {browser} tcp dport 9222 ct state {{ new, established }} counter accept\n'
                  f'ip daddr {gateway} tcp dport 8081 ct state {{ new, established }} counter accept\n'
                  'ip daddr 127.0.0.1 tcp dport 8080 ct state { new, established } counter accept\n'
                  'ip saddr 127.0.0.1 tcp sport 8080 ct state established counter accept\n')
        incoming = (f'ip saddr {browser} tcp sport 9222 ct state established counter accept\n'
                    f'ip saddr {gateway} tcp sport 8081 ct state established counter accept\n'
                    'iifname "lo" ip saddr 127.0.0.1 tcp dport 8080 ct state { new, established } counter accept\n'
                    'iifname "lo" ip saddr 127.0.0.1 tcp sport 8080 ct state established counter accept\n')
    elif role == 'gateway':
        common += 'ct state invalid counter drop\n'
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
    if role == 'browser':
        output += fallback_diag
    return (f'table inet {TABLE} {{\nchain output {{\ntype filter hook output priority -150; policy drop;\n'
            + output_common + output + 'counter drop\n}\nchain input {\n'
            'type filter hook input priority -150; policy drop;\n' + input_common + incoming
            + input_diag + 'counter drop\n}\nchain forward {\ntype filter hook forward priority -150; policy drop;\n}\n}\n')


def mem_available():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) * 1024


def probe_denied(result, delta, ip):
    address=ipaddress.ip_address(ip)
    no_route=result.get('errno') in (97,101) and (address.is_global or address.version==6)
    return not result['connected'] and (delta > 0 or no_route)


def validate_supervisor_cleanup_options(*, supervisor_cleanup, bounded_profile, bootstrap,
                                        gate_only, live, variant, deadline, supervisor_label,
                                        quality_profile=False):
    if not supervisor_cleanup:
        return
    gate_mode = (bounded_profile and bootstrap and gate_only and not live and not quality_profile
                 and variant in (None, 'C'))
    quality_mode = (bounded_profile and quality_profile and live and not bootstrap and not gate_only
                    and variant == 'C')
    if (not (gate_mode or quality_mode) or deadline is None or not supervisor_label
            or not re.fullmatch(r'linux-vps-gate=[a-z0-9-]{8,48}', supervisor_label)):
        raise StandError('supervisor_cleanup_requires_bounded_profile_and_label')


def validate_quality_gate_receipt(path, parent_label):
    """Require a gate receipt produced after the outer supervisor confirmed cleanup."""
    path = Path(path)
    if path.is_symlink() or not path.is_dir() or path.stat().st_mode & 0o077:
        raise StandError('controlled_gate_receipt_invalid')
    try:
        receipt_files = [path / name for name in ('manifest.json', 'browser-trap.json', 'outer-cleanup.json')]
        if any(item.is_symlink() or not item.is_file() or item.stat().st_size > 64 * 1024
               for item in receipt_files):
            raise ValueError
        manifest = json.loads((path / 'manifest.json').read_text())
        trap = json.loads((path / 'browser-trap.json').read_text())
        cleanup = json.loads((path / 'outer-cleanup.json').read_text())
    except (OSError, ValueError):
        raise StandError('controlled_gate_receipt_invalid') from None
    if (not isinstance(manifest, dict) or not isinstance(trap, dict) or not isinstance(cleanup, dict)
            or manifest.get('state') != 'cleaned'
            or manifest.get('previous_phase') != 'controlled_browser_gate_passed'
            or manifest.get('bootstrap') is not True
            or not re.fullmatch(r'news-native-shadow-[0-9a-f]{10}', str(manifest.get('prefix', '')))
            or trap.get('accepted') != 0
            or cleanup.get('state') != 'confirmed'
            or cleanup.get('parent_label') != parent_label
            or cleanup.get('owned_containers_remaining') != 0
            or cleanup.get('owned_networks_remaining') != 0
            or cleanup.get('final_guard_passed') is not True
            or not re.fullmatch(r'[0-9a-f]{64}', str(cleanup.get('source_manifest_sha256', '')))
            or not re.fullmatch(r'[0-9a-f]{64}', str(cleanup.get('runtime_bundle_manifest_sha256', '')))):
        raise StandError('controlled_gate_receipt_invalid')
    return manifest


class Stand:
    def __init__(self, stage, *, bootstrap=False, deadline=None, bounded_profile=False,
                 supervisor_label=None, supervisor_cleanup=False, quality_profile=False,
                 synthetic_soak=False):
        self.stage = Path(stage).resolve()
        if not self.stage.is_dir() or self.stage.is_symlink():
            raise StandError('stage_missing')
        self.bootstrap = bootstrap
        self.bounded_profile = bounded_profile
        self.quality_profile = quality_profile
        self.synthetic_soak = synthetic_soak
        if synthetic_soak and (not bootstrap or not bounded_profile or quality_profile):
            raise StandError('synthetic_soak_profile_invalid')
        if supervisor_label and not re.fullmatch(r'linux-vps-gate=[a-z0-9-]{8,48}',supervisor_label):
            raise StandError('invalid_supervisor_label')
        self.supervisor_label=supervisor_label
        self.supervisor_cleanup=supervisor_cleanup
        if supervisor_cleanup and (not bounded_profile or not supervisor_label):
            raise StandError('supervisor_cleanup_requires_bounded_profile_and_label')
        self.prefix = 'news-native-shadow-' + secrets.token_hex(5)
        self.label = 'native-shadow-run=' + self.prefix
        self.containers = []
        self.networks = []
        self.anchors = {}
        self.started = time.monotonic()
        self.deadline = min(time.time()+7200,deadline) if deadline is not None else time.time()+7200
        self.phase = 'created'
        self.phase_deadline = None
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

    def diagnostic_logs(self, name):
        if name != getattr(self, 'worker', None) or name not in self.containers:
            raise StandError('diagnostic_log_target_invalid')
        self.owned(name)
        result = subprocess.run(['docker', 'logs', '--tail', '1000', name],
                                capture_output=True, text=True, timeout=10)
        if result.returncode:
            raise StandError('diagnostic_logs_unavailable')
        return (result.stdout + '\n' + result.stderr)[-262144:]

    def run(self, suffix, image, args, *, network, memory='32m', cpu='0.25', pids=64,
            mounts=(), env=(), entrypoint='sleep', readonly=True, extra=()):
        name = self.prefix + '-' + suffix
        command_args = ['run', '-d', '--name', name, '--label', self.label, '--network', network,
                        '--no-healthcheck', '--restart=no', '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
                        '--cpus', cpu, '--memory', memory, '--pids-limit', str(pids),
                        '--log-opt', 'max-size=10m', '--log-opt', 'max-file=2']
        if self.supervisor_label:
            command_args += ['--label',self.supervisor_label]
        if self.bounded_profile:
            command_args += ['--memory-swap', memory]
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
            if self.supervisor_label:
                args += ['--label',self.supervisor_label]
            if kind == 'private':
                args += ['--internal']
            self.networks.append(name)
            self.docker(*args, name)
        for role in ('browser', 'control', 'gateway'):
            network = self.networks[1] if role == 'gateway' and not self.bootstrap else self.networks[0]
            profile = {'memory':'16m','cpu':'0.0625','pids':32} if self.bounded_profile else {}
            name = self.run(role + '-anchor', APP, ['7200'], network=network, **profile,
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
        if self.phase_deadline is not None and time.time() >= self.phase_deadline:
            raise StandError(self.phase + '_phase_timeout')
        minimum = 1536 * 1024**2 if self.bounded_profile else 1024**3
        if mem_available() < minimum:
            raise StandError('host_memory_watchdog')

    def begin_phase(self, name, seconds):
        self.phase = name
        self.phase_deadline = min(self.deadline, time.time() + seconds)
        if self.bounded_profile and hasattr(signal, 'setitimer'):
            remaining = max(0.001, self.phase_deadline-time.time())
            signal.setitimer(signal.ITIMER_REAL, remaining)

    def check_runtime(self):
        self.check_budget()
        if not self.containers:
            raise StandError('runtime_empty')
        lines=self.docker('inspect','--format','{{.State.Running}} {{.RestartCount}} {{.State.OOMKilled}}',
                          *self.containers).splitlines()
        if len(lines)!=len(self.containers) or any(row.split()!=['true','0','false'] for row in lines):
            raise StandError('runtime_stopped_restarted_or_oom')

    def monitored_exec(self, args):
        """Run the long job detached so its Docker CLI does not inflate host-tree RSS."""
        token=getattr(self,'token',None)
        if (not args or any(not isinstance(arg, str) for arg in args)
                or (token and any(token in arg for arg in args))):
            raise StandError('runner_arguments_invalid')
        command_id=secrets.token_hex(16)
        command_path=self.run_dir/('runner-command-'+command_id+'.json')
        status_path=self.run_dir/('runner-status-'+command_id+'.json')
        if command_path.exists() or command_path.is_symlink() or status_path.exists() or status_path.is_symlink():
            raise StandError('runner_receipt_stale')
        fd,tmp_name=tempfile.mkstemp(prefix='.runner-command-',dir=self.run_dir)
        try:
            os.fchmod(fd,0o600)
            with os.fdopen(fd,'w',encoding='utf-8') as command_file:
                json.dump({'command_id':command_id,'args':args},command_file,separators=(',',':'))
                command_file.write('\n'); command_file.flush(); os.fsync(command_file.fileno())
            os.replace(tmp_name,command_path)
        finally:
            try: os.unlink(tmp_name)
            except FileNotFoundError: pass
        try:
            start=subprocess.run(['docker','exec','-d',self.observer,'python','-c',
                                  RUNNER_WRAPPER,command_id],capture_output=True,text=True,timeout=10)
            if start.returncode:
                raise StandError('runner_start_failed')
            while True:
                self.check_runtime()
                if status_path.exists() or status_path.is_symlink():
                    exit_code=read_runner_receipt(status_path,command_id)
                    self.check_runtime()
                    if exit_code:
                        raise StandError('runner_failed')
                    return
                time.sleep(2)
        except subprocess.TimeoutExpired:
            raise StandError('runner_start_timeout') from None
        finally:
            try:
                info=command_path.lstat()
                if not command_path.is_symlink() and command_path.is_file() and info.st_size<=65536:
                    command_path.unlink()
            except FileNotFoundError:
                pass

    def resource_groups(self,variant):
        groups={'cloak':self.cloak,'gateway':self.gateway,'relay':self.relay,
                'browser_anchor':self.anchors['browser']['name'],
                'control_anchor':self.anchors['control']['name'],
                'gateway_anchor':self.anchors['gateway']['name']}
        if variant != 'C':
            raise StandError('invalid_variant')
        groups.update(worker=self.worker,observer=self.observer)
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
        if variant!='C':
            raise StandError('invalid_variant')
        args+=['--endpoint-c','http://127.0.0.1:8080']
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

    def browser_loopback_diagnostic_counters(self):
        """Read only finite loopback counters from the exact owned browser namespace."""
        output_names = (
            'diag_loopback5100_invalid_syn', 'diag_loopback5100_invalid_synack',
            'diag_loopback5100_fallback_syn', 'diag_loopback5100_fallback_relay_uid_syn',
            'diag_loopback5100_fallback_synack', 'diag_loopback5100_fallback_cloak_uid_synack',
            'diag_loopback5100_accept_relay_uid_syn', 'diag_loopback5100_accept_cloak_uid_synack',
        )
        input_names = (
            'diag_loopback5100_input_invalid_syn', 'diag_loopback5100_input_invalid_synack',
            'diag_loopback5100_input_fallback_syn', 'diag_loopback5100_input_fallback_synack',
            'diag_loopback5100_input_accept_syn', 'diag_loopback5100_input_accept_synack',
        )
        all_names = BROWSER_LOOPBACK_COUNTERS
        anchor = self.anchors['browser']['name']
        pid = self.verify_ns(anchor, anchor)
        try:
            result = {}
            for chain, names in (('output', output_names), ('input', input_names)):
                data = json.loads(command(['nsenter', '-t', pid, '-n', '--', 'nft', '-j',
                                           'list', 'chain', 'inet', TABLE, chain], timeout=5))
                for item in data['nftables']:
                    rule = item.get('rule')
                    if not isinstance(rule, dict):
                        continue
                    comment = rule.get('comment')
                    if isinstance(comment, str) and comment.startswith('diag_loopback5100_') and comment not in names:
                        raise ValueError
                    if comment not in names:
                        continue
                    counters = [expr['counter'] for expr in rule['expr']
                                if isinstance(expr, dict) and isinstance(expr.get('counter'), dict)]
                    if len(counters) != 1:
                        raise ValueError
                    value = counters[0].get('packets')
                    if type(value) is not int or not 0 <= value <= (1 << 63) - 1:
                        raise ValueError
                    if comment in result:
                        raise ValueError
                    result[comment] = value
            if set(result) != set(all_names):
                raise ValueError
            return result
        except (AttributeError, IndexError, KeyError, TypeError, ValueError, OSError, StandError):
            raise StandError('browser_nft_diagnostics_unavailable') from None

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
        if getattr(self, 'synthetic_soak', False):
            try:
                baseline = self.browser_loopback_diagnostic_counters()
                if (set(baseline) == set(BROWSER_LOOPBACK_COUNTERS)
                        and all(type(baseline[name]) is int and 0 <= baseline[name] <= (1 << 63) - 1
                                for name in BROWSER_LOOPBACK_COUNTERS)):
                    evidence['browser_loopback_nft_counters_after_gate'] = baseline
                    path = self.run_dir / 'raw-network-gate.json'
                    if not path.is_symlink():
                        path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + '\n',
                                        encoding='utf-8')
                        path.chmod(0o600)
            except Exception:
                # The diagnostic baseline is optional and never weakens or blocks the gate.
                pass
        self.write_manifest('raw_network_gate_passed')
        return evidence

    def start_runtime(self, variant='C'):
        if variant != 'C':
            raise StandError('invalid_variant')
        browser, control, gateway = (self.anchors[x] for x in ('browser','control','gateway'))
        bin_mount = str(self.stage/'bin-soak') + ':/bin-native:ro'
        bench_mount = str(self.stage/'benchmark') + ':/bench:ro'
        env_mount = ['--env-file',str(self.env_file)]
        gateway_env = ['EGRESS_LISTEN_ADDR='+gateway['ip']+':8081',
                       'EGRESS_MAX_CONCURRENT='+('4' if self.bounded_profile else '16')]
        if self.bootstrap:
            gateway_env += ['EGRESS_TEST_MODE=true','EGRESS_TEST_ALLOWLIST={"native-fixture.test":"127.0.0.1"}']
        self.gateway = self.run('egress',APP,[],network='container:'+gateway['name'],
                                memory='64m' if self.bounded_profile else '128m',
                                cpu='0.125' if self.bounded_profile else '0.25',pids=64,
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
        launch_info=info/'launch-info.json'
        if self.bounded_profile:
            try:
                launch=json.loads(launch_info.read_text())
            except (OSError,ValueError):
                raise StandError('pinned_browser_launch_info_missing') from None
            if launch.get('binary_sha256')!='715722e8605ae3ce81523c1218aba1ec89425786ab33ceaf99f8a6cb5e70e6e8':
                raise StandError('pinned_browser_binary_digest_mismatch')
        relay_args=['/entry/relay.py','--controller-ip',control['ip']]
        relay_mounts=[str(self.stage/'benchmark/scripts/shadow_cdp_relay.py')+':/entry/relay.py:ro']
        if self.bounded_profile:
            relay_control=self.run_dir/'relay-arm'
            relay_report=self.run_dir/'relay-report'
            relay_control.mkdir(mode=0o755)
            os.chmod(relay_control,0o755)
            relay_report.mkdir(mode=0o700)
            os.chown(relay_report,65534,65534)
            relay_args+=['--synthetic-arm-dir','/relay-arm','--synthetic-report','/relay-report/metrics.json']
            relay_args+=['--synthetic-diagnostics-report','/relay-report/relay-diagnostics.json']
            relay_mounts += [str(relay_control)+':/relay-arm:ro',str(relay_report)+':/relay-report:rw']
        self.relay=self.run('relay',APP,relay_args,
                            network='container:'+browser['name'],memory='32m',
                            cpu='0.125' if self.bounded_profile else '0.25',pids=64,entrypoint='python',
                            mounts=relay_mounts,
                            extra=['--user','65534:65534'])
        self.verify_ns(self.relay,browser['name'])
        chrome_endpoint = 'http://'+browser['ip']+':9222'
        observer_memory = ('256m' if self.quality_profile else
                           '128m' if self.bounded_profile else '256m')
        observer_cpu = ('0.25' if self.quality_profile else
                        '0.125' if self.bounded_profile else '0.25')
        production_core_mounts=(observer_core_extractor_mounts(self.stage)
                                if getattr(self,'worker_recovery_gate',False) else [])
        self.observer = self.run('observer',APP,['7200'],network='container:'+control['name'],
            memory=observer_memory,
            cpu=observer_cpu,
            pids=64 if self.bounded_profile else 128,
            mounts=[bench_mount,bin_mount,*observer_extraction_mounts(self.stage),
                    *production_core_mounts,str(self.run_dir)+':/out',
                    '/sys/fs/cgroup:/host-cg:ro'] + ([str(self.corpus)+':/corpus/corpus.json:ro',str(self.captures)+':/captures']
                    if hasattr(self,'corpus') else []),env=['PYTHONPATH=/app','BROWSER_WS_ENDPOINT='+chrome_endpoint,
                    'EXTRACTION_SERVICE_MODE=go','EXTRACTION_SERVICE_PYTHON_FALLBACK=false',
                    'SCHEDULER_ENABLED=false'],extra=env_mount)
        self.verify_ns(self.observer,control['name'])
        self.wait_http(self.observer,chrome_endpoint+'/json/version')
        uid_rows=self.docker('top',self.cloak,'-eo','uid,pid,comm').splitlines()[1:]
        if not uid_rows or any(row.split()[0]!='0' for row in uid_rows):
            raise StandError('browser_uid_boundary_changed')
        relay_rows=self.docker('top',self.relay,'-eo','uid,pid,comm').splitlines()[1:]
        if not relay_rows or any(row.split()[0]!='65534' for row in relay_rows):
            raise StandError('relay_uid_boundary_changed')
        self.wait_http(self.observer,'http://'+gateway['ip']+':8081/health/ready')
        self.worker = self.run('worker',APP,[],network='container:'+control['name'],
                memory='384m' if self.quality_profile or not self.bounded_profile else '128m',
                cpu='0.5' if self.quality_profile or not self.bounded_profile else '0.25',
                pids=128,mounts=[bin_mount],
                entrypoint='/bin-native/extract-service',extra=env_mount,
                env=['LISTEN_ADDR=127.0.0.1:8080','EXTRACTOR_HTTP_ONLY=false','EXTRACTOR_BROWSER_BACKEND=native',
                     'CHROME_ENDPOINT='+chrome_endpoint,'EXTRACTOR_NATIVE_PROXY_URL=http://'+gateway['ip']+':8081',
                     'EXTRACTOR_NATIVE_NETWORK_ISOLATED=true',
                     'EXTRACTOR_NATIVE_READY_SELECTOR='+('[data-ready="true"]'
                         if self.synthetic_soak or (self.bootstrap and self.bounded_profile
                                                     and not self.quality_profile) else ''),
                     'EXTRACTOR_NATIVE_DIAGNOSTICS=' + native_diagnostics_mode(
                         quality_profile=self.quality_profile, synthetic_soak=self.synthetic_soak,
                         bounded_gate=(self.bootstrap and self.bounded_profile
                                       and not self.quality_profile and not self.synthetic_soak))])
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

    def production_core_extractor_native_gate(self, browser_generation):
        """Route fixed success and auth-failure fixtures through production Python into Go/Cloak."""
        self.check_budget()
        path=self.run_dir/'core-extractor-native.json'
        if path.exists() or path.is_symlink():
            raise StandError('core_extractor_native_receipt_stale')
        if not isinstance(browser_generation,str) or not re.fullmatch('[0-9a-f]{64}',browser_generation):
            raise StandError('core_extractor_native_browser_generation_invalid')
        self.check_runtime()
        try:
            self.docker('exec',self.observer,'python','-c',CORE_EXTRACTOR_NATIVE_GATE,
                        browser_generation,timeout=75)
        except (StandError,subprocess.TimeoutExpired,subprocess.CalledProcessError):
            raise StandError('core_extractor_native_gate_failed') from None
        receipt=read_core_extractor_native_receipt(path)
        self.check_runtime()
        self.check_budget()
        return receipt

    @staticmethod
    def browser_request_script():
        return '''import json,os,sys,time,urllib.request,urllib.error
route,prefix=sys.argv[1:3]
payload={'request_id':prefix+'-'+route+'-'+str(time.monotonic_ns()),'url':'https://native-fixture.test/'+route,
         'strategy':'browser_rendering','source_type':'article','use_browser':True,'budget_ms':30000}
req=urllib.request.Request('http://127.0.0.1:8080/v1/extract',data=json.dumps(payload).encode(),
 headers={'Content-Type':'application/json','Authorization':'Bearer '+os.environ['EXTRACTOR_TEST_TOKEN']})
try:
 with urllib.request.urlopen(req,timeout=35) as response: status=response.status; value=json.load(response)
except urllib.error.HTTPError as response: status=response.code; value=json.load(response)
content=value.get('result',{}).get('content','') or ''
print(json.dumps({'status':status,'outcome':value.get('outcome'),
 'error':(value.get('error') or {}).get('code') or (value.get('result') or {}).get('error'),
 'chars':len(content),'cookie_confirmed':'cookie confirmed' in content}))
'''

    @staticmethod
    def stopped_worker_request_script():
        return '''import json,os,sys,time,urllib.request,urllib.error
route,prefix=sys.argv[1:3]
payload={'request_id':prefix+'-'+route+'-'+str(time.monotonic_ns()),'url':'https://native-fixture.test/'+route,
         'strategy':'browser_rendering','source_type':'article','use_browser':True,'budget_ms':30000}
req=urllib.request.Request('http://127.0.0.1:8080/v1/extract',data=json.dumps(payload).encode(),
 headers={'Content-Type':'application/json','Authorization':'Bearer '+os.environ['EXTRACTOR_TEST_TOKEN']})
transport_unavailable=False
try:
 with urllib.request.urlopen(req,timeout=3) as response: status=response.status; value=json.load(response)
except urllib.error.HTTPError as response:
 status=response.code; value=json.load(response)
except urllib.error.URLError as error:
 if not isinstance(error.reason,ConnectionRefusedError): raise
 status=None; value={}; transport_unavailable=True
content=value.get('result',{}).get('content','') or ''
print(json.dumps({'status':status,'outcome':value.get('outcome'),
 'error':(value.get('error') or {}).get('code') or (value.get('result') or {}).get('error')
         or ('unavailable' if transport_unavailable else None),
 'chars':len(content),'cookie_confirmed':'cookie confirmed' in content,
 'transport_unavailable':transport_unavailable}))
'''

    def _start_controlled_crash_helper(self):
        receipt=self.run_dir/'controlled-crash.json'
        if receipt.exists() or receipt.is_symlink():
            raise StandError('native_crash_receipt_stale')
        self.docker('exec','-d',self.observer,'python','-c',CONTROLLED_CRASH_HELPER,timeout=10)

    def _wait_controlled_crash_receipt(self):
        receipt=self.run_dir/'controlled-crash.json'
        deadline=time.monotonic()+3
        while not receipt.exists() and not receipt.is_symlink():
            self.check_budget()
            if time.monotonic()>=deadline:
                raise StandError('native_crash_receipt_missing')
            time.sleep(.05)
        return read_controlled_crash_receipt(receipt)

    def synthetic_overload_gate(self, post=None):
        if not self.bounded_profile:
            return None
        post = post or self.browser_request_script()
        self.begin_phase('synthetic_overload',120)
        arm=self.run_dir/'relay-arm'/'overload-once.json'
        fd=os.open(arm,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
        os.fchmod(fd,0o644)
        with os.fdopen(fd,'w') as stream:
            stream.write('{"arm":"overload-once"}')
            stream.flush();os.fsync(stream.fileno())
        overload=json.loads(self.docker('exec',self.observer,'python','-c',post,
                                        'js-article','synthetic-overload',timeout=40))
        (self.run_dir/'overload.json').write_text(json.dumps(overload)+'\n')
        if (overload.get('status')!=200 or overload.get('outcome')!='rejected'
                or overload.get('error')!='native_browser_unavailable' or overload.get('chars')!=0):
            raise StandError('synthetic_overload_not_terminal')
        ready=json.loads(self.docker('exec',self.observer,'python','-c',
            "import json,urllib.error,urllib.request\ntry:\n r=urllib.request.urlopen('http://127.0.0.1:8080/health/ready',timeout=2); status=r.status; value=json.load(r)\nexcept urllib.error.HTTPError as e: status=e.code; value=json.load(e)\nprint(json.dumps({'status':status,'ready':value.get('ready'),'browser':value.get('capabilities',{}).get('browser')}))"))
        if ready.get('status')!=503 or ready.get('ready') is not False or ready.get('browser') is not False:
            raise StandError('tainted_native_readiness_not_false')
        report_path=self.run_dir/'relay-report'/'metrics.json'
        try:
            metrics=json.loads(report_path.read_text())
        except (OSError,ValueError):
            raise StandError('synthetic_relay_report_missing') from None
        if (metrics.get('overload_barriers')!=1 or metrics.get('notifications_injected')!=257
                or not metrics.get('cleanup_confirmed')):
            raise StandError('synthetic_cleanup_receipts_missing')
        rejected=json.loads(self.docker('exec',self.observer,'python','-c',post,
                                        'js-article','synthetic-post-taint',timeout=10))
        (self.run_dir/'post-taint-rejection.json').write_text(json.dumps(rejected)+'\n')
        after=json.loads(report_path.read_text())
        if (rejected.get('status')!=503 or rejected.get('error')!='unavailable'
                or after.get('owned_context_allocations')!=metrics.get('owned_context_allocations')
                or after.get('owned_target_allocations')!=metrics.get('owned_target_allocations')):
            raise StandError('tainted_next_request_allocated_browser')
        return {'overload_rejected': True, 'barrier_count': metrics['overload_barriers'],
                'notifications_injected': metrics['notifications_injected'],
                'cleanup_confirmed': True, 'post_taint_rejected': True}

    def _controlled_browser_routes(self):
        fixture_dir=self.stage/'benchmark/tests/fixtures'
        proof=self.run_dir/'origin-proof.jsonl'
        self.origin=self.run('origin',APP,['/fixtures/native-shadow/gate_origin.py',
            '--cert','/tls/cert.pem','--key','/tls/key.pem','--proof-output','/out/origin-proof.jsonl',
            '--trap-ip',self.anchors['control']['ip']],network='container:'+self.anchors['gateway']['name'],
            memory='128m',cpu='0.125' if self.bounded_profile else '0.25',pids=64,
            mounts=[str(fixture_dir)+':/fixtures:ro',str(self.stage/'tls')+':/tls:ro',
                                 str(self.run_dir)+':/out'],entrypoint='python')
        self.verify_ns(self.origin,self.anchors['gateway']['name'])
        self.wait_listener('gateway',443,0)
        post=self.browser_request_script()
        results=[]
        generation_script = ("import hashlib,json,os,urllib.request; "
            "r=urllib.request.urlopen(os.environ['BROWSER_WS_ENDPOINT'].rstrip('/')+'/json/version',timeout=2); "
            "v=json.load(r); print(json.dumps({'browser_generation':hashlib.sha256(v['webSocketDebuggerUrl'].encode()).hexdigest()}))")
        def browser_generation():
            value=json.loads(self.docker('exec',self.observer,'python','-c',generation_script))
            generation=value.get('browser_generation')
            if not isinstance(generation,str) or not re.fullmatch('[0-9a-f]{64}',generation):
                raise StandError('native_browser_generation_invalid')
            return generation
        generation=browser_generation()
        if getattr(self, 'worker_recovery_gate', False):
            self.production_core_extractor_native_gate(generation)
            if browser_generation()!=generation:
                raise StandError('core_extractor_native_browser_generation_changed')
        crash_recovered=False
        crash_helper_receipt_verified=False
        routes=[('js-article','js-article'),('wire-limit','wire-limit'),
                ('js-article-after-wire-limit','js-article'),
                ('controlled-crash','controlled-crash'),('js-article-after-crash','js-article'),
                ('private-subrequest','private-subrequest'),('redirect-private','redirect-private'),
                ('iframe-private','iframe-private'),('worker-private','worker-private'),
                ('popup-private','popup-private'),('websocket-private','websocket-private'),
                ('download','download'),('origin-auth','origin-auth')]
        for case,route in routes:
            if route=='controlled-crash':
                self._start_controlled_crash_helper()
            result=json.loads(self.docker('exec',self.observer,'python','-c',post,route,'gate',timeout=40))
            helper_receipt_verified=False
            if route=='controlled-crash':
                helper_receipt_verified=self._wait_controlled_crash_receipt()
                crash_helper_receipt_verified=helper_receipt_verified
            result['case']=case
            results.append(result)
            (self.run_dir/'browser-gate.json').write_text(json.dumps(results,indent=2)+'\n')
            if case in ('js-article','js-article-after-wire-limit','js-article-after-crash'):
                if result['outcome']!='success' or not result['cookie_confirmed']:
                    raise StandError('positive_native_browser_gate_failed')
                if case=='js-article-after-crash':
                    if not crash_recovered or browser_generation()!=generation:
                        raise StandError('native_crash_generation_changed')
                    (self.run_dir/'crash-recovery.json').write_text(json.dumps({
                        'owned_page_crash_observed':True,'failed_output_empty':True,
                        'readiness_recovered':True,'browser_generation_unchanged':True,
                        'next_article_succeeded':True,'failed_article_retried':False,
                        'controlled_helper_receipt_verified':crash_helper_receipt_verified})+'\n')
            elif route in ('wire-limit','controlled-crash'):
                crashed=route=='controlled-crash'
                expected_error='native_browser_target_crashed' if crashed else 'resource_limit'
                expected_outcome='failed' if crashed else 'rejected'
                if (result.get('status')!=200 or result.get('outcome')!=expected_outcome
                        or result.get('error')!=expected_error or result.get('chars')!=0):
                    raise StandError('native_crash_gate_failed' if crashed else 'native_wire_limit_gate_failed')
                readiness=json.loads(self.docker('exec',self.observer,'python','-c',
                    "import json,urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8080/health/ready',timeout=2); value=json.load(r); print(json.dumps({'status':r.status,'ready':value.get('ready'),'capabilities':value.get('capabilities',{})}))"))
                capabilities=readiness.get('capabilities') if isinstance(readiness,dict) else None
                if (readiness.get('status')!=200 or readiness.get('ready') is not True
                        or not isinstance(capabilities,dict) or capabilities.get('browser') is not True
                        or capabilities.get('native_browser') is not True):
                    raise StandError('native_crash_readiness_lost' if crashed else 'native_wire_limit_readiness_lost')
                if crashed:
                    if browser_generation()!=generation:
                        raise StandError('native_crash_generation_changed')
                    crash_recovered=True
            else:
                expected={'native_browser_policy_denied','native_browser_unsupported','public_url_denied'}
                if route=='origin-auth':
                    expected={'upstream_auth_required','native_browser_http_rejected'}
                if result['outcome']=='success' or result['error'] not in expected:
                    raise StandError('adversarial_browser_gate_failed')
            self.wait_http(self.observer,'http://127.0.0.1:8080/health/ready')
            self.check_budget()
        if getattr(self, 'worker_recovery_gate', False):
            self.synthetic_worker_recovery_gate(post, generation)
        else:
            self.synthetic_overload_gate(post)
        # Origin credentials must never include proxy auth; no raw header values are stored.
        records=[json.loads(line) for line in proof.read_text().splitlines()]
        if any(row.get('has_authorization') or row.get('has_proxy_authorization') for row in records):
            raise StandError('origin_auth_leak')
        return results

    def synthetic_worker_recovery_gate(self, post, browser_generation):
        """Exercise terminal native overflow, then explicitly restart only its owned worker."""
        self.begin_phase('synthetic_worker_recovery', 120)
        marker=self.stage/'.worker-recovery-active'
        if marker.exists() or marker.is_symlink():
            raise StandError('worker_recovery_marker_stale')
        fd=os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
        try:
            os.fchmod(fd,0o600)
            os.write(fd,self.worker.encode('ascii'))
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            return self._run_worker_recovery(post, browser_generation)
        finally:
            marker.unlink(missing_ok=True)

    def _run_worker_recovery(self, post, browser_generation):
        cloak_format=('{{.Id}}|{{.State.Running}}|{{.RestartCount}}|{{.State.OOMKilled}}|'
                      '{{index .Config.Labels "native-shadow-run"}}|'
                      '{{index .Config.Labels "linux-vps-gate"}}')
        cloak_identity=self.docker('inspect','--format',cloak_format,self.cloak).split('|')
        if (len(cloak_identity)!=6 or cloak_identity[1:]!=[
                'true','0','false',self.prefix,self.supervisor_label.split('=',1)[-1]]):
            raise StandError('native_browser_container_identity_invalid')
        arm=self.run_dir/'relay-arm'/'overload-once.json'
        fd=os.open(arm,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
        os.fchmod(fd,0o644)
        with os.fdopen(fd,'w') as stream:
            stream.write('{"arm":"overload-once"}')
            stream.flush();os.fsync(stream.fileno())
        overload=json.loads(self.docker('exec',self.observer,'python','-c',post,
                                        'js-article','synthetic-overflow-terminal',timeout=40))
        (self.run_dir/'worker-overflow.json').write_text(json.dumps({
            'status':overload.get('status'),'outcome':overload.get('outcome'),
            'error':overload.get('error'),'chars':overload.get('chars')})+'\n')
        if (overload.get('status')!=200 or overload.get('outcome')!='rejected'
                or overload.get('error')!='native_browser_unavailable' or overload.get('chars')!=0):
            raise StandError('synthetic_overflow_not_terminal_empty')
        report_path=self.run_dir/'relay-report'/'metrics.json'
        try:
            before_metrics=json.loads(report_path.read_text())
        except (OSError,ValueError):
            raise StandError('synthetic_relay_report_missing') from None
        before=(before_metrics.get('owned_context_allocations'),
                before_metrics.get('owned_target_allocations'))
        deadline=time.monotonic()+120
        while True:
            state=self.docker('inspect','--format',
                '{{.Name}}|{{.State.Running}}|{{.State.Pid}}|{{.State.ExitCode}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{index .Config.Labels "native-shadow-run"}}|{{index .Config.Labels "linux-vps-gate"}}',self.worker).split('|')
            expected_name=self.prefix+'-worker'
            if (len(state)==8 and state[0].lstrip('/')==expected_name and state[1]=='false'
                    and state[2]=='0' and state[3]=='1' and state[4]=='0' and state[5]=='false'
                    and state[6]==self.prefix and state[7]==self.supervisor_label.split('=',1)[-1]):
                break
            if time.monotonic()>=deadline:
                raise StandError('native_worker_terminal_exit_not_observed')
            self.check_budget();time.sleep(.25)
        try:
            metrics=json.loads(report_path.read_text())
        except (OSError,ValueError):
            raise StandError('synthetic_relay_report_missing') from None
        if (metrics.get('overload_barriers')!=1 or metrics.get('notifications_injected')!=257
                or not metrics.get('cleanup_confirmed')):
            raise StandError('synthetic_cleanup_receipts_missing')
        # Capture counters while the worker is stopped; do not restart after any new allocation.
        after_exit=(metrics.get('owned_context_allocations'),metrics.get('owned_target_allocations'))
        stopped=json.loads(self.docker('exec',self.observer,'python','-c',
            self.stopped_worker_request_script(),
            'js-article','synthetic-while-worker-stopped',timeout=10))
        try:
            after_request_metrics=json.loads(report_path.read_text())
        except (OSError,ValueError):
            raise StandError('synthetic_relay_report_missing') from None
        after_request=(after_request_metrics.get('owned_context_allocations'),
                       after_request_metrics.get('owned_target_allocations'))
        stopped_unavailable=(stopped.get('status')==503 and stopped.get('error')=='unavailable'
                             and stopped.get('transport_unavailable') is False)
        stopped_refused=(stopped.get('status') is None and stopped.get('error')=='unavailable'
                         and stopped.get('transport_unavailable') is True)
        if (not (stopped_unavailable or stopped_refused) or stopped.get('chars')!=0
                or before != after_exit or after_exit != after_request):
            raise StandError('native_allocation_after_terminal_overflow')
        self.docker('start',self.worker,timeout=10)
        self.wait_http(self.observer,'http://127.0.0.1:8080/health/ready')
        ready=json.loads(self.docker('exec',self.observer,'python','-c',
            "import json,urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8080/health/ready',timeout=2); v=json.load(r); print(json.dumps({'status':r.status,'ready':v.get('ready'),'browser':v.get('capabilities',{}).get('browser'),'native_browser':v.get('capabilities',{}).get('native_browser')}))"))
        if ready!={'status':200,'ready':True,'browser':True,'native_browser':True}:
            raise StandError('native_worker_recovery_not_ready')
        after_article=json.loads(self.docker('exec',self.observer,'python','-c',post,
            'js-article','synthetic-after-worker-restart',timeout=40))
        if after_article.get('outcome')!='success' or not after_article.get('cookie_confirmed'):
            raise StandError('native_worker_recovery_article_failed')
        generation_script=("import hashlib,json,os,urllib.request; r=urllib.request.urlopen(os.environ['BROWSER_WS_ENDPOINT'].rstrip('/')+'/json/version',timeout=2); v=json.load(r); print(json.dumps({'browser_generation':hashlib.sha256(v['webSocketDebuggerUrl'].encode()).hexdigest()}))")
        generation=json.loads(self.docker('exec',self.observer,'python','-c',generation_script)).get('browser_generation')
        if generation!=browser_generation:
            raise StandError('native_worker_recovery_browser_changed')
        after_cloak=self.docker('inspect','--format',cloak_format,self.cloak).split('|')
        if after_cloak!=cloak_identity:
            raise StandError('native_browser_container_changed')
        receipt={'terminal_overflow_empty':True,'worker_exit_code_1':True,
                 'cleanup_receipts_verified':True,'no_allocation_before_restart':True,
                 'worker_restarted':True,'fresh_readiness':True,'next_article_succeeded':True,
                 'browser_generation_unchanged':True,'browser_container_unchanged':True}
        (self.run_dir/'worker-recovery.json').write_text(json.dumps(receipt)+'\n')
        return receipt

    def cleanup(self):
        if getattr(self, 'supervisor_cleanup', False):
            # The supervisor owns exact-label container/network cleanup; only
            # remove the temporary token file here.
            self.env_file.unlink(missing_ok=True)
            return
        for name in reversed(self.containers):
            try:
                self.owned(name)
                if not self.bounded_profile:
                    logs = subprocess.run(['docker','logs','--tail','200',name],capture_output=True,text=True,timeout=10)
                    # Container logs are private evidence, never forwarded to the shared report.
                    (self.run_dir / (name + '.log')).write_text(logs.stdout + logs.stderr)
                self.docker('rm', '-f', name, timeout=5 if self.bounded_profile else 30)
            except (StandError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
                continue
        for name in reversed(self.networks):
            try:
                label = self.docker('network', 'inspect', '--format', '{{index .Labels "native-shadow-run"}}', name)
                if label == self.prefix:
                    self.docker('network', 'rm', name, timeout=5 if self.bounded_profile else 30)
            except (StandError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
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
    parser.add_argument('--variant',choices=['C'])
    parser.add_argument('--corpus',type=Path)
    parser.add_argument('--captures',type=Path)
    parser.add_argument('--case',action='append',default=[])
    parser.add_argument('--approved-gate',type=Path)
    parser.add_argument('--quality-profile',action='store_true')
    parser.add_argument('--quality-phase',choices=tuple(QUALITY_PHASE_CASES))
    parser.add_argument('--long-synthetic-soak',action='store_true',
                        help='run the fixed 10-minute native fixture stability soak and overload check')
    parser.add_argument('--worker-recovery-gate',action='store_true',
                        help='run bounded synthetic fatal-worker recovery acceptance')
    parser.add_argument('--deadline',type=int)
    parser.add_argument('--bounded-profile',action='store_true')
    parser.add_argument('--supervisor-label')
    parser.add_argument('--supervisor-cleanup',action='store_true',
                        help='leave Docker teardown to the exact-label gate supervisor')
    args = parser.parse_args()
    if args.worker_recovery_gate and (not args.bootstrap or not args.gate_only or args.live
            or not args.bounded_profile or args.variant not in (None,'C')
            or args.long_synthetic_soak or args.quality_profile):
        raise StandError('worker_recovery_gate_profile_invalid')
    validate_synthetic_soak_options(
        enabled=args.long_synthetic_soak, bootstrap=args.bootstrap, gate_only=args.gate_only,
        live=args.live, bounded_profile=args.bounded_profile,
        supervisor_cleanup=args.supervisor_cleanup, supervisor_label=args.supervisor_label,
        deadline=args.deadline, variant=args.variant, quality_profile=args.quality_profile,
        quality_phase=args.quality_phase, corpus=args.corpus, captures=args.captures,
        approved_gate=args.approved_gate, case_ids=args.case)
    if (args.bounded_profile and not args.quality_profile
            and (not args.bootstrap or not args.gate_only or args.live
                 or args.variant not in (None,'C') or args.deadline is None)):
        raise StandError('bounded_profile_requires_synthetic_gate')
    validate_supervisor_cleanup_options(
        supervisor_cleanup=args.supervisor_cleanup, bounded_profile=args.bounded_profile,
        bootstrap=args.bootstrap, gate_only=args.gate_only, live=args.live,
        variant=args.variant, deadline=args.deadline, supervisor_label=args.supervisor_label,
        quality_profile=args.quality_profile)
    if args.quality_profile:
        if (not args.live or not args.bounded_profile or not args.supervisor_cleanup
                or not args.quality_phase or not args.approved_gate or not args.corpus
                or not args.captures):
            raise StandError('quality_profile_configuration_invalid')
        validate_quality_phase(args.variant, args.quality_phase, args.case)
        validate_quality_gate_receipt(args.approved_gate, args.supervisor_label)
        validate_quality_corpus(args.corpus)
        validate_quality_captures(args.captures)
    elif args.quality_phase:
        raise StandError('quality_phase_requires_quality_profile')
    def interrupted(*_):
        raise StandError('stand_interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    if args.bounded_profile and hasattr(signal, 'setitimer'):
        def phase_timeout(*_):
            raise StandError('bounded_phase_timeout')
        signal.signal(signal.SIGALRM,phase_timeout)
    if args.live:
        if args.bootstrap or not all([args.variant,args.corpus,args.captures,args.approved_gate,args.deadline]):
            raise StandError('live_configuration_missing')
        gate=json.loads((args.approved_gate/'manifest.json').read_text())
        trap=json.loads((args.approved_gate/'browser-trap.json').read_text())
        if (gate.get('state')!='cleaned' or gate.get('previous_phase')!='controlled_browser_gate_passed'
                or gate.get('bootstrap') is not True or trap.get('accepted')!=0):
            raise StandError('controlled_gate_not_passed')
    stand = Stand(args.stage, bootstrap=args.bootstrap,deadline=args.deadline,
                  bounded_profile=args.bounded_profile,supervisor_label=args.supervisor_label,
                  supervisor_cleanup=args.supervisor_cleanup, quality_profile=args.quality_profile,
                  synthetic_soak=args.long_synthetic_soak)
    stand.worker_recovery_gate=args.worker_recovery_gate
    try:
        if args.quality_profile:
            stand.begin_phase('live_quality_'+args.quality_phase, 360)
        elif args.bounded_profile:
            stand.begin_phase('synthetic_network_gate',240)
        stand.setup()
        stand.raw_network_gate()
        if args.live:
            case_ids = (QUALITY_PHASE_CASES[args.quality_phase] if args.quality_profile else args.case)
            stand.run_live(args.variant,args.corpus,args.captures,case_ids)
        elif args.bootstrap:
            if args.long_synthetic_soak:
                stand.begin_phase('synthetic_soak_startup',180)
                stand.start_runtime('C')
                stand.begin_phase('synthetic_stability_soak',660)
                soak_result = run_synthetic_stability(stand)
                overload_result = stand.synthetic_overload_gate()
                stand.write_manifest('synthetic_stability_gate_passed')
            elif args.bounded_profile:
                stand.begin_phase('synthetic_browser_fixture',240)
                stand.start_runtime()
                stand.controlled_browser_gate()
            else:
                stand.start_runtime()
                stand.controlled_browser_gate()
        print(json.dumps({'state':'synthetic_stability_completed' if args.long_synthetic_soak
                          else 'live_batch_completed' if args.live else 'controlled_gate_passed'
                          if args.bootstrap else 'raw_network_gate_passed',
                          'run_id':stand.prefix}))
    except Exception as exc:
        code=str(exc) if isinstance(exc,StandError) else type(exc).__name__
        if (args.quality_profile or (args.bootstrap and args.bounded_profile and not args.live)) and hasattr(stand, 'worker'):
            collect_native_diagnostic_receipt(stand)
        (stand.run_dir/'failure.json').write_text(json.dumps({'phase':stand.phase,'error':code})+'\n')
        raise
    finally:
        if args.bounded_profile and hasattr(signal, 'setitimer'):
            signal.setitimer(signal.ITIMER_REAL, 0)
        stand.cleanup()


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        code = str(exc) if isinstance(exc,StandError) and re.fullmatch(r'[a-z0-9_]{1,80}',str(exc)) else 'stand_failed'
        print(json.dumps({'error':type(exc).__name__,'code':code}),file=sys.stderr)
        raise SystemExit(1)
