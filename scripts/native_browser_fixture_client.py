"""Standalone bounded browser fixture HTTP client; no News application imports."""


import asyncio
from collections import deque
from contextlib import asynccontextmanager
from threading import Lock
from urllib.parse import urlsplit
from weakref import WeakKeyDictionary


MAX_WAITERS = 8
MAX_WAIT_SECONDS = 120.0
MAX_BUSY_RETRIES = 8
MAX_ENDPOINTS_PER_LOOP = 16
_PRIVATE_ROUTES = (
    '/v1/extract', '/v1/telegram/fetch', '/v1/page-monitor/extract', '/v1/sources/collect',
)


class AdmissionWaitTimeout(Exception):
    """The request could not enter the local endpoint queue in time."""


class _State:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.active = False
        self.waiters = deque()
        self.references = 0


_registry_lock = Lock()
_registry = WeakKeyDictionary()


def _retain_state(loop, endpoint):
    # Registry access is synchronous and protected because event loops may run
    # on different threads. The endpoint contains no credentials or payload.
    with _registry_lock:
        endpoints = _registry.setdefault(loop, {})
        state = endpoints.get(endpoint)
        if state is None:
            if len(endpoints) >= MAX_ENDPOINTS_PER_LOOP:
                raise AdmissionWaitTimeout
            state = endpoints[endpoint] = _State()
        state.references += 1
        return state


def _release_reference(loop, endpoint, state):
    with _registry_lock:
        endpoints = _registry.get(loop)
        if endpoints is None:
            return
        state.references -= 1
        if state.references == 0 and not state.active and not state.waiters:
            if endpoints.get(endpoint) is state:
                del endpoints[endpoint]
            if not endpoints:
                _registry.pop(loop, None)


async def _acquire(state, loop, deadline):
    async with state.lock:
        if not state.active and not state.waiters:
            state.active = True
            return
        if len(state.waiters) >= MAX_WAITERS:
            raise AdmissionWaitTimeout
        future = loop.create_future()
        state.waiters.append(future)

    timeout = min(MAX_WAIT_SECONDS, deadline - loop.time())
    if timeout <= 0:
        await _remove_waiter(state, loop, future)
        raise AdmissionWaitTimeout
    try:
        await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
    except TimeoutError:
        transferred = await _remove_waiter(state, loop, future)
        if transferred:
            await _release(state, loop)
        raise AdmissionWaitTimeout from None
    except asyncio.CancelledError:
        transferred = await _remove_waiter(state, loop, future)
        if transferred:
            await _release(state, loop)
        raise


async def _remove_waiter(state, loop, future):
    async with state.lock:
        try:
            state.waiters.remove(future)
            return False
        except ValueError:
            # The permit may have been handed off just as timeout/cancellation
            # arrived. The canceled waiter must return that permit.
            return future.done() and not future.cancelled()


async def _release(state, loop):
    async with state.lock:
        while state.waiters:
            future = state.waiters.popleft()
            if future.done():
                continue
            state.active = True
            future.set_result(None)
            return
        state.active = False


@asynccontextmanager
async def endpoint_admission(endpoint, timeout_seconds, phase=None):
    """Acquire one FIFO permit, bounded by the caller deadline and queue cap."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout_seconds))
    parsed = urlsplit(endpoint)
    path = parsed.path.rstrip('/')
    for route in _PRIVATE_ROUTES:
        if path.endswith(route):
            path = path[:-len(route)]
            break
    endpoint_key = f'{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}'
    state = _retain_state(loop, endpoint_key)
    acquired = False
    try:
        if phase is not None:
            phase['value'] = 'waiting'
        await _acquire(state, loop, deadline)
        acquired = True
        if phase is not None:
            phase['value'] = 'active'
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise AdmissionWaitTimeout
        yield min(remaining, MAX_WAIT_SECONDS)
    finally:
        if acquired:
            await _release(state, loop)
        if phase is not None and acquired:
            phase['value'] = 'idle'
        _release_reference(loop, endpoint_key, state)


def _make_error(error_type, code, retryable):
    return error_type(code, retryable)


async def run_admitted(endpoint, timeout_seconds, operation, error_type, *,
                       admission_timeout_seconds=None, phase=None):
    """Run one request under admission; retry only explicit bounded 429 busy."""
    loop = asyncio.get_running_loop()
    deadline_budget = (timeout_seconds if admission_timeout_seconds is None
                       else admission_timeout_seconds)
    deadline = loop.time() + float(deadline_budget)
    busy_retries = 0
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise _make_error(error_type, 'busy', True)
        admitted = False
        try:
            async with endpoint_admission(endpoint, remaining, phase=phase) as admitted_budget:
                admitted = True
                remaining = deadline - loop.time()
                if remaining <= 0 or admitted_budget <= 0:
                    raise AdmissionWaitTimeout
                request_budget = min(float(timeout_seconds), remaining)
                async with asyncio.timeout(request_budget):
                    return await operation(request_budget)
        except asyncio.CancelledError:
            # External cancellation always remains cancellation. The phase is
            # retained as 'waiting' while queue acquisition unwinds so an
            # owning deadline can classify only its own timeout.
            raise
        except AdmissionWaitTimeout:
            raise _make_error(error_type, 'busy', True) from None
        except TimeoutError:
            raise _make_error(error_type, 'deadline_exceeded', True) from None
        except error_type as exc:
            delay = getattr(exc, 'retry_after_seconds', None)
            if exc.code != 'busy' or delay is None or busy_retries >= MAX_BUSY_RETRIES:
                raise
            remaining = deadline - loop.time()
            if delay > remaining:
                raise
            busy_retries += 1
            # The endpoint context and response/session have closed before this
            # sleep, so the permit is available to the next FIFO waiter.
            if phase is not None:
                phase['value'] = 'waiting'
            await asyncio.sleep(delay)
            if phase is not None:
                phase['value'] = 'idle'



import json
from uuid import uuid4
from urllib.parse import urlsplit

import aiohttp



class ExtractionServiceError(Exception):
    """A stable classification safe to log without response content or credentials."""

    def __init__(self, code, retryable=False, retry_after_seconds=None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds

    @property
    def terminal(self):
        """Whether article processing is durably terminal; this differs from wire retryability."""
        return not self.retryable and self.code in {
            'resource_limit', 'invalid_input', 'unsupported_input', 'request_too_large', 'invalid_request',
            'public_url_denied', 'upstream_auth_required', 'native_browser_policy_denied',
            'native_browser_unsupported', 'native_browser_challenge', 'native_browser_no_content',
            'native_browser_http_rejected'}


class ExtractionServiceClient:
    def __init__(self, endpoint, token, *, max_bytes=5 * 1024 * 1024,
                 max_chars=100000, session_factory=aiohttp.ClientSession):
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError('invalid_extractor_endpoint')
        if not token:
            raise ValueError('missing_extractor_token')
        self.endpoint = endpoint.rstrip('/') + '/v1/extract'
        self._token = token
        self.max_bytes = min(max_bytes, 5 * 1024 * 1024)
        self.max_chars = min(max_chars, 100000)
        self._session_factory = session_factory

    async def extract(self, url, *, timeout_seconds, learned_pattern=None, html=None,
                      content=None, title=None, strategy='auto', source_type='article', use_browser=False,
                      admission_timeout_seconds=None, admission_phase=None):
        if strategy != "browser_rendering" or source_type != "article" or use_browser is not True or html is not None or content is not None or title is not None or learned_pattern is not None:
            raise ExtractionServiceError("invalid_fixture_request")
        if timeout_seconds <= 0:
            raise ExtractionServiceError('deadline_exceeded', True)
        request_id = str(uuid4())
        async def request(attempt_timeout):
            payload = {'request_id': request_id, 'url': url, 'strategy': strategy, 'source_type': source_type,
                       'budget_ms': max(1, min(30000, int(attempt_timeout * 1000))),
                       'max_bytes': self.max_bytes, 'max_chars': self.max_chars}
            for name, value in (('html', html), ('content', content), ('title', title), ('use_browser', use_browser)):
                if value is not None:
                    payload[name] = value
            if learned_pattern:
                payload.update(learned_selector=learned_pattern.get('selector') or '',
                               learned_method=learned_pattern.get('method') or '')
            body = json.dumps(payload).encode('utf-8')
            if len(body) > self.max_bytes:
                raise ExtractionServiceError('request_too_large')
            # The session timeout starts after local admission, and the owning
            # response/session are closed before the permit is released.
            async with self._session_factory(timeout=aiohttp.ClientTimeout(total=attempt_timeout),
                    trust_env=False, auto_decompress=False) as session:
                async with session.post(self.endpoint, data=body, allow_redirects=False,
                        headers={'Authorization': 'Bearer ' + self._token,
                                 'Content-Type': 'application/json',
                                 'Accept-Encoding': 'identity'}) as response:
                    if response.status != 200:
                        codes = {400: 'invalid_request', 401: 'unauthorized', 403: 'unauthorized',
                                 413: 'request_too_large', 429: 'busy', 503: 'unavailable'}
                        retry_after = response.headers.get('Retry-After', '')
                        retry_after = min(int(retry_after), 86400) if retry_after.isdigit() else None
                        raise ExtractionServiceError(codes.get(response.status, 'http_error'),
                            response.status in {429, 502, 503, 504}, retry_after)
                    if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                        raise ExtractionServiceError('invalid_payload')
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(16384):
                        if len(data) + len(chunk) > self.max_bytes:
                            raise ExtractionServiceError('response_too_large')
                        data.extend(chunk)
                    try:
                        result = json.loads(data)
                    except (ValueError, UnicodeError, RecursionError):
                        raise ExtractionServiceError('invalid_payload') from None
                    return self._validate(result, request_id, url)

        try:
            return await run_admitted(self.endpoint, timeout_seconds, request, ExtractionServiceError,
                                      admission_timeout_seconds=admission_timeout_seconds,
                                      phase=admission_phase)
        except aiohttp.ClientError:
            raise ExtractionServiceError('transport_error', True) from None

    def _validate(self, payload, request_id, url):
        if (not isinstance(payload, dict) or payload.get('contract_version') != '1'
                or payload.get('request_id') != request_id
                or payload.get('original_url') != url
                or payload.get('outcome') not in {'success', 'rejected', 'failed', 'needs_selector_study'}):
            raise ExtractionServiceError('invalid_payload')
        result = payload.get('result')
        if not isinstance(result, dict):
            raise ExtractionServiceError('invalid_payload')
        error = payload.get('error')
        if error is not None and (not isinstance(error, dict)
                or not isinstance(error.get('code'), str)
                or not isinstance(error.get('retryable'), bool)):
            raise ExtractionServiceError('invalid_payload')
        if error is not None and error.get('retry_after_seconds') is not None:
            retry_after = error['retry_after_seconds']
            if type(retry_after) is not int or not 0 <= retry_after <= 86400:
                raise ExtractionServiceError('invalid_payload')
        for field in ('content', 'title', 'publication_date', 'author', 'description', 'image_url',
                      'method', 'method_used', 'selector_used'):
            if result.get(field) is not None and not isinstance(result[field], str):
                raise ExtractionServiceError('invalid_payload')
        content = result.get('content') or ''
        if len(content) > self.max_chars:
            raise ExtractionServiceError('response_too_large')
        if payload['outcome'] == 'success' and not content.strip():
            raise ExtractionServiceError('invalid_payload')
        media = result.get('media') or []
        if not isinstance(media, list) or any(not isinstance(m, dict)
                or not isinstance(m.get('type'), str) or not isinstance(m.get('url'), str) for m in media):
            raise ExtractionServiceError('invalid_payload')
        observations = payload.get('observations') or []
        if not isinstance(observations, list) or len(observations) > 64:
            raise ExtractionServiceError('invalid_payload')
        for observation in observations:
            if (not isinstance(observation, dict) or not isinstance(observation.get('selector'), str)
                    or not isinstance(observation.get('method'), str)
                    or not isinstance(observation.get('success'), bool)):
                raise ExtractionServiceError('invalid_payload')
        return payload
