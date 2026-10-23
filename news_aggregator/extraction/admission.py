"""Bounded, event-loop-local admission for the private extractor endpoint."""

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
