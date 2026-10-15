"""Shared browser pool — connects to a remote Chrome instance via CDP (nodriver)."""

import asyncio
import inspect
import json
import logging
import math
import os
import socket
import sys
import time
import urllib.parse
import urllib.request
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Awaitable, Optional, TypeVar

import nodriver as uc
from nodriver import cdp
from nodriver.core import util as nodriver_util
from nodriver.core.browser import Browser as NodriverBrowser, HTTPApi
from nodriver.core.config import Config
from nodriver.core.connection import Connection
from nodriver.core.tab import Tab

try:
    from websockets.exceptions import ConnectionClosed as WebSocketConnectionClosed
except ImportError:  # nodriver currently depends on websockets, but keep import optional.
    _WEBSOCKET_TRANSPORT_ERRORS: tuple[type[BaseException], ...] = ()
else:
    _WEBSOCKET_TRANSPORT_ERRORS = (WebSocketConnectionClosed,)

from .exceptions import (
    BrowserBusyError,
    BrowserOperationTimeoutError,
    BrowserUnavailableError,
)

logger = logging.getLogger(__name__)

_browser: Optional[NodriverBrowser] = None
_browser_generation = 0
_lock = asyncio.Lock()
_cycle_lock = asyncio.Lock()
_active_cycle_leases: set[object] = set()
_active_tab_leases: set[object] = set()
_close_when_idle = False
_detached_operation_tasks: set[asyncio.Task] = set()
_last_failure_at: Optional[float] = None
_last_failure_error: Optional[str] = None
_consecutive_browser_failures = 0
_restart_marker_latched = False
_managed_connections: weakref.WeakValueDictionary = weakref.WeakValueDictionary()

_CONNECT_TIMEOUT_SECONDS = 15.0
_CLOSE_TIMEOUT_SECONDS = 5.0
_RECONNECT_COOLDOWN_SECONDS = 60.0

# Serializes ALL browser tab usage across the entire app.
# Remote Chrome has limited RAM and each attached tab owns a WebSocket.
# Opening multiple tabs concurrently increases memory use and can cause commands
# (including tab.close()) to hang indefinitely.
_tab_semaphore = asyncio.Semaphore(1)

T = TypeVar("T")


class _OwnedConnection:
    """Prevent retired pool connections from reopening in late CDP operations."""

    async def aopen(self):
        if self.__dict__.get("_browser_pool_closing", False):
            raise ConnectionError("Browser pool connection is closing")
        tasks = self.__dict__.setdefault("_browser_pool_operations", set())
        task = asyncio.current_task()
        outermost = task not in tasks
        tasks.add(task)
        try:
            await super().aopen()
            if self.__dict__.get("_browser_pool_closing", False):
                # A third-party connect may suppress cancellation and finish late.
                await _await_shielded(_close_transport(self, exclude_tasks={task}))
                raise ConnectionError("Browser closed while its websocket was opening")
        finally:
            if outermost:
                tasks.discard(task)

    async def send(self, command, *args, **kwargs):
        if self.__dict__.get("_browser_pool_closing", False):
            command.close()
            raise ConnectionError("Browser pool connection is closing")
        tasks = self.__dict__.setdefault("_browser_pool_operations", set())
        task = asyncio.current_task()
        outermost = task not in tasks
        tasks.add(task)
        try:
            return await super().send(command, *args, **kwargs)
        finally:
            if outermost:
                tasks.discard(task)


class _OwnedTab(_OwnedConnection, Tab):
    pass


class Browser(_OwnedConnection, NodriverBrowser):
    """Keep ownership before attaching, including tabs missing from targets."""

    async def get(self, url="chrome://welcome", new_tab=False, new_window=False):
        if not (new_tab or new_window):
            return await super().get(url, new_tab=new_tab, new_window=new_window)
        target_id = await self.send(cdp.target.create_target(
            url, new_window=new_window, enable_begin_frame_control=True,
        ))
        tab = _OwnedTab(target=target_id, parent=self, auto_attach=False)
        _register_tab(self, tab, target_id)
        self._targets.append(tab)
        try:
            if self.__dict__.get("_browser_pool_closing", False):
                raise ConnectionError("Browser closed during tab creation")
            await tab.attach()
        except BaseException:
            await _await_shielded(_close_tab(tab))
            raise
        return tab


def _register_tab(browser, tab, target_id=None) -> None:
    state = browser.__dict__
    state.setdefault("_browser_pool_tabs", {})[id(tab)] = tab
    tab._browser_pool_browser = browser
    tab._browser_pool_generation = _browser_generation
    if target_id is None and isinstance(tab, Connection):
        target = tab.target
        target_id = target.target_id if isinstance(target, cdp.target.TargetInfo) else target
    tab._browser_pool_target_id = target_id
    _managed_connections[id(browser)] = browser
    _managed_connections[id(tab)] = tab


def get_browser_resource_stats() -> dict[str, int]:
    """Return passive process-local counters without keeping connections alive."""
    connections = list(_managed_connections.values())
    return {
        "registered_browsers": sum(
            browser in nodriver_util.get_registered_instances()
            for browser in connections if isinstance(browser, NodriverBrowser)
        ),
        "owned_tabs": sum(len(c.__dict__.get("_browser_pool_tabs", {})) for c in connections),
        "open_transports": sum(
            isinstance(c, Connection) and c.socket is not None
            and c.socket.close_code is None for c in connections
        ),
        "listener_tasks": sum(
            isinstance(c.__dict__.get("_listener_task"), asyncio.Task)
            and not c._listener_task.done() for c in connections
        ),
        "pending_commands": sum(len(c.__dict__.get("_mapper", {})) for c in connections),
        "retained_transactions": sum(len(c.__dict__.get("_transactions", [])) for c in connections),
        "detached_operation_tasks": len(_detached_operation_tasks),
        "active_cycle_leases": len(_active_cycle_leases),
        "active_tab_leases": len(_active_tab_leases),
    }


async def _httpapi_request_uppercase_method(self, endpoint, method: str = "get", data: dict = None):
    """Patch nodriver HTTPApi for strict CDP proxies such as CloakBrowser.

    nodriver 0.38 sends lowercase HTTP methods ("get"/"post"). Chrome accepts
    that, but aiohttp-based CDP multiplexers reject it before routing.
    """
    url = urllib.parse.urljoin(
        self.api,
        f"json/{endpoint}" if endpoint else "/json",
    )
    if data and method.lower() == "get":
        raise ValueError("get requests cannot contain data")

    request = urllib.request.Request(url)
    request.method = method.upper()
    request.data = json.dumps(data).encode("utf-8") if data else None

    def _open_and_read():
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read()

    body = await asyncio.get_running_loop().run_in_executor(
        None,
        _open_and_read,
    )
    return json.loads(body)


HTTPApi._request = _httpapi_request_uppercase_method


@dataclass(frozen=True)
class BrowserPoolStatus:
    connected: bool
    last_error: Optional[str]
    cooldown_remaining_seconds: float


def _is_browser_connected(browser: Optional[Browser]) -> bool:
    if browser is None:
        return False

    socket_connection = getattr(browser, "socket", None)
    if socket_connection is not None:
        return getattr(socket_connection, "close_code", None) is None

    # Compatibility with older nodriver objects and lightweight test doubles.
    connection = getattr(browser, "connection", None)
    if connection is not None:
        return not bool(getattr(connection, "closed", True))

    return False


def _cooldown_remaining(now: Optional[float] = None) -> float:
    if _last_failure_at is None:
        return 0.0
    elapsed = (time.monotonic() if now is None else now) - _last_failure_at
    return max(0.0, _RECONNECT_COOLDOWN_SECONDS - elapsed)


def get_browser_pool_status() -> BrowserPoolStatus:
    """Return passive pool state without opening a new browser connection."""
    return BrowserPoolStatus(
        connected=_is_browser_connected(_browser),
        last_error=_last_failure_error,
        cooldown_remaining_seconds=_cooldown_remaining(),
    )


def _write_restart_marker(path: str) -> bool:
    """Create an idempotent data-only restart request without replacing it."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        return True
    except OSError as exc:
        logger.error("Could not create browser restart marker %s: %s", path, exc)
        return False

    marker_complete = False
    try:
        payload = b"browser-recovery-request\n"
        written = os.write(descriptor, payload)
        if written != len(payload):
            raise OSError(f"short browser restart marker write: {written} bytes")
        os.close(descriptor)
        descriptor = None
        marker_complete = True
    except OSError as exc:
        logger.error("Could not finish browser restart marker %s: %s", path, exc)
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as exc:
                logger.error("Could not close browser restart marker %s: %s", path, exc)
        if not marker_complete:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                logger.error("Could not remove partial browser restart marker %s: %s", path, exc)
    return marker_complete


def _session_matches(
    expected_browser: Optional[Browser], expected_generation: Optional[int]
) -> bool:
    return (
        (expected_browser is None or _browser is expected_browser)
        and (expected_generation is None or _browser_generation == expected_generation)
    )


def record_browser_failure(
    error: BaseException,
    *,
    expected_browser: Optional[Browser] = None,
    expected_generation: Optional[int] = None,
) -> bool:
    """Record one browser-only failure and request recovery at the threshold."""
    global _last_failure_at, _last_failure_error
    global _consecutive_browser_failures, _restart_marker_latched
    if not _session_matches(expected_browser, expected_generation):
        return False
    _last_failure_at = time.monotonic()
    detail = str(error).strip()
    _last_failure_error = f"{type(error).__name__}: {detail}" if detail else type(error).__name__
    _consecutive_browser_failures += 1

    from ..config import settings

    threshold = settings.browser_failure_threshold
    marker_path = settings.browser_restart_marker_path
    if (
        marker_path
        and _consecutive_browser_failures >= threshold
        and not _restart_marker_latched
    ):
        _restart_marker_latched = _write_restart_marker(marker_path)
    return True


def record_browser_success(
    *,
    expected_browser: Optional[Browser] = None,
    expected_generation: Optional[int] = None,
) -> bool:
    """Reset browser failure state after a confirmed CDP success."""
    global _last_failure_at, _last_failure_error
    global _consecutive_browser_failures, _restart_marker_latched
    if not _session_matches(expected_browser, expected_generation):
        return False
    _last_failure_at = None
    _last_failure_error = None
    _consecutive_browser_failures = 0
    _restart_marker_latched = False
    return True


def _is_transport_failure(error: BaseException) -> bool:
    explicit_types = (
        ConnectionError,
        OSError,
        EOFError,
        *_WEBSOCKET_TRANSPORT_ERRORS,
    )
    if isinstance(error, explicit_types):
        return True
    error_type = type(error).__name__.lower()
    return any(
        token in error_type
        for token in (
            "connectionclosed",
            "connectionreset",
            "websocket",
            "transport",
            "brokenpipe",
            "endoffile",
        )
    )


def _bounded_timeout_seconds(
    operation: Awaitable[object], timeout_seconds: Optional[float]
) -> tuple[float, bool]:
    """Validate a caller override and cap it at the configured finite budget."""
    from ..config import settings

    configured_max = float(settings.browser_operation_timeout_seconds)
    try:
        candidate = configured_max if timeout_seconds is None else float(timeout_seconds)
    except (TypeError, ValueError) as exc:
        close = getattr(operation, "close", None)
        if callable(close):
            close()
        raise ValueError(
            "Browser operation timeout must be finite positive seconds"
        ) from exc
    if not math.isfinite(candidate) or candidate <= 0:
        close = getattr(operation, "close", None)
        if callable(close):
            close()
        raise ValueError("Browser operation timeout must be finite positive seconds")
    return min(candidate, configured_max), candidate >= configured_max


async def _resolve_host(host: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, socket.gethostbyname, host)


async def _check_remote_port(host: str, port: int) -> None:
    _reader = None
    writer = None
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=3.0,
        )
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()


async def _await_shielded(operation):
    """Finish shared cleanup despite repeated cancellation of its caller."""
    cleanup_task = asyncio.ensure_future(operation)
    cancelled = False
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            cancelled = True
    result = await cleanup_task
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _bounded_cleanup(operation, timeout):
    task = asyncio.ensure_future(operation)
    done, _ = await asyncio.wait({task}, timeout=max(0.0, timeout))
    if task not in done:
        task.cancel()
        _track_detached_task(task)
        raise TimeoutError("Browser cleanup exceeded its deadline")
    return task.result()


def _clear_connection_state(connection) -> None:
    """Release response graphs only after their writers have stopped."""
    for name in (
        "_transactions", "_tx_by_id", "_mapper", "handlers", "_targets",
        "_browser_pool_operations",
    ):
        container = connection.__dict__.get(name)
        if isinstance(container, (dict, list, set)):
            container.clear()
    connection.session_id = None
    if isinstance(connection, Connection):
        connection.socket = None
        connection._listener_task = None


async def _close_transport(connection, *, exclude_tasks=()) -> None:
    state = connection.__dict__
    state["_browser_pool_closing"] = True
    children = [
        child for child in state.get("_targets", ())
        if isinstance(child, Connection) and child.parent is connection
    ]
    if children:
        await asyncio.gather(*(_close_transport(child) for child in children))
    deadline = asyncio.get_running_loop().time() + _CLOSE_TIMEOUT_SECONDS
    operations = set(state.get("_browser_pool_operations", ()))
    operations.discard(asyncio.current_task())
    operations.difference_update(exclude_tasks)
    listener = state.get("_listener_task")
    if isinstance(listener, asyncio.Task):
        operations.add(listener)
    for future in list(state.get("_mapper", {}).values()):
        if not future.done():
            future.cancel()
    for task in operations:
        if not task.done():
            task.cancel()

    socket_connection = state.get("socket")
    close_task = None
    aclose = getattr(connection, "aclose", None)
    if inspect.iscoroutinefunction(aclose):
        close_task = asyncio.create_task(aclose())
        try:
            await _bounded_cleanup(close_task, _CLOSE_TIMEOUT_SECONDS)
        except (Exception, asyncio.CancelledError) as exc:
            logger.warning("Browser transport cleanup failed (%s): %s", type(exc).__name__, exc)
            # Abort only this websocket when a graceful close is stuck.
            transport = getattr(socket_connection, "transport", None)
            if transport is not None:
                try:
                    transport.abort()
                except Exception:
                    logger.warning("Could not abort the closing websocket", exc_info=True)
        operations.add(close_task)

    pending = {task for task in operations if not task.done()}
    if pending:
        _, pending = await asyncio.wait(
            pending, timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
        )
    if not pending:
        _clear_connection_state(connection)
    else:
        # Cancellation-suppressing third-party tasks must not race a history clear.
        def clear_when_settled(_):
            if all(task.done() for task in pending):
                _clear_connection_state(connection)
        for task in pending:
            _track_detached_task(task)
            task.add_done_callback(clear_when_settled)


async def _close_remote_target(tab) -> None:
    if not isinstance(tab, Connection):
        await tab.close()
        return
    target_id = tab.__dict__.get("_browser_pool_target_id")
    if not target_id:
        return
    browser = tab.__dict__.get("_browser_pool_browser")
    for connection in (browser, tab):
        if isinstance(connection, Connection) and _is_browser_connected(connection):
            # Use an existing websocket only; send() normally auto-attaches.
            await Connection.send(connection, cdp.target.close_target(target_id), _attach=True)
            return


async def _close_tab_impl(tab):
    tab.__dict__["_browser_pool_closing"] = True
    error = None
    try:
        from ..config import settings
        await _bounded_cleanup(_close_remote_target(tab), settings.browser_tab_close_timeout_seconds)
    except (Exception, asyncio.CancelledError) as exc:
        error = exc.with_traceback(None)
        error.__context__ = None
        error.__cause__ = None
    finally:
        await _close_transport(tab)
        browser = tab.__dict__.get("_browser_pool_browser")
        if browser is not None:
            browser.__dict__.get("_browser_pool_tabs", {}).pop(id(tab), None)
            targets = browser.__dict__.get("_targets", [])
            targets[:] = [target for target in targets if target is not tab]
    return error


async def _close_tab(tab):
    task = tab.__dict__.get("_browser_pool_tab_cleanup")
    if task is None:
        task = asyncio.create_task(_close_tab_impl(tab))
        tab.__dict__["_browser_pool_tab_cleanup"] = task
    return await asyncio.shield(task)


async def _close_browser_impl(browser) -> None:
    browser.__dict__["_browser_pool_closing"] = True
    for task in tuple(browser.__dict__.get("_browser_pool_operations", ())):
        if task is not asyncio.current_task() and not task.done():
            task.cancel()
    owned_tabs = list(browser.__dict__.get("_browser_pool_tabs", {}).values())
    try:
        await asyncio.gather(*(_close_tab(tab) for tab in owned_tabs))
        # Discovered targets own only local transports; never close their targets.
        await _close_transport(browser)
    finally:
        nodriver_util.get_registered_instances().discard(browser)
    process = getattr(browser, "_process", None)
    if process is not None:
        try:
            process.terminate()
            await _bounded_cleanup(process.wait(), _CLOSE_TIMEOUT_SECONDS)
        except TimeoutError:
            process.kill()
            await _bounded_cleanup(process.wait(), _CLOSE_TIMEOUT_SECONDS)
        except ProcessLookupError:
            pass
        except Exception as exc:
            logger.warning("Local browser process cleanup failed: %s", exc)


async def _close_browser_instance(browser: Optional[Browser]) -> None:
    if browser is None:
        return
    _managed_connections[id(browser)] = browser
    task = browser.__dict__.get("_browser_pool_cleanup")
    if task is None:
        task = asyncio.create_task(_close_browser_impl(browser))
        browser.__dict__["_browser_pool_cleanup"] = task
    await _await_shielded(task)


async def _await_cleanup(browser: Optional[Browser]) -> None:
    await _close_browser_instance(browser)


async def _start_browser(candidate) -> None:
    """Retain the candidate through partial startup and enforce a hard deadline."""
    _managed_connections[id(candidate)] = candidate
    task = asyncio.create_task(candidate.start())
    operations = candidate.__dict__.setdefault("_browser_pool_operations", set())
    operations.add(task)
    task.add_done_callback(operations.discard)
    try:
        done, _ = await asyncio.wait({task}, timeout=_CONNECT_TIMEOUT_SECONDS)
        if task not in done:
            raise TimeoutError("Browser startup exceeded its deadline")
        await task
    finally:
        if not task.done():
            task.cancel()
            _track_detached_task(task)


async def invalidate_browser(
    error: BaseException,
    *,
    expected_browser: Optional[Browser] = None,
    expected_generation: Optional[int] = None,
) -> None:
    """Detach the shared session, record the failure, and await bounded cleanup."""
    global _browser

    async with _lock:
        matches_expected = _session_matches(expected_browser, expected_generation)
        browser = _browser if matches_expected else expected_browser
        if matches_expected:
            record_browser_failure(
                error,
                expected_browser=expected_browser,
                expected_generation=expected_generation,
            )
            _browser = None
    await _await_cleanup(browser)


def _track_detached_task(task: asyncio.Task) -> None:
    """Keep timed-out operations alive until they finish and consume outcomes."""
    _detached_operation_tasks.add(task)

    def _consume_result(done_task: asyncio.Task) -> None:
        _detached_operation_tasks.discard(done_task)
        try:
            done_task.result()
        except (asyncio.CancelledError, Exception):
            pass

    task.add_done_callback(_consume_result)


async def run_browser_operation(
    operation: Awaitable[T],
    operation_name: str,
    *,
    timeout_seconds: Optional[float] = None,
    confirm_success: bool = True,
    browser_session: Optional[object] = None,
    invalidate_on_timeout: bool = False,
) -> T:
    """Run one CDP operation with a finite deadline and session invalidation."""
    timeout_seconds, uses_pool_deadline = _bounded_timeout_seconds(
        operation, timeout_seconds
    )

    if browser_session is None:
        expected_browser = _browser
        expected_generation = _browser_generation
    else:
        session_state = getattr(browser_session, "__dict__", {})
        expected_browser = session_state.get("_browser_pool_browser", browser_session)
        expected_generation = session_state.get(
            "_browser_pool_generation", _browser_generation
        )
    operation_task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait({operation_task}, timeout=timeout_seconds)
        if operation_task not in done:
            operation_task.cancel()
            _track_detached_task(operation_task)
            if invalidate_on_timeout or uses_pool_deadline:
                failure = BrowserUnavailableError(
                    f"Browser {operation_name} timed out after {timeout_seconds:.1f}s"
                )
                await invalidate_browser(
                    failure,
                    expected_browser=expected_browser,
                    expected_generation=expected_generation,
                )
                raise failure
            raise BrowserOperationTimeoutError(
                f"Browser {operation_name} timed out after {timeout_seconds:.1f}s"
            )
        try:
            result = operation_task.result()
        except asyncio.TimeoutError as exc:
            raise BrowserOperationTimeoutError(
                f"Browser {operation_name} reported a caller-scoped timeout"
            ) from exc
    except asyncio.CancelledError:
        if not operation_task.done():
            operation_task.cancel()
            _track_detached_task(operation_task)
        raise
    except Exception as exc:
        if _is_transport_failure(exc):
            await invalidate_browser(
                exc,
                expected_browser=expected_browser,
                expected_generation=expected_generation,
            )
            raise BrowserUnavailableError(
                f"Browser {operation_name} failed: {type(exc).__name__}: {exc}"
            ) from exc
        raise

    if confirm_success:
        record_browser_success(
            expected_browser=expected_browser,
            expected_generation=expected_generation,
        )
    return result


def _parse_endpoint(endpoint: str) -> tuple[str, int]:
    normalized = endpoint if "://" in endpoint else f"ws://{endpoint}"
    parsed = urllib.parse.urlsplit(normalized)
    if not parsed.hostname:
        raise ValueError(f"Invalid browser endpoint: {endpoint}")
    try:
        port = parsed.port or 9222
    except ValueError as exc:
        raise ValueError(f"Invalid browser endpoint port: {endpoint}") from exc
    return parsed.hostname, port


async def get_browser() -> uc.Browser:
    """Get or create a shared browser connection via CDP.

    Connects to a remote Chrome/Chromium (e.g. Alpine Chrome) using the
    Chrome DevTools Protocol through nodriver, eliminating the need for
    a Playwright server (Node.js) container.
    """
    global _browser, _browser_generation

    if _is_browser_connected(_browser):
        return _browser

    async with _lock:
        if _is_browser_connected(_browser):
            return _browser

        stale_browser = _browser
        _browser = None
        if stale_browser is not None:
            logger.warning("Shared browser connection lost; closing it before reconnect")
            await _close_browser_instance(stale_browser)

        remaining = _cooldown_remaining()
        if remaining > 0:
            raise BrowserUnavailableError(
                f"Browser reconnect cooldown active for {remaining:.1f}s after {_last_failure_error}"
            )

        from ..config import settings
        cdp_endpoint = settings.browser_ws_endpoint
        
        # Log the actual endpoint being used to help with debugging
        logger.info(f"  Browser connection config: endpoint='{cdp_endpoint}'")

        if cdp_endpoint:
            try:
                host, port = _parse_endpoint(cdp_endpoint)
            except ValueError as exc:
                record_browser_failure(exc)
                raise BrowserUnavailableError(str(exc)) from exc

            logger.info(f"  Connecting to remote Chrome via CDP at {host}:{port}...")
            
            # Resolve hostname to IP address to bypass Chrome's Host header restrictions.
            # Chrome DevTools rejects requests to /json/version if the Host header is a non-localhost hostname.
            try:
                ip_addr = await _resolve_host(host)
                logger.info(f"  Resolved hostname '{host}' to IP '{ip_addr}'")
                actual_host = ip_addr
            except Exception as e:
                logger.warning(f"  Failed to resolve hostname '{host}': {e}")
                actual_host = host
            
            # Pre-flight check: can we even reach the port?
            try:
                await _check_remote_port(actual_host, port)
                logger.info(f"  ✅ Network check: {actual_host}:{port} is reachable")
            except Exception as e:
                error_msg = f"Network check failed for {actual_host}:{port} ({host}): {e}"
                logger.error(f"  ❌ {error_msg}")
                record_browser_failure(e)
                raise BrowserUnavailableError(error_msg) from e

            candidate = Browser(
                Config(
                    host=actual_host,
                    port=port,
                    browser_executable_path=sys.executable,
                )
            )
            try:
                await _start_browser(candidate)
                if not _is_browser_connected(candidate):
                    raise ConnectionError("nodriver started without an open CDP socket")
                _browser = candidate
                _browser_generation += 1
                record_browser_success(
                    expected_browser=candidate,
                    expected_generation=_browser_generation,
                )
                logger.info("  Connected to remote Chrome via CDP")
            except asyncio.CancelledError:
                await _await_cleanup(candidate)
                raise
            except Exception as e:
                await _close_browser_instance(candidate)
                record_browser_failure(e)
                logger.error(
                    "Failed to connect to remote Chrome at %s:%s (%s): %s",
                    actual_host,
                    port,
                    type(e).__name__,
                    e,
                )
                raise BrowserUnavailableError(
                    f"Could not connect to remote browser at {actual_host}:{port}: "
                    f"{type(e).__name__}: {e}"
                ) from e
        else:
            # In Docker environment, BROWSER_WS_ENDPOINT must be set
            if os.path.exists('/.dockerenv'):
                error = RuntimeError("BROWSER_WS_ENDPOINT must be set when running in Docker")
                logger.error("  ❌ %s", error)
                record_browser_failure(error)
                raise BrowserUnavailableError(str(error)) from error
                
            logger.info("  Launching shared local Chromium...")
            candidate = None
            try:
                candidate = Browser(Config(
                    headless=True,
                    browser_args=["--no-sandbox", "--disable-setuid-sandbox"],
                ))
                await _start_browser(candidate)
                if not _is_browser_connected(candidate):
                    raise ConnectionError("nodriver started without an open CDP socket")
                _browser = candidate
                _browser_generation += 1
                record_browser_success(
                    expected_browser=_browser,
                    expected_generation=_browser_generation,
                )
                logger.info("  Shared local browser launched")
            except asyncio.CancelledError:
                await _await_cleanup(candidate)
                raise
            except Exception as exc:
                await _close_browser_instance(candidate)
                _browser = None
                record_browser_failure(exc)
                raise BrowserUnavailableError(
                    f"Could not launch local browser: {type(exc).__name__}: {exc}"
                ) from exc

        return _browser


async def close_browser():
    """Close the shared browser (call on app shutdown only)."""
    global _browser

    async with _lock:
        browser = _browser
        _browser = None
    await _await_cleanup(browser)


async def acquire_browser_cycle() -> object:
    """Register one processing cycle that may use the shared CDP session."""
    lease = object()
    async with _cycle_lock:
        _active_cycle_leases.add(lease)
    return lease


async def release_browser_cycle(lease: object) -> None:
    """Release a cycle lease and close CDP only after the last cycle exits."""
    global _close_when_idle
    async with _cycle_lock:
        if lease not in _active_cycle_leases:
            return
        _active_cycle_leases.remove(lease)
        if _active_cycle_leases:
            return
        if _active_tab_leases:
            _close_when_idle = True
            return
        _close_when_idle = False
        await close_browser()


async def _acquire_browser_tab_usage() -> object:
    lease = object()
    async with _cycle_lock:
        _active_tab_leases.add(lease)
    return lease


async def _release_browser_tab_usage(lease: object) -> None:
    global _close_when_idle
    async with _cycle_lock:
        if lease not in _active_tab_leases:
            return
        _active_tab_leases.remove(lease)
        if _close_when_idle and not _active_cycle_leases and not _active_tab_leases:
            _close_when_idle = False
            await close_browser()


@asynccontextmanager
async def browser_tab(url: str):
    """Open a browser tab with exclusive access to Chrome.

    Usage::

        async with browser_tab("https://example.com") as tab:
            html = await run_browser_operation(
                tab.get_content(), "content retrieval", browser_session=tab
            )

    This guarantees:
    - Only one tab is open at a time (via _tab_semaphore)
    - The tab is always closed, even on error
    - tab.close() has a timeout so it never hangs
    """
    tab = None
    creation_task = None
    tab_usage_lease = await _acquire_browser_tab_usage()
    acquired = False
    from ..config import settings

    try:
        try:
            await asyncio.wait_for(
                _tab_semaphore.acquire(),
                timeout=settings.browser_tab_acquire_timeout_seconds,
            )
            acquired = True
        except asyncio.TimeoutError as exc:
            failure = BrowserBusyError(
                "Browser tab acquisition timed out after "
                f"{settings.browser_tab_acquire_timeout_seconds:.1f}s"
            )
            raise failure from exc

        browser = await get_browser()
        creation_task = asyncio.create_task(browser.get(url, new_tab=True))
        tab = await run_browser_operation(
            creation_task,
            "create tab",
            timeout_seconds=settings.browser_tab_create_timeout_seconds,
            browser_session=browser,
            invalidate_on_timeout=True,
        )
        _register_tab(browser, tab)
        yield tab
    finally:
        async def finish():
            nonlocal tab
            try:
                if creation_task is not None and tab is None:
                    # Cancellation may arrive after get() returned but before assignment.
                    if not creation_task.done():
                        creation_task.cancel()
                        await asyncio.wait({creation_task}, timeout=_CLOSE_TIMEOUT_SECONDS)
                    if creation_task.done() and not creation_task.cancelled():
                        try:
                            tab = creation_task.result()
                        except Exception:
                            pass
                        else:
                            _register_tab(browser, tab)
                if tab is None:
                    return
                error = await _close_tab(tab)
                if error is not None:
                    logger.warning("Browser tab cleanup failed for %s (%s): %s", url[:60], type(error).__name__, error)
                    if isinstance(error, TimeoutError) or _is_transport_failure(error):
                        await invalidate_browser(
                            error,
                            expected_browser=tab.__dict__.get("_browser_pool_browser"),
                            expected_generation=tab.__dict__.get("_browser_pool_generation"),
                        )
            finally:
                if acquired:
                    _tab_semaphore.release()
                await _release_browser_tab_usage(tab_usage_lease)

        await _await_shielded(finish())
