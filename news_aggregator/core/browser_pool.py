"""Shared browser pool — connects to a remote Chrome instance via CDP (nodriver)."""

import asyncio
import json
import logging
import math
import os
import socket
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Awaitable, Optional, TypeVar

import nodriver as uc
from nodriver.core.browser import Browser, HTTPApi
from nodriver.core.config import Config

try:
    from websockets.exceptions import ConnectionClosed as WebSocketConnectionClosed
except ImportError:  # nodriver currently depends on websockets, but keep import optional.
    _WEBSOCKET_TRANSPORT_ERRORS: tuple[type[BaseException], ...] = ()
else:
    _WEBSOCKET_TRANSPORT_ERRORS = (WebSocketConnectionClosed,)

from .exceptions import BrowserBusyError, BrowserUnavailableError

logger = logging.getLogger(__name__)

_browser: Optional[Browser] = None
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

_CONNECT_TIMEOUT_SECONDS = 15.0
_CLOSE_TIMEOUT_SECONDS = 5.0
_RECONNECT_COOLDOWN_SECONDS = 60.0

# Serializes ALL browser tab usage across the entire app.
# Remote Chrome has limited RAM (512 MB) and a single WebSocket connection.
# Opening multiple tabs concurrently overwhelms both, causing CDP commands
# (including tab.close()) to hang indefinitely.
_tab_semaphore = asyncio.Semaphore(1)

T = TypeVar("T")


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
    if isinstance(
        error,
        (ConnectionError, OSError, EOFError) + _WEBSOCKET_TRANSPORT_ERRORS,
    ):
        return True
    detail = f"{type(error).__name__}: {error}".lower()
    return any(
        token in detail
        for token in (
            "cdp",
            "connection closed",
            "connection reset",
            "websocket",
            "transport",
            "broken pipe",
        )
    )


def _bounded_timeout_seconds(
    operation: Awaitable[object], timeout_seconds: Optional[float]
) -> float:
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
    return min(candidate, configured_max)


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


async def _close_browser_instance(browser: Optional[Browser]) -> None:
    """Await connection cleanup and terminate only a locally launched process."""
    if browser is None:
        return

    close_error = None
    aclose = getattr(browser, "aclose", None)
    if callable(aclose):
        try:
            close_task = asyncio.create_task(aclose())
            done, _ = await asyncio.wait({close_task}, timeout=_CLOSE_TIMEOUT_SECONDS)
            if close_task not in done:
                close_task.cancel()
                _track_detached_task(close_task)
                raise TimeoutError("browser aclose exceeded its deadline")
            await close_task
        except Exception as exc:  # Cleanup must not hide the primary failure.
            close_error = exc

    if getattr(browser, "_process", None) is not None:
        try:
            browser.stop()
        except Exception as exc:
            close_error = close_error or exc

    if close_error is not None:
        logger.warning(
            "Browser cleanup failed (%s): %s",
            type(close_error).__name__,
            close_error,
        )


async def _await_cleanup(browser: Optional[Browser]) -> None:
    """Finish connection cleanup despite repeated cancellation requests."""
    cleanup_task = asyncio.create_task(_close_browser_instance(browser))
    cancelled = False
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            cancelled = True
    await cleanup_task
    if cancelled:
        raise asyncio.CancelledError


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
) -> T:
    """Run one CDP operation with a finite deadline and session invalidation."""
    timeout_seconds = _bounded_timeout_seconds(operation, timeout_seconds)

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
            raise asyncio.TimeoutError
        result = operation_task.result()
    except asyncio.TimeoutError as exc:
        failure = BrowserUnavailableError(
            f"Browser {operation_name} timed out after {timeout_seconds:.1f}s"
        )
        await invalidate_browser(
            failure,
            expected_browser=expected_browser,
            expected_generation=expected_generation,
        )
        raise failure from exc
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
                await asyncio.wait_for(
                    candidate.start(),
                    timeout=_CONNECT_TIMEOUT_SECONDS,
                )
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
            try:
                _browser = await asyncio.wait_for(
                    uc.start(
                        headless=True,
                        browser_args=["--no-sandbox", "--disable-setuid-sandbox"],
                    ),
                    timeout=_CONNECT_TIMEOUT_SECONDS,
                )
                _browser_generation += 1
                record_browser_success(
                    expected_browser=_browser,
                    expected_generation=_browser_generation,
                )
                logger.info("  Shared local browser launched")
            except Exception as exc:
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


from contextlib import asynccontextmanager

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
        tab = await run_browser_operation(
            browser.get(url, new_tab=True),
            "create tab",
            timeout_seconds=settings.browser_tab_create_timeout_seconds,
            browser_session=browser,
        )
        setattr(tab, "_browser_pool_browser", browser)
        setattr(tab, "_browser_pool_generation", _browser_generation)
        yield tab
    finally:
        try:
            if tab:
                try:
                    await run_browser_operation(
                        tab.close(),
                        "close tab",
                        timeout_seconds=settings.browser_tab_close_timeout_seconds,
                        confirm_success=False,
                        browser_session=tab,
                    )
                except Exception as exc:
                    logger.warning(
                        "Browser tab cleanup failed for %s (%s): %s",
                        url[:60],
                        type(exc).__name__,
                        exc,
                    )
        finally:
            if acquired:
                _tab_semaphore.release()
            release_task = asyncio.create_task(
                _release_browser_tab_usage(tab_usage_lease)
            )
            cancelled = False
            while not release_task.done():
                try:
                    await asyncio.shield(release_task)
                except asyncio.CancelledError:
                    cancelled = True
            await release_task
            if cancelled:
                raise asyncio.CancelledError
