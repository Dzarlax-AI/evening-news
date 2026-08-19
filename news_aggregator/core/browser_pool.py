"""Shared browser pool — connects to a remote Chrome instance via CDP (nodriver)."""

import asyncio
import json
import logging
import os
import socket
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

import nodriver as uc
from nodriver.core.browser import Browser, HTTPApi
from nodriver.core.config import Config

from .exceptions import BrowserUnavailableError

logger = logging.getLogger(__name__)

_browser: Optional[Browser] = None
_lock = asyncio.Lock()
_last_failure_at: Optional[float] = None
_last_failure_error: Optional[str] = None

_CONNECT_TIMEOUT_SECONDS = 15.0
_CLOSE_TIMEOUT_SECONDS = 5.0
_RECONNECT_COOLDOWN_SECONDS = 60.0

# Serializes ALL browser tab usage across the entire app.
# Remote Chrome has limited RAM (512 MB) and a single WebSocket connection.
# Opening multiple tabs concurrently overwhelms both, causing CDP commands
# (including tab.close()) to hang indefinitely.
_tab_semaphore = asyncio.Semaphore(1)


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


def _record_failure(error: BaseException) -> None:
    global _last_failure_at, _last_failure_error
    _last_failure_at = time.monotonic()
    detail = str(error).strip()
    _last_failure_error = f"{type(error).__name__}: {detail}" if detail else type(error).__name__


def _record_success() -> None:
    global _last_failure_at, _last_failure_error
    _last_failure_at = None
    _last_failure_error = None


async def _resolve_host(host: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, socket.gethostbyname, host)


async def _check_remote_port(host: str, port: int) -> None:
    reader = None
    writer = None
    try:
        reader, writer = await asyncio.wait_for(
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
            await asyncio.wait_for(aclose(), timeout=_CLOSE_TIMEOUT_SECONDS)
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
    global _browser

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
                _record_failure(exc)
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
                _record_failure(e)
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
                _record_success()
                logger.info("  Connected to remote Chrome via CDP")
            except asyncio.CancelledError:
                cleanup_task = asyncio.create_task(_close_browser_instance(candidate))
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError:
                    await cleanup_task
                raise
            except Exception as e:
                await _close_browser_instance(candidate)
                _record_failure(e)
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
                _record_failure(error)
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
                _record_success()
                logger.info("  Shared local browser launched")
            except Exception as exc:
                _browser = None
                _record_failure(exc)
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
    await _close_browser_instance(browser)


from contextlib import asynccontextmanager

@asynccontextmanager
async def browser_tab(url: str):
    """Open a browser tab with exclusive access to Chrome.

    Usage::

        async with browser_tab("https://example.com") as tab:
            html = await tab.get_content()

    This guarantees:
    - Only one tab is open at a time (via _tab_semaphore)
    - The tab is always closed, even on error
    - tab.close() has a timeout so it never hangs
    """
    tab = None
    await _tab_semaphore.acquire()
    try:
        browser = await get_browser()
        tab = await browser.get(url, new_tab=True)
        yield tab
    finally:
        if tab:
            try:
                await asyncio.wait_for(tab.close(), timeout=5)
            except Exception:
                logger.warning(f"  ⚠️ tab.close() timed out for {url[:60]}")
        _tab_semaphore.release()
