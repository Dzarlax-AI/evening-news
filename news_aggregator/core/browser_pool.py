"""Shared browser pool — connects to a remote Chrome instance via CDP (nodriver)."""

import asyncio
import json
import logging
import os
import urllib.parse
import urllib.request
from typing import Optional

import nodriver as uc
from nodriver.core.browser import Browser, HTTPApi

logger = logging.getLogger(__name__)

_browser: Optional[Browser] = None
_lock = asyncio.Lock()

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


async def get_browser() -> uc.Browser:
    """Get or create a shared browser connection via CDP.

    Connects to a remote Chrome/Chromium (e.g. Alpine Chrome) using the
    Chrome DevTools Protocol through nodriver, eliminating the need for
    a Playwright server (Node.js) container.
    """
    global _browser

    if _browser is not None:
        try:
            # Quick liveness check — list targets
            if _browser.connection and not _browser.connection.closed:
                return _browser
        except Exception:
            logger.warning("  Shared browser connection lost, reconnecting...")
            _browser = None

    async with _lock:
        # Double-check after acquiring lock
        if _browser is not None:
            try:
                if _browser.connection and not _browser.connection.closed:
                    return _browser
            except Exception:
                _browser = None

        from ..config import settings
        cdp_endpoint = settings.browser_ws_endpoint
        
        # Log the actual endpoint being used to help with debugging
        logger.info(f"  Browser connection config: endpoint='{cdp_endpoint}'")

        if cdp_endpoint:
            # Parse host:port from endpoint like "ws://chrome:9222" or "chrome:9222"
            endpoint = cdp_endpoint.replace("ws://", "").replace("http://", "").rstrip("/")
            host, _, port_str = endpoint.partition(":")
            
            try:
                # Handle cases like "host:9222/" or just "host"
                port = int(port_str.split("/")[0]) if port_str and port_str.split("/")[0] else 9222
            except ValueError:
                logger.warning(f"  Invalid port in endpoint '{cdp_endpoint}', falling back to 9222")
                port = 9222

            logger.info(f"  Connecting to remote Chrome via CDP at {host}:{port}...")
            
            # Resolve hostname to IP address to bypass Chrome's Host header restrictions.
            # Chrome DevTools rejects requests to /json/version if the Host header is a non-localhost hostname.
            import socket
            try:
                ip_addr = socket.gethostbyname(host)
                logger.info(f"  Resolved hostname '{host}' to IP '{ip_addr}'")
                actual_host = ip_addr
            except Exception as e:
                logger.warning(f"  Failed to resolve hostname '{host}': {e}")
                actual_host = host
            
            # Pre-flight check: can we even reach the port?
            try:
                # Direct TCP check to distinguish between network and nodriver issues
                conn = asyncio.open_connection(actual_host, port)
                reader, writer = await asyncio.wait_for(conn, timeout=3.0)
                writer.close()
                await writer.wait_closed()
                logger.info(f"  ✅ Network check: {actual_host}:{port} is reachable")
            except Exception as e:
                error_msg = f"Network check failed for {actual_host}:{port} ({host}): {e}"
                logger.error(f"  ❌ {error_msg}")
                if os.path.exists('/.dockerenv'):
                    raise RuntimeError(error_msg)

            try:
                import sys
                from nodriver.core.browser import Browser
                # Pass the resolved IP address to nodriver so it can successfully
                # request /json/version without being blocked by Chrome's Host header policy.
                # ALSO pass browser_executable_path=sys.executable to bypass the hardcoded local binary check in Config.
                _browser = await Browser.create(
                    host=actual_host,
                    port=port,
                    browser_executable_path=sys.executable
                )
                logger.info("  Connected to remote Chrome via CDP")
            except Exception as e:
                logger.error(f"  Failed to connect to remote Chrome at {actual_host}:{port}: {e}")
                # In Docker, we shouldn't try to launch a local browser if remote fails
                if os.path.exists('/.dockerenv'):
                    raise RuntimeError(f"Could not connect to remote browser at {actual_host}:{port}: {e}")
                
                logger.info("  Falling back to local browser launch (not in Docker)...")
                _browser = await uc.start(
                    headless=True,
                    browser_args=["--no-sandbox", "--disable-setuid-sandbox"],
                )
        else:
            # In Docker environment, BROWSER_WS_ENDPOINT must be set
            if os.path.exists('/.dockerenv'):
                logger.error("  ❌ BROWSER_WS_ENDPOINT is not set, but running in Docker!")
                raise RuntimeError("BROWSER_WS_ENDPOINT must be set when running in Docker.")
                
            logger.info("  Launching shared local Chromium...")
            _browser = await uc.start(
                headless=True,
                browser_args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            logger.info("  Shared local browser launched")

        return _browser


async def close_browser():
    """Close the shared browser (call on app shutdown only)."""
    global _browser

    if _browser:
        try:
            _browser.stop()
        except Exception:
            pass
        _browser = None


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
    browser = await get_browser()
    tab = None
    await _tab_semaphore.acquire()
    try:
        tab = await browser.get(url, new_tab=True)
        yield tab
    finally:
        if tab:
            try:
                await asyncio.wait_for(tab.close(), timeout=5)
            except Exception:
                logger.warning(f"  ⚠️ tab.close() timed out for {url[:60]}")
        _tab_semaphore.release()
