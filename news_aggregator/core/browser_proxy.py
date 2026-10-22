"""Opt-in, per-tab proxy contexts for the shared nodriver browser."""

import asyncio
import ipaddress
import urllib.parse

from nodriver import cdp
from .exceptions import BrowserUnavailableError

_MAX_HANDLER_TASKS = 32
_ownership_unknown = False
_disposed_contexts = set()


def _safe_destination(url: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or parsed.username is not None or parsed.password is not None:
            return False
        if parsed.port not in (None, 443) or not parsed.hostname:
            return False
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            host = parsed.hostname.rstrip(".").lower()
            if host in {"localhost", "localhost.localdomain"} or host.endswith((".localhost", ".local", ".internal")):
                return False
            return True  # The gateway performs DNS address classification.
        return address.is_global
    except ValueError:
        return False


def _proxy_origin(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("invalid proxy origin") from None
    if (
        not parsed.hostname or parsed.username is not None or parsed.password is not None
        or parsed.path not in ("", "/") or parsed.query or parsed.fragment
        or "?" in url or "#" in url or (port is not None and not 1 <= port <= 65535)
    ):
        raise ValueError("invalid proxy origin")
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    port = port or (443 if parsed.scheme.lower() == "https" else 80)
    return f"{parsed.scheme.lower()}://{host}:{port}"


async def _late_dispose_context(browser, generation, task):
    from . import browser_pool
    try:
        context_id = task.result()
        await browser_pool._send_existing_connection(
            browser, generation, cdp.target.dispose_browser_context(context_id),
            browser_pool._CLOSE_TIMEOUT_SECONDS,
        )
    except BaseException:
        return


def _consume_late_target(task):
    try:
        task.result()
    except BaseException:
        pass


async def _allocate_context(browser, command):
    global _ownership_unknown
    if _ownership_unknown:
        raise BrowserUnavailableError("Browser proxy context ownership is unresolved")
    task = asyncio.create_task(browser.send(command))
    from . import browser_pool
    generation = browser_pool._browser_generation
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        _ownership_unknown = True
        browser_pool._track_detached_task(task)
        def cleanup(done):
            cleanup_task = asyncio.create_task(_late_dispose_context(browser, generation, done))
            browser_pool._track_detached_task(cleanup_task)
        task.add_done_callback(cleanup)
        raise
    except BaseException:
        # A transport failure can happen after Chrome accepted the allocation.
        _ownership_unknown = True
        raise BrowserUnavailableError("Browser proxy context allocation outcome is unknown") from None


async def _allocate_target(browser, command, context_id):
    global _ownership_unknown
    task = asyncio.create_task(browser.send(command))
    from . import browser_pool
    generation = browser_pool._browser_generation
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        _ownership_unknown = True
        task.cancel()
        browser_pool._track_detached_task(task)
        def cleanup(done):
            done.add_done_callback(_consume_late_target)
        task.add_done_callback(cleanup)
        try:
            await browser_pool._await_shielded(browser_pool._send_existing_connection(
                browser, generation, cdp.target.dispose_browser_context(context_id),
                browser_pool._CLOSE_TIMEOUT_SECONDS,
            ))
        except BaseException:
            _ownership_unknown = True
        else:
            _ownership_unknown = False
            _disposed_contexts.add(context_id)
        raise


async def create_proxy_tab(browser, initial_url: str):
    """Allocate one isolated context and install guards before navigation."""
    global _ownership_unknown
    from ..config import settings

    if not settings.browser_proxy_enabled:
        raise RuntimeError("Proxy context requested while disabled")
    if initial_url != "about:blank" and not _safe_destination(initial_url):
        raise BrowserUnavailableError("Browser destination blocked by proxy policy")
    context_id = await _allocate_context(browser, cdp.target.create_browser_context(
        proxy_server=settings.browser_proxy_url,
        proxy_bypass_list="<-loopback>",
        dispose_on_detach=True,
    ))
    if not context_id:
        _ownership_unknown = True
        raise BrowserUnavailableError("Browser proxy context allocation returned no ownership receipt")
    from . import browser_pool
    context_generation = browser_pool._browser_generation
    tab = None
    try:
        target_id = await _allocate_target(browser, cdp.target.create_target(
            "about:blank", browser_context_id=context_id,
            enable_begin_frame_control=True,
        ), context_id)
        if not target_id:
            raise BrowserUnavailableError("Browser proxy target allocation returned no ownership receipt")
        from .browser_pool import _OwnedTab, _register_tab
        tab = _OwnedTab(target=target_id, parent=browser, auto_attach=False)
        _register_tab(browser, tab, target_id)
        browser._targets.append(tab)
        tab._browser_pool_context_id = context_id
        tab._browser_pool_proxy_error = None
        tab._browser_pool_proxy_tasks = set()
        tab._browser_pool_proxy_auth_used = set()
        tab._browser_pool_proxy_handlers = []
        await tab.attach()

        def on_request(event):
            async def handle():
                if not _safe_destination(event.request.url):
                    _set_proxy_error(tab, "Browser destination blocked by proxy policy")
                    await tab.send(cdp.fetch.fail_request(event.request_id, cdp.network.ErrorReason.BLOCKED_BY_CLIENT))
                    return
                await tab.send(cdp.fetch.continue_request(event.request_id))
            # nodriver schedules async handlers without a bound. Keep this callback sync.
            tasks = tab._browser_pool_proxy_tasks
            if len(tasks) >= _MAX_HANDLER_TASKS:
                _set_proxy_error(tab, "Proxy request handling capacity exceeded")
                return
            task = asyncio.create_task(handle())
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            task.add_done_callback(lambda done: _handler_failed(tab, done))

        def on_auth(event):
            async def handle():
                challenge = event.auth_challenge
                source = getattr(challenge.source, "value", challenge.source)
                try:
                    origin_ok = (
                        source == "Proxy"
                        and _proxy_origin(challenge.origin) == _proxy_origin(settings.browser_proxy_url)
                    )
                except (TypeError, ValueError):
                    origin_ok = False
                key = str(event.request_id)
                if (
                    not origin_ok or key in tab._browser_pool_proxy_auth_used
                    or len(tab._browser_pool_proxy_auth_used) >= 128
                ):
                    _set_proxy_error(tab, "Proxy authentication challenge rejected")
                    response = cdp.fetch.AuthChallengeResponse(response="CancelAuth")
                else:
                    tab._browser_pool_proxy_auth_used.add(key)
                    response = cdp.fetch.AuthChallengeResponse(
                        response="ProvideCredentials", username="extractor",
                        password=settings.browser_proxy_token.get_secret_value(),
                    )
                await tab.send(cdp.fetch.continue_with_auth(event.request_id, response))
            tasks = tab._browser_pool_proxy_tasks
            if len(tasks) >= _MAX_HANDLER_TASKS:
                _set_proxy_error(tab, "Proxy request handling capacity exceeded")
                return
            task = asyncio.create_task(handle())
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            task.add_done_callback(lambda done: _handler_failed(tab, done))

        tab.add_handler(cdp.fetch.RequestPaused, on_request)
        tab.add_handler(cdp.fetch.AuthRequired, on_auth)
        tab._browser_pool_proxy_handlers.extend(((cdp.fetch.RequestPaused, on_request), (cdp.fetch.AuthRequired, on_auth)))
        await tab.send(cdp.fetch.enable(handle_auth_requests=True))
        if initial_url != "about:blank":
            await tab.get(initial_url, new_tab=False)
            await check_proxy_error(tab)
        return tab
    except BaseException as exc:
        if tab is not None:
            from .browser_pool import _close_tab
            cleanup_error = await _close_tab(tab)
            if cleanup_error is not None:
                raise BrowserUnavailableError("Browser proxy context cleanup failed") from None
        elif context_id not in _disposed_contexts and not _ownership_unknown:
            try:
                await browser_pool._send_existing_connection(
                    browser, context_generation,
                    cdp.target.dispose_browser_context(context_id),
                    browser_pool._CLOSE_TIMEOUT_SECONDS,
                )
            except BaseException:
                _ownership_unknown = True
                raise BrowserUnavailableError("Browser proxy context cleanup failed") from None
        _disposed_contexts.discard(context_id)
        if isinstance(exc, asyncio.CancelledError):
            raise
        if isinstance(exc, BrowserUnavailableError):
            raise
        if isinstance(exc, Exception):
            raise BrowserUnavailableError("Browser proxy setup failed") from None
        raise


def _handler_failed(tab, task):
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        _set_proxy_error(tab, "Proxy request handling failed")


def _set_proxy_error(tab, message):
    if tab.__dict__.get("_browser_pool_proxy_error") is None:
        tab._browser_pool_proxy_error = message


def _raise_proxy_error(tab):
    error = tab.__dict__.get("_browser_pool_proxy_error")
    if error:
        raise BrowserUnavailableError(error)


async def check_proxy_error(tab):
    tasks = tuple(tab.__dict__.get("_browser_pool_proxy_tasks", ()))
    if tasks:
        _, pending = await asyncio.wait(tasks, timeout=2.0)
        if pending:
            _set_proxy_error(tab, "Proxy request handling did not complete")
    _raise_proxy_error(tab)


def remove_handlers(tab):
    for event_type, callback in tab.__dict__.get("_browser_pool_proxy_handlers", ()):
        handlers = tab.handlers.get(event_type, [])
        if callback in handlers:
            handlers.remove(callback)
