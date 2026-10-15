"""Lifecycle regressions using real nodriver connection and listener objects."""

import asyncio
import gc
import json
import sys
import weakref
from unittest.mock import AsyncMock

import pytest
from nodriver import cdp
from nodriver.core import util
from nodriver.core.browser import Browser as NodriverBrowser
from nodriver.core.config import Config
from nodriver.core.connection import Connection, Transaction
from nodriver.core.tab import Tab

import news_aggregator.core.browser_pool as bp
from news_aggregator.core.exceptions import BrowserUnavailableError


class MemorySocket:
    """Exercise the library listener without opening a network connection."""

    def __init__(self):
        self.close_code = None
        self.responses = asyncio.Queue()
        self.requests = []
        self.closed = 0

    async def send(self, payload):
        request = json.loads(payload)
        self.requests.append(request)
        if request["method"] == "Target.closeTarget":
            await self.responses.put(json.dumps({
                "id": request["id"], "result": {"success": True},
            }))

    async def recv(self):
        return await self.responses.get()

    async def close(self):
        self.closed += 1
        self.close_code = 1000

    async def wait_closed(self):
        return None


def target_info(target_id):
    return cdp.target.TargetInfo(
        target_id=cdp.target.TargetID(target_id), type_="page", title="test",
        url="https://example.test", attached=True, can_access_opener=False,
    )


def real_browser():
    return NodriverBrowser(Config(
        host="127.0.0.1", port=9222, browser_executable_path=sys.executable,
    ))


def connect(connection):
    socket = MemorySocket()
    connection.socket = socket
    connection.session_id = cdp.target.SessionID("test-session")
    connection._listener_task = asyncio.create_task(connection._listener())
    return socket


@pytest.fixture(autouse=True)
def reset_pool(monkeypatch):
    for name, value in {
        "_browser": None, "_browser_generation": 0,
        "_lock": asyncio.Lock(), "_cycle_lock": asyncio.Lock(),
        "_active_cycle_leases": set(), "_active_tab_leases": set(),
        "_close_when_idle": False, "_detached_operation_tasks": set(),
        "_tab_semaphore": asyncio.Semaphore(1), "_last_failure_at": None,
        "_last_failure_error": None, "_consecutive_browser_failures": 0,
        "_restart_marker_latched": False,
    }.items():
        monkeypatch.setattr(bp, name, value)


@pytest.mark.asyncio
async def test_closed_target_also_releases_real_tab_listener_and_history():
    browser = real_browser()
    browser_socket = connect(browser)
    tab = Tab(target=target_info("owned"), parent=browser)
    tab_socket = connect(tab)
    listener = tab._listener_task
    tx = Transaction({"id": 100, "method": "DOM.getDocument"})
    tx.result = {"large-dom": bytearray(512_000)}
    tab._transactions.append(tx)
    tab._tx_by_id[100] = tx
    browser._targets.append(tab)
    browser.get = AsyncMock(return_value=tab)
    bp._browser = browser

    try:
        async with bp.browser_tab("https://example.test"):
            pass
        assert listener.done(), "Target.closeTarget left the tab websocket listener alive"
        assert tab_socket.closed == 1
        assert not tab._transactions
        assert not tab._tx_by_id
        assert tab not in browser.targets
        assert browser_socket.closed == 0
    finally:
        await Connection.aclose(tab)
        await Connection.aclose(browser)


@pytest.mark.asyncio
async def test_browser_close_removes_nodriver_strong_registry_root():
    browser = real_browser()
    connect(browser)
    util.get_registered_instances().add(browser)
    bp._browser = browser
    try:
        await bp.close_browser()
        assert browser not in util.get_registered_instances()
    finally:
        util.get_registered_instances().discard(browser)
        await Connection.aclose(browser)


@pytest.mark.asyncio
async def test_parent_closes_removed_owned_tab_but_never_foreign_targets():
    browser = real_browser()
    parent_socket = connect(browser)
    owned = bp._OwnedTab(target=target_info("owned"), parent=browser)
    owned_socket = connect(owned)
    bp._register_tab(browser, owned)
    discovered = Tab(target=target_info("foreign-target"), parent=browser)
    discovered_socket = connect(discovered)
    browser._targets[:] = [discovered]
    foreign_browser = real_browser()
    foreign_socket = connect(foreign_browser)
    util.get_registered_instances().update([browser, foreign_browser])
    try:
        await asyncio.gather(
            bp._close_browser_instance(browser), bp._close_browser_instance(browser),
        )
        assert owned_socket.closed == 1
        assert discovered_socket.closed == 1
        assert parent_socket.closed == 1
        assert foreign_socket.closed == 0
        assert foreign_browser in util.get_registered_instances()
        assert browser not in util.get_registered_instances()
        assert [request["params"]["targetId"] for request in parent_socket.requests] == ["owned"]
        assert not discovered_socket.requests
        assert not browser._browser_pool_tabs
        assert not browser.targets
    finally:
        util.get_registered_instances().discard(foreign_browser)
        await Connection.aclose(foreign_browser)


@pytest.mark.asyncio
async def test_partial_attach_failure_closes_created_target_and_transport(monkeypatch):
    browser = bp.Browser(Config(
        host="127.0.0.1", port=9222, browser_executable_path=sys.executable,
    ))
    parent_socket = connect(browser)
    browser.send = AsyncMock(return_value=cdp.target.TargetID("partial"))
    attached = []

    async def fail_attach(tab):
        attached.append((tab, connect(tab)))
        raise RuntimeError("attach failed after websocket opened")

    monkeypatch.setattr(bp._OwnedTab, "attach", fail_attach)
    try:
        with pytest.raises(RuntimeError, match="attach failed"):
            await browser.get("https://example.test", new_tab=True)
        tab, socket = attached[0]
        assert socket.closed == 1
        assert tab._listener_task is None
        assert not browser._browser_pool_tabs
        assert not browser.targets
        assert parent_socket.requests[0]["params"]["targetId"] == "partial"
        assert parent_socket.closed == 0
    finally:
        await bp._close_browser_instance(browser)


@pytest.mark.asyncio
async def test_pending_real_cdp_command_is_settled_before_history_release():
    browser = real_browser()
    connect(browser)
    tab = bp._OwnedTab(target=target_info("owned"), parent=browser)
    connect(tab)
    bp._register_tab(browser, tab)
    command = asyncio.create_task(tab.send(cdp.runtime.evaluate("pending forever")))
    await asyncio.sleep(0)
    assert tab._mapper
    assert tab._transactions
    await bp._close_browser_instance(browser)
    assert command.done()
    assert command.cancelled()
    assert not tab._mapper
    assert not tab._tx_by_id
    assert not tab._transactions
    assert not tab.handlers
    assert tab._listener_task is None


@pytest.mark.asyncio
async def test_closed_owned_tab_cannot_reopen_a_websocket():
    browser = real_browser()
    connect(browser)
    tab = bp._OwnedTab(target=target_info("owned"), parent=browser)
    connect(tab)
    bp._register_tab(browser, tab)
    await bp._close_browser_instance(browser)
    with pytest.raises(ConnectionError, match="closing"):
        await tab.send(cdp.runtime.evaluate("late evaluation"))
    with pytest.raises(ConnectionError, match="closing"):
        await tab.aopen()
    assert tab.socket is None


@pytest.mark.asyncio
async def test_context_cleanup_survives_repeated_cancellation_and_releases_lease():
    browser = real_browser()
    connect(browser)
    tab = bp._OwnedTab(target=target_info("owned"), parent=browser)
    socket = connect(tab)
    close_started = asyncio.Event()
    finish_close = asyncio.Event()
    original_close = socket.close

    async def delayed_close():
        close_started.set()
        await finish_close.wait()
        await original_close()

    socket.close = delayed_close
    browser.get = AsyncMock(return_value=tab)
    bp._browser = browser

    async def use_tab():
        async with bp.browser_tab("https://example.test"):
            pass

    task = asyncio.create_task(use_tab())
    await close_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    finish_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert socket.closed == 1
    assert not bp._active_tab_leases
    assert bp._tab_semaphore._value == 1
    assert not browser._browser_pool_tabs
    await bp.close_browser()


@pytest.mark.asyncio
async def test_cancelled_partial_attach_is_cleaned_before_context_lease_release(monkeypatch):
    browser = bp.Browser(Config(
        host="127.0.0.1", port=9222, browser_executable_path=sys.executable,
    ))
    parent_socket = connect(browser)
    browser.send = AsyncMock(return_value=cdp.target.TargetID("partial"))
    started = asyncio.Event()
    sockets = []

    async def hang_attach(tab):
        sockets.append(connect(tab))
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(bp._OwnedTab, "attach", hang_attach)
    bp._browser = browser

    async def use_tab():
        async with bp.browser_tab("https://example.test"):
            pytest.fail("The tab must never be yielded")

    task = asyncio.create_task(use_tab())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sockets[0].closed == 1
    assert not browser._browser_pool_tabs
    assert not bp._active_tab_leases
    assert bp._tab_semaphore._value == 1
    assert parent_socket.requests[0]["params"]["targetId"] == "partial"
    await bp.close_browser()


@pytest.mark.asyncio
async def test_timeout_close_defers_history_clear_until_suppressing_task_settles(monkeypatch):
    browser = real_browser()
    socket = connect(browser)
    tx = Transaction({"id": 7, "method": "test"})
    tx.result = bytearray(128_000)
    browser._transactions.append(tx)
    finish = asyncio.Event()
    cancelled = asyncio.Event()

    async def stubborn_close():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await finish.wait()
        await socket.close()

    browser.aclose = stubborn_close
    monkeypatch.setattr(bp, "_CLOSE_TIMEOUT_SECONDS", 0.01)
    await bp._close_browser_instance(browser)
    await cancelled.wait()
    assert browser._transactions, "A still-running close must not race history release"
    assert bp._detached_operation_tasks
    finish.set()
    for _ in range(10):
        await asyncio.sleep(0)
        if not bp._detached_operation_tasks:
            break
    assert not browser._transactions
    assert not bp._detached_operation_tasks
    assert socket.closed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("local", [False, True])
async def test_failed_start_removes_partially_registered_browser(monkeypatch, local):
    from news_aggregator.config import settings

    browser = real_browser()
    async def fail_start():
        util.get_registered_instances().add(browser)
        raise RuntimeError("partially registered startup")
    browser.start = fail_start
    monkeypatch.setattr(settings, "browser_ws_endpoint", None if local else "ws://chrome:9222")
    monkeypatch.setattr(bp, "Browser", lambda *args, **kwargs: browser)
    monkeypatch.setattr(bp, "Config", lambda **kwargs: None)
    monkeypatch.setattr(bp, "_resolve_host", AsyncMock(return_value="127.0.0.1"))
    monkeypatch.setattr(bp, "_check_remote_port", AsyncMock())
    monkeypatch.setattr(bp.os.path, "exists", lambda path: False)
    with pytest.raises(BrowserUnavailableError, match="partially registered"):
        await bp.get_browser()
    assert browser not in util.get_registered_instances()
    assert bp._browser is None


@pytest.mark.asyncio
async def test_closed_graphs_are_collectible_without_nodriver_registry_root():
    async def cycle():
        browser = real_browser()
        connect(browser)
        tab = bp._OwnedTab(target=target_info("owned"), parent=browser)
        connect(tab)
        bp._register_tab(browser, tab)
        util.get_registered_instances().add(browser)
        browser_reference = weakref.ref(browser)
        tab_reference = weakref.ref(tab)
        await bp._close_browser_instance(browser)
        return browser_reference, tab_reference

    browser_reference, tab_reference = await cycle()
    await asyncio.sleep(0)
    gc.collect()
    assert browser_reference() is None
    assert tab_reference() is None


@pytest.mark.asyncio
async def test_parent_close_cancels_partial_websocket_open(monkeypatch):
    import nodriver.core.connection as connection_module

    browser = real_browser()
    connect(browser)
    tab = bp._OwnedTab(target=target_info("owned"), parent=browser)
    tab.websocket_url = "ws://127.0.0.1:9222/devtools/browser/test"
    bp._register_tab(browser, tab)
    started = asyncio.Event()

    async def delayed_connect(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(connection_module.websockets, "connect", delayed_connect)
    opening = asyncio.create_task(tab.aopen())
    await started.wait()
    await bp._close_browser_instance(browser)
    assert opening.cancelled()
    assert tab.socket is None
    assert not tab._browser_pool_operations


@pytest.mark.asyncio
async def test_late_cancel_suppressing_websocket_open_closes_its_transport(monkeypatch):
    import nodriver.core.connection as connection_module

    browser = real_browser()
    connect(browser)
    tab = bp._OwnedTab(target=target_info("owned"), parent=browser)
    tab.websocket_url = "ws://127.0.0.1:9222/devtools/browser/test"
    bp._register_tab(browser, tab)
    started = asyncio.Event()
    finish = asyncio.Event()
    late_socket = MemorySocket()

    async def stubborn_connect(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await finish.wait()
        return late_socket

    monkeypatch.setattr(connection_module.websockets, "connect", stubborn_connect)
    monkeypatch.setattr(bp, "_CLOSE_TIMEOUT_SECONDS", 0.01)
    opening = asyncio.create_task(tab.aopen())
    await started.wait()
    await bp._close_browser_instance(browser)
    assert not opening.done()
    finish.set()
    with pytest.raises(ConnectionError, match="websocket was opening"):
        await opening
    await asyncio.sleep(0)
    assert late_socket.closed == 1
    assert tab.socket is None
    assert tab._listener_task is None
    assert not bp._detached_operation_tasks
