"""Focused tests for opt-in browser proxy policy and credentials."""

import asyncio
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nodriver import cdp

import news_aggregator.core.browser_pool as browser_pool
from news_aggregator.core import browser_proxy
from news_aggregator.core.exceptions import BrowserUnavailableError


@pytest.mark.parametrize(
    "url, allowed",
    [
        ("https://example.com/article", True),
        ("https://example.com:443/article", True),
        ("about:blank", False),
        ("http://example.com/", False),
        ("https://example.com:8443/", False),
        ("https://user@example.com/", False),
        ("https://127.0.0.1/", False),
        ("https://192.168.1.2/", False),
        ("https://[::1]/", False),
        ("about:blank?x", False),
    ],
)
def test_proxy_destination_policy(url, allowed):
    assert browser_proxy._safe_destination(url) is allowed


class FakeTab:
    def __init__(self, *args, **kwargs):
        self.handlers = {}
        self.sent = []
        self.operations = []

    async def send(self, command):
        self.sent.append(next(command))
        self.operations.append("command")

    def add_handler(self, event_type, callback):
        self.handlers.setdefault(event_type, []).append(callback)

    def remove_handler(self, event_type, callback):
        handlers = self.handlers.get(event_type, [])
        if callback in handlers:
            handlers.remove(callback)

    async def attach(self):
        return None

    async def get(self, *args, **kwargs):
        self.operations.append("navigate")
        return None


@pytest.mark.asyncio
async def test_proxy_tab_context_is_configured_before_initial_navigation():
    browser = MagicMock()
    browser._targets = []
    browser.send = AsyncMock(side_effect=["ctx-1", "target-1"])
    settings = SimpleNamespace(
        browser_proxy_enabled=True,
        browser_proxy_url="http://gateway:8080",
        browser_proxy_token=MagicMock(get_secret_value=lambda: "x" * 24),
    )
    with (
        patch("news_aggregator.config.settings", settings),
        patch("news_aggregator.core.browser_pool._OwnedTab", FakeTab),
        patch("news_aggregator.core.browser_pool._register_tab"),
    ):
        tab = await browser_proxy.create_proxy_tab(browser, "https://example.com/")

    create_context = next(browser.send.call_args_list[0].args[0])
    assert create_context["params"]["proxyServer"] == "http://gateway:8080"
    assert create_context["params"]["proxyBypassList"] == "<-loopback>"
    assert create_context["params"]["disposeOnDetach"] is True
    assert tab.sent[0]["method"] == "Fetch.enable"
    assert tab.operations == ["command", "navigate"]


@pytest.mark.asyncio
async def test_proxy_auth_only_provides_secret_to_exact_proxy_challenge():
    browser = MagicMock()
    browser._targets = []
    browser.send = AsyncMock(side_effect=["ctx-1", "target-1"])
    secret = "proxy-secret-not-for-origin-requests"
    settings = SimpleNamespace(
        browser_proxy_enabled=True,
        browser_proxy_url="http://gateway:8080",
        browser_proxy_token=MagicMock(get_secret_value=lambda: secret),
    )
    with (
        patch("news_aggregator.config.settings", settings),
        patch("news_aggregator.core.browser_pool._OwnedTab", FakeTab),
        patch("news_aggregator.core.browser_pool._register_tab"),
    ):
        tab = await browser_proxy.create_proxy_tab(browser, "about:blank")

    handler = tab.handlers[cdp.fetch.AuthRequired][0]
    event = SimpleNamespace(
        request_id=cdp.fetch.RequestId("request-1"),
        auth_challenge=SimpleNamespace(source="Proxy", origin="http://gateway:8080"),
    )
    handler(event)
    await asyncio.gather(*tuple(tab._browser_pool_proxy_tasks))
    auth_command = tab.sent[-1]
    assert auth_command["method"] == "Fetch.continueWithAuth"
    auth_response = auth_command["params"]["authChallengeResponse"]
    assert auth_response == {"response": "ProvideCredentials", "username": "extractor", "password": secret}

    handler(event)
    await asyncio.gather(*tuple(tab._browser_pool_proxy_tasks))
    repeated = tab.sent[-1]["params"]["authChallengeResponse"]
    assert repeated == {"response": "CancelAuth"}

    wrong = SimpleNamespace(
        request_id=cdp.fetch.RequestId("request-2"),
        auth_challenge=SimpleNamespace(source="Proxy", origin="http://other:8080"),
    )
    handler(wrong)
    await asyncio.gather(*tuple(tab._browser_pool_proxy_tasks))
    denied = tab.sent[-1]["params"]["authChallengeResponse"]
    assert denied == {"response": "CancelAuth"}
    assert secret not in repr(tab.sent[-1])
    assert tab._browser_pool_proxy_error == "Proxy authentication challenge rejected"

    for request_id, source, origin in (
        ("request-3", "Server", "http://gateway:8080"),
        ("request-4", "Proxy", "http://extractor@gateway:8080/path"),
    ):
        rejected = SimpleNamespace(
            request_id=cdp.fetch.RequestId(request_id),
            auth_challenge=SimpleNamespace(source=source, origin=origin),
        )
        handler(rejected)
        await asyncio.gather(*tuple(tab._browser_pool_proxy_tasks))
        response = tab.sent[-1]["params"]["authChallengeResponse"]
        assert response == {"response": "CancelAuth"}


def proxy_settings(**overrides):
    values = {
        "browser_proxy_enabled": True,
        "browser_proxy_url": "http://gateway:8080",
        "browser_proxy_token": MagicMock(get_secret_value=lambda: "x" * 24),
        "browser_tab_close_timeout_seconds": 0.05,
        "browser_tab_create_timeout_seconds": 0.05,
        "browser_tab_acquire_timeout_seconds": 0.05,
        "browser_operation_timeout_seconds": 1.0,
        "browser_failure_threshold": 1,
        "browser_restart_marker_path": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def mock_connected_browser():
    browser = MagicMock()
    browser.socket = SimpleNamespace(close_code=None)
    browser._targets = []
    browser._browser_pool_closing = False
    browser.aclose = AsyncMock()
    return browser


@pytest.mark.asyncio
async def test_cancelled_context_allocation_cleans_late_receipt_without_reopening():
    browser_proxy._ownership_unknown = False
    browser_pool._detached_operation_tasks = set()
    browser_pool._browser_generation = 9
    browser = mock_connected_browser()
    allocation_started = asyncio.Event()
    finish_allocation = asyncio.Event()
    disposed = []
    disposed_event = asyncio.Event()

    async def send(command):
        method = next(command)["method"]
        assert method == "Target.createBrowserContext"
        allocation_started.set()
        await finish_allocation.wait()
        return cdp.browser.BrowserContextID("late-context")

    async def dispose(b, generation, command, timeout):
        disposed.append(next(command))
        disposed_event.set()

    browser.send = send
    with (
        patch("news_aggregator.config.settings", proxy_settings()),
        patch.object(browser_pool, "_send_existing_connection", AsyncMock(side_effect=dispose)),
    ):
        operation = asyncio.create_task(browser_proxy.create_proxy_tab(browser, "about:blank"))
        await allocation_started.wait()
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert browser_proxy._ownership_unknown is True
        finish_allocation.set()
        await asyncio.wait_for(disposed_event.wait(), timeout=1)

    assert disposed == [{"method": "Target.disposeBrowserContext", "params": {"browserContextId": "late-context"}}]
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert browser_pool._detached_operation_tasks == set()
    assert browser_proxy._ownership_unknown is True  # unknown allocation keeps this adapter tainted
    browser_proxy._ownership_unknown = False


@pytest.mark.asyncio
async def test_cancelled_target_allocation_disposes_known_context_without_closing_target():
    browser_proxy._ownership_unknown = False
    browser_pool._detached_operation_tasks = set()
    browser_pool._browser_generation = 10
    browser = mock_connected_browser()
    target_started = asyncio.Event()
    dispose_commands = []

    async def send(command):
        request = next(command)
        if request["method"] == "Target.createBrowserContext":
            return cdp.browser.BrowserContextID("known-context")
        assert request["method"] == "Target.createTarget"
        target_started.set()
        await asyncio.Event().wait()

    async def dispose(b, generation, command, timeout):
        dispose_commands.append(next(command))

    browser.send = AsyncMock(side_effect=send)
    with (
        patch("news_aggregator.config.settings", proxy_settings()),
        patch.object(browser_pool, "_send_existing_connection", AsyncMock(side_effect=dispose)),
        patch("news_aggregator.core.browser_pool._OwnedTab", FakeTab),
        patch("news_aggregator.core.browser_pool._register_tab"),
    ):
        operation = asyncio.create_task(browser_proxy.create_proxy_tab(browser, "about:blank"))
        await target_started.wait()
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation

    assert dispose_commands == [{"method": "Target.disposeBrowserContext", "params": {"browserContextId": "known-context"}}]
    assert browser_proxy._ownership_unknown is False
    assert all(command["method"] != "Target.closeTarget" for command in dispose_commands)


@pytest.mark.asyncio
async def test_target_transport_failure_is_sanitized_after_context_disposal():
    browser_proxy._ownership_unknown = False
    browser_pool._browser_generation = 11
    browser = mock_connected_browser()
    dispose = AsyncMock()

    async def send(command):
        method = next(command)["method"]
        if method == "Target.createBrowserContext":
            return cdp.browser.BrowserContextID("known-context")
        raise ConnectionError("transport included internal endpoint details")

    browser.send = AsyncMock(side_effect=send)
    with (
        patch("news_aggregator.config.settings", proxy_settings()),
        patch.object(browser_pool, "_send_existing_connection", dispose),
        patch("news_aggregator.core.browser_pool._OwnedTab", FakeTab),
        patch("news_aggregator.core.browser_pool._register_tab"),
        pytest.raises(BrowserUnavailableError, match="setup failed") as raised,
    ):
        await browser_proxy.create_proxy_tab(browser, "about:blank")

    assert "internal endpoint" not in str(raised.value)
    dispose.assert_awaited_once()
    assert browser_proxy._ownership_unknown is False


@pytest.mark.asyncio
async def test_attach_protocol_failure_is_sanitized_and_disposes_only_owned_context():
    browser_proxy._ownership_unknown = False
    browser_pool._browser_generation = 111
    browser = mock_connected_browser()
    browser.send = AsyncMock(side_effect=[
        cdp.browser.BrowserContextID("owned-context"), cdp.target.TargetID("owned-target"),
    ])
    commands = []

    class AttachFailureTab(FakeTab):
        async def attach(self):
            raise RuntimeError("untrusted CDP protocol payload")

    async def dispose(_browser, _generation, command, _timeout):
        commands.append(next(command))

    with (
        patch("news_aggregator.config.settings", proxy_settings()),
        patch("news_aggregator.core.browser_pool._OwnedTab", AttachFailureTab),
        patch.object(browser_pool, "_send_existing_connection", AsyncMock(side_effect=dispose)),
        pytest.raises(BrowserUnavailableError, match="setup failed") as raised,
    ):
        await browser_proxy.create_proxy_tab(browser, "about:blank")

    assert "untrusted CDP" not in str(raised.value)
    assert commands == [{
        "method": "Target.disposeBrowserContext",
        "params": {"browserContextId": "owned-context"},
    }]
    assert "Target.closeTarget" not in {command["method"] for command in commands}


@pytest.mark.asyncio
async def test_cleanup_refuses_stale_generation_without_sending_or_reopening():
    browser = mock_connected_browser()
    browser_pool._browser_generation = 112
    with (
        patch.object(browser_pool.Connection, "send", new=AsyncMock()) as send,
        pytest.raises(ConnectionError, match="unavailable"),
    ):
        await browser_pool._send_existing_connection(
            browser, 111, cdp.target.dispose_browser_context(cdp.browser.BrowserContextID("ctx")), 1,
        )
    send.assert_not_awaited()
    browser.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_proxy_tab_cleanup_disposes_context_removes_handlers_and_never_closes_target():
    browser_proxy._ownership_unknown = False
    browser_pool._browser_generation = 12
    browser = mock_connected_browser()
    tab = FakeTab()
    tab._browser_pool_context_id = cdp.browser.BrowserContextID("owned-context")
    tab._browser_pool_browser = browser
    tab._browser_pool_generation = browser_pool._browser_generation
    tab._browser_pool_proxy_tasks = set()
    callback = lambda event: None
    tab.add_handler(cdp.fetch.RequestPaused, callback)
    tab._browser_pool_proxy_handlers = [(cdp.fetch.RequestPaused, callback)]
    browser._browser_pool_tabs = {id(tab): tab}
    browser._targets = [tab]
    commands = []

    async def send_existing(_browser, generation, command, timeout):
        commands.append(next(command))

    with patch.object(browser_pool, "_send_existing_connection", AsyncMock(side_effect=send_existing)):
        error = await browser_pool._close_tab_impl(tab)

    assert error is None
    assert commands == [{"method": "Target.disposeBrowserContext", "params": {"browserContextId": "owned-context"}}]
    assert callback not in tab.handlers.get(cdp.fetch.RequestPaused, [])
    assert id(tab) not in browser._browser_pool_tabs
    assert tab not in browser._targets
    browser.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_proxy_dispose_failure_taints_adapter_without_global_restart():
    browser_proxy._ownership_unknown = False
    browser_pool._browser_generation = 13
    browser = mock_connected_browser()
    browser_pool._browser = browser
    tab = FakeTab()
    tab._browser_pool_context_id = "owned-context"
    tab._browser_pool_browser = browser
    tab._browser_pool_generation = browser_pool._browser_generation
    tab._browser_pool_proxy_tasks = set()
    marker = AsyncMock()

    async def fail_dispose(*args, **kwargs):
        raise ConnectionError("cannot dispose")

    with (
        patch("news_aggregator.config.settings", proxy_settings()),
        patch.object(browser_pool, "_send_existing_connection", AsyncMock(side_effect=fail_dispose)),
        patch.object(browser_pool, "_write_restart_marker", marker),
    ):
        error = await browser_pool._close_tab_impl(tab)

    assert isinstance(error, ConnectionError)
    assert browser_proxy._ownership_unknown is True
    assert browser_pool._browser is browser
    assert browser_pool._consecutive_browser_failures == 0
    marker.assert_not_called()
    browser.aclose.assert_not_awaited()
    browser_pool._browser = None
    browser_proxy._ownership_unknown = False


@pytest.mark.asyncio
async def test_owned_tab_navigation_rejects_unsafe_url_and_chrome_error_text():
    tab = object.__new__(browser_pool._OwnedTab)
    tab.__dict__["_browser_pool_context_id"] = "context"
    tab.handlers = defaultdict(list)
    tab.__dict__["_browser_pool_proxy_tasks"] = set()
    tab.__dict__["_browser_pool_proxy_error"] = None
    tab.send = AsyncMock(return_value=(None, None, "net::ERR_BLOCKED_BY_CLIENT"))
    tab.attach = AsyncMock()

    with pytest.raises(BrowserUnavailableError, match="destination blocked"):
        await browser_pool._OwnedTab.get(tab, "http://example.com/")
    tab.send.assert_not_awaited()

    with pytest.raises(BrowserUnavailableError, match="navigation was blocked"):
        await browser_pool._OwnedTab.get(tab, "https://example.com/")
    command = next(tab.send.await_args.args[0])
    assert command["method"] == "Page.navigate"
    assert command["params"]["url"] == "https://example.com/"


@pytest.mark.asyncio
async def test_about_blank_setup_can_navigate_through_owned_proxy_tab():
    tab = object.__new__(browser_pool._OwnedTab)
    tab.__dict__["_browser_pool_context_id"] = "context"
    tab.handlers = defaultdict(list)
    tab.__dict__["_browser_pool_proxy_tasks"] = set()
    tab.__dict__["_browser_pool_proxy_error"] = None
    tab.send = AsyncMock(return_value=(None, None, None))
    tab.attach = AsyncMock()

    assert await browser_pool._OwnedTab.get(tab, "https://example.com/article") is tab
    command = next(tab.send.await_args.args[0])
    assert command["method"] == "Page.navigate"
    tab.attach.assert_not_awaited()


@pytest.mark.asyncio
async def test_proxy_navigation_transport_failure_never_invalidates_shared_browser():
    tab = SimpleNamespace(
        _browser_pool_context_id=cdp.browser.BrowserContextID("owned-context"),
        _browser_pool_browser=mock_connected_browser(),
        _browser_pool_generation=44,
    )
    invalidate = AsyncMock()
    with patch.object(browser_pool, "invalidate_browser", invalidate):
        with pytest.raises(BrowserUnavailableError, match="ConnectionError"):
            await browser_pool.run_browser_operation(
                _raise_transport_error(), "proxy navigation", browser_session=tab,
            )
    invalidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_proxy_handler_and_auth_tracking_limits_fail_closed():
    browser = MagicMock()
    browser._targets = []
    browser.send = AsyncMock(side_effect=[cdp.browser.BrowserContextID("ctx"), cdp.target.TargetID("target")])
    with (
        patch("news_aggregator.config.settings", proxy_settings()),
        patch("news_aggregator.core.browser_pool._OwnedTab", FakeTab),
        patch("news_aggregator.core.browser_pool._register_tab"),
    ):
        tab = await browser_proxy.create_proxy_tab(browser, "about:blank")
    request_handler = tab.handlers[cdp.fetch.RequestPaused][0]
    tab._browser_pool_proxy_tasks.update(range(32))
    request_handler(SimpleNamespace(request=SimpleNamespace(url="https://example.com/"), request_id="r"))
    assert tab._browser_pool_proxy_error == "Proxy request handling capacity exceeded"
    assert tab.sent == [{"method": "Fetch.enable", "params": {"handleAuthRequests": True}}]

    auth_handler = tab.handlers[cdp.fetch.AuthRequired][0]
    tab._browser_pool_proxy_tasks.clear()
    tab._browser_pool_proxy_auth_used.update(str(i) for i in range(128))
    auth_handler(SimpleNamespace(
        request_id=cdp.fetch.RequestId("new"),
        auth_challenge=SimpleNamespace(source="Proxy", origin="http://gateway:8080"),
    ))
    await asyncio.gather(*tuple(tab._browser_pool_proxy_tasks))
    assert tab.sent[-1]["params"]["authChallengeResponse"]["response"] == "CancelAuth"


async def _raise_transport_error():
    raise ConnectionError("private websocket endpoint")
