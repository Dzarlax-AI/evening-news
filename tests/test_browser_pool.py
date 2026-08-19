"""Tests for the shared browser pool (nodriver + CDP)."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import news_aggregator.core.browser_pool as bp
from news_aggregator.core.exceptions import BrowserUnavailableError


def make_browser(*, connected: bool = True) -> MagicMock:
    browser = MagicMock()
    browser.socket = MagicMock()
    browser.socket.close_code = None if connected else 1006
    browser.start = AsyncMock(return_value=browser)
    browser.aclose = AsyncMock()
    browser.get = AsyncMock()
    browser._process = None
    return browser


@pytest.fixture(autouse=True)
def reset_browser_pool():
    """Reset all process-global browser state before each test."""
    bp._browser = None
    bp._lock = asyncio.Lock()
    bp._tab_semaphore = asyncio.Semaphore(1)
    bp._last_failure_at = None
    bp._last_failure_error = None
    yield
    bp._browser = None


@pytest.fixture
def remote_settings():
    settings = MagicMock()
    settings.browser_ws_endpoint = "ws://chrome:9222"
    return settings


@pytest.fixture
def local_settings():
    settings = MagicMock()
    settings.browser_ws_endpoint = None
    return settings


@pytest.mark.asyncio
async def test_get_browser_remote(remote_settings):
    candidate = make_browser()

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp, "_resolve_host", AsyncMock(return_value="10.0.0.2")),
        patch.object(bp, "_check_remote_port", AsyncMock()),
        patch.object(bp, "Browser", return_value=candidate) as browser_factory,
    ):
        browser = await bp.get_browser()

    candidate.start.assert_awaited_once()
    config = browser_factory.call_args.args[0]
    assert config.host == "10.0.0.2"
    assert config.port == 9222
    assert browser is candidate
    assert bp.get_browser_pool_status().connected is True


@pytest.mark.asyncio
async def test_get_browser_local(local_settings):
    browser = make_browser()

    with (
        patch("news_aggregator.config.settings", local_settings),
        patch.object(bp.os.path, "exists", return_value=False),
        patch.object(bp.uc, "start", AsyncMock(return_value=browser)) as start,
    ):
        result = await bp.get_browser()

    start.assert_awaited_once_with(
        headless=True,
        browser_args=["--no-sandbox", "--disable-setuid-sandbox"],
    )
    assert result is browser


@pytest.mark.asyncio
async def test_get_browser_reuses_open_socket():
    browser = make_browser()
    bp._browser = browser

    assert await bp.get_browser() is browser


@pytest.mark.asyncio
async def test_get_browser_closes_stale_connection_before_reconnect(remote_settings):
    stale = make_browser(connected=False)
    candidate = make_browser()
    bp._browser = stale

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp, "_resolve_host", AsyncMock(return_value="10.0.0.2")),
        patch.object(bp, "_check_remote_port", AsyncMock()),
        patch.object(bp, "Browser", return_value=candidate),
    ):
        browser = await bp.get_browser()

    stale.aclose.assert_awaited_once()
    candidate.start.assert_awaited_once()
    assert browser is candidate


@pytest.mark.asyncio
async def test_failed_start_closes_candidate_and_never_publishes_it(remote_settings):
    candidate = make_browser()
    candidate.start.side_effect = ConnectionError("CDP refused")

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp, "_resolve_host", AsyncMock(return_value="10.0.0.2")),
        patch.object(bp, "_check_remote_port", AsyncMock()),
        patch.object(bp, "Browser", return_value=candidate),
        pytest.raises(BrowserUnavailableError, match="CDP refused"),
    ):
        await bp.get_browser()

    candidate.aclose.assert_awaited_once()
    assert bp._browser is None
    assert bp.get_browser_pool_status().last_error is not None


@pytest.mark.asyncio
async def test_cancelled_start_closes_candidate_and_propagates_cancel(remote_settings):
    candidate = make_browser()
    started = asyncio.Event()

    async def wait_forever():
        started.set()
        await asyncio.Event().wait()

    candidate.start.side_effect = wait_forever

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp, "_resolve_host", AsyncMock(return_value="10.0.0.2")),
        patch.object(bp, "_check_remote_port", AsyncMock()),
        patch.object(bp, "Browser", return_value=candidate),
    ):
        task = asyncio.create_task(bp.get_browser())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    candidate.aclose.assert_awaited_once()
    assert bp._browser is None


@pytest.mark.asyncio
async def test_reconnect_cooldown_prevents_storm(remote_settings):
    bp._last_failure_at = time.monotonic()
    bp._last_failure_error = "ConnectionError: down"

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp, "Browser") as browser_factory,
        pytest.raises(BrowserUnavailableError, match="cooldown"),
    ):
        await bp.get_browser()

    browser_factory.assert_not_called()


@pytest.mark.asyncio
async def test_successful_retry_clears_failure_state(remote_settings):
    candidate = make_browser()
    bp._last_failure_at = time.monotonic() - bp._RECONNECT_COOLDOWN_SECONDS - 1
    bp._last_failure_error = "ConnectionError: old"

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp, "_resolve_host", AsyncMock(return_value="10.0.0.2")),
        patch.object(bp, "_check_remote_port", AsyncMock()),
        patch.object(bp, "Browser", return_value=candidate),
    ):
        await bp.get_browser()

    status = bp.get_browser_pool_status()
    assert status.last_error is None
    assert status.cooldown_remaining_seconds == 0


@pytest.mark.asyncio
async def test_close_browser_awaits_connection_close():
    browser = make_browser()
    bp._browser = browser

    await bp.close_browser()

    browser.aclose.assert_awaited_once()
    assert bp._browser is None


@pytest.mark.asyncio
async def test_browser_tab_closes_tab_on_success_and_failure():
    browser = make_browser()
    tab = MagicMock()
    tab.close = AsyncMock()
    browser.get.return_value = tab
    bp._browser = browser

    async with bp.browser_tab("https://example.com") as yielded:
        assert yielded is tab
    tab.close.assert_awaited_once()

    tab.close.reset_mock()
    with pytest.raises(RuntimeError, match="consumer failed"):
        async with bp.browser_tab("https://example.com"):
            raise RuntimeError("consumer failed")
    tab.close.assert_awaited_once()


def test_endpoint_parsing_variants():
    cases = [
        ("ws://myhost:1234", "myhost", 1234),
        ("http://chrome:9222", "chrome", 9222),
        ("browser:5555", "browser", 5555),
        ("chrome", "chrome", 9222),
    ]

    for endpoint, expected_host, expected_port in cases:
        assert bp._parse_endpoint(endpoint) == (expected_host, expected_port)


@pytest.mark.parametrize("endpoint", ["", "ws://:9222", "ws://chrome:notaport"])
def test_endpoint_parsing_rejects_invalid_values(endpoint):
    with pytest.raises(ValueError):
        bp._parse_endpoint(endpoint)
