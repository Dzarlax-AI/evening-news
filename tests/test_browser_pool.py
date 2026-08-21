"""Tests for the shared browser pool (nodriver + CDP)."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import news_aggregator.core.browser_pool as bp
from news_aggregator.core.exceptions import BrowserBusyError, BrowserUnavailableError


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
    bp._browser_generation = 0
    bp._lock = asyncio.Lock()
    bp._cycle_lock = asyncio.Lock()
    bp._active_cycle_leases = set()
    bp._active_tab_leases = set()
    bp._close_when_idle = False
    bp._detached_operation_tasks = set()
    bp._tab_semaphore = asyncio.Semaphore(1)
    bp._last_failure_at = None
    bp._last_failure_error = None
    bp._consecutive_browser_failures = 0
    bp._restart_marker_latched = False
    yield
    bp._browser = None


@pytest.fixture
def remote_settings():
    settings = MagicMock()
    settings.browser_ws_endpoint = "ws://chrome:9222"
    settings.browser_tab_acquire_timeout_seconds = 120.0
    settings.browser_tab_create_timeout_seconds = 30.0
    settings.browser_tab_close_timeout_seconds = 5.0
    settings.browser_operation_timeout_seconds = 90.0
    settings.browser_failure_threshold = 3
    settings.browser_restart_marker_path = None
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


@pytest.mark.asyncio
async def test_browser_tab_creation_timeout_invalidates_session(remote_settings):
    browser = make_browser()
    started = asyncio.Event()

    async def hang(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    browser.get.side_effect = hang
    bp._browser = browser
    remote_settings.browser_tab_acquire_timeout_seconds = 0.1
    remote_settings.browser_tab_create_timeout_seconds = 0.01
    remote_settings.browser_operation_timeout_seconds = 0.01
    remote_settings.browser_failure_threshold = 3
    remote_settings.browser_restart_marker_path = None

    with (
        patch("news_aggregator.config.settings", remote_settings),
        pytest.raises(BrowserUnavailableError, match="create tab.*timed out"),
    ):
        async with bp.browser_tab("https://example.com"):
            pass

    assert started.is_set()
    assert bp._browser is None
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_tab_acquisition_timeout_is_busy_without_invalidating_owner(
    remote_settings,
):
    browser = make_browser()
    bp._browser = browser
    remote_settings.browser_tab_acquire_timeout_seconds = 0.01
    await bp._tab_semaphore.acquire()

    try:
        with (
            patch("news_aggregator.config.settings", remote_settings),
            pytest.raises(BrowserBusyError, match="acquisition timed out"),
        ):
            async with bp.browser_tab("https://example.com"):
                pass
    finally:
        bp._tab_semaphore.release()

    assert bp._browser is browser
    assert bp._consecutive_browser_failures == 0
    browser.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_browser_operation_timeout_invalidates_and_closes():
    browser = make_browser()
    bp._browser = browser

    async def hang():
        await asyncio.Event().wait()

    with pytest.raises(BrowserUnavailableError, match="navigation.*timed out"):
        await bp.run_browser_operation(hang(), "navigation", timeout_seconds=0.01)

    assert bp._browser is None
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_name", ["navigation", "content retrieval"])
async def test_each_hanging_cdp_operation_has_a_finite_deadline(operation_name):
    browser = make_browser()
    bp._browser = browser

    async def hang():
        await asyncio.Event().wait()

    with pytest.raises(BrowserUnavailableError, match=f"{operation_name}.*timed out"):
        await bp.run_browser_operation(hang(), operation_name, timeout_seconds=0.01)


@pytest.mark.asyncio
async def test_transport_failure_invalidates_browser_session():
    browser = make_browser()
    bp._browser = browser

    async def disconnect():
        raise ConnectionResetError("peer reset")

    with pytest.raises(BrowserUnavailableError, match="content retrieval failed"):
        await bp.run_browser_operation(disconnect(), "content retrieval")

    assert bp._browser is None
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_stale_operation_failure_does_not_detach_newer_browser_session():
    old_browser = make_browser()
    new_browser = make_browser()
    stale_session = MagicMock()
    stale_session._browser_pool_browser = old_browser
    stale_session._browser_pool_generation = 1
    bp._browser = old_browser
    bp._browser_generation = 1
    fail_now = asyncio.Event()

    async def stale_operation():
        await fail_now.wait()
        raise ConnectionResetError("old websocket reset")

    task = asyncio.create_task(
        bp.run_browser_operation(
            stale_operation(),
            "stale navigation",
            browser_session=stale_session,
        )
    )
    await asyncio.sleep(0)
    bp._browser = new_browser
    bp._browser_generation = 2
    bp.record_browser_success()
    fail_now.set()

    with pytest.raises(BrowserUnavailableError):
        await task

    assert bp._browser is new_browser
    old_browser.aclose.assert_awaited_once()
    new_browser.aclose.assert_not_awaited()
    assert bp._consecutive_browser_failures == 0
    assert bp._last_failure_at is None


@pytest.mark.asyncio
async def test_stale_operation_success_does_not_reset_new_generation_failure():
    old_browser = make_browser()
    new_browser = make_browser()
    stale_session = MagicMock()
    stale_session._browser_pool_browser = old_browser
    stale_session._browser_pool_generation = 1
    bp._browser = old_browser
    bp._browser_generation = 1
    finish_old = asyncio.Event()

    async def delayed_old_success():
        await finish_old.wait()
        return "old result"

    task = asyncio.create_task(
        bp.run_browser_operation(
            delayed_old_success(),
            "old success",
            browser_session=stale_session,
        )
    )
    await asyncio.sleep(0)
    bp._browser = new_browser
    bp._browser_generation = 2
    bp.record_browser_failure(ConnectionError("new incident"))
    finish_old.set()

    assert await task == "old result"
    assert bp._consecutive_browser_failures == 1
    assert bp._last_failure_at is not None


@pytest.mark.asyncio
async def test_websocket_connection_closed_invalidates_browser_session():
    from websockets.exceptions import ConnectionClosed

    browser = make_browser()
    bp._browser = browser

    async def disconnect():
        raise ConnectionClosed(None, None)

    with pytest.raises(BrowserUnavailableError, match="navigation failed"):
        await bp.run_browser_operation(disconnect(), "navigation")

    assert bp._browser is None
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_seconds", [0, -1, float("inf"), float("nan")])
async def test_browser_operation_rejects_non_finite_or_non_positive_timeout(
    timeout_seconds,
):
    with pytest.raises(ValueError, match="finite positive"):
        await bp.run_browser_operation(
            asyncio.sleep(0),
            "invalid timeout",
            timeout_seconds=timeout_seconds,
        )


@pytest.mark.asyncio
async def test_browser_operation_clamps_override_to_configured_upper_bound(
    remote_settings,
):
    remote_settings.browser_operation_timeout_seconds = 0.25
    original_wait = asyncio.wait
    observed = {}

    async def recording_wait(fs, *, timeout=None, **kwargs):
        observed["timeout"] = timeout
        return await original_wait(fs, timeout=timeout, **kwargs)

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(asyncio, "wait", side_effect=recording_wait),
    ):
        assert await bp.run_browser_operation(
            asyncio.sleep(0, result="ok"),
            "bounded override",
            timeout_seconds=999,
        ) == "ok"

    assert observed["timeout"] == 0.25


@pytest.mark.asyncio
async def test_invalidation_cleanup_finishes_when_caller_is_cancelled():
    browser = make_browser()
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def delayed_close():
        close_started.set()
        await allow_close.wait()

    browser.aclose.side_effect = delayed_close
    bp._browser = browser

    task = asyncio.create_task(bp.invalidate_browser(ConnectionError("lost")))
    await close_started.wait()
    task.cancel()
    allow_close.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    browser.aclose.assert_awaited_once()
    assert bp._browser is None


@pytest.mark.asyncio
async def test_invalidation_cleanup_finishes_after_repeated_cancellation():
    browser = make_browser()
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def delayed_close():
        close_started.set()
        await allow_close.wait()

    browser.aclose.side_effect = delayed_close
    bp._browser = browser
    task = asyncio.create_task(bp.invalidate_browser(ConnectionError("lost")))
    await close_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    allow_close.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_hard_deadline_does_not_wait_for_cancel_suppressing_operation():
    browser = make_browser()
    bp._browser = browser
    allow_finish = asyncio.Event()

    async def suppress_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await allow_finish.wait()

    started = time.monotonic()
    with pytest.raises(BrowserUnavailableError, match="timed out"):
        await bp.run_browser_operation(
            suppress_cancellation(),
            "stubborn CDP call",
            timeout_seconds=0.01,
        )
    elapsed = time.monotonic() - started

    assert elapsed < 0.2
    assert bp._detached_operation_tasks
    allow_finish.set()
    for _ in range(20):
        if not bp._detached_operation_tasks:
            break
        await asyncio.sleep(0.01)
    assert not bp._detached_operation_tasks


@pytest.mark.asyncio
async def test_browser_failure_threshold_writes_one_atomic_marker(tmp_path, remote_settings):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 3
    remote_settings.browser_restart_marker_path = str(marker)

    with patch("news_aggregator.config.settings", remote_settings):
        bp.record_browser_failure(ConnectionError("one"))
        bp.record_browser_failure(ConnectionError("two"))
        assert not marker.exists()
        bp.record_browser_failure(ConnectionError("three"))
        first_stat = marker.stat()
        bp.record_browser_failure(ConnectionError("four"))

    assert marker.read_text(encoding="utf-8") == "browser-recovery-request\n"
    assert marker.stat().st_ino == first_stat.st_ino


@pytest.mark.asyncio
async def test_restart_marker_is_latched_once_per_incident(tmp_path, remote_settings):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 3
    remote_settings.browser_restart_marker_path = str(marker)

    with patch("news_aggregator.config.settings", remote_settings):
        for index in range(3):
            bp.record_browser_failure(ConnectionError(f"incident-one-{index}"))
        assert marker.exists()

        marker.unlink()
        bp.record_browser_failure(ConnectionError("incident-one-four"))
        assert not marker.exists()

        bp.record_browser_success()
        for index in range(3):
            bp.record_browser_failure(ConnectionError(f"incident-two-{index}"))

    assert marker.exists()


@pytest.mark.asyncio
async def test_restart_marker_open_error_does_not_latch_incident(
    tmp_path, remote_settings
):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 3
    remote_settings.browser_restart_marker_path = str(marker)
    real_open = bp.os.open
    calls = 0

    def fail_once(path, flags, mode):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("temporary filesystem error")
        return real_open(path, flags, mode)

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp.os, "open", side_effect=fail_once),
    ):
        for index in range(3):
            bp.record_browser_failure(ConnectionError(f"failure-{index}"))
        assert not marker.exists()
        assert bp._restart_marker_latched is False
        bp.record_browser_failure(ConnectionError("failure-four"))

    assert marker.exists()
    assert bp._restart_marker_latched is True


@pytest.mark.asyncio
async def test_restart_marker_write_error_is_contained_and_retried(
    tmp_path, remote_settings
):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 1
    remote_settings.browser_restart_marker_path = str(marker)
    browser = make_browser()
    bp._browser = browser

    async def disconnect():
        raise ConnectionResetError("primary transport failure")

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp.os, "write", side_effect=OSError("disk full")),
        pytest.raises(BrowserUnavailableError, match="primary transport failure"),
    ):
        await bp.run_browser_operation(disconnect(), "navigation")

    assert not marker.exists()
    assert bp._restart_marker_latched is False
    browser.aclose.assert_awaited_once()

    with patch("news_aggregator.config.settings", remote_settings):
        bp.record_browser_failure(ConnectionError("retry"))
    assert marker.exists()


@pytest.mark.asyncio
async def test_restart_marker_close_error_is_contained_and_retried(
    tmp_path, remote_settings
):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 1
    remote_settings.browser_restart_marker_path = str(marker)
    real_close = bp.os.close
    calls = 0

    def fail_once(descriptor):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("close interrupted")
        return real_close(descriptor)

    with (
        patch("news_aggregator.config.settings", remote_settings),
        patch.object(bp.os, "close", side_effect=fail_once),
    ):
        bp.record_browser_failure(ConnectionError("first"))

    assert not marker.exists()
    assert bp._restart_marker_latched is False

    with patch("news_aggregator.config.settings", remote_settings):
        bp.record_browser_failure(ConnectionError("retry"))
    assert marker.exists()


@pytest.mark.asyncio
async def test_overlapping_cycle_leases_close_only_after_last_release():
    browser = make_browser()
    bp._browser = browser
    first = await bp.acquire_browser_cycle()
    second = await bp.acquire_browser_cycle()

    await bp.release_browser_cycle(first)
    assert bp._browser is browser
    browser.aclose.assert_not_awaited()

    await bp.release_browser_cycle(second)
    assert bp._browser is None
    browser.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_last_cycle_defers_close_until_direct_browser_tab_exits():
    browser = make_browser()
    tab = MagicMock()
    tab.close = AsyncMock()
    browser.get.return_value = tab
    bp._browser = browser
    cycle = await bp.acquire_browser_cycle()
    tab_entered = asyncio.Event()
    release_tab = asyncio.Event()

    async def hold_tab():
        async with bp.browser_tab("https://example.com"):
            tab_entered.set()
            await release_tab.wait()

    tab_task = asyncio.create_task(hold_tab())
    await tab_entered.wait()
    await bp.release_browser_cycle(cycle)
    await bp.release_browser_cycle(cycle)

    assert bp._browser is browser
    browser.aclose.assert_not_awaited()
    release_tab.set()
    await tab_task

    assert bp._browser is None
    browser.aclose.assert_awaited_once()
    assert bp._tab_semaphore._value == 1


@pytest.mark.asyncio
async def test_confirmed_browser_success_resets_failure_streak(tmp_path, remote_settings):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 3
    remote_settings.browser_restart_marker_path = str(marker)

    with patch("news_aggregator.config.settings", remote_settings):
        bp.record_browser_failure(ConnectionError("one"))
        bp.record_browser_failure(ConnectionError("two"))
        bp.record_browser_success()
        bp.record_browser_failure(ConnectionError("new incident"))

    assert bp._consecutive_browser_failures == 1
    assert not marker.exists()


@pytest.mark.asyncio
async def test_non_browser_failure_does_not_touch_restart_marker(tmp_path, remote_settings):
    marker = tmp_path / "restart.request"
    remote_settings.browser_failure_threshold = 1
    remote_settings.browser_restart_marker_path = str(marker)

    with patch("news_aggregator.config.settings", remote_settings):
        # Ordinary application errors are never fed to the browser failure tracker.
        await asyncio.sleep(0)

    assert not marker.exists()

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
