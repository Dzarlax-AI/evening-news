"""Input validation retained for the Go-backed Page Monitor adapter."""

import pytest

from news_aggregator.sources.page_monitor_source import PageMonitorConfig


@pytest.mark.parametrize("wait_timeout_ms", [0, -1, float("inf"), float("nan")])
def test_page_monitor_config_rejects_invalid_browser_timeout(wait_timeout_ms):
    with pytest.raises(ValueError, match="wait_timeout_ms"):
        PageMonitorConfig(
            url="https://example.com",
            name="Example",
            wait_timeout_ms=wait_timeout_ms,
        )
