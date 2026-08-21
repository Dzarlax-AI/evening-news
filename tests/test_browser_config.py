"""Validation tests for browser lifecycle settings."""

import math

import pytest
from pydantic import ValidationError

from news_aggregator.config import Settings


def test_browser_timeout_defaults_are_finite_and_positive():
    settings = Settings(_env_file=None)

    for value in (
        settings.browser_tab_acquire_timeout_seconds,
        settings.browser_tab_create_timeout_seconds,
        settings.browser_tab_close_timeout_seconds,
        settings.browser_operation_timeout_seconds,
    ):
        assert math.isfinite(value)
        assert value > 0
    assert settings.browser_failure_threshold == 3


@pytest.mark.parametrize(
    "name",
    [
        "BROWSER_TAB_ACQUIRE_TIMEOUT_SECONDS",
        "BROWSER_TAB_CREATE_TIMEOUT_SECONDS",
        "BROWSER_TAB_CLOSE_TIMEOUT_SECONDS",
        "BROWSER_OPERATION_TIMEOUT_SECONDS",
        "BROWSER_FAILURE_THRESHOLD",
    ],
)
def test_browser_budgets_reject_non_positive_values(name):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{name: 0})


def test_browser_restart_marker_must_be_absolute():
    with pytest.raises(ValidationError, match="must be an absolute path"):
        Settings(_env_file=None, BROWSER_RESTART_MARKER_PATH="relative/restart.request")
