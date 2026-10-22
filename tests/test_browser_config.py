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


def test_browser_proxy_is_opt_in_and_secret_is_redacted():
    defaults = Settings(_env_file=None)
    assert defaults.browser_proxy_enabled is False
    assert defaults.browser_proxy_url is None
    assert defaults.browser_proxy_token is None

    configured = Settings(
        _env_file=None,
        BROWSER_PROXY_ENABLED=True,
        BROWSER_PROXY_URL="http://egress-gateway:8080",
        BROWSER_PROXY_TOKEN="a-token-that-is-long-enough",
    )
    assert configured.browser_proxy_token.get_secret_value() == "a-token-that-is-long-enough"
    assert "a-token-that-is-long-enough" not in repr(configured)


def test_extraction_native_browser_is_opt_in_and_requires_go_without_python_fallback():
    assert Settings(_env_file=None).extraction_service_native_browser is False
    valid = Settings(_env_file=None, EXTRACTION_SERVICE_MODE='go',
                     EXTRACTION_SERVICE_NATIVE_BROWSER=True,
                     EXTRACTION_SERVICE_PYTHON_FALLBACK=False)
    assert valid.extraction_service_native_browser is True
    for values in (
        {'EXTRACTION_SERVICE_NATIVE_BROWSER': True},
        {'EXTRACTION_SERVICE_MODE': 'shadow', 'EXTRACTION_SERVICE_NATIVE_BROWSER': True},
        {'EXTRACTION_SERVICE_MODE': 'go', 'EXTRACTION_SERVICE_NATIVE_BROWSER': True,
         'EXTRACTION_SERVICE_PYTHON_FALLBACK': True},
    ):
        with pytest.raises(ValidationError, match='EXTRACTION_SERVICE_NATIVE_BROWSER'):
            Settings(_env_file=None, **values)


@pytest.mark.parametrize(
    "values",
    [
        {"BROWSER_PROXY_ENABLED": True},
        {"BROWSER_PROXY_URL": "http://gateway:8080", "BROWSER_PROXY_TOKEN": "x" * 16},
        {"BROWSER_PROXY_ENABLED": True, "BROWSER_PROXY_URL": "https://gateway:8080", "BROWSER_PROXY_TOKEN": "x" * 16},
        {"BROWSER_PROXY_ENABLED": True, "BROWSER_PROXY_URL": "http://user:pass@gateway:8080", "BROWSER_PROXY_TOKEN": "x" * 16},
        {"BROWSER_PROXY_ENABLED": True, "BROWSER_PROXY_URL": "http://gateway/path:8080", "BROWSER_PROXY_TOKEN": "x" * 16},
        {"BROWSER_PROXY_ENABLED": True, "BROWSER_PROXY_URL": "http://gateway", "BROWSER_PROXY_TOKEN": "x" * 16},
        {"BROWSER_PROXY_ENABLED": True, "BROWSER_PROXY_URL": "http://gateway:8080", "BROWSER_PROXY_TOKEN": "short"},
    ],
)
def test_browser_proxy_rejects_partial_or_invalid_configuration(values):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)
