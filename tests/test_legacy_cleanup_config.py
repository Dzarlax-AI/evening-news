"""Startup contracts for Go-owned collection, extraction, and scheduling."""

import pytest
from pydantic import ValidationError

from news_aggregator.config import Settings


TOKEN = "synthetic-service-token-value"


def configured(**overrides):
    values = {
        "_env_file": None,
        "EXTRACTION_SERVICE_URL": "http://extractor:8080",
        "EXTRACTION_SERVICE_TOKEN": TOKEN,
        "SCHEDULER_API_URL": "https://scheduler.example.test",
        "SCHEDULER_API_TOKEN": TOKEN,
        "SCHEDULED_JOBS_TOKEN": TOKEN,
    }
    values.update(overrides)
    return Settings(**values)


def test_go_modes_are_the_only_runtime_defaults_and_deprecated_go_values_are_inert():
    settings = Settings(_env_file=None)
    assert settings.source_manager_mode == "go"
    assert settings.telegram_source_mode == "go"
    assert settings.page_monitor_source_mode == "go"
    assert settings.extraction_service_mode == "go"
    assert settings.schedule_dispatch_mode == "service"

    settings = configured(
        SOURCE_MANAGER_MODE="go", TELEGRAM_SOURCE_MODE="go", PAGE_MONITOR_SOURCE_MODE="go",
        EXTRACTION_SERVICE_MODE="go", EXTRACTION_SERVICE_PYTHON_FALLBACK="false",
        SCHEDULE_DISPATCH_MODE="service", SCHEDULER_ENABLED="false",
    )
    assert settings.scheduler_enabled is False
    assert settings.validate_runtime_services() is True


def test_scheduler_enabled_false_does_not_change_go_service_requirements():
    settings = configured(SCHEDULER_ENABLED=False, SCHEDULER_API_URL=None,
                          SCHEDULER_API_TOKEN=None)
    with pytest.raises(RuntimeError, match="SCHEDULER_API_URL"):
        settings.validate_runtime_services()


@pytest.mark.parametrize("name,value", [
    ("SOURCE_MANAGER_MODE", "python"),
    ("TELEGRAM_SOURCE_MODE", "shadow"),
    ("PAGE_MONITOR_SOURCE_MODE", "python"),
    ("EXTRACTION_SERVICE_MODE", "python"),
    ("SCHEDULE_DISPATCH_MODE", "python"),
    ("EXTRACTION_SERVICE_PYTHON_FALLBACK", "true"),
])
def test_removed_python_and_shadow_modes_fail_clearly(name, value):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{name: value})


def test_runtime_missing_config_is_reported_without_secrets():
    settings = Settings(
        _env_file=None,
        EXTRACTION_SERVICE_URL=None,
        EXTRACTION_SERVICE_TOKEN=None,
        SCHEDULER_API_URL=None,
        SCHEDULER_API_TOKEN=None,
        SCHEDULED_JOBS_TOKEN=None,
    )
    with pytest.raises(RuntimeError) as error:
        settings.validate_runtime_services()
    assert "EXTRACTION_SERVICE_URL" in str(error.value)
    assert TOKEN not in str(error.value)


def test_removed_browser_proxy_settings_do_not_echo_secrets_or_urls():
    marker = "retired-proxy-secret-marker"
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, BROWSER_PROXY_TOKEN=marker,
                 BROWSER_PROXY_URL="http://user:private@proxy.example.test:3128")
    rendered = str(error.value)
    assert marker not in rendered
    assert "private" not in rendered
    assert "proxy.example.test" not in rendered


def test_runtime_rejects_invalid_service_urls_without_echoing_values():
    for value in (
        "https://user:private@example.test/path",
        "https://service.example.test/path?token=private",
        "https://service.example.test/#secret",
        "ftp://service.example.test",
        "https://service.example.test:bad",
    ):
        with pytest.raises(RuntimeError) as error:
            configured(EXTRACTION_SERVICE_URL=value).validate_runtime_services()
        assert value not in str(error.value)
        assert "private" not in str(error.value)
        assert "secret" not in str(error.value)


def test_scheduler_http_requires_explicit_opt_in_and_receiver_token_is_required():
    with pytest.raises(RuntimeError, match="SCHEDULER_API_URL"):
        configured(SCHEDULER_API_URL="http://scheduler:8080").validate_runtime_services()
    assert configured(SCHEDULER_API_URL="http://scheduler:8080",
                      SCHEDULER_API_ALLOW_HTTP=True).validate_runtime_services()
    with pytest.raises(RuntimeError, match="SCHEDULED_JOBS_TOKEN"):
        configured(SCHEDULED_JOBS_TOKEN=None).validate_runtime_services()
