"""Configuration contracts after Python browser ownership removal."""

import pytest
from pydantic import ValidationError

from news_aggregator.config import Settings


def test_extraction_native_browser_remains_an_explicit_go_service_option():
    assert Settings(_env_file=None).extraction_service_native_browser is False
    configured = Settings(_env_file=None, EXTRACTION_SERVICE_MODE="go",
                          EXTRACTION_SERVICE_NATIVE_BROWSER=True,
                          EXTRACTION_SERVICE_PYTHON_FALLBACK=False)
    assert configured.extraction_service_native_browser is True


def test_python_fallback_is_rejected():
    with pytest.raises(ValidationError, match="EXTRACTION_SERVICE_PYTHON_FALLBACK=true"):
        Settings(_env_file=None, EXTRACTION_SERVICE_PYTHON_FALLBACK=True)
