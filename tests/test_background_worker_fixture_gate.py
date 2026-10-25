from pathlib import Path

import pytest

from news_aggregator.config import Settings
from scripts.run_background_worker_fixture_gate import DockerGate


@pytest.mark.parametrize("role", ["web", "worker"])
def test_background_worker_fixture_provides_isolated_go_service_settings(role):
    environment = DockerGate("fixture-image", Path("/tmp/background-worker-fixture")).app_environment(role)
    required = {
        "EXTRACTION_SERVICE_URL": environment["EXTRACTION_SERVICE_URL"],
        "EXTRACTION_SERVICE_TOKEN": environment["EXTRACTION_SERVICE_TOKEN"],
        "ARTICLE_INGESTION_MODE": environment["ARTICLE_INGESTION_MODE"],
        "ARTICLE_INGESTION_URL": environment["ARTICLE_INGESTION_URL"],
        "ARTICLE_INGESTION_TOKEN": environment["ARTICLE_INGESTION_TOKEN"],
        "SCHEDULER_API_URL": environment["SCHEDULER_API_URL"],
        "SCHEDULER_API_TOKEN": environment["SCHEDULER_API_TOKEN"],
        "SCHEDULER_API_ALLOW_HTTP": environment["SCHEDULER_API_ALLOW_HTTP"],
        "SCHEDULED_JOBS_TOKEN": environment["SCHEDULED_JOBS_TOKEN"],
    }
    settings = Settings(_env_file=None, **required)

    assert settings.validate_runtime_services() is True
    assert environment["SCHEDULER_API_URL"] == "http://fixture-services:8090"
    assert all("fixture-only" in environment[key] for key in (
        "EXTRACTION_SERVICE_TOKEN", "ARTICLE_INGESTION_TOKEN",
        "SCHEDULER_API_TOKEN", "SCHEDULED_JOBS_TOKEN"
    ))
