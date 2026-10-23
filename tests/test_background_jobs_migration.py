"""Required migration startup guard behavior."""

import pytest

from news_aggregator.migrations.background_jobs import ensure_background_jobs_migration


def test_background_jobs_migration_guard_rejects_its_failure():
    ensure_background_jobs_migration({"errors": []})
    ensure_background_jobs_migration({"errors": ["Migration 008 failed"]})
    with pytest.raises(RuntimeError, match="009_background_jobs"):
        ensure_background_jobs_migration({"errors": ["Migration 009_background_jobs failed"]})
    with pytest.raises(RuntimeError, match="Migration system error"):
        ensure_background_jobs_migration({"errors": ["Migration system error: unavailable"]})
