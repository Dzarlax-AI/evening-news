"""Migration contract tests; no production connection is opened."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from news_aggregator.migrations.article_processing_state import (
    ArticleProcessingStateMigration, ensure_article_processing_migration,
)


@pytest.mark.asyncio
async def test_additive_migration_backfills_only_unclassified_legacy_rows():
    db = MagicMock(execute=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    await ArticleProcessingStateMigration().execute(db)
    sql = [str(call.args[0]) for call in db.execute.await_args_list]
    assert sum("ADD COLUMN IF NOT EXISTS" in statement for statement in sql) == 4
    backfill = next(statement for statement in sql if "UPDATE articles" in statement)
    assert "WHERE processing_state IS NULL" in backfill
    assert "summary_processed IS TRUE" in backfill
    assert "category_processed IS TRUE" in backfill
    assert "ad_processed IS TRUE" in backfill
    assert "summary IS" not in backfill
    assert "is_advertisement" not in backfill
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_migration_failure_rolls_back_and_is_not_swallowed():
    db = MagicMock(execute=AsyncMock(side_effect=RuntimeError("permission denied")),
                   commit=AsyncMock(), rollback=AsyncMock())
    with pytest.raises(RuntimeError, match="permission denied"):
        await ArticleProcessingStateMigration().execute(db)
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


def test_required_article_migration_failure_blocks_startup():
    with pytest.raises(RuntimeError, match="article processing migration"):
        ensure_article_processing_migration({"errors": ["Migration 008_article_processing_state failed"]})
    with pytest.raises(RuntimeError):
        ensure_article_processing_migration({"errors": ["Migration system error: unavailable"]})
    ensure_article_processing_migration({"errors": []})
