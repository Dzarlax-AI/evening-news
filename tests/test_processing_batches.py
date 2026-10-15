"""Execute production batch queries and writes against an isolated SQLite DB."""

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine, event, select, update
from sqlalchemy.orm import Session

from news_aggregator.database import Base
from news_aggregator.models import Article, ArticleCategory, Category
from news_aggregator.orchestrator import NewsOrchestrator
from news_aggregator.processing.processing_outcome import ProcessingOutcome


class AsyncSessionAdapter:
    def __init__(self, db):
        self.db = db

    async def execute(self, statement, parameters=None):
        return self.db.execute(statement, parameters or {})


class IsolatedQueue:
    """Match DatabaseQueueManager's commit-on-success contract using SQLite."""
    def __init__(self, engine):
        self.engine = engine
        self.fail_save = False

    async def execute_read(self, operation, **kwargs):
        with Session(self.engine) as db:
            return await operation(AsyncSessionAdapter(db))

    async def execute_write(self, operation, **kwargs):
        with Session(self.engine) as db:
            result = await operation(AsyncSessionAdapter(db))
            if self.fail_save and operation.__name__ == "save":
                raise RuntimeError("crash before outcome commit")
            db.commit()
            return result


@pytest.fixture
def isolated():
    engine = create_engine("sqlite://")
    @event.listens_for(engine, "connect")
    def add_now(connection, _):
        connection.create_function("now", 0, lambda: datetime.utcnow().isoformat())
    Base.metadata.create_all(engine)
    orchestrator = NewsOrchestrator.__new__(NewsOrchestrator)
    orchestrator.db_queue_manager = IsolatedQueue(engine)
    orchestrator.article_limiter = SimpleNamespace(
        is_enabled=lambda: False, apply_date_filters_to_query=lambda query: query)
    orchestrator.ai_processor = SimpleNamespace(process_article_outcome=AsyncMock(
        return_value=ProcessingOutcome("completed", {"summary": "A successful summary", "summary_processed": True,
                                                     "category_processed": True, "ad_processed": True})))
    yield orchestrator, engine
    engine.dispose()


def add_articles(engine, count, **fields):
    with Session(engine) as db:
        for index in range(count):
            db.add(Article(title=f"Article {index}", url=f"https://example.org/{index}",
                           content="Article content", **fields))
        db.commit()


@pytest.mark.asyncio
async def test_batches_are_bounded_stable_and_skip_completed_history(isolated):
    orchestrator, engine = isolated
    add_articles(engine, 61)
    with Session(engine) as db:
        db.add(Article(title="Historical", url="https://example.org/old", content="Old content",
                       summary=None, processed=True, summary_processed=True, category_processed=True,
                       ad_processed=True, processing_state="completed"))
        db.commit()
    statements = []
    @event.listens_for(engine, "before_cursor_execute")
    def capture(_connection, _cursor, statement, parameters, _context, _executemany):
        if statement.startswith("SELECT") and "LEFT OUTER JOIN sources" in statement:
            statements.append((statement, parameters))
    result = await orchestrator._process_unprocessed_articles({"errors": []})
    assert result["articles_processed"] == 61
    assert orchestrator.ai_processor.process_article_outcome.await_count == 61
    assert len(statements) == 4
    assert all("LIMIT" in sql and "media_files" not in sql for sql, _ in statements)
    assert [call.args[0]["id"] for call in orchestrator.ai_processor.process_article_outcome.await_args_list] == list(range(1, 62))


@pytest.mark.asyncio
async def test_failed_outcome_write_has_durable_budget_and_restart_deadline(isolated):
    orchestrator, engine = isolated
    add_articles(engine, 1)
    orchestrator.db_queue_manager.fail_save = True
    stats = {"errors": []}
    await orchestrator._process_unprocessed_articles(stats)
    assert stats["errors"]
    with Session(engine) as db:
        article = db.get(Article, 1)
        assert article.processing_attempts == 1
        assert article.processing_state == "retry_wait"
        assert article.next_attempt_at > datetime.utcnow()
        assert article.summary is None
    orchestrator.db_queue_manager.fail_save = False
    await orchestrator._process_unprocessed_articles({"errors": []})
    assert orchestrator.ai_processor.process_article_outcome.await_count == 1
    with Session(engine) as db:
        db.execute(update(Article).values(processing_attempts=3, next_attempt_at=datetime.utcnow() - timedelta(seconds=1)))
        db.commit()
    await orchestrator._process_unprocessed_articles({"errors": []})
    with Session(engine) as db:
        assert db.get(Article, 1).processing_state == "exhausted"


@pytest.mark.asyncio
async def test_only_explicit_retry_selection_resets_budget(isolated):
    orchestrator, engine = isolated
    add_articles(engine, 2, processing_state="exhausted", processing_attempts=3)
    assert await orchestrator.retry_selected_articles([1]) == 1
    with Session(engine) as db:
        assert db.get(Article, 1).processing_state == "pending"
        assert db.get(Article, 1).processing_attempts == 0
        assert db.get(Article, 2).processing_state == "exhausted"
    with pytest.raises(ValueError):
        await orchestrator.retry_selected_articles([])


@pytest.mark.asyncio
async def test_explicit_force_preserves_saved_results_while_resetting_only_selected_flags(isolated):
    orchestrator, engine = isolated
    add_articles(engine, 2, summary='Saved valid summary', summary_processed=True,
                 category_processed=True, ad_processed=True, processing_state='completed')
    await orchestrator.retry_selected_articles([1])
    with Session(engine) as db:
        assert db.get(Article, 1).summary_processed is True
        assert db.get(Article, 1).summary == 'Saved valid summary'
    await orchestrator._reset_selected_articles([1], force=True)
    with Session(engine) as db:
        assert db.get(Article, 1).summary == 'Saved valid summary'
        assert db.get(Article, 1).summary_processed is False
        assert db.get(Article, 2).summary_processed is True
        assert db.get(Article, 2).processing_state == 'completed'


@pytest.mark.asyncio
async def test_category_writes_are_idempotent_after_explicit_retry(isolated):
    orchestrator, engine = isolated
    add_articles(engine, 1)
    with Session(engine) as db:
        db.add(Category(name="Tech", display_name="Tech"))
        db.commit()
    orchestrator.ai_processor.process_article_outcome.return_value = ProcessingOutcome(
        "completed", {"categories": [{"name": "Tech"}, {"name": "Tech"}],
                      "summary_processed": True, "category_processed": True, "ad_processed": True})
    with patch("news_aggregator.services.category_display_service.get_category_display_service", AsyncMock()):
        await orchestrator._process_unprocessed_articles({"errors": []})
        await orchestrator.retry_selected_articles([1])
        await orchestrator._process_unprocessed_articles({"errors": []})
    with Session(engine) as db:
        assert len(db.execute(select(ArticleCategory)).scalars().all()) == 1


@pytest.mark.asyncio
async def test_outcome_write_failure_cancels_and_awaits_other_batch_workers(isolated):
    orchestrator, engine = isolated
    add_articles(engine, 2)
    second_started = asyncio.Event()
    second_cancelled = asyncio.Event()
    async def process(article, stats):
        if article['id'] == 1:
            await second_started.wait()
            return ProcessingOutcome('completed')
        second_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            second_cancelled.set()
    orchestrator.ai_processor.process_article_outcome.side_effect = process
    orchestrator.db_queue_manager.fail_save = True
    await orchestrator._process_unprocessed_articles({'errors': []})
    assert second_cancelled.is_set()


@pytest.mark.asyncio
async def test_manual_reservation_prevents_duplicate_background_requests(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'test-only-local-admin')
    from fastapi import BackgroundTasks
    from news_aggregator.api.processing_router import trigger_processing
    from news_aggregator.orchestrator import release_processing_cycle
    background = BackgroundTasks()
    first = await trigger_processing(background)
    try:
        second = await trigger_processing(BackgroundTasks())
        assert first['status'] == 'running'
        assert second['status'] == 'busy'
        assert len(background.tasks) == 1
    finally:
        release_processing_cycle(background.tasks[0].args[0])
