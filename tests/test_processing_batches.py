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


@pytest.mark.asyncio
@pytest.mark.parametrize('existing_image', [None, 'https://example.org/original.jpg'])
async def test_forced_selected_processing_persists_refreshed_body_and_missing_image(isolated, existing_image):
    from news_aggregator.processing.ai_processor import AIProcessor
    orchestrator, engine = isolated
    add_articles(engine, 1, summary='Saved summary', summary_processed=True,
                 category_processed=True, ad_processed=True, processing_state='completed',
                 image_url=existing_image)
    with Session(engine) as db:
        db.add(Category(name='Tech', display_name='Tech'))
        db.commit()
    processor = AIProcessor.__new__(AIProcessor)
    processor.ai_client = SimpleNamespace(
        analyze_article_complete=AsyncMock(return_value={
            'summary': 'Refreshed summary', 'categories': ['Tech'], 'is_advertisement': False}),
        get_article_summary_with_metadata=AsyncMock(side_effect=AssertionError('duplicate extraction')))
    orchestrator.ai_processor = processor
    fresh_content = 'Fresh article content with details that were missing from the old body. ' * 5
    extractor = SimpleNamespace(extract_article_content_with_metadata=AsyncMock(return_value={
        'content': fresh_content, 'image_url': 'https://example.org/refreshed.jpg'}))
    context = AsyncMock()
    context.__aenter__.return_value = extractor
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context), patch(
            'news_aggregator.services.category_display_service.get_category_display_service', AsyncMock()):
        result = await orchestrator.process_selected_articles([1], force=True)
    assert result['success'] is True
    extractor.extract_article_content_with_metadata.assert_awaited_once_with('https://example.org/0', retry_count=1)
    assert processor.ai_client.analyze_article_complete.await_args.args[1] == fresh_content
    processor.ai_client.get_article_summary_with_metadata.assert_not_awaited()
    with Session(engine) as db:
        article = db.get(Article, 1)
        assert article.content == fresh_content
        assert article.summary == 'Refreshed summary'
        assert article.image_url == (existing_image or 'https://example.org/refreshed.jpg')
        assert article.processing_state == 'completed'


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,expected_state', [('transport', 'retry_wait'), ('oversized', 'rejected')])
async def test_forced_refresh_failure_preserves_saved_body_summary_and_image(isolated, monkeypatch, failure, expected_state):
    from news_aggregator.config import settings
    from news_aggregator.processing.ai_processor import AIProcessor
    orchestrator, engine = isolated
    add_articles(engine, 1, summary='Saved summary', summary_processed=True,
                 category_processed=True, ad_processed=True, processing_state='completed',
                 image_url='https://example.org/original.jpg')
    processor = AIProcessor.__new__(AIProcessor)
    processor.ai_client = SimpleNamespace(analyze_article_complete=AsyncMock())
    orchestrator.ai_processor = processor
    monkeypatch.setattr(settings, 'extraction_text_max_chars', 100)
    extract = AsyncMock(side_effect=TimeoutError('unavailable')) if failure == 'transport' else AsyncMock(
        return_value={'content': 'x' * 101, 'image_url': 'https://example.org/new.jpg'})
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(extract_article_content_with_metadata=extract)
    with patch('news_aggregator.extraction.ContentExtractor', return_value=context):
        result = await orchestrator.process_selected_articles([1], force=True)
    assert result['success'] is (expected_state == 'rejected')
    assert result['settled'] is (expected_state == 'rejected')
    assert result['retryable'] is (expected_state == 'retry_wait')
    processor.ai_client.analyze_article_complete.assert_not_awaited()
    with Session(engine) as db:
        article = db.get(Article, 1)
        assert article.content == 'Article content'
        assert article.summary == 'Saved summary'
        assert article.image_url == 'https://example.org/original.jpg'
        assert article.processing_state == expected_state


@pytest.mark.asyncio
@pytest.mark.parametrize('mapping_error', [ValueError, LookupError])
async def test_category_mapping_error_retains_claim_without_cancelling_other_articles(isolated, mapping_error):
    orchestrator, engine = isolated
    add_articles(engine, 2)
    with Session(engine) as db:
        db.add(Category(name='Tech', display_name='Tech'))
        db.commit()
    orchestrator.ai_processor.process_article_outcome.return_value = ProcessingOutcome(
        'completed', {'summary': 'Successful summary', 'summary_processed': True,
                      'categories': [{'name': 'Tech'}], 'category_processed': True, 'ad_processed': True})
    save_categories = orchestrator._save_ai_categories_to_database
    async def map_categories(db, article_id, categories):
        await save_categories(db, article_id, categories)
        if article_id == 1:
            raise mapping_error('unsupported configured category')
    orchestrator._save_ai_categories_to_database = map_categories
    stats = {'errors': []}
    with patch('news_aggregator.services.category_display_service.get_category_display_service', AsyncMock()):
        result = await orchestrator._process_unprocessed_articles(stats)
    assert result['articles_processed'] == 1
    assert len(stats['errors']) == 1
    with Session(engine) as db:
        failed = db.get(Article, 1)
        assert failed.processing_state == 'retry_wait'
        assert failed.processing_attempts == 1
        assert failed.next_attempt_at > datetime.utcnow()
        assert failed.last_processing_error == 'category_mapping_failed'
        assert failed.summary is None
        assert db.get(Article, 2).processing_state == 'completed'
        categories = db.execute(select(ArticleCategory)).scalars().all()
        assert [category.article_id for category in categories] == [2]


@pytest.mark.asyncio
@pytest.mark.parametrize('ai_failed', [False, True])
async def test_legacy_incremental_uses_typed_summary_metadata_and_never_reports_failed_ai_as_success(isolated, ai_failed):
    from news_aggregator.processing.ai_processor import AIProcessor
    orchestrator, engine = isolated
    add_articles(engine, 1)
    with Session(engine) as db:
        db.add(Category(name='Tech', display_name='Tech'))
        db.commit()
    processor = AIProcessor.__new__(AIProcessor)
    analysis = {'_processing_error': 'APIError'} if ai_failed else {
        'summary': 'A valid generated summary', 'categories': ['Tech'],
        'publication_date': '2026-10-15', 'is_advertisement': False}
    processor.ai_client = SimpleNamespace(get_article_summary_with_metadata=AsyncMock(return_value=analysis))
    processor._save_article_fields = AsyncMock()
    stats = {'api_calls_made': 0}
    with patch('news_aggregator.orchestrator.get_database_queue', return_value=orchestrator.db_queue_manager), patch(
            'news_aggregator.orchestrator.get_article_limiter', return_value=orchestrator.article_limiter), patch(
            'news_aggregator.orchestrator.get_telegram_service') as telegram, patch(
            'news_aggregator.services.category_display_service.get_category_display_service', AsyncMock()):
        result = await processor.process_article_incremental({'id': 1}, stats)
    assert result['success'] is (not ai_failed)
    processor._save_article_fields.assert_not_awaited()
    telegram.assert_not_called()
    assert stats['api_calls_made'] == 1
    with Session(engine) as db:
        saved = db.get(Article, 1)
        assert saved.processing_state == ('retry_wait' if ai_failed else 'completed')
        assert saved.summary == (None if ai_failed else 'A valid generated summary')
        if not ai_failed:
            assert saved.published_at == datetime(2026, 10, 15)


@pytest.mark.asyncio
@pytest.mark.parametrize('state,reason,settled', [
    ('completed', None, True), ('rejected', 'advertisement', True),
    ('rejected', 'resource_limit', True), ('retry_wait', 'APIError', False),
    ('exhausted', 'APIError', False),
])
async def test_selected_api_reports_durable_terminal_outcomes_without_inflating_news_counts(
        isolated, monkeypatch, state, reason, settled):
    from unittest.mock import MagicMock
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'test-only-admin')
    from news_aggregator.api.articles_router import reprocess_article, ReprocessRequest
    orchestrator, engine = isolated
    add_articles(engine, 1)
    fields = {'is_advertisement': True, 'ad_processed': True} if reason == 'advertisement' else {}
    orchestrator.ai_processor.process_article_outcome.return_value = ProcessingOutcome(state, fields, reason)
    article = SimpleNamespace(
        id=1, title='Original title', content='Original content', url='https://example.org/0',
        summary=None, summary_processed=False, category_processed=False, ad_processed=False,
        processed=False, media_files=[], image_url=None, source=None)
    lookup = MagicMock()
    lookup.scalar_one_or_none.return_value = article
    request_db = SimpleNamespace(execute=AsyncMock(return_value=lookup), rollback=AsyncMock())
    with patch('news_aggregator.api.articles_router.NewsOrchestrator', return_value=orchestrator):
        result = await reprocess_article(1, ReprocessRequest(force=True), request_db)
    assert result['status'] == state
    assert result['settled'] is settled
    assert result['success'] is settled
    assert result['reason'] == reason
    assert result['retryable'] is (state == 'retry_wait')
    assert result['stats']['articles_processed'] == (1 if state == 'completed' else 0)
    if reason == 'advertisement':
        assert 'распознана как реклама' in result['message']
    elif reason == 'resource_limit':
        assert 'превышает допустимый размер' in result['message']
        assert 'повтор не запланирован' in result['message']
    elif state == 'exhausted':
        assert 'повтор остановлен' in result['message']
    elif state == 'retry_wait':
        assert 'попытка запланирована' in result['message']
    with Session(engine) as db:
        saved = db.get(Article, 1)
        assert saved.processing_state == state
        assert saved.next_attempt_at is None if state != 'retry_wait' else saved.next_attempt_at > datetime.utcnow()
        if reason == 'advertisement':
            assert saved.is_advertisement is True
