"""Opt-in PostgreSQL checks restricted to an explicitly supplied local test DB."""

import os
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.database import Base
from news_aggregator.models import Article, ArticleCategory, Category
from news_aggregator.migrations.article_processing_state import ArticleProcessingStateMigration
from news_aggregator.orchestrator import NewsOrchestrator
from news_aggregator.processing.processing_outcome import ProcessingOutcome
from news_aggregator.services.database_queue import DatabaseQueueManager


def local_test_url():
    url = os.environ.get('TEST_DATABASE_URL')
    if not url:
        pytest.skip('TEST_DATABASE_URL is required for isolated PostgreSQL tests')
    from sqlalchemy.engine import make_url
    parsed = make_url(url)
    if parsed.host not in {'127.0.0.1', 'localhost'}:
        raise RuntimeError('These tests require a loopback-only PostgreSQL test DB')
    return url


@pytest.mark.asyncio
async def test_postgres_legacy_backfill_is_idempotent_and_preserves_retry_budget():
    url = local_test_url()
    schema = 'test_processing_' + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={'server_settings': {'search_path': schema}})
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA {schema}'))
        async with engine.begin() as connection:
            await connection.execute(text('''CREATE TABLE articles (
                id SERIAL PRIMARY KEY, processed BOOLEAN, summary_processed BOOLEAN,
                category_processed BOOLEAN, ad_processed BOOLEAN, summary TEXT,
                content TEXT, is_advertisement BOOLEAN DEFAULT FALSE)'''))
            await connection.execute(text('''INSERT INTO articles
                (processed, summary_processed, category_processed, ad_processed, content)
                VALUES (TRUE, TRUE, TRUE, TRUE, 'Historical summaryless content'),
                       (NULL, NULL, NULL, NULL, 'Pending content'),
                       (TRUE, FALSE, TRUE, TRUE, 'Incomplete content')'''))
        factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        migration = ArticleProcessingStateMigration()
        async with factory() as db:
            assert await migration.check_needed(db)
            await migration.execute(db)
            rows = (await db.execute(text('SELECT processing_state, is_advertisement FROM articles ORDER BY id'))).all()
            assert rows == [('completed', False), ('pending', False), ('pending', False)]
            await db.execute(text("UPDATE articles SET processing_state='retry_wait', processing_attempts=2 WHERE id=2"))
            await db.commit()
            await migration.execute(db)
            assert (await db.execute(text('SELECT processing_state, processing_attempts FROM articles WHERE id=2'))).one() == ('retry_wait', 2)
            assert not await migration.check_needed(db)
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_postgres_crash_between_attempt_and_outcome_recovers_without_duplicate_categories():
    url = local_test_url()
    schema = 'test_processing_' + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={'server_settings': {'search_path': schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA {schema}'))
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as db:
            db.add(Article(title='Test article', url='https://example.org/test', content='Test body'))
            db.add(Category(name='Tech', display_name='Tech'))
            await db.commit()
        value = NewsOrchestrator.__new__(NewsOrchestrator)
        value.db_queue_manager = DatabaseQueueManager()
        value.article_limiter = SimpleNamespace(is_enabled=lambda: False, apply_date_filters_to_query=lambda query: query)
        value.ai_processor = SimpleNamespace(process_article_outcome=AsyncMock(return_value=ProcessingOutcome(
            'completed', {'summary': 'Successful summary', 'summary_processed': True,
                          'categories': [{'name': 'Tech'}, {'name': 'Tech'}],
                          'category_processed': True, 'ad_processed': True})))
        original_save = value._save_ai_categories_to_database
        async def fail_after_category_writes(*args, **kwargs):
            await original_save(*args, **kwargs)
            raise RuntimeError('simulated outcome write interruption')
        with patch('news_aggregator.services.database_queue.AsyncSessionLocal', factory), patch(
            'news_aggregator.services.category_display_service.get_category_display_service', AsyncMock()):
            value._save_ai_categories_to_database = fail_after_category_writes
            stats = {'errors': []}
            with pytest.raises(ExceptionGroup):
                await value._process_unprocessed_articles(stats)
            assert stats['errors']
            async with factory() as db:
                article = await db.get(Article, 1)
                assert article.processing_attempts == 1
                assert article.processing_state == 'retry_wait'
                assert article.summary is None
                assert (await db.execute(select(ArticleCategory))).scalars().all() == []
                article.next_attempt_at = datetime.utcnow() - timedelta(seconds=1)
                await db.commit()
            value._save_ai_categories_to_database = original_save
            result = await value._process_unprocessed_articles({'errors': []})
            assert result['articles_processed'] == 1
            async with factory() as db:
                article = await db.get(Article, 1)
                assert article.processing_state == 'completed'
                assert article.processing_attempts == 2
                assert len((await db.execute(select(ArticleCategory))).scalars().all()) == 1
            assert (await value._process_unprocessed_articles({'errors': []}))['articles_processed'] == 0
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        await admin.dispose()
