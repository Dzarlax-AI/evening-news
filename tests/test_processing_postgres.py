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
from news_aggregator.models import Article, ArticleCategory, Category, Source
from news_aggregator.migrations.article_processing_state import ArticleProcessingStateMigration
from news_aggregator.orchestrator import NewsOrchestrator
from news_aggregator.processing.processing_outcome import ProcessingOutcome
from news_aggregator.services.database_queue import DatabaseQueueManager
from news_aggregator.services.source_manager import SourceManager
from news_aggregator.sources.base import Article as FetchedArticle


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
async def test_telegram_go_adapter_timestamp_round_trips_as_naive_utc():
    from news_aggregator.telegram.telegram_source import TelegramSource

    url = local_test_url()
    schema = 'test_telegram_datetime_' + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={'server_settings': {'search_path': schema}})
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA {schema}'))
        async with engine.begin() as connection:
            await connection.run_sync(Source.__table__.create)
            await connection.run_sync(Article.__table__.create)

        item = TelegramSource._article_from_go({
            'title': 'Telegram test', 'url': 'https://t.me/test/1', 'content': 'Post body',
            'summary': None, 'published_at': '2026-10-22T10:15:00+02:00',
            'source_type': 'telegram', 'source_name': 'Test', 'image_url': None,
            'media_files': [], 'raw_data': {'telegram_url': 'https://t.me/test/1'},
        })
        expected = datetime(2026, 10, 22, 8, 15)
        assert item.published_at == expected
        assert item.published_at.tzinfo is None

        factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            source = Source(name='Test', source_type='telegram', url='https://t.me/test')
            db.add(source)
            await db.flush()
            saved = await SourceManager().save_fetched_articles_with_sources(
                {'Test': [item]}, {'Test': source}, db)
            assert len(saved['Test']) == 1
            await db.commit()
            stored = await db.get(Article, saved['Test'][0].id)
            assert stored.published_at == expected
            assert stored.published_at.tzinfo is None
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_source_manager_id_mapping_persists_same_named_sources_separately():
    url = local_test_url()
    schema = 'test_source_id_mapping_' + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={'server_settings': {'search_path': schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA {schema}'))
        async with engine.begin() as connection:
            await connection.run_sync(Source.__table__.create)
            await connection.run_sync(Article.__table__.create)

        manager = SourceManager()
        async with factory() as db:
            first = Source(name='Same name', source_type='rss', url='https://example.test/feed-a')
            second = Source(name='Same name', source_type='rss', url='https://example.test/feed-b')
            db.add_all([first, second])
            await db.flush()
            assert first.id != second.id
            raw = {
                first.id: [FetchedArticle(
                    title='Story A', url='https://example.test/story-a', content='First source story',
                )],
                second.id: [FetchedArticle(
                    title='Story B', url='https://example.test/story-b', content='Second source story',
                )],
            }
            saved = await manager.save_fetched_articles_with_sources(
                raw, {first.id: first, second.id: second}, db)
            await db.commit()
            rows = (await db.execute(select(Article.source_id, Article.title).order_by(Article.source_id))).all()
            assert rows == [(first.id, 'Story A'), (second.id, 'Story B')]
            assert len(saved[first.id]) == len(saved[second.id]) == 1
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        await admin.dispose()


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
