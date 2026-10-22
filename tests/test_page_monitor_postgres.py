"""Opt-in Page Monitor Go to SourceManager PostgreSQL round-trip."""

import asyncio
import os
import uuid
from datetime import datetime

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.config import settings
from news_aggregator.models import Article, Source
from news_aggregator.services.source_manager import SourceManager
from news_aggregator.sources.page_monitor_source import PageMonitorConfig, PageMonitorSource


def local_test_url():
    url = os.environ.get('TEST_DATABASE_URL')
    if not url:
        pytest.skip('TEST_DATABASE_URL is required for isolated PostgreSQL tests')
    if make_url(url).host not in {'127.0.0.1', 'localhost'}:
        raise RuntimeError('These tests require a loopback-only PostgreSQL test DB')
    return url


@pytest.mark.asyncio
async def test_go_page_monitor_article_round_trips_through_source_manager(monkeypatch):
    url = local_test_url()
    schema = 'test_page_monitor_' + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={'server_settings': {'search_path': schema}})
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA {schema}'))
        async with engine.begin() as connection:
            await connection.run_sync(Source.__table__.create)
            await connection.run_sync(Article.__table__.create)

        monkeypatch.setattr(settings, 'page_monitor_source_mode', 'go')
        monkeypatch.setattr(settings, 'extraction_service_url', 'http://extractor:8080')
        monkeypatch.setattr(settings, 'extraction_service_token', SecretStr('private-test-token'))
        monkeypatch.setattr(settings, 'extraction_service_total_budget_seconds', 120.0)
        config = PageMonitorConfig('https://example.test/releases', 'Postgres fixture', use_browser=False)
        monitor = PageMonitorSource(config)
        monkeypatch.setattr(monitor, '_load_learned_structure', lambda: asyncio.sleep(0))

        class FixtureClient:
            def __init__(self, *args, **kwargs):
                pass
            async def fetch(self, sent_config, *, limit, timeout_seconds):
                assert sent_config.url == config.url and limit == config.max_articles_per_check
                return {
                    'outcome': 'success', 'snapshot_at': '2026-10-22T08:15:00Z',
                    'content_hash': 'a' * 32, 'selectors_used': ['article'],
                    'needs_selector_study': False,
                    'items': [{
                        'title': 'PostgreSQL fixture release', 'link': 'https://example.test/release/1',
                        'description': 'A bounded Go extracted summary.',
                        'published': '2026-10-22T08:15:00Z', 'content_type': 'general',
                        'id': 'fixture-id', 'source': config.name, 'source_url': config.url,
                    }],
                }

        monkeypatch.setattr('news_aggregator.sources.page_monitor_service_client.PageMonitorServiceClient', FixtureClient)
        expected = datetime(2026, 10, 22, 8, 15)
        async with factory() as db:
            source = Source(name=config.name, source_type='custom', url=config.url)
            db.add(source)
            await db.flush()
            raw = await monitor.fetch_articles()
            assert len(raw) == 1 and raw[0].published_at == expected and raw[0].published_at.tzinfo is None
            saved = await SourceManager().save_fetched_articles_with_sources(
                {config.name: raw}, {config.name: source}, db)
            await db.commit()
            stored = await db.get(Article, saved[config.name][0].id)
            assert stored.published_at == expected
            assert stored.published_at.tzinfo is None
            assert stored.title == raw[0].title and stored.url == raw[0].url
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        await admin.dispose()
