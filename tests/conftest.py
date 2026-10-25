"""Shared isolated PostgreSQL fixture for executor contract tests."""

import os
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

from news_aggregator.migrations.background_jobs import BackgroundJobsMigration
from news_aggregator.migrations.job_executor import JobExecutorMigration
from news_aggregator.migrations.scheduled_job_metadata import ScheduledJobMetadataMigration
from news_aggregator.models import (
    ArticleIngestionRun, ArticleIngestionSourceRun, ScheduleSettings, Setting,
)
from news_aggregator.database import FencedAsyncSession


@pytest.fixture
async def executor_pg():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for isolated PostgreSQL tests")
    if make_url(url).host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("These tests require a loopback-only PostgreSQL test DB")
    schema = "test_executor_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = sessionmaker(engine, class_=FencedAsyncSession, expire_on_commit=False)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with factory() as session:
            await BackgroundJobsMigration().execute(session)
            await ScheduledJobMetadataMigration().execute(session)
            migration = JobExecutorMigration()
            await migration.execute(session)
            assert not await migration.check_needed(session)
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync: ScheduleSettings.__table__.create(sync, checkfirst=True))
            await conn.run_sync(lambda sync: Setting.__table__.create(sync, checkfirst=True))
            await conn.run_sync(lambda sync: ArticleIngestionRun.__table__.create(sync, checkfirst=True))
            await conn.run_sync(lambda sync: ArticleIngestionSourceRun.__table__.create(sync, checkfirst=True))
        yield engine, factory, schema
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()
