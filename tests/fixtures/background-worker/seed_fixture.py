"""Seed a single synthetic RSS source/category into the disposable gate database."""

import asyncio
import json
from datetime import datetime, timedelta

from sqlalchemy import select
from news_aggregator.database import AsyncSessionLocal
from news_aggregator.models import Article, Category, ScheduleSettings, Source


async def main():
    async with AsyncSessionLocal() as session:
        category = await session.scalar(select(Category).where(Category.name == "Tech"))
        if category is None:
            session.add(Category(name="Tech", display_name="Technology", description="Fixture category",
                                 color="#334155"))
        source = await session.scalar(select(Source).where(Source.name == "Background Gate RSS"))
        if source is None:
            source = Source(name="Background Gate RSS", source_type="rss",
                            url="https://fixture.example.test/feed.xml", enabled=True,
                            config={"limit": 1}, fetch_interval=3600)
            session.add(source)
            await session.flush()
        article = await session.scalar(select(Article).where(
            Article.url == "https://fixture.example.test/articles/one"))
        if article is None:
            session.add(Article(
                source_id=source.id,
                title="Fixture technology article",
                url="https://fixture.example.test/articles/one",
                content="Short synthetic placeholder for the executor pipeline.",
                published_at=datetime.utcnow(),
                fetched_at=datetime.utcnow(),
                processing_state="pending",
                processing_attempts=0,
                summary_processed=False,
                category_processed=False,
                ad_processed=False,
                processed=False,
            ))
        schedule = await session.scalar(select(ScheduleSettings).where(
            ScheduleSettings.task_name == "news_processing"))
        if schedule is None:
            session.add(ScheduleSettings(
                task_name="news_processing", enabled=True, schedule_type="interval",
                hour=0, minute=0, weekdays=[], timezone="UTC",
                task_config={"interval_minutes": 60},
                next_run=datetime.utcnow() - timedelta(minutes=2), is_running=False,
            ))
        await session.commit()
        await session.refresh(source)
        print(json.dumps({"source_id": source.id, "category": "Tech"}))


if __name__ == "__main__":
    asyncio.run(main())
