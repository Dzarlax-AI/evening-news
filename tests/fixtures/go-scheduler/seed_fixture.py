"""Seed synthetic input in the disposable fixture database."""

import asyncio
import json
from datetime import datetime
from sqlalchemy import select
from news_aggregator.database import AsyncSessionLocal
from news_aggregator.models import Article, Category, Source


async def main():
    async with AsyncSessionLocal() as session:
        category = await session.scalar(select(Category).where(Category.name == "Tech"))
        if category is None:
            session.add(Category(name="Tech", display_name="Technology", description="Fixture category",
                                 color="#334155"))
        source = await session.scalar(select(Source).where(Source.name == "Go Scheduler Fixture RSS"))
        if source is None:
            source = Source(name="Go Scheduler Fixture RSS", source_type="rss",
                            url="https://fixture.invalid/feed.xml", enabled=True,
                            config={"limit": 1}, fetch_interval=3600)
            session.add(source)
            await session.flush()
        article = await session.scalar(select(Article).where(
            Article.url == "https://fixture.invalid/articles/one"))
        if article is None:
            session.add(Article(
                source_id=source.id,
                title="Fixture technology article",
                url="https://fixture.invalid/articles/one",
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
        await session.commit()
        await session.refresh(source)
        print(json.dumps({"source_id": source.id, "category": "Tech"}))


if __name__ == "__main__":
    asyncio.run(main())
