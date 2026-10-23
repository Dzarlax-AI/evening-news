"""Seed synthetic input in the disposable fixture database."""

import asyncio
import json
from sqlalchemy import select
from news_aggregator.database import AsyncSessionLocal
from news_aggregator.models import Category, Source


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
        await session.commit()
        await session.refresh(source)
        print(json.dumps({"source_id": source.id, "category": "Tech"}))


if __name__ == "__main__":
    asyncio.run(main())
