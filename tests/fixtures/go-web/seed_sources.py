"""Seed eight synthetic sources in the isolated web acceptance database."""

import asyncio
import json

from sqlalchemy import select, update

from news_aggregator.database import AsyncSessionLocal
from news_aggregator.models import Category, Source


async def main():
    sources = []
    for index in range(1, 5):
        edition = "one" if index % 2 else "two"
        sources.append(Source(
            name=f"Go Web Gate RSS {index}", source_type="rss",
            url=f"https://rss.fixture.test/rss.xml?edition={edition}", enabled=True,
            config={"limit": 2}, fetch_interval=3600,
        ))
    sources.append(Source(
        name="Go Web Gate Telegram", source_type="telegram",
        url="https://t.me/s/fixture_channel", enabled=True,
        config={"limit": 2}, fetch_interval=3600,
    ))
    for index in range(1, 4):
        sources.append(Source(
            name=f"Go Web Gate Page Monitor {index}", source_type="custom",
            url="https://pm.fixture.test/news", enabled=True,
            config={"article_selectors": ["main"], "title_selectors": [],
                    "date_selectors": [], "link_selectors": [], "enable_ai_analysis": True,
                    "max_articles_per_check": 2, "use_browser": False,
                    "wait_for_js": False}, fetch_interval=3600,
        ))
    async with AsyncSessionLocal() as session:
        await session.execute(update(Source).values(enabled=False))
        category = await session.scalar(select(Category).where(Category.name == "Tech"))
        if category is None:
            session.add(Category(name="Tech", display_name="Technology",
                                 description="Synthetic Go web gate category", color="#334155"))
        existing = set((await session.scalars(select(Source.name))).all())
        for source in sources:
            if source.name not in existing:
                session.add(source)
        await session.flush()
        names = [source.name for source in sources]
        await session.execute(update(Source).where(Source.name.in_(names)).values(enabled=True))
        await session.commit()
        rows = (await session.execute(select(Source.id, Source.name).where(Source.name.in_(names)))).all()
    print(json.dumps({"source_ids": {name: source_id for source_id, name in rows}}, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
