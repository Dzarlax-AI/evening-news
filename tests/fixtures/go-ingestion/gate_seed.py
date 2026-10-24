"""Seed three synthetic sources in the disposable News fixture database."""

import asyncio
import json
from sqlalchemy import select, update
from news_aggregator.database import AsyncSessionLocal
from news_aggregator.models import Article, Category, Source


async def main():
    sources = [
        Source(name="Go Ingestion Gate RSS", source_type="rss",
               url="https://rss.fixture.test/rss.xml", enabled=True,
               config={"limit": 2}, fetch_interval=3600),
        Source(name="Go Ingestion Gate Telegram", source_type="telegram",
               url="https://t.me/s/fixture_channel", enabled=True,
               config={"limit": 2}, fetch_interval=3600),
        Source(name="Go Ingestion Gate Page Monitor", source_type="custom",
               url="https://pm.fixture.test/news", enabled=True,
               config={"article_selectors": ["main"], "title_selectors": [], "date_selectors": [],
                       "link_selectors": [], "enable_ai_analysis": True,
                       "max_articles_per_check": 2, "use_browser": False,
                       "wait_for_js": False}, fetch_interval=3600),
    ]
    async with AsyncSessionLocal() as session:
        await session.execute(update(Source).values(enabled=False))
        category = await session.scalar(select(Category).where(Category.name == "Tech"))
        if category is None:
            session.add(Category(name="Tech", display_name="Technology",
                                 description="Synthetic gate category", color="#334155"))
        existing = set((await session.scalars(select(Source.name))).all())
        for source in sources:
            if source.name not in existing:
                session.add(source)
        await session.flush()
        await session.execute(update(Source).where(Source.name.in_([source.name for source in sources]))
                              .values(enabled=True))
        rss_source = await session.scalar(select(Source).where(Source.name == "Go Ingestion Gate RSS"))
        if rss_source is None:
            raise RuntimeError("synthetic RSS source was not seeded")
        duplicate_url = "http://rss.fixture.test/story/one/1"
        duplicate = await session.scalar(select(Article).where(Article.url == duplicate_url))
        if duplicate is None:
            session.add(Article(source_id=rss_source.id, title="Existing Gate RSS Metadata",
                                url=duplicate_url, content="Pre-existing fixture content",
                                summary=None, processed=False, hash_content=None))
        await session.commit()
        rows = (await session.execute(select(Source.id, Source.name)
                                      .where(Source.name.like("Go Ingestion Gate %")))).all()
    print(json.dumps({"source_ids": {name: source_id for source_id, name in rows}}, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
