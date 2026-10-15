"""Public feeds expose only completed work and preserve explicit ad views."""

from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from news_aggregator.database import Base
from news_aggregator.models import Article, ArticleCategory, Category, Source
from news_aggregator.services.data_service import DataService


class AsyncSessionAdapter:
    """Run production async query code against a real synchronous SQLite DB."""

    def __init__(self, session):
        self.session = session

    async def execute(self, statement, params=None):
        return self.session.execute(statement, params or {})


@pytest.fixture
def publication_db(monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'publication-feed-test-only')
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        source = Source(name="Fixture source", source_type="rss", url="https://example.test")
        world = Category(name="world", display_name="World")
        ads = Category(name="advertisements", display_name="Advertisements")
        session.add_all([source, world, ads])
        session.flush()

        cases = [
            # state, is_ad, ad_processed, title, include_ad_category
            ("pending", False, False, "pending article", False),
            ("retry_wait", False, False, "retry article", False),
            ("completed", False, True, "completed news", False),
            ("completed", True, True, "completed ad", True),
            ("rejected", True, True, "classified rejected ad", True),
            ("rejected", False, False, "rejected article", False),
            ("exhausted", False, True, "exhausted article", False),
            ("retry_wait", True, False, "unknown ad article", True),
        ]
        for index, (state, is_ad, ad_processed, title, include_ad_category) in enumerate(cases, 1):
            article = Article(
                source=source,
                title=title,
                url=f"https://example.test/article-{index}",
                content=f"Body for {title}",
                summary=f"Summary for {title}",
                processing_state=state,
                processing_attempts=1 if state == "retry_wait" else 0,
                is_advertisement=is_ad,
                ad_processed=ad_processed,
                published_at=datetime(2026, 10, 15),
                fetched_at=datetime(2026, 10, 15),
            )
            session.add(article)
            session.flush()
            session.add(ArticleCategory(
                article=article,
                category=ads if include_ad_category else world,
                ai_category="Advertisements" if include_ad_category else "World",
                confidence=0.9,
            ))
        session.commit()

    with Session(engine) as session:
        yield AsyncSessionAdapter(session)
    engine.dispose()


@pytest.mark.asyncio
async def test_json_and_rss_feeds_only_publish_completed_articles(publication_db):
    from news_aggregator.api.feed_router import get_main_feed, get_rss_feed
    json_feed = await get_main_feed(
        limit=100, offset=0, since_hours=None, category=None, db=publication_db
    )
    assert {item["title"] for item in json_feed["items"]} == {
        "completed news", "completed ad",
    }

    explicit_ads_feed = await get_main_feed(
        limit=100, offset=0, since_hours=None, category="advertisements", db=publication_db
    )
    assert {item["title"] for item in explicit_ads_feed["items"]} == {
        "completed ad", "classified rejected ad",
    }

    rss_response = await get_rss_feed(limit=100, db=publication_db)
    rss = rss_response.body.decode("utf-8")
    assert "completed news" in rss
    assert "completed ad" not in rss
    for unpublished in (
        "pending article", "retry article", "classified rejected ad",
        "rejected article", "exhausted article", "unknown ad article",
    ):
        assert unpublished not in rss


@pytest.mark.asyncio
async def test_explicit_ad_view_keeps_only_completed_or_confirmed_rejected_ads(publication_db):
    data_service = DataService(publication_db)
    explicit_ads = await data_service.get_articles_feed(
        limit=100, category="advertisements", hide_ads=True
    )
    assert {article["title"] for article in explicit_ads} == {
        "completed ad", "classified rejected ad",
    }

    visible_with_ads = await data_service.get_articles_feed(limit=100, hide_ads=False)
    assert {article["title"] for article in visible_with_ads} == {
        "completed news", "completed ad", "classified rejected ad",
    }
    assert "unknown ad article" not in {article["title"] for article in visible_with_ads}


@pytest.mark.asyncio
async def test_public_article_by_id_rejects_unpublished_work_but_allows_admin_preview(publication_db):
    service = DataService(publication_db)
    assert await service.get_article_by_id(2) is None
    assert (await service.get_article_by_id(2, include_unpublished=True))['title'] == 'retry article'
    assert (await service.get_article_by_id(3))['title'] == 'completed news'
    assert (await service.get_article_by_id(5))['title'] == 'classified rejected ad'


@pytest.mark.asyncio
async def test_public_modal_only_bypasses_publication_for_authenticated_admin(publication_db, monkeypatch):
    from fastapi import HTTPException
    from news_aggregator import public
    service = DataService(publication_db)
    monkeypatch.setattr(public, '_has_public_admin_access', lambda request: False)
    with pytest.raises(HTTPException) as error:
        await public.get_public_article(2, object(), service)
    assert error.value.status_code == 404
    monkeypatch.setattr(public, '_has_public_admin_access', lambda request: True)
    result = await public.get_public_article(2, object(), service)
    assert result['id'] == 2


@pytest.mark.asyncio
async def test_data_service_feed_and_counts_exclude_partial_or_terminal_work(publication_db):
    data_service = DataService(publication_db)
    news_feed = await data_service.get_articles_feed(limit=100, hide_ads=True)
    assert {article["title"] for article in news_feed} == {"completed news"}

    category_stats = await data_service.get_categories_stats(hide_ads=True)
    assert category_stats["total"] == 1
    assert category_stats["categories"]["world"] == 1
    assert "advertisements" not in category_stats["categories"]

    source_stats = await data_service.get_sources_stats(hide_ads=True)
    assert source_stats["total"] == 1
    assert source_stats["sources"] == {"1": 1}

    ads_inclusive_categories = await data_service.get_categories_stats(hide_ads=False)
    assert ads_inclusive_categories["total"] == 3
    assert ads_inclusive_categories["categories"]["world"] == 1
    assert ads_inclusive_categories["categories"]["advertisements"] == 2

    ads_inclusive_sources = await data_service.get_sources_stats(hide_ads=False)
    assert ads_inclusive_sources["total"] == 3
    assert ads_inclusive_sources["sources"] == {"1": 3}
