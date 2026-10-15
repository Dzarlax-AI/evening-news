"""Partial stage results are private to processing until publication is ready."""

from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from news_aggregator.models import Article, ArticleCategory, Category
from tests.test_processing_batches import isolated, AsyncSessionAdapter


@pytest.mark.asyncio
async def test_digest_and_search_exclude_incomplete_outcomes_but_keep_explicit_ad_view(isolated, monkeypatch):
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'test-only-publication')
    from news_aggregator import public
    orchestrator, engine = isolated
    with Session(engine) as db:
        category = Category(name='Tech', display_name='Tech')
        db.add(category)
        db.flush()
        for index, state in enumerate(('completed', 'pending', 'retry_wait', 'rejected', 'exhausted'), 1):
            db.add(Article(id=index, title=f'Publication fixture {state}',
                url=f'https://example.test/{index}', content='A source article.', summary='A saved partial summary.',
                fetched_at=datetime.utcnow(), processing_state=state,
                summary_processed=True, category_processed=True, ad_processed=state == 'completed',
                is_advertisement=False))
            db.add(ArticleCategory(article_id=index, category_id=category.id, confidence=0.9, ai_category='Tech'))
        db.add(Article(id=6, title='Publication fixture confirmed ad', url='https://example.test/6',
            summary='An ad summary.', content='A promotion.', processing_state='rejected',
            ad_processed=True, is_advertisement=True))
        db.commit()
    grouped = await orchestrator.db_queue_manager.execute_read(
        lambda db: orchestrator._group_articles_by_category(db, datetime.utcnow().date()))
    assert [item.id for item in grouped['Tech']] == [1]
    monkeypatch.setattr(public, 'get_database_queue', lambda: orchestrator.db_queue_manager)
    monkeypatch.setattr(public.templates, 'TemplateResponse', lambda request, template, context: context)
    digest = await public.public_digest_view(datetime.utcnow().date().isoformat(), object())
    assert digest['total_articles'] == 1
    assert [item['id'] for item in digest['articles_by_category']['Tech']] == [1]
    with Session(engine) as db:
        results = await public.search_articles(q='Publication', limit=20, offset=0, category=None,
            since_hours=None, sort='date', hide_ads=True, db=AsyncSessionAdapter(db))
        assert [item['id'] for item in results['articles']] == [1]
        assert results['pagination']['total'] == 1
        results = await public.search_articles(q='Publication', limit=20, offset=0, category=None,
            since_hours=None, sort='date', hide_ads=False, db=AsyncSessionAdapter(db))
        assert {item['id'] for item in results['articles']} == {1, 6}
