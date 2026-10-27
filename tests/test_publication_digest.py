"""Partial stage results are private to processing until publication is ready."""

from datetime import date, datetime

import pytest
from sqlalchemy.orm import Session

from news_aggregator.models import Article, ArticleCategory, Category, Source
from news_aggregator.services.editorial_digest import _fetch_snapshot_rows, _snapshot_articles
from .processing_test_helpers import AsyncSessionAdapter, isolated_publication_database


@pytest.fixture
def isolated():
    queue, engine = isolated_publication_database()
    yield queue, engine
    engine.dispose()


@pytest.mark.asyncio
async def test_digest_excludes_incomplete_outcomes(isolated):
    queue, engine = isolated
    with Session(engine) as db:
        source = Source(name='Fixture Source', source_type='rss', url='https://feed.example/test')
        category = Category(name='Tech', display_name='Tech')
        db.add_all([source, category])
        db.flush()
        for index, state in enumerate(('completed', 'pending', 'retry_wait', 'rejected', 'exhausted'), 1):
            db.add(Article(id=index, title=f'Publication fixture {state}',
                url=f'https://example.test/{index}', content='A source article.', summary='A saved partial summary.',
                source_id=source.id, fetched_at=datetime.utcnow(), processing_state=state,
                summary_processed=True, category_processed=True, ad_processed=state == 'completed',
                is_advertisement=False))
            db.add(ArticleCategory(article_id=index, category_id=category.id, confidence=0.9, ai_category='Tech'))
        db.add(Article(id=6, title='Publication fixture confirmed ad', url='https://example.test/6',
            summary='An ad summary.', content='A promotion.', processing_state='rejected',
            source_id=source.id, ad_processed=True, is_advertisement=True))
        db.commit()

    async def snapshot_articles(db):
        rows = await _fetch_snapshot_rows(db, date.today())
        return _snapshot_articles(rows)

    selected = await queue.execute_read(snapshot_articles)
    assert [item['id'] for item in selected] == ['1']
    assert selected[0]['category_name'] == 'Технологии'
