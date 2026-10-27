"""Partial stage results are private to processing until publication is ready."""

from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from news_aggregator.models import Article, ArticleCategory, Category
from news_aggregator.services.domain_operations import _group_articles_by_category
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
    grouped = await queue.execute_read(lambda db: _group_articles_by_category(db))
    assert [item.id for item in grouped['Tech']] == [1]
