"""Historical Telegram media refresh stays independent of channel collection."""

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from news_aggregator.database import Base
from news_aggregator.models import Article, Source
from news_aggregator.services import article_media_reextract as refresh


@pytest.mark.asyncio
@pytest.mark.parametrize('html,status,expected', [
    ('<div data-post="fixture/999"><a class="tgme_widget_message_photo_wrap" '
     'style="background-image:url(https://cdn.example/new.jpg)"></a></div>'
     '<div data-post="fixture/1"><a class="tgme_widget_message_photo_wrap" '
     'style="background-image:url(https://cdn.example/old.jpg)"></a></div>',
     200, 'https://cdn.example/old.jpg'),
    ('<div data-post="fixture/999">latest only</div>', 200, None),
    ('<div data-post="fixture/1">no media</div>', 200, []),
    ('<div data-post="fixture/1">redirect</div>', 302, None),
])
async def test_fetches_exact_historical_post_without_redirects(monkeypatch, html, status, expected):
    response = SimpleNamespace(status=status)
    class ResponseContext:
        async def __aenter__(self):
            return response
        async def __aexit__(self, *_):
            pass
    class Client:
        def __init__(self, *, timeout):
            assert timeout.total == 30
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
        def get(self, url, **kwargs):
            assert url == 'https://t.me/s/fixture/1'
            assert kwargs['allow_redirects'] is False
            assert kwargs['auto_decompress'] is False
            return ResponseContext()
    async def read(actual):
        assert actual is response
        return html
    monkeypatch.setattr(refresh.aiohttp, 'ClientSession', Client)
    monkeypatch.setattr(refresh, 'read_page_text', read)
    media = await refresh._fetch_post_media('https://t.me/fixture/1', 'https://t.me/s/fixture')
    if isinstance(expected, str):
        assert any(item['url'] == expected for item in media)
        assert all(item['url'] != 'https://cdn.example/new.jpg' for item in media)
    else:
        assert media == expected


@pytest.mark.parametrize('url', [
    'https://evil.test/fixture/1', 'https://t.me@evil.test/fixture/1',
    'https://t.me/other/1', 'https://t.me/fixture/1?single',
    'https://t.me/fixture/1#fragment', 'https://t.me/fixture/0',
    'https://t.me/fixture/1/extra', 'https://t.me:bad/fixture/1',
])
def test_rejects_nonmatching_or_unsafe_post_urls(url):
    assert refresh._post_target(url, 'https://t.me/fixture') is None


@pytest.mark.asyncio
@pytest.mark.parametrize('result', [None, [], [{'type': 'image', 'url': 'https://cdn.example/fresh.jpg'}]])
async def test_refresh_closes_read_and_preserves_media_on_missing_post(monkeypatch, result):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        source = Source(name='Fixture', source_type='telegram', url='https://t.me/fixture', enabled=False)
        article = Article(source=source, title='Old post', url='https://t.me/fixture/1',
                          media_files=[{'type': 'image', 'url': 'https://cdn.example/stored.jpg'}],
                          image_url='https://cdn.example/stored.jpg')
        session.add(article)
        session.commit()
        article_id = article.id
    opened = []
    class AsyncSession:
        async def __aenter__(self):
            self.session = Session(engine)
            opened.append(self)
            return self
        async def __aexit__(self, *_):
            self.session.close()
            opened.remove(self)
        async def scalar(self, query):
            return self.session.scalar(query)
        async def get(self, *args):
            return self.session.get(*args)
        async def execute(self, query):
            return self.session.execute(query)
        async def commit(self):
            self.session.commit()
    async def fetch(url, source_url):
        assert not opened
        assert url == 'https://t.me/fixture/1'
        return result
    monkeypatch.setattr(refresh, 'AsyncSessionLocal', AsyncSession)
    monkeypatch.setattr(refresh, '_fetch_post_media', fetch)
    assert await refresh.reextract_article_media(article_id) is (result is not None)
    with Session(engine) as session:
        article = session.get(Article, article_id)
        if result is None:
            assert article.image_url == 'https://cdn.example/stored.jpg'
            assert article.media_files[0]['url'] == article.image_url
        else:
            assert article.media_files == result
            assert article.image_url == (result[0]['url'] if result else None)
        assert article.source.enabled is False
    engine.dispose()


@pytest.mark.asyncio
async def test_embedded_route_releases_request_read_before_media_network(monkeypatch):
    from unittest.mock import AsyncMock
    from news_aggregator.config import settings
    monkeypatch.setattr(settings, 'admin_password', 'fixture-admin-password')
    monkeypatch.setattr(settings, 'background_execution_mode', 'embedded')
    import importlib
    route = importlib.import_module('news_aggregator.api.articles_router')
    row = SimpleNamespace(source=SimpleNamespace(source_type='telegram'))
    db = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: row)),
        rollback=AsyncMock())
    async def fetch(article_id):
        db.rollback.assert_awaited_once()
        return True
    runner = SimpleNamespace(process_selected_articles=AsyncMock(return_value={
        'success': True, 'status': 'completed'}))
    monkeypatch.setattr(route, 'NewsOrchestrator', lambda: runner)
    monkeypatch.setattr(route, 'reserve_processing_cycle', lambda: object())
    monkeypatch.setattr(route, 'release_processing_cycle', lambda *_: None)
    monkeypatch.setattr(refresh, 'reextract_article_media', fetch)
    result = await route.reprocess_article(1, route.ReprocessRequest(reextract_media=True), db)
    assert result['success'] is True
