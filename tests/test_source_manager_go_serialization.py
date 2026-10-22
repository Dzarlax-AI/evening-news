import asyncio
from types import SimpleNamespace

from news_aggregator.config import settings
from news_aggregator.services.source_manager import SourceManager
from news_aggregator.sources.base import Article, SourceType


def source(source_id, name, source_type):
    return SimpleNamespace(id=source_id, name=name, source_type=source_type)


class TrackingSource:
    def __init__(self, name, *, active=None, peak=None, started=None, duration=0.03):
        self.name = name
        self.active = active
        self.peak = peak
        self.started = started
        self.duration = duration

    async def fetch_articles(self):
        if self.active is not None:
            self.active[0] += 1
            self.peak[0] = max(self.peak[0], self.active[0])
            if self.started is not None:
                self.started.set()
        try:
            await asyncio.sleep(self.duration)
            yield Article(title=self.name, url=f'https://example.test/{self.name}')
        finally:
            if self.active is not None:
                self.active[0] -= 1


def test_go_backed_sources_serialize_as_a_batch_while_rss_runs_in_parallel(monkeypatch):
    monkeypatch.setattr(settings, 'page_monitor_source_mode', 'go')
    monkeypatch.setattr(settings, 'telegram_source_mode', 'go')
    manager = SourceManager()

    active_go = [0]
    peak_go = [0]
    first_go_started = asyncio.Event()
    rss_overlapped_go = []
    instances = {}
    go_sources = [
        source(1, 'custom-one', SourceType.CUSTOM),
        source(2, 'custom-two', 'custom'),
        source(3, 'custom-three', SourceType.CUSTOM),
        source(4, 'telegram-one', SourceType.TELEGRAM),
    ]
    for item in go_sources:
        instances[item.id] = TrackingSource(item.name, active=active_go, peak=peak_go,
                                             started=first_go_started)

    class RSSSource:
        async def fetch_articles(self):
            await first_go_started.wait()
            rss_overlapped_go.append(active_go[0] > 0)
            yield Article(title='rss-one', url='https://example.test/rss-one')

    instances[5] = RSSSource()
    rss_source = source(5, 'rss', SourceType.RSS)

    async def get_instance(item):
        return instances[item.id]

    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    results = asyncio.run(manager.fetch_from_all_sources_no_db(
        [*go_sources, rss_source], per_source_timeout=0.08))

    assert peak_go == [1]
    assert rss_overlapped_go == [True]
    assert {name: [article.title for article in articles] for name, articles in results.items()} == {
        'custom-one': ['custom-one'],
        'custom-two': ['custom-two'],
        'custom-three': ['custom-three'],
        'telegram-one': ['telegram-one'],
        'rss': ['rss-one'],
    }


def test_python_mode_custom_sources_remain_parallel(monkeypatch):
    monkeypatch.setattr(settings, 'page_monitor_source_mode', 'python')
    monkeypatch.setattr(settings, 'telegram_source_mode', 'python')
    manager = SourceManager()
    active = [0]
    peak = [0]
    items = [source(i, f'custom-{i}', SourceType.CUSTOM) for i in range(1, 4)]
    instances = {item.id: TrackingSource(item.name, active=active, peak=peak) for item in items}

    async def get_instance(item):
        return instances[item.id]

    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    results = asyncio.run(manager.fetch_from_all_sources_no_db(items))

    assert peak[0] > 1
    assert all(len(articles) == 1 for articles in results.values())


def test_go_timeout_starts_after_shared_semaphore_admission(monkeypatch):
    monkeypatch.setattr(settings, 'page_monitor_source_mode', 'go')
    monkeypatch.setattr(settings, 'telegram_source_mode', 'go')
    manager = SourceManager()
    go_active = [0]
    go_peak = [0]
    go_started = []

    class TimedSource:
        def __init__(self, name, delay, *, is_go=False):
            self.name = name
            self.delay = delay
            self.is_go = is_go

        async def fetch_articles(self):
            if self.is_go:
                go_active[0] += 1
                go_peak[0] = max(go_peak[0], go_active[0])
                go_started.append(asyncio.get_running_loop().time())
            try:
                await asyncio.sleep(self.delay)
                yield Article(title=self.name, url=f'https://example.test/{self.name}')
            finally:
                if self.is_go:
                    go_active[0] -= 1

    items = [source(1, 'rss-one', SourceType.RSS), source(2, 'rss-two', SourceType.RSS),
             source(3, 'custom-one', SourceType.CUSTOM), source(4, 'telegram-one', SourceType.TELEGRAM)]
    instances = {
        1: TimedSource('rss-one', 0.09),
        2: TimedSource('rss-two', 0.09),
        3: TimedSource('custom-one', 0.09, is_go=True),
        4: TimedSource('telegram-one', 0.09, is_go=True),
    }

    async def get_instance(item):
        return instances[item.id]

    monkeypatch.setattr(manager, 'get_source_instance', get_instance)
    results = asyncio.run(manager.fetch_from_all_sources_no_db(
        items, max_concurrent=2, per_source_timeout=0.15))

    assert go_peak == [1]
    assert len(go_started) == 2
    assert all(len(results[item.name]) == 1 for item in items)


def test_cancelling_go_fetch_releases_admission_contexts(monkeypatch):
    monkeypatch.setattr(settings, 'page_monitor_source_mode', 'go')
    manager = SourceManager()
    entered = asyncio.Event()
    cleaned_up = asyncio.Event()

    class BlockingSource:
        async def fetch_articles(self):
            entered.set()
            try:
                await asyncio.Future()
                yield Article(title='unreachable', url='https://example.test/unreachable')
            finally:
                cleaned_up.set()

    item = source(1, 'custom-one', SourceType.CUSTOM)
    instances = {item.id: BlockingSource()}

    async def get_instance(source_item):
        return instances[source_item.id]

    monkeypatch.setattr(manager, 'get_source_instance', get_instance)

    async def run():
        task = asyncio.create_task(manager.fetch_from_all_sources_no_db(
            [item], max_concurrent=1, per_source_timeout=5))
        await entered.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert cleaned_up.is_set()

        instances[item.id] = TrackingSource('custom-one', duration=0)
        results = await manager.fetch_from_all_sources_no_db(
            [item], max_concurrent=1, per_source_timeout=1)
        assert [article.title for article in results['custom-one']] == ['custom-one']

    asyncio.run(run())
