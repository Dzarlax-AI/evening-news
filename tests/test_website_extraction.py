"""Input validation retained for the Go-backed Page Monitor adapter."""

import pytest

from news_aggregator.sources.page_monitor_source import PageMonitorConfig


@pytest.mark.parametrize("wait_timeout_ms", [0, -1, float("inf"), float("nan")])
def test_page_monitor_config_rejects_invalid_browser_timeout(wait_timeout_ms):
    with pytest.raises(ValueError, match="wait_timeout_ms"):
        PageMonitorConfig(
            url="https://example.com",
            name="Example",
            wait_timeout_ms=wait_timeout_ms,
        )


@pytest.mark.asyncio
async def test_page_monitor_selector_learning_callback_persists_captured_structure(monkeypatch):
    from types import SimpleNamespace

    from news_aggregator.sources.page_monitor_source import PageMonitorSource

    analysis = SimpleNamespace(
        content_selectors=['.story'], title_selectors=['.headline'], date_selectors=['time']
    )
    observed = {}

    class Analyzer:
        async def analyze_page_structure(self, url, html, context):
            observed['analysis_input'] = (url, html, context)
            return analysis

    class Memory:
        async def record_page_structure(self, domain, result):
            observed['recorded'] = (domain, result)

    async def get_analyzer():
        return Analyzer()

    async def get_memory():
        return Memory()

    monkeypatch.setattr('news_aggregator.sources.ai_page_analyzer.get_ai_page_analyzer', get_analyzer)
    monkeypatch.setattr('news_aggregator.services.extraction_memory.get_extraction_memory', get_memory)
    monitor = PageMonitorSource(PageMonitorConfig(url='https://example.test/news', name='Example'))

    assert await monitor._study_source_structure('<html>captured fixture</html>') is True
    assert observed['analysis_input'] == (
        'https://example.test/news', '<html>captured fixture</html>', 'news'
    )
    assert observed['recorded'] == ('example.test', analysis)
    assert monitor.learned_container_selectors == ['.story']
    assert monitor.learned_title_selectors == ['.headline']
    assert monitor.learned_date_selectors == ['time']
