import asyncio
import json
from datetime import datetime as RealDateTime
from pathlib import Path

from news_aggregator.sources import page_monitor_source as module


FIXTURE = Path(__file__).parent / "fixtures/page-monitor/list-parity.json"


def _extract_case(monkeypatch, case):
    frozen = RealDateTime.fromisoformat(case["now"])

    class FrozenDateTime(RealDateTime):
        @classmethod
        def utcnow(cls):
            return frozen

    monkeypatch.setattr(module, "datetime", FrozenDateTime)
    config = module.PageMonitorConfig(
        url=case["url"], name=case["source_name"],
        article_selectors=case["article_selectors"],
        title_selectors=case["title_selectors"],
        date_selectors=case["date_selectors"],
        link_selectors=case["link_selectors"],
        min_title_length=case["min_title_length"],
        max_articles_per_check=case["limit"],
        use_browser=False, enable_ai_analysis=False,
    )
    source = module.PageMonitorSource(config)
    source.learned_container_selectors = case["learned_container_selectors"]
    source.learned_title_selectors = case["learned_title_selectors"]
    source.learned_link_selectors = case["learned_link_selectors"]
    source.learned_date_selectors = case["learned_date_selectors"]
    source.learned_selectors = {
        selector: confidence for selector, confidence in case["learned_general_selectors"]
    }

    async def no_study(_articles, _html):
        return False

    source._detect_list_page_fallback = no_study
    result = asyncio.run(source._extract_articles_from_html(case["html"]))
    return [
        {key: (value.isoformat() if key == "published" else value)
         for key, value in item.items()}
        for item in result
    ]


def test_python_page_monitor_parser_matches_shared_frozen_oracle(monkeypatch):
    for case in json.loads(FIXTURE.read_text())["cases"]:
        assert _extract_case(monkeypatch, case) == case["expected"], case["name"]


def test_python_page_monitor_configured_link_selectors_are_currently_ignored(monkeypatch):
    case = {
        "url": "https://example.com/updates", "source_name": "Example",
        "article_selectors": ["article"], "title_selectors": ["h2"],
        "date_selectors": [], "link_selectors": [".configured-link"],
        "min_title_length": 10, "limit": 20, "learned_container_selectors": [],
        "learned_title_selectors": [], "learned_link_selectors": [],
        "learned_date_selectors": [], "learned_general_selectors": [],
        "now": "2026-10-22T08:30:00",
        "html": "<article><h2>Configured link test</h2><a href='/first'>first</a><a class='configured-link' href='/configured'>configured</a></article>",
    }
    result = _extract_case(monkeypatch, case)
    assert result[0]["link"] == "https://example.com/first"
