"""Regression coverage for bounded extraction and page-analysis caches."""

import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta

import pytest

from news_aggregator.services import extraction_memory as memory_module
from news_aggregator.services.extraction_memory import (
    ExtractionMemoryEntry,
    ExtractionMemoryService,
)
from news_aggregator.sources.ai_page_analyzer import AIPageAnalyzer, PageAnalysis


def entry(
    domain, selector, *, quality=0.0, successes=0, failures=0, identifier=None,
    consecutive_successes=0, consecutive_failures=0, last_success_at=None,
):
    return ExtractionMemoryEntry(
        domain=domain,
        selector_pattern=selector,
        extraction_strategy="css",
        success_count=successes,
        failure_count=failures,
        success_rate=(successes / (successes + failures) * 100) if successes + failures else 0.0,
        quality_score_avg=quality,
        consecutive_successes=consecutive_successes,
        consecutive_failures=consecutive_failures,
        last_success_at=last_success_at,
        id=identifier,
    )


def selector_names(service):
    return {
        domain: [pattern.selector_pattern for pattern in patterns]
        for domain, patterns in service._patterns.items()
    }


@pytest.mark.asyncio
async def test_success_failure_and_ai_admission_share_pattern_and_domain_limits(monkeypatch):
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_PER_DOMAIN", 2)
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_IN_MEMORY", 4)
    monkeypatch.setattr(memory_module, "MAX_DOMAINS_IN_MEMORY", 2)
    service = ExtractionMemoryService()
    service._initialized = True
    monkeypatch.setattr(service, "_save_pattern_to_db", AsyncMock())

    def no_task(coro):
        coro.close()

    monkeypatch.setattr(memory_module.asyncio, "create_task", no_task)
    await service._add_successful_pattern("a.test", ".old", "css", 0.1, 10)
    await service._add_successful_pattern("a.test", ".better", "css", 0.9, 10)
    await service._add_successful_pattern("a.test", ".new", "css", 0.5, 10)
    await service._add_failed_pattern("a.test", ".failure", "css")
    await service.record_ai_pattern_discovery(
        "b.test", [{"selector": ".ai", "strategy": "ai", "confidence": 0.8}]
    )
    await service.record_ai_pattern_discovery(
        "c.test", [{"selector": ".last", "strategy": "ai", "confidence": 0.9}]
    )

    assert len(service._patterns) <= 2
    assert sum(map(len, service._patterns.values())) <= 4
    assert all(len(patterns) <= 2 for patterns in service._patterns.values())
    assert ".better" in selector_names(service).get("a.test", [])


@pytest.mark.asyncio
async def test_startup_db_load_is_limited_in_sql_and_admits_ranked_rows(monkeypatch):
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_PER_DOMAIN", 2)
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_IN_MEMORY", 3)
    monkeypatch.setattr(memory_module, "MAX_DOMAINS_IN_MEMORY", 2)
    rows = [
        (30, "b.test", ".b", "css", 4, 0, 0.8, 50, "manual", False, 0, 0, None, None, None, None),
        (40, "c.test", ".c", "css", 4, 0, 0.8, 50, "manual", False, 0, 0, None, None, None, None),
        (10, "a.test", ".high", "css", 8, 1, 0.9, 50, "manual", False, 0, 0, None, None, None, None),
    ]
    result = SimpleNamespace(fetchall=lambda: rows)
    session = SimpleNamespace(execute=AsyncMock(return_value=result))

    class SessionContext:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(memory_module, "AsyncSessionLocal", SessionContext)
    service = ExtractionMemoryService()
    await service._load_patterns_from_db()

    statement = session.execute.await_args.args[0].text
    params = session.execute.await_args.args[1]
    assert "ROW_NUMBER() OVER" in statement.upper()
    assert "LIMIT :total_limit" in statement
    assert "success_rate DESC" in statement
    assert "consecutive_failures" in statement
    assert "last_success_at DESC NULLS LAST" in statement
    assert "selector_pattern ASC" in statement
    assert params == {"per_domain_limit": 2, "domain_limit": 2, "total_limit": 3}
    assert len(service._patterns) <= 2
    assert sum(map(len, service._patterns.values())) <= 3
    assert ".b" in selector_names(service).get("b.test", [])
    assert ".c" in selector_names(service).get("c.test", [])


def test_extraction_eviction_uses_success_evidence_then_stable_ties(monkeypatch):
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_PER_DOMAIN", 2)
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_IN_MEMORY", 10)
    monkeypatch.setattr(memory_module, "MAX_DOMAINS_IN_MEMORY", 10)
    service = ExtractionMemoryService()
    service._patterns = {
        "site.test": [
            entry("site.test", ".beta", quality=0.5, successes=10, identifier=3),
            entry("site.test", ".alpha", quality=0.5, successes=10, identifier=8),
            entry("site.test", ".worse", quality=0.99, successes=2, failures=2,
                  consecutive_failures=2, identifier=1),
        ]
    }

    service._evict_patterns("site.test")

    assert selector_names(service)["site.test"] == [".alpha", ".beta"]


@pytest.mark.asyncio
async def test_recent_new_success_is_learned_at_full_domain_capacity(monkeypatch):
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_PER_DOMAIN", 2)
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_IN_MEMORY", 10)
    monkeypatch.setattr(memory_module, "MAX_DOMAINS_IN_MEMORY", 10)
    service = ExtractionMemoryService()
    service._initialized = True
    old = datetime.utcnow() - timedelta(days=90)
    service._patterns["site.test"] = [
        entry("site.test", ".old-a", quality=0.8, successes=1,
              consecutive_successes=1, last_success_at=old, identifier=1),
        entry("site.test", ".old-z", quality=0.8, successes=1,
              consecutive_successes=1, last_success_at=old, identifier=2),
    ]
    monkeypatch.setattr(service, "_save_pattern_to_db", AsyncMock())

    def no_task(coro):
        coro.close()

    monkeypatch.setattr(memory_module.asyncio, "create_task", no_task)
    await service._add_successful_pattern("site.test", ".new-success", "css", 0.5, 500)

    assert ".new-success" in selector_names(service)["site.test"]
    assert len(service._patterns["site.test"]) == 2


@pytest.mark.asyncio
async def test_high_confidence_untried_ai_pattern_is_kept_at_full_capacity(monkeypatch):
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_PER_DOMAIN", 2)
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_IN_MEMORY", 10)
    monkeypatch.setattr(memory_module, "MAX_DOMAINS_IN_MEMORY", 10)
    service = ExtractionMemoryService()
    service._initialized = True
    service._patterns["site.test"] = [
        entry("site.test", ".a-manual", successes=0, quality=0),
        entry("site.test", ".b-manual", successes=0, quality=0),
    ]

    def no_task(coro):
        coro.close()

    monkeypatch.setattr(memory_module.asyncio, "create_task", no_task)
    await service.record_ai_pattern_discovery("site.test", [{
        "selector": ".z-ai-confidence",
        "strategy": "source_container",
        "confidence": 0.95,
    }])

    retained = service._patterns["site.test"]
    ai_pattern = next(pattern for pattern in retained if pattern.selector_pattern == ".z-ai-confidence")
    assert ai_pattern.success_count == 0
    assert ai_pattern.success_rate == 95.0
    assert ai_pattern.quality_score_avg == 0.95
    assert len(retained) == 2


def test_failures_do_not_improve_pattern_rank():
    good = entry("site.test", ".good", quality=0.4, successes=3,
                 consecutive_successes=3, last_success_at=datetime.utcnow())
    failed = entry("site.test", ".failed", quality=0.99, successes=3, failures=3,
                   consecutive_failures=3, last_success_at=datetime.utcnow())

    assert ExtractionMemoryService._pattern_rank(good) < ExtractionMemoryService._pattern_rank(failed)

    persisted = entry("site.test", ".same", quality=0.5, successes=2,
                      consecutive_successes=2, identifier=99)
    not_yet_persisted = entry("site.test", ".same", quality=0.5, successes=2,
                              consecutive_successes=2, identifier=None)
    assert ExtractionMemoryService._pattern_rank(persisted) == (
        ExtractionMemoryService._pattern_rank(not_yet_persisted)
    )


@pytest.mark.asyncio
async def test_startup_sql_and_python_choose_same_pattern_order(monkeypatch):
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_PER_DOMAIN", 4)
    monkeypatch.setattr(memory_module, "MAX_PATTERNS_IN_MEMORY", 10)
    monkeypatch.setattr(memory_module, "MAX_DOMAINS_IN_MEMORY", 10)

    connection = sqlite3.connect(":memory:")
    connection.execute("""
        CREATE TABLE extraction_patterns (
            id INTEGER, domain TEXT, selector_pattern TEXT, extraction_strategy TEXT,
            success_count INTEGER, failure_count INTEGER, quality_score_avg REAL,
            content_length_avg INTEGER, discovered_by TEXT, is_stable INTEGER,
            consecutive_successes INTEGER, consecutive_failures INTEGER,
            first_success_at TEXT, last_success_at TEXT, created_at TEXT, updated_at TEXT
        )
    """)
    rows = [
        (1, "a.test", ".failed-high-quality", "css", 3, 3, 0.99, 100,
         "manual", 0, 0, 3, None, "2026-10-14T00:00:00", None, None),
        (2, "a.test", ".steady", "css", 3, 0, 0.50, 100,
         "manual", 0, 3, 0, None, "2026-10-01T00:00:00", None, None),
        (3, "a.test", ".recent", "css", 3, 0, 0.10, 100,
         "manual", 0, 3, 0, None, "2026-10-14T00:00:00", None, None),
        (4, "b.test", ".other-domain", "css", 2, 0, 0.80, 100,
         "manual", 0, 2, 0, None, "2026-10-14T00:00:00", None, None),
        (5, "a.test", ".ai-confidence", "css", 0, 0, 0.95, 0,
         "ai", 0, 0, 0, None, None, None, None),
        (6, "b.test", ".unknown", "css", 0, 0, 0.00, 0,
         "manual", 0, 0, 0, None, None, None, None),
    ]
    connection.executemany(
        "INSERT INTO extraction_patterns VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    connection.commit()

    class SqliteSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            connection.close()
            return False

        async def execute(self, statement, params):
            cursor = connection.execute(str(statement), params)
            return SimpleNamespace(fetchall=cursor.fetchall)

    monkeypatch.setattr(memory_module, "AsyncSessionLocal", SqliteSession)
    service = ExtractionMemoryService()
    await service._load_patterns_from_db()

    loaded = [pattern for patterns in service._patterns.values() for pattern in patterns]
    python_order = [pattern.selector_pattern for pattern in sorted(
        loaded, key=service._pattern_rank
    )]
    ai_confidence_pattern = next(
        pattern for pattern in loaded if pattern.selector_pattern == ".ai-confidence"
    )
    assert ai_confidence_pattern.success_rate == 95.0
    assert python_order == [
        ".recent", ".steady", ".other-domain", ".ai-confidence",
        ".failed-high-quality", ".unknown",
    ]


def analysis(reasoning="ok"):
    return PageAnalysis(["article"], ["h1"], ["time"], {}, "news", reasoning, {})


@pytest.mark.asyncio
async def test_ai_analysis_cache_enforces_entry_ttl_and_replacement_accounting(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.time.monotonic", lambda: now[0])
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.ANALYSIS_CACHE_MAX_ENTRIES", 1)
    analyzer = AIPageAnalyzer()
    fake_client = SimpleNamespace(
        summarization_model="test",
        _make_raw_ai_request=AsyncMock(return_value={
            "choices": [{"message": {"content": json.dumps({
                "content_selectors": ["article"], "title_selectors": ["h1"], "page_type": "news"
            })}}]
        }),
    )
    with patch("news_aggregator.services.ai_client.get_ai_client", return_value=fake_client):
        first = await analyzer.analyze_page_structure("https://one.test", "<article>one</article>")
        first_key = next(iter(analyzer.analysis_cache))
        first_size = analyzer._analysis_cache_metadata[first_key][1]
        first.content_selectors.append("x" * 100_000)
        assert analyzer.analysis_cache[first_key].content_selectors == ["article"]
        analyzer.analysis_cache[first_key] = analysis("a longer replacement payload")
        analyzer._store_cache_entry(first_key, analyzer.analysis_cache[first_key])
        assert analyzer._analysis_cache_bytes != first_size

        await analyzer.analyze_page_structure("https://two.test", "<article>two</article>")
        assert len(analyzer.analysis_cache) == 1

        now[0] += 24 * 60 * 60 + 1
        await analyzer.analyze_page_structure("https://three.test", "<article>three</article>")
        assert len(analyzer.analysis_cache) == 1
        assert fake_client._make_raw_ai_request.await_count == 3


@pytest.mark.asyncio
async def test_ai_cache_skips_oversized_key_and_value(monkeypatch):
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.ANALYSIS_CACHE_MAX_KEY_BYTES", 32)
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.ANALYSIS_CACHE_MAX_BYTES", 128)
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.ANALYSIS_CACHE_MAX_ENTRY_BYTES", 1024)
    analyzer = AIPageAnalyzer()
    response = {"choices": [{"message": {"content": json.dumps({
        "content_selectors": ["article"], "title_selectors": ["h1"], "page_type": "news",
        "reasoning": "x" * 500,
    })}}]}
    fake_client = SimpleNamespace(summarization_model="test", _make_raw_ai_request=AsyncMock(return_value=response))
    with patch("news_aggregator.services.ai_client.get_ai_client", return_value=fake_client):
        await analyzer.analyze_page_structure("https://long.test/" + "x" * 40, "<p>x</p>")
        await analyzer.analyze_page_structure("https://small.test", "<p>x</p>")

    assert analyzer.analysis_cache == {}
    assert analyzer._analysis_cache_bytes == 0


def test_ai_cache_enforces_total_serialized_byte_budget(monkeypatch):
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.ANALYSIS_CACHE_MAX_BYTES", 500)
    monkeypatch.setattr("news_aggregator.sources.ai_page_analyzer.ANALYSIS_CACHE_MAX_ENTRY_BYTES", 500)
    analyzer = AIPageAnalyzer()
    analyzer._store_cache_entry("a" * 64, analysis("a" * 100))
    first_size = analyzer._analysis_cache_bytes
    analyzer._store_cache_entry("b" * 64, analysis("b" * 100))

    assert len(analyzer.analysis_cache) == 1
    assert analyzer._analysis_cache_bytes <= 500
    assert analyzer._analysis_cache_bytes != first_size * 2
