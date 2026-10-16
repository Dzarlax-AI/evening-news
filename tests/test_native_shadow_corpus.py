"""Focused safety checks for the standalone production shadow exporter."""

import asyncio
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "export_native_shadow_corpus.py"
spec = importlib.util.spec_from_file_location("export_native_shadow_corpus", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def row(article_id, url, **extra):
    value = {"id": article_id, "source_type": "rss", "url": url, "title": "Article",
             "archived_content": "old content", "content_truncated": False,
             "published_at": datetime(2026, 10, 15, tzinfo=timezone.utc),
             "media_files": [], "media_truncated": False, "image_url": None}
    value.update(extra)
    return value


@pytest.mark.parametrize("url,reason", [
    ("http://news.example.org/a", "non_https_443"),
    ("https://news.example.org:444/a", "non_https_443"),
    ("https://user:token@news.example.org/a", "credentials"),
    ("https://news.example.org/a?key=secret", "query_or_fragment"),
    ("https://127.0.0.1/a", "ip_literal"),
    ("https://[::1]/a", "ip_literal"),
    ("https://localhost/a", "non_public_host"),
    ("https://news.local/a", "non_public_host"),
    ("https://admin.news.org/a", "non_public_host"),
    ("https://news.org/admin", "admin_url"),
    ("https://t.me/s/channel/1", "telegram_url"),
    ("https://single/a", "non_public_host"),
    ("https://news.org/%61dmin/people", "admin_url"),
    ("https://news.org/a/../admin", "admin_url"),
    ("https://news.org/a/%2e%2e/admin", "admin_url"),
    ("https://news.org/%2561dmin", "malformed_url"),
    ("https://news.org/a%2fb", "malformed_url"),
    ("https://news.org/a%zz", "malformed_url"),
])
def test_rejects_nonpublic_or_sensitive_url(url, reason):
    assert module.public_article_url(url) == (None, reason)


def test_selects_bounded_distinct_cases_and_sanitizes_media():
    rows = [row(i, f"https://news.org/{i}") for i in range(1, 13)]
    rows += [row(13, "https://news.org/1"), row(14, "https://other.org/14",
                                                   image_url="https://other.org/img?token=x",
                                                   media_files=json.dumps([{"url": "https://other.org/i",
                                                                            "type": "image", "cookie": "private"},
                                                                           {"url": "https://localhost/x"}]),
                                                   content_truncated=True, media_truncated=True)]
    cases, exclusions, truncations = module.select_cases(rows, 50)
    assert len(cases) == 11
    assert cases[0]["id"] == "case-0001"
    assert cases[-1]["article_id"] == 14
    assert cases[-1]["media_files"] == [{"url": "https://other.org/i", "type": "image"}]
    assert cases[-1]["image_url"] is None
    assert exclusions == {"hostname_cap": 2, "duplicate_url": 1}
    assert truncations == {"image_url_filtered": 1, "archived_content": 1, "media_files": 1}


def test_global_case_cap_and_sql_candidate_bound():
    rows = [row(i, f"https://news{i}.org/story") for i in range(55)]
    cases, exclusions, _ = module.select_cases(rows, 50)
    assert len(cases) == 50
    assert exclusions == {"case_cap": 5}
    assert "LIMIT $1" in module.CANDIDATES_SQL
    assert "a.content" not in module.CANDIDATES_SQL
    assert "left(a.content, 100000)" in module.DETAILS_SQL
    assert "a.id = ANY($1::integer[])" in module.DETAILS_SQL
    assert "a.processing_state = 'completed'" in module.CANDIDATES_SQL
    assert "a.is_advertisement IS FALSE" in module.CANDIDATES_SQL


def test_deduplicates_host_case_and_explicit_default_port():
    rows = [row(1, "https://NEWS.org:443/story"), row(2, "https://news.org/story")]
    cases, exclusions, _ = module.select_cases(rows, 50)
    assert len(cases) == 1
    assert exclusions == {"duplicate_url": 1}


def test_schema_requires_all_columns():
    rows = [{"table_name": table, "column_name": column}
            for table, columns in module.REQUIRED_COLUMNS.items() for column in columns]
    module.check_schema(rows)
    with pytest.raises(module.ExportError, match="schema"):
        module.check_schema(rows[:-1])


def test_private_atomic_artifacts_and_no_overwrite(tmp_path):
    output = tmp_path / "corpus"
    manifest = module.write_artifacts(output, [row(1, "https://news.org/a")], 50,
                                      datetime(2026, 10, 16, tzinfo=timezone.utc))
    assert manifest["expires_at"] == "2026-10-23T00:00:00+00:00"
    assert manifest["case_count"] == 1
    assert not list(output.glob("*.tmp"))
    assert json.loads((output / "corpus.json").read_text())["cases"][0]["url"] == "https://news.org/a"
    if os.name != "nt":
        assert stat.S_IMODE(output.stat().st_mode) == 0o700
        assert stat.S_IMODE((output / "corpus.json").stat().st_mode) == 0o600
    with pytest.raises(module.ExportError, match="new absolute"):
        module.write_artifacts(output, [], 50, datetime.now(timezone.utc))


def test_corpus_byte_limit_prevents_payload(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir()
    monkeypatch.setattr(module, "MAX_CORPUS_BYTES", 5)
    with pytest.raises(module.ExportError, match="byte limit"):
        module.atomic_private_json(directory, "corpus.json", {"cases": []}, module.MAX_CORPUS_BYTES)
    assert not (directory / "corpus.json").exists()


def test_corpus_limit_counts_utf8_bytes(tmp_path):
    directory = tmp_path / "private"
    directory.mkdir()
    payload = {"content": "😀" * 4}
    character_count = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    assert len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) > character_count
    with pytest.raises(module.ExportError, match="byte limit"):
        module.atomic_private_json(directory, "corpus.json", payload, character_count)
    assert not (directory / "corpus.json").exists()


def test_database_read_uses_readonly_transaction_and_local_timeouts(monkeypatch):
    calls = []

    class Transaction:
        async def __aenter__(self):
            calls.append("begin")

        async def __aexit__(self, *args):
            calls.append("end")

    class Connection:
        def transaction(self, **kwargs):
            assert kwargs == {"readonly": True, "isolation": "repeatable_read"}
            return Transaction()

        async def execute(self, sql):
            calls.append(sql)

        async def fetch(self, sql, arg):
            calls.append((sql, arg))
            if sql == module.SCHEMA_SQL:
                return [{"table_name": table, "column_name": column}
                        for table, columns in module.REQUIRED_COLUMNS.items() for column in columns]
            if sql == module.CANDIDATES_SQL:
                return [{"id": 1, "url": "https://news.org/a", "source_type": "rss",
                         "url_truncated": False}]
            assert sql == module.DETAILS_SQL
            assert arg == [1]
            value = row(1, "https://news.org/a")
            return [{key: value[key] for key in value if key not in {"url", "source_type"}}]

        async def close(self):
            calls.append("close")

    async def connect(**kwargs):
        assert kwargs["database"] == "news"
        return Connection()

    monkeypatch.setitem(sys.modules, "asyncpg", SimpleNamespace(connect=connect))
    rows, count, excluded = asyncio.run(module.read_candidates("postgresql+asyncpg://user:pass@localhost/news", 20, 10))
    assert len(rows) == 1
    assert count == 1 and excluded == {}
    assert calls[0] == "begin"
    assert calls[1:3] == ["SET LOCAL statement_timeout = '3000ms'",
                           "SET LOCAL lock_timeout = '1000ms'"]
    assert calls[-2:] == ["end", "close"]


def test_main_redacts_driver_exception(capsys, monkeypatch, tmp_path):
    async def fail(*args):
        raise RuntimeError("postgresql://secret@host and https://news.org/private")

    was_called = []
    async def fail_with_marker(*args):
        was_called.append(True)
        return await fail(*args)

    monkeypatch.setattr(module.os, "geteuid", lambda: 0, raising=False)
    monkeypatch.setattr(module, "check_output_parent", lambda _: None)
    monkeypatch.setattr(module, "read_candidates", fail_with_marker)
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret@host/news")
    assert module.main(["--output-dir", str(tmp_path / "new")]) == 1
    output = capsys.readouterr()
    assert "secret" not in output.out + output.err
    assert "news.org" not in output.out + output.err
    assert was_called


def test_database_url_options_are_preserved_or_rejected():
    kwargs = module.connection_kwargs(
        "postgresql+asyncpg://user:pass@localhost/news?sslmode=verify-full&options=-c%20search_path%3Dnews"
    )
    assert "sslmode=verify-full" in kwargs["dsn"]
    assert "options=" in kwargs["dsn"]
    with pytest.raises(module.ExportError):
        module.connection_kwargs("postgresql://user:pass@localhost/news?unknown=ignored")
