"""Export a bounded, private article corpus for the isolated native shadow run.

Run this inside the cached production app image, after the network fixture gate.
The process reads DATABASE_URL itself; neither the URL nor database errors are logged.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import sys
from urllib.parse import unquote_to_bytes, urlsplit

MAX_CASES = 50
MAX_CANDIDATES = 1000
MAX_CORPUS_BYTES = 64 * 1024 * 1024
MAX_CONTENT_CHARS = 100_000
MAX_MEDIA_BYTES = 65_536
REQUIRED_COLUMNS = {
    "articles": {"id", "source_id", "title", "url", "content", "image_url", "media_files",
                 "published_at", "fetched_at", "processing_state", "is_advertisement"},
    "sources": {"id", "source_type"},
}
SPECIAL_SUFFIXES = {"localhost", "local", "internal", "invalid", "test", "example",
                    "onion", "arpa", "home", "lan"}
ADMIN_LABELS = {"admin", "intranet", "metadata", "router", "gateway", "postgres", "db", "nas"}
ADMIN_PATH_PARTS = {"admin", "wp-admin", "dashboard", "login", "signin"}
HOST_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")

SCHEMA_SQL = """
SELECT table_name, column_name FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = ANY($1::text[])
"""

CANDIDATES_SQL = """
SELECT a.id, s.source_type, left(a.url, 4097) AS url,
       char_length(a.url) > 4096 AS url_truncated
FROM articles AS a JOIN sources AS s ON s.id = a.source_id
WHERE a.fetched_at >= now() - interval '7 days'
  AND a.processing_state = 'completed'
  AND a.is_advertisement IS FALSE
  AND s.source_type IN ('rss', 'custom')
ORDER BY a.fetched_at DESC, a.id DESC
LIMIT $1
"""

DETAILS_SQL = """
SELECT a.id, left(a.title, 4096) AS title,
       char_length(a.title) > 4096 AS title_truncated,
       left(a.content, 100000) AS archived_content,
       char_length(a.content) > 100000 AS content_truncated,
       a.published_at,
       CASE WHEN octet_length(a.media_files::text) <= 65536
            THEN a.media_files::jsonb ELSE '[]'::jsonb END AS media_files,
       octet_length(a.media_files::text) > 65536 AS media_truncated,
       left(a.image_url, 4097) AS image_url,
       char_length(a.image_url) > 4096 AS image_truncated
FROM articles AS a WHERE a.id = ANY($1::integer[])
"""


class ExportError(Exception):
    """Expected export failure with a public, non-sensitive message."""


def public_article_url(value: object) -> tuple[str | None, str]:
    """Syntactic first pass only; the gateway must verify every DNS answer."""
    if not isinstance(value, str) or len(value) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in value):
        return None, "malformed_url"
    try:
        parts = urlsplit(value)
        host = parts.hostname
        port = parts.port
    except ValueError:
        return None, "malformed_url"
    if parts.scheme != "https" or not parts.netloc or port not in (None, 443):
        return None, "non_https_443"
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        return None, "credentials"
    if parts.query or parts.fragment or "?" in value or "#" in value:
        return None, "query_or_fragment"
    if not host or "%" in parts.netloc or "\\" in value or host.endswith("."):
        return None, "malformed_url"
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return None, "ip_literal"
    host = host.lower()
    labels = host.split(".")
    if len(labels) < 2 or any(not HOST_LABEL.fullmatch(label) for label in labels):
        return None, "non_public_host"
    if labels[-1] in SPECIAL_SUFFIXES or labels[-1].isdigit() or any(label in ADMIN_LABELS for label in labels):
        return None, "non_public_host"
    if host == "t.me" or host.endswith(".t.me") or host == "telegram.me" or host.endswith(".telegram.me"):
        return None, "telegram_url"
    path = parts.path
    if re.search(r"%(?![0-9a-fA-F]{2})|%(?:2f|5c|3f|23)", path, re.IGNORECASE):
        return None, "malformed_url"
    try:
        decoded_path = unquote_to_bytes(path).decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None, "malformed_url"
    if "%" in decoded_path or "\\" in decoded_path:
        return None, "malformed_url"
    segments: list[str] = []
    for segment in decoded_path.split("/"):
        if segment == "..":
            if segments:
                segments.pop()
        elif segment and segment != ".":
            segments.append(segment)
        if segment.casefold() in ADMIN_PATH_PARTS:
            return None, "admin_url"
    if any(segment.casefold() in ADMIN_PATH_PARTS for segment in segments):
        return None, "admin_url"
    return value, host


def bounded_media(value: object) -> tuple[list[dict[str, str]], bool]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return [], True
    if not isinstance(value, list):
        return [], bool(value)
    result = []
    truncated = len(value) > 20
    for item in value[:20]:
        if not isinstance(item, dict):
            truncated = True
            continue
        media: dict[str, str] = {}
        for key in ("url", "thumbnail"):
            safe, _ = public_article_url(item.get(key))
            if safe:
                media[key] = safe
            elif item.get(key):
                truncated = True
        kind = item.get("type")
        if kind in {"image", "video", "document", "audio"}:
            media["type"] = kind
        if "url" in media:
            result.append(media)
    return result, truncated


def select_candidate_rows(rows: list[object], limit: int) -> tuple[list[dict], dict[str, int]]:
    selected: list[dict] = []
    excluded: Counter[str] = Counter()
    hosts: defaultdict[str, int] = defaultdict(int)
    seen: set[tuple[str, str]] = set()
    for row in rows:
        row = dict(row)
        if row.get("url_truncated"):
            excluded["malformed_url"] += 1
            continue
        url, reason = public_article_url(row.get("url"))
        if url is None:
            excluded[reason] += 1
            continue
        canonical_path = urlsplit(url).path or "/"
        duplicate_key = (reason, canonical_path)
        if duplicate_key in seen:
            excluded["duplicate_url"] += 1
            continue
        seen.add(duplicate_key)
        if hosts[reason] >= 10:
            excluded["hostname_cap"] += 1
            continue
        if len(selected) >= limit:
            excluded["case_cap"] += 1
            continue
        hosts[reason] += 1
        selected.append(row)
    return selected, dict(excluded)


def format_cases(rows: list[dict]) -> tuple[list[dict], dict[str, int]]:
    cases: list[dict] = []
    truncated: Counter[str] = Counter()
    for row in rows:
        url = row["url"]
        media, media_filtered = bounded_media(row.get("media_files"))
        image, _ = public_article_url(row.get("image_url")) if not row.get("image_truncated") else (None, "malformed_url")
        if row.get("image_url") and not image:
            truncated["image_url_filtered"] += 1
        if row.get("content_truncated"):
            truncated["archived_content"] += 1
        if row.get("title_truncated"):
            truncated["title"] += 1
        if row.get("media_truncated") or media_filtered:
            truncated["media_files"] += 1
        publication = row.get("published_at")
        cases.append({
            "id": f"case-{len(cases) + 1:04d}",
            "article_id": row["id"],
            "source_type": row["source_type"],
            "url": url,
            "title": row.get("title") or "",
            "archived_content": row.get("archived_content") or "",
            "published_at": publication.isoformat() if isinstance(publication, datetime) else None,
            "media_files": media,
            "image_url": image,
        })
    return cases, dict(truncated)


def select_cases(rows: list[object], limit: int) -> tuple[list[dict], dict[str, int], dict[str, int]]:
    selected, excluded = select_candidate_rows(rows, limit)
    cases, truncated = format_cases(selected)
    return cases, excluded, truncated


def check_schema(rows: list[object]) -> None:
    found: defaultdict[str, set[str]] = defaultdict(set)
    for row in rows:
        found[row["table_name"]].add(row["column_name"])
    if any(not columns.issubset(found[table]) for table, columns in REQUIRED_COLUMNS.items()):
        raise ExportError("Database schema does not meet the exporter contract")


def connection_kwargs(raw_url: str) -> dict:
    from sqlalchemy.engine import make_url

    try:
        url = make_url(raw_url)
        if url.drivername not in {"postgresql", "postgresql+asyncpg", "postgres"}:
            raise ValueError
        if not url.host or not url.database:
            raise ValueError
        query = dict(url.query)
        if not set(query).issubset({"sslmode", "sslrootcert", "sslcert", "sslkey", "options"}):
            raise ValueError
        if "sslmode" in query and query["sslmode"] not in {
            "disable", "allow", "prefer", "require", "verify-ca", "verify-full"
        }:
            raise ValueError
        if query:
            # asyncpg parses these libpq-compatible URI options itself. Unknown
            # options fail closed rather than silently changing TLS/search_path.
            return {"dsn": url.set(drivername="postgresql").render_as_string(hide_password=False),
                    "timeout": 5}
        return {"host": url.host, "port": url.port or 5432, "user": url.username,
                "password": url.password, "database": url.database, "timeout": 5}
    except (TypeError, ValueError, AttributeError):
        raise ExportError("Invalid database URL configuration") from None


async def read_candidates(raw_url: str, candidate_limit: int, limit: int) -> tuple[list[dict], int, dict[str, int]]:
    import asyncpg

    conn = await asyncpg.connect(**connection_kwargs(raw_url))
    try:
        async with conn.transaction(readonly=True, isolation="repeatable_read"):
            await conn.execute("SET LOCAL statement_timeout = '3000ms'")
            await conn.execute("SET LOCAL lock_timeout = '1000ms'")
            check_schema(await conn.fetch(SCHEMA_SQL, list(REQUIRED_COLUMNS)))
            candidate_rows = await conn.fetch(CANDIDATES_SQL, candidate_limit)
            selected, excluded = select_candidate_rows(candidate_rows, limit)
            if not selected:
                return [], len(candidate_rows), excluded
            detail_rows = await conn.fetch(DETAILS_SQL, [row["id"] for row in selected])
            details = {row["id"]: dict(row) for row in detail_rows}
            if len(details) != len(selected):
                raise ExportError("Selected articles changed during export")
            return [{**row, **details[row["id"]]} for row in selected], len(candidate_rows), excluded
    finally:
        await conn.close()


def atomic_private_json(directory: Path, filename: str, value: object, max_bytes: int) -> int:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    if len(payload) > max_bytes:
        raise ExportError("Corpus byte limit exceeded")
    temporary = directory / (filename + ".tmp")
    target = directory / filename
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.link(temporary, target)  # Fails closed if a target exists.
    finally:
        temporary.unlink(missing_ok=True)
    return len(payload)


def write_artifacts(output_dir: Path, rows: list[object], limit: int, now: datetime,
                    *, candidate_count: int | None = None,
                    candidate_exclusions: dict[str, int] | None = None) -> dict:
    if not output_dir.is_absolute() or output_dir.exists() or output_dir.is_symlink():
        raise ExportError("Output directory must be a new absolute path")
    output_dir.mkdir(mode=0o700)
    os.chmod(output_dir, 0o700)
    if candidate_exclusions is None:
        cases, excluded, truncated = select_cases(rows, limit)
    else:
        cases, truncated = format_cases([dict(row) for row in rows])
        excluded = candidate_exclusions
    corpus = {"version": 1, "cases": cases}
    corpus_bytes = atomic_private_json(output_dir, "corpus.json", corpus, MAX_CORPUS_BYTES)
    manifest = {"version": 1, "created_at": now.isoformat(),
                "expires_at": (now + timedelta(days=7)).isoformat(),
                "retention": "manual cleanup after expiry; no automatic deletion",
                "candidate_count": len(rows) if candidate_count is None else candidate_count,
                "case_count": len(cases),
                "exclusion_counts": excluded, "truncation_counts": truncated,
                "corpus_bytes": corpus_bytes, "corpus_file": "corpus.json"}
    atomic_private_json(output_dir, "manifest.json", manifest, 1024 * 1024)
    return manifest


def check_output_parent(output_dir: Path) -> None:
    """Reject a writable or redirected parent before creating root-only evidence."""
    parent = output_dir.parent
    try:
        if parent.resolve(strict=True) != parent.absolute():
            raise ExportError("Output parent must not contain symbolic links")
        details = parent.stat()
    except OSError:
        raise ExportError("Output parent is unavailable") from None
    if details.st_uid != 0 or details.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ExportError("Output parent must be root-owned and private")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export bounded private live-shadow corpus")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--database-env", default="DATABASE_URL")
    parser.add_argument("--limit", type=int, default=MAX_CASES)
    parser.add_argument("--candidate-limit", type=int, default=MAX_CANDIDATES)
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= MAX_CASES or not 1 <= args.candidate_limit <= MAX_CANDIDATES:
        parser.error("Limits must be positive and within 50 cases / 1000 candidates")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", args.database_env):
        parser.error("Invalid database environment variable name")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if not hasattr(os, "geteuid") or os.geteuid() != 0:
            raise ExportError("Exporter must run as root")
        if not args.output_dir.is_absolute() or args.output_dir.exists() or args.output_dir.is_symlink():
            raise ExportError("Output directory must be a new absolute path")
        check_output_parent(args.output_dir)
        raw_url = os.environ.get(args.database_env)
        if not raw_url:
            raise ExportError("Database URL environment variable is missing")
        rows, candidate_count, excluded = asyncio.run(read_candidates(raw_url, args.candidate_limit, args.limit))
        manifest = write_artifacts(args.output_dir, rows, args.limit, datetime.now(timezone.utc),
                                   candidate_count=candidate_count, candidate_exclusions=excluded)
        print(json.dumps({"case_count": manifest["case_count"],
                          "candidate_count": manifest["candidate_count"],
                          "output_dir": str(args.output_dir)}))
        return 0
    except Exception:
        # Driver exceptions can contain connection strings, SQL values, or article URLs.
        print("Corpus export failed; inspect schema, database access, and output permissions locally.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
