"""Private, bounded adapter for the isolated Russian editorial Go engine."""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any, Awaitable, Callable
from urllib.parse import quote, urlsplit, urlunsplit

from sqlalchemy import and_, func, select, text

from ..config import settings
from ..database import AsyncSessionLocal
from ..models import Article, ArticleCategory, Category, Source

LOCALE = "ru"
POLICY_VERSION = "ru-v2-summaries"
EDITORIAL_NAMESPACE = uuid.UUID("9e341f33-e4dc-5ee8-9013-817f41e9d442")
MAX_ARTICLES = 500
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_REQUEST_BYTES = 4 * 1024 * 1024
MAX_STDOUT_BYTES = 2 * 1024 * 1024
MAX_STDERR_BYTES = 16 * 1024
MAX_PROVIDER_SECONDS = 60.0
SAFE_GO_ERROR_CODES = {
    "provider_not_configured", "provider_invalid_configuration", "provider_request_too_large", "provider_summary_missing",
    "provider_cancelled", "provider_timeout", "provider_transport_error",
    "provider_response_read_error", "provider_response_too_large", "provider_invalid_response",
    "provider_empty_response", "provider_incomplete_response", "provider_invalid_editorial_json",
    "provider_invalid_editorial_edition", "provider_rejected_credentials", "provider_rate_limited",
    "provider_unavailable", "provider_rejected_request", "invalid_snapshot", "invalid_edition",
    "unsupported_locale", "render_story_too_large", "invalid_request", "request_too_large",
    "unsupported_operation", "invalid_operation_result",
}

_MODEL_RE = re.compile(r"[A-Za-z0-9._-]{1,128}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_ARTICLE_LIMITS = {
    "id": (1, 10), "title": (1, 1000), "summary": (0, 10000), "content": (0, 30000),
    "source_name": (1, 300), "url": (1, 2048), "category_id": (1, 10), "category_name": (1, 200),
}

_RUSSIAN_CATEGORY_LABELS = {
    "serbia": "Сербия", "tech": "Технологии", "technology": "Технологии",
    "ai": "Искусственный интеллект", "politics": "Политика", "business": "Бизнес",
    "media": "Медиа", "science": "Наука", "health": "Здоровье", "world": "Мир",
    "culture": "Культура", "other": "Другое",
}


class EditorialOperationError(RuntimeError):
    """Finite, safe failure that contains no article or provider payload."""

    def __init__(self, code: str):
        self.code = code if code in _SAFE_OPERATION_CODES else "editorial_generation_failed"
        super().__init__(self.code)


_SAFE_OPERATION_CODES = {
    "editorial_not_ready", "editorial_resource_limit", "editorial_provider_timeout",
    "editorial_provider_unavailable", "editorial_invalid_result", "editorial_generation_failed",
}


def _size_bytes(value: str) -> int:
    return len(value.encode("utf-8"))


def _positive_db_id(value: Any) -> bool:
    return type(value) is int and 0 < value <= 2_147_483_647


def _category_label(category_name: str, display_name: str | None) -> str:
    mapped = _RUSSIAN_CATEGORY_LABELS.get(category_name.strip().casefold())
    return mapped or (display_name.strip() if isinstance(display_name, str) and display_name.strip()
                      else category_name.strip())


def _validate_snapshot(snapshot: dict[str, Any]) -> bytes:
    if not isinstance(snapshot, dict) or set(snapshot) != {
        "date", "cutoff_utc", "site_base_url", "edition_url", "articles",
    }:
        raise EditorialOperationError("editorial_invalid_result")
    articles = snapshot["articles"]
    if not isinstance(articles, list) or not 1 <= len(articles) <= MAX_ARTICLES:
        raise EditorialOperationError("editorial_resource_limit")
    seen: set[str] = set()
    for article in articles:
        if not isinstance(article, dict) or set(article) != {
            "id", "title", "summary", "content", "source_name", "url", "category_id", "category_name",
        }:
            raise EditorialOperationError("editorial_invalid_result")
        for field, value in article.items():
            if not isinstance(value, str):
                raise EditorialOperationError("editorial_invalid_result")
            low, high = _ARTICLE_LIMITS[field]
            size = _size_bytes(value)
            if size < low or size > high:
                raise EditorialOperationError("editorial_resource_limit")
        if (not article["id"].isascii() or not article["id"].isdecimal()
                or str(int(article["id"])) != article["id"] or not _positive_db_id(int(article["id"]))
                or not article["category_id"].isascii() or not article["category_id"].isdecimal()
                or str(int(article["category_id"])) != article["category_id"]
                or not _positive_db_id(int(article["category_id"]))):
            raise EditorialOperationError("editorial_invalid_result")
        if article["id"] in seen:
            raise EditorialOperationError("editorial_invalid_result")
        seen.add(article["id"])
        source_url = urlsplit(article["url"])
        if (source_url.scheme not in {"http", "https"} or not source_url.hostname
                or source_url.username or source_url.password
                or any(ch.isspace() for ch in article["url"])):
            raise EditorialOperationError("editorial_invalid_result")
    try:
        encoded = json.dumps(snapshot, ensure_ascii=False, allow_nan=False,
                             separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise EditorialOperationError("editorial_invalid_result") from None
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        raise EditorialOperationError("editorial_resource_limit")
    return encoded


def _gemini_endpoint(base_url: str, model: str) -> str:
    if not isinstance(model, str) or _MODEL_RE.fullmatch(model) is None:
        raise EditorialOperationError("editorial_provider_unavailable")
    try:
        parsed = urlsplit(base_url)
        port = parsed.port
    except (ValueError, AttributeError):
        raise EditorialOperationError("editorial_provider_unavailable") from None
    hostname = parsed.hostname or ""
    if (parsed.scheme not in {"https", "http"} or not hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise EditorialOperationError("editorial_provider_unavailable")
    path = parsed.path.rstrip("/")
    suffix = f"/{quote(model, safe='')}:generateContent"
    if path.endswith(":generateContent"):
        if not path.endswith(suffix):
            raise EditorialOperationError("editorial_provider_unavailable")
        endpoint_path = path
    elif path.endswith("/models"):
        endpoint_path = path + suffix
    else:
        raise EditorialOperationError("editorial_provider_unavailable")
    return urlunsplit((parsed.scheme, parsed.netloc, endpoint_path, "", ""))


def _child_environment() -> dict[str, str]:
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    # Settings currently declares this as Optional[str]; mirror that runtime type.
    key = settings.gemini_api_key or ""
    model = settings.digest_model
    endpoint = _gemini_endpoint(settings.gemini_api_endpoint, model)
    if key:
        env["GEMINI_API_KEY"] = key
    env["GEMINI_MODEL"] = model
    env["GEMINI_ENDPOINT"] = endpoint
    return env


async def _read_bounded(stream: asyncio.StreamReader, max_bytes: int) -> bytes:
    result = bytearray()
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            return bytes(result)
        if len(result) + len(chunk) > max_bytes:
            raise EditorialOperationError("editorial_resource_limit")
        result.extend(chunk)


async def _write_and_close(stream: asyncio.StreamWriter, payload: bytes) -> None:
    stream.write(payload)
    await stream.drain()
    stream.close()
    await stream.wait_closed()


def _kill_child(process: asyncio.subprocess.Process) -> None:
    try:
        process.kill()
    except ProcessLookupError:
        pass


async def run_editorial_cli(request: dict[str, Any], deadline: datetime,
                            *, binary: str | None = None) -> dict[str, Any]:
    """Invoke the configured executable without a shell, bounding every pipe and wait."""
    payload = json.dumps(request, ensure_ascii=False, allow_nan=False,
                         separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_REQUEST_BYTES:
        raise EditorialOperationError("editorial_resource_limit")
    target = binary or settings.editorial_binary
    if not isinstance(target, str) or not target.startswith("/"):
        raise EditorialOperationError("editorial_generation_failed")
    timeout = (deadline - datetime.now(timezone.utc)).total_seconds()
    if timeout <= 0:
        raise EditorialOperationError("editorial_provider_timeout")
    try:
        process = await asyncio.create_subprocess_exec(
            target, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=_child_environment(),
        )
    except (OSError, ValueError):
        raise EditorialOperationError("editorial_generation_failed") from None
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    tasks = [
        asyncio.create_task(_write_and_close(process.stdin, payload)),
        asyncio.create_task(_read_bounded(process.stdout, MAX_STDOUT_BYTES)),
        asyncio.create_task(_read_bounded(process.stderr, MAX_STDERR_BYTES)),
        asyncio.create_task(process.wait()),
    ]
    try:
        remaining = min(timeout, MAX_PROVIDER_SECONDS)
        _, stdout, _stderr, returncode = await asyncio.wait_for(asyncio.gather(*tasks), remaining)
    except asyncio.TimeoutError:
        _kill_child(process)
        await process.wait()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise EditorialOperationError("editorial_provider_timeout") from None
    except asyncio.CancelledError:
        _kill_child(process)
        try:
            await asyncio.shield(process.wait())
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except EditorialOperationError:
        _kill_child(process)
        await process.wait()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except (OSError, BrokenPipeError):
        _kill_child(process)
        await process.wait()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise EditorialOperationError("editorial_generation_failed") from None
    if returncode != 0:
        raise _go_error(stdout)
    try:
        result = json.loads(stdout.decode("utf-8"), parse_constant=lambda _x: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, TypeError):
        raise EditorialOperationError("editorial_invalid_result") from None
    if not isinstance(result, dict) or result.get("ok") is not True:
        raise _go_error(stdout)
    if set(result) != {"ok", "edition", "rendered", "provider_usage", "result_hash"}:
        raise EditorialOperationError("editorial_invalid_result")
    return result


def _go_error(stdout: bytes) -> EditorialOperationError:
    try:
        envelope = json.loads(stdout.decode("utf-8"))
        code = envelope.get("error_code") if isinstance(envelope, dict) else None
    except (UnicodeError, ValueError, AttributeError):
        code = None
    if code not in SAFE_GO_ERROR_CODES:
        return EditorialOperationError("editorial_generation_failed")
    if code == "provider_summary_missing":
        return EditorialOperationError("editorial_not_ready")
    if code in {"provider_timeout", "provider_cancelled"}:
        return EditorialOperationError("editorial_provider_timeout")
    if code in {"provider_transport_error", "provider_unavailable", "provider_rate_limited",
                "provider_rejected_credentials", "provider_not_configured"}:
        return EditorialOperationError("editorial_provider_unavailable")
    if code in {"provider_request_too_large", "provider_response_too_large", "request_too_large",
                "render_story_too_large"}:
        return EditorialOperationError("editorial_resource_limit")
    if code in {"provider_invalid_response", "provider_empty_response", "provider_incomplete_response",
                "provider_invalid_editorial_json", "provider_invalid_editorial_edition",
                "invalid_snapshot", "invalid_edition"}:
        return EditorialOperationError("editorial_invalid_result")
    return EditorialOperationError("editorial_generation_failed")


async def _fetch_snapshot_rows(session, edition_date: date):
    ranked_categories = (
        select(
            ArticleCategory.article_id.label("article_id"),
            ArticleCategory.category_id.label("category_id"),
            ArticleCategory.confidence.label("confidence"),
            func.row_number().over(
                partition_by=ArticleCategory.article_id,
                order_by=(func.coalesce(ArticleCategory.confidence, 0.0).desc(),
                          ArticleCategory.category_id.asc()),
            ).label("category_rank"),
        ).subquery()
    )
    stmt = (
        select(
            Article.id.label("article_id"), Article.title, Article.summary, Article.content,
            Article.url, Source.name.label("source_name"),
            ranked_categories.c.category_id, Category.name.label("category_name"),
            Category.display_name.label("category_display_name"), ranked_categories.c.confidence,
        )
        .select_from(Article)
        .outerjoin(Source, Source.id == Article.source_id)
        .join(ranked_categories, and_(
            ranked_categories.c.article_id == Article.id,
            ranked_categories.c.category_rank == 1,
        ))
        .outerjoin(Category, Category.id == ranked_categories.c.category_id)
        .where(func.date(Article.fetched_at) == edition_date)
        .where(Article.processing_state == "completed")
        .where(Article.is_advertisement.is_not(True))
        .order_by(
            Article.fetched_at.desc(), Article.id.asc(),
        )
        .limit(MAX_ARTICLES + 1)
    )
    return (await session.execute(stmt)).mappings().all()


def _snapshot_articles(rows) -> list[dict[str, str]]:
    selected: dict[int, dict[str, str]] = {}
    for row in rows:
        article_id = row["article_id"]
        if article_id in selected:
            continue
        if not _positive_db_id(article_id) or not _positive_db_id(row["category_id"]):
            raise EditorialOperationError("editorial_invalid_result")
        category_name = row["category_name"]
        if not isinstance(category_name, str) or not category_name.strip():
            raise EditorialOperationError("editorial_invalid_result")
        source_name = row["source_name"]
        if not isinstance(source_name, str) or not source_name.strip():
            raise EditorialOperationError("editorial_invalid_result")
        selected[article_id] = {
            "id": str(article_id), "title": row["title"] or "", "summary": row["summary"] or "",
            "content": row["content"] or "", "source_name": source_name,
            "url": row["url"] or "", "category_id": str(row["category_id"]),
            "category_name": _category_label(category_name, row["category_display_name"]),
        }
    return list(selected.values())


def _edition_id(job_id: str, attempt_generation: int, operation_id: str) -> uuid.UUID:
    return uuid.uuid5(EDITORIAL_NAMESPACE,
                      f"{job_id}:{attempt_generation}:{operation_id}:{LOCALE}")


def _edition_url(edition_date: date, edition_id: uuid.UUID) -> str:
    base = settings.site_base_url.rstrip("/")
    parts = urlsplit(base)
    if (parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password
            or parts.query or parts.fragment or any(ch.isspace() for ch in base)):
        raise EditorialOperationError("editorial_invalid_result")
    return f"{base}/digest/{edition_date.isoformat()}?edition={edition_id}"


def _validate_cli_result(result: dict[str, Any], snapshot: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    edition = result.get("edition")
    rendered = result.get("rendered")
    usage = result.get("provider_usage")
    if not isinstance(edition, dict) or not isinstance(rendered, dict) or not isinstance(usage, dict):
        raise EditorialOperationError("editorial_invalid_result")
    try:
        result_cutoff = datetime.fromisoformat(str(edition.get("cutoff_utc", "")).replace("Z", "+00:00"))
        requested_cutoff = datetime.fromisoformat(snapshot["cutoff_utc"].replace("Z", "+00:00"))
        cutoff_matches = (result_cutoff.tzinfo is not None and requested_cutoff.tzinfo is not None
                          and result_cutoff.astimezone(timezone.utc) == requested_cutoff.astimezone(timezone.utc))
    except (TypeError, ValueError):
        cutoff_matches = False
    if (edition.get("version") != "1" or edition.get("locale") != LOCALE
            or edition.get("date") != snapshot["date"]
            or not cutoff_matches
            or edition.get("site_base_url") != snapshot["site_base_url"]
            or edition.get("edition_url") != snapshot["edition_url"]
            or not isinstance(edition.get("stories"), list)
            or not _SHA256_RE.fullmatch(str(edition.get("source_snapshot_hash", "")))
            or not _SHA256_RE.fullmatch(str(edition.get("content_hash", "")))
            or result.get("result_hash") != edition.get("content_hash")):
        raise EditorialOperationError("editorial_invalid_result")
    if (set(rendered) != {"rich_html", "plain_parts", "preview_text"}
            or not isinstance(rendered["rich_html"], str) or not isinstance(rendered["preview_text"], str)
            or not isinstance(rendered["plain_parts"], list) or not rendered["plain_parts"]
            or any(not isinstance(part, str) or len(part.encode("utf-8")) > 65536
                   for part in rendered["plain_parts"])):
        raise EditorialOperationError("editorial_invalid_result")
    if (set(usage) != {"model", "prompt_token_count", "candidates_token_count", "total_token_count"}
            or not isinstance(usage["model"], str) or not usage["model"]
            or any(type(usage[key]) is not int or not 0 <= usage[key] <= 100_000_000
                   for key in ("prompt_token_count", "candidates_token_count", "total_token_count"))):
        raise EditorialOperationError("editorial_invalid_result")
    return edition, rendered, usage


async def count_ready_editions(session, edition_date: date) -> int:
    count = await session.scalar(text("""
        SELECT count(*) FROM editorial_digest_editions
        WHERE edition_date=:edition_date AND locale='ru'
    """), {"edition_date": edition_date})
    return int(count or 0)


async def latest_ready_edition(session, edition_date: date):
    result = await session.execute(text("""
        SELECT id, snapshot, edition, rendered, provider_usage, edition_date, locale
        FROM editorial_digest_editions
        WHERE edition_date=:edition_date AND locale='ru'
        ORDER BY created_at DESC, id DESC LIMIT 1
    """), {"edition_date": edition_date})
    return result.mappings().one_or_none()


async def generate_and_persist(record, context, fence,
                               revalidate: Callable[[], Awaitable[Any]]) -> int:
    """Return 1 for an existing/new edition, 0 for a genuinely empty daily snapshot."""
    async with AsyncSessionLocal() as session:
        async with fence.transaction_fence(session, fence.job_id, fence.attempt_generation):
            result = await session.execute(text("""
                SELECT id FROM editorial_digest_editions
                WHERE job_id=:job_id AND attempt_generation=:attempt_generation AND locale='ru'
                LIMIT 1
            """), {"job_id": uuid.UUID(record.job_id), "attempt_generation": record.attempt_generation})
            existing = result.scalar_one_or_none()
            await session.rollback()
    if existing is not None:
        return 1

    cutoff = datetime.now(timezone.utc)
    edition_date = cutoff.date()
    edition_id = _edition_id(record.job_id, record.attempt_generation, record.operation_id)
    cutoff_text = cutoff.isoformat(timespec="microseconds").replace("+00:00", "Z")
    base_url = settings.site_base_url.rstrip("/")
    edition_url = _edition_url(edition_date, edition_id)
    async with AsyncSessionLocal() as session:
        async with fence.transaction_fence(session, fence.job_id, fence.attempt_generation):
            rows = await _fetch_snapshot_rows(session, edition_date)
            await session.rollback()
    articles = _snapshot_articles(rows)
    if not articles:
        return 0
    snapshot = {
        "date": edition_date.isoformat(), "cutoff_utc": cutoff_text,
        "site_base_url": base_url, "edition_url": edition_url, "articles": articles,
    }
    snapshot_bytes = _validate_snapshot(snapshot)
    request = {"operation": "generate", "locale": LOCALE, "snapshot": snapshot}
    if len(snapshot_bytes) > MAX_SNAPSHOT_BYTES:
        raise EditorialOperationError("editorial_resource_limit")
    result = await run_editorial_cli(request, record.deadline)
    edition, rendered, usage = _validate_cli_result(result, snapshot)
    await revalidate()

    async with AsyncSessionLocal() as session:
        async with fence.transaction_fence(session, fence.job_id, fence.attempt_generation):
            inserted = await session.execute(text("""
                INSERT INTO editorial_digest_editions
                    (id, edition_date, locale, policy_version, snapshot_hash, snapshot,
                     edition, rendered, provider_usage, job_id, attempt_generation)
                VALUES
                    (:id, :edition_date, 'ru', :policy_version, :snapshot_hash,
                     CAST(:snapshot AS JSONB), CAST(:edition AS JSONB), CAST(:rendered AS JSONB),
                     CAST(:provider_usage AS JSONB), :job_id, :attempt_generation)
                ON CONFLICT (job_id, attempt_generation, locale) DO NOTHING
                RETURNING id
            """), {
                "id": edition_id, "edition_date": edition_date, "policy_version": POLICY_VERSION,
                "snapshot_hash": edition["source_snapshot_hash"],
                "snapshot": snapshot_bytes.decode("utf-8"),
                "edition": json.dumps(edition, ensure_ascii=False, separators=(",", ":")),
                "rendered": json.dumps(rendered, ensure_ascii=False, separators=(",", ":")),
                "provider_usage": json.dumps(usage, ensure_ascii=False, separators=(",", ":")),
                "job_id": uuid.UUID(record.job_id), "attempt_generation": record.attempt_generation,
            })
            inserted_id = inserted.scalar_one_or_none()
            if inserted_id is None:
                # Another authorized operation won the unique idempotency race. Its row is immutable.
                existing = await session.execute(text("""
                    SELECT id FROM editorial_digest_editions
                    WHERE job_id=:job_id AND attempt_generation=:attempt_generation AND locale='ru'
                    LIMIT 1
                """), {"job_id": uuid.UUID(record.job_id), "attempt_generation": record.attempt_generation})
                inserted_id = existing.scalar_one_or_none()
                if inserted_id is None:
                    raise EditorialOperationError("editorial_generation_failed")
            await session.commit()
    return 1
