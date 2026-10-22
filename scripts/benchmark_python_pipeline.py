"""Replay extraction fixtures using production Python helpers without external I/O.

This measures extraction components, not the deployed service or browser process.
Recorded rendered markup replaces CDP transport. Network and database adapters
are replaced at their boundaries; parsing and strategy selection remain real.
"""

import argparse
import asyncio
from contextlib import ExitStack
from dataclasses import asdict
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import socket
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs4 import BeautifulSoup
import feedparser
import psutil
from readability.readability import Document

from news_aggregator.config import settings
from news_aggregator.core.resource_limits import ResourceLimitError
from news_aggregator.extraction.date_extractor import DateExtractor
from news_aggregator.extraction.core_extractor import CoreExtractor
from news_aggregator.extraction.extraction_strategies import ExtractionStrategies
from news_aggregator.extraction.extraction_utils import ExtractionUtils
from news_aggregator.extraction.html_processor import HTMLProcessor
from news_aggregator.extraction.metadata_extractor import MetadataExtractor


def matches(expected, actual):
    """Compare required object fields without losing array count/order checks."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(key in actual and matches(value, actual[key])
                                               for key, value in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(expected) == len(actual) and all(
            matches(left, right) for left, right in zip(expected, actual))
    return expected == actual


def check_expected(case, result):
    errors = []
    expected = case.get("expected", {})
    if expected.get("state") not in {"extracted", "failed", "resource_limit"}:
        return ["case requires an explicit supported expected state"]
    if not expected or not any(key in expected for key in ("contains", "fields", "state", "method")):
        return ["case has no meaningful expected outcome"]
    if not result.get("content") and not expected.get("state"):
        errors.append("empty result requires an explicit expected failure state")
    for value in expected.get("contains", []):
        if value not in (result.get("content") or ""):
            errors.append(f"missing content: {value!r}")
    for value in expected.get("not_contains", []):
        if value in (result.get("content") or ""):
            errors.append(f"unexpected content: {value!r}")
    for key, value in expected.get("fields", {}).items():
        if not matches(value, result.get(key)):
            errors.append(f"{key}: expected {value!r}, got {result.get(key)!r}")
    for key in ("method", "state"):
        if key in expected and result.get(key) != expected[key]:
            errors.append(f"{key}: expected {expected[key]!r}, got {result.get(key)!r}")
    if expected.get("error") and expected["error"] not in (result.get("error") or ""):
        errors.append(f"missing error: {expected['error']!r}")
    for value in expected.get("trace_contains", []):
        if value not in result.get("trace", []):
            errors.append(f"missing trace: {value!r}")
    return errors


def metadata(html, utils):
    soup = BeautifulSoup(html or "", "html.parser")
    extractor = MetadataExtractor(utils)
    return {
        "title": extractor.extract_meta_title(soup),
        "description": extractor.extract_meta_description(soup),
        "publication_date": DateExtractor(utils).extract_publication_date(soup)[0],
        "author": extractor.extract_author_info(soup),
        "image_url": extractor.extract_primary_image(soup),
    }


def article_fields(article):
    if article is None:
        return {"content": ""}
    value = asdict(article)
    value["content"] = value.get("content") or value.get("summary") or ""
    date = value.pop("published_at", None)
    value["publication_date"] = date.isoformat() if isinstance(date, datetime) else date
    value["media"] = value.pop("media_files", None) or []
    value["author"] = (value.get("raw_data") or {}).get("author") or value.get("author")
    value["description"] = value.get("summary")
    return value


async def replay(case):
    data = case["input"]
    strategy = data.get("strategy", "auto")
    trace = []
    blocked_io = []
    utils = ExtractionUtils()
    def retained_source_text():
        value = data.get("content", "")
        return utils.finalize_content(value) if case.get("source_type", "") in {"", "article", "web"} else value
    processor = HTMLProcessor(utils)
    meta = MetadataExtractor(utils)
    extractor = ExtractionStrategies(utils, processor, DateExtractor(utils), meta)
    for attr, name in (("extract_by_enhanced_selectors", "enhanced_selectors"),
                       ("extract_by_enhanced_heuristics", "enhanced_heuristics")):
        original = getattr(processor, attr)
        def observed(*args, _original=original, _name=name, **kwargs):
            trace.append(_name)
            return _original(*args, **kwargs)
        setattr(processor, attr, observed)
    html = data.get("html", "")
    rendered = data.get("rendered_html", "")
    url = data.get("url", "https://fixture.invalid/article")
    result = {"url": url, "content": "", "method": strategy, "trace": trace}
    limits = data.get("limits") or {}
    frozen = None
    if data.get("now"):
        frozen = datetime.fromisoformat(data["now"].replace("Z", "+00:00"))
        if frozen.tzinfo is None:
            raise ValueError("Replay clock must include a timezone")
    class ReplayClock(datetime):
        @classmethod
        def utcnow(cls):
            return frozen.astimezone(timezone.utc).replace(tzinfo=None)
        @classmethod
        def now(cls, tz=None):
            return frozen.astimezone(tz) if tz else frozen.astimezone(timezone.utc).replace(tzinfo=None)
    max_bytes = limits.get("max_bytes", data.get("max_bytes", 5 * 1024 * 1024))
    max_chars = limits.get("max_chars", data.get("max_chars", 100000))
    for key in ("html", "rendered_html", "rss_xml", "telegram_html", "reddit_json", "linked_html", "content"):
        if len(data.get(key, "").encode("utf-8")) > max_bytes:
            return {**result, "state": "resource_limit", "error": f"{key} byte cap exceeded"}
    for value in data.get("alternative_html", {}).values():
        if len(value.encode("utf-8")) > max_bytes:
            return {**result, "state": "resource_limit", "error": "alternative_html byte cap exceeded"}
    learned = None
    if data.get("learned_selector"):
        learned = {"selector": data["learned_selector"], "method": data.get("learned_method", "css")}
    memory = SimpleNamespace(get_successful_pattern=AsyncMock(return_value=learned),
                             record_extraction_attempt=AsyncMock(),
                             degrade_pattern=AsyncMock())
    async def fetch(request_url):
        trace.append("http")
        if request_url in data.get("alternative_html", {}):
            return data["alternative_html"][request_url]
        return data.get("linked_html", "") if request_url != utils.clean_url(url) else html
    async def browser(request_url):
        trace.append("browser_rendering")
        if data.get("cancel_at") == "browser_html_capture":
            raise asyncio.CancelledError()
        soup = BeautifulSoup(rendered, "html.parser")
        content, selector = processor.extract_by_enhanced_selectors(soup)
        if not content:
            content = processor.extract_by_enhanced_heuristics(soup)
            selector = "heuristics"
        return content, selector, rendered
    extractor.fetch_html_content = fetch
    extractor.fetch_html_content_fallback = fetch
    extractor._extract_with_browser_and_html = browser
    extractor.record_extraction_success = AsyncMock()
    extractor.record_extraction_failure = AsyncMock()
    with ExitStack() as stack:
        def deny_network(*args, **kwargs):
            blocked_io.append("network_attempt")
            raise RuntimeError("External network is forbidden in fixture replay")
        stack.enter_context(patch.object(socket.socket, "connect", deny_network))
        stack.enter_context(patch.object(socket.socket, "connect_ex", deny_network))
        stack.enter_context(patch.object(socket, "create_connection", deny_network))
        stack.enter_context(patch.object(settings, "extraction_text_max_chars", max_chars))
        stack.enter_context(patch("news_aggregator.extraction.extraction_strategies.get_extraction_memory",
                                  AsyncMock(return_value=memory)))
        stack.enter_context(patch("news_aggregator.extraction.core_extractor.get_extraction_memory",
                                  AsyncMock(return_value=memory)))
        tracker = SimpleNamespace(update_domain_stats=lambda **kwargs: None)
        stack.enter_context(patch("news_aggregator.services.domain_stability_tracker.get_stability_tracker",
                                  AsyncMock(return_value=tracker)))
        class ReplayHTTP:
            status = 200
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            async def get(self, *args, **kwargs):
                return self
        stack.enter_context(patch("news_aggregator.extraction.extraction_strategies.get_http_client", ReplayHTTP))
        stack.enter_context(patch("news_aggregator.extraction.extraction_strategies.read_page_bytes",
                                  AsyncMock(return_value=data.get("reddit_json", "").encode("utf-8"))))
        try:
            if data.get("cancel_at") not in {None, "browser_html_capture"}:
                raise ValueError("Unsupported cancellation injection point")
            if len(data.get("content", "")) > max_chars:
                raise ResourceLimitError("Replay text cap exceeded")
            if strategy == "source_content":
                result["content"] = retained_source_text()
                trace.append("source_content_retained")
            elif strategy in {"rss", "atom"}:
                from news_aggregator.sources.base import SourceInfo, SourceType
                from news_aggregator.sources.rss_source import RSSSource
                if frozen:
                    stack.enter_context(patch("news_aggregator.sources.rss_source.datetime", ReplayClock))
                source = RSSSource(SourceInfo(case.get("source_name", "Fixture"), SourceType.RSS, url, "Fixture"))
                entries = feedparser.parse(data["rss_xml"].encode("utf-8")).entries
                index = data.get("entry_index", 0)
                result.update(article_fields(source._parse_entry(entries[index]) if index < len(entries) else None))
                if index < len(entries):
                    result["method"] = "rss_content" if entries[index].get("content") else "rss_entry"
            elif strategy in {"telegram_http", "telegram_browser"}:
                from news_aggregator.telegram.message_parser import MessageParser
                if frozen:
                    stack.enter_context(patch("news_aggregator.telegram.message_parser.datetime", ReplayClock))
                parser = MessageParser(case.get("source_name", "Fixture"), "fixture")
                async def expand(link, original):
                    if not data.get("linked_html"):
                        return None
                    body = BeautifulSoup(Document(data["linked_html"]).summary(html_partial=True),
                                         "html.parser").get_text(" ", strip=True)
                    return body if len(body) > len(original) * 2 else None
                parser._try_extract_full_content = expand
                soup = BeautifulSoup(data.get("telegram_html") or rendered or html, "html.parser")
                entries = soup.select(".tgme_widget_message") or soup.select(".message") or soup.select("[data-post]")
                index = data.get("entry_index", 0)
                article = await parser.parse_message_element(entries[index], url, soup) if index < len(entries) else None
                result.update(article_fields(article))
            elif strategy in {"custom_selectors", "ai_selectors"} or (
                    case.get("source_type") == "custom" and strategy == "browser_rendering"):
                from news_aggregator.sources.page_monitor_source import PageMonitorSource, PageMonitorConfig
                if frozen:
                    stack.enter_context(patch("news_aggregator.sources.page_monitor_source.datetime", ReplayClock))
                selectors = data.get("selectors", {})
                config = PageMonitorConfig(url=url, name=case.get("source_name", "Fixture"),
                    use_browser=False, enable_ai_analysis=False,
                    article_selectors=selectors.get("article"), title_selectors=selectors.get("title"),
                    link_selectors=selectors.get("link"), date_selectors=selectors.get("date"))
                source = PageMonitorSource(config)
                source._study_source_structure = AsyncMock()
                if strategy == "ai_selectors":
                    source.learned_container_selectors = selectors.get("article", [])
                browser_mode = data.get("use_browser", strategy == "browser_rendering" or (
                    strategy == "custom_selectors" and bool(rendered)))
                entries = await source._extract_articles_from_html(rendered if browser_mode else html)
                index = data.get("entry_index", 0)
                if index < len(entries):
                    articles = await source._convert_extracted_to_articles([entries[index]])
                    result.update(article_fields(articles[0]))
            elif strategy in {"auto", "learned_pattern", "learned_pattern_browser_rendering",
                              "reddit_json", "reddit_linked_article"}:
                domain = urlparse(url).netloc.lower()
                if strategy == "auto":
                    value = await CoreExtractor(utils, processor, extractor).extract_article_content_with_metadata(url, retry_count=1)
                else:
                    value = await extractor.attempt_extraction_with_metadata(url, domain, 1)
                result.update(value)
                result["method"] = value.get("method_used") or "failed"
                if value.get("method_used"):
                    trace.append(value["method_used"])
                if strategy == "auto" and not result.get("content") and data.get("content"):
                    # Model the caller's retention of already-fetched source
                    # text after extraction fails; this is not quality approval.
                    result["content"] = retained_source_text()
                    result["method"] = "source_content"
                    trace.append("source_content_fallback")
            else:
                result.update(metadata(rendered if strategy.startswith("browser") else html, utils))
                soup = BeautifulSoup(rendered if strategy.startswith("browser") else html, "html.parser")
                if strategy == "enhanced_selectors":
                    result["content"], result["selector_used"] = processor.extract_by_enhanced_selectors(soup)
                elif strategy == "readability":
                    result["content"] = processor.clean_text(BeautifulSoup(Document(html).summary(), "html.parser").get_text(" ", strip=True))
                elif strategy in {"browser_rendering", "browser_heuristics"}:
                    result["content"] = (await browser(url))[0] if strategy == "browser_rendering" else processor.extract_by_enhanced_heuristics(soup)
                elif strategy == "json_ld":
                    result["content"] = meta.extract_from_json_ld(soup)
                elif strategy in {"open_graph", "meta_description"}:
                    result["content"] = meta.extract_from_open_graph(soup)
                elif strategy == "enhanced_heuristics":
                    result["content"] = processor.extract_by_enhanced_heuristics(soup)
                elif strategy == "fallback_extraction":
                    result["content"] = (meta.extract_from_json_ld(soup) or meta.extract_from_open_graph(soup)
                                         or processor.extract_by_enhanced_heuristics(soup))
                else:
                    raise ValueError(f"Unsupported Python replay strategy: {strategy}")
                if strategy in {"json_ld", "open_graph", "meta_description", "enhanced_heuristics"}:
                    result["method"] = "fallback_extraction"
                # Forced components must pass the same acceptance and finalization
                # boundaries as article candidates, rather than comparing raw
                # Python text with finalized Go output.
                candidate = result.get("content") or ""
                result["candidate_chars"] = len(candidate)
                full_article = strategy in {"enhanced_selectors", "browser_rendering", "browser_heuristics"}
                accepted = utils.is_good_content(candidate, is_full_article=full_article)
                if accepted and strategy in {"enhanced_selectors", "readability", "browser_rendering", "browser_heuristics"}:
                    accepted = await extractor._is_high_quality_content(candidate, url=url)
                if not accepted:
                    result["content"] = ""
                    result["error"] = "quality_rejected"
                    trace.append(strategy + ":quality_rejected")
                else:
                    result["original_chars"] = len(candidate)
                    result["content"] = utils.finalize_content(candidate)
                    result["truncated"] = len(result["content"]) < len(candidate)
                    if result["truncated"]:
                        result["truncation_policy"] = "sentence_or_word_8000"
                        trace.append("finalize_truncated")
            content = result.get("content") or ""
            if "M04" in case.get("methods", []):
                from news_aggregator.processing.media_extractor import MediaExtractor
                result["media"] = MediaExtractor().extract_media_files(html, url)
            if len(content) > max_chars:
                raise ResourceLimitError("Replay text cap exceeded")
            result["content"] = content
            media_only_source = strategy in {"telegram_http", "telegram_browser"} or case.get("source_type") == "telegram"
            result["state"] = "extracted" if content or (media_only_source and result.get("media")) else "failed"
            trace.append(strategy)
            if blocked_io:
                raise RuntimeError("Replay isolation violation: production helper attempted external network")
        except asyncio.CancelledError:
            result.update(content="", state="failed", error="cancelled")
            trace.append("cancelled")
        except ResourceLimitError as exc:
            result.update(content="", state="resource_limit", error=str(exc))
    return result


async def run(args):
    manifest = json.loads(Path(args.corpus).read_text(encoding="utf-8"))
    if manifest.get("version") != 1 or not manifest.get("cases"):
        raise ValueError("Expected version 1 with nonempty cases")
    if args.cycles < 1:
        raise ValueError("cycles must be positive")
    ids = [case["id"] for case in manifest["cases"]]
    if len(set(ids)) != len(ids):
        raise ValueError("Fixture identifiers must be unique")
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    failures = 0
    process = psutil.Process()
    with output.open("w", encoding="utf-8") as file:
        for cycle in range(args.cycles):
            for case in manifest["cases"]:
                started = time.perf_counter()
                try:
                    result = await replay(case)
                    errors = check_expected(case, result)
                except Exception as exc:
                    result = {"state": "unsupported", "error": str(exc)}
                    errors = [f"{type(exc).__name__}: {exc}"]
                failures += bool(errors)
                row = {"id": case["id"], "cycle": cycle + 1, "result": result, "errors": errors,
                       "duration_seconds": time.perf_counter() - started,
                       "rss_bytes": process.memory_info().rss}
                file.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    print(json.dumps({"cases": len(manifest["cases"]), "cycles": args.cycles,
                      "failures": failures, "scope": "component replay, no live browser/network/DB"}))
    return 1 if failures else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--output", required=True)
    raise SystemExit(asyncio.run(run(parser.parse_args())))
