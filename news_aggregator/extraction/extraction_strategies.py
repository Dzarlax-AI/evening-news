import logging
"""Different strategies for content extraction from web pages."""

import asyncio
import html as html_lib
import time
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

import aiohttp
import chardet
from bs4 import BeautifulSoup
from readability.readability import Document
import nodriver as uc
from nodriver import cdp

from ..core.http_client import get_http_client
from ..core.exceptions import BrowserUnavailableError, ContentExtractionError
from ..services.extraction_memory import get_extraction_memory, ExtractionAttempt
from ..services.domain_stability_tracker import get_stability_tracker
from ..services.ai_extraction_optimizer import get_ai_extraction_optimizer
from ..services.extraction_constants import (
    BROWSER_CONCURRENCY,
    BROWSER_TIMEOUT_FIRST_MS,
    BROWSER_TIMEOUT_RETRY_MS,
    BROWSER_TOTAL_BUDGET_MS,
)

from .extraction_utils import ExtractionUtils
from .html_processor import HTMLProcessor
from .date_extractor import DateExtractor
from .metadata_extractor import MetadataExtractor

logger = logging.getLogger(__name__)


class ExtractionStrategies:
    """Various strategies for extracting content from web pages."""

    def __init__(
        self,
        utils: ExtractionUtils,
        html_processor: HTMLProcessor,
        date_extractor: DateExtractor,
        metadata_extractor: MetadataExtractor,
    ):
        self.utils = utils
        self.html_processor = html_processor
        self.date_extractor = date_extractor
        self.metadata_extractor = metadata_extractor

        # Browser management is now handled centrally via browser_pool.browser_tab()
        # which provides global serialization and ensures proper tab lifecycle.

    def _normalize_learned_pattern_method(self, original_method: Optional[str]) -> str:
        """Collapse repeated learned-pattern prefixes into a single, stable name."""
        prefix = "learned_pattern_"
        if not original_method:
            return f"{prefix}unknown"

        method = str(original_method).strip()
        while method.startswith(prefix):
            method = method[len(prefix) :]

        if not method:
            method = "unknown"

        return f"{prefix}{method}"

    async def _is_high_quality_content(
        self, content: str, title: str = "", url: str = ""
    ) -> bool:
        """Check if extracted content passes quality checks using Smart Filter."""
        if not content or not content.strip():
            return False

        try:
            from ..services.smart_filter import get_smart_filter

            smart_filter = get_smart_filter()

            # Use Smart Filter to check content quality
            # We use allow_extraction=False to get strict quality assessment
            should_process, reason = await smart_filter.should_process_with_ai(
                title=title or "Extracted Content",
                content=content,
                url=url,
                source_type="extraction",
                allow_extraction=False,
            )

            if not should_process and "Metadata/low-quality content detected" in reason:
                logger.error(f"    ❌ Content failed quality check: {reason}")
                return False

            return True

        except Exception as e:
            logger.warning(f"    ⚠️ Quality check failed: {e}, assuming content is valid")
            return True  # Fail open - if quality check fails, assume content is OK

    async def _extract_reddit_json(self, url: str) -> Optional[Dict[str, Optional[str]]]:
        """Extract Reddit post content via the public JSON API.

        Returns a result dict (same shape as attempt_extraction_with_metadata) or None
        if the URL is not a Reddit post or extraction fails.
        """
        import re
        # Only handle reddit.com comment/post URLs
        if not re.search(r'reddit\.com/r/\w+/comments/', url):
            return None

        json_url = url.rstrip('/') + '.json'
        try:
            async with get_http_client() as client:
                async with await client.get(
                    json_url,
                    headers={**self.utils.get_headers(), 'Accept': 'application/json'},
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"    ⚠️ Reddit JSON API returned {resp.status}")
                        return None
                    data = await resp.json(content_type=None)
        except Exception as e:
            logger.warning(f"    ⚠️ Reddit JSON fetch failed: {e}")
            return None

        try:
            post = data[0]['data']['children'][0]['data']
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"    ⚠️ Reddit JSON parse failed: {e}")
            return None

        title = post.get('title', '')
        selftext = post.get('selftext', '').strip()
        post_url = post.get('url', '')
        post_hint = post.get('post_hint', '')
        is_self = post.get('is_self', False)
        author = post.get('author', '')
        score = post.get('score', 0)
        created_utc = post.get('created_utc')

        # Extract image URL
        image_url = None
        if post_hint == 'image' or (post_url and re.search(r'\.(jpg|jpeg|png|gif|webp)$', post_url, re.I)):
            image_url = post_url
        # Prefer preview image (higher quality, always available)
        preview_images = post.get('preview', {}).get('images', [])
        if preview_images:
            raw = preview_images[0].get('source', {}).get('url', '')
            if raw:
                image_url = html_lib.unescape(raw)

        # Top comments for context (skip bots and auto-moderator)
        comments_text = ''
        try:
            top_comments = data[1]['data']['children'][:10]
            comment_lines = []
            bot_authors = {'automoderator', 'automod', 'bot'}
            for c in top_comments:
                cdata = c.get('data', {})
                body = cdata.get('body', '').strip()
                cauthor = (cdata.get('author') or '').lower()
                if (body
                        and body not in ('[deleted]', '[removed]')
                        and cauthor not in bot_authors
                        and not cdata.get('distinguished')  # skip mod-distinguished
                        and len(body) > 20):
                    comment_lines.append(body)
                if len(comment_lines) >= 5:
                    break
            if comment_lines:
                comments_text = '\n\n'.join(comment_lines)
        except Exception:
            pass

        # Build content depending on post type
        if is_self and selftext:
            content = f"{title}\n\n{selftext}"
        elif post_hint == 'image' or not selftext:
            # Image post or bare link — use title + comments
            content = title
            if comments_text:
                content += f"\n\nКомментарии:\n{comments_text}"
        else:
            content = f"{title}\n\n{selftext}"
            if comments_text:
                content += f"\n\nКомментарии:\n{comments_text}"

        if not content.strip():
            return None

        # Publication date from created_utc
        pub_date = None
        if created_utc:
            from datetime import datetime, timezone
            pub_date = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        logger.info(f"    ✅ Reddit JSON: post_hint={post_hint!r}, content={len(content)} chars, image={'yes' if image_url else 'no'}")
        return {
            'content': content,
            'title': title,
            'author': author,
            'publication_date': pub_date,
            'description': selftext[:200] if selftext else None,
            'image_url': image_url,
            'method_used': 'reddit_json',
            'selector_used': 'reddit_json',
            # For link posts, expose the linked URL so caller can optionally extract it
            '_reddit_link_url': post_url if not is_self and post_hint not in ('image',) else None,
        }

    async def attempt_extraction_with_metadata(
        self, url: str, domain: str, attempt_num: int
    ) -> Dict[str, Optional[str]]:
        """Attempt content extraction with comprehensive metadata extraction."""
        logger.info(f"    🔄 Extraction attempt #{attempt_num} for domain: {domain}")
        result = {
            "content": None,
            "title": None,
            "publication_date": None,
            "author": None,
            "description": None,
            "image_url": None,
            "method_used": None,
        }

        # Strategy 0: Reddit JSON API (before any HTML fetch)
        if 'reddit.com' in domain:
            reddit_result = await self._extract_reddit_json(url)
            if reddit_result and reddit_result.get('content'):
                result.update(reddit_result)
                # For link posts, try to also extract the linked article
                link_url = reddit_result.get('_reddit_link_url')
                if link_url:
                    logger.info(f"    🔗 Reddit link post — extracting linked URL: {link_url}")
                    try:
                        linked_html = await self.fetch_html_content(link_url)
                        if linked_html:
                            linked_soup = BeautifulSoup(linked_html, "html.parser")
                            linked_content, _ = self.html_processor.extract_by_enhanced_selectors(linked_soup)
                            if linked_content and self.utils.is_good_content(linked_content, is_full_article=True):
                                result['content'] = linked_content
                                if not result.get('image_url'):
                                    result['image_url'] = self.metadata_extractor.extract_primary_image(linked_soup)
                    except Exception as e:
                        logger.warning(f"    ⚠️ Reddit linked URL extraction failed: {e}")
                return result

        # Fetch HTML once and reuse across strategies 1-3
        shared_html: Optional[str] = None
        shared_soup = None
        try:
            shared_html = await self.fetch_html_content(url)
            if shared_html:
                shared_soup = BeautifulSoup(shared_html, "html.parser")
        except Exception as e:
            logger.warning(f"    ⚠️ Initial HTML fetch failed: {e}")

        def _fill_metadata(r: dict, soup) -> None:
            """Populate metadata fields from a BeautifulSoup object."""
            try:
                r["title"] = self.metadata_extractor.extract_meta_title(soup)
                pub_date, _ = self.date_extractor.extract_publication_date(soup)
                r["publication_date"] = pub_date
                r["author"] = self.metadata_extractor.extract_author_info(soup)
                r["description"] = self.metadata_extractor.extract_meta_description(soup)
                if not r.get("image_url"):
                    r["image_url"] = self.metadata_extractor.extract_primary_image(soup)
            except Exception as e:
                logger.warning(f"    ⚠️ Metadata extraction failed: {e}")

        # Strategy 1: Try learned patterns first (fastest)
        extraction_memory = await get_extraction_memory()
        learned_pattern = await extraction_memory.get_successful_pattern(domain)

        # If learned pattern requires browser, skip HTTP strategies and go straight to browser
        if learned_pattern and learned_pattern.get("method") == "browser_rendering":
            logger.info(f"    📚 Learned pattern for {domain} requires browser — jumping to Strategy 4")
            selector = learned_pattern.get("selector")
            try:
                content, sel, browser_html = await self._extract_with_browser_and_html(url)
                if content and self.utils.is_good_content(content, is_full_article=True):
                    # Try learned selector on browser-rendered HTML
                    if selector and browser_html:
                        b_soup = BeautifulSoup(browser_html, "html.parser")
                        elements = b_soup.select(selector)
                        if elements:
                            sel_content = self.html_processor.clean_text(
                                elements[0].get_text(separator=" ", strip=True)
                            )
                            if sel_content and len(sel_content) > len(content or ''):
                                content = sel_content
                                sel = selector
                    if await self._is_high_quality_content(content, url=url):
                        result["content"] = content
                        result["selector_used"] = sel
                        result["method_used"] = "learned_pattern_browser_rendering"
                        if browser_html:
                            _fill_metadata(result, BeautifulSoup(browser_html, "html.parser"))
                        await self.record_extraction_success(
                            domain, "browser_rendering", sel, len(content)
                        )
                        return result
            except Exception as e:
                logger.error(f"    ❌ Learned browser pattern failed: {e}")

        elif learned_pattern and shared_soup:
            logger.info(f"    📚 Trying learned pattern for {domain}")
            selector = learned_pattern.get("selector")
            try:
                elements = shared_soup.select(selector) if selector else []
                if elements:
                    content = self.html_processor.clean_text(
                        elements[0].get_text(separator=" ", strip=True)
                    )
                    if (
                        content
                        and self.utils.is_good_content(content, is_full_article=True)
                        and await self._is_high_quality_content(content, url=url)
                    ):
                        result["content"] = content
                        result["selector_used"] = selector
                        result["method_used"] = self._normalize_learned_pattern_method(
                            learned_pattern.get("method", "unknown")
                        )
                        _fill_metadata(result, shared_soup)
                        await self.record_extraction_success(
                            domain, result["method_used"], selector, len(content)
                        )
                        return result
                # Learned pattern didn't produce good content — degrade it
                logger.warning(f"    ⬇️ Learned pattern produced no/bad content for {domain}")
                await extraction_memory.degrade_pattern(
                    domain, selector, learned_pattern.get("method", "unknown")
                )
            except Exception as e:
                logger.error(f"    ❌ Learned pattern failed: {e}")
                if selector:
                    await extraction_memory.degrade_pattern(
                        domain, selector, learned_pattern.get("method", "unknown")
                    )

        # Strategy 2: Enhanced HTML selectors (cheaper than readability — try first)
        if shared_soup:
            try:
                logger.info(f"    🎯 Trying enhanced selectors")
                content, selector = self.html_processor.extract_by_enhanced_selectors(
                    shared_soup
                )
                if (
                    content
                    and self.utils.is_good_content(content, is_full_article=True)
                    and await self._is_high_quality_content(content, url=url)
                ):
                    result["content"] = content
                    result["selector_used"] = selector
                    result["method_used"] = "enhanced_selectors"
                    _fill_metadata(result, shared_soup)
                    await self.record_extraction_success(
                        domain, "enhanced_selectors", selector, len(content)
                    )
                    return result
            except Exception as e:
                logger.error(f"    ❌ Enhanced selectors failed: {e}")

        # Strategy 3: Readability (reuse already-fetched HTML)
        if shared_html:
            try:
                logger.info(f"    📖 Trying readability extraction")
                doc = Document(shared_html)
                readability_html = doc.summary()
                if readability_html:
                    r_soup = BeautifulSoup(readability_html, "html.parser")
                    text_content = r_soup.get_text(separator=" ", strip=True)
                    content = self.html_processor.clean_text(text_content)
                    if (
                        content
                        and self.utils.is_good_content(content)
                        and await self._is_high_quality_content(content, url=url)
                    ):
                        result["content"] = content
                        result["selector_used"] = "readability"
                        result["method_used"] = "readability"
                        if shared_soup:
                            _fill_metadata(result, shared_soup)
                        await self.record_extraction_success(
                            domain, "readability", "readability", len(content)
                        )
                        return result
            except Exception as e:
                logger.error(f"    ❌ Readability extraction failed: {e}")
        # Strategy 4: Browser rendering (more expensive) — extracts metadata in same session
        try:
            logger.info(f"    🎭 Trying browser rendering")
            content, selector, browser_html = await self._extract_with_browser_and_html(url)
            if (
                content
                and self.utils.is_good_content(content, is_full_article=True)
                and await self._is_high_quality_content(content, url=url)
            ):
                result["content"] = content
                result["selector_used"] = selector
                result["method_used"] = "browser_rendering"
                await self.record_extraction_success(
                    domain, "browser_rendering", selector, len(content)
                )

                # Use HTML from the same browser session — no second request
                if browser_html:
                    try:
                        b_soup = BeautifulSoup(browser_html, "html.parser")
                        _fill_metadata(result, b_soup)
                    except Exception as e:
                        logger.warning(f"    ⚠️ Browser metadata extraction failed: {e}")
                        # Fallback: metadata from initial shared_soup if available
                        if shared_soup:
                            _fill_metadata(result, shared_soup)

                return result
        except Exception as e:
            logger.error(f"    ❌ Browser rendering failed: {e}")

        # Strategy 5: Fallback to basic text extraction
        try:
            logger.info(f"    🔄 Trying fallback extraction")
            # Reuse already-fetched HTML when possible; only use sync fallback if needed
            fallback_html = shared_html or await self.fetch_html_content_fallback(url)
            if fallback_html:
                soup = BeautifulSoup(fallback_html, "html.parser")

                res = (
                    self.metadata_extractor.extract_from_json_ld(soup)
                    or self.metadata_extractor.extract_from_open_graph(soup)
                    or self.html_processor.extract_by_enhanced_heuristics(soup)
                )

                content = None
                selector = "fallback"
                if isinstance(res, tuple):
                    content, selector = res
                else:
                    content = res

                if content and self.utils.is_good_content(content):
                    result["content"] = content
                    result["selector_used"] = selector
                    result["method_used"] = "fallback_extraction"
                    _fill_metadata(result, soup)
                    await self.record_extraction_success(
                        domain, "fallback_extraction", selector, len(content)
                    )
                    return result
        except Exception as e:
            logger.error(f"    ❌ Fallback extraction failed: {e}")

        # All strategies failed — record failure for the domain
        await self.record_extraction_failure(domain, "all_strategies", "No strategy produced content")
        return result

    async def _extract_with_browser_and_html(
        self, url: str
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract content via browser and also return the rendered HTML for metadata.

        Returns:
            Tuple of (content, selector, page_html)
        """
        from ..core.browser_pool import browser_tab, run_browser_operation

        # Use browser_tab context manager for serialized, safe access
        async with browser_tab("about:blank") as tab:
            try:
                # Set user agent
                await run_browser_operation(
                    tab.send(cdp.network.set_user_agent_override(
                        user_agent="Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )),
                    "set user agent",
                    browser_session=tab,
                )

                # Block images, media, fonts to speed up loading
                await run_browser_operation(
                    tab.send(cdp.network.set_blocked_ur_ls(
                        urls=["*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
                              "*.mp4", "*.mp3", "*.webm", "*.ogg", "*.wav",
                              "*.woff", "*.woff2", "*.ttf", "*.eot"]
                    )),
                    "block resource URLs",
                    browser_session=tab,
                )

                budget_start = time.time()
                from urllib.parse import urlparse as _urlparse
                domain = _urlparse(url).netloc
                stability_tracker = await get_stability_tracker()
                adaptive_timeout = stability_tracker.get_method_timeout(
                    domain, "browser_rendering", BROWSER_TIMEOUT_FIRST_MS
                )
                adaptive_total_budget = min(adaptive_timeout * 3, BROWSER_TOTAL_BUDGET_MS)

                def remaining_s() -> float:
                    return max(0.0, adaptive_total_budget / 1000 - (time.time() - budget_start))

                navigation_budget = min(adaptive_timeout / 1000, remaining_s())
                if navigation_budget <= 0:
                    return None, None, None
                await run_browser_operation(
                    tab.get(url, new_tab=False),
                    "navigation",
                    timeout_seconds=navigation_budget,
                    browser_session=tab,
                )

                # Wait for content to appear (poll JS condition)
                try:
                    deadline = time.time() + min(10, remaining_s())
                    while time.time() < deadline:
                        evaluation_budget = remaining_s()
                        if evaluation_budget <= 0:
                            break
                        result = await run_browser_operation(
                            tab.evaluate("document.body ? document.body.innerText.length : 0"),
                            "content readiness evaluation",
                            timeout_seconds=evaluation_budget,
                            browser_session=tab,
                        )
                        if result and int(result) > 500:
                            break
                        await asyncio.sleep(0.5)
                except BrowserUnavailableError:
                    raise
                except Exception:
                    pass  # Continue even if content detection times out

                # Capture rendered HTML once for both content and metadata
                content_budget = remaining_s()
                if content_budget <= 0:
                    return None, None, None
                page_html = await run_browser_operation(
                    tab.get_content(),
                    "content retrieval",
                    timeout_seconds=content_budget,
                    browser_session=tab,
                )

                # Extract content from rendered HTML using existing logic
                content = None
                selector_used = None
                if page_html:
                    soup = BeautifulSoup(page_html, "html.parser")
                    content, selector_used = self.html_processor.extract_by_enhanced_selectors(soup)
                    if not content:
                        content = self.html_processor.extract_by_enhanced_heuristics(soup)
                        selector_used = "heuristics"

                return content, selector_used, page_html

            except Exception as e:
                raise ContentExtractionError(f"Browser extraction failed: {e}")

    @staticmethod
    def _decode_response_bytes(data: bytes, charset: Optional[str]) -> str:
        """Decode raw response bytes, falling back to chardet then replacing errors."""
        if charset:
            try:
                return data.decode(charset)
            except (UnicodeDecodeError, LookupError):
                pass
        detected = chardet.detect(data)
        enc = detected.get('encoding') or 'utf-8'
        return data.decode(enc, errors='replace')

    async def fetch_html_content(self, url: str) -> Optional[str]:
        """Fetch HTML content using shared HTTP client."""
        try:
            async with get_http_client() as client:
                async with await client.get(
                    url, headers=self.utils.get_headers()
                ) as response:
                    if response.status == 200:
                        data = await response.read()
                        return self._decode_response_bytes(data, response.charset)
                    else:
                        logger.warning(f"    ⚠️ HTTP {response.status} for {url}")
                        return None

        except Exception as e:
            logger.error(f"    ❌ HTML fetch failed: {e}")
            return None

    async def fetch_html_content_fallback(self, url: str) -> Optional[str]:
        """Fallback HTML fetching with a different User-Agent (async, non-blocking)."""
        try:
            import aiohttp
            fallback_headers = dict(self.utils.get_headers())
            fallback_headers["User-Agent"] = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
            )
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=fallback_headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        data = await response.read()
                        return self._decode_response_bytes(data, response.charset)
                    else:
                        logger.warning(f"    ⚠️ Fallback fetch HTTP {response.status} for {url}")
                        return None
        except Exception as e:
            logger.error(f"    ❌ Fallback fetch failed: {e}")
            return None

    async def record_extraction_success(
        self,
        domain: str,
        method: str,
        selector: Optional[str] = None,
        content_length: int = 0,
        extraction_time: float = 0,
    ):
        """Record successful extraction for learning."""
        try:
            extraction_memory = await get_extraction_memory()
            attempt = ExtractionAttempt(
                article_url="unknown",
                domain=domain,
                extraction_strategy=method,
                selector_used=selector,
                success=True,
                content_length=content_length,
                extraction_time_ms=int(extraction_time * 1000),
            )

            await extraction_memory.record_extraction_attempt(attempt)

        except Exception as e:
            logger.warning(f"    ⚠️ Failed to record success: {e}")
    async def record_extraction_failure(
        self, domain: str, method: str, error: str, selector: Optional[str] = None
    ):
        """Record failed extraction for learning."""
        try:
            extraction_memory = await get_extraction_memory()
            attempt = ExtractionAttempt(
                article_url="unknown",
                domain=domain,
                extraction_strategy=method,
                selector_used=selector,
                success=False,
                content_length=0,
                extraction_time_ms=0,
                error_message=error,
            )

            await extraction_memory.record_extraction_attempt(attempt)

        except Exception as e:
            logger.warning(f"    ⚠️ Failed to record failure: {e}")


    async def close_browser(self):
        """No-op — browser lifecycle is managed by the shared pool."""
        pass
