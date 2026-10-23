"""Core content extractor - main coordinator for all extraction strategies."""

import asyncio
import hashlib
import json
import logging
import time
import unicodedata
from urllib.parse import urlsplit, urlunsplit
from ..core.resource_limits import ResourceLimitError, enforce_text_limit
from typing import Optional, Dict, Any

from ..services.extraction_memory import get_extraction_memory, ExtractionAttempt
from ..services.domain_stability_tracker import get_stability_tracker

from .extraction_utils import ExtractionUtils
from .html_processor import HTMLProcessor
from .extraction_strategies import ExtractionStrategies
from .extraction_logger import get_extraction_logger

# Get structured logger
logger = logging.getLogger('extraction.core')

_BROWSER_LEARNED_METHODS = frozenset({
    'browser_rendering', 'browser_heuristics',
    'learned_pattern_browser_rendering', 'learned_pattern_browser_heuristics',
})


class CoreExtractor:
    """Core content extractor that coordinates all extraction strategies."""
    
    def __init__(self, utils: ExtractionUtils, html_processor: HTMLProcessor, strategies: ExtractionStrategies):
        self.utils = utils
        self.html_processor = html_processor
        self.strategies = strategies
        
        # Browser management (delegated to strategies)
        self.browser = None  # Will be managed by strategies
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_browser()
    
    async def close_browser(self):
        """Close browser if open."""
        await self.strategies.close_browser()

    def _is_valid_url(self, url: str) -> bool:
        """
        Validate URL format and scheme.

        Args:
            url: URL to validate

        Returns:
            True if URL is valid for extraction, False otherwise
        """
        if not url or not isinstance(url, str):
            return False

        # Check URL scheme
        if not url.startswith(('http://', 'https://')):
            return False

        # Basic URL format validation
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)

            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False

            # Scheme must be http or https
            if parsed.scheme not in ('http', 'https'):
                return False

            # Netloc must have a domain (at least one dot)
            if '.' not in parsed.netloc:
                return False

            # Skip known non-content URLs
            skip_patterns = [
                '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico',
                '.mp4', '.avi', '.mov', '.wmv', '.flv',
                '.mp3', '.wav', '.ogg', '.flac',
                '.zip', '.rar', '.tar', '.gz',
                '.pdf', '.doc', '.docx', '.xls', '.xlsx'
            ]

            lower_path = parsed.path.lower()
            if any(lower_path.endswith(pattern) for pattern in skip_patterns):
                return False

            return True

        except Exception:
            return False

    async def extract_article_content_with_metadata(self, url: str, retry_count: int = 3) -> Dict[str, Optional[str]]:
        from ..config import get_settings
        from .service_client import ExtractionServiceError
        self._service_previous = None
        try:
            result = await self._extract_article_content_routed(url, retry_count)
        except ExtractionServiceError as exc:
            self._service_previous = (url, None, exc)
            raise
        if get_settings().extraction_service_mode == 'go':
            self._service_previous = (url, result, None)
        return result

    async def _extract_article_content_routed(self, url: str, retry_count: int) -> Dict[str, Optional[str]]:
        """Route compatible article extraction; Python remains the default owner."""
        from ..config import get_settings
        from .service_client import ExtractionServiceClient, ExtractionServiceError

        settings = get_settings()
        mode = settings.extraction_service_mode
        native_mode = (mode == 'go'
                       and bool(getattr(settings, 'extraction_service_native_browser', False)))
        if mode == 'python' or not self._is_valid_url(url):
            return await self._extract_article_content_python(url, retry_count)

        supplied_pattern = None
        clean_url = None
        browser_eligible = False

        async def prepare_service_request():
            nonlocal supplied_pattern, clean_url, browser_eligible
            if clean_url is not None:
                return
            candidate_url = self.utils.clean_url(url)
            try:
                parsed_url = urlsplit(candidate_url)
                # Accessing port also validates malformed bracket/port syntax.
                port = parsed_url.port
                if parsed_url.scheme not in {'http', 'https'} or not parsed_url.hostname:
                    raise ValueError('invalid URL')
                browser_eligible = (native_mode and parsed_url.scheme.lower() == 'https'
                                    and (port is None or port == 443))
                clean_url = urlunsplit(parsed_url._replace(fragment=''))
            except (TypeError, ValueError):
                raise ExtractionServiceError('invalid_input') from None
            memory = await get_extraction_memory()
            pattern = await memory.get_successful_pattern(self.utils.extract_domain(clean_url))
            supplied_pattern = dict(pattern) if pattern else None

        async def call_service(timeout, *, use_browser=False, admission_timeout=None,
                               admission_phase=None):
            token = settings.extraction_service_token
            if not settings.extraction_service_url or not token:
                raise ExtractionServiceError('not_configured')
            await prepare_service_request()
            max_bytes = settings.extraction_http_max_bytes
            if use_browser:
                max_bytes = min(max_bytes, settings.extraction_browser_max_bytes)
            client = ExtractionServiceClient(settings.extraction_service_url, token.get_secret_value(),
                max_bytes=max_bytes, max_chars=settings.extraction_text_max_chars)
            learned_pattern = (self._request_learned_pattern(supplied_pattern, use_browser)
                               if native_mode else supplied_pattern)
            return await client.extract(clean_url, timeout_seconds=timeout, learned_pattern=learned_pattern,
                                        use_browser=use_browser,
                                        admission_timeout_seconds=admission_timeout,
                                        admission_phase=admission_phase)

        if mode == 'shadow':
            # Authoritative processing runs once. The service has no DB/LLM access;
            # discard its observations and never persist comparison results here.
            # This inline diagnostic still consumes the caller's outer deadline.
            # Production shadow needs an out-of-band observer lifecycle first.
            shadow_started = time.monotonic()
            result = await self._extract_article_content_python(url, retry_count)
            python_elapsed = time.monotonic() - shadow_started
            go_started = time.monotonic()
            comparison = None
            error_code = None
            try:
                async with asyncio.timeout(settings.extraction_service_timeout_seconds):
                    comparison = await call_service(settings.extraction_service_timeout_seconds)
            except (ExtractionServiceError, TimeoutError, ValueError) as exc:
                error_code = ('deadline_exceeded' if isinstance(exc, TimeoutError)
                              else exc.code if isinstance(exc, ExtractionServiceError) else 'invalid_configuration')
            summary = self._shadow_comparison(
                result, comparison, python_elapsed, time.monotonic() - go_started, error_code)
            # Production formatters keep message but may discard structured extras.
            logger.info('Extractor shadow comparison %s', json.dumps(summary, sort_keys=True), extra=summary)
            return result

        started = time.monotonic()
        total_budget = settings.extraction_service_total_budget_seconds
        total_deadline = asyncio.timeout(total_budget)
        admission_phase = {'value': None}
        try:
            async with total_deadline:
                failure = None
                response = None
                use_browser = False
                while True:
                    remaining = total_budget - (time.monotonic() - started)
                    if remaining <= 0:
                        raise ExtractionServiceError('deadline_exceeded', True)
                    failure = None
                    try:
                        # Finish any awaited request preparation before fixing the
                        # shared admission deadline for this service attempt.
                        await prepare_service_request()
                        remaining = total_budget - (time.monotonic() - started)
                        if remaining <= 0:
                            raise ExtractionServiceError('deadline_exceeded', True)
                        response = await call_service(
                            min(settings.extraction_service_timeout_seconds, remaining),
                            use_browser=use_browser, admission_timeout=remaining,
                            admission_phase=admission_phase)
                    except (ExtractionServiceError, ValueError) as exc:
                        code = self._service_error_label(
                            exc.code if isinstance(exc, ExtractionServiceError) else 'invalid_configuration')
                        logger.warning('Extractor service unavailable code=%s', code,
                            extra={'event': 'extractor_service_failed', 'code': code})
                        failure = (exc if isinstance(exc, ExtractionServiceError)
                                   else ExtractionServiceError('invalid_configuration'))
                        response = None
                    if response is not None and response['outcome'] == 'success':
                        result = dict(response['result'])
                        enforce_text_limit(result['content'])
                        if not self.utils.is_good_content(result['content']):
                            failure = ExtractionServiceError('invalid_content')
                        else:
                            result['method_used'] = result.get('method_used') or result.get('method') or 'go'
                            result['media_files'] = result.get('media') or []
                            final_url = response.get('final_url')
                            original_url = response.get('original_url')
                            try:
                                final_document = urlunsplit(urlsplit(final_url)._replace(fragment=''))
                                original_document = urlunsplit(urlsplit(original_url)._replace(fragment=''))
                            except (TypeError, ValueError):
                                final_document = original_document = None
                            if final_document and original_document and final_document != original_document:
                                result['full_article_url'] = final_url
                            await self._record_service_success(
                                url, response, time.monotonic() - started, supplied_pattern)
                            return result
                    elif failure is None:
                        error = response.get('error') or {}
                        failure = ExtractionServiceError(
                            error.get('code', 'no_content' if response['outcome'] == 'rejected' else response['outcome']),
                            error.get('retryable', False), error.get('retry_after_seconds'))

                    if (not use_browser and browser_eligible
                            and self._native_browser_retry_allowed(failure, response)):
                        reason = self._service_error_label(failure.code)
                        logger.info('Extractor native browser retry reason=%s', reason,
                            extra={'event': 'extractor_native_browser_retry', 'code': reason})
                        use_browser = True
                        continue
                    break
                allow_python_fallback = (settings.extraction_service_python_fallback
                                          and not native_mode)
                if not allow_python_fallback or not self._native_fallback_allowed(failure):
                    await self._record_service_failures(url, response, supplied_pattern)
                    raise failure
                # A study request may use the existing Python extraction path.
                # Only the remaining deadline is available; no parallel extraction,
                # durable retry, analysis, or DB outcome is started by this adapter.
                remaining = total_budget - (time.monotonic() - started)
                if remaining <= 0:
                    raise ExtractionServiceError('deadline_exceeded', True)
                reason = self._service_error_label(failure.code)
                logger.info('Extractor Python fallback reason=%s', reason,
                    extra={'event': 'extractor_python_fallback', 'code': reason})
                async with asyncio.timeout(remaining):
                    return await self._extract_article_content_python(url, retry_count)
        except TimeoutError as exc:
            if total_deadline.expired() and admission_phase['value'] == 'waiting':
                raise ExtractionServiceError('busy', True) from None
            raise ExtractionServiceError('deadline_exceeded', True) from None

    @staticmethod
    def _request_learned_pattern(pattern, use_browser):
        if pattern and not use_browser and pattern.get('method') in _BROWSER_LEARNED_METHODS:
            return None
        return pattern

    @staticmethod
    def _native_fallback_allowed(failure):
        """A positive allowlist keeps admission/rate/config failures out of native retries."""
        return not failure.terminal and failure.code in {
            'browser_required', 'no_content', 'needs_selector_study', 'upstream_forbidden', 'challenge',
            'invalid_content', 'transport_error', 'transport_failed', 'unavailable', 'upstream_unavailable',
            'deadline_exceeded'}

    @staticmethod
    def _native_browser_retry_allowed(failure, response):
        """Retry only classified extraction outcomes from a valid Go response."""
        return (response is not None and not failure.terminal and failure.code in {
            'browser_required', 'no_content', 'needs_selector_study', 'upstream_forbidden',
            'challenge', 'invalid_content'})

    @staticmethod
    def _service_error_label(code):
        errors = {'deadline_exceeded', 'transport_error', 'transport_failed', 'busy', 'unavailable', 'unauthorized',
                  'invalid_request', 'invalid_input', 'unsupported_input', 'resource_limit', 'invalid_content',
                  'request_too_large', 'response_too_large', 'invalid_payload', 'upstream_rate_limited',
                  'upstream_unavailable', 'browser_unavailable', 'cancelled', 'http_error',
                  'not_configured', 'invalid_configuration', 'browser_required', 'no_content',
                  'needs_selector_study', 'upstream_forbidden', 'challenge', 'public_url_denied', 'upstream_auth_required',
                  'admission_unavailable', 'native_browser_policy_denied', 'native_browser_unsupported',
                  'native_browser_challenge', 'native_browser_no_content', 'native_browser_http_rejected',
                  'native_browser_unavailable', 'native_browser_timeout', 'native_browser_busy'}
        return code if code in errors else 'other' if code else 'none'

    @staticmethod
    def _shadow_comparison(python_result, response, python_elapsed, go_elapsed, error_code):
        """Emit bounded labels and comparison facts, never source values or digests."""
        def normalized(value):
            return ' '.join(unicodedata.normalize('NFC', value or '').split())

        def method_label(value):
            methods = {'readability', 'enhanced_selectors', 'browser_rendering', 'fallback_extraction',
                       'reddit_json', 'reddit_linked_article', 'json_ld', 'open_graph', 'meta_description',
                       'enhanced_heuristics', 'source_content', 'heuristics', 'failed'}
            value = value or ''
            if value.startswith('learned_pattern_'):
                return 'learned_pattern'
            return value if value in methods else 'other' if value else 'none'

        go_result = response.get('result', {}) if response else {}
        python_content = python_result.get('content') or ''
        go_content = go_result.get('content') or ''
        python_success = bool(python_content.strip())
        go_success = bool(response and response.get('outcome') == 'success' and go_content.strip())
        outcome = response.get('outcome') if response else 'unavailable'
        if outcome not in {'success', 'rejected', 'failed', 'needs_selector_study', 'unavailable'}:
            outcome = 'other'
        summary = {'event': 'extractor_shadow_comparison', 'observer_mode': 'inline_diagnostic',
                   'observer_deadline_isolated': False, 'python_success': python_success,
                   'go_success': go_success, 'success_equivalent': python_success == go_success,
                   'go_outcome': outcome, 'service_error': CoreExtractor._service_error_label(error_code),
                   'python_method': method_label(python_result.get('method_used') or python_result.get('method')),
                   'go_method': method_label(go_result.get('method_used') or go_result.get('method')),
                   'python_content_chars': len(python_content),
                   'go_content_chars': len(go_content) if response else None,
                   'content_chars_delta': len(go_content) - len(python_content) if response else None,
                   'content_digest_match': None, 'metadata_equivalent': None, 'media_equivalent': None,
                   'python_duration_ms': round(python_elapsed * 1000, 2),
                   'go_duration_ms': round(go_elapsed * 1000, 2),
                   'total_duration_ms': round((python_elapsed + go_elapsed) * 1000, 2)}
        metadata_fields = ('title', 'publication_date', 'author', 'description', 'image_url')
        summary.update({field + '_equivalent': None for field in metadata_fields})
        if response:
            def digest(value):
                return hashlib.sha256(normalized(value).encode('utf-8')).digest()
            summary['content_digest_match'] = digest(python_content) == digest(go_content)
            for field in metadata_fields:
                summary[field + '_equivalent'] = normalized(python_result.get(field)) == normalized(go_result.get(field))
            summary['metadata_equivalent'] = all(summary[field + '_equivalent'] for field in metadata_fields)
            # Preserve list order and all media attributes; compare without logging values.
            python_media = python_result.get('media_files') or python_result.get('media') or []
            go_media = go_result.get('media') or go_result.get('media_files') or []
            summary['media_equivalent'] = json.dumps(python_media, sort_keys=True) == json.dumps(go_media, sort_keys=True)
        return summary

    @staticmethod
    def _empty_service_result():
        return {'content': None, 'title': None, 'publication_date': None,
                'author': None, 'description': None, 'method_used': 'failed'}

    async def _record_service_success(self, url, response, elapsed, pattern):
        """Persist accepted observations through the existing Python learning API."""
        await self._record_service_failures(url, response, pattern)
        result = response['result']
        domain = self.utils.extract_domain(self.utils.clean_url(url))
        try:
            memory = await get_extraction_memory()
            await memory.record_extraction_attempt(ExtractionAttempt(
                article_url=url, domain=domain, extraction_strategy=result['method_used'],
                selector_used=result.get('selector_used'), success=True,
                content_length=len(result['content']), extraction_time_ms=int(elapsed * 1000)))
            tracker = await get_stability_tracker()
            tracker.update_domain_stats(domain=domain, success=True,
                extraction_time_ms=int(elapsed * 1000), content_length=len(result['content']),
                quality_score=0.8, method=result['method_used'])
        except Exception:
            logger.warning('Failed to record extractor service learning')

    async def _record_service_failures(self, url, response, pattern):
        """Apply only supplied learned identity; fallback delegates learning to Python."""
        if not response or not pattern:
            return
        domain = self.utils.extract_domain(self.utils.clean_url(url))
        try:
            memory = await get_extraction_memory()
            seen = set()
            for observation in response.get('observations') or []:
                identity = (observation['selector'], observation['method'])
                if identity in seen:
                    continue
                seen.add(identity)
                # Degrade only the learned identity Python actually supplied.
                if (not observation['success'] and pattern and identity ==
                        (pattern.get('selector'), pattern.get('method'))):
                    await memory.degrade_pattern(domain, *identity)
        except Exception:
            logger.warning('Failed to record extractor service learned failure')

    async def _extract_article_content_python(self, url: str, retry_count: int = 3) -> Dict[str, Optional[str]]:
        """
        Extract article content along with metadata (title, publication date, author).

        Args:
            url: URL to extract from
            retry_count: Number of retry attempts

        Returns:
            Dict with content, title, publication_date, author, description, method_used
        """
        if not url:
            logger.warning("Empty URL provided for extraction")
            return {'content': None, 'title': None, 'publication_date': None,
                   'author': None, 'description': None, 'method_used': None}

        # Validate URL scheme and format
        if not self._is_valid_url(url):
            logger.warning(f"Invalid URL format: {url[:100]}")
            return {'content': None, 'title': None, 'publication_date': None,
                   'author': None, 'description': None, 'method_used': None}

        # Clean URL first
        clean_url = self.utils.clean_url(url)
        domain = self.utils.extract_domain(clean_url)

        # Start structured logging
        ext_logger = get_extraction_logger()
        metrics = ext_logger.start_extraction(clean_url, domain)

        logger.info(f"Starting extraction", extra={
            'event': 'extraction_start',
            'domain': domain,
            'url': clean_url[:100]
        })

        extraction_start = time.time()
        last_exception = None

        for attempt_num in range(1, retry_count + 1):
            try:
                logger.debug(f"Extraction attempt {attempt_num}/{retry_count}", extra={
                    'event': 'extraction_attempt',
                    'attempt': attempt_num,
                    'domain': domain
                })

                # Use comprehensive extraction with metadata
                result = await self.strategies.attempt_extraction_with_metadata(clean_url, domain, attempt_num)

                if result.get('content'):
                    enforce_text_limit(result['content'])

                if result.get('content') and self.utils.is_good_content(result['content']):
                    extraction_time = time.time() - extraction_start
                    content_length = len(result['content'])
                    method_used = result.get('method_used', 'unknown')
                    selector_used = result.get('selector_used')

                    # Log success
                    ext_logger.complete_extraction(
                        clean_url, True, method_used, content_length, 0.8
                    )
                    logger.info(f"Extraction successful", extra={
                        'event': 'extraction_success',
                        'domain': domain,
                        'method': method_used,
                        'content_length': content_length,
                        'duration_s': round(extraction_time, 2)
                    })
                    
                    # Record success for learning
                    try:
                        extraction_memory = await get_extraction_memory()
                        attempt_record = ExtractionAttempt(
                            article_url=url,
                            domain=domain,
                            extraction_strategy=method_used,
                            selector_used=selector_used,
                            success=True,
                            content_length=content_length,
                            extraction_time_ms=int(extraction_time * 1000)
                        )
                        await extraction_memory.record_extraction_attempt(attempt_record)
                    except Exception as e:
                        logger.warning(f"Failed to record success: {e}")
                    
                    # Update domain stability
                    try:
                        stability_tracker = await get_stability_tracker()
                        stability_tracker.update_domain_stats(
                            domain=domain,
                            success=True,
                            extraction_time_ms=int(extraction_time * 1000),
                            content_length=content_length,
                            quality_score=0.8,
                            method=method_used
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update domain stability: {e}")

                    # Finalize content
                    result['content'] = self.utils.finalize_content(result['content'])

                    return result

                else:
                    logger.debug(f"Attempt {attempt_num} failed - content quality insufficient", extra={
                        'event': 'attempt_quality_fail',
                        'attempt': attempt_num,
                        'domain': domain
                    })
                    if attempt_num < retry_count:
                        await asyncio.sleep(1)

            except ResourceLimitError:
                ext_logger.complete_extraction(clean_url, False)
                raise
            except Exception as e:
                last_exception = e
                ext_logger.log_error(clean_url, str(e), f"attempt_{attempt_num}")
                logger.warning(f"Attempt {attempt_num} failed with error", extra={
                    'event': 'attempt_error',
                    'attempt': attempt_num,
                    'domain': domain,
                    'error': str(e)
                })

                # Record failure for learning
                try:
                    extraction_memory = await get_extraction_memory()
                    attempt_obj = ExtractionAttempt(
                        article_url=url,
                        domain=domain,
                        extraction_strategy=f"attempt_{attempt_num}",
                        success=False,
                        error_message=str(e)
                    )
                    await extraction_memory.record_extraction_attempt(attempt_obj)

                    # Update domain stability for failure
                    stability_tracker = await get_stability_tracker()
                    stability_tracker.update_domain_stats(
                        domain=domain,
                        success=False,
                        method=f"attempt_{attempt_num}"
                    )
                except Exception as record_error:
                    logger.warning(f"Failed to record failure: {record_error}")

                if attempt_num < retry_count:
                    await asyncio.sleep(1)
        
        # All attempts failed
        extraction_time = time.time() - extraction_start
        ext_logger.complete_extraction(clean_url, False)
        logger.warning(f"All extraction attempts failed", extra={
            'event': 'extraction_failed',
            'domain': domain,
            'duration_s': round(extraction_time, 2),
            'attempts': retry_count
        })

        # Try alternative URLs if available
        try:
            html = await self.strategies.fetch_html_content(clean_url)
            if html:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                alt_urls = self.strategies.metadata_extractor.find_alt_article_links(soup, clean_url)

                for alt_url in alt_urls[:2]:
                    logger.debug(f"Trying alternative URL: {alt_url[:60]}")
                    try:
                        alt_result = await self.strategies.attempt_extraction_with_metadata(alt_url, domain, 1)
                        if alt_result.get('content') and self.utils.is_good_content(alt_result['content']):
                            logger.info(f"Alternative URL successful", extra={
                                'event': 'alt_url_success',
                                'domain': domain,
                                'alt_url': alt_url[:80]
                            })
                            alt_result['content'] = self.utils.finalize_content(alt_result['content'])
                            return alt_result
                    except Exception as e:
                        logger.debug(f"Alternative URL failed: {e}")
                        continue
        except Exception as e:
            logger.warning(f"Failed to try alternative URLs: {e}")

        return {'content': None, 'title': None, 'publication_date': None,
               'author': None, 'description': None, 'method_used': 'failed'}
    
    async def extract_article_content(self, url: str, retry_count: int = 2) -> Optional[str]:
        """Extract article content (delegates to extract_article_content_with_metadata)."""
        previous = getattr(self, '_service_previous', None)
        if previous and previous[0] == url:
            # Legacy callers request content again after metadata fails. Reuse
            # that operation rather than reset its service/fallback deadline.
            if previous[2] is not None:
                raise previous[2]
            return previous[1].get('content') or None
        result = await self.extract_article_content_with_metadata(url, retry_count)
        content = result.get('content')
        return content if content else None
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get extraction performance statistics."""
        return {
            'browser_active': self.browser is not None
        }

