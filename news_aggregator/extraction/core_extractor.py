"""Python compatibility adapter for Go-owned article extraction."""

import asyncio
import logging
import time
from urllib.parse import urlparse, urlsplit, urlunsplit
from typing import Dict, Any

from ..core.resource_limits import enforce_text_limit
from ..services.extraction_memory import get_extraction_memory, ExtractionAttempt
from ..services.domain_stability_tracker import get_stability_tracker
from .extraction_utils import ExtractionUtils

logger = logging.getLogger('extraction.core')
_BROWSER_LEARNED_METHODS = frozenset({
    'browser_rendering', 'browser_heuristics',
    'learned_pattern_browser_rendering', 'learned_pattern_browser_heuristics',
})


class CoreExtractor:
    """Thin Go-service adapter retained for existing Python callers."""

    def __init__(self, utils=None, html_processor=None, strategies=None):
        self.utils = utils or ExtractionUtils()
        self.browser = None
        self._service_previous = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def close_browser(self):
        """Compatibility method: browser lifecycle belongs to the Go service."""
        return None

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            return False
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc or parsed.scheme not in {'http', 'https'}:
                return False
            if '.' not in parsed.netloc:
                return False
            skip_extensions = (
                '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico',
                '.mp4', '.avi', '.mov', '.wmv', '.flv',
                '.mp3', '.wav', '.ogg', '.flac',
                '.zip', '.rar', '.tar', '.gz',
                '.pdf', '.doc', '.docx', '.xls', '.xlsx',
            )
            return not parsed.path.lower().endswith(skip_extensions)
        except (TypeError, ValueError):
            return False

    async def extract_article_content_with_metadata(self, url: str, retry_count: int = 3):
        from ..config import get_settings
        from .service_client import ExtractionServiceClient, ExtractionServiceError

        self._service_previous = None
        if not self._is_valid_url(url):
            return {'content': None, 'title': None, 'publication_date': None,
                    'author': None, 'description': None, 'method_used': 'failed'}
        settings = get_settings()
        token = settings.extraction_service_token
        if not settings.extraction_service_url or not token:
            raise ExtractionServiceError('not_configured')

        started = time.monotonic()
        total_budget = settings.extraction_service_total_budget_seconds
        deadline = asyncio.get_running_loop().time() + total_budget
        admission_phase = {'value': None}
        clean_url = self.utils.clean_url(url)
        try:
            parsed = urlsplit(clean_url)
            port = parsed.port
            if parsed.scheme not in {'http', 'https'} or not parsed.hostname:
                raise ExtractionServiceError('invalid_input')
            clean_url = urlunsplit(parsed._replace(fragment=''))
        except (TypeError, ValueError):
            raise ExtractionServiceError('invalid_input') from None

        supplied_pattern = None
        try:
            async with asyncio.timeout_at(deadline):
                memory = await get_extraction_memory()
                supplied_pattern = await memory.get_successful_pattern(
                    self.utils.extract_domain(clean_url))
        except TimeoutError:
            error = ExtractionServiceError('deadline_exceeded', True)
            self._service_previous = (url, None, error)
            raise error from None
        except Exception:
            logger.warning('Extractor learned-pattern lookup failed')

        try:
            client = ExtractionServiceClient(
                settings.extraction_service_url, token.get_secret_value(),
                max_bytes=settings.extraction_http_max_bytes,
                max_chars=settings.extraction_text_max_chars)
        except ValueError:
            raise ExtractionServiceError('invalid_configuration') from None
        browser_eligible = (settings.extraction_service_native_browser
                            and parsed.scheme == 'https' and port in (None, 443))
        use_browser = False
        try:
            async with asyncio.timeout_at(deadline):
                while True:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise ExtractionServiceError('deadline_exceeded', True)
                    failure = None
                    client.max_bytes = (min(settings.extraction_http_max_bytes,
                                            settings.extraction_browser_max_bytes)
                                        if use_browser else settings.extraction_http_max_bytes)
                    pattern = supplied_pattern
                    if pattern and pattern.get('method') in _BROWSER_LEARNED_METHODS and not use_browser:
                        pattern = None
                    try:
                        response = await client.extract(
                            clean_url, timeout_seconds=min(settings.extraction_service_timeout_seconds, remaining),
                            learned_pattern=self._request_learned_pattern(pattern, use_browser),
                            use_browser=use_browser, admission_timeout_seconds=remaining,
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
                                if (final_url and original_url and
                                        urlunsplit(urlsplit(final_url)._replace(fragment='')) !=
                                        urlunsplit(urlsplit(original_url)._replace(fragment=''))):
                                    result['full_article_url'] = final_url
                            except (TypeError, ValueError):
                                pass
                            await self._record_service_success(url, response, time.monotonic() - started,
                                                               supplied_pattern)
                            self._service_previous = (url, result, None)
                            return result
                    elif failure is None:
                        error = response.get('error') or {}
                        failure = ExtractionServiceError(
                            error.get('code', 'no_content' if response['outcome'] == 'rejected'
                                      else response['outcome']),
                            error.get('retryable', False), error.get('retry_after_seconds'))
                    if (not use_browser and browser_eligible and
                            self._native_browser_retry_allowed(failure, response)):
                        reason = self._service_error_label(failure.code)
                        logger.info('Extractor native browser retry reason=%s', reason,
                                    extra={'event': 'extractor_native_browser_retry', 'code': reason})
                        use_browser = True
                        continue
                    await self._record_service_failures(url, response, supplied_pattern)
                    self._service_previous = (url, None, failure)
                    raise failure
        except TimeoutError:
            code = 'busy' if admission_phase['value'] == 'waiting' else 'deadline_exceeded'
            error = ExtractionServiceError(code, True)
            self._service_previous = (url, None, error)
            raise error from None

    @staticmethod
    def _request_learned_pattern(pattern, use_browser):
        if pattern and not use_browser and pattern.get('method') in _BROWSER_LEARNED_METHODS:
            return None
        return pattern

    @staticmethod
    def _service_error_label(code):
        errors = {
            'deadline_exceeded', 'transport_error', 'transport_failed', 'busy', 'unavailable',
            'unauthorized', 'invalid_request', 'invalid_input', 'unsupported_input',
            'resource_limit', 'invalid_content', 'request_too_large', 'response_too_large',
            'invalid_payload', 'upstream_rate_limited', 'upstream_unavailable',
            'browser_unavailable', 'cancelled', 'http_error', 'not_configured',
            'invalid_configuration', 'browser_required', 'no_content', 'needs_selector_study',
            'upstream_forbidden', 'challenge', 'public_url_denied', 'upstream_auth_required',
            'admission_unavailable', 'native_browser_policy_denied', 'native_browser_unsupported',
            'native_browser_challenge', 'native_browser_no_content', 'native_browser_http_rejected',
            'native_browser_unavailable', 'native_browser_timeout', 'native_browser_busy',
            'native_browser_target_crashed',
        }
        return code if code in errors else 'other' if code else 'none'

    @staticmethod
    def _native_browser_retry_allowed(failure, response):
        return (response is not None and not failure.terminal and failure.code in {
            'browser_required', 'no_content', 'needs_selector_study', 'upstream_forbidden',
            'challenge', 'invalid_content'})

    async def _record_service_success(self, url, response, elapsed, pattern):
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
                if (not observation['success'] and identity ==
                        (pattern.get('selector'), pattern.get('method'))):
                    await memory.degrade_pattern(domain, *identity)
        except Exception:
            logger.warning('Failed to record extractor service learned failure')

    async def extract_article_content(self, url: str, retry_count: int = 2):
        previous = self._service_previous
        if previous and previous[0] == url:
            if previous[2] is not None:
                raise previous[2]
            return previous[1].get('content') if previous[1] else None
        result = await self.extract_article_content_with_metadata(url, retry_count)
        return result.get('content') or None

    def get_extraction_stats(self) -> Dict[str, Any]:
        return {'browser_active': False}
