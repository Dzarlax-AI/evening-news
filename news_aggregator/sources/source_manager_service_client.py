"""Bounded client for the private Go source collection manager."""

import json
import math
from datetime import datetime, timezone
from urllib.parse import urlsplit
from uuid import uuid4

import aiohttp
from ..extraction.admission import run_admitted


CONTRACT_VERSION = '1'
MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_SOURCES = 8
MAX_BUDGET_MS = 120_000
SOURCE_TYPES = {'rss', 'telegram', 'custom'}
OUTCOMES = {'success', 'partial', 'deferred', 'failed', 'rejected'}
SOURCE_ERROR_CODES = {
    'batch_deadline_exceeded', 'busy', 'cancelled', 'canceled', 'deadline_exceeded',
    'duplicate_source_id', 'invalid_feed', 'invalid_input', 'invalid_request', 'invalid_runner_response',
    'invalid_source', 'page_monitor_unavailable', 'public_url_denied', 'request_too_large',
    'response_budget_exhausted',
    'resource_limit', 'response_too_large', 'rss_unavailable', 'source_disabled', 'source_failed',
    'source_panic', 'telegram_unavailable', 'timeout', 'transport_error', 'unauthorized',
    'unavailable', 'unsupported_source_type', 'upstream_auth_required', 'upstream_http_error',
    'upstream_rate_limited', 'upstream_timeout', 'write_deadline_unavailable',
}


class SourceManagerServiceError(Exception):
    """Finite bridge error that never includes remote response contents."""

    def __init__(self, code, retryable=False, retry_after_seconds=None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class SourceManagerServiceClient:
    def __init__(self, endpoint, token, *, session_factory=aiohttp.ClientSession):
        parsed = urlsplit(endpoint or '')
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment):
            raise ValueError('invalid_source_manager_endpoint')
        if not token:
            raise ValueError('missing_source_manager_token')
        self.endpoint = endpoint.rstrip('/') + '/v1/sources/collect'
        self._token = token
        self._session_factory = session_factory

    async def collect(self, sources, *, timeout_seconds=120.0, request_id=None,
                      admission_timeout_seconds=None):
        """Collect one bounded batch and validate that every input ID is returned once."""
        if not isinstance(sources, list) or not 1 <= len(sources) <= MAX_SOURCES:
            raise SourceManagerServiceError('invalid_request')
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 120):
            raise SourceManagerServiceError('invalid_request')
        request_id = request_id or str(uuid4())
        if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
            raise SourceManagerServiceError('invalid_request')
        source_ids = []
        for source in sources:
            if not isinstance(source, dict):
                raise SourceManagerServiceError('invalid_request')
            source_id = source.get('source_id')
            if type(source_id) is not int or source_id <= 0:
                raise SourceManagerServiceError('invalid_request')
            if source.get('type') not in SOURCE_TYPES:
                raise SourceManagerServiceError('invalid_request')
            source_ids.append(source_id)
        if len(set(source_ids)) != len(source_ids):
            raise SourceManagerServiceError('invalid_request')

        timeout_seconds = min(float(timeout_seconds), 120.0)

        async def request(attempt_timeout):
            transport_margin = min(5.0, attempt_timeout * 0.10)
            payload = {
                'contract_version': CONTRACT_VERSION,
                'request_id': request_id,
                'batch_budget_ms': min(MAX_BUDGET_MS, max(1, round((attempt_timeout - transport_margin) * 1000))),
                'sources': sources,
            }
            try:
                body = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
            except (UnicodeEncodeError, ValueError, TypeError):
                raise SourceManagerServiceError('invalid_request') from None
            if len(body) > MAX_REQUEST_BYTES:
                raise SourceManagerServiceError('request_too_large')
            async with self._session_factory(
                    timeout=aiohttp.ClientTimeout(total=attempt_timeout), trust_env=False,
                    auto_decompress=False) as session:
                async with session.post(
                        self.endpoint, data=body, allow_redirects=False,
                        headers={'Authorization': 'Bearer ' + self._token,
                                 'Content-Type': 'application/json',
                                 'Accept-Encoding': 'identity'}) as response:
                    if response.status != 200:
                        code = {400: 'invalid_request', 401: 'unauthorized', 403: 'unauthorized',
                                413: 'request_too_large', 429: 'busy', 503: 'unavailable'}.get(
                                    response.status, 'http_error')
                        retry_after = response.headers.get('Retry-After', '')
                        retry_after = min(int(retry_after), 86400) if retry_after.isdigit() else None
                        raise SourceManagerServiceError(
                            code, response.status in {429, 502, 503, 504}, retry_after)
                    if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                        raise SourceManagerServiceError('invalid_payload')
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(16_384):
                        if len(data) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise SourceManagerServiceError('response_too_large')
                        data.extend(chunk)
                    try:
                        decoded = json.loads(data)
                    except (ValueError, UnicodeError, RecursionError):
                        raise SourceManagerServiceError('invalid_payload') from None
                    return self._validate(decoded, request_id, sources)

        try:
            return await run_admitted(self.endpoint, timeout_seconds, request, SourceManagerServiceError,
                                      admission_timeout_seconds=admission_timeout_seconds)
        except aiohttp.ClientError:
            raise SourceManagerServiceError('transport_error', True) from None

    @staticmethod
    def _validate(data, request_id, sources):
        required = {'contract_version', 'build_version', 'request_id', 'results'}
        if (not isinstance(data, dict) or not required.issubset(data)
                or set(data) != required or data.get('contract_version') != CONTRACT_VERSION
                or data.get('request_id') != request_id
                or not isinstance(data.get('build_version'), str) or not data['build_version']):
            raise SourceManagerServiceError('invalid_payload')
        results = data.get('results')
        if not isinstance(results, list) or len(results) != len(sources):
            raise SourceManagerServiceError('invalid_payload')
        expected = {source['source_id']: source for source in sources}
        seen = set()
        for result in results:
            if not isinstance(result, dict):
                raise SourceManagerServiceError('invalid_payload')
            source_id = result.get('source_id')
            if (type(source_id) is not int or source_id not in expected or source_id in seen
                    or result.get('type') != expected[source_id]['type']
                    or result.get('outcome') not in OUTCOMES):
                raise SourceManagerServiceError('invalid_payload')
            seen.add(source_id)
            if 'error' in result and result['error'] is not None:
                error = result['error']
                if (not isinstance(error, dict) or not {'code', 'retryable'}.issubset(error)
                        or not SourceManagerServiceClient._finite_source_error_code(error['code'])
                        or type(error['retryable']) is not bool):
                    raise SourceManagerServiceError('invalid_payload')
                if ('retry_after_seconds' in error and
                        (type(error['retry_after_seconds']) is not int or error['retry_after_seconds'] < 0)):
                    raise SourceManagerServiceError('invalid_payload')
            if 'final_url' in result and (not isinstance(result['final_url'], str)
                                          or len(result['final_url']) > 8192):
                raise SourceManagerServiceError('invalid_payload')
            if 'fetched_at' in result and result['fetched_at'] is not None:
                try:
                    fetched_at = datetime.fromisoformat(result['fetched_at'].replace('Z', '+00:00'))
                except (AttributeError, ValueError):
                    raise SourceManagerServiceError('invalid_payload') from None
                if fetched_at.tzinfo is None:
                    raise SourceManagerServiceError('invalid_payload')
            outcome = result['outcome']
            expected_payload = {'rss': 'rss_articles', 'telegram': 'telegram', 'custom': 'page_monitor'}[
                result['type']]
            if outcome in {'success', 'partial'} and expected_payload not in result:
                raise SourceManagerServiceError('invalid_payload')
            if result['type'] == 'rss' and expected_payload in result:
                articles = result[expected_payload]
                if articles is None:
                    if outcome in {'success', 'partial'}:
                        raise SourceManagerServiceError('invalid_payload')
                else:
                    if not isinstance(articles, list) or len(articles) > 1000:
                        raise SourceManagerServiceError('invalid_payload')
                    for article in articles:
                        SourceManagerServiceClient._validate_rss_article(
                            article, expected[source_id]['source_name'])
            elif result['type'] == 'telegram' and expected_payload in result:
                SourceManagerServiceClient._validate_telegram_result(
                    result[expected_payload], expected[source_id])
            elif result['type'] == 'custom' and expected_payload in result:
                SourceManagerServiceClient._validate_page_monitor_result(
                    result[expected_payload], expected[source_id])
            if outcome in {'failed', 'rejected'} and result.get('error') is None:
                raise SourceManagerServiceError('invalid_payload')
        if seen != set(expected):
            raise SourceManagerServiceError('invalid_payload')
        return data

    @staticmethod
    def _finite_source_error_code(code):
        if code in SOURCE_ERROR_CODES:
            return True
        from ..telegram.service_client import TELEGRAM_ERROR_CODES
        from .page_monitor_service_client import ERROR_CODES as PAGE_MONITOR_ERROR_CODES
        return code in TELEGRAM_ERROR_CODES or code in PAGE_MONITOR_ERROR_CODES

    @staticmethod
    def _validate_rss_article(article, source_name):
        fields = {'title', 'url', 'content', 'summary', 'image_url', 'published_at',
                  'source_type', 'source_name', 'raw_data'}
        if (not isinstance(article, dict) or set(article) != fields
                or not isinstance(article.get('title'), str)
                or not SourceManagerServiceClient._safe_rss_url(article.get('url'))
                or article.get('source_type') != 'rss' or article.get('source_name') != source_name):
            raise SourceManagerServiceError('invalid_payload')
        for field in ('content', 'summary', 'image_url'):
            if article[field] is not None and not isinstance(article[field], str):
                raise SourceManagerServiceError('invalid_payload')
        if article['image_url'] is not None and not SourceManagerServiceClient._safe_rss_url(article['image_url']):
            raise SourceManagerServiceError('invalid_payload')
        published_at = article.get('published_at')
        if not isinstance(published_at, str):
            raise SourceManagerServiceError('invalid_payload')
        try:
            parsed = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        except ValueError:
            raise SourceManagerServiceError('invalid_payload') from None
        if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise SourceManagerServiceError('invalid_payload')
        raw = article.get('raw_data')
        if (not isinstance(raw, dict) or set(raw) != {'guid', 'author', 'tags'}
                or not isinstance(raw['guid'], str) or raw['author'] is not None and not isinstance(raw['author'], str)
                or not isinstance(raw['tags'], list)
                or any(not isinstance(tag, str) for tag in raw['tags'])):
            raise SourceManagerServiceError('invalid_payload')

    @staticmethod
    def _safe_rss_url(value):
        if (not isinstance(value, str) or not value or len(value) > 8192
                or any(char in value for char in ('"', "'", '<', '>', '`'))
                or any(ord(char) < 0x20 or 0x7F <= ord(char) <= 0x9F or char.isspace()
                       for char in value)):
            return False
        try:
            parsed = urlsplit(value)
            # Reading .port validates malformed port syntax.
            parsed.port
        except ValueError:
            return False
        return (parsed.scheme in {'http', 'https'} and parsed.hostname is not None
                and parsed.username is None and parsed.password is None and not parsed.netloc.endswith('@'))

    @staticmethod
    def _validate_telegram_result(response, source):
        from ..telegram.service_client import TelegramServiceClient, TelegramServiceError

        limit = source.get('telegram', {}).get('limit') or source.get('limit') or 100
        try:
            TelegramServiceClient._validate(
                response, f"source-{source['source_id']}", source['url'],
                source['source_name'], limit)
        except TelegramServiceError:
            raise SourceManagerServiceError('invalid_payload') from None

    @staticmethod
    def _validate_page_monitor_result(response, source):
        from types import SimpleNamespace
        from ..sources.page_monitor_service_client import (
            PageMonitorServiceClient, PageMonitorServiceError,
        )

        config_data = source.get('page_monitor') or {}
        config = SimpleNamespace(
            url=source['url'], name=source['source_name'],
            min_title_length=config_data.get('min_title_length', 10),
        )
        request = {
            'operation': 'fetch', 'request_id': f"source-{source['source_id']}",
            'offset': config_data.get('offset', 0),
        }
        limit = config_data.get('limit') or source.get('limit') or 20
        try:
            PageMonitorServiceClient._validate(response, request, config, limit)
        except PageMonitorServiceError as exc:
            # The existing validator raises a classified source failure only
            # after validating the complete failed/rejected payload.
            if (response.get('outcome') in {'failed', 'rejected'}
                    and isinstance(response.get('error'), dict)
                    and response['error'].get('code') == exc.code):
                return
            raise SourceManagerServiceError('invalid_payload') from None
