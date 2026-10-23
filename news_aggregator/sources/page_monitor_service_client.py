"""Bounded client for the private Go Page Monitor extraction API."""

import hashlib
import json
import math
from datetime import datetime, timezone
from uuid import uuid4
from urllib.parse import urlsplit

import aiohttp
from ..extraction.admission import run_admitted


MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_HTML_BYTES = 5 * 1024 * 1024
MAX_ITEMS = 100
MAX_DESCRIPTION_BYTES = 2_048
ERROR_CODES = {
    'invalid_request', 'request_too_large', 'invalid_runner_response', 'busy', 'unavailable',
    'unsupported_selector', 'public_url_denied', 'admission_unavailable', 'deadline_exceeded',
    'cancelled', 'event_overflow', 'resource_limit', 'parse_failed', 'transport_failed',
    'native_browser_policy_denied', 'native_browser_busy', 'native_browser_unavailable',
    'native_browser_timeout', 'native_browser_http_rejected', 'response_too_large',
}


class PageMonitorServiceError(Exception):
    """Finite error classification safe to log without remote response data."""

    def __init__(self, code, retryable=False, retry_after_seconds=None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class PageMonitorServiceClient:
    def __init__(self, endpoint, token, *, max_bytes=MAX_RESPONSE_BYTES,
                 session_factory=aiohttp.ClientSession):
        parsed = urlsplit(endpoint or '')
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment):
            raise ValueError('invalid_extractor_endpoint')
        if not token:
            raise ValueError('missing_extractor_token')
        self.endpoint = endpoint.rstrip('/') + '/v1/page-monitor/extract'
        self._token = token
        self.max_bytes = min(max_bytes, MAX_RESPONSE_BYTES)
        self._session_factory = session_factory

    async def fetch(self, config, *, limit, timeout_seconds, offset=0, admission_timeout_seconds=None):
        return await self._call(config, operation='fetch', limit=limit,
                                timeout_seconds=timeout_seconds, offset=offset,
                                admission_timeout_seconds=admission_timeout_seconds)

    async def parse(self, config, html, snapshot_at, *, limit, timeout_seconds, offset=0,
                    admission_timeout_seconds=None):
        if not isinstance(html, str) or not isinstance(snapshot_at, datetime):
            raise PageMonitorServiceError('invalid_request')
        try:
            encoded = html.encode('utf-8')
        except UnicodeEncodeError:
            raise PageMonitorServiceError('invalid_request') from None
        if len(encoded) > MAX_HTML_BYTES:
            raise PageMonitorServiceError('request_too_large')
        if snapshot_at.tzinfo is None:
            snapshot_at = snapshot_at.replace(tzinfo=timezone.utc)
        return await self._call(config, operation='parse', limit=limit,
                                timeout_seconds=timeout_seconds, html=html,
                                snapshot_at=snapshot_at.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z'),
                                offset=offset, admission_timeout_seconds=admission_timeout_seconds)

    async def _call(self, config, *, operation, limit, timeout_seconds, html=None, snapshot_at=None,
                    offset=0, admission_timeout_seconds=None):
        if type(limit) is not int or not 1 <= limit <= MAX_ITEMS:
            raise ValueError('invalid_page_monitor_limit')
        if type(offset) is not int or not 0 <= offset < limit:
            raise ValueError('invalid_page_monitor_offset')
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise PageMonitorServiceError('deadline_exceeded', True)
        if (not self._bounded_text(config.url, 8192) or not self._valid_url(config.url)
                or not self._bounded_text(config.name, 256)
                or type(config.min_title_length) is not int or not 1 <= config.min_title_length <= 100_000):
            raise PageMonitorServiceError('invalid_request')
        selectors = {
            'article_selectors': config.article_selectors,
            'title_selectors': config.title_selectors,
            'date_selectors': config.date_selectors,
            'link_selectors': config.link_selectors,
            'learned_container_selectors': config.learned_container_selectors,
            'learned_title_selectors': config.learned_title_selectors,
            'learned_link_selectors': config.learned_link_selectors,
            'learned_date_selectors': config.learned_date_selectors,
            'learned_general_selectors': [
                {'selector': selector, 'confidence': confidence}
                for selector, confidence in config.learned_selectors
            ],
        }
        groups = [selectors[name] for name in (
            'article_selectors', 'title_selectors', 'date_selectors', 'link_selectors',
            'learned_container_selectors', 'learned_title_selectors', 'learned_link_selectors',
            'learned_date_selectors')]
        general = selectors['learned_general_selectors']
        if len(general) > 64 or any(not isinstance(item, dict)
                or not self._bounded_text(item.get('selector'), 2048)
                or isinstance(item.get('confidence'), bool)
                or not isinstance(item.get('confidence'), (int, float))
                or not math.isfinite(item['confidence']) for item in general):
            raise PageMonitorServiceError('invalid_request')
        if any(not isinstance(group, list) or len(group) > 64
               or any(not self._bounded_text(value, 2048) for value in group) for group in groups):
            raise PageMonitorServiceError('invalid_request')
        if sum(map(len, groups)) + len(general) > 256:
            raise PageMonitorServiceError('invalid_request')
        async def request(attempt_timeout):
            payload = {
                'contract_version': '1', 'request_id': str(uuid4()), 'operation': operation,
                'url': config.url, 'source_name': config.name,
                'budget_ms': max(1, min(30_000, int(attempt_timeout * 1000))),
                'limit': limit, 'offset': offset, 'min_title_length': config.min_title_length,
                **selectors,
            }
            if operation == 'fetch':
                payload['use_browser'] = config.use_browser
                payload['wait_for_js'] = config.wait_for_js
            else:
                payload['provided_html'] = html
                payload['snapshot_at'] = snapshot_at
            try:
                body = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
            except (UnicodeEncodeError, ValueError, TypeError):
                raise PageMonitorServiceError('invalid_request') from None
            if len(body) > MAX_REQUEST_BYTES:
                raise PageMonitorServiceError('request_too_large')
            async with self._session_factory(timeout=aiohttp.ClientTimeout(total=attempt_timeout),
                                             trust_env=False, auto_decompress=False) as session:
                async with session.post(self.endpoint, data=body, allow_redirects=False,
                                        headers={'Authorization': 'Bearer ' + self._token,
                                                 'Content-Type': 'application/json',
                                                 'Accept-Encoding': 'identity'}) as response:
                    if response.status != 200:
                        codes = {400: 'invalid_request', 401: 'unauthorized', 403: 'unauthorized',
                                 413: 'request_too_large', 429: 'busy', 503: 'unavailable'}
                        retry_after = response.headers.get('Retry-After', '')
                        retry_after = min(int(retry_after), 86400) if retry_after.isdigit() else None
                        raise PageMonitorServiceError(codes.get(response.status, 'http_error'),
                                                      response.status in {429, 502, 503, 504}, retry_after)
                    if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                        raise PageMonitorServiceError('invalid_payload')
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(16_384):
                        if len(data) + len(chunk) > self.max_bytes:
                            raise PageMonitorServiceError('response_too_large')
                        data.extend(chunk)
                    try:
                        result = json.loads(data)
                    except (ValueError, UnicodeError, RecursionError):
                        raise PageMonitorServiceError('invalid_payload') from None
                    return self._validate(result, payload, config, limit)

        try:
            return await run_admitted(self.endpoint, min(float(timeout_seconds), 30.0), request,
                                       PageMonitorServiceError,
                                       admission_timeout_seconds=admission_timeout_seconds)
        except aiohttp.ClientError:
            raise PageMonitorServiceError('transport_error', True) from None

    @classmethod
    def _validate(cls, data, request, config, limit):
        required = {'contract_version', 'build_version', 'request_id', 'outcome',
                    'original_url', 'final_url', 'selectors_used', 'items', 'needs_selector_study'}
        allowed = required | {'fetch_method', 'snapshot_at', 'content_hash', 'study_html',
                              'study_html_omitted', 'error', 'next_offset'}
        if (not isinstance(data, dict) or not required.issubset(data) or set(data) - allowed
                or data.get('contract_version') != '1' or data.get('request_id') != request['request_id']
                or data.get('original_url') != config.url
                or not isinstance(data.get('build_version'), str) or not data['build_version']
                or data.get('outcome') not in {'success', 'partial', 'failed', 'rejected'}
                or type(data.get('needs_selector_study')) is not bool):
            raise PageMonitorServiceError('invalid_payload')
        method = data.get('fetch_method')
        expected_methods = {'http', 'native_browser'} if request['operation'] == 'fetch' else {'provided_html'}
        valid_method = method in expected_methods
        if ((data['outcome'] in {'success', 'partial'} and not valid_method)
                or (data['outcome'] in {'failed', 'rejected'} and method not in ({None, ''} | expected_methods))):
            raise PageMonitorServiceError('invalid_payload')
        if not cls._bounded_text(data.get('final_url'), 8192) or not cls._valid_url(data.get('final_url')):
            raise PageMonitorServiceError('invalid_payload')
        if data.get('snapshot_at') is not None:
            cls._parse_utc(data['snapshot_at'])
        elif data['outcome'] in {'success', 'partial'}:
            raise PageMonitorServiceError('invalid_payload')
        if data.get('content_hash') is not None and (not isinstance(data['content_hash'], str)
                or len(data['content_hash']) != 32 or any(c not in '0123456789abcdef' for c in data['content_hash'])):
            raise PageMonitorServiceError('invalid_payload')
        if data['outcome'] in {'success', 'partial'} and data.get('content_hash') is None:
            raise PageMonitorServiceError('invalid_payload')
        selectors = data['selectors_used']
        if not isinstance(selectors, list) or len(selectors) > 256 or any(not cls._bounded_text(v, 2048) for v in selectors):
            raise PageMonitorServiceError('invalid_payload')
        items = data['items']
        if (not isinstance(items, list)
                or len(items) > min(limit - request['offset'], MAX_ITEMS)):
            raise PageMonitorServiceError('invalid_payload')
        for item in items:
            cls._validate_item(item, config)
        study_html = data.get('study_html', '')
        if not cls._bounded_text(study_html, MAX_HTML_BYTES):
            raise PageMonitorServiceError('invalid_payload')
        if 'study_html_omitted' in data and type(data['study_html_omitted']) is not bool:
            raise PageMonitorServiceError('invalid_payload')
        error = data.get('error')
        if error is not None:
            optional = {'upstream_status', 'retry_after_seconds', 'reason'}
            if (not isinstance(error, dict) or not {'code', 'retryable'}.issubset(error)
                    or set(error) - ({'code', 'retryable'} | optional)
                    or error.get('code') not in ERROR_CODES or type(error.get('retryable')) is not bool
                    or ('upstream_status' in error and (type(error['upstream_status']) is not int or not 0 <= error['upstream_status'] <= 599))
                    or ('retry_after_seconds' in error and (type(error['retry_after_seconds']) is not int or error['retry_after_seconds'] < 0))
                    or ('reason' in error and not cls._bounded_text(error['reason'], 256))):
                raise PageMonitorServiceError('invalid_payload')
        outcome = data['outcome']
        if ((outcome in {'failed', 'rejected'} and (items or error is None))
                or (outcome == 'partial' and error is None)
                or (outcome == 'success' and error is not None)):
            raise PageMonitorServiceError('invalid_payload')
        if outcome == 'partial' and error['code'] not in {
                'resource_limit', 'deadline_exceeded', 'cancelled', 'parse_failed'}:
            raise PageMonitorServiceError('invalid_payload')
        if 'next_offset' in data:
            next_offset = data['next_offset']
            offset = request['offset']
            if (outcome != 'partial' or type(next_offset) is not int
                    or not offset < next_offset <= limit):
                raise PageMonitorServiceError('invalid_payload')
            if items:
                if next_offset != offset + len(items):
                    raise PageMonitorServiceError('invalid_payload')
            elif error['code'] != 'resource_limit' or next_offset != offset + 1:
                raise PageMonitorServiceError('invalid_payload')
        if outcome in {'failed', 'rejected'}:
            raise PageMonitorServiceError(error['code'], error['retryable'])
        return data

    @classmethod
    def _validate_item(cls, item, config):
        fields = {'title', 'link', 'description', 'published', 'content_type', 'id', 'source', 'source_url'}
        if not isinstance(item, dict) or set(item) != fields:
            raise PageMonitorServiceError('invalid_payload')
        bounds = {'title': MAX_HTML_BYTES, 'link': 8192, 'description': MAX_DESCRIPTION_BYTES,
                  'content_type': 64, 'id': 64, 'source': 256, 'source_url': 8192}
        for key, maximum in bounds.items():
            if not cls._bounded_text(item[key], maximum):
                raise PageMonitorServiceError('invalid_payload')
        if item['source'] != config.name or item['source_url'] != config.url:
            raise PageMonitorServiceError('invalid_payload')
        if not cls._valid_url(item['link']):
            raise PageMonitorServiceError('invalid_payload')
        cls._parse_utc(item['published'])
        expected_id = hashlib.md5((item['title'] + item['link']).encode('utf-8')).hexdigest()
        if item['id'] != expected_id or item['content_type'] not in {'changelog', 'news', 'blog', 'general'}:
            raise PageMonitorServiceError('invalid_payload')

    @staticmethod
    def _bounded_text(value, maximum):
        if not isinstance(value, str) or len(value) > maximum:
            return False
        try:
            return len(value.encode('utf-8')) <= maximum
        except UnicodeEncodeError:
            return False

    @staticmethod
    def _valid_url(value):
        if not isinstance(value, str) or len(value) > 8192:
            return False
        try:
            parsed = urlsplit(value)
            return parsed.scheme in {'http', 'https'} and bool(parsed.hostname) and not parsed.username and not parsed.password
        except ValueError:
            return False

    @staticmethod
    def _parse_utc(value):
        if (not isinstance(value, str) or 'T' not in value
                or not (value.endswith('Z') or value.endswith('+00:00'))):
            raise PageMonitorServiceError('invalid_payload')
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            raise PageMonitorServiceError('invalid_payload') from None
        if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise PageMonitorServiceError('invalid_payload')
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
