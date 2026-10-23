"""Bounded client for the private Go Telegram channel fetch API."""

import json
import math
from uuid import uuid4
from urllib.parse import urlsplit

import aiohttp
from ..extraction.admission import run_admitted


MAX_REQUEST_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_ARTICLES = 100
MAX_CONTENT_CHARS = 100_000
TELEGRAM_ERROR_CODES = {
    'invalid_request', 'request_too_large', 'invalid_runner_response', 'telegram_unavailable',
    'busy', 'unavailable', 'invalid_page', 'no_posts', 'parse_failed', 'resource_limit', 'response_too_large',
    'event_overflow',
    'public_url_denied', 'admission_unavailable', 'upstream_challenge',
    'upstream_rate_limited', 'upstream_auth_required', 'upstream_forbidden',
    'upstream_http_rejected', 'upstream_unavailable', 'transport_failed',
    'deadline_exceeded', 'cancelled', 'native_browser_policy_denied',
    'native_browser_busy', 'native_browser_unavailable', 'native_browser_timeout',
    'native_browser_http_rejected',
}


class TelegramServiceError(Exception):
    """Finite error classification safe to log without remote response data."""

    def __init__(self, code, retryable=False, retry_after_seconds=None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class TelegramServiceClient:
    def __init__(self, endpoint, token, *, max_bytes=MAX_RESPONSE_BYTES,
                 session_factory=aiohttp.ClientSession):
        parsed = urlsplit(endpoint or '')
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError('invalid_extractor_endpoint')
        if not token:
            raise ValueError('missing_extractor_token')
        self.endpoint = endpoint.rstrip('/') + '/v1/telegram/fetch'
        self._token = token
        self.max_bytes = min(max_bytes, MAX_RESPONSE_BYTES)
        self._session_factory = session_factory

    async def fetch(self, channel_url, source_name, *, limit, timeout_seconds,
                    admission_timeout_seconds=None):
        if type(limit) is not int or not 1 <= limit <= MAX_ARTICLES:
            raise ValueError('invalid_telegram_limit')
        if not isinstance(source_name, str) or not source_name.strip():
            raise ValueError('invalid_telegram_source_name')
        if not isinstance(channel_url, str) or not channel_url.strip():
            raise ValueError('invalid_telegram_channel_url')
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise TelegramServiceError('deadline_exceeded', True)

        request_id = str(uuid4())

        async def request(attempt_timeout):
            payload = {
                'contract_version': '1', 'request_id': request_id,
                'channel_url': channel_url, 'source_name': source_name, 'limit': limit,
                'budget_ms': max(1, min(30_000, int(attempt_timeout * 1000))),
            }
            try:
                body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
            except UnicodeEncodeError:
                raise TelegramServiceError('invalid_request') from None
            if len(body) > MAX_REQUEST_BYTES:
                raise TelegramServiceError('request_too_large')
            async with self._session_factory(
                    timeout=aiohttp.ClientTimeout(total=attempt_timeout),
                    trust_env=False, auto_decompress=False) as session:
                async with session.post(
                        self.endpoint, data=body, allow_redirects=False,
                        headers={'Authorization': 'Bearer ' + self._token,
                                 'Content-Type': 'application/json',
                                 'Accept-Encoding': 'identity'}) as response:
                    if response.status != 200:
                        status_codes = {
                            400: 'invalid_request', 401: 'unauthorized', 403: 'unauthorized',
                            413: 'request_too_large', 429: 'busy', 503: 'unavailable',
                        }
                        retry_after = response.headers.get('Retry-After', '')
                        retry_after = min(int(retry_after), 86400) if retry_after.isdigit() else None
                        raise TelegramServiceError(
                            status_codes.get(response.status, 'http_error'),
                            response.status in {429, 502, 503, 504}, retry_after)
                    if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                        raise TelegramServiceError('invalid_payload')
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(16_384):
                        if len(data) + len(chunk) > self.max_bytes:
                            raise TelegramServiceError('response_too_large')
                        data.extend(chunk)
                    try:
                        result = json.loads(data)
                    except (ValueError, UnicodeError, RecursionError):
                        raise TelegramServiceError('invalid_payload') from None
                    return self._validate(result, request_id, channel_url, source_name, limit)

        try:
            return await run_admitted(self.endpoint, min(float(timeout_seconds), 30.0), request,
                                       TelegramServiceError,
                                       admission_timeout_seconds=admission_timeout_seconds)
        except aiohttp.ClientError:
            raise TelegramServiceError('transport_error', True) from None

    @classmethod
    def _validate(cls, payload, request_id, channel_url, source_name, requested_limit):
        if not isinstance(payload, dict):
            raise TelegramServiceError('invalid_payload')
        required = {'contract_version', 'build_version', 'request_id', 'original_url',
                    'final_url', 'outcome', 'articles'}
        allowed = required | {'fetch_method', 'error'}
        if (not required.issubset(payload) or set(payload) - allowed
                or payload.get('contract_version') != '1'
                or payload.get('request_id') != request_id
                or payload.get('original_url') != channel_url
                or not isinstance(payload.get('build_version'), str)
                or not payload['build_version']
                or payload.get('outcome') not in {'success', 'partial', 'failed', 'rejected'}
                or payload.get('fetch_method') not in {'http', 'native_browser', None}
                or (payload.get('outcome') in {'success', 'partial'}
                    and payload.get('fetch_method') not in {'http', 'native_browser'})
                or not cls._canonical_final_url(payload.get('final_url'), channel_url)):
            raise TelegramServiceError('invalid_payload')
        articles = payload['articles']
        if (not isinstance(articles, list) or len(articles) > MAX_ARTICLES
                or len(articles) > requested_limit):
            raise TelegramServiceError('invalid_payload')
        if payload['outcome'] in {'success', 'partial'} and not articles:
            raise TelegramServiceError('invalid_payload')
        if payload['outcome'] in {'failed', 'rejected'} and articles:
            raise TelegramServiceError('invalid_payload')
        for article in articles:
            cls._validate_article(article, source_name)
        error = payload.get('error')
        if error is not None and (
                not isinstance(error, dict) or set(error) != {'code', 'retryable'}
                or error.get('code') not in TELEGRAM_ERROR_CODES
                or type(error.get('retryable')) is not bool):
            raise TelegramServiceError('invalid_payload')
        if payload['outcome'] == 'partial' and error is None:
            raise TelegramServiceError('invalid_payload')
        if payload['outcome'] in {'failed', 'rejected'} and error is None:
            raise TelegramServiceError('invalid_payload')
        if payload['outcome'] == 'success' and error is not None:
            raise TelegramServiceError('invalid_payload')
        return payload

    @classmethod
    def _canonical_final_url(cls, final, original):
        channel = cls._channel_name(original)
        if channel is None or not isinstance(final, str):
            return False
        try:
            parsed = urlsplit(final)
            port = parsed.port
        except ValueError:
            return False
        return (parsed.scheme == 'https' and parsed.hostname == 't.me' and parsed.path == f'/s/{channel}'
                and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
                and port in (None, 443))

    @staticmethod
    def _channel_name(value):
        if not isinstance(value, str):
            return None
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError:
            return None
        if (parsed.scheme != 'https' or parsed.username or parsed.password
                or parsed.query or parsed.fragment or (port is not None and port != 443)):
            return None
        host = (parsed.hostname or '').lower()
        if host not in {'t.me', 'telegram.me'}:
            return None
        parts = [part for part in parsed.path.split('/') if part]
        if len(parts) == 2 and parts[0] == 's':
            name = parts[1]
        elif len(parts) == 1:
            name = parts[0]
        else:
            return None
        return name.lower()

    @staticmethod
    def _validate_article(article, source_name):
        fields = {'title', 'url', 'content', 'summary', 'published_at', 'source_type',
                  'source_name', 'image_url', 'media_files', 'raw_data'}
        if not isinstance(article, dict) or set(article) != fields:
            raise TelegramServiceError('invalid_payload')
        for key in ('title', 'url', 'content', 'source_type', 'source_name'):
            if not isinstance(article[key], str):
                raise TelegramServiceError('invalid_payload')
        if article['source_type'] != 'telegram' or article['source_name'] != source_name:
            raise TelegramServiceError('invalid_payload')
        if len(article['content']) > MAX_CONTENT_CHARS:
            raise TelegramServiceError('response_too_large')
        for key in ('summary', 'image_url', 'published_at'):
            if article[key] is not None and not isinstance(article[key], str):
                raise TelegramServiceError('invalid_payload')
        if article['published_at'] is not None:
            from datetime import datetime
            try:
                parsed_date = datetime.fromisoformat(article['published_at'].replace('Z', '+00:00'))
            except ValueError:
                raise TelegramServiceError('invalid_payload') from None
            if parsed_date.tzinfo is None or parsed_date.utcoffset() is None:
                raise TelegramServiceError('invalid_payload')
        media = article['media_files']
        if not isinstance(media, list) or len(media) > 4096:
            raise TelegramServiceError('invalid_payload')
        for item in media:
            if (not isinstance(item, dict) or not isinstance(item.get('type'), str)
                    or ('url' in item and item['url'] is not None and not isinstance(item['url'], str))):
                raise TelegramServiceError('invalid_payload')
        raw = article['raw_data']
        raw_fields = {'channel', 'access_method', 'content_length', 'media_info', 'has_media',
                      'media_count', 'external_links', 'hashtags', 'original_link',
                      'forwarded_from', 'message_id', 'telegram_url'}
        if not isinstance(raw, dict) or set(raw) != raw_fields:
            raise TelegramServiceError('invalid_payload')
        for key in ('channel', 'access_method', 'telegram_url'):
            if not isinstance(raw[key], str):
                raise TelegramServiceError('invalid_payload')
        for key in ('original_link', 'forwarded_from', 'message_id'):
            if raw[key] is not None and not isinstance(raw[key], str):
                raise TelegramServiceError('invalid_payload')
        for key in ('content_length', 'media_count'):
            if type(raw[key]) is not int or raw[key] < 0:
                raise TelegramServiceError('invalid_payload')
        if type(raw['has_media']) is not bool or not isinstance(raw['media_info'], dict):
            raise TelegramServiceError('invalid_payload')
        for key in ('external_links', 'hashtags'):
            if (not isinstance(raw[key], list) or len(raw[key]) > 4096
                    or any(not isinstance(value, str) for value in raw[key])):
                raise TelegramServiceError('invalid_payload')
