"""Bounded private extractor API v1 client. No retries or persistence live here."""

import asyncio
import json
from uuid import uuid4
from urllib.parse import urlsplit

import aiohttp


class ExtractionServiceError(Exception):
    """A stable classification safe to log without response content or credentials."""

    def __init__(self, code, retryable=False, retry_after_seconds=None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds

    @property
    def terminal(self):
        """Whether article processing is durably terminal; this differs from wire retryability."""
        return not self.retryable and self.code in {
            'resource_limit', 'invalid_input', 'unsupported_input', 'request_too_large', 'invalid_request',
            'public_url_denied', 'upstream_auth_required', 'native_browser_policy_denied',
            'native_browser_unsupported', 'native_browser_challenge', 'native_browser_no_content',
            'native_browser_http_rejected'}


class ExtractionServiceClient:
    def __init__(self, endpoint, token, *, max_bytes=5 * 1024 * 1024,
                 max_chars=100000, session_factory=aiohttp.ClientSession):
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError('invalid_extractor_endpoint')
        if not token:
            raise ValueError('missing_extractor_token')
        self.endpoint = endpoint.rstrip('/') + '/v1/extract'
        self._token = token
        self.max_bytes = min(max_bytes, 5 * 1024 * 1024)
        self.max_chars = min(max_chars, 100000)
        self._session_factory = session_factory

    async def extract(self, url, *, timeout_seconds, learned_pattern=None, html=None,
                      content=None, title=None, strategy='auto', source_type='article', use_browser=False):
        if timeout_seconds <= 0:
            raise ExtractionServiceError('deadline_exceeded', True)
        request_id = str(uuid4())
        payload = {'request_id': request_id, 'url': url, 'strategy': strategy, 'source_type': source_type,
                   'budget_ms': max(1, min(30000, int(timeout_seconds * 1000))),
                   'max_bytes': self.max_bytes, 'max_chars': self.max_chars}
        for name, value in (('html', html), ('content', content), ('title', title), ('use_browser', use_browser)):
            if value is not None:
                payload[name] = value
        if learned_pattern:
            payload.update(learned_selector=learned_pattern.get('selector') or '',
                           learned_method=learned_pattern.get('method') or '')
        body = json.dumps(payload).encode('utf-8')
        if len(body) > self.max_bytes:
            raise ExtractionServiceError('request_too_large')
        try:
            # One request owns one session: completion/cancellation always closes it.
            async with asyncio.timeout(timeout_seconds):
                async with self._session_factory(timeout=aiohttp.ClientTimeout(total=timeout_seconds),
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
                            raise ExtractionServiceError(codes.get(response.status, 'http_error'),
                                response.status in {429, 502, 503, 504}, retry_after)
                        if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                            raise ExtractionServiceError('invalid_payload')
                        data = bytearray()
                        async for chunk in response.content.iter_chunked(16384):
                            if len(data) + len(chunk) > self.max_bytes:
                                raise ExtractionServiceError('response_too_large')
                            data.extend(chunk)
                        try:
                            result = json.loads(data)
                        except (ValueError, UnicodeError, RecursionError):
                            raise ExtractionServiceError('invalid_payload') from None
                        return self._validate(result, request_id, url)
        except TimeoutError:
            raise ExtractionServiceError('deadline_exceeded', True) from None
        except aiohttp.ClientError:
            raise ExtractionServiceError('transport_error', True) from None

    def _validate(self, payload, request_id, url):
        if (not isinstance(payload, dict) or payload.get('contract_version') != '1'
                or payload.get('request_id') != request_id
                or payload.get('original_url') != url
                or payload.get('outcome') not in {'success', 'rejected', 'failed', 'needs_selector_study'}):
            raise ExtractionServiceError('invalid_payload')
        result = payload.get('result')
        if not isinstance(result, dict):
            raise ExtractionServiceError('invalid_payload')
        error = payload.get('error')
        if error is not None and (not isinstance(error, dict)
                or not isinstance(error.get('code'), str)
                or not isinstance(error.get('retryable'), bool)):
            raise ExtractionServiceError('invalid_payload')
        if error is not None and error.get('retry_after_seconds') is not None:
            retry_after = error['retry_after_seconds']
            if type(retry_after) is not int or not 0 <= retry_after <= 86400:
                raise ExtractionServiceError('invalid_payload')
        for field in ('content', 'title', 'publication_date', 'author', 'description', 'image_url',
                      'method', 'method_used', 'selector_used'):
            if result.get(field) is not None and not isinstance(result[field], str):
                raise ExtractionServiceError('invalid_payload')
        content = result.get('content') or ''
        if len(content) > self.max_chars:
            raise ExtractionServiceError('response_too_large')
        if payload['outcome'] == 'success' and not content.strip():
            raise ExtractionServiceError('invalid_payload')
        media = result.get('media') or []
        if not isinstance(media, list) or any(not isinstance(m, dict)
                or not isinstance(m.get('type'), str) or not isinstance(m.get('url'), str) for m in media):
            raise ExtractionServiceError('invalid_payload')
        observations = payload.get('observations') or []
        if not isinstance(observations, list) or len(observations) > 64:
            raise ExtractionServiceError('invalid_payload')
        for observation in observations:
            if (not isinstance(observation, dict) or not isinstance(observation.get('selector'), str)
                    or not isinstance(observation.get('method'), str)
                    or not isinstance(observation.get('success'), bool)):
                raise ExtractionServiceError('invalid_payload')
        return payload
