"""Core content extractor - main coordinator for all extraction strategies."""

import asyncio
import logging
import time
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
        result = await self.extract_article_content_with_metadata(url, retry_count)
        content = result.get('content')
        return content if content else None
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get extraction performance statistics."""
        return {
            'browser_active': self.browser is not None
        }

