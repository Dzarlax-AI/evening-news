"""AI API client for article summarization using Google Gemini."""

from typing import Optional, Dict, Any

import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception, before_sleep_log

from ..config import settings
from ..core.cache import cached
from ..core.exceptions import APIError
from ..core.resource_limits import ResourceLimitError, enforce_text_limit

logger = logging.getLogger(__name__)


def is_retryable_api_error(exception):
    """Check if API error is transient and should be retried."""
    if isinstance(exception, APIError):
        # Long provider cooldowns belong to durable article scheduling, not an
        # immediate transport retry that ignores Retry-After.
        if getattr(exception, 'retry_after_seconds', 0):
            return False
        # Retry on rate limits (429) or server errors (5xx)
        return exception.status_code in [429, 500, 502, 503, 504]
    return False


class AIClient:
    """Client for AI summarization using Google Gemini API."""
    
    def __init__(self):
        # Gemini configuration
        self.endpoint = getattr(
            settings, 
            'gemini_api_endpoint',
            'https://generativelanguage.googleapis.com/v1/models'
        )
        self.api_key = settings.gemini_api_key
        
        if not self.api_key:
            raise APIError("Gemini API key must be configured")
            
        # Task-specific models
        self.summarization_model = settings.summarization_model
        self.categorization_model = settings.categorization_model
        self.digest_model = settings.digest_model
        
        # Gemini supports structured output
        self.supports_structured_output = True
        self.provider = "gemini"
        self.enabled = True
    
    # Note: get_article_summary() method was removed and replaced by analyze_article_complete()
    # which provides combined analysis (summarization + categorization + ad detection) in a single request.
    
    async def extract_publication_date(self, html_content: str, url: str) -> Optional[str]:
        """
        Extract publication date from HTML content using AI.
        
        Args:
            html_content: HTML content of the page
            url: URL of the article (for context)
            
        Returns:
            Publication date in ISO format (YYYY-MM-DD) or None if not found
        """
        if not html_content or len(html_content) < 100:
            return None
        
        try:
            logger.info(f"  🗓️ AI extracting publication date for {url}")
            # Truncate HTML to manageable size
            content_sample = html_content[:3000] if len(html_content) > 3000 else html_content
            
            from .prompts import NewsPrompts
            prompt = NewsPrompts.extract_publication_date(content_sample)

            # Schema for structured output (Gemini)
            schema = {
                "type": "object",
                "properties": {
                    "date_found": {"type": "boolean"},
                    "publication_date": {"type": ["string", "null"]},
                    "confidence": {"type": "number"}
                },
                "required": ["date_found", "publication_date", "confidence"]
            }

            if self.supports_structured_output:
                # Use structured output for Gemini
                response_data = await self._make_structured_ai_request(
                    prompt,
                    model=self.summarization_model,
                    schema=schema,
                    analysis_type="publication_date",
                    domain=url
                )
                result = response_data.get("result") or {}
                
                if result.get("date_found") and result.get("publication_date"):
                    pub_date = result.get("publication_date", "").strip()
                    confidence = result.get("confidence", 0.0)
                    
                    if pub_date and confidence >= 0.5:
                        from datetime import datetime
                        try:
                            datetime.strptime(pub_date, '%Y-%m-%d')
                            logger.info(f"  ✅ AI found publication date: {pub_date} (confidence: {confidence:.2f})")
                            return pub_date
                        except ValueError:
                            logger.warning(f"  ⚠️ Invalid date format from AI: {pub_date}")
                            return None
                return None
        
        except Exception as e:
            logger.warning(f"  ⚠️ Error extracting publication date: {e}")
            return None
    
    async def extract_full_article_link(self, html_content: str, base_url: str) -> Optional[str]:
        """
        Extract link to full article content using AI.
        
        Args:
            html_content: HTML content of the current page
            base_url: Base URL for resolving relative links
            
        Returns:
            Full article URL or None if not found
        """
        if not html_content or len(html_content) < 100:
            return None
        
        try:
            logger.info(f"  🔗 AI extracting full article link for {base_url}")
            # Truncate HTML to manageable size
            content_sample = html_content[:4000] if len(html_content) > 4000 else html_content
            
            from .prompts import NewsPrompts
            prompt = NewsPrompts.find_full_article_link(content_sample, base_url)

            # Schema for structured output (Gemini)
            schema = {
                "type": "object",
                "properties": {
                    "link_found": {"type": "boolean"},
                    "full_article_url": {"type": ["string", "null"]},
                    "confidence": {"type": "number"}
                },
                "required": ["link_found", "full_article_url", "confidence"]
            }

            if self.supports_structured_output:
                # Use structured output for Gemini
                response_data = await self._make_structured_ai_request(
                    prompt,
                    model=self.summarization_model,
                    schema=schema,
                    analysis_type="full_article_link",
                    domain=base_url
                )
                result = response_data.get("result") or {}
                
                if result.get("link_found") and result.get("full_article_url"):
                    full_url = result.get("full_article_url", "").strip()
                    confidence = result.get("confidence", 0.0)
                    
                    if full_url and confidence >= 0.5:
                        from urllib.parse import urljoin, urlparse
                        # Make sure URL is absolute
                        if not full_url.startswith(('http://', 'https://')):
                            full_url = urljoin(base_url, full_url)
                        
                        # Validate URL format
                        parsed = urlparse(full_url)
                        if parsed.scheme and parsed.netloc:
                            logger.info(f"  ✅ AI found full article link: {full_url} (confidence: {confidence:.2f})")
                            return full_url
                        else:
                            logger.warning(f"  ⚠️ Invalid URL from AI: {full_url}")
                            return None
                return None
        
        except Exception as e:
            logger.warning(f"  ⚠️ Error extracting full article link: {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_retryable_api_error),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    async def _make_raw_ai_request(self, prompt: str, model: str = None,
                                    analysis_type: str = "combined_analysis",
                                    domain: str = "unknown",
                                    temperature: float = 0.1,
                                    max_tokens: int = 2000) -> dict:
        """
        Make raw AI API request to Gemini.
        Returns OpenAI-like response for backward compatibility.

        Args:
            prompt: Raw prompt text
            model: Model to use (defaults to summarization_model)
            analysis_type: Type of analysis for tracking
            domain: Domain being analyzed for tracking
            temperature: Temperature for generation (default: 0.1)
            max_tokens: Maximum tokens to generate (default: 2000)

        Returns:
            OpenAI-like response dict with 'choices' and 'usage'
        """
        if not model:
            model = self.summarization_model

        return await self._make_gemini_request(prompt, model, analysis_type, domain, temperature, max_tokens)
    
    async def _make_gemini_request(self, prompt: str, model: str,
                                   analysis_type: str, domain: str,
                                   temperature: float = 0.1, max_tokens: int = 2000) -> dict:
        """Generate the legacy OpenAI-like text response through the native runtime."""
        from .ai_runtime import get_ai_runtime

        return await get_ai_runtime().generate(
            mode="text", prompt=prompt, model=model, schema=None,
            temperature=temperature, max_tokens=max_tokens,
            analysis_type=analysis_type, domain=domain, timeout=30,
        )

    async def _make_structured_ai_request(self, prompt: str, model: str, schema: dict,
                                          analysis_type: str, domain: str) -> dict:
        """Return the legacy structured response shape from the native runtime."""
        from .ai_runtime import get_ai_runtime

        return await get_ai_runtime().generate(
            mode="structured", prompt=prompt, model=model, schema=schema,
            temperature=0.1, max_tokens=2000, analysis_type=analysis_type,
            domain=domain, timeout=40,
        )

    # ============================================================================
    # REMOVED: detect_advertising() - Replaced by unified analysis
    # ============================================================================
    # This method was replaced by analyze_article_complete() which includes
    # advertisement detection along with categorization and summarization
    # in a single API call for efficiency.
    
    # ============================================================================
    # REMOVED: Advertising detection helper methods - Replaced by unified analysis
    # ============================================================================
    # _parse_advertising_response() and _default_ad_response() were removed as
    # they are now handled by analyze_article_complete() unified processing.
    
    # Note: _summarize_content() method removed - all summarization now uses 
    # analyze_article_complete() which provides better quality results

    async def _call_summary_llm(self, prompt: str, *, system_prompt: str | None = None,
                                max_tokens: int = 1000) -> Optional[str]:
        # Build full prompt with system message if provided
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"

        logger.info(f"  🤖 Sending to AI for summarization...")
        # Use unified API with appropriate parameters for summarization
        response_data = await self._make_raw_ai_request(
            full_prompt,
            model=self.summarization_model,
            analysis_type="summary",
            domain="summary",
            temperature=0.2,  # Slightly higher for summarization
            max_tokens=max_tokens
        )
        
        if response_data and 'choices' in response_data and response_data['choices']:
            return (response_data['choices'][0]['message']['content'] or '').strip()
        return None

    def _is_summary_valid(self, summary: str, original: str) -> bool:
        if not summary or len(summary) < 60:
            return False
        # Check for Russian (presence of Cyrillic)
        has_cyrillic = any('а' <= ch.lower() <= 'я' for ch in summary)
        if not has_cyrillic:
            return False
        # Similarity check: avoid copies
        try:
            import difflib
            ratio = difflib.SequenceMatcher(None, summary[:1000], original[:1000]).ratio()
            if ratio > 0.80:
                return False
        except Exception:
            pass
        return True

    def _simple_extractive_summary(self, content: str) -> str:
        # Возьмём 3-4 первых информативных предложения до 600 символов
        import re
        sentences = re.split(r"(?<=[\.!?])\s+", content.strip())
        picked = []
        total = 0
        for s in sentences:
            if len(s) < 15:
                continue
            picked.append(s)
            total += len(s)
            if len(picked) >= 4 or total > 600:
                break
        text = " ".join(picked).strip()
        return text[:700] + ('...' if len(text) > 700 else '')

    async def test_connection(self) -> bool:
        """Test AI API connectivity with minimal request."""
        try:
            # Simple test prompt to check API availability (avoids heavy content extraction)
            test_prompt = "Test connectivity. Respond briefly."
            response = await self._make_raw_ai_request(test_prompt, model=self.summarization_model)
            return bool(response and 'choices' in response and response['choices'])
        except Exception:
            return False

    async def analyze_article_complete(self, title: str, content: str, url: str,
                                       original_context: str = None, deadline=None) -> Dict[str, Any]:
        """
        Complete article analysis with combined prompts - categorization, summarization,
        advertisement detection, and date extraction in one API call.
        
        Args:
            title: Article title
            content: Article content
            url: Article URL
            
        Returns:
            Dictionary with all analysis results
        """
        enforce_text_limit(content or '')
        enforce_text_limit(original_context or '')
        # Enhanced content length protection  
        if not content or not isinstance(content, str) or len(content.strip()) < 30:
            # Special case for scientific sources: allow analysis of informative titles
            is_scientific_source = url and any(domain in url.lower() for domain in ['nplus1.ru', 'arxiv.org', 'nature.com', 'sciencedirect.com'])
            
            if is_scientific_source and title and len(title.strip()) > 20:
                logger.info("Scientific source detected: analyzing informative title")
                # Use title as content for analysis
                content = title
            else:
                logger.warning(f"  ⚠️ Content too short for analysis: {len(content.strip()) if content else 0} chars < 30")
                return {'summary': None, 'categories': [], 'is_advertisement': None,
                        '_insufficient_content': True}
        
        from datetime import datetime, timedelta, timezone
        from .article_analysis import ArticleAnalysisError, analyze_article

        if deadline is None:
            deadline = datetime.now(timezone.utc) + timedelta(seconds=132)
        try:
            result = await analyze_article(
                title, content, url, original_context,
                model=self.summarization_model,
                max_text_chars=settings.extraction_text_max_chars,
                deadline=deadline,
            )
            return result
        except ResourceLimitError:
            raise
        except ArticleAnalysisError as exc:
            from ..core.exceptions import APIError
            error = APIError(exc.code, status_code=exc.status_code)
            error.retry_after_seconds = exc.retry_after_seconds
            logger.warning("Combined article analysis failed (%s)", exc.code)
            return {
                **self._get_fallback_analysis(),
                "_processing_error": type(error).__name__,
                "_processing_terminal": error.status_code in {400, 413, 422},
                "_retry_after_seconds": error.retry_after_seconds,
            }
        except Exception as exc:
            logger.error("Combined article analysis failed (%s)", type(exc).__name__)
            return {
                **self._get_fallback_analysis(),
                "_processing_error": type(exc).__name__,
                "_processing_terminal": getattr(exc, "status_code", None) in {400, 413, 422},
                "_retry_after_seconds": getattr(exc, "retry_after_seconds", None),
            }
    
    def _get_fallback_analysis(self) -> Dict[str, Any]:
        """Get fallback analysis when AI fails."""
        return {
            '_processing_error': 'ai_unavailable',
            'optimized_title': None,  # No title optimization in fallback
            'summary': None,
            'categories': ['Other'],  # Updated to new array format
            'category_confidences': [0.1],  # Array of confidences matching categories
            'category': 'Other',  # Keep for backward compatibility
            'category_confidence': 0.1,  # Keep for backward compatibility
            'summary_confidence': 0.0,
            'is_advertisement': False,
            'ad_type': 'news_article',
            'ad_confidence': 0.0,
            'ad_reasoning': 'Fallback analysis - AI unavailable',
            'publication_date': None,
            'content_quality': 0.2,
            'confidence': 0.1  # Legacy field
        }




# Global AI client instance
_ai_client: Optional[AIClient] = None


def get_ai_client() -> AIClient:
    """Get AI client instance."""
    global _ai_client
    
    if _ai_client is None:
        _ai_client = AIClient()
    
    return _ai_client
