import logging
"""AI-powered page structure analysis for better content extraction."""

import copy
import json
import hashlib
import re
import time
from dataclasses import asdict, dataclass
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

ANALYSIS_CACHE_MAX_ENTRIES = 128
ANALYSIS_CACHE_TTL_SECONDS = 24 * 60 * 60
ANALYSIS_CACHE_MAX_BYTES = 8 * 1024 * 1024
ANALYSIS_CACHE_MAX_KEY_BYTES = 4096
ANALYSIS_CACHE_MAX_ENTRY_BYTES = ANALYSIS_CACHE_MAX_BYTES



@dataclass
class PageAnalysis:
    """Result of AI page analysis."""
    content_selectors: List[str]
    title_selectors: List[str] 
    date_selectors: List[str]
    confidence_scores: Dict[str, float]
    page_type: str  # 'changelog', 'blog', 'news', 'docs'
    reasoning: str
    suggested_config: Dict[str, any]


class AIPageAnalyzer:
    """AI-powered analyzer for discovering optimal extraction patterns."""
    
    def __init__(self):
        self.analysis_cache: Dict[str, PageAnalysis] = {}
        self._analysis_cache_metadata: Dict[str, tuple[float, int]] = {}
        self._analysis_cache_bytes = 0

    def _cache_key(self, url: str, context: str) -> Optional[str]:
        """Return a fixed-size key, declining cache use for oversized inputs."""
        url = url or ""
        context = context or ""
        if len(url) + len(context) > ANALYSIS_CACHE_MAX_KEY_BYTES:
            return None
        url_bytes = url.encode("utf-8")
        context_bytes = context.encode("utf-8")
        if len(url_bytes) + len(context_bytes) > ANALYSIS_CACHE_MAX_KEY_BYTES:
            return None
        digest = hashlib.sha256()
        digest.update(url_bytes)
        digest.update(b"\0")
        digest.update(context_bytes)
        return digest.hexdigest()

    def _remove_cache_entry(self, key: str) -> None:
        self.analysis_cache.pop(key, None)
        metadata = self._analysis_cache_metadata.pop(key, None)
        if metadata:
            self._analysis_cache_bytes -= metadata[1]

    def _prune_expired_cache_entries(self, now: float) -> None:
        expired = [
            key for key, (created_at, _) in self._analysis_cache_metadata.items()
            if now - created_at >= ANALYSIS_CACHE_TTL_SECONDS
        ]
        for key in expired:
            self._remove_cache_entry(key)

    def _store_cache_entry(self, key: str, analysis: PageAnalysis) -> None:
        payload = json.dumps(asdict(analysis), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        size = len(key.encode("ascii")) + len(payload)
        if size > min(ANALYSIS_CACHE_MAX_ENTRY_BYTES, ANALYSIS_CACHE_MAX_BYTES):
            return

        now = time.monotonic()
        self._prune_expired_cache_entries(now)
        self._remove_cache_entry(key)
        while self.analysis_cache and (
            len(self.analysis_cache) >= ANALYSIS_CACHE_MAX_ENTRIES
            or self._analysis_cache_bytes + size > ANALYSIS_CACHE_MAX_BYTES
        ):
            oldest = min(
                self._analysis_cache_metadata,
                key=lambda candidate: (self._analysis_cache_metadata[candidate][0], candidate),
            )
            self._remove_cache_entry(oldest)

        self.analysis_cache[key] = copy.deepcopy(analysis)
        self._analysis_cache_metadata[key] = (now, size)
        self._analysis_cache_bytes += size
    
    async def analyze_page_structure(self, url: str, html: str, context: str = "news") -> Optional[PageAnalysis]:
        """
        Analyze page structure using AI to discover optimal selectors.
        
        Args:
            url: Page URL for context
            html: Raw HTML content
            context: What we're looking for ("changelog", "news", "blog")
        """
        # Oversized URL/context inputs still get analyzed, but never enter the cache.
        cache_key = self._cache_key(url, context)
        now = time.monotonic()
        self._prune_expired_cache_entries(now)
        if cache_key is not None and cache_key in self.analysis_cache:
            return copy.deepcopy(self.analysis_cache[cache_key])
        
        try:
            # Extract domain for tracking
            from urllib.parse import urlparse
            domain = urlparse(url).netloc if url else "unknown"
            logger.info(f"🤖 AI analyzing page structure for {url} (domain: {domain})")
            # Prepare HTML for analysis (reduce size)
            clean_html = self._prepare_html_for_analysis(html)

            # Build AI prompt for structure analysis
            prompt = self._build_structure_analysis_prompt(url, clean_html, context)

            # Get AI analysis
            from ..services.ai_client import get_ai_client
            ai_client = get_ai_client()
            response = await ai_client._make_raw_ai_request(
                prompt,
                model=ai_client.summarization_model,
                domain=domain  # Pass domain for tracking
            )

            if response and 'choices' in response:
                analysis_text = response['choices'][0]['message']['content']
                analysis = self._parse_ai_analysis(analysis_text, url)
                
                if analysis:
                    # Cache successful analysis
                    if cache_key is not None:
                        self._store_cache_entry(cache_key, analysis)
                    logger.info(f"  ✅ AI discovered {len(analysis.content_selectors)} content selectors")
                    return analysis
            
            logger.error("  ❌ AI analysis failed to parse response")
            return None
            
        except Exception as e:
            logger.error(f"  ❌ AI analysis error: {e}")
            return None
    
    def _prepare_html_for_analysis(self, html: str, max_length: int = 8000) -> str:
        """Prepare HTML for AI analysis by cleaning and truncating."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script, style, and other non-content elements
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'embed']):
            tag.decompose()
        
        # Get clean HTML string
        clean_html = str(soup)
        
        # Truncate if too long (keep beginning and end)
        if len(clean_html) > max_length:
            middle = max_length // 2
            clean_html = clean_html[:middle] + "\n\n... [TRUNCATED] ...\n\n" + clean_html[-middle:]
        
        return clean_html
    
    def _build_structure_analysis_prompt(self, url: str, html: str, context: str) -> str:
        """Build prompt for AI structure analysis."""
        
        context_examples = {
            'changelog': {
                'description': 'changelog entries, version updates, release notes',
                'typical_patterns': 'version numbers, dates, "Added", "Fixed", "Changed"',
                'examples': 'changelog items, release entries, update notes'
            },
            'news': {
                'description': 'news articles, announcements, press releases',
                'typical_patterns': 'publication dates, headlines, article summaries',
                'examples': 'news items, articles, announcements'
            },
            'blog': {
                'description': 'blog posts, articles, tutorials',
                'typical_patterns': 'post titles, author names, publication dates',
                'examples': 'blog posts, articles, tutorials'
            }
        }
        
        ctx = context_examples.get(context, context_examples['news'])
        
        prompt = f"""Analyze this webpage to find the best CSS selectors for extracting {ctx['description']}.

URL: {url}
Context: Looking for {ctx['examples']}
Typical patterns: {ctx['typical_patterns']}

HTML STRUCTURE:
```html
{html}
```

TASK: Identify CSS selectors that would reliably extract {ctx['examples']} from this page.

Look for:
1. CONTENT SELECTORS - Elements that contain the main {ctx['examples']}
2. TITLE SELECTORS - Elements with titles/headlines within those content elements
3. DATE SELECTORS - Elements with dates/timestamps
4. PAGE TYPE - What kind of page this is (changelog, blog, news, docs)

RESPOND IN JSON FORMAT:
{{
  "content_selectors": ["selector1", "selector2", "selector3"],
  "title_selectors": ["h1", "h2", ".title"],
  "date_selectors": ["time", ".date", ".published"],
  "confidence_scores": {{
    "selector1": 0.9,
    "selector2": 0.8
  }},
  "page_type": "changelog",
  "reasoning": "This page contains version update entries organized in a timeline format...",
  "suggested_config": {{
    "min_title_length": 10,
    "max_articles_per_check": 20,
    "wait_for_js": true
  }}
}}

Focus on selectors that:
- Target actual content, not navigation/ads/boilerplate
- Are specific enough to avoid false positives
- Will work consistently across page updates
- Follow common patterns for this type of content"""

        return prompt
    
    def _parse_ai_analysis(self, analysis_text: str, url: str) -> Optional[PageAnalysis]:
        """Parse AI analysis response into structured data."""
        try:
            # Extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', analysis_text)
            if not json_match:
                return None
            
            data = json.loads(json_match.group())
            
            # Validate required fields
            required_fields = ['content_selectors', 'title_selectors', 'page_type']
            for field in required_fields:
                if field not in data:
                    logger.warning(f"  ⚠️ Missing required field: {field}")
                    return None
            
            # Create analysis object
            analysis = PageAnalysis(
                content_selectors=data['content_selectors'][:10],  # Limit to top 10
                title_selectors=data['title_selectors'][:5],
                date_selectors=data.get('date_selectors', [])[:5],
                confidence_scores=data.get('confidence_scores', {}),
                page_type=data['page_type'],
                reasoning=data.get('reasoning', ''),
                suggested_config=data.get('suggested_config', {})
            )
            
            return analysis
            
        except json.JSONDecodeError as e:
            logger.error(f"  ❌ Failed to parse AI JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"  ❌ Error parsing AI analysis: {e}")
            return None
    
    async def analyze_content_changes(self, old_html: str, new_html: str, context: str = "changelog") -> Dict[str, any]:
        """
        Use AI to detect semantic changes between page versions.
        
        This is much smarter than hash comparison - it understands what changed.
        """
        try:
            # Prepare content for comparison
            old_clean = self._extract_content_for_comparison(old_html)
            new_clean = self._extract_content_for_comparison(new_html)
            
            if old_clean == new_clean:
                return {'changes_detected': False, 'change_type': 'none'}
            
            prompt = f"""Compare these two versions of a {context} page and detect meaningful changes.

OLD VERSION:
{old_clean[:2000]}

NEW VERSION:  
{new_clean[:2000]}

Identify:
1. What type of changes occurred?
2. Are these meaningful content updates or just UI/layout changes?
3. How many new items were added?
4. What's the significance level?

RESPOND IN JSON:
{{
  "changes_detected": true,
  "change_type": "new_content_added",
  "new_items_count": 2,
  "significance": "high",
  "summary": "Two new changelog entries added",
  "details": ["New version 1.2.3 released", "Bug fix for authentication issue"]
}}

Change types: "new_content_added", "content_updated", "content_removed", "layout_only", "none"
Significance: "high", "medium", "low"
"""
            
            from ..services.ai_client import get_ai_client
            ai_client = get_ai_client()
            response = await ai_client._make_raw_ai_request(prompt, model=ai_client.summarization_model)
            
            if response and 'choices' in response:
                analysis_text = response['choices'][0]['message']['content']
                json_match = re.search(r'\{[\s\S]*\}', analysis_text)
                
                if json_match:
                    return json.loads(json_match.group())
            
            # Fallback to simple comparison
            return {
                'changes_detected': True,
                'change_type': 'unknown',
                'significance': 'medium',
                'summary': 'Content changes detected'
            }
            
        except Exception as e:
            logger.error(f"❌ AI change analysis failed: {e}")
            return {'changes_detected': True, 'change_type': 'unknown'}
    
    def _extract_content_for_comparison(self, html: str) -> str:
        """Extract main content text for comparison."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove noise
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            tag.decompose()
        
        # Get text content
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text[:3000]  # Limit for comparison


# Global instance
_ai_analyzer = None

async def get_ai_page_analyzer() -> AIPageAnalyzer:
    """Get or create AI page analyzer instance."""
    global _ai_analyzer
    if _ai_analyzer is None:
        _ai_analyzer = AIPageAnalyzer()
    return _ai_analyzer
