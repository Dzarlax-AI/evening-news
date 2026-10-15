import logging
from ..core.resource_limits import ResourceLimitError
"""AI-powered extraction optimization service."""

import json
import re
import time
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from urllib.parse import urlparse

from ..core.http_client import get_http_client
from ..core.exceptions import APIError
# Import ai_client dynamically to avoid circular import
from .extraction_memory import get_extraction_memory, ExtractionAttempt
from .domain_stability_tracker import get_stability_tracker

logger = logging.getLogger(__name__)


@dataclass
class AIAnalysisResult:
    """Result of AI analysis for content extraction."""
    domain: str
    selectors_discovered: List[str]
    confidence_scores: Dict[str, float]
    analysis_reasoning: str
    tokens_used: int
    analysis_time_ms: int
    recommended_strategy: str = "html_parsing"
    # New fields for metadata extraction
    date_selectors: List[str] = None
    date_confidence_scores: Dict[str, float] = None
    requires_link_following: bool = False
    link_patterns: List[str] = None


@dataclass
class OptimizationRecommendation:
    """Optimization recommendation from AI analysis."""
    domain: str
    current_success_rate: float
    recommended_selectors: List[str]
    estimated_improvement: float
    reasoning: str
    priority: str  # high, medium, low


class AIExtractionOptimizer:
    """AI-powered extraction optimization service."""
    
    def __init__(self):
        self.max_selectors_per_analysis = 5
        self.min_confidence_threshold = 0.3
        self.ai_analysis_cooldown = 3600  # 1 hour between analyses for same domain
        
        # Selector patterns that AI commonly suggests
        self.common_ai_patterns = {
            'content_containers': [
                'article', 'main', '[role="main"]', '.content', '.article-content',
                '.post-content', '.entry-content', '.story-content'
            ],
            'text_blocks': [
                '.text', '.article-text', '.story-text', '.content-text',
                'p:not(.meta):not(.byline)', '.paragraph', '.text-block'
            ],
            'modern_frameworks': [
                '.prose', '.prose-lg', '.prose-xl',  # Tailwind
                '.container .text-base', '.max-w-prose',
                '.leading-relaxed', '.text-gray-900'
            ]
        }
    
    async def analyze_domain_extraction(self, domain: str, sample_urls: List[str] = None) -> Optional[AIAnalysisResult]:
        """Perform AI analysis of domain extraction patterns."""
        start_time = time.time()
        
        logger.info(f"🤖 Starting AI analysis for domain: {domain}")
        try:
            # Check if we should skip AI analysis
            stability_tracker = await get_stability_tracker()
            should_analyze, reason = stability_tracker.should_use_ai_optimization(domain)
            
            if not should_analyze:
                logger.info(f"  ⏭️ Skipping AI analysis: {reason}")
                stability_tracker.increment_credits_saved(domain)
                return None
            
            logger.info(f"  🎯 AI analysis triggered: {reason}")
            # Get sample HTML if URLs provided
            sample_html = None
            if sample_urls:
                sample_html = await self._fetch_sample_html(sample_urls[0])
            
            # Get existing extraction patterns from memory
            memory = await get_extraction_memory()
            existing_patterns = await memory.get_best_patterns_for_domain(domain, limit=10)
            
            # Build AI analysis prompt
            prompt = self._build_analysis_prompt(domain, sample_html, existing_patterns)
            
            # Import and get AI client dynamically to avoid circular import
            from .ai_client import get_ai_client
            ai_client = get_ai_client()
            response = await ai_client._make_raw_ai_request(prompt, model=ai_client.summarization_model)
            
            if not response or 'choices' not in response:
                logger.error(f"  ❌ No valid AI response for {domain}")
                return None
            
            # Parse AI response
            ai_text = response['choices'][0]['message']['content']
            analysis_result = self._parse_ai_analysis(domain, ai_text)
            
            if analysis_result:
                analysis_time = int((time.time() - start_time) * 1000)
                analysis_result.analysis_time_ms = analysis_time
                
                # Prefer API usage metadata when available (Gemini/Constructor), fallback to estimate
                usage = response.get("usage", {}) if isinstance(response, dict) else {}
                prompt_tokens = usage.get("prompt_tokens", 0) or 0
                completion_tokens = usage.get("completion_tokens", 0) or 0
                total_tokens = usage.get("total_tokens")

                if not total_tokens:
                    total_tokens = prompt_tokens + completion_tokens

                if not total_tokens:
                    total_tokens = len(prompt.split()) + len(ai_text.split())

                analysis_result.tokens_used = int(total_tokens)
                
                logger.info(f"  ✅ AI analysis complete: {len(analysis_result.selectors_discovered)} selectors discovered")
                # Record AI analysis in database
                await memory.record_ai_pattern_discovery(
                    domain,
                    [
                        {
                            'selector': sel,
                            'confidence': analysis_result.confidence_scores.get(sel, 0.5),
                            'strategy': analysis_result.recommended_strategy
                        }
                        for sel in analysis_result.selectors_discovered
                    ],
                    analysis_type="domain_optimization"
                )
                
                # Update stability tracker
                stability_tracker.mark_ai_analysis_completed(
                    domain,
                    len(analysis_result.selectors_discovered),
                    analysis_result.tokens_used,
                    analysis_result.tokens_used * 0.0001  # Rough cost estimate
                )
                
                return analysis_result
            
            return None
            
        except ResourceLimitError:
            raise
        except Exception as e:
            logger.error(f"  ❌ AI analysis failed for {domain}: {e}")
            return None
    
    async def _fetch_sample_html(self, url: str) -> Optional[str]:
        """Fetch sample HTML for analysis."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
            
            async with get_http_client() as client:
                from ..core.resource_limits import read_page_text
                async with await client.get(url, headers=headers, timeout=15, auto_decompress=False) as response:
                    if response.status == 200:
                        html = await read_page_text(response)
                        # Bound the body before reducing the AI sample.
                        return html[:8000] if html else None
                    
        except ResourceLimitError:
            raise
        except Exception as e:
            logger.warning(f"    ⚠️ Failed to fetch sample HTML from {url}: {e}")
        return None
    
    def _build_analysis_prompt(self, domain: str, sample_html: Optional[str], existing_patterns: List) -> str:
        """Build prompt for AI analysis."""
        prompt = f"""Analyze the website {domain} for content extraction optimization.

TASK: Find CSS selectors that can reliably extract main article content.

DOMAIN ANALYSIS:
- Target domain: {domain}
- Known extraction issues: Content extraction failing or producing low-quality results
- Goal: Find reliable CSS selectors for main article text content

"""
        
        if existing_patterns:
            prompt += "EXISTING PATTERNS (with limited success):\n"
            for pattern in existing_patterns[:5]:
                success_rate = pattern.success_rate
                prompt += f"- {pattern.selector_pattern} ({pattern.extraction_strategy}) - {success_rate:.1f}% success\n"
            prompt += "\n"
        
        if sample_html:
            prompt += f"SAMPLE HTML STRUCTURE:\n```html\n{sample_html}\n```\n\n"
        
        prompt += """REQUIREMENTS:
1. Suggest 3-5 CSS selectors that likely contain main article content
2. Find selectors for publication date/time metadata
3. Detect if content requires following links to get full article text
4. Focus on semantic HTML elements and common content patterns
5. Avoid navigation, ads, sidebars, comments, related articles
6. Consider modern CSS frameworks (Tailwind, Bootstrap) and CMS patterns
7. Prioritize selectors that work across multiple pages on the domain

RESPONSE FORMAT (JSON):
{
  "selectors": [
    {
      "selector": ".article-content",
      "confidence": 0.8,
      "reasoning": "Common CMS pattern for main content"
    }
  ],
  "date_selectors": [
    {
      "selector": ".published-date",
      "confidence": 0.9,
      "reasoning": "Publication date metadata"
    }
  ],
  "requires_link_following": false,
  "link_patterns": [
    {
      "selector": ".read-more-link",
      "confidence": 0.7,
      "reasoning": "Links to full article content"
    }
  ],
  "recommended_strategy": "html_parsing",
  "analysis": "Brief analysis of the site structure and why these selectors were chosen"
}

Focus on finding robust, reliable selectors that will work consistently across the domain."""
        
        return prompt
    
    def _parse_ai_analysis(self, domain: str, ai_response: str) -> Optional[AIAnalysisResult]:
        """Parse AI analysis response."""
        try:
            # Try to extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', ai_response)
            if not json_match:
                logger.warning("  ⚠️ No JSON found in AI response")
                return None
            
            data = json.loads(json_match.group())
            
            selectors = []
            confidence_scores = {}
            
            for selector_data in data.get('selectors', []):
                if isinstance(selector_data, dict):
                    selector = selector_data.get('selector', '').strip()
                    confidence = float(selector_data.get('confidence', 0.5))
                    
                    if selector and confidence >= self.min_confidence_threshold:
                        selectors.append(selector)
                        confidence_scores[selector] = confidence
                elif isinstance(selector_data, str):
                    # Fallback for simple string selectors
                    selector = selector_data.strip()
                    if selector:
                        selectors.append(selector)
                        confidence_scores[selector] = 0.5
            
            # Parse date selectors
            date_selectors = []
            date_confidence_scores = {}
            
            for date_data in data.get('date_selectors', []):
                if isinstance(date_data, dict):
                    selector = date_data.get('selector', '').strip()
                    confidence = float(date_data.get('confidence', 0.5))
                    
                    if selector and confidence >= self.min_confidence_threshold:
                        date_selectors.append(selector)
                        date_confidence_scores[selector] = confidence
            
            # Parse link patterns for full article content
            link_patterns = []
            for link_data in data.get('link_patterns', []):
                if isinstance(link_data, dict):
                    selector = link_data.get('selector', '').strip()
                    if selector:
                        link_patterns.append(selector)
            
            if not selectors:
                logger.warning("  ⚠️ No valid selectors found in AI response")
                return None
            
            return AIAnalysisResult(
                domain=domain,
                selectors_discovered=selectors[:self.max_selectors_per_analysis],
                confidence_scores=confidence_scores,
                analysis_reasoning=data.get('analysis', 'AI-generated selector analysis'),
                tokens_used=0,  # Will be set by caller
                analysis_time_ms=0,  # Will be set by caller
                recommended_strategy=data.get('recommended_strategy', 'html_parsing'),
                # New fields
                date_selectors=date_selectors,
                date_confidence_scores=date_confidence_scores,
                requires_link_following=data.get('requires_link_following', False),
                link_patterns=link_patterns
            )
            
        except json.JSONDecodeError as e:
            logger.warning(f"  ⚠️ Failed to parse AI JSON response: {e}")
            return None
        except Exception as e:
            logger.warning(f"  ⚠️ Error parsing AI analysis: {e}")
            return None
    
    async def get_optimization_recommendations(self, limit: int = 5) -> List[OptimizationRecommendation]:
        """Get optimization recommendations for domains that need improvement."""
        memory = await get_extraction_memory()
        domains_needing_help = await memory.get_domains_needing_ai_analysis(limit)
        
        recommendations = []
        
        for domain in domains_needing_help:
            stats = await memory.get_domain_extraction_stats(domain)
            
            # Determine priority based on stats
            current_success_rate = stats.get('success_rate', 0)
            total_attempts = stats.get('total_attempts', 0)
            
            if current_success_rate < 20 and total_attempts >= 5:
                priority = "high"
                estimated_improvement = 60.0
            elif current_success_rate < 50 and total_attempts >= 3:
                priority = "medium"
                estimated_improvement = 40.0
            else:
                priority = "low"
                estimated_improvement = 20.0
            
            # Get best existing patterns to build on
            patterns = await memory.get_best_patterns_for_domain(domain, limit=3)
            
            recommendation = OptimizationRecommendation(
                domain=domain,
                current_success_rate=current_success_rate,
                recommended_selectors=self._suggest_fallback_selectors(domain),
                estimated_improvement=estimated_improvement,
                reasoning=self._build_recommendation_reasoning(stats, patterns),
                priority=priority
            )
            
            recommendations.append(recommendation)
        
        # Sort by priority and success rate
        priority_order = {'high': 3, 'medium': 2, 'low': 1}
        recommendations.sort(
            key=lambda x: (priority_order[x.priority], -x.current_success_rate),
            reverse=True
        )
        
        return recommendations
    
    def _suggest_fallback_selectors(self, domain: str) -> List[str]:
        """Suggest fallback selectors based on domain characteristics."""
        selectors = []
        
        # Add domain-specific patterns
        if any(news_indicator in domain for news_indicator in ['news', 'новости', 'times', 'post']):
            selectors.extend([
                '.article-content', '.news-content', '.story-text',
                'article', '[role="main"]', '.content'
            ])
        
        if any(blog_indicator in domain for blog_indicator in ['blog', 'блог', 'medium']):
            selectors.extend([
                '.post-content', '.entry-content', '.blog-post',
                'article', '.prose', '.content'
            ])
        
        # Add modern framework patterns
        selectors.extend(self.common_ai_patterns['modern_frameworks'])
        
        # Add general content patterns
        selectors.extend(self.common_ai_patterns['content_containers'][:3])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_selectors = []
        for selector in selectors:
            if selector not in seen:
                seen.add(selector)
                unique_selectors.append(selector)
        
        return unique_selectors[:5]
    
    def _build_recommendation_reasoning(self, stats: Dict, patterns: List) -> str:
        """Build reasoning for optimization recommendation."""
        success_rate = stats.get('success_rate', 0)
        total_attempts = stats.get('total_attempts', 0)
        
        reasoning_parts = []
        
        if success_rate < 20:
            reasoning_parts.append("Very low success rate indicates fundamental extraction issues")
        elif success_rate < 50:
            reasoning_parts.append("Below-average success rate suggests suboptimal selectors")
        
        if total_attempts >= 10:
            reasoning_parts.append("Sufficient data available for reliable optimization")
        elif total_attempts >= 5:
            reasoning_parts.append("Moderate data available for optimization")
        
        if patterns:
            best_pattern = patterns[0]
            if best_pattern.success_rate < 30:
                reasoning_parts.append("Current best patterns show poor performance")
            else:
                reasoning_parts.append("Some patterns show promise but need improvement")
        else:
            reasoning_parts.append("No successful patterns identified yet")
        
        if not reasoning_parts:
            reasoning_parts.append("Domain analysis needed to improve extraction reliability")
        
        return ". ".join(reasoning_parts) + "."
    
    async def optimize_domain_extraction(self, domain: str, sample_urls: List[str] = None) -> bool:
        """Perform full optimization analysis and update for a domain."""
        logger.info(f"🔧 Optimizing extraction for domain: {domain}")
        try:
            # Run AI analysis
            analysis = await self.analyze_domain_extraction(domain, sample_urls)
            
            if analysis and analysis.selectors_discovered:
                logger.info(f"  ✅ Optimization complete: {len(analysis.selectors_discovered)} new patterns added")
                return True
            else:
                logger.warning(f"  ⚠️ No optimization improvements found for {domain}")
                return False
                
        except Exception as e:
            logger.error(f"  ❌ Optimization failed for {domain}: {e}")
            return False
    
    async def get_ai_optimization_stats(self) -> Dict:
        """Get statistics about AI optimization effectiveness."""
        memory = await get_extraction_memory()
        stats = await memory.get_extraction_efficiency_stats()
        
        stability_tracker = await get_stability_tracker()
        stability_stats = stability_tracker.get_domain_performance_summary()
        
        return {
            **stats,
            'stability_stats': stability_stats,
            'optimization_active': True,
            'ai_analysis_enabled': True
        }


# Global instance
_ai_optimizer = None

async def get_ai_extraction_optimizer() -> AIExtractionOptimizer:
    """Get or create AI extraction optimizer instance."""
    global _ai_optimizer
    if _ai_optimizer is None:
        _ai_optimizer = AIExtractionOptimizer()
    return _ai_optimizer
