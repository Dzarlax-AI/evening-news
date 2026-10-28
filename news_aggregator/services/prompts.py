"""
Centralized AI prompts for news aggregator system.

This module contains all AI prompts used throughout the application.
Centralizing prompts makes them easier to maintain, version, and improve.
"""

from typing import Optional


class NewsPrompts:
    """Collection of all AI prompts for news processing."""



class PromptBuilder:
    """Helper class for building prompts with dynamic parameters."""
    
    @staticmethod  
    def get_valid_categories() -> set:
        """Get list of valid news categories."""
        return {"Business", "Tech", "Science", "Nature", "Serbia", "Marketing", "Other"}
    
    # =============================================================================
    # CONTENT EXTRACTION PROMPTS
    # =============================================================================
    
    @staticmethod
    def extract_publication_date(html_content: str) -> str:
        """Generate prompt for extracting publication date from HTML."""
        return f"""Extract the publication date from this HTML content.

HTML CONTENT:
{html_content[:3000] if len(html_content) > 3000 else html_content}

TASK: Find the publication date/time when this article was published.

Look for:
- Published date/time metadata
- Article timestamps  
- Date in structured markup (JSON-LD, microdata, etc.)
- Visible date/time near article title
- Time elements with datetime attributes

RESPONSE FORMAT (JSON):
{{
  "date_found": true,
  "publication_date": "2025-01-15",
  "confidence": 0.8,
  "source": "meta tag with property='article:published_time'",
  "raw_text": "January 15, 2025"
}}

If no date found, respond with:
{{
  "date_found": false,
  "confidence": 0.0,
  "reason": "No publication date indicators found"
}}

Focus on finding the actual publication date, not update dates or other timestamps."""

    @staticmethod
    def find_full_article_link(html_content: str, base_url: str) -> str:
        """Generate prompt for finding full article link."""
        return f"""Find the link to the full article content from this HTML.

BASE URL: {base_url}

HTML CONTENT:
{html_content[:4000] if len(html_content) > 4000 else html_content}

TASK: Find a link that leads to the full article content (not summary/excerpt).

Look for:
- "Read more", "Continue reading", "Full article" links
- Links in article cards that point to detailed pages
- Main article title links
- Links with text like "Read full story", "See more", etc.

RESPONSE FORMAT (JSON):
{{
  "link_found": true,
  "full_article_url": "https://example.com/full-article",
  "confidence": 0.8,
  "link_text": "Read more",
  "selector": "a.read-more-link"
}}

If no link found, respond with:
{{
  "link_found": false,
  "confidence": 0.0,
  "reason": "No full article link found"
}}

Return the complete, absolute URL. If the link is relative, make it absolute using the base URL."""


# =============================================================================
# GLOBAL PROMPT CONSTANTS
# =============================================================================

# Character limits for different message types
TELEGRAM_SINGLE_MESSAGE_LIMIT = 3800
TELEGRAM_SPLIT_MESSAGE_LIMIT = 3700  
TELEGRAM_MAX_MESSAGE_LENGTH = 4096

# Default categories
DEFAULT_CATEGORIES = ["Business", "Tech", "Science", "Nature", "Serbia", "Marketing", "Other"]

# News source domains for context
DEFAULT_NEWS_DOMAINS = [
    'balkaninsight.com', 'biznis.rs', 'rts.rs', 'b92.net', 
    'politika.rs', 'blic.rs', 'novosti.rs', 'euronews.rs'
]
