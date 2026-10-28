"""
Centralized AI prompts for news aggregator system.

This module contains all AI prompts used throughout the application.
Centralizing prompts makes them easier to maintain, version, and improve.
"""

from typing import Optional


class NewsPrompts:
    """Collection of all AI prompts for news processing."""

    # =============================================================================
    # CATEGORY SUMMARY GENERATION (only needed for individual categories)
    # =============================================================================
    
    @staticmethod
    def category_summary(category: str, articles_text: str) -> str:
        """
        Category-specific summary generation for daily digest.
        
        Used by: Orchestrator._generate_and_save_daily_summaries()
        
        NOTE: This summary will be combined with other categories in Telegram digest
        without additional AI processing - see Orchestrator._create_combined_digest()
        """
        return f"""Ты - новостной аналитик. Создай обзор ВСЕХ новостей категории {category} за день.

🎯 ЗАДАЧА: Обозреть ВСЕ значимые новости категории {category} в едином связном тексте
📏 ЛИМИТ: Максимум 850 символов (для объединения с другими категориями в телеграм)
📝 КОНТЕКСТ: Эта сводка будет частью общего дайджеста с другими категориями

ТРЕБОВАНИЯ:
- Единый связный текст, охватывающий ВСЕ важные новости дня
- Логическая структура: от главных событий к менее значимым
- Связки между событиями через контекст и причинно-следственные связи
- Живой журналистский язык с профессиональными переходами
- Используй: "На фоне этого", "В то же время", "Кроме того", "По данным"

🚫 НЕ ИСПОЛЬЗУЙ:
- Списки и перечисления (•, -, 1., 2.)
- Отдельные абзацы для каждой новости
- Фразы типа "в категории произошло", "среди новостей дня"

НОВОСТИ ЗА ДЕНЬ:
{articles_text}

Создай целостный обзор всех новостей категории {category} одним связным текстом:"""
    
    # Note: deprecated_telegram_digest() removed - TelegramAI.generate_daily_digest() 
    # was replaced by Orchestrator._create_combined_digest() which combines pre-generated summaries


class PromptBuilder:
    """Helper class for building prompts with dynamic parameters."""
    
    @staticmethod  
    def get_valid_categories() -> set:
        """Get list of valid news categories."""
        return {"Business", "Tech", "Science", "Nature", "Serbia", "Marketing", "Other"}
    
    @staticmethod
    def format_articles_for_summary(articles: list) -> str:
        """Format articles list for summary generation."""
        articles_text = ""
        for article in articles:
            articles_text += f"Заголовок: {article['headline']}\n"
            if article.get('description'):
                articles_text += f"Описание: {article['description'][:300]}...\n"
            articles_text += "---\n"
        return articles_text
    
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
