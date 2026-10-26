"""Formatting helpers shared by public API handlers and domain services."""

import html
import re
from urllib.parse import urlparse


def extract_domain(url: str) -> str:
    """Extract the lowercase network location from a URL."""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return "unknown"


def clean_html_entities(text: str) -> str:
    """Clean HTML entities and malformed markup from display text."""
    if not text:
        return text

    cleaned = html.unescape(text)
    cleaned = re.sub(r"[\"']>", " ", cleaned)
    cleaned = re.sub(r'<?\s*a\s+href=[\'\"]?[^\s\'\"<>]*[\'\"]?\s*[^<>]*/?a\s*>?', ' ', cleaned)
    cleaned = re.sub(r"<[^>]*>", "", cleaned)
    cleaned = re.sub(r"@\w+\s+/[^\s]*\s+Читать оригинал", "", cleaned)
    cleaned = cleaned.replace("<", " ").replace(">", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def clean_summary_text(raw_summary: str) -> str:
    """Remove common service text and prompt repetition from summaries."""
    if not raw_summary:
        return ""

    service_phrases = [
        r"^Краткое содержание статьи на русском языке с основными тезисами:\s*",
        r"^Краткое содержание статьи на русском языке:\s*",
        r"^Краткое содержание:\s*",
        r"^Основные тезисы статьи:\s*",
        r"^Основные тезисы:\s*",
        r"^Суммаризация статьи:\s*",
        r"^Пересказ статьи:\s*",
        r"^Содержание статьи:\s*",
        r"^Вот краткое содержание:\s*",
        r"^Вот основные тезисы:\s*",
        r"^Статья содержит следующие основные моменты:\s*",
        r"^Статья рассказывает о следующем:\s*",
        r"^ПЕРЕСКАЗ:\s*",
    ]
    cleaned_summary = raw_summary
    for phrase_pattern in service_phrases:
        cleaned_summary = re.sub(phrase_pattern, "", cleaned_summary, flags=re.IGNORECASE | re.MULTILINE)
    cleaned_summary = cleaned_summary.strip()
    cleaned_summary = re.sub(r"^[-•·*]\s*", "", cleaned_summary, flags=re.MULTILINE)
    cleaned_summary = re.sub(r"^\d+\.\s*$", "", cleaned_summary, flags=re.MULTILINE)
    cleaned_summary = re.sub(r"\n\s*\n", "\n\n", cleaned_summary)
    cleaned_summary = re.sub(r":\s*$", "", cleaned_summary)
    return cleaned_summary.strip()
