"""HTML utilities for news processing."""

import re
from typing import Optional
from bs4 import BeautifulSoup


def validate_telegram_html(html: str) -> Optional[str]:
    """
    Validate and clean HTML for Telegram Bot API.
    
    Telegram supports only a subset of HTML tags:
    <b>, <strong>, <i>, <em>, <u>, <ins>, <s>, <strike>, <del>, <a>, <code>, <pre>
    
    Args:
        html: Input HTML string
        
    Returns:
        Clean HTML or None if validation fails
    """
    if not html or not html.strip():
        return None
        
    try:
        # Parse HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Allowed tags for Telegram
        allowed_tags = {
            'b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 
            'a', 'code', 'pre', 'tg-spoiler'
        }
        
        # Remove disallowed tags but keep their content
        for tag in soup.find_all():
            if tag.name not in allowed_tags:
                tag.unwrap()
        
        # Clean up link attributes (only href allowed)
        for a_tag in soup.find_all('a'):
            # Keep only href attribute
            attrs_to_remove = [attr for attr in a_tag.attrs if attr != 'href']
            for attr in attrs_to_remove:
                del a_tag[attr]
        
        # Get clean HTML
        clean_html = str(soup)
        
        # Remove excessive whitespace
        clean_html = re.sub(r'\n\s*\n', '\n\n', clean_html)
        clean_html = re.sub(r' +', ' ', clean_html)
        
        # Check length (Telegram limit is 4096)
        if len(clean_html) > 4000:
            return smart_truncate_html(clean_html, 4000)
        
        return clean_html.strip()
        
    except Exception:
        # If parsing fails, return plain text version
        try:
            soup = BeautifulSoup(html, 'html.parser')
            plain_text = soup.get_text(separator=' ')
            return plain_text[:4000] if len(plain_text) > 4000 else plain_text
        except Exception:
            return None


def validate_telegram_rich_html(html: str) -> Optional[str]:
    """
    Validate and clean HTML for Telegram Rich Messages.

    Rich Message HTML supports block-level structure that regular
    parse_mode=HTML does not. Keep this separate from validate_telegram_html()
    so regular Telegram messages stay constrained to their older tag subset.
    """
    if not html or not html.strip():
        return None

    try:
        soup = BeautifulSoup(html, 'html.parser')

        allowed_tags = {
            'a', 'b', 'blockquote', 'br', 'cite', 'code', 'details', 'em',
            'figcaption', 'figure', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'hr', 'i', 'li', 'ol', 'p', 'pre', 's', 'strong', 'sub',
            'summary', 'sup', 'tg-reference', 'u', 'ul',
        }
        allowed_attrs = {
            'a': {'href', 'name'},
            'details': {'open'},
            'tg-reference': {'name'},
        }

        for tag in soup.find_all():
            if tag.name not in allowed_tags:
                tag.unwrap()
                continue

            allowed = allowed_attrs.get(tag.name, set())
            for attr in list(tag.attrs):
                if attr not in allowed:
                    del tag[attr]

            if tag.name == 'a' and tag.get('href'):
                href = str(tag['href']).strip()
                if not (
                    href.startswith('http://')
                    or href.startswith('https://')
                    or href.startswith('#')
                    or href.startswith('mailto:')
                    or href.startswith('tel:')
                    or href.startswith('tg://user?id=')
                ):
                    del tag['href']

        clean_html = str(soup)
        clean_html = re.sub(r'\n\s*\n\s*\n+', '\n\n', clean_html)
        clean_html = re.sub(r' +', ' ', clean_html)
        return clean_html.strip()

    except Exception:
        return None


def smart_truncate_html(html: str, max_length: int) -> str:
    """
    Smart truncation of HTML that preserves structure.
    
    Args:
        html: HTML string to truncate
        max_length: Maximum character length
        
    Returns:
        Truncated HTML with proper tag closure
    """
    if len(html) <= max_length:
        return html
    
    # Find a good truncation point (end of sentence, word, etc.)
    truncate_at = max_length - 50  # Leave some room for closing tags
    
    # Look for good break points in reverse order
    break_points = ['. ', '! ', '? ', '\n\n', '\n', '. ', ', ', ' ']
    
    for break_point in break_points:
        pos = html.rfind(break_point, 0, truncate_at)
        if pos > max_length // 2:  # Don't truncate too early
            truncate_at = pos + len(break_point)
            break
    
    # Truncate and add ellipsis
    truncated = html[:truncate_at]
    
    # Try to close open tags
    try:
        soup = BeautifulSoup(truncated + '...', 'html.parser')
        return str(soup)
    except Exception:
        return truncated + '...'


def strip_html_tags(html: str) -> str:
    """
    Remove all HTML tags and return plain text.
    
    Args:
        html: HTML string
        
    Returns:
        Plain text without HTML tags
    """
    if not html:
        return ""
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text(separator=' ')
    except Exception:
        # Fallback: simple regex replacement
        clean_text = re.sub(r'<[^>]+>', '', html)
        clean_text = re.sub(r'\s+', ' ', clean_text)
        return clean_text.strip()


def clean_html_content(html: str, max_length: Optional[int] = None) -> str:
    """
    Clean HTML content for processing.
    
    Args:
        html: HTML string
        max_length: Optional maximum length
        
    Returns:
        Cleaned HTML or plain text
    """
    if not html:
        return ""
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text content
        text = soup.get_text(separator=' ')
        
        # Clean whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        if max_length and len(text) > max_length:
            text = text[:max_length] + '...'
        
        return text
        
    except Exception:
        # Fallback
        clean_text = re.sub(r'<[^>]+>', '', html)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        if max_length and len(clean_text) > max_length:
            clean_text = clean_text[:max_length] + '...'
        
        return clean_text


def extract_text_from_html(html: str, max_words: Optional[int] = None) -> str:
    """
    Extract readable text from HTML.
    
    Args:
        html: HTML string
        max_words: Optional maximum number of words
        
    Returns:
        Extracted text
    """
    if not html:
        return ""
    
    # Clean HTML and get text
    clean_text = clean_html_content(html)
    
    if max_words:
        words = clean_text.split()
        if len(words) > max_words:
            clean_text = ' '.join(words[:max_words]) + '...'
    
    return clean_text
