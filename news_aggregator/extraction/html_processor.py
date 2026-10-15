"""HTML processing and content extraction from DOM elements."""

import re
from ..core.resource_limits import ResourceLimitError
from typing import Optional, List
from bs4 import BeautifulSoup, Comment, NavigableString

from .extraction_utils import ExtractionUtils


class HTMLProcessor:
    """Process HTML content and extract meaningful text."""
    
    def __init__(self, utils: ExtractionUtils):
        self.utils = utils
    
    def extract_by_enhanced_selectors(self, soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
        """
        Extract content using enhanced CSS selectors.
        
        Returns:
            Tuple of (content, successful_selector)
        """
        # Priority selectors for main content
        content_selectors = [
            # Site-specific patterns (high priority)
            '.mb-14',  # N+1.ru main content
            
            # Schema.org microdata
            '[itemprop="articleBody"]',
            '[itemprop="text"]',
            
            # Common semantic HTML5 selectors
            'article', 'main',
            
            # Specific content classes (ordered by reliability)
            '.article-content', '.post-content', '.entry-content',
            '.content', '.main-content', '.page-content',
            '.story-content', '.news-content', '.blog-content',
            
            # ID-based selectors
            '#content', '#main-content', '#article-content',
            '#post-content', '#story', '#article',
            
            # Fallback selectors
            '.container .content', '.wrapper .content',
            'div[role="main"]', '[role="article"]'
        ]
        
        for selector in content_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    content = self._extract_text_from_element(element)
                    if content and self.utils.is_good_content(content):
                        return content, selector
            except ResourceLimitError:
                raise
            except Exception:
                continue
        
        return None, None
    
    def extract_by_enhanced_heuristics(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract content using heuristic analysis of DOM structure."""
        from ..core.resource_limits import enforce_text_limit
        # Find all text-containing elements
        candidates = []
        
        for element in soup.find_all(['p', 'div', 'article', 'section']):
            text = self._extract_text_from_element(element)
            if text and len(text) > 50:  # Minimum meaningful content
                try:
                    enforce_text_limit(text)
                except ResourceLimitError:
                    continue
                score = self._score_content_element(element, text)
                candidates.append((element, text, score))
                # Only the top five (top three for combination) are ever used.
                candidates.sort(key=lambda x: x[2], reverse=True)
                del candidates[5:]
        
        # Sort by score (highest first)
        candidates.sort(key=lambda x: x[2], reverse=True)
        
        # Try top candidates
        for element, text, score in candidates[:5]:
            if self.utils.is_good_content(text):
                return text
        
        # If no single element is good enough, try combining related elements
        if candidates:
            combined_text = self._combine_related_elements(candidates[:3])
            if combined_text and self.utils.is_good_content(combined_text):
                return combined_text
        
        return None
    
    def is_likely_content_element(self, element) -> bool:
        """Heuristically determine if element likely contains main content."""
        if not element or not hasattr(element, 'name'):
            return False
        
        # Positive indicators
        positive_classes = [
            'content', 'article', 'post', 'entry', 'story', 'news',
            'text', 'body', 'main', 'primary'
        ]
        
        # Negative indicators
        negative_classes = [
            'nav', 'navigation', 'menu', 'sidebar', 'aside', 'footer',
            'header', 'ad', 'advertisement', 'promo', 'related', 'comments',
            'social', 'share', 'widget', 'plugin'
        ]
        
        # Check element classes and IDs
        classes = element.get('class', [])
        if isinstance(classes, list):
            class_str = ' '.join(classes)
        else:
            class_str = str(classes) if classes else ''
        element_attrs = (class_str + ' ' + element.get('id', '')).lower()
        
        # Score based on positive/negative indicators
        score = 0
        
        for pos_class in positive_classes:
            if pos_class in element_attrs:
                score += 1
        
        for neg_class in negative_classes:
            if neg_class in element_attrs:
                score -= 2
        
        # Semantic HTML5 elements get bonus points
        if element.name in ['article', 'main', 'section']:
            score += 2
        
        # Navigation elements are usually not content
        if element.name in ['nav', 'aside', 'footer', 'header']:
            score -= 3
        
        return score > 0
    
    def remove_unwanted_elements(self, soup: BeautifulSoup) -> None:
        """Remove navigation, ads, and other unwanted elements."""
        # Elements to remove completely
        unwanted_tags = ['script', 'style', 'nav', 'aside', 'footer', 'header']
        for tag in unwanted_tags:
            for element in soup.find_all(tag):
                element.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Remove elements with unwanted classes/IDs
        unwanted_patterns = [
            'nav', 'navigation', 'menu', 'sidebar', 'aside',
            'ad', 'advertisement', 'promo', 'banner',
            'social', 'share', 'sharing', 'follow',
            'related', 'recommended', 'popular',
            'comments', 'comment', 'discussion',
            'widget', 'plugin', 'gadget'
        ]
        
        for element in soup.find_all():
            if not hasattr(element, 'get'):
                continue
                
            classes = element.get('class', [])
            element_id = element.get('id', '')
            
            # Convert to string for pattern matching
            element_text = ' '.join(classes) + ' ' + element_id
            element_text = element_text.lower()
            
            for pattern in unwanted_patterns:
                if pattern in element_text:
                    element.decompose()
                    break
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize extracted text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common unwanted standalone patterns (anchored to avoid removing mid-sentence words)
        unwanted_patterns = [
            r'(?<!\w)Advertisement(?!\w)\s*',
            r'(?<!\w)Sponsored(?!\w)\s*',
            r'(?<!\w)Click here(?!\w)\s*',
            r'(?<!\w)Read more(?!\w)\s*',
            r'(?<!\w)Subscribe(?!\w)\s*',
            r'(?<!\w)Follow us(?!\w)\s*',
            r'(?<!\w)Share(?!\w)\s*',
            r'(?<!\w)Tweet(?!\w)\s*',
            r'(?<!\w)Continue reading(?!\w)\s*'
        ]

        for pattern in unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # Clean up punctuation spacing
        text = re.sub(r'\s+([.,:;!?])', r'\1', text)
        
        return text.strip()
    
    def _extract_text_from_element(self, element) -> str:
        """Extract clean text from a DOM element."""
        if not element:
            return ""
        
        # Create a copy to avoid modifying original
        element_copy = element.__copy__()
        
        # Remove unwanted child elements
        for unwanted in element_copy.find_all(['script', 'style', 'nav', 'aside']):
            unwanted.decompose()
        
        text = element_copy.get_text(separator=' ', strip=True)
        return self.clean_text(text)
    
    def _score_content_element(self, element, text: str) -> float:
        """Score an element based on how likely it is to contain main content."""
        score = 0.0
        
        # Text length score
        text_length = len(text)
        if text_length > 500:
            score += 3.0
        elif text_length > 200:
            score += 2.0
        elif text_length > 100:
            score += 1.0
        
        # Element type score
        if element.name == 'article':
            score += 4.0
        elif element.name == 'main':
            score += 3.0
        elif element.name in ['section', 'div']:
            score += 1.0
        elif element.name == 'p':
            score += 0.5
        
        # Class/ID scoring
        classes = element.get('class', [])
        element_id = element.get('id', '')
        
        positive_indicators = [
            'content', 'article', 'post', 'entry', 'story', 'news', 'text', 'body'
        ]
        negative_indicators = [
            'nav', 'sidebar', 'footer', 'header', 'ad', 'comment', 'social'
        ]
        
        attr_text = ' '.join(classes + [element_id]).lower()
        
        for indicator in positive_indicators:
            if indicator in attr_text:
                score += 2.0
        
        for indicator in negative_indicators:
            if indicator in attr_text:
                score -= 3.0
        
        # Paragraph count (more paragraphs = more likely to be main content)
        paragraph_count = len(element.find_all('p'))
        score += min(paragraph_count * 0.5, 3.0)
        
        return max(0.0, score)
    
    def _combine_related_elements(self, candidates: List) -> str:
        """Combine text from related DOM elements."""
        combined_parts = []
        
        for element, text, score in candidates:
            if score > 1.0:  # Only include elements with decent scores
                combined_parts.append(text)
        
        return ' '.join(combined_parts)
