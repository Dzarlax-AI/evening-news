import logging
"""Smart Filtering service for reducing unnecessary AI requests."""

import re
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SmartFilter:
    """
    Smart filter to determine if articles need AI processing.
    Reduces unnecessary AI requests by filtering out low-quality or inappropriate content.
    """
    
    def __init__(self):
        # Content quality thresholds
        self.min_content_length = 10  # Minimum content length for AI processing (lowered for Telegram)
        self.max_content_length = 50000  # Maximum content length to avoid token limits
        self.min_title_length = 10  # Minimum title length
        
        # Language detection patterns (simple heuristic)
        self.cyrillic_ratio_threshold = 0.3  # Minimum cyrillic characters for Russian content
        
        # Spam/low-quality indicators
        self.spam_patterns = [
            r'\b(?:click here|кликни здесь|жми сюда)\b',
            r'\b(?:buy now|купи сейчас|заказать сейчас)\b', 
            r'\$\$\$+',  # Multiple dollar signs
            r'!!!{3,}',  # Multiple exclamation marks
            r'\b(?:free|бесплатно)\s+(?:download|скачать)\b',
            r'\b(?:limited time|ограниченное время)\b',
            r'\b(?:act now|действуй сейчас)\b',
        ]
        
        # Navigation/boilerplate patterns to exclude
        self.navigation_patterns = [
            r'^\s*(?:home|главная|news|новости|about|о нас|contact|контакты)\s*$',
            r'^\s*(?:menu|меню|navigation|навигация)\s*',
            r'^\s*(?:cookie|куки)\s+(?:policy|политика)',
            r'^\s*(?:privacy|конфиденциальность)\s+(?:policy|политика)',
            r'^\s*(?:terms|условия)\s+(?:of service|использования)',
            r'^\s*(?:404|error|ошибка)\s*',
        ]
        
        # Metadata/low-quality content patterns
        self.metadata_patterns = [
            r'\d+\s+min\s+read',  # "5 min read"
            r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{2}',  # "Sep 05"
            r'^\s*\w{3}\s+\d{2}\s+.*\d+\s+min\s+read\s*$',  # Typical metadata line
            r'\bsubscribe\s+(now|here|today)\b|\bподписаться\b',  # Subscribe prompts (more specific)
            r'\bshare\s+(this|on|now)\b|\bpoделиться\b',  # Share prompts (more specific, avoid "shareholder")
        ]
        
        # Duplicate detection (simple hash-based)
        self.recent_content_hashes = {}  # Hash -> timestamp
        self._content_hash_owners = {}  # Hash -> admitted article ID
        self.duplicate_detection_window = timedelta(hours=24)
        
    async def should_process_with_ai(self, title: str, content: str, url: str, 
                               source_type: str = 'rss', allow_extraction: bool = True,
                               db_session: Optional[Any] = None, check_duplicates: bool = True,
                               article_id: Optional[int] = None) -> Tuple[bool, str]:
        """
        Determine if article should be processed with AI.
        
        Args:
            title: Article title
            content: Article content
            url: Article URL
            source_type: Source type (rss, telegram, etc.)
            db_session: Database session for duplicate checking
            
        Returns:
            Tuple of (should_process, reason)
        """
        # Check content length
        if not self._check_content_length(content):
            return False, f"Content too short ({len(content)} chars < {self.min_content_length})"
            
        if len(content) > self.max_content_length:
            return False, f"Content too long ({len(content)} chars > {self.max_content_length})"
        
        # Check title quality
        if not self._check_title_quality(title):
            return False, f"Title too short or low quality ({len(title)} chars)"
        
        # Check for navigation/boilerplate content
        if self._is_navigation_content(title, content):
            return False, "Navigation/boilerplate content detected"
        
        # Check language (for Russian content focus)
        if not self._is_target_language(content):
            return False, "Content not in target language (Russian/English)"
        
        # Check for spam patterns
        if self._is_spam_content(title, content):
            return False, "Spam patterns detected"
        
        # Check for metadata/low-quality content
        if self._is_metadata_content(content):
            # If extraction is allowed and we have a valid URL, let it pass to trigger extraction
            if allow_extraction and url and url.startswith(('http://', 'https://')):
                skip_domains = ['t.me', 'telegram.me', 'twitter.com', 'x.com', 'instagram.com']
                if not any(domain in url.lower() for domain in skip_domains):
                    # Pass through for extraction, but mark as needing extraction
                    pass  # Continue with other checks instead of returning False
                else:
                    return False, "Metadata/low-quality content detected (non-extractable URL)"
            else:
                return False, "Metadata/low-quality content detected (no extraction possible)"
        
        # Check for duplicates
        if check_duplicates and await self._is_duplicate_content(content, db_session, article_id):
            return False, "Duplicate content detected"
        
        # Check for suspiciously short articles that might need extraction
        if allow_extraction and url and url.startswith(('http://', 'https://')):
            skip_domains = ['t.me', 'telegram.me', 'twitter.com', 'x.com', 'instagram.com']
            if not any(domain in url.lower() for domain in skip_domains):
                # Check if content is suspiciously short for a full article
                word_count = len(content.split()) if content else 0
                char_count = len(content.strip()) if content else 0
                
                # Flag for extraction if content is very short but has extractable URL
                # Typical RSS summaries are 20-200 characters, full articles are 500+ chars
                if char_count < 300 and word_count < 50:
                    # Additional check: if title is long/detailed but content is short, likely needs extraction
                    title_words = len(title.split()) if title else 0
                    if title_words >= 5 and char_count < 200:
                        return False, f"Content suspiciously short ({char_count} chars, {word_count} words) for detailed title (needs extraction)"
                    elif char_count < 150:
                        return False, f"Content very short ({char_count} chars, {word_count} words) - likely RSS summary (needs extraction)"
        
        # Check content quality score
        quality_score = self._calculate_quality_score(title, content, source_type)
        
        # Special case: if we detected metadata but have extractable URL, flag for extraction
        if self._is_metadata_content(content) and allow_extraction and url and url.startswith(('http://', 'https://')):
            skip_domains = ['t.me', 'telegram.me', 'twitter.com', 'x.com', 'instagram.com']
            if not any(domain in url.lower() for domain in skip_domains):
                return False, f"Metadata/low-quality content detected (needs extraction)"
        
        # Slightly more lenient threshold for extracted content or certain domains
        quality_threshold = 0.4
        if source_type == 'extraction':
            quality_threshold = 0.35  # Be more lenient with extracted content
            
        if quality_score < quality_threshold:
            return False, f"Content quality too low (score: {quality_score:.2f})"
        
        return True, f"Passed all filters (quality: {quality_score:.2f})"
    
    def _check_content_length(self, content: str) -> bool:
        """Check if content length is appropriate for AI processing."""
        if not content:
            return False
        clean_content = content.strip()
        return self.min_content_length <= len(clean_content) <= self.max_content_length
    
    def _check_title_quality(self, title: str) -> bool:
        """Check if title is meaningful and not too short."""
        if not title:
            return False
        clean_title = title.strip()
        return len(clean_title) >= self.min_title_length
    
    def _is_navigation_content(self, title: str, content: str) -> bool:
        """Check if content is navigation/boilerplate."""
        text = f"{title} {content}".lower()
        
        for pattern in self.navigation_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        return False
    
    def _is_target_language(self, content: str) -> bool:
        """Check if content is in target language (Russian/English)."""
        if not content:
            return False
        
        # Count cyrillic characters
        cyrillic_count = len(re.findall(r'[а-яёА-ЯЁ]', content))
        latin_count = len(re.findall(r'[a-zA-Z]', content))
        total_letters = cyrillic_count + latin_count
        
        if total_letters < 50:  # Too short to determine language
            return True  # Give benefit of doubt for short content
        
        cyrillic_ratio = cyrillic_count / total_letters if total_letters > 0 else 0
        
        # Accept if significant Russian content or mostly Latin (English)
        return cyrillic_ratio >= self.cyrillic_ratio_threshold or cyrillic_ratio <= 0.1
    
    def _is_spam_content(self, title: str, content: str) -> bool:
        """Check if content contains spam patterns."""
        text = f"{title} {content}".lower()
        
        for pattern in self.spam_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        return False
    
    def _is_metadata_content(self, content: str) -> bool:
        """Check if content is mostly metadata/boilerplate."""
        if not content:
            return False
        
        content_lower = content.lower().strip()
        
        # Check for metadata patterns
        for pattern in self.metadata_patterns:
            if re.search(pattern, content_lower, re.IGNORECASE):
                return True
        
        # Check if content is too short and repetitive
        if len(content.strip()) < 100:  # Very short content
            words = content.split()
            unique_words = set(word.lower() for word in words)
            if len(words) > 5 and len(unique_words) / len(words) < 0.6:  # Less than 60% unique words
                return True
        
        return False
    
    async def _is_duplicate_content(self, content: str, db_session: Optional[Any] = None,
                                    article_id: Optional[int] = None) -> bool:
        """Check if content is a duplicate of recently processed content."""
        if not content:
            return False

        try:
            # Generate hash (MD5 for DB compatibility and speed)
            import hashlib
            content_hash = hashlib.md5(content.strip().lower().encode('utf-8')).hexdigest()
            current_time = datetime.now()
            
            # 1. Check in-memory cache (Level 1 - Fastest)
            # Clean old entries first
            cutoff_time = current_time - self.duplicate_detection_window
            self.recent_content_hashes = {
                h: timestamp for h, timestamp in self.recent_content_hashes.items()
                if timestamp > cutoff_time
            }
            self._content_hash_owners = {
                key: owner for key, owner in self._content_hash_owners.items()
                if key in self.recent_content_hashes
            }
            
            if content_hash in self.recent_content_hashes:
                if article_id is not None and self._content_hash_owners.get(content_hash) == article_id:
                    return False
                last_seen = self.recent_content_hashes[content_hash]
                time_diff = current_time - last_seen
                seconds_ago = int(time_diff.total_seconds())
                logger.info(f"  🔍 Smart Filter: Content hash {content_hash} found in RAM cache ({seconds_ago}s ago)")
                return True
            
            # 2. Check database (Level 2 - Persistent)
            if db_session:
                from sqlalchemy import select
                from ..models import Article
                
                # Check if hash exists in DB
                # Note: We rely on the index we added to hash_content
                stmt = select(Article.id).where(Article.hash_content == content_hash).limit(1)
                if article_id is not None:
                    stmt = stmt.where(Article.id != article_id)
                result = await db_session.execute(stmt)
                if result.scalar_one_or_none():
                    logger.info(f"  🔍 Smart Filter: Content hash {content_hash} found in Database")
                    # Update RAM cache to save future DB hits
                    self.recent_content_hashes[content_hash] = current_time
                    return True
            
            # Not found: Add to RAM cache
            self.recent_content_hashes[content_hash] = current_time
            if article_id is not None:
                self._content_hash_owners[content_hash] = article_id
            return False
            
        except Exception as e:
            logger.warning(f"  ⚠️ Smart Filter: Duplicate check error: {e}")
            return False
    
    def _calculate_quality_score(self, title: str, content: str, source_type: str) -> float:
        """Calculate content quality score (0.0 - 1.0)."""
        score = 0.5  # Base score
        
        # Title quality
        if title and len(title.strip()) > 20:
            score += 0.1
        
        # Content structure
        if content:
            # Sentence count (good structure indicator)
            sentence_count = len(re.findall(r'[.!?]+', content))
            if 3 <= sentence_count <= 50:
                score += 0.15
            elif sentence_count > 50:
                score += 0.05  # Very long articles get less bonus
            
            # Paragraph structure
            paragraph_count = len([p for p in content.split('\\n\\n') if p.strip()])
            if paragraph_count >= 2:
                score += 0.1
            
            # Word count
            word_count = len(content.split())
            if 50 <= word_count <= 2000:
                score += 0.1
            elif word_count > 2000:
                score += 0.05  # Very long gets less bonus
        
        # Source type bonus
        if source_type == 'rss':
            score += 0.05  # RSS usually higher quality
        elif source_type == 'telegram':
            score += 0.02  # Telegram can be more varied
        elif source_type == 'extraction':
            score += 0.1   # Extra bonus for successful web extraction
        
        # Penalty for suspicious patterns
        if content and re.search(r'[A-Z]{10,}', content):  # Too much CAPS
            score -= 0.1
        
        if content and len(re.findall(r'[!]{2,}', content)) > 3:  # Too many exclamations
            score -= 0.1
        
        # Penalty for personal service patterns (likely ads)
        text = f"{title} {content}".lower()
        personal_patterns = [
            r"я\s+программист", r"мой\s+опыт", r"моя\s+компания", r"предлагаю\s+услуги",
            r"обращайтесь\s+по\s+телефону", r"консультаци[ия]\s+по\s+телефону",
            r"специализируюсь", r"оказываю\s+услуги", r"свяжитесь\s+со\s+мной"
        ]
        for pattern in personal_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                score -= 0.3  # Strong penalty for personal services
        
        return max(0.0, min(1.0, score))  # Clamp to 0-1 range
    
    def get_filter_stats(self) -> Dict[str, Any]:
        """Get statistics about filtering."""
        return {
            'recent_hashes_count': len(self.recent_content_hashes),
            'min_content_length': self.min_content_length,
            'max_content_length': self.max_content_length,
            'cyrillic_ratio_threshold': self.cyrillic_ratio_threshold,
            'spam_patterns_count': len(self.spam_patterns),
            'navigation_patterns_count': len(self.navigation_patterns)
        }


# Global smart filter instance
_smart_filter: Optional[SmartFilter] = None


def get_smart_filter() -> SmartFilter:
    """Get smart filter instance."""
    global _smart_filter
    
    if _smart_filter is None:
        _smart_filter = SmartFilter()
    
    return _smart_filter
