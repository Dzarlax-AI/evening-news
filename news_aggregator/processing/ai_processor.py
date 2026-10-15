import logging
"""AI Processing Engine for articles."""

import time
import math
from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from ..services.ai_client import get_ai_client

logger = logging.getLogger(__name__)


def _category_confidence(value, fallback):
    """Keep finite numeric confidence, clamped like the legacy combined path."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return fallback
    try:
        value = float(value)
    except (ValueError, OverflowError):
        return fallback
    return max(0.0, min(1.0, value)) if math.isfinite(value) else fallback


def normalize_analysis_categories(analysis):
    """Preserve aligned confidences and original labels before filtering entries."""
    categories = analysis.get('categories')
    if not isinstance(categories, list):
        return []
    confidences = analysis.get('category_confidences')
    if not isinstance(confidences, list):
        confidences = [confidences]
    originals = analysis.get('original_categories')
    if not isinstance(originals, list):
        originals = []
    default_confidence = _category_confidence(analysis.get('category_confidence'), 0.8)
    normalized = []
    for index, category in enumerate(categories):
        data = category if isinstance(category, dict) else {}
        name = data.get('name') if data else category
        if not isinstance(name, str) or not name.strip():
            continue
        name = name.strip()
        aligned_confidence = confidences[index] if index < len(confidences) else None
        confidence = _category_confidence(data.get('confidence'),
            _category_confidence(aligned_confidence, default_confidence))
        original = data.get('ai_category')
        if not isinstance(original, str) or not original.strip():
            original = originals[index] if index < len(originals) else None
        if not isinstance(original, str) or not original.strip():
            original = name
        normalized.append({**data, 'name': name, 'confidence': confidence,
                           'ai_category': original.strip()})
    return normalized


class AIProcessor:
    """AI processing engine for articles."""
    
    def __init__(self):
        """Initialize AI processor with required services."""
        self.ai_client = get_ai_client()
    
    async def process_article_combined(
        self, 
        article_data: Dict[str, Any], 
        stats: Dict[str, Any], 
        force_processing: bool = False,
        db: AsyncSession = None
    ) -> Dict[str, Any]:
        """Process article with combined AI analysis - all tasks in one API call."""
        source_type = article_data.get('source_type', 'rss')
        source_name = article_data.get('source_name', 'Unknown')
        article_id = article_data['id']
        
        logger.info(f"  📡 Source: {source_name} (type: {source_type})")
        logger.info(f"  🔧 DEBUG: force_processing = {force_processing}")
        # Check what processing is needed
        needs_summary = force_processing or (not article_data.get('summary_processed', False) and not article_data.get('summary'))
        needs_category = force_processing or not article_data.get('category_processed', False)
        needs_ad_detection = force_processing or not article_data.get('ad_processed', False)
        
        # Smart Filtering: Check if article needs AI processing at all
        if needs_summary or needs_category or needs_ad_detection:
            from ..services.smart_filter import get_smart_filter
            smart_filter = get_smart_filter()
            
            article_content = article_data.get('content') or ''
            article_url = article_data.get('url') or ''
            
            if force_processing:
                should_process = True
                filter_reason = "Force processing enabled (reprocessing mode)"
                logger.info(f"  🔄 Smart Filter: Bypassed for forced reprocessing")
            else:
                should_process, filter_reason = await smart_filter.should_process_with_ai(
                    title=article_data.get('title') or '',
                    content=article_content,
                    url=article_url,
                    source_type=source_type,
                    db_session=db
                )
            
            # If content is too short or empty, try to extract full content from URL
            content_too_short = (len(article_content.strip()) < 10 and source_type == 'rss') or len(article_content.strip()) < 10
            logger.info(f"  🔍 Debug: content_too_short={content_too_short}, should_process={should_process}, filter_reason='{filter_reason}'")
            # Try content extraction if content is short OR metadata detected OR needs extraction OR force_processing
            should_extract = (
                force_processing or  # Always extract content during reprocessing
                (not should_process and ("Content too short" in filter_reason)) or 
                (not should_process and ("Metadata/low-quality content detected" in filter_reason)) or
                (not should_process and ("needs extraction" in filter_reason)) or
                content_too_short
            )
            
            if should_extract:
                if article_url and article_url.startswith(('http://', 'https://')):
                    # Skip URLs that are known to not have extractable content
                    skip_domains = [
                        't.me', 'telegram.me', 'twitter.com', 'x.com', 'instagram.com',
                        # Non-article URLs (maps, app stores, etc.)
                        'maps.app.goo.gl', 'maps.google.com', 'maps.google.ru',
                        '2gis.ru', '2gis.com', 'waze.com',
                        'play.google.com', 'apps.apple.com',
                    ]
                    if not any(domain in article_url.lower() for domain in skip_domains):
                        try:
                            if "Metadata/low-quality content detected" in filter_reason or "needs extraction" in filter_reason:
                                logger.info(f"  🔧 Metadata detected ({len(article_content)} chars), trying full content extraction: {article_url}")
                            else:
                                logger.info(f"  🔍 Content empty/short ({len(article_content)} chars), trying content extraction: {article_url}")
                            # Use full content extraction pipeline with all parsing schemas
                            from ..extraction import ContentExtractor
                            
                            # Try AI-enhanced extraction with metadata first
                            extracted_content = None
                            async with ContentExtractor() as content_extractor:
                                try:
                                    extraction_result = await content_extractor.extract_article_content_with_metadata(article_url, retry_count=4)
                                    extracted_content = extraction_result.get('content')
                                except Exception as e:
                                    logger.warning(f"  ⚠️ AI-enhanced extraction failed after retries, trying standard extraction: {e}")
                                    # Fallback to standard content extraction
                                    try:
                                        extracted_content = await content_extractor.extract_article_content(article_url, retry_count=3)
                                    except Exception as e2:
                                        logger.error(f"  ❌ Standard extraction also failed after retries: {e2}")
                                        extracted_content = None
                            
                            # Check if extracted content is meaningful
                            if extracted_content and len(extracted_content.strip()) > len(article_content):
                                logger.info(f"  ✅ Extracted {len(extracted_content)} chars from external URL using parsing schemas")
                                # Update article with extracted content
                                article_data['content'] = extracted_content
                                update_fields = {'content': extracted_content}

                                # Also save image_url if article has none and extraction found one
                                if not article_data.get('image_url') and isinstance(extraction_result, dict):
                                    extracted_image = extraction_result.get('image_url')
                                    if extracted_image:
                                        article_data['image_url'] = extracted_image
                                        update_fields['image_url'] = extracted_image
                                        logger.info(f"  🖼️ Found primary image via extraction: {extracted_image[:80]}")

                                await self._save_article_fields(article_id, update_fields)
                                
                                # Re-check smart filter with new content (unless force_processing is enabled)
                                if force_processing:
                                    should_process = True
                                    filter_reason = "Force processing enabled (after content extraction)"
                                    logger.info(f"  🔄 Smart Filter: Bypassed after content extraction (forced reprocessing)")
                                else:
                                    should_process, filter_reason = await smart_filter.should_process_with_ai(
                                        title=article_data.get('title') or '',
                                        content=extracted_content,
                                        url=article_url,
                                        source_type=source_type,
                                        db_session=db
                                    )
                            else:
                                logger.warning(f"  ⚠️ Could not extract meaningful content from external URL")
                        except Exception as e:
                            logger.error(f"  ❌ Failed to extract content from external URL: {e}")
            if not should_process:
                logger.info(f"  🚫 Smart Filter: Skipping AI processing - {filter_reason}")
                # Mark as processed with fallback values to avoid reprocessing
                update_fields = {}
                if needs_summary:
                    # Better fallback for summary: try RSS description/content if available
                    fallback_summary = article_data.get('title', 'No summary available')
                    rss_content = article_data.get('raw_description') or article_data.get('description') or article_data.get('content')
                    
                    if rss_content and len(rss_content.strip()) > len(fallback_summary) * 1.5:
                        # If RSS content is significantly longer than title, use it as fallback
                        from ..extraction.extraction_utils import ExtractionUtils
                        utils = ExtractionUtils()
                        # Use first 500 chars as a mini-summary if extraction failed/skipped
                        fallback_summary = utils.smart_truncate(rss_content, 500)
                        logger.info(f"  📝 Using RSS description as fallback summary ({len(fallback_summary)} chars)")
                    else:
                        logger.info(f"  📝 Using title as fallback summary (no better RSS content found)")
                    update_fields['summary'] = fallback_summary
                    update_fields['summary_processed'] = True
                if needs_category:
                    update_fields['category_processed'] = True
                if needs_ad_detection:
                    update_fields['is_advertisement'] = False
                    update_fields['ad_confidence'] = 0.0
                    update_fields['ad_type'] = 'news_article'
                    update_fields['ad_reasoning'] = f'Smart Filter: {filter_reason}'
                    update_fields['ad_processed'] = True
                
                # Mark as processed even when skipped by Smart Filter
                update_fields['processed'] = True
                
                # Extract media files even when Smart Filter skips AI processing
                logger.info(f"  🖼️ Extracting media files (Smart Filter skip)...")
                from .media_extractor import get_media_extractor
                
                try:
                    # Check if media files already exist in article_data (e.g., from Telegram sources)
                    existing_media = article_data.get('media_files', [])
                    extracted_media = []
                    
                    if existing_media and isinstance(existing_media, list) and len(existing_media) > 0:
                        logger.info(f"  📎 Using existing media files from article data: {len(existing_media)} files")
                        extracted_media = existing_media
                    else:
                        # Extract media files from HTML content
                        logger.info(f"  🔍 Extracting media files from HTML content...")
                        media_extractor = get_media_extractor()
                        extracted_media = media_extractor.extract_media_files(
                            article_data.get('content', ''), 
                            base_url=article_data.get('url')
                        )
                    
                    if extracted_media:
                        # Add extracted media to update fields and article data
                        article_data['media_files'] = extracted_media
                        update_fields['media_files'] = extracted_media
                        
                        # Get media extractor for summary (if not already created)
                        if 'media_extractor' not in locals():
                            media_extractor = get_media_extractor()
                            
                        summary = media_extractor.get_media_summary(extracted_media)
                        logger.info(f"  📎 Found {summary['total']} media files (Smart Filter skip)")
                        # Save media files to database
                        await self._save_article_fields(article_id, {'media_files': extracted_media})
                    else:
                        logger.info(f"  📭 No media files found in content (Smart Filter skip)")
                except Exception as media_error:
                    logger.error(f"  ❌ Media extraction failed (Smart Filter skip): {media_error}")
                    # Don't raise - media processing is optional
                
                # Save fallback results
                await self._save_article_fields(article_id, update_fields)
                stats.setdefault('smart_filter_skipped', 0)
                stats['smart_filter_skipped'] += 1
                
                return {**article_data, **update_fields}
            else:
                logger.info(f"  ✅ Smart Filter: Approved for AI processing - {filter_reason}")
                stats.setdefault('smart_filter_approved', 0)
                stats['smart_filter_approved'] += 1
        
        # If all processing is already done, skip
        if not (needs_summary or needs_category or needs_ad_detection):
            logger.info(f"  ✅ All processing already completed")
            # Ensure processed flag is set (safety check)
            if not article_data.get('processed', False):
                await self._save_article_fields(article_id, {'processed': True})
                logger.info(f"  🔧 Safety: Set processed=True for fully processed article")
            return article_data
            
        logger.error(f"  🔍 Processing needs: {'✅ Summary' if needs_summary else '❌ Summary'}, {'✅ Category' if needs_category else '❌ Category'}, {'✅ Ad Detection' if needs_ad_detection else '❌ Ad Detection'}")
        # Use combined AI analysis if we need multiple tasks
        if sum([needs_summary, needs_category, needs_ad_detection]) >= 2:
            return await self._process_with_combined_ai(
                article_id, article_data, stats, 
                needs_summary, needs_category, needs_ad_detection, db
            )
        else:
            # Use incremental processing for single tasks
            return await self.process_article_incremental(article_data, stats, force_processing)

    async def _process_with_combined_ai(
        self,
        article_id: int,
        article_data: Dict[str, Any],
        stats: Dict[str, Any],
        needs_summary: bool,
        needs_category: bool,
        needs_ad_detection: bool,
        db: AsyncSession = None
    ) -> Dict[str, Any]:
        """Process article with combined AI analysis."""
        logger.info(f"  🧠 Using combined AI analysis for efficiency...")
        try:
            # Check if we need to extract content first
            article_content = article_data.get('content') or ''
            article_url = article_data.get('url') or ''
            source_type = article_data.get('source_type', 'rss')
            
            content_too_short = (len(article_content.strip()) < 10 and source_type == 'rss') or len(article_content.strip()) < 10
            logger.info(f"  🔍 Debug combined: content_too_short={content_too_short}, content_len={len(article_content.strip())}")
            if content_too_short and article_url and article_url.startswith(('http://', 'https://')):
                # Skip URLs that are known to not have extractable content
                skip_domains = [
                    't.me', 'telegram.me', 'twitter.com', 'x.com', 'instagram.com',
                    # Non-article URLs (maps, app stores, etc.)
                    'maps.app.goo.gl', 'maps.google.com', 'maps.google.ru',
                    '2gis.ru', '2gis.com', 'waze.com',
                    'play.google.com', 'apps.apple.com',
                ]
                if not any(domain in article_url.lower() for domain in skip_domains):
                    try:
                        logger.info(f"  🔍 Content empty/short ({len(article_content)} chars), trying content extraction: {article_url}")
                        # Use full content extraction pipeline with all parsing schemas
                        from ..extraction import ContentExtractor
                        
                        # Try AI-enhanced extraction with metadata first
                        extracted_content = None
                        async with ContentExtractor() as content_extractor:
                            try:
                                extraction_result = await content_extractor.extract_article_content_with_metadata(article_url, retry_count=4)
                                extracted_content = extraction_result.get('content')
                            except Exception as e:
                                logger.warning(f"  ⚠️ AI-enhanced extraction failed after retries, trying standard extraction: {e}")
                                # Fallback to standard content extraction
                                try:
                                    extracted_content = await content_extractor.extract_article_content(article_url, retry_count=3)
                                except Exception as e2:
                                    logger.error(f"  ❌ Standard extraction also failed after retries: {e2}")
                                    extracted_content = None
                        
                        # Check if extracted content is meaningful
                        if extracted_content and len(extracted_content.strip()) > len(article_content):
                            logger.info(f"  ✅ Extracted {len(extracted_content)} chars from external URL using parsing schemas")
                            # Update article with extracted content
                            article_data['content'] = extracted_content
                            update_fields = {'content': extracted_content}
                            await self._save_article_fields(article_id, update_fields)
                            
                        else:
                            logger.warning(f"  ⚠️ Could not extract meaningful content from external URL")
                    except Exception as e:
                        logger.error(f"  ❌ Failed to extract content from external URL: {e}")
            start_time = time.time()

            # Validate article content before AI processing
            article_content = article_data.get('content') or ''
            article_title = article_data.get('title') or ''

            # Skip AI processing if content is too short
            if len(article_content.strip()) < 50 and len(article_title.strip()) < 20:
                logger.warning(f"  ⚠️ Content too short for AI processing (content: {len(article_content)} chars, title: {len(article_title)} chars)")
                # Use fallback values
                update_fields = {
                    'summary': article_title or 'No summary available',
                    'summary_processed': True,
                    'category_processed': True,
                    'is_advertisement': False,
                    'ad_confidence': 0.0,
                    'ad_type': 'news_article',
                    'ad_reasoning': 'Content too short for AI processing',
                    'ad_processed': True,
                    'processed': True
                }
                await self._save_article_fields(article_id, update_fields)
                return {**article_data, **update_fields, 'success': True, 'skipped_reason': 'content_too_short'}

            # For Telegram articles with non-article URLs (maps, app stores),
            # the URL content is unreliable — use article content as-is
            article_url = article_data.get('url') or ''
            original_context = None

            # If this is a Telegram post with an external (non-Telegram) URL,
            # pass the Telegram content as original_context so AI can fall back to it
            if source_type == 'telegram' and article_url:
                from urllib.parse import urlparse
                host = urlparse(article_url).netloc.lower()
                if host and 't.me' not in host and 'telegram.me' not in host:
                    original_context = article_content if len(article_content.strip()) >= 30 else None

            # Get combined analysis
            ai_result = await self.ai_client.analyze_article_complete(
                title=article_title,
                content=article_content,
                url=article_url,
                original_context=original_context,
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"  ✅ Combined analysis completed in {elapsed_time:.1f}s")
            stats['api_calls_made'] += 1
            
            # Save all results at once
            update_fields = {}
            if needs_summary and ai_result.get('summary'):
                update_fields['summary'] = ai_result['summary']
                update_fields['summary_processed'] = True

            # Update title if AI provided optimized version
            optimized_title_raw = ai_result.get('optimized_title')
            current_title = article_data.get('title', '')
            logger.info(f"  🔍 Title check: AI returned optimized_title='{optimized_title_raw}'")
            if optimized_title_raw:
                optimized_title = str(optimized_title_raw).strip()
                logger.info(f"  🔍 Cleaned optimized title: '{optimized_title}' (len={len(optimized_title)})")
                if optimized_title and len(optimized_title) <= 200:  # Reasonable title length limit
                    if optimized_title != current_title:
                        update_fields['title'] = optimized_title
                        logger.info(f"  📝 Title optimized: {optimized_title[:60]}...")
                    else:
                        logger.info(f"  ✅ Title unchanged: AI returned same title")
                else:
                    logger.warning(f"  ⚠️ Title optimization skipped: empty or too long")
            else:
                logger.error(f"  ❌ AI did not return optimized_title")
            if needs_category:
                # Extract categories from AI response (new format: array or fallback to single)
                categories_result = ai_result.get('categories', ai_result.get('category', ['Other']))
                if isinstance(categories_result, str):
                    categories_result = [categories_result]  # Convert single string to array
                elif not isinstance(categories_result, list):
                    categories_result = ['Other']  # Fallback for invalid format
                
                # Extract original AI categories (before mapping)
                original_categories = ai_result.get('original_categories', categories_result)
                
                # Extract category confidences (new format: array matching categories)
                confidences_result = ai_result.get('category_confidences', ai_result.get('category_confidence', [1.0]))
                if isinstance(confidences_result, (int, float)):
                    confidences_result = [float(confidences_result)]  # Convert single number to array
                elif not isinstance(confidences_result, list):
                    confidences_result = [1.0]  # Fallback for invalid format
                
                # Ensure arrays have same length
                while len(confidences_result) < len(categories_result):
                    confidences_result.append(0.8)  # Default confidence for extra categories
                confidences_result = confidences_result[:len(categories_result)]  # Trim if too long
                
                # Handle multiple categories with confidences
                try:
                    from ..services.category_service import get_category_service
                    if db:
                        category_service = await get_category_service(db)
                        
                        # Build categories with confidence data
                        categories_with_confidence = []
                        for i, category_name in enumerate(categories_result):
                            confidence = confidences_result[i] if i < len(confidences_result) else 0.8
                            # Use original AI category from original_categories array
                            ai_category = original_categories[i] if i < len(original_categories) else category_name
                            categories_with_confidence.append({
                                'name': category_name,
                                'confidence': max(0.0, min(1.0, float(confidence))),  # Clamp to 0-1 range
                                'ai_category': ai_category  # Store ACTUAL original AI category
                            })
                        
                        assigned_categories = await category_service.assign_categories_with_confidences(
                            article_id=article_id,
                            categories_with_confidence=categories_with_confidence
                        )
                        update_fields['category_processed'] = True
                        
                        if assigned_categories:
                            categories_info = [f"{c['display_name']} ({c['confidence']:.2f})" for c in assigned_categories]
                            logger.info(f"  🏷️ Multiple categories assigned: {', '.join(categories_info)}")
                    else:
                        from ..services.database_queue import get_db_queue_manager

                        # Build categories with confidence data
                        categories_with_confidence = []
                        for i, category_name in enumerate(categories_result):
                            confidence = confidences_result[i] if i < len(confidences_result) else 0.8
                            # Use original AI category from original_categories array
                            ai_category = original_categories[i] if i < len(original_categories) else category_name
                            categories_with_confidence.append({
                                'name': category_name,
                                'confidence': max(0.0, min(1.0, float(confidence))),  # Clamp to 0-1 range
                                'ai_category': ai_category  # Store ACTUAL original AI category
                            })

                        async def _assign_categories(category_db):
                            category_service = await get_category_service(category_db)
                            return await category_service.assign_categories_with_confidences(
                                article_id=article_id,
                                categories_with_confidence=categories_with_confidence
                            )

                        assigned_categories = await get_db_queue_manager().execute_write(_assign_categories)
                        update_fields['category_processed'] = True

                        if assigned_categories:
                            categories_info = [f"{c['display_name']} ({c['confidence']:.2f})" for c in assigned_categories]
                            logger.info(f"  🏷️ Multiple categories assigned: {', '.join(categories_info)}")
                except Exception as e:
                    logger.warning(f"  ⚠️ Multiple categories assignment failed: {e}")
                    # DO NOT set category_processed = True here - let it retry
                
            if needs_ad_detection:
                update_fields['is_advertisement'] = ai_result.get('is_advertisement', False)
                update_fields['ad_confidence'] = ai_result.get('ad_confidence', 0.0)
                update_fields['ad_type'] = ai_result.get('ad_type', 'news_article')
                update_fields['ad_reasoning'] = ai_result.get('ad_reasoning', 'Combined analysis')
                update_fields['ad_processed'] = True
                
            # Mark article as fully processed after successful AI processing
            update_fields['processed'] = True
                
            # Save all fields in one database operation
            await self._save_article_fields(article_id, update_fields)
            
            logger.info(f"  💾 All results saved to database")
            logger.info(f"  📊 Summary: {ai_result.get('summary', 'None')[:100]}...")
            # Display categories with confidences (support both old and new format)
            categories_display = ai_result.get('categories', ai_result.get('category', ['Other']))
            if isinstance(categories_display, str):
                categories_display = [categories_display]
            elif not isinstance(categories_display, list) or categories_display is None:
                categories_display = ['Other']  # Fallback if categories are None
            
            confidences_display = ai_result.get('category_confidences', ai_result.get('category_confidence', [1.0]))
            if isinstance(confidences_display, (int, float)):
                confidences_display = [float(confidences_display)]
            elif not isinstance(confidences_display, list) or confidences_display is None:
                confidences_display = [1.0] * len(categories_display)  # Default confidence for all categories
            
            # Format categories with confidences
            if len(confidences_display) >= len(categories_display):
                categories_info = [f"{cat} ({conf:.2f})" for cat, conf in zip(categories_display, confidences_display)]
                logger.info(f"  🏷️ Categories: {', '.join(categories_info)}")
            else:
                logger.info(f"  🏷️ Categories: {', '.join(categories_display)}")
            logger.info(f"  🚨 Advertisement: {ai_result.get('is_advertisement', False)} (confidence: {ai_result.get('ad_confidence', 0.0):.2f})")
            # Extract media files from article content
            logger.info(f"  🖼️ Extracting media files from content...")
            from .media_extractor import get_media_extractor
            
            try:
                # Check if media files already exist in article_data (e.g., from Telegram sources)
                existing_media = article_data.get('media_files', [])
                extracted_media = []
                
                if existing_media and isinstance(existing_media, list) and len(existing_media) > 0:
                    logger.info(f"  📎 Using existing media files from article data: {len(existing_media)} files")
                    extracted_media = existing_media
                else:
                    # Extract media files from HTML content
                    logger.info(f"  🔍 Extracting media files from HTML content...")
                    media_extractor = get_media_extractor()
                    extracted_media = media_extractor.extract_media_files(
                        article_data.get('content', ''), 
                        base_url=article_data.get('url')
                    )
                
                if extracted_media:
                    # Add extracted media to article data and update fields
                    article_data['media_files'] = extracted_media
                    update_fields['media_files'] = extracted_media
                    
                    # Get media extractor for summary (if not already created)
                    if 'media_extractor' not in locals():
                        media_extractor = get_media_extractor()
                        
                    summary = media_extractor.get_media_summary(extracted_media)
                    logger.info(f"  📎 Found {summary['total']} media files: {summary['image']} images, {summary['video']} videos, {summary['document']} documents")
                    # Save media files to database
                    await self._save_article_fields(article_id, {'media_files': extracted_media})
                else:
                    logger.info(f"  📭 No media files found in content")
            except Exception as media_error:
                logger.error(f"  ❌ Media extraction failed: {media_error}")
                # Don't raise - media processing is optional
            
            # Note: processed flag already set in update_fields and saved above
            
            return {**article_data, **update_fields, 'success': True, 'content_length': len(article_data.get('content', ''))}
            
        except Exception as e:
            # Check if this is just a duplicate category error (normal during reprocessing)
            if "duplicate key value violates unique constraint" in str(e) and "article_categories" in str(e):
                logger.warning(f"  ⚠️ Combined analysis completed with duplicate category warning (normal during reprocessing)")
                # Extract media even with category duplication error
                logger.info(f"  🖼️ Extracting media files (after category error)...")
                from .media_extractor import get_media_extractor
                
                try:
                    # Extract media files from content
                    media_extractor = get_media_extractor()
                    extracted_media = media_extractor.extract_media_files(
                        article_data.get('content', ''), 
                        base_url=article_data.get('url')
                    )
                    
                    if extracted_media:
                        article_data['media_files'] = extracted_media
                        summary = media_extractor.get_media_summary(extracted_media)
                        logger.info(f"  📎 Found {summary['total']} media files")
                        # Save to database
                        await self._save_article_fields(article_id, {'media_files': extracted_media})
                    
                except Exception as media_error:
                    logger.error(f"  ❌ Media extraction failed: {media_error}")
                # Return success even with duplicate category error
                safe_update_fields = locals().get('update_fields', {})
                safe_update_fields['processed'] = True  # Mark as processed even with duplicate category warning
                return {**article_data, **safe_update_fields, 'success': True, 'content_length': len(article_data.get('content', ''))}
            else:
                logger.error(f"  ❌ Combined analysis failed: {e}")
                # Fall back to incremental processing
                return await self.process_article_incremental(article_data, stats, False)

    async def process_article_incremental(
        self,
        article_data: Dict[str, Any],
        stats: Dict[str, Any],
        force_processing: bool = False,
    ) -> Dict[str, Any]:
        """Route legacy callers through authoritative typed outcomes and retries."""
        from ..orchestrator import NewsOrchestrator
        orchestrator = NewsOrchestrator.for_article_processing(self)
        result = await orchestrator.process_selected_articles(
            [article_data['id']], force=force_processing)
        stats['api_calls_made'] = stats.get('api_calls_made', 0) + result.get('api_calls_made', 0)
        stats.setdefault('errors', []).extend(result.get('errors', []))
        return result

    async def process_article_outcome(self, article_data, stats, force_refresh=False):
        """Compute a typed outcome outside a database transaction.

        Successful stages survive a later failure. Missing AI output is never a
        successful summary, and source/title fallbacks do not hide API failures.
        """
        from types import SimpleNamespace
        from .processing_outcome import ProcessingOutcome, is_challenge_text
        from ..core.resource_limits import ResourceLimitError, enforce_text_limit

        fields = {}
        summary_done = bool(article_data.get('summary_processed') and article_data.get('summary'))
        category_done = bool(article_data.get('category_processed'))
        ad_done = bool(article_data.get('ad_processed'))
        if ad_done and article_data.get('is_advertisement') is True:
            return ProcessingOutcome('rejected', error='advertisement')
        if summary_done and category_done and ad_done:
            return ProcessingOutcome('completed')
        try:
            # Remaining classification stages use an already completed summary.
            # An unused archival body must not prevent processing that summary.
            analysis_input = article_data.get('summary') if summary_done else article_data.get('content')
            content = enforce_text_limit(analysis_input or '')
            refreshed_metadata = None
            if force_refresh:
                from urllib.parse import urlparse
                from ..extraction import ContentExtractor
                parsed = urlparse(article_data.get('url') or '')
                skip_hosts = ('t.me', 'telegram.me', 'twitter.com', 'x.com', 'instagram.com',
                              'maps.app.goo.gl', 'maps.google.com', 'maps.google.ru',
                              '2gis.ru', '2gis.com', 'waze.com', 'play.google.com', 'apps.apple.com')
                host = (parsed.hostname or '').lower()
                if parsed.scheme in {'http', 'https'} and not any(
                        host == domain or host.endswith('.' + domain) for domain in skip_hosts):
                    # This remains inside the persisted whole-pipeline attempt and
                    # its timeout. Extraction owns bounded HTTP/browser readers.
                    async with ContentExtractor() as extractor:
                        refreshed_metadata = await extractor.extract_article_content_with_metadata(
                            article_data['url'], retry_count=1)
                    refreshed_content = refreshed_metadata.get('content')
                    if not isinstance(refreshed_content, str) or not refreshed_content.strip():
                        return ProcessingOutcome('retry_wait', error='content_refresh_unavailable')
                    enforce_text_limit(refreshed_content)
                    if is_challenge_text(refreshed_content):
                        return ProcessingOutcome('retry_wait', error='extraction_challenge')
                    # Retain the previous improvement-only body/image semantics.
                    if len(refreshed_content.strip()) > len(content):
                        content = refreshed_content
                        fields['content'] = content
                        image = refreshed_metadata.get('image_url')
                        if not article_data.get('image_url') and isinstance(image, str) and image:
                            fields['image_url'] = image
            if is_challenge_text(content):
                return ProcessingOutcome('retry_wait', error='extraction_challenge')
            article = SimpleNamespace(**{**article_data, 'content': content})
            if refreshed_metadata is not None:
                self._update_article_publication_date(article, refreshed_metadata.get('publication_date'), 'forced refresh')
                if article.published_at != article_data.get('published_at'):
                    fields['published_at'] = article.published_at
                stats['api_calls_made'] = stats.get('api_calls_made', 0) + 1
                analysis = await self.ai_client.analyze_article_complete(
                    article.title, content, article.url)
            elif not summary_done:
                analysis = await self.get_summary_by_source_type(
                    article, article_data.get('source_type') or 'rss', stats)
            else:
                stats['api_calls_made'] = stats.get('api_calls_made', 0) + 1
                analysis = await self.ai_client.analyze_article_complete(
                    article.title, article.summary or content, article.url)
            if analysis.get('_processing_error'):
                return ProcessingOutcome('rejected' if analysis.get('_processing_terminal') else 'retry_wait', fields,
                    analysis['_processing_error'], analysis.get('_retry_after_seconds'))
            summary = analysis.get('summary')
            # A validated positive verdict is terminal even if optional summary
            # or category output is absent. Preserve any independently valid data.
            if analysis.get('is_advertisement') is True:
                fields.update(ad_processed=True, is_advertisement=True,
                              ad_confidence=analysis.get('ad_confidence', 0),
                              ad_type=analysis.get('ad_type'), ad_reasoning=analysis.get('ad_reasoning'))
                if not summary_done and isinstance(summary, str) and summary.strip() and not is_challenge_text(summary):
                    fields.update(summary=summary.strip(), summary_processed=True)
                    title = analysis.get('optimized_title')
                    if title and not is_challenge_text(title):
                        fields['title'] = title
                    if article.published_at != article_data.get('published_at'):
                        fields['published_at'] = article.published_at
                if not category_done:
                    normalized = normalize_analysis_categories(analysis)
                    if normalized:
                        fields.update(categories=normalized, category_processed=True)
                return ProcessingOutcome('rejected', fields, 'advertisement')
            if not summary_done:
                if not isinstance(summary, str) or not summary.strip():
                    return ProcessingOutcome('retry_wait', fields, error='missing_summary')
                if is_challenge_text(summary):
                    return ProcessingOutcome('retry_wait', fields, error='summary_describes_challenge')
                fields.update(summary=summary.strip(), summary_processed=True)
                title = analysis.get('optimized_title')
                if title and not is_challenge_text(title):
                    fields['title'] = title
                if article.published_at != article_data.get('published_at'):
                    fields['published_at'] = article.published_at
            if not category_done:
                normalized = normalize_analysis_categories(analysis)
                if not normalized:
                    return ProcessingOutcome('retry_wait', fields, 'missing_categories')
                fields.update(categories=normalized, category_processed=True)
            if not ad_done:
                if not isinstance(analysis.get('is_advertisement'), bool):
                    return ProcessingOutcome('retry_wait', fields, 'missing_ad_classification')
                fields.update(ad_processed=True, is_advertisement=analysis['is_advertisement'],
                              ad_confidence=analysis.get('ad_confidence', 0),
                              ad_type=analysis.get('ad_type'), ad_reasoning=analysis.get('ad_reasoning'))
            return ProcessingOutcome('completed', fields)
        except ResourceLimitError as exc:
            return ProcessingOutcome('rejected', fields, 'resource_limit')
        except Exception as exc:
            return ProcessingOutcome('retry_wait', fields,
                type(exc).__name__, getattr(exc, 'retry_after_seconds', None))

    async def get_summary_by_source_type(self, article, source_type: str, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Return explicit analysis; a failed summary remains failed."""
        from .processing_outcome import is_challenge_text
        from ..core.resource_limits import enforce_text_limit

        content = enforce_text_limit(article.content or '')
        analysis = {}
        if source_type in {'rss', 'custom'} or (source_type == 'telegram' and not
                any(host in article.url for host in ('t.me/', 'telegram.me/'))):
            stats['api_calls_made'] = stats.get('api_calls_made', 0) + 1
            analysis = await self.ai_client.get_article_summary_with_metadata(
                article.url, original_context=content or None)
            self._update_article_publication_date(article, analysis.get('publication_date'), source_type)
            if analysis.get('_processing_error'):
                return analysis
            if analysis.get('is_advertisement') is True:
                return analysis
        if (not analysis.get('summary') or is_challenge_text(analysis.get('summary'))) and len(content.strip()) >= 30:
            stats['api_calls_made'] = stats.get('api_calls_made', 0) + 1
            analysis = await self.ai_client.analyze_article_complete(
                title=article.title or '', content=content, url=article.url or '')
        return analysis

    def _update_article_publication_date(self, article, pub_date: str, source_type: str):
        """Update article publication date if provided."""
        if not pub_date:
            return
            
        try:
            import dateutil.parser as date_parser
            parsed_date = date_parser.parse(pub_date, fuzzy=True)
            if parsed_date and (not article.published_at or article.published_at.year < 2020):
                # Remove timezone info to match DateTime field in database
                article.published_at = parsed_date.replace(tzinfo=None) if parsed_date.tzinfo else parsed_date
                logger.info(f"  📅 Updated publication date from {source_type}: {parsed_date.strftime('%Y-%m-%d %H:%M')}")
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to parse publication date '{pub_date}': {e}")
    async def _categorize_by_source_type_new(self, article, source_type: str, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Categorize article and return categories with confidences."""
        # Implementation would go here - simplified for now
        return []

    async def _save_article_fields(self, article_id: int, fields_dict: Dict[str, Any], db: AsyncSession = None):
        """Save specific fields to article in database."""
        try:
            if db:
                # Use provided session
                from ..models import Article
                from sqlalchemy import select
                
                result = await db.execute(select(Article).where(Article.id == article_id))
                article = result.scalar_one_or_none()
                
                if not article:
                    logger.warning(f"⚠️ Article {article_id} not found for fields update")
                    return
                
                # Set all fields from dictionary
                for field_name, field_value in fields_dict.items():
                    if hasattr(article, field_name):
                        setattr(article, field_name, field_value)
                    else:
                        logger.warning(f"⚠️ Article has no field '{field_name}'")
                await db.commit()
            else:
                from ..services.database_queue import get_db_queue_manager
                from ..models import Article
                from sqlalchemy import select

                async def _update_fields(new_db):
                    result = await new_db.execute(select(Article).where(Article.id == article_id))
                    article = result.scalar_one_or_none()
                    if not article:
                        logger.warning(f"⚠️ Article {article_id} not found for fields update")
                        return
                    for field_name, field_value in fields_dict.items():
                        if hasattr(article, field_name):
                            setattr(article, field_name, field_value)
                        else:
                            logger.warning(f"⚠️ Article has no field '{field_name}'")

                await get_db_queue_manager().execute_write(_update_fields)
                    
        except Exception as e:
            logger.error(f"  ❌ Failed to save article fields for {article_id}: {e}")
            raise


