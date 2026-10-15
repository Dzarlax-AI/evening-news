import logging
"""Articles API router - handles individual article operations."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload
import sqlalchemy

from ..database import get_db
from ..models import Article, ArticleCategory
from ..orchestrator import NewsOrchestrator, reserve_processing_cycle, release_processing_cycle

logger = logging.getLogger(__name__)


router = APIRouter()


# ============================================================================
# Pydantic Models for Articles
# ============================================================================

class ReprocessRequest(BaseModel):
    force: bool = True  # Enable force processing by default for reprocessing
    reextract_media: bool = False


# ============================================================================
# Articles Endpoints
# ============================================================================

@router.post("/{article_id}/reprocess")
async def reprocess_article(
    article_id: int,
    request: ReprocessRequest,
    db: AsyncSession = Depends(get_db)
):
    """Reprocess a single article with AI."""
    reservation = reserve_processing_cycle()
    if reservation is None:
        raise HTTPException(status_code=409, detail="News processing is already running")
    try:
        # Get article
        result = await db.execute(
            select(Article)
            .options(selectinload(Article.source))
            .where(Article.id == article_id)
        )
        article = result.scalar_one_or_none()
        
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        
        # Convert to article data format
        article_data = {
            'id': article.id,
            'title': article.title,
            'content': article.content,
            'url': article.url,
            'summary': article.summary,
            'summary_processed': article.summary_processed,
            'category_processed': article.category_processed,
            'ad_processed': article.ad_processed,
            'processed': article.processed,
            'media_files': article.media_files,
            'image_url': article.image_url  # Include image_url for media caching
        }
        
        # Re-extract media if requested
        logger.info(f"🔍 Debug: reextract_media={request.reextract_media}, source={article.source}, source_type={article.source.source_type if article.source else 'None'}")
        if request.reextract_media and article.source and article.source.source_type == 'telegram':
            logger.info(f"🔧 Starting media re-extraction for article {article.id}")
            try:
                from ..services.source_manager import SourceManager
                source_manager = SourceManager()
                source_instance = await source_manager.get_source_instance(article.source)
                
                # Re-extract media using direct aiohttp
                import aiohttp
                import random
                from bs4 import BeautifulSoup
                
                try:
                    logger.info(f"🌐 Making HTTP request to {article.url}")
                    headers = random.choice(source_instance.BROWSER_HEADERS) if hasattr(source_instance, 'BROWSER_HEADERS') else {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                    }
                    logger.info(f"📡 HTTP headers: {headers}")
                    async with aiohttp.ClientSession() as session:
                        async with session.get(article.url, headers=headers, timeout=30, auto_decompress=False) as response:
                            if response.status == 200:
                                from ..core.resource_limits import read_page_text
                                html = await read_page_text(response)
                                soup = BeautifulSoup(html, 'html.parser')
                                
                                # Re-extract media using updated MediaExtractor
                                if hasattr(source_instance, 'media_extractor'):
                                    # Debug: check what divs are available
                                    all_divs = soup.find_all('div')
                                    logger.info(f"📋 Found {len(all_divs)} div elements")
                                    div_classes = [div.get('class', []) for div in all_divs[:10]]  # First 10
                                    logger.info(f"📋 First 10 div classes: {div_classes}")
                                    message_div = soup.find('div', class_=['tgme_widget_message', 'message', 'post'])
                                    if not message_div:
                                        # Try alternative selectors
                                        message_div = soup.find('div', class_='tgme_widget_message')
                                        if not message_div:
                                            message_div = soup.find('div', {'data-post': True})
                                            if not message_div:
                                                message_div = soup.find('body')  # Use body as fallback
                                                logger.info(f"🔄 Using body as fallback for article {article.id}")
                                    if message_div:
                                        new_media_files = source_instance.media_extractor.extract_media_files(message_div)
                                        if new_media_files:
                                            article_data['media_files'] = new_media_files
                                            # Update image_url to first image if available
                                            new_image_url = None
                                            for media in new_media_files:
                                                if media.get('type') == 'image':
                                                    new_image_url = media.get('url')
                                                    article_data['image_url'] = new_image_url
                                                    break
                                            
                                            # Save updated media_files to database
                                            try:
                                                update_stmt = update(Article).where(Article.id == article.id).values(
                                                    media_files=new_media_files,
                                                    image_url=new_image_url
                                                )
                                                await db.execute(update_stmt)
                                                await db.commit()
                                                logger.info(f"✅ Re-extracted {len(new_media_files)} media files and saved to DB for article {article.id}")
                                            except Exception as save_error:
                                                logger.warning(f"⚠️ Failed to save re-extracted media to DB: {save_error}")
                                        else:
                                            # No new media found, clear media_files if reextract was requested
                                            article_data['media_files'] = []
                                            article_data['image_url'] = None
                                            try:
                                                update_stmt = update(Article).where(Article.id == article.id).values(
                                                    media_files=[],
                                                    image_url=None
                                                )
                                                await db.execute(update_stmt)
                                                await db.commit()
                                                logger.info(f"🧹 No media files extracted on re-extraction, cleared media_files for article {article.id}")
                                            except Exception as save_error:
                                                logger.warning(f"⚠️ Failed to clear media_files in DB: {save_error}")
                                            logger.error(f"❌ No media files extracted on re-extraction for article {article.id}")
                                    else:
                                        logger.error(f"❌ No message div found for article {article.id}")
                                else:
                                    logger.error(f"❌ No media_extractor available for source {article.source.name}")
                            else:
                                logger.error(f"❌ HTTP {response.status} for article {article.id}")
                except Exception as http_error:
                    logger.error(f"❌ HTTP request failed for article {article.id}: {http_error}")
            except Exception as e:
                logger.error(f"❌ Media re-extraction failed for article {article.id}: {e}")
                # Continue with original media_files
        
        
        # Add source information  
        if article.source:
            article_data['source_type'] = article.source.source_type
            article_data['source_name'] = article.source.name
        else:
            article_data['source_type'] = 'rss'
            article_data['source_name'] = 'Unknown'
        
        # Release the request's read transaction before any AI work.
        await db.rollback()
        orchestrator = NewsOrchestrator()
        stats = await orchestrator.process_selected_articles(
            [article_id], reservation=reservation, force=request.force)
        status = stats.get('status', 'incomplete')
        outcome = next((item for item in stats.get('article_outcomes', [])
                        if item['article_id'] == article_id), {})
        reason = outcome.get('reason')
        if status == 'rejected':
            if reason == 'advertisement':
                message = 'Статья распознана как реклама.'
            elif reason == 'resource_limit':
                message = 'Страница превышает допустимый размер. Автоматический повтор не запланирован.'
            else:
                message = 'Статья исключена из обработки. Автоматический повтор не запланирован.'
        elif status == 'exhausted':
            message = 'Лимит попыток обработки исчерпан. Автоматический повтор остановлен.'
        elif stats.get('success'):
            message = 'Статья обновлена.'
        elif stats.get('retryable'):
            message = 'Обработка временно не завершена. Повторная попытка запланирована.'
        else:
            message = 'Обработка статьи не завершена.'
        return {
            "message": message,
            "article_id": article_id,
            "success": stats.get('success', False),
            "status": status,
            "settled": stats.get('settled', False),
            "retryable": stats.get('retryable', False),
            "reason": reason,
            "stats": stats,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.info(f"Article reprocessing failed: {e}")
        raise HTTPException(status_code=500, detail="Article reprocessing failed") from e
    finally:
        release_processing_cycle(reservation)


@router.get("/{article_id}")
async def get_article(
    article_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a single article with full details."""
    try:
        # Get article with all related data
        result = await db.execute(
            select(Article)
            .options(
                selectinload(Article.source),
                selectinload(Article.article_categories).selectinload(ArticleCategory.category)
            )
            .where(Article.id == article_id)
        )
        article = result.scalar_one_or_none()
        
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        
        # Get media files
        image_url = article.image_url
        media_files = article.media_files or []
        
        return {
            "id": article.id,
            "title": article.title,
            "summary": article.summary,
            "content": article.content,
            "url": article.url,
            "image_url": image_url,
            "media_files": media_files,
            "published_at": article.published_at.isoformat() if article.published_at else None,
            "fetched_at": article.fetched_at.isoformat() if article.fetched_at else None,
            "processed": article.processed,
            "processing_state": article.processing_state,
            "processing_attempts": article.processing_attempts,
            "next_attempt_at": article.next_attempt_at.isoformat() if article.next_attempt_at else None,
            "summary_processed": article.summary_processed,
            "category_processed": article.category_processed,
            "ad_processed": article.ad_processed,
            # Source information
            "source": {
                "id": article.source.id if article.source else None,
                "name": article.source.name if article.source else None,
                "type": article.source.source_type if article.source else None
            },
            # Categories
            "categories": article.categories_with_confidence,
            "primary_category": article.primary_category,
            # Advertising detection
            "is_advertisement": bool(getattr(article, 'is_advertisement', False)),
            "ad_confidence": float(getattr(article, 'ad_confidence', 0.0)),
            "ad_type": getattr(article, 'ad_type', None),
            "ad_reasoning": getattr(article, 'ad_reasoning', None),
            "ad_markers": getattr(article, 'ad_markers', [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.info(f"Error getting article: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get article: {str(e)}")
