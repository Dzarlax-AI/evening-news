"""Unified data service layer for database operations.

Централизованный доступ к данным для всех endpoints.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func, text
from sqlalchemy.orm import selectinload
from datetime import datetime, timedelta

from ..models import Article, Source, Category, ArticleCategory
from ..database import get_db
from fastapi import Depends
from .publication import publication_filter


class DataService:
    """Unified service for all database operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_articles_feed(
        self,
        limit: int = 20,
        offset: int = 0,
        since_hours: Optional[int] = None,
        category: Optional[str] = None,
        source: Optional[str] = None,
        hide_ads: bool = True
    ) -> List[Dict[str, Any]]:
        """Get articles feed with filters."""

        # Build base query
        query = select(Article).options(
            selectinload(Article.source),
            selectinload(Article.article_categories).selectinload(ArticleCategory.category)
        )

        advertisements_view = bool(category and category.lower() == 'advertisements')
        query = query.where(publication_filter(
            include_classified_ads=(not hide_ads or advertisements_view)
        ))

        # Apply filters
        if since_hours:
            since_time = datetime.utcnow() - timedelta(hours=since_hours)
            query = query.where(Article.published_at >= since_time)

        if hide_ads and not advertisements_view:
            query = query.where(Article.is_advertisement.is_(False))

        if category and category.lower() != 'all':
            if category.lower() == 'advertisements':
                query = query.where(Article.is_advertisement.is_(True))
            else:
                query = query.join(ArticleCategory).join(Category).where(
                    func.lower(Category.name) == category.lower()
                )

        # Apply source filter
        if source and source.lower() != 'all':
            # Support multiple sources (comma-separated)
            sources = [s.strip() for s in source.split(',') if s.strip()]
            if sources:
                # Try matching by source ID or name
                try:
                    # If it's a number, filter by ID
                    source_ids = [int(s) for s in sources if s.isdigit()]
                    if source_ids:
                        query = query.where(Article.source_id.in_(source_ids))
                    else:
                        # Filter by source name
                        query = query.join(Source).where(Source.name.in_(sources))
                except ValueError:
                    # Filter by source name
                    query = query.join(Source).where(Source.name.in_(sources))

        # Order and paginate
        query = query.order_by(desc(Article.published_at)).offset(offset).limit(limit)
        
        # Execute
        result = await self.db.execute(query)
        articles = result.scalars().all()
        
        # Convert to dict format
        return [self._article_to_dict(article) for article in articles]
    
    async def get_article_by_id(self, article_id: int, *, include_unpublished: bool = False) -> Optional[Dict[str, Any]]:
        """Get single article by ID."""
        query = select(Article).options(
            selectinload(Article.source),
            selectinload(Article.article_categories).selectinload(ArticleCategory.category)
        ).where(Article.id == article_id)
        if not include_unpublished:
            query = query.where(publication_filter(include_classified_ads=True))
        
        result = await self.db.execute(query)
        article = result.scalar_one_or_none()
        
        if not article:
            return None
            
        return self._article_to_dict(article, include_content=True)
    
    async def get_categories_config(self) -> Dict[str, Any]:
        """Get categories configuration."""
        query = select(Category.name, Category.display_name, Category.color)
        result = await self.db.execute(query)
        categories = result.fetchall()
        
        category_config = {}
        for cat in categories:
            category_config[cat.name.lower()] = {
                "name": cat.display_name or cat.name,
                "color": cat.color or "#6c757d"
            }
        
        return {"categories": category_config}
    
    async def get_categories_stats(
        self,
        since_hours: Optional[int] = None,
        hide_ads: bool = True
    ) -> Dict[str, Any]:
        """Get category statistics."""
        
        # Base conditions
        conditions = [publication_filter(include_classified_ads=not hide_ads)]
        if hide_ads:
            conditions.append(Article.is_advertisement.is_(False))
        
        if since_hours:
            since_time = datetime.utcnow() - timedelta(hours=since_hours)
            conditions.append(Article.published_at >= since_time)
        
        # Use simpler approach - count articles directly
        total_query = select(func.count(Article.id))
        for condition in conditions:
            total_query = total_query.where(condition)
        
        total_result = await self.db.execute(total_query)
        total_count = total_result.scalar()
        
        # Get category stats using display categories from categories table
        category_query = select(
            Category.name,
            func.count(ArticleCategory.article_id).label('count')
        ).join(ArticleCategory, Category.id == ArticleCategory.category_id
        ).join(Article, ArticleCategory.article_id == Article.id)
        
        for condition in conditions:
            category_query = category_query.where(condition)
            
        category_query = category_query.where(ArticleCategory.category_id.isnot(None))
        category_query = category_query.group_by(Category.name)
        
        category_result = await self.db.execute(category_query)
        categories_data = category_result.fetchall()
        
        # Build category stats
        category_counts = {"all": total_count}
        
        for category_name, count in categories_data:
            if category_name:
                category_counts[category_name.lower()] = count
        
        return {
            "categories": category_counts,
            "total": total_count,
            "filters": {
                "since_hours": since_hours,
                "hide_ads": hide_ads
            }
        }

    async def get_sources_stats(
        self,
        since_hours: Optional[int] = None,
        hide_ads: bool = True
    ) -> Dict[str, Any]:
        """Get source statistics."""

        # Base conditions
        conditions = [publication_filter(include_classified_ads=not hide_ads)]
        if hide_ads:
            conditions.append(Article.is_advertisement.is_(False))

        if since_hours:
            since_time = datetime.utcnow() - timedelta(hours=since_hours)
            conditions.append(Article.published_at >= since_time)

        # Get total count
        total_query = select(func.count(Article.id))
        for condition in conditions:
            total_query = total_query.where(condition)

        total_result = await self.db.execute(total_query)
        total_count = total_result.scalar()

        # Get source stats
        source_query = select(
            Source.id,
            Source.name,
            func.count(Article.id).label('count')
        ).join(Article, Source.id == Article.source_id
        ).where(Source.enabled == True)

        for condition in conditions:
            source_query = source_query.where(condition)

        source_query = source_query.group_by(Source.id, Source.name).order_by(desc(func.count(Article.id)))

        source_result = await self.db.execute(source_query)
        sources_data = source_result.fetchall()

        # Build source stats
        source_counts = {}
        source_names = {}

        for source_id, source_name, count in sources_data:
            if source_id and source_name:
                source_counts[str(source_id)] = count
                source_names[str(source_id)] = source_name

        return {
            "sources": source_counts,
            "source_names": source_names,
            "total": total_count,
            "filters": {
                "since_hours": since_hours,
                "hide_ads": hide_ads
            }
        }

    def _article_to_dict(self, article: Article, include_content: bool = False) -> Dict[str, Any]:
        """Convert article to dict format."""
        from .feed_formatters import extract_domain, clean_html_entities, clean_summary_text
        
        domain = extract_domain(article.url) if article.url else "unknown"
        cleaned_summary = clean_summary_text(article.summary) if article.summary else None
        
        # Get categories
        article_categories = []
        display_categories = []
        
        for ac in article.article_categories:
            article_categories.append({
                'ai_category': ac.ai_category or 'Other',
                'confidence': ac.confidence or 1.0
            })
            
            # Only access category if it's eagerly loaded (avoid lazy loading)
            if hasattr(ac, '_sa_instance_state') and ac.category_id and ac.category:
                try:
                    display_categories.append({
                        'name': ac.category.name,
                        'display_name': ac.category.display_name or ac.category.name,
                        'confidence': ac.confidence if ac.confidence is not None else 0.0,
                        'color': ac.category.color if hasattr(ac.category, 'color') else '#6c757d'
                    })
                except Exception:
                    # If category is not loaded, skip it
                    pass
        
        primary_category = display_categories[0]['display_name'] if display_categories else 'Other'
        
        result = {
            "id": article.id,
            "article_id": article.id,  # Legacy compatibility
            "title": clean_html_entities(article.title) if article.title else None,
            "summary": clean_html_entities(cleaned_summary) if cleaned_summary else None,
            "image_url": article.image_url,
            "media_files": article.media_files or [],
            "url": article.url,
            "domain": domain,
            "category": primary_category,
            "categories": display_categories,
            "ai_categories": article_categories,
            "published_at": article.published_at.isoformat() if article.published_at else None,
            "fetched_at": article.fetched_at.isoformat() if article.fetched_at else None,
            "is_advertisement": bool(article.is_advertisement or False),
            "ad_confidence": float(article.ad_confidence or 0.0),
            "ad_type": article.ad_type,
            "ad_reasoning": article.ad_reasoning,
            "source_id": article.source_id,
            "source_name": getattr(article.source, 'name', 'Unknown') if article.source else "Unknown"
        }
        
        if include_content:
            result["content"] = clean_html_entities(article.content) if article.content else None
        else:
            # Truncated content for feed
            if article.content:
                truncated = article.content[:500] + "..." if len(article.content) > 500 else article.content
                result["content"] = clean_html_entities(truncated)
        
        return result


# Convenience function for dependency injection
async def get_data_service(db: AsyncSession = Depends(get_db)) -> DataService:
    """Get DataService instance with database dependency."""
    return DataService(db)
