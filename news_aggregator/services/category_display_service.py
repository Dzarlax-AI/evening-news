import logging
"""
Service for mapping AI categories to display categories at runtime.
"""

from typing import List, Dict, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from ..models import Category, CategoryMapping

logger = logging.getLogger(__name__)


class CategoryDisplayService:
    """Service for mapping AI categories to display categories."""

    # Fallback display info used only when DB is unavailable
    FALLBACK_CATEGORIES = {
        'Serbia': {'display_name': 'Сербия', 'color': '#dc3545'},
        'Tech': {'display_name': 'Технологии', 'color': '#007bff'},
        'AI': {'display_name': 'Искусственный интеллект', 'color': '#17a2b8'},
        'Business': {'display_name': 'Бизнес', 'color': '#28a745'},
        'Science': {'display_name': 'Наука', 'color': '#6f42c1'},
        'Politics': {'display_name': 'Политика', 'color': '#839933'},
        'International': {'display_name': 'Международные', 'color': '#cd51bc'},
        'Other': {'display_name': 'Прочее', 'color': '#6c757d'}
    }
    
    # Default mapping rules
    DEFAULT_MAPPING = {
        # Technology terms
        'technology': 'Tech',
        'tech': 'Tech',
        'software': 'Tech',
        'ai': 'AI',
        'artificial intelligence': 'AI',
        'computer': 'Tech',
        'digital': 'Tech',
        'internet': 'Tech',
        'programming': 'Tech',
        'innovation': 'Tech',
        'технологии': 'Tech',
        'компьютер': 'Tech',
        'программирование': 'Tech',
        'интернет': 'Tech',
        'цифровой': 'Tech',
        
        # Business terms
        'business': 'Business',
        'economy': 'Business',
        'finance': 'Business',
        'market': 'Business',
        'trade': 'Business',
        'investment': 'Business',
        'company': 'Business',
        'startup': 'Business',
        'бизнес': 'Business',
        'экономика': 'Business',
        'финансы': 'Business',
        'торговля': 'Business',
        'инвестиции': 'Business',
        'компания': 'Business',
        
        # Politics terms
        'politics': 'Politics',
        'government': 'Politics',
        'election': 'Politics',
        'policy': 'Politics',
        'law': 'Politics',
        'legal': 'Politics',
        'parliament': 'Politics',
        'president': 'Politics',
        'minister': 'Politics',
        'политика': 'Politics',
        'правительство': 'Politics',
        'выборы': 'Politics',
        'закон': 'Politics',
        'парламент': 'Politics',
        'президент': 'Politics',
        'министр': 'Politics',
        
        # International terms
        'international': 'International',
        'world': 'International',
        'global': 'International',
        'foreign': 'International',
        'europe': 'International',
        'usa': 'International',
        'china': 'International',
        'russia': 'International',
        'nato': 'International',
        'eu': 'International',
        'международные': 'International',
        'мир': 'International',
        'глобальный': 'International',
        'европа': 'International',
        'россия': 'International',
        'китай': 'International',
        
        # Serbia terms
        'serbia': 'Serbia',
        'belgrade': 'Serbia',
        'serbian': 'Serbia',
        'вучич': 'Serbia',
        'белград': 'Serbia',
        'сербия': 'Serbia',
        'сербский': 'Serbia',
        
        # Science terms
        'science': 'Science',
        'research': 'Science',
        'study': 'Science',
        'health': 'Science',
        'medicine': 'Science',
        'environment': 'Science',
        'climate': 'Science',
        'nature': 'Science',
        'наука': 'Science',
        'исследование': 'Science',
        'здоровье': 'Science',
        'медицина': 'Science',
        'природа': 'Science',
        'климат': 'Science',
        
        # General/Other terms
        'news': 'Other',
        'general': 'Other',
        'other': 'Other',
        'society': 'Other',
        'culture': 'Other',
        'sports': 'Other',
        'entertainment': 'Other',
        'новости': 'Other',
        'общество': 'Other',
        'культура': 'Other',
        'спорт': 'Other',
        'развлечения': 'Other',
        'образование': 'Other',
        'общее': 'Other',
    }
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _get_category_info(self, category_name: str) -> Dict[str, str]:
        """Get category display info from DB, fallback to FALLBACK_CATEGORIES."""
        try:
            result = await self.db.execute(
                select(Category).where(Category.name == category_name).limit(1)
            )
            cat = result.scalar_one_or_none()
            if cat:
                return {'display_name': cat.display_name, 'color': cat.color}
        except Exception as e:
            logger.debug(f"DB category lookup failed for '{category_name}': {e}")
        return self.FALLBACK_CATEGORIES.get(category_name, self.FALLBACK_CATEGORIES['Other'])

    async def map_ai_category_to_display(self, ai_category: str, *, commit: bool = True) -> Dict[str, Any]:
        """Map a category; commit=False keeps usage writes in the caller's transaction."""
        if not ai_category or ai_category.strip() == '':
            ai_category = 'Other'
        
        # First try database mapping
        try:
            result = await self.db.execute(
                select(CategoryMapping).where(
                    func.lower(CategoryMapping.ai_category) == ai_category.lower().strip(),
                    CategoryMapping.is_active == True
                ).limit(1)
            )
            db_mapping = result.scalar_one_or_none()
            
            if db_mapping:
                # Update usage stats
                db_mapping.usage_count = (db_mapping.usage_count or 0) + 1
                db_mapping.last_used = func.now()
                if commit:
                    await self.db.commit()

                fixed_category = db_mapping.fixed_category
                display_info = await self._get_category_info(fixed_category)

                return {
                    'name': fixed_category,
                    'display_name': display_info['display_name'],
                    'color': display_info['color'],
                    'ai_category': ai_category,
                    'mapping_source': 'database'
                }
        except Exception as e:
            logger.warning(f"  ⚠️ Database mapping lookup failed for '{ai_category}': {e}")
            if not commit:
                raise

        # Try default mapping rules
        ai_lower = ai_category.lower().strip()

        # Exact match
        if ai_lower in self.DEFAULT_MAPPING:
            fixed_category = self.DEFAULT_MAPPING[ai_lower]
            display_info = await self._get_category_info(fixed_category)

            return {
                'name': fixed_category,
                'display_name': display_info['display_name'],
                'color': display_info['color'],
                'ai_category': ai_category,
                'mapping_source': 'default_exact'
            }

        # Partial match
        for keyword, fixed_category in self.DEFAULT_MAPPING.items():
            if keyword in ai_lower or ai_lower in keyword:
                display_info = await self._get_category_info(fixed_category)

                return {
                    'name': fixed_category,
                    'display_name': display_info['display_name'],
                    'color': display_info['color'],
                    'ai_category': ai_category,
                    'mapping_source': f'default_partial:{keyword}'
                }

        # Fallback to Other
        display_info = await self._get_category_info('Other')
        return {
            'name': 'Other',
            'display_name': display_info['display_name'],
            'color': display_info['color'],
            'ai_category': ai_category,
            'mapping_source': 'fallback'
        }
    
    async def get_article_display_categories(self, ai_categories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert list of AI categories to display categories."""
        display_categories = []
        
        for ai_cat in ai_categories:
            ai_category_name = ai_cat.get('ai_category') or ai_cat.get('name', 'Other')
            confidence = ai_cat.get('confidence', 1.0)
            
            # Map to display category
            display_cat = await self.map_ai_category_to_display(ai_category_name)
            display_cat['confidence'] = confidence
            
            display_categories.append(display_cat)
        
        # Remove duplicates (if AI gave same category mapped to same fixed category)
        unique_categories = {}
        for cat in display_categories:
            key = cat['name']
            if key not in unique_categories or cat['confidence'] > unique_categories[key]['confidence']:
                unique_categories[key] = cat
        
        return list(unique_categories.values())
    
    async def get_all_display_categories(self) -> List[Dict[str, Any]]:
        """Get all available display categories from DB, fallback to FALLBACK_CATEGORIES."""
        try:
            result = await self.db.execute(select(Category).order_by(Category.name))
            db_categories = result.scalars().all()
            if db_categories:
                return [
                    {'name': cat.name, 'display_name': cat.display_name, 'color': cat.color}
                    for cat in db_categories
                ]
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to load display categories from DB: {e}")

        return [
            {'name': name, 'display_name': info['display_name'], 'color': info['color']}
            for name, info in self.FALLBACK_CATEGORIES.items()
        ]


async def get_category_display_service(db: AsyncSession) -> CategoryDisplayService:
    """Get category display service instance."""
    return CategoryDisplayService(db)
