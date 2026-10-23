import logging
"""Categories API router - handles categories and category mappings."""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete, update, text
from sqlalchemy.orm import selectinload

from ..database import get_db
from ..models import Article, Category, ArticleCategory, CategoryMapping
from ..config import settings as app_settings

logger = logging.getLogger(__name__)


router = APIRouter()


# ============================================================================
# Pydantic Models for Categories
# ============================================================================

class CategoryCreateRequest(BaseModel):
    name: str
    display_name: str
    description: Optional[str] = None
    color: Optional[str] = "#6c757d"


class CategoryResponse(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    color: str
    created_at: datetime


class CategoryUpdateRequest(BaseModel):
    display_name: str
    color: Optional[str] = None
    description: Optional[str] = None
    is_local: Optional[bool] = None


# ============================================================================
# Pydantic Models for Category Mappings
# ============================================================================

class CategoryMappingRequest(BaseModel):
    ai_category: str
    fixed_category: str
    confidence_threshold: Optional[float] = 0.7
    description: Optional[str] = None


class CategoryMappingResponse(BaseModel):
    id: int
    ai_category: str
    fixed_category: str
    confidence_threshold: Optional[float] = 0.7
    description: Optional[str] = None
    created_by: Optional[str] = 'system'
    usage_count: Optional[int] = 0
    last_used: Optional[datetime] = None
    is_active: Optional[bool] = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CategoryMappingUpdateResponse(BaseModel):
    id: int
    ai_category: str
    fixed_category: str
    confidence_threshold: float
    description: Optional[str]
    is_active: bool
    updated_at: datetime


# ============================================================================
# Categories Endpoints
# ============================================================================

@router.get("/")
async def get_categories(db: AsyncSession = Depends(get_db)):
    """Get all available categories with article counts."""
    logger.debug("GET /api/v1/categories/ called")

    # Get categories from new table with article counts
    result = await db.execute(
        select(
            Category.id,
            Category.name,
            Category.display_name,
            Category.description,
            Category.color,
            Category.is_local,
            func.count(ArticleCategory.article_id).label('count')
        )
        .outerjoin(ArticleCategory)
        .group_by(Category.id, Category.name, Category.display_name, Category.description, Category.color, Category.is_local)
        .order_by(func.count(ArticleCategory.article_id).desc())
    )

    categories = []
    total_count = 0

    for id, name, display_name, description, color, is_local, count in result.all():
        categories.append({
            "id": id,
            "category": name,
            "display_name": display_name,
            "description": description,
            "color": color,
            "is_local": bool(is_local),
            "count": count
        })
        total_count += count
    
    # Get advertising count
    ad_result = await db.execute(
        select(func.count(Article.id).label('ad_count'))
        .where(Article.is_advertisement == True)
    )
    ad_count = ad_result.scalar() or 0
    
    # Add advertising as a special category if there are any ads
    if ad_count > 0:
        categories.append({
            "category": "advertisements",
            "count": ad_count
        })
    
    return {
        "categories": categories,
        "total_articles": total_count,
        "advertisements": ad_count
    }


@router.post("/", response_model=CategoryResponse)
@router.post("", response_model=CategoryResponse)  # Alternative path without trailing slash
async def create_category(
    payload: CategoryCreateRequest,
    db: AsyncSession = Depends(get_db)
):
    """Create a main category (admin)."""
    logger.debug(f"POST /api/v1/categories/ called with payload: {payload}")
    # TODO: Add admin auth when security is fixed
    
    # Check if category already exists
    existing_result = await db.execute(
        select(Category).where(Category.name == payload.name)
    )
    
    if existing_result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Category already exists")
    
    # Create new category
    category = Category(
        name=payload.name,
        display_name=payload.display_name,
        description=payload.description,
        color=payload.color
    )
    
    db.add(category)
    await db.commit()
    await db.refresh(category)
    
    return category


@router.put("/{category_id}", response_model=CategoryResponse)
async def update_category(
    category_id: int,
    payload: CategoryUpdateRequest,
    db: AsyncSession = Depends(get_db)
):
    """Update a category (admin)."""
    # TODO: Add admin auth when security is fixed
    
    # Find category
    result = await db.execute(
        select(Category).where(Category.id == category_id)
    )
    category = result.scalar_one_or_none()
    
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")
    
    # Update fields
    category.display_name = payload.display_name
    if payload.color:
        category.color = payload.color
    if payload.description is not None:
        category.description = payload.description
    if payload.is_local is not None:
        # Only one category can be local — clear others first
        if payload.is_local:
            await db.execute(
                text("UPDATE categories SET is_local = false WHERE is_local = true AND id != :id"),
                {"id": category_id}
            )
        category.is_local = payload.is_local
    
    await db.commit()
    await db.refresh(category)
    
    return category


# ============================================================================
# Category Mappings Endpoints
# ============================================================================

@router.get("/mappings", response_model=List[CategoryMappingResponse])
async def get_category_mappings(
    active_only: bool = True,
    db: AsyncSession = Depends(get_db)
):
    """Get all category mappings with their usage statistics."""
    query = select(CategoryMapping)
    
    if active_only:
        query = query.where(CategoryMapping.is_active == True)
    
    result = await db.execute(query.order_by(CategoryMapping.usage_count.desc()))
    mappings = result.scalars().all()
    
    return [
        CategoryMappingResponse(
            id=mapping.id,
            ai_category=mapping.ai_category,
            fixed_category=mapping.fixed_category,
            confidence_threshold=mapping.confidence_threshold,
            description=mapping.description,
            created_by=mapping.created_by,
            usage_count=mapping.usage_count,
            last_used=mapping.last_used,
            is_active=mapping.is_active,
            created_at=mapping.created_at,
            updated_at=mapping.updated_at
        ) for mapping in mappings
    ]


@router.post("/mappings", response_model=CategoryMappingResponse)
async def create_category_mapping(
    payload: CategoryMappingRequest,
    db: AsyncSession = Depends(get_db)
):
    """Create a new AI category to fixed category mapping."""
    # TODO: Add admin auth when security is fixed
    
    # Upsert: if a mapping for this ai_category already exists, update it
    existing_result = await db.execute(
        select(CategoryMapping).where(
            CategoryMapping.ai_category == payload.ai_category
        )
    )
    existing = existing_result.scalar_one_or_none()

    if existing:
        existing.fixed_category = payload.fixed_category
        existing.confidence_threshold = payload.confidence_threshold if payload.confidence_threshold is not None else 0.7
        existing.description = payload.description
        existing.is_active = True
        existing.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(existing)
        mapping = existing
    else:
        # Create new mapping
        mapping = CategoryMapping(
            ai_category=payload.ai_category,
            fixed_category=payload.fixed_category,
            confidence_threshold=payload.confidence_threshold if payload.confidence_threshold is not None else 0.7,
            description=payload.description,
            created_by="admin",
            usage_count=0,
            is_active=True
        )
        db.add(mapping)
        await db.commit()
        await db.refresh(mapping)
    
    return CategoryMappingResponse(
        id=mapping.id,
        ai_category=mapping.ai_category,
        fixed_category=mapping.fixed_category,
        confidence_threshold=mapping.confidence_threshold,
        description=mapping.description,
        created_by=mapping.created_by,
        usage_count=mapping.usage_count,
        last_used=mapping.last_used,
        is_active=mapping.is_active,
        created_at=mapping.created_at,
        updated_at=mapping.updated_at
    )


@router.put("/mappings/{mapping_id}", response_model=CategoryMappingUpdateResponse)
async def update_category_mapping(
    mapping_id: int,
    payload: CategoryMappingRequest,
    db: AsyncSession = Depends(get_db)
):
    """Update an existing category mapping."""
    # TODO: Add admin auth when security is fixed
    
    # Find mapping
    result = await db.execute(
        select(CategoryMapping).where(CategoryMapping.id == mapping_id)
    )
    mapping = result.scalar_one_or_none()
    
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    # Update fields
    mapping.ai_category = payload.ai_category
    mapping.fixed_category = payload.fixed_category
    mapping.confidence_threshold = payload.confidence_threshold if payload.confidence_threshold is not None else 0.7
    mapping.description = payload.description
    mapping.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(mapping)
    
    return CategoryMappingUpdateResponse(
        id=mapping.id,
        ai_category=mapping.ai_category,
        fixed_category=mapping.fixed_category,
        confidence_threshold=mapping.confidence_threshold,
        description=mapping.description,
        is_active=mapping.is_active,
        updated_at=mapping.updated_at
    )


@router.delete("/mappings/{mapping_id}")
async def delete_category_mapping(
    mapping_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a category mapping."""
    # TODO: Add admin auth when security is fixed
    
    # Find mapping
    result = await db.execute(
        select(CategoryMapping).where(CategoryMapping.id == mapping_id)
    )
    mapping = result.scalar_one_or_none()
    
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    # Delete mapping
    await db.execute(delete(CategoryMapping).where(CategoryMapping.id == mapping_id))
    await db.commit()
    
    return {"message": "Mapping deleted successfully"}


@router.post("/mappings/{mapping_id}/toggle")
async def toggle_category_mapping(
    mapping_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Toggle active status of a category mapping."""
    # TODO: Add admin auth when security is fixed
    
    # Find mapping
    result = await db.execute(
        select(CategoryMapping).where(CategoryMapping.id == mapping_id)
    )
    mapping = result.scalar_one_or_none()
    
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    # Toggle active status
    mapping.is_active = not mapping.is_active
    mapping.updated_at = datetime.utcnow()
    
    await db.commit()
    
    return {
        "message": f"Mapping {'activated' if mapping.is_active else 'deactivated'}",
        "is_active": mapping.is_active
    }


@router.get("/mappings/fixed-categories")
async def get_fixed_categories(db: AsyncSession = Depends(get_db)):
    """Get all available fixed categories from the main categories table."""
    result = await db.execute(
        select(Category.name).order_by(Category.name)
    )
    
    fixed_categories = [row[0] for row in result.all()]
    
    return {"fixed_categories": fixed_categories}


@router.get("/mappings/unmapped")
async def get_unmapped_ai_categories(
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """Get AI categories that don't have mappings yet."""
    
    # Get all unique AI categories from article_categories
    # that don't have mappings in category_mapping table
    from sqlalchemy import text
    
    query = text("""
        SELECT DISTINCT ac.ai_category, COUNT(*) as usage_count
        FROM article_categories ac
        LEFT JOIN category_mapping cm ON LOWER(ac.ai_category) = LOWER(cm.ai_category)
        WHERE ac.ai_category IS NOT NULL
          AND ac.ai_category != ''
          AND cm.ai_category IS NULL
        GROUP BY ac.ai_category
        ORDER BY usage_count DESC, ac.ai_category
        LIMIT :limit
    """)
    
    result = await db.execute(query, {"limit": limit})
    unmapped_data = result.fetchall()
    
    unmapped_categories = [
        {
            "ai_category": row.ai_category,
            "usage_count": row.usage_count,
            "suggested_fixed_category": _suggest_fixed_category(row.ai_category),
            "examples": []  # Could be populated with article titles using this category
        }
        for row in unmapped_data
    ]
    
    return {
        "message": "Unmapped AI categories analysis",
        "unmapped_categories": unmapped_categories,
        "total_unmapped": len(unmapped_categories),
        "note": "These AI categories need manual mapping to fixed categories"
    }


def _suggest_fixed_category(ai_category: str) -> str:
    """Suggest a fixed category based on AI category name."""
    if not ai_category:
        return "Other"
    
    ai_lower = ai_category.lower()
    
    # Technology keywords
    if any(keyword in ai_lower for keyword in [
        'tech', 'software', 'innovation', 'digital', 'ai', 'computer', 'programming'
    ]):
        return "Tech"
    
    # Business keywords  
    elif any(keyword in ai_lower for keyword in [
        'business', 'economy', 'finance', 'market', 'trade', 'investment', 'company'
    ]):
        return "Business"
    
    # Politics keywords
    elif any(keyword in ai_lower for keyword in [
        'politics', 'government', 'election', 'policy', 'law', 'legal', 'parliament'
    ]):
        return "Politics"
    
    # International keywords
    elif any(keyword in ai_lower for keyword in [
        'international', 'world', 'global', 'foreign', 'europe', 'russia', 'china'
    ]):
        return "International"
    
    # Serbia keywords
    elif any(keyword in ai_lower for keyword in [
        'serbia', 'belgrade', 'serbian', 'вучич', 'белград', 'сербия'
    ]):
        return "Serbia"
    
    # Science keywords
    elif any(keyword in ai_lower for keyword in [
        'science', 'research', 'study', 'health', 'medicine', 'environment', 'education'
    ]):
        return "Science"
    
    else:
        return "Other"


# ============================================================================
# AI Category Mapping Analysis
# ============================================================================

class AICategoryAnalysisRequest(BaseModel):
    """Request model for AI category analysis."""
    limit: Optional[int] = 50
    include_examples: Optional[bool] = True
    confidence_threshold: Optional[float] = 0.7


class AISuggestion(BaseModel):
    """Individual AI suggestion for category mapping."""
    category: str
    confidence: float
    reasoning: str


class AICategoryAnalysisResponse(BaseModel):
    """Response model for AI category analysis."""
    ai_category: str
    suggested_fixed_category: str  # Primary suggestion (for backward compatibility)
    confidence: float  # Primary confidence (for backward compatibility)
    usage_count: int
    example_articles: List[str]
    reasoning: str  # Primary reasoning (for backward compatibility)
    all_suggestions: List[AISuggestion]  # All AI suggestions


class AICategoryAnalysisBatchResponse(BaseModel):
    """Response model for batch AI category analysis."""
    suggestions: List[AICategoryAnalysisResponse]
    total_analyzed: int
    execution_time_seconds: float
    

class AICategoryMappingApprovalRequest(BaseModel):
    """Request model for approving AI category mappings."""
    ai_category: str
    approved_fixed_category: str
    confidence_threshold: Optional[float] = 0.7
    description: Optional[str] = None


@router.post("/ai-category-analysis", response_model=AICategoryAnalysisBatchResponse)
async def analyze_categories_with_ai(
    request: AICategoryAnalysisRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Analyze uncategorized articles and suggest category mappings using AI.
    Provides confidence scores and reasoning for each suggestion.
    """
    if app_settings.background_execution_mode == "worker":
        raise HTTPException(
            status_code=409,
            detail="AI category analysis is unavailable in queue-only web mode",
        )
    import time
    from ..services.ai_client import get_ai_client
    from ..services.prompts import NewsPrompts
    
    start_time = time.time()
    
    # Get unmapped AI categories with examples
    query = text("""
        SELECT DISTINCT ac.ai_category, COUNT(*) as usage_count,
               GROUP_CONCAT(DISTINCT a.title SEPARATOR ' | ') as example_titles
        FROM article_categories ac
        JOIN articles a ON ac.article_id = a.id
        LEFT JOIN category_mapping cm ON LOWER(ac.ai_category) = LOWER(cm.ai_category)
        WHERE ac.ai_category IS NOT NULL
          AND ac.ai_category != ''
          AND cm.ai_category IS NULL
        GROUP BY ac.ai_category
        ORDER BY usage_count DESC, ac.ai_category
        LIMIT :limit
    """)
    
    result = await db.execute(query, {"limit": request.limit})
    unmapped_data = result.fetchall()
    
    if not unmapped_data:
        return AICategoryAnalysisBatchResponse(
            suggestions=[],
            total_analyzed=0,
            execution_time_seconds=time.time() - start_time
        )
    
    # Get available fixed categories from database
    categories_result = await db.execute(
        select(Category.name, Category.display_name)
        .order_by(Category.name)
    )
    available_categories = categories_result.fetchall()
    categories_list = [f"{name} ({display_name})" for name, display_name in available_categories]
    
    # Prepare AI analysis
    ai_client = get_ai_client()
    suggestions = []
    
    for row in unmapped_data:
        ai_category = row.ai_category
        usage_count = row.usage_count
        example_titles = (row.example_titles or "").split(" | ")[:5]  # Limit to 5 examples
        
        # Create AI prompt for category analysis with multiple suggestions
        prompt = f"""Analyze this AI-generated category and suggest the top 3 most appropriate mappings to our fixed categories.

AI CATEGORY: "{ai_category}"
USAGE COUNT: {usage_count} articles
EXAMPLE ARTICLES: {example_titles[:3]}

AVAILABLE FIXED CATEGORIES:
{chr(10).join([f"- {cat}" for cat in categories_list])}

TASK: Provide 3 ranked suggestions for mapping this AI category to fixed categories.

Response in JSON format:
{{
    "suggestions": [
        {{
            "category": "Most appropriate category name (exact match from list above)",
            "confidence": 0.85,
            "reasoning": "Brief explanation why this is the best match"
        }},
        {{
            "category": "Second best category name",
            "confidence": 0.65,
            "reasoning": "Brief explanation for second choice"
        }},
        {{
            "category": "Third alternative category name",
            "confidence": 0.45,
            "reasoning": "Brief explanation for third choice"
        }}
    ]
}}

Requirements:
- Always provide exactly 3 suggestions
- Categories must be exact matches from the available list
- Confidence scores should reflect semantic similarity
- Consider semantic similarity, example articles context, and categorization patterns
- Order suggestions by confidence (highest first)
"""

        try:
            # Get AI suggestion using the correct method
            ai_response = await ai_client._call_summary_llm(
                prompt=prompt,
                system_prompt="You are a news categorization expert. Analyze categories and provide accurate mappings. Always respond with valid JSON."
            )
            
            # Parse AI response
            import json
            
            if ai_response is None:
                raise Exception("AI response is None")
            
            try:
                # Clean the response - remove any non-JSON content
                ai_response_clean = ai_response.strip()
                if not ai_response_clean.startswith('{'):
                    # Try to find JSON in the response
                    json_start = ai_response_clean.find('{')
                    if json_start != -1:
                        ai_response_clean = ai_response_clean[json_start:]
                    else:
                        raise json.JSONDecodeError("No JSON found in response", ai_response, 0)
                
                ai_suggestion = json.loads(ai_response_clean)
                suggestions_list = ai_suggestion.get("suggestions", [])
                
                if suggestions_list and len(suggestions_list) > 0:
                    # Use the first (highest confidence) suggestion as primary
                    primary = suggestions_list[0]
                    suggested_category = primary.get("category", "Other").split(" (")[0]
                    confidence = float(primary.get("confidence", 0.5))
                    reasoning = primary.get("reasoning", "AI analysis")
                    
                    # Store all suggestions for UI
                    all_suggestions = []
                    for sugg in suggestions_list[:3]:  # Max 3 suggestions
                        all_suggestions.append({
                            "category": sugg.get("category", "Other").split(" (")[0],
                            "confidence": float(sugg.get("confidence", 0.0)),
                            "reasoning": sugg.get("reasoning", "")
                        })
                else:
                    # Fallback if no suggestions array
                    suggested_category = "Other"
                    confidence = 0.3
                    reasoning = "No AI suggestions provided"
                    all_suggestions = [{"category": "Other", "confidence": 0.3, "reasoning": "Fallback suggestion"}]
            except json.JSONDecodeError as json_err:
                logger.info(f"JSON parsing failed for '{ai_category}'. AI response: {ai_response[:200]}...")
                # Fallback to rule-based suggestion
                suggested_category = _suggest_fixed_category(ai_category)
                confidence = 0.3
                reasoning = f"Fallback to keyword-based analysis (JSON error: {str(json_err)[:50]})"
                all_suggestions = [{"category": suggested_category, "confidence": 0.3, "reasoning": reasoning}]
            except Exception as parse_err:
                logger.info(f"Response parsing failed for '{ai_category}': {parse_err}")
                # Fallback to rule-based suggestion
                suggested_category = _suggest_fixed_category(ai_category)
                confidence = 0.3
                reasoning = "Fallback to keyword-based analysis (parse error)"
                all_suggestions = [{"category": suggested_category, "confidence": 0.3, "reasoning": reasoning}]
            
            # Convert dict suggestions to AISuggestion objects
            ai_suggestions = [
                AISuggestion(
                    category=sugg["category"],
                    confidence=sugg["confidence"],
                    reasoning=sugg["reasoning"]
                ) for sugg in all_suggestions
            ]
            
            suggestions.append(AICategoryAnalysisResponse(
                ai_category=ai_category,
                suggested_fixed_category=suggested_category,
                confidence=confidence,
                usage_count=usage_count,
                example_articles=example_titles[:5] if request.include_examples else [],
                reasoning=reasoning,
                all_suggestions=ai_suggestions
            ))
            
        except Exception as e:
            logger.info(f"AI analysis failed for category '{ai_category}': {type(e).__name__}: {str(e)}")
            # Fallback to rule-based suggestion
            fallback_category = _suggest_fixed_category(ai_category)
            fallback_reasoning = f"Keyword-based analysis (AI error: {type(e).__name__})"
            
            suggestions.append(AICategoryAnalysisResponse(
                ai_category=ai_category,
                suggested_fixed_category=fallback_category,
                confidence=0.3,
                usage_count=usage_count,
                example_articles=example_titles[:5] if request.include_examples else [],
                reasoning=fallback_reasoning,
                all_suggestions=[AISuggestion(
                    category=fallback_category,
                    confidence=0.3,
                    reasoning=fallback_reasoning
                )]
            ))
    
    # Sort by confidence descending
    suggestions.sort(key=lambda x: x.confidence, reverse=True)
    
    return AICategoryAnalysisBatchResponse(
        suggestions=suggestions,
        total_analyzed=len(suggestions),
        execution_time_seconds=time.time() - start_time
    )


@router.post("/ai-category-mapping/approve")
async def approve_ai_category_mapping(
    request: AICategoryMappingApprovalRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Approve and apply an AI category mapping suggestion.
    Creates a category mapping entry and optionally applies it to existing articles.
    """
    try:
        # Check if mapping already exists
        existing_mapping = await db.execute(
            select(CategoryMapping)
            .where(CategoryMapping.ai_category == request.ai_category)
        )
        existing = existing_mapping.scalar_one_or_none()
        
        if existing:
            if existing.is_active:
                return {
                    "success": False,
                    "message": f"Active mapping already exists for '{request.ai_category}'"
                }
            else:
                # Reactivate existing mapping with new settings
                existing.fixed_category = request.approved_fixed_category
                existing.confidence_threshold = request.confidence_threshold
                existing.description = request.description
                existing.is_active = True
                existing.updated_at = func.now()
                await db.commit()
                return {
                    "success": True,
                    "message": f"Reactivated mapping: '{request.ai_category}' → '{request.approved_fixed_category}'"
                }
        
        # Create new mapping
        new_mapping = CategoryMapping(
            ai_category=request.ai_category,
            fixed_category=request.approved_fixed_category,
            confidence_threshold=request.confidence_threshold,
            description=request.description or f"AI-suggested mapping approved via admin panel",
            created_by="admin_ai_analysis",
            is_active=True
        )
        
        db.add(new_mapping)
        await db.commit()
        
        # Get count of articles that will be affected
        count_result = await db.execute(
            select(func.count(ArticleCategory.article_id))
            .where(ArticleCategory.ai_category == request.ai_category)
        )
        affected_articles = count_result.scalar() or 0
        
        return {
            "success": True,
            "message": f"Created mapping: '{request.ai_category}' → '{request.approved_fixed_category}'",
            "affected_articles": affected_articles,
            "note": "Mapping will be applied to new articles automatically. Existing articles can be updated via reprocessing."
        }
        
    except Exception as e:
        await db.rollback()
        logger.info(f"Error creating AI category mapping: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create category mapping: {str(e)}"
        )

