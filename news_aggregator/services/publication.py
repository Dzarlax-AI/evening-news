"""Article visibility rules shared by public and API readers."""


def publication_filter(include_classified_ads=False):
    """Expose completed work, with confirmed ads available in explicit ad views."""
    from sqlalchemy import and_, or_
    from ..models import Article

    completed = Article.processing_state == "completed"
    if include_classified_ads:
        return or_(completed, and_(
            Article.processing_state == "rejected",
            Article.ad_processed.is_(True),
            Article.is_advertisement.is_(True),
        ))
    return completed
