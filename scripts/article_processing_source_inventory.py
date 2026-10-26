"""Assertions for the Python API side of the Go article-processing boundary."""

REQUIRED_ARTICLE_PROCESSING_SOURCES = {
    "news_aggregator/api/processing_router.py",
    "news_aggregator/services/article_processing_client.py",
}
REMOVED_PYTHON_PROCESSOR_SOURCES = {
    "news_aggregator/processing/ai_processor.py",
    "news_aggregator/processing/categorization_processor.py",
    "news_aggregator/processing/processing_outcome.py",
    "news_aggregator/processing/summarization_processor.py",
    "news_aggregator/services/article_retry.py",
    "news_aggregator/services/smart_filter.py",
}


def article_processing_inventory_matches(source_hashes: dict[str, str]) -> bool:
    """Require both sides of the API boundary and reject stale processor modules."""
    return (REQUIRED_ARTICLE_PROCESSING_SOURCES <= source_hashes.keys()
            and not (REMOVED_PYTHON_PROCESSOR_SOURCES & source_hashes.keys()))
