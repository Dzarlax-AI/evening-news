from scripts.article_processing_source_inventory import (
    REMOVED_PYTHON_PROCESSOR_SOURCES,
    REQUIRED_ARTICLE_PROCESSING_SOURCES,
    article_processing_inventory_matches,
)


def test_article_processing_source_inventory_requires_client_and_callback():
    hashes = {path: "synthetic" for path in REQUIRED_ARTICLE_PROCESSING_SOURCES}
    assert article_processing_inventory_matches(hashes)
    for path in REQUIRED_ARTICLE_PROCESSING_SOURCES:
        incomplete = {key: value for key, value in hashes.items() if key != path}
        assert not article_processing_inventory_matches(incomplete)


def test_article_processing_source_inventory_rejects_stale_python_processor():
    hashes = {path: "synthetic" for path in REQUIRED_ARTICLE_PROCESSING_SOURCES}
    for path in REMOVED_PYTHON_PROCESSOR_SOURCES:
        assert not article_processing_inventory_matches({**hashes, path: "stale"})


def test_article_processing_source_inventory_rejects_python_quality_filter():
    hashes = {path: "synthetic" for path in REQUIRED_ARTICLE_PROCESSING_SOURCES}
    stale_filter = "news_aggregator/services/smart_filter.py"
    assert stale_filter in REMOVED_PYTHON_PROCESSOR_SOURCES
    assert not article_processing_inventory_matches({**hashes, stale_filter: "stale"})
