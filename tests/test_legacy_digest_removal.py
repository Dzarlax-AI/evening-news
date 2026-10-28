"""The retired Python digest path must not become a second runtime implementation."""

from pathlib import Path

from news_aggregator.models import DailySummary
from news_aggregator.services.ai_client import AIClient
import news_aggregator.processing as processing
import news_aggregator.services as services


def test_native_digest_cutover_retains_summary_reader_and_category_ai():
    root = Path(__file__).resolve().parents[1] / "news_aggregator"
    assert not (root / "processing" / "digest_builder.py").exists()
    assert not (root / "services" / "summary_generator.py").exists()
    assert not hasattr(processing, "DigestBuilder")
    assert not hasattr(services, "SummaryGenerator")
    assert not hasattr(AIClient, "generate_digest")
    assert callable(AIClient._call_summary_llm)
    assert DailySummary.__tablename__ == "daily_summaries"
