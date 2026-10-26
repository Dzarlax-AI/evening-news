"""Domain-level feed formatting remains available without the legacy API router."""

from news_aggregator.services.feed_formatters import (
    clean_html_entities,
    clean_summary_text,
    extract_domain,
)


def test_extract_domain_normalizes_host_and_preserves_empty_host():
    assert extract_domain("HTTPS://Example.Test/path") == "example.test"
    assert extract_domain("/relative/path") == ""


def test_clean_html_entities_removes_markup_and_malformed_feed_text():
    assert clean_html_entities("News &amp; <b>markets</b>") == "News & markets"
    assert clean_html_entities("A @channel /post Читать оригинал B") == "A B"


def test_clean_summary_text_removes_service_prefix_and_empty_bullets():
    assert clean_summary_text("Краткое содержание: •  Рост рынка\n\n\nНовые данные:") == "Рост рынка\n\nНовые данные"
    assert clean_summary_text("  ") == ""
