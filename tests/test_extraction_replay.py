"""Keep the parity harness honest about source semantics and offline operation."""

import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_python_pipeline.py"
SPEC = importlib.util.spec_from_file_location("extraction_replay", MODULE_PATH)
replay_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay_module)
COMPARE_SPEC = importlib.util.spec_from_file_location("compare_replay", MODULE_PATH.with_name("compare_extraction_replays.py"))
compare_module = importlib.util.module_from_spec(COMPARE_SPEC)
COMPARE_SPEC.loader.exec_module(compare_module)


def test_oracle_rejects_unspecified_or_silent_empty_results():
    assert replay_module.check_expected({}, {"content": "", "state": "failed"})
    assert replay_module.check_expected({"expected": {"contains": []}}, {"content": "", "state": "failed"})
    assert replay_module.check_expected({"expected": {"contains": []}}, {"content": "Valid content", "state": "extracted"})
    assert not replay_module.check_expected({"expected": {"state": "failed"}}, {"content": "", "state": "failed"})


def test_comparison_rejects_disjoint_cycle_sets_instead_of_reporting_zero_differences():
    with pytest.raises(ValueError, match="Different case/cycle sets"):
        compare_module.compare({("case", 0): {}}, {("case", 1): {}})


def test_comparison_checks_unasserted_metadata_even_when_both_oracles_pass():
    left = {("case", 1): {"result": {"state": "extracted", "title": "Original title"}, "errors": []}}
    right = {("case", 1): {"result": {"state": "extracted", "title": "Changed title"}, "passed": True}}
    report = compare_module.compare(left, right)
    assert not report["assertion_failures"]
    assert report["unreviewed_differences"][0]["field"] == "title"


def test_media_comparison_preserves_metadata_and_normalizes_only_documented_ids():
    a = [{"type": "contact", "url": "#contact_random", "contact_data": {"phone": "123"}, "source": "contact"}]
    b = [{"type": "contact", "url": "#contact_stable", "contact_data": {"phone": "123"}, "source": "contact"}]
    assert compare_module.canonical("media", a) == compare_module.canonical("media", b)
    b[0]["source"] = "wrong"
    assert compare_module.canonical("media", a) != compare_module.canonical("media", b)


@pytest.mark.asyncio
async def test_rss_source_uses_production_parser_without_treating_input_as_url(monkeypatch):
    called = []
    original = replay_module.feedparser.parse
    def capture(value):
        called.append(value)
        assert isinstance(value, bytes)
        return original(value)
    monkeypatch.setattr(replay_module.feedparser, "parse", capture)
    result = await replay_module.replay({"source_name": "Fixture", "input": {
        "strategy": "rss", "rss_xml": '<rss><channel><item><title>Offline title</title>'
        '<link>https://fixture.test/news</link><description>Useful source text.</description>'
        '</item></channel></rss>'}})
    assert called and result["content"] == "Useful source text."
    assert result["state"] == "extracted"


@pytest.mark.asyncio
async def test_custom_conversion_preserves_description_and_link():
    text = ("Researchers published detailed measurements from a river monitoring project. "
            "The technical report includes calibration tables and independent observations.")
    result = await replay_module.replay({"source_name": "Fixture", "input": {
        "strategy": "custom_selectors", "url": "https://fixture.test/",
        "html": f'<article><h2><a href="/news">Independent river monitoring report</a></h2><p>{text}</p></article>',
        "selectors": {"article": ["article"], "title": ["h2"], "link": ["a"]}}})
    assert result["state"] == "extracted"
    assert "calibration tables" in result["content"]
    assert result["url"] == "https://fixture.test/news"


@pytest.mark.asyncio
async def test_custom_browser_uses_rendered_capture_without_http_fallback():
    base = {"strategy": "browser_rendering", "url": "https://fixture.test/", "use_browser": True,
            "html": '<article><h2>Stale HTTP title</h2><p>Old content.</p></article>',
            "rendered_html": '<article><h2>Rendered browser title</h2><p>Fresh content.</p></article>'}
    result = await replay_module.replay({"source_type": "custom", "input": base})
    assert result["title"] == "Rendered browser title"
    result = await replay_module.replay({"source_type": "custom", "input": {**base, "rendered_html": ""}})
    assert result["state"] == "failed"


@pytest.mark.asyncio
async def test_media_only_telegram_post_is_not_reported_failed():
    result = await replay_module.replay({"source_name": "Fixture", "input": {
        "strategy": "telegram_http", "url": "https://t.me/s/fixture",
        "telegram_html": '<div class="tgme_widget_message" data-post="fixture/1">'
        '<a class="tgme_widget_message_photo_wrap" style="background-image:url(\'https://cdn.test/photo.jpg\')"></a>'
        '<a class="tgme_widget_message_date" href="https://t.me/fixture/1">'
        '<time datetime="2026-10-15T10:00:00Z"></time></a></div>'}})
    assert result["state"] == "extracted"
    assert result["media"]


@pytest.mark.asyncio
async def test_static_markup_does_not_claim_transport_cancellation():
    result = await replay_module.replay({"input": {
        "strategy": "browser_rendering", "cancel_at": "browser_html_capture"}})
    assert result["state"] == "failed" and result["error"] == "cancelled"
    assert "cancelled" in result["trace"]
    assert "tab_closed" not in result["trace"]


@pytest.mark.asyncio
async def test_forced_readability_does_not_accept_short_subscription_copy():
    result = await replay_module.replay({"input": {
        "url": "https://fixture.test/members", "strategy": "readability",
        "html": "<article><p>This story is for members. Create a free account to continue reading.</p></article>"}})
    assert result["candidate_chars"] > 0
    assert result["state"] == "failed"
    assert result["error"] == "quality_rejected"


@pytest.mark.asyncio
async def test_article_attachment_does_not_mask_rejected_article_body():
    result = await replay_module.replay({"source_type": "article", "methods": ["M04"], "input": {
        "url": "https://fixture.test/members", "strategy": "readability",
        "html": '<article><p>This story is for members.</p><video src="https://cdn.test/clip.mp4"></video></article>'}})
    assert result["media"]
    assert not result["content"]
    assert result["state"] == "failed"


@pytest.mark.asyncio
async def test_forced_candidate_uses_final_presentation_limit():
    sentences = [" ".join(f"{word}{i}" for word in (
        "Independent", "measurements", "describe", "regional", "seasonal", "calibration",
        "observations", "published", "technical", "methods", "available", "research")) + "."
        for i in range(100)]
    body = " ".join(sentences)
    result = await replay_module.replay({"input": {
        "url": "https://fixture.test/report", "strategy": "json_ld",
        "html": '<script type="application/ld+json">' + replay_module.json.dumps({
            "@type": "Article", "articleBody": body}) + '</script>'}})
    assert result["state"] == "extracted"
    assert result["original_chars"] == len(body)
    assert len(result["content"]) <= 8000
    assert result["truncated"]
    assert result["truncation_policy"] == "sentence_or_word_8000"


@pytest.mark.asyncio
async def test_alternative_capture_obeys_utf8_byte_cap_before_parsing():
    result = await replay_module.replay({"input": {
        "url": "https://fixture.test/article", "strategy": "auto", "max_bytes": 10,
        "alternative_html": {"https://fixture.test/amp": "界" * 4}}})
    assert result["state"] == "resource_limit"
    assert result["error"] == "alternative_html byte cap exceeded"


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["auto", "source_content"])
async def test_original_source_retention_is_distinct_from_article_quality(strategy):
    result = await replay_module.replay({"input": {
        "url": "https://fixture.test/article", "strategy": strategy,
        "html": "", "content": "Original stored source text."}})
    assert result["state"] == "extracted"
    assert result["method"] == "source_content"
    assert result["content"] == "Original stored source text."


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["article", "rss", "telegram", "custom"])
async def test_long_retained_text_uses_source_type_presentation_contract(source_type):
    body = "Original stored source paragraph. " * 400
    result = await replay_module.replay({"source_type": source_type, "input": {
        "url": "https://fixture.test/source", "strategy": "source_content", "content": body}})
    assert result["state"] == "extracted"
    if source_type == "article":
        assert len(result["content"]) <= 8000
    else:
        assert result["content"] == body


def reviewed_example():
    case = {"id": "reviewed.case", "input": {"html": "Captured source"}, "expected": {"state": "extracted"}}
    left = {(case["id"], 1): {"result": {"state": "extracted", "content": "Menus. Required introduction. Complete body."}, "errors": []}}
    right = {(case["id"], 1): {"result": {"state": "extracted", "content": "Required introduction. Complete body."}, "passed": True, "failures": []}}
    review = {"kind": "difference", "case_id": case["id"], "field": "content",
              "case_sha256": compare_module.fingerprint(case),
              "python_sha256": compare_module.fingerprint(left[(case["id"], 1)]["result"]["content"]),
              "go_sha256": compare_module.fingerprint(right[(case["id"], 1)]["result"]["content"]),
              "classification": "improvement", "reason": "Source navigation removed, required body preserved",
              "evidence": "Pinned structural fixture", "golden": {"state": "extracted", "contains": ["Required introduction", "Complete body"], "not_contains": ["Menus."]}}
    return case, left, right, review


def test_review_accepts_only_pinned_source_and_output_pair():
    case, left, right, review = reviewed_example()
    report = compare_module.compare(left, right, [review], {case["id"]: case})
    assert report["comparison_gate"] == "passed"
    assert len(report["reviewed_differences"]) == 1
    right[(case["id"], 1)]["result"]["content"] = "Complete body."
    report = compare_module.compare(left, right, [review], {case["id"]: case})
    assert report["unreviewed_differences"]


def test_changing_source_or_golden_requirement_invalidates_review():
    case, left, right, review = reviewed_example()
    case["input"]["html"] = "Changed captured source"
    assert compare_module.compare(left, right, [review], {case["id"]: case})["unreviewed_differences"]
    review["case_sha256"] = compare_module.fingerprint(case)
    review["golden"]["contains"].append("Missing required paragraph")
    assert compare_module.compare(left, right, [review], {case["id"]: case})["unreviewed_differences"]


def test_go_assertion_failure_cannot_be_waived_by_review():
    case, left, right, review = reviewed_example()
    right[(case["id"], 1)].update(passed=False, failures=["missing required content"])
    report = compare_module.compare(left, right, [review], {case["id"]: case})
    assert report["assertion_failures"] and report["unreviewed_differences"]
    assert not report["reviewed_differences"]


def test_review_loader_rejects_wildcards_and_unknown_permissions(tmp_path):
    _, _, _, review = reviewed_example()
    path = tmp_path / "decisions.json"
    review["case_id"] = "reviewed.*"
    path.write_text(replay_module.json.dumps({"version": 1, "reviews": [review]}), encoding="utf-8")
    with pytest.raises(ValueError, match="specific case"):
        compare_module.load_reviews(path)
    review["case_id"] = "reviewed.case"
    review["allow_any"] = True
    path.write_text(replay_module.json.dumps({"version": 1, "reviews": [review]}), encoding="utf-8")
    with pytest.raises(ValueError, match="exact fields"):
        compare_module.load_reviews(path)


def test_reviewed_baseline_defect_pins_errors_and_requires_go_golden():
    case, left, right, review = reviewed_example()
    left[(case["id"], 1)]["errors"] = ["baseline accepted membership UI"]
    review.update(kind="baseline_failure", classification="baseline_defect",
                  python_sha256=compare_module.fingerprint(left[(case["id"], 1)]["errors"]),
                  go_sha256=compare_module.fingerprint(right[(case["id"], 1)]["result"]))
    review.pop("field")
    report = compare_module.compare(left, right, [review], {case["id"]: case})
    assert not report["assertion_failures"]
    assert len(report["reviewed_baseline_defects"]) == 1
    left[(case["id"], 1)]["errors"].append("unrelated new baseline failure")
    assert compare_module.compare(left, right, [review], {case["id"]: case})["assertion_failures"]


def test_equal_incomplete_replays_cannot_omit_a_corpus_case():
    case, left, right, review = reviewed_example()
    with pytest.raises(ValueError, match="case IDs"):
        compare_module.compare(left, right, [review], {case["id"]: case, "missing": {"id": "missing"}})


def test_equal_replays_cannot_silently_omit_required_cycles():
    case, left, right, review = reviewed_example()
    with pytest.raises(ValueError, match="required cycles"):
        compare_module.compare(left, right, [review], {case["id"]: case}, expected_cycles=2)


def test_source_golden_runs_even_if_both_implementations_agree():
    case, left, right, _ = reviewed_example()
    left[(case["id"], 1)]["result"] = right[(case["id"], 1)]["result"].copy()
    golden = {case["id"]: {"case_sha256": compare_module.fingerprint(case),
                          "evidence": "Original source introduction", "expected": {
                              "state": "extracted", "contains": ["Missing source introduction"]}}}
    report = compare_module.compare(left, right, cases={case["id"]: case}, goldens=golden)
    assert not report["unreviewed_differences"]
    assert report["golden_failures"] and report["comparison_gate"] == "requires_review"


def test_source_golden_cannot_omit_a_corpus_case():
    case, left, right, _ = reviewed_example()
    with pytest.raises(ValueError, match="cover every"):
        compare_module.compare(left, right, cases={case["id"]: case}, goldens={})


def test_source_golden_requires_inline_media_even_when_both_outputs_omit_it():
    case, left, right, _ = reviewed_example()
    left[(case["id"], 1)]["result"] = right[(case["id"], 1)]["result"].copy()
    golden = {case["id"]: {"case_sha256": compare_module.fingerprint(case), "evidence": "Source inline photograph",
                          "expected": {"state": "extracted", "media_contains": [{"type": "image", "url": "https://source.test/photo.jpg"}]}}}
    report = compare_module.compare(left, right, cases={case["id"]: case}, goldens=golden)
    assert report["golden_failures"]
    right[(case["id"], 1)]["result"]["media"] = [{"type": "image", "url": "https://source.test/photo.jpg", "alt_text": "Independent observation"}]
    assert not compare_module.compare(left, right, cases={case["id"]: case}, goldens=golden)["golden_failures"]
