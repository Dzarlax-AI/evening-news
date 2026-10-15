"""Admission and duplicate-cache ownership regressions for SmartFilter."""

import hashlib
from datetime import datetime, timedelta

import pytest

from news_aggregator.services.smart_filter import SmartFilter


def valid_article_content():
    return "\n\n".join([
        "Researchers released a detailed report on the regional transit project. "
        "The team reviewed several years of service data and interviewed local riders.",
        "The analysis found that schedule changes improved reliability on the busiest routes. "
        "Officials said the next phase will measure travel times during the winter season.",
        "Community groups asked the agency to publish monthly results and explain any delays. "
        "The report recommends keeping the public dashboard available throughout the review.",
    ])


async def admit(smart_filter, *, article_id, content=None, check_duplicates=True):
    return await smart_filter.should_process_with_ai(
        title="Transit reliability report explains recent changes",
        content=content or valid_article_content(),
        url="https://example.test/transit-report",
        source_type="extraction",
        allow_extraction=False,
        article_id=article_id,
        check_duplicates=check_duplicates,
    )


@pytest.mark.asyncio
async def test_same_article_can_be_admitted_repeatedly():
    smart_filter = SmartFilter()
    first = await admit(smart_filter, article_id=71)
    repeated = await admit(smart_filter, article_id=71)

    assert first[0] is True
    assert repeated[0] is True


@pytest.mark.asyncio
async def test_identical_body_for_different_article_is_rejected():
    smart_filter = SmartFilter()
    first = await admit(smart_filter, article_id=71)
    duplicate = await admit(smart_filter, article_id=72)

    assert first[0] is True
    assert duplicate == (False, "Duplicate content detected")


@pytest.mark.asyncio
async def test_ttl_cleanup_removes_expired_owner_entries():
    smart_filter = SmartFilter()
    expired_hash = hashlib.md5(b"expired body").hexdigest()
    expired_at = datetime.now() - smart_filter.duplicate_detection_window - timedelta(seconds=1)
    smart_filter.recent_content_hashes[expired_hash] = expired_at
    smart_filter._content_hash_owners[expired_hash] = 71

    fresh_content = valid_article_content()
    fresh_hash = hashlib.md5(fresh_content.strip().lower().encode("utf-8")).hexdigest()
    assert await smart_filter._is_duplicate_content(fresh_content, article_id=72) is False

    assert expired_hash not in smart_filter.recent_content_hashes
    assert expired_hash not in smart_filter._content_hash_owners
    assert smart_filter._content_hash_owners[fresh_hash] == 72


@pytest.mark.asyncio
async def test_quality_check_without_duplicate_check_does_not_poison_admission():
    smart_filter = SmartFilter()
    content = valid_article_content()
    quality_check = await admit(
        smart_filter, article_id=81, content=content, check_duplicates=False
    )

    assert quality_check[0] is True
    assert smart_filter.recent_content_hashes == {}
    assert smart_filter._content_hash_owners == {}

    later_admission = await admit(
        smart_filter, article_id=82, content=content, check_duplicates=True
    )
    assert later_admission[0] is True
    assert len(smart_filter.recent_content_hashes) == 1
    assert next(iter(smart_filter._content_hash_owners.values())) == 82
