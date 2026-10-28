import pytest

from news_aggregator.database import split_db_pool_budget


@pytest.mark.parametrize(
    ("pool_size", "overflow", "expected"),
    [(5, 10, (5, 8)), (4, 0, (2, 0)), (2, 2, (2, 0)), (10, 0, (8, 0))],
)
def test_native_pool_is_reserved_from_existing_total(pool_size, overflow, expected):
    assert split_db_pool_budget(pool_size, overflow) == expected


@pytest.mark.parametrize(("pool_size", "overflow"), [(0, 10), (0, 4), (1, 2), (0, 0), (3, -1)])
def test_pool_budget_fails_when_python_would_have_less_than_guard_and_operational(pool_size, overflow):
    with pytest.raises(ValueError):
        split_db_pool_budget(pool_size, overflow)


def test_pool_budget_rejects_boolean_counts():
    with pytest.raises(ValueError):
        split_db_pool_budget(True, 3)
