"""Legacy Python result-projection characterization, not worker tests.

Python job-worker lifecycle cases were retired with the worker. Their current
Go coverage is in go-execution/execution_postgres_test.go
(TestPostgresAcceptFenceContextCancelAndDuplicate),
go-execution/execution_close_postgres_test.go
(TestCloseBoundsCancellationResistantPipelineAndFencesLateResult), and
go-execution/orchestration/legacy_postgres_test.go
(TestLegacyAdapterPostgresFinalizationAndRecovery). Domain-side PostgreSQL
fence coverage remains in tests/test_domain_fence_postgres.py.
"""

from news_aggregator.services.background_jobs import _safe_result
from tests.frozen_execution_contract import bounded_result as _bounded_result


def test_legacy_selected_result_projection_stays_bounded_and_aggregate_safe():
    domain = {
        "status": "incomplete",
        "article_outcomes": [
            {"article_id": article_id, "state": "completed", "reason": "transient detail"}
            for article_id in range(1, 100)
        ],
    }
    domain["article_outcomes"].append(
        {"article_id": 100, "state": "rejected", "reason": "resource_limit"}
    )
    result = _bounded_result("article_reprocess", domain)
    assert result["article_outcomes_total"] == 100
    assert result["article_outcomes_omitted"] == 80
    assert len(result["article_outcomes"]) == 20
    assert result["status"] == "incomplete"
    assert _safe_result(result) == result


def test_retired_python_runtime_has_no_job_dispatch_or_orchestrator():
    import importlib
    import importlib.util

    runtime = importlib.import_module("news_aggregator.services.background_execution")
    executor = importlib.import_module("news_aggregator.executor")
    assert not hasattr(runtime, "execute_job")
    assert not hasattr(runtime, "DurableBackgroundWorker")
    assert not hasattr(runtime, "_run_worker")
    assert not hasattr(executor, "PythonJobExecutor")
    assert importlib.util.find_spec("news_aggregator.orchestrator") is None
