"""Worker-side invariants for externally owned schedules."""

import asyncio
from types import SimpleNamespace


def test_service_job_completion_never_writes_legacy_schedule_row():
    from news_aggregator.services.background_execution import DurableBackgroundWorker

    worker = DurableBackgroundWorker()

    class SessionThatMustNotBeRead:
        async def get(self, *_args, **_kwargs):
            raise AssertionError("service-owned schedule must not be updated locally")

    job = SimpleNamespace(schedule_key="news:42", payload={"schedule_id": 42})
    asyncio.run(worker._finish_schedule_in_session(SessionThatMustNotBeRead(), job, "succeeded", {"status": "success"}))


def test_service_job_alert_key_uses_stable_schedule_identity():
    from news_aggregator.services.background_execution import DurableBackgroundWorker

    class Alerts:
        def __init__(self):
            self.calls = []

        async def failure(self, key, *_args):
            self.calls.append(("failure", key))

        async def recovery(self, key, *_args):
            self.calls.append(("recovery", key))

    alerts = Alerts()
    worker = DurableBackgroundWorker()
    worker.alerts = alerts
    job = SimpleNamespace(schedule_key="news:42", kind="news_digest", payload={})
    asyncio.run(worker._notify_schedule(job, "failed", None))
    assert alerts.calls == [("failure", "scheduler-warning-news:42")]
