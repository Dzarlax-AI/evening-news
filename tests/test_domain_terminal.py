"""Terminal event boundary checks without database or provider calls."""

import asyncio
import gc
import uuid
from types import SimpleNamespace

import pytest

from news_aggregator.services import domain_terminal


def event_body(**overrides):
    body = {
        "version": 1,
        "job_id": str(uuid.uuid4()),
        "attempt_generation": 0,
        "owner_id": str(uuid.uuid4()),
        "generation": 1,
        "backend_pid": 4321,
        "executor_instance_id": str(uuid.uuid4()),
        "executor_backend_pid": 5432,
    }
    body.update(overrides)
    return body


def test_terminal_event_requires_exact_fields_canonical_uuid_and_nonboolean_integers():
    body = event_body()
    parsed = domain_terminal.parse_terminal_event(body)
    assert parsed.attempt_generation == 0
    assert parsed.worker_backend_pid == 4321
    with pytest.raises(domain_terminal.TerminalEventError):
        domain_terminal.parse_terminal_event({**body, "extra": True})
    with pytest.raises(ValueError):
        domain_terminal.parse_terminal_event({**body, "attempt_generation": True})
    with pytest.raises(ValueError):
        domain_terminal.parse_terminal_event({**body, "generation": 1.0})
    with pytest.raises(ValueError):
        domain_terminal.parse_terminal_event({**body, "executor_instance_id": body["executor_instance_id"].upper()})


@pytest.mark.asyncio
async def test_terminal_event_requires_current_go_instance_before_database_work(monkeypatch):
    body = event_body()
    event = domain_terminal.parse_terminal_event(body)
    monkeypatch.setattr(domain_terminal, "_current_go_execution_instance", _async_value(uuid.uuid4()))

    async def no_database(*_args, **_kwargs):
        raise AssertionError("database access must follow current Go instance validation")

    monkeypatch.setattr(domain_terminal, "_validate_terminal_db", no_database)
    with pytest.raises(domain_terminal.TerminalEventError, match="not current"):
        await domain_terminal.process_finalized_event(event)


@pytest.mark.asyncio
async def test_duplicate_finalized_events_for_one_attempt_are_serialized(monkeypatch):
    body = event_body()
    event = domain_terminal.parse_terminal_event(body)
    monkeypatch.setattr(domain_terminal, "_current_go_execution_instance",
                        _async_value(event.executor_instance_id))
    receipt = SimpleNamespace(legacy_alert_state="pending")
    job = SimpleNamespace(kind="news_processing", state="succeeded", result={"status": "success"},
                          payload={}, schedule_key=None, queue_position=1)
    active = 0
    max_active = 0

    async def validate(_session, _event):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return job, receipt

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def commit(self):
            pass

        async def rollback(self):
            pass

    monkeypatch.setattr(domain_terminal, "_validate_terminal_db", validate)
    monkeypatch.setattr(domain_terminal, "AsyncSessionLocal", Session)
    monkeypatch.setattr(domain_terminal, "_alert_manager", lambda: SimpleNamespace(is_active=lambda _key: False))

    first, second = await asyncio.gather(
        domain_terminal.process_finalized_event(event),
        domain_terminal.process_finalized_event(event),
    )
    assert max_active == 1
    assert {first["alert_state"], second["alert_state"]} == {"not_needed"}
    gc.collect()
    assert len(domain_terminal._EVENT_LOCKS) == 0


@pytest.mark.asyncio
async def test_finalized_unique_job_locks_are_reclaimed(monkeypatch):
    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def commit(self):
            pass

        async def rollback(self):
            pass

    async def validate(_session, event):
        receipt = SimpleNamespace(legacy_alert_state="pending")
        job = SimpleNamespace(kind="news_processing", state="succeeded", result={"status": "success"},
                              payload={}, schedule_key=None, queue_position=1)
        return job, receipt

    current_instance = None

    async def current_go_instance():
        return current_instance

    monkeypatch.setattr(domain_terminal, "_current_go_execution_instance", current_go_instance)
    monkeypatch.setattr(domain_terminal, "_validate_terminal_db", validate)
    monkeypatch.setattr(domain_terminal, "AsyncSessionLocal", Session)
    monkeypatch.setattr(domain_terminal, "_alert_manager", lambda: SimpleNamespace(is_active=lambda _key: False))

    for _ in range(128):
        body = event_body()
        current_instance = uuid.UUID(body["executor_instance_id"])
        event = domain_terminal.parse_terminal_event(body)
        assert (await domain_terminal.process_finalized_event(event))["alert_state"] == "not_needed"
    gc.collect()
    assert len(domain_terminal._EVENT_LOCKS) == 0


def _async_value(value):
    async def result():
        return value
    return result
