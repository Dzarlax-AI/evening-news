"""PostgreSQL checks for the two-authority domain fence in an isolated schema."""

import json
from pathlib import Path
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from news_aggregator.models import BackgroundJob, BackgroundJobExecution, BackgroundWorkerStatus
from news_aggregator.services.domain_fence import (
    ExecutionContext, GO_EXECUTION_GUARD_XOR, RemoteDomainFence, StaleWorker, parse_envelope,
)

_JOB_PAYLOAD_FIXTURE = Path(__file__).parent / "fixtures" / "execution-contract-v1" / "job-payloads.json"
_NEWS_DIGEST_PAYLOADS = tuple(
    item["normalized"] for item in json.loads(_JOB_PAYLOAD_FIXTURE.read_text(encoding="utf-8"))
    if item["kind"] == "news_digest"
)


def test_digest_payload_fixture_covers_omitted_and_partial_flags():
    assert {} in _NEWS_DIGEST_PAYLOADS
    assert {"run_processing": False} in _NEWS_DIGEST_PAYLOADS
    assert {"send_telegram": False} in _NEWS_DIGEST_PAYLOADS


@pytest.mark.asyncio
async def test_remote_domain_fence_requires_worker_and_go_session_locks(executor_pg):
    engine, factory, _schema = executor_pg
    owner_id, executor_id, job_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    worker_generation, attempt = 17, 1
    deadline = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=2)
    async with engine.connect() as worker_lock, engine.connect() as execution_lock:
        await worker_lock.execute(text(
            "SELECT pg_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"))
        await worker_lock.commit()
        await execution_lock.execute(text(
            "SELECT pg_advisory_lock(hashtext(current_database()), hashtext(current_schema()) # :xor)"),
            {"xor": GO_EXECUTION_GUARD_XOR})
        await execution_lock.commit()
        worker_pid = await worker_lock.scalar(text("SELECT pg_backend_pid()"))
        execution_pid = await execution_lock.scalar(text("SELECT pg_backend_pid()"))

        async with factory() as session:
            status = await session.get(BackgroundWorkerStatus, 1)
            status.owner_id = owner_id
            status.generation = worker_generation
            status.status = "running"
            status.lock_backend_pid = worker_pid
            status.heartbeat_at = datetime.now(timezone.utc).replace(tzinfo=None)
            session.add(BackgroundJob(
                id=job_id, kind="news_processing", payload={}, idempotency_key="domain-fence-fixture",
                state="running", owner_generation=worker_generation, attempt_generation=attempt,
            ))
            session.add(BackgroundJobExecution(
                job_id=job_id, attempt_generation=attempt, envelope_sha256="a" * 64,
                executor_instance_id=executor_id, owner_id=owner_id,
                worker_generation=worker_generation, deadline_at=deadline, state="running",
            ))
            await session.commit()

        raw_envelope = {
            "version": 1, "job_id": str(job_id), "attempt_generation": attempt,
            "owner_id": str(owner_id), "generation": worker_generation,
            "backend_pid": worker_pid, "executor_instance_id": str(executor_id),
            "deadline_at": deadline.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        context = ExecutionContext(parse_envelope(raw_envelope), execution_pid, "news_processing", {})
        fence = RemoteDomainFence(context)
        async with factory() as session:
            assert await fence.validate(session)
            await session.rollback()

        no_go_lock = RemoteDomainFence(ExecutionContext(
            parse_envelope(raw_envelope), execution_pid + 1, "news_processing", {}))
        async with factory() as session:
            with pytest.raises(StaleWorker, match="Go execution guard"):
                await no_go_lock.validate(session)
            await session.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", _NEWS_DIGEST_PAYLOADS)
async def test_remote_domain_fence_accepts_frozen_news_digest_payload_shapes(executor_pg, payload):
    engine, factory, _schema = executor_pg
    owner_id, executor_id, job_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    worker_generation, attempt = 23, 2
    deadline = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=2)
    async with engine.connect() as worker_lock, engine.connect() as execution_lock:
        await worker_lock.execute(text(
            "SELECT pg_advisory_lock(hashtext(current_database()), hashtext(current_schema()))"))
        await worker_lock.commit()
        await execution_lock.execute(text(
            "SELECT pg_advisory_lock(hashtext(current_database()), "
            "hashtext(current_schema()) # :xor)"), {"xor": GO_EXECUTION_GUARD_XOR})
        await execution_lock.commit()
        worker_pid = await worker_lock.scalar(text("SELECT pg_backend_pid()"))
        execution_pid = await execution_lock.scalar(text("SELECT pg_backend_pid()"))

        async with factory() as session:
            status = await session.get(BackgroundWorkerStatus, 1)
            status.owner_id = owner_id
            status.generation = worker_generation
            status.status = "running"
            status.lock_backend_pid = worker_pid
            status.heartbeat_at = datetime.now(timezone.utc).replace(tzinfo=None)
            session.add(BackgroundJob(
                id=job_id, kind="news_digest", payload=payload,
                idempotency_key="digest-payload-" + uuid.uuid4().hex,
                state="running", owner_generation=worker_generation,
                attempt_generation=attempt,
            ))
            session.add(BackgroundJobExecution(
                job_id=job_id, attempt_generation=attempt, envelope_sha256="b" * 64,
                executor_instance_id=executor_id, owner_id=owner_id,
                worker_generation=worker_generation, deadline_at=deadline, state="running",
            ))
            await session.commit()

        envelope = {
            "version": 1, "job_id": str(job_id), "attempt_generation": attempt,
            "owner_id": str(owner_id), "generation": worker_generation, "backend_pid": worker_pid,
            "executor_instance_id": str(executor_id),
            "deadline_at": deadline.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        context = ExecutionContext(parse_envelope(envelope), execution_pid, "news_digest", payload)
        fence = RemoteDomainFence(context)
        async with factory() as session:
            assert await fence.validate(session)
            await session.rollback()
