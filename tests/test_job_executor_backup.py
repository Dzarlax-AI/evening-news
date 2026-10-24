"""Attempt-owned backup publication checks."""

from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import uuid

import pytest

from news_aggregator.services import backup_service
from news_aggregator.services.job_fence import JobExecutionFence, current_job_fence


class _FenceLease:
    def __init__(self, *, reject=False):
        self.reject = reject

    @asynccontextmanager
    async def publication_fence(self, session):
        if self.reject:
            raise RuntimeError("stale worker")
        yield


class _FakeSession:
    async def commit(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None


@pytest.mark.asyncio
async def test_backup_publication_uses_attempt_specific_non_overwriting_path(tmp_path, monkeypatch):
    from news_aggregator import database

    monkeypatch.setattr(database, "AsyncSessionLocal", _FakeSession)
    service = backup_service.BackupService()
    service.project_root = Path(tmp_path)
    service.config_file = service.project_root / "backup_schedule.json"
    service.backups_dir = service.project_root / "backups"

    async def fake_dump(path):
        path.write_bytes(b"synthetic dump")
        return True

    monkeypatch.setattr(service, "_run_pg_dump", fake_dump)
    tokens = []
    for attempt in (1, 2):
        token = current_job_fence.set(JobExecutionFence(
            _FenceLease(), uuid.uuid4(), attempt, active=True,
        ))
        try:
            archive = await service.create_backup()
            tokens.append(archive)
        finally:
            current_job_fence.reset(token)

    assert tokens[0] != tokens[1]
    assert all(path.exists() for path in tokens)
    assert len(list(service.backups_dir.glob("news_aggregator_backup_*.tar.gz"))) == 2


@pytest.mark.asyncio
async def test_backup_stale_attempt_cannot_publish_archive(tmp_path, monkeypatch):
    from news_aggregator import database

    monkeypatch.setattr(database, "AsyncSessionLocal", _FakeSession)
    service = backup_service.BackupService()
    service.project_root = Path(tmp_path)
    service.config_file = service.project_root / "backup_schedule.json"
    service.backups_dir = service.project_root / "backups"

    async def fake_dump(path):
        path.write_bytes(b"synthetic dump")
        return True

    monkeypatch.setattr(service, "_run_pg_dump", fake_dump)
    token = current_job_fence.set(JobExecutionFence(
        _FenceLease(reject=True), uuid.uuid4(), 1, active=True,
    ))
    try:
        with pytest.raises(RuntimeError, match="Backup creation failed"):
            await service.create_backup()
    finally:
        current_job_fence.reset(token)
    assert not list(service.backups_dir.glob("news_aggregator_backup_*.tar.gz"))


@pytest.mark.asyncio
async def test_cancelled_backup_removes_attempt_staging(tmp_path, monkeypatch):
    from news_aggregator import database

    monkeypatch.setattr(database, "AsyncSessionLocal", _FakeSession)
    service = backup_service.BackupService()
    service.project_root = Path(tmp_path)
    service.config_file = service.project_root / "backup_schedule.json"
    service.backups_dir = service.project_root / "backups"
    started = asyncio.Event()

    async def blocked_dump(_path):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(service, "_run_pg_dump", blocked_dump)
    token = current_job_fence.set(JobExecutionFence(
        _FenceLease(), uuid.uuid4(), 1, active=True,
    ))
    try:
        task = asyncio.create_task(service.create_backup())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        current_job_fence.reset(token)

    assert not list(service.backups_dir.glob("*.working"))
    assert not list(service.backups_dir.glob(".*.working"))
    assert not list(service.backups_dir.glob("news_aggregator_backup_*.tar.gz"))


@pytest.mark.asyncio
async def test_cancelled_pg_dump_is_terminated_and_reaped(tmp_path, monkeypatch):
    service = backup_service.BackupService()
    service._parse_database_url = lambda: ("127.0.0.1", "5432", "fixture", "fixture-only", "fixture")
    monkeypatch.setattr(backup_service.settings, "background_execution_mode", "worker")
    started = asyncio.Event()

    class FakeProcess:
        returncode = None
        terminated = False
        reaped = False

        async def communicate(self):
            started.set()
            await asyncio.Future()

        def terminate(self):
            self.terminated = True

        async def wait(self):
            self.reaped = True
            self.returncode = -15
            return self.returncode

        def kill(self):
            self.returncode = -9

    process = FakeProcess()

    async def create_process(*_args, **_kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    task = asyncio.create_task(service._run_pg_dump(Path(tmp_path) / "database.sql"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated
    assert process.reaped
