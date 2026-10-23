"""Worker-mode backup subprocess boundary checks."""

from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_worker_pg_dump_uses_local_binary_and_environment_password(monkeypatch, tmp_path):
    from news_aggregator.config import settings
    from news_aggregator.services import backup_service as module

    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(
        settings, "database_url",
        "postgresql+asyncpg://backup_user:fixture-password@db.fixture.invalid:5544/news_test",
    )
    invocation = {}

    class StubProcess:
        returncode = 0

        async def communicate(self):
            return b"fixture dump", b""

    async def fake_subprocess(*args, **kwargs):
        invocation["args"] = args
        invocation["kwargs"] = kwargs
        return StubProcess()

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", fake_subprocess)
    backup_file = Path(tmp_path) / "database.sql"
    assert await module.BackupService()._run_pg_dump(backup_file)
    assert invocation["args"][0] == "pg_dump"
    assert "fixture-password" not in repr(invocation["args"])
    assert invocation["kwargs"]["env"]["PGPASSWORD"] == "fixture-password"
    assert "-h" in invocation["args"] and "db.fixture.invalid" in invocation["args"]
    assert backup_file.read_bytes() == b"fixture dump"


@pytest.mark.asyncio
async def test_cancelled_worker_pg_dump_terminates_and_reaps_child(monkeypatch, tmp_path):
    import asyncio
    from news_aggregator.config import settings
    from news_aggregator.services import backup_service as module

    monkeypatch.setattr(settings, "background_execution_mode", "worker")
    monkeypatch.setattr(
        settings, "database_url",
        "postgresql+asyncpg://backup_user:fixture-password@db.fixture.invalid:5544/news_test",
    )
    spawned = asyncio.Event()

    class HangingProcess:
        returncode = None

        async def communicate(self):
            spawned.set()
            await asyncio.Future()

        def terminate(self):
            self.terminated = True

        async def wait(self):
            self.returncode = 0
            self.reaped = True
            return 0

        def kill(self):
            self.killed = True

    process = HangingProcess()

    async def fake_subprocess(*args, **kwargs):
        return process

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", fake_subprocess)
    task = asyncio.create_task(module.BackupService()._run_pg_dump(Path(tmp_path) / "dump.sql"))
    await spawned.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.terminated is True
    assert process.reaped is True
    assert not hasattr(process, "killed")
