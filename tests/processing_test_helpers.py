"""Small isolated SQLite helpers for publication and digest tests."""

from datetime import datetime

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from news_aggregator.database import Base
from news_aggregator.orchestrator import NewsOrchestrator


class AsyncSessionAdapter:
    def __init__(self, db):
        self.db = db

    async def execute(self, statement, parameters=None):
        return self.db.execute(statement, parameters or {})

    async def commit(self):
        self.db.commit()


class IsolatedQueue:
    def __init__(self, engine):
        self.engine = engine

    async def execute_read(self, operation, **kwargs):
        with Session(self.engine) as db:
            return await operation(AsyncSessionAdapter(db))


def isolated_publication_database():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def add_now(connection, _):
        connection.create_function("now", 0, lambda: datetime.utcnow().isoformat())

    Base.metadata.create_all(engine)
    orchestrator = NewsOrchestrator.__new__(NewsOrchestrator)
    orchestrator.db_queue_manager = IsolatedQueue(engine)
    return orchestrator, engine
