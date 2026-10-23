"""Context-local fencing for database commits made during a worker job."""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


@dataclass
class JobExecutionFence:
    lease: Any
    job_id: Any
    attempt_generation: int
    active: bool = True


current_job_fence: ContextVar[JobExecutionFence | None] = ContextVar(
    "current_background_job_fence", default=None
)
