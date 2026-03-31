from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Awaitable, Callable

from enterprise_starter.domain.events import WorkflowEvent

TaskFunc = Callable[["ExecutionContext"], Awaitable[Any]]


class TaskState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CACHED = "cached"


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff_seconds: float = 0.0
    exponential: bool = False

    def delay_for_attempt(self, attempt: int) -> float:
        if attempt <= 1:
            return 0.0
        if self.exponential:
            return self.backoff_seconds * (2 ** (attempt - 2))
        return self.backoff_seconds


@dataclass(slots=True, frozen=True)
class TaskSpec:
    name: str
    handler: TaskFunc
    dependencies: tuple[str, ...] = ()
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    cache_ttl_seconds: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class WorkflowSpec:
    name: str
    tasks: tuple[TaskSpec, ...]
    max_concurrency: int = 4
    fail_fast: bool = True


@dataclass(slots=True)
class TaskRecord:
    name: str
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    started_at: float | None = None
    finished_at: float | None = None
    result: Any = None
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        if self.started_at is None or self.finished_at is None:
            return 0.0
        return self.finished_at - self.started_at


@dataclass(slots=True)
class ExecutionContext:
    workflow_name: str
    task_name: str
    attempt: int
    inputs: dict[str, Any]
    emit: Callable[[WorkflowEvent], Awaitable[None]]
    scratchpad: dict[str, Any]


@dataclass(slots=True, frozen=True)
class WorkflowSummary:
    run_id: str
    workflow_name: str
    completed: tuple[str, ...]
    failed: tuple[str, ...]
    cached: tuple[str, ...]
    skipped: tuple[str, ...]
    task_durations: dict[str, float]
    critical_path_seconds: float
    total_wall_time_seconds: float


@dataclass(slots=True)
class ExecutionArtifacts:
    records: dict[str, TaskRecord]
    events: list[WorkflowEvent] = field(default_factory=list)
    started_at: float = field(default_factory=time.perf_counter)
    finished_at: float | None = None


@dataclass(slots=True, frozen=True)
class PersistedRun:
    run_id: str
    workflow_name: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    total_wall_time_seconds: float
    critical_path_seconds: float
    payload_json: str
