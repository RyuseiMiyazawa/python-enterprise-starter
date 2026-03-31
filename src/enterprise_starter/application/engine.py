from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from enterprise_starter.domain.events import WorkflowEvent
from enterprise_starter.domain.exceptions import TaskExecutionError, WorkflowValidationError
from enterprise_starter.domain.models import (
    ExecutionArtifacts,
    ExecutionContext,
    RetryPolicy,
    TaskRecord,
    TaskSpec,
    TaskState,
    WorkflowSpec,
    WorkflowSummary,
)
from enterprise_starter.infrastructure.cache import InMemoryTTLCache
from enterprise_starter.infrastructure.event_bus import EventBus
from enterprise_starter.infrastructure.persistence import WorkflowRunStoreProtocol


@dataclass(slots=True)
class WorkflowRunResult:
    outputs: dict[str, Any]
    summary: WorkflowSummary
    events: list[WorkflowEvent]
    records: dict[str, TaskRecord]


class WorkflowEngine:
    def __init__(
        self,
        cache: InMemoryTTLCache | None = None,
        run_store: WorkflowRunStoreProtocol | None = None,
    ) -> None:
        self._cache = cache or InMemoryTTLCache()
        self._run_store = run_store

    async def run(self, workflow: WorkflowSpec) -> WorkflowRunResult:
        run_id = uuid4().hex
        task_map = self._validate(workflow)
        dependents, indegree = self._build_graph(task_map)
        event_bus = EventBus()
        artifacts = ExecutionArtifacts(records={name: TaskRecord(name=name) for name in task_map})
        outputs: dict[str, Any] = {}
        scratchpad: dict[str, Any] = {}
        completed: set[str] = set()
        failed: set[str] = set()
        cached: set[str] = set()
        skipped: set[str] = set()

        queue: asyncio.Queue[str] = asyncio.Queue()
        for task_name, degree in indegree.items():
            if degree == 0:
                await queue.put(task_name)

        semaphore = asyncio.Semaphore(workflow.max_concurrency)
        active: set[asyncio.Task[tuple[str, bool, Any]]] = set()
        stop_scheduling = False

        async def launch_ready_tasks() -> None:
            while not queue.empty() and not stop_scheduling:
                task_name = await queue.get()
                if task_name in completed or task_name in failed or task_name in skipped:
                    continue
                active.add(asyncio.create_task(run_one(task_map[task_name], semaphore)))

        async def run_one(spec: TaskSpec, concurrency_guard: asyncio.Semaphore) -> tuple[str, bool, Any]:
            async with concurrency_guard:
                return await self._execute_task(
                    workflow_name=workflow.name,
                    spec=spec,
                    event_bus=event_bus,
                    outputs=outputs,
                    scratchpad=scratchpad,
                    record=artifacts.records[spec.name],
                )

        while queue.qsize() > 0 or active:
            await launch_ready_tasks()
            if not active:
                break

            done, pending = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
            active = set(pending)
            for finished in done:
                task_name, ok, value = await finished
                if ok:
                    outputs[task_name] = value
                    completed.add(task_name)
                    if artifacts.records[task_name].state == TaskState.CACHED:
                        cached.add(task_name)
                    for dependent in dependents[task_name]:
                        indegree[dependent] -= 1
                        if indegree[dependent] == 0:
                            await queue.put(dependent)
                else:
                    failed.add(task_name)
                    blocked = self._mark_blocked_descendants(
                        root=task_name,
                        dependents=dependents,
                        records=artifacts.records,
                        failed=failed,
                    )
                    skipped.update(blocked)
                    if workflow.fail_fast:
                        stop_scheduling = True
                        for task in active:
                            task.cancel()
                        if active:
                            await asyncio.gather(*active, return_exceptions=True)
                        active.clear()

        artifacts.finished_at = time.perf_counter()
        await event_bus.close()
        summary = self._build_summary(
            run_id=run_id,
            workflow=workflow,
            task_map=task_map,
            records=artifacts.records,
            completed=completed,
            failed=failed,
            cached=cached,
            skipped=skipped,
            started_at=artifacts.started_at,
            finished_at=artifacts.finished_at,
        )
        if self._run_store is not None:
            await asyncio.to_thread(
                self._run_store.save_run,
                summary=summary,
                outputs=outputs,
                records=artifacts.records,
                events=event_bus.events,
            )
        return WorkflowRunResult(
            outputs=outputs,
            summary=summary,
            events=event_bus.events,
            records=artifacts.records,
        )

    def _validate(self, workflow: WorkflowSpec) -> dict[str, TaskSpec]:
        if workflow.max_concurrency < 1:
            raise WorkflowValidationError("max_concurrency must be >= 1")

        task_map: dict[str, TaskSpec] = {}
        for task in workflow.tasks:
            if task.name in task_map:
                raise WorkflowValidationError(f"duplicate task name: {task.name}")
            task_map[task.name] = task

        for task in workflow.tasks:
            for dependency in task.dependencies:
                if dependency not in task_map:
                    raise WorkflowValidationError(
                        f"task {task.name} depends on unknown task {dependency}"
                    )

        self._ensure_acyclic(task_map)
        return task_map

    def _ensure_acyclic(self, task_map: dict[str, TaskSpec]) -> None:
        temporary: set[str] = set()
        permanent: set[str] = set()

        def visit(name: str) -> None:
            if name in permanent:
                return
            if name in temporary:
                raise WorkflowValidationError(f"cycle detected at task {name}")
            temporary.add(name)
            for dependency in task_map[name].dependencies:
                visit(dependency)
            temporary.remove(name)
            permanent.add(name)

        for name in task_map:
            visit(name)

    def _build_graph(
        self, task_map: dict[str, TaskSpec]
    ) -> tuple[dict[str, list[str]], dict[str, int]]:
        dependents: dict[str, list[str]] = defaultdict(list)
        indegree = {name: len(spec.dependencies) for name, spec in task_map.items()}
        for task_name, spec in task_map.items():
            for dependency in spec.dependencies:
                dependents[dependency].append(task_name)
        for name in task_map:
            dependents.setdefault(name, [])
        return dependents, indegree

    async def _execute_task(
        self,
        workflow_name: str,
        spec: TaskSpec,
        event_bus: EventBus,
        outputs: dict[str, Any],
        scratchpad: dict[str, Any],
        record: TaskRecord,
    ) -> tuple[str, bool, Any]:
        cache_key = f"{workflow_name}:{spec.name}"
        if spec.cache_ttl_seconds is not None:
            cached_value = self._cache.get(cache_key)
            if cached_value is not None:
                record.state = TaskState.CACHED
                record.started_at = time.perf_counter()
                record.finished_at = record.started_at
                record.result = cached_value
                await event_bus.publish(
                    WorkflowEvent(kind="task_cached", task_name=spec.name, payload={"cache_key": cache_key})
                )
                return spec.name, True, cached_value

        await event_bus.publish(
            WorkflowEvent(kind="task_queued", task_name=spec.name, payload={"dependencies": spec.dependencies})
        )

        try:
            value = await self._attempt_execution(
                workflow_name=workflow_name,
                spec=spec,
                event_bus=event_bus,
                outputs=outputs,
                scratchpad=scratchpad,
                record=record,
            )
        except TaskExecutionError as exc:
            record.state = TaskState.FAILED
            record.error = str(exc)
            await event_bus.publish(
                WorkflowEvent(kind="task_failed", task_name=spec.name, payload={"error": str(exc)})
            )
            return spec.name, False, exc

        if spec.cache_ttl_seconds is not None:
            self._cache.set(cache_key, value, spec.cache_ttl_seconds)
        return spec.name, True, value

    async def _attempt_execution(
        self,
        workflow_name: str,
        spec: TaskSpec,
        event_bus: EventBus,
        outputs: dict[str, Any],
        scratchpad: dict[str, Any],
        record: TaskRecord,
    ) -> Any:
        policy: RetryPolicy = spec.retry_policy
        for attempt in range(1, policy.max_attempts + 1):
            record.attempts = attempt
            record.state = TaskState.RUNNING
            record.started_at = record.started_at or time.perf_counter()
            await event_bus.publish(
                WorkflowEvent(
                    kind="task_started",
                    task_name=spec.name,
                    payload={"attempt": attempt, "workflow": workflow_name},
                )
            )
            try:
                context = ExecutionContext(
                    workflow_name=workflow_name,
                    task_name=spec.name,
                    attempt=attempt,
                    inputs={dependency: outputs[dependency] for dependency in spec.dependencies},
                    emit=event_bus.publish,
                    scratchpad=scratchpad,
                )
                result = await spec.handler(context)
                record.finished_at = time.perf_counter()
                record.result = result
                record.state = TaskState.SUCCEEDED
                await event_bus.publish(
                    WorkflowEvent(kind="task_succeeded", task_name=spec.name, payload={"attempt": attempt})
                )
                return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await event_bus.publish(
                    WorkflowEvent(
                        kind="task_retrying" if attempt < policy.max_attempts else "task_exhausted",
                        task_name=spec.name,
                        payload={"attempt": attempt, "error": repr(exc)},
                    )
                )
                if attempt >= policy.max_attempts:
                    record.finished_at = time.perf_counter()
                    raise TaskExecutionError(spec.name, attempt, repr(exc)) from exc
                await asyncio.sleep(policy.delay_for_attempt(attempt + 1))
        raise AssertionError("unreachable")

    def _mark_blocked_descendants(
        self,
        root: str,
        dependents: dict[str, list[str]],
        records: dict[str, TaskRecord],
        failed: set[str],
    ) -> set[str]:
        blocked: set[str] = set()
        queue: deque[str] = deque(dependents[root])
        while queue:
            current = queue.popleft()
            if current in blocked or current in failed:
                continue
            blocked.add(current)
            records[current].state = TaskState.SKIPPED
            records[current].error = f"blocked by upstream failure: {root}"
            queue.extend(dependents[current])
        return blocked

    def _build_summary(
        self,
        run_id: str,
        workflow: WorkflowSpec,
        task_map: dict[str, TaskSpec],
        records: dict[str, TaskRecord],
        completed: set[str],
        failed: set[str],
        cached: set[str],
        skipped: set[str],
        started_at: float,
        finished_at: float,
    ) -> WorkflowSummary:
        durations = {name: record.duration_seconds for name, record in records.items()}
        critical_path_seconds = self._critical_path(task_map, durations)
        return WorkflowSummary(
            run_id=run_id,
            workflow_name=workflow.name,
            completed=tuple(sorted(completed)),
            failed=tuple(sorted(failed)),
            cached=tuple(sorted(cached)),
            skipped=tuple(sorted(skipped)),
            task_durations=durations,
            critical_path_seconds=critical_path_seconds,
            total_wall_time_seconds=finished_at - started_at,
        )

    def _critical_path(self, task_map: dict[str, TaskSpec], durations: dict[str, float]) -> float:
        memo: dict[str, float] = {}

        def longest_to(name: str) -> float:
            if name in memo:
                return memo[name]
            dependencies = task_map[name].dependencies
            own = durations.get(name, 0.0)
            if not dependencies:
                memo[name] = own
                return own
            longest = own + max(longest_to(dep) for dep in dependencies)
            memo[name] = longest
            return longest

        return max((longest_to(name) for name in task_map), default=0.0)
