from __future__ import annotations

import asyncio
import statistics

from enterprise_starter.application.plugins import WorkflowRegistry
from enterprise_starter.domain.events import WorkflowEvent
from enterprise_starter.domain.models import ExecutionContext, RetryPolicy, TaskSpec, WorkflowSpec


async def produce_seed(context: ExecutionContext) -> list[int]:
    seed = [4, 8, 15, 16, 23, 42]
    await context.emit(WorkflowEvent(kind="plugin_debug", task_name=context.task_name, payload={"seed": seed}))
    return seed


async def normalize(context: ExecutionContext) -> list[float]:
    values = context.inputs["produce_seed"]
    total = sum(values)
    await asyncio.sleep(0.01)
    return [value / total for value in values]


async def rolling_entropy(context: ExecutionContext) -> float:
    probabilities = context.inputs["normalize"]
    await asyncio.sleep(0.01)
    return -sum(p * 0 if p == 0 else p * __import__("math").log2(p) for p in probabilities)


async def spread_metrics(context: ExecutionContext) -> dict[str, float]:
    values = context.inputs["produce_seed"]
    await asyncio.sleep(0.01)
    return {
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "pstdev": statistics.pstdev(values),
    }


async def synthesize(context: ExecutionContext) -> dict[str, float]:
    return {
        "entropy": context.inputs["rolling_entropy"],
        **context.inputs["spread_metrics"],
    }


def advanced_analytics_workflow() -> WorkflowSpec:
    return WorkflowSpec(
        name="advanced-analytics",
        max_concurrency=4,
        tasks=(
            TaskSpec(name="produce_seed", handler=produce_seed, cache_ttl_seconds=30.0),
            TaskSpec(name="normalize", handler=normalize, dependencies=("produce_seed",)),
            TaskSpec(
                name="rolling_entropy",
                handler=rolling_entropy,
                dependencies=("normalize",),
                retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.005),
            ),
            TaskSpec(name="spread_metrics", handler=spread_metrics, dependencies=("produce_seed",)),
            TaskSpec(
                name="synthesize",
                handler=synthesize,
                dependencies=("rolling_entropy", "spread_metrics"),
            ),
        ),
    )


def register_workflows(registry: WorkflowRegistry) -> None:
    registry.register("advanced-analytics", advanced_analytics_workflow)
