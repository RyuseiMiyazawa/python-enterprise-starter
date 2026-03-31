from __future__ import annotations

import asyncio
import unittest

from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.domain.exceptions import WorkflowValidationError
from enterprise_starter.domain.models import ExecutionContext, RetryPolicy, TaskSpec, WorkflowSpec


class WorkflowEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_executes_dag_and_produces_outputs(self) -> None:
        async def source(_: ExecutionContext) -> int:
            await asyncio.sleep(0.01)
            return 4

        async def double(context: ExecutionContext) -> int:
            return context.inputs["source"] * 2

        async def merge(context: ExecutionContext) -> int:
            return context.inputs["source"] + context.inputs["double"]

        workflow = WorkflowSpec(
            name="dag",
            tasks=(
                TaskSpec(name="source", handler=source),
                TaskSpec(name="double", handler=double, dependencies=("source",)),
                TaskSpec(name="merge", handler=merge, dependencies=("source", "double")),
            ),
        )

        result = await WorkflowEngine().run(workflow)

        self.assertEqual(result.outputs["merge"], 12)
        self.assertEqual(result.summary.failed, ())
        self.assertEqual(result.records["merge"].attempts, 1)

    async def test_retries_then_succeeds(self) -> None:
        attempts = {"count": 0}

        async def flaky(_: ExecutionContext) -> str:
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("transient")
            return "ok"

        workflow = WorkflowSpec(
            name="retry",
            tasks=(
                TaskSpec(
                    name="flaky",
                    handler=flaky,
                    retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.001),
                ),
            ),
        )

        result = await WorkflowEngine().run(workflow)

        self.assertEqual(result.outputs["flaky"], "ok")
        self.assertEqual(result.records["flaky"].attempts, 2)

    async def test_cache_short_circuits_second_run(self) -> None:
        calls = {"count": 0}

        async def expensive(_: ExecutionContext) -> int:
            calls["count"] += 1
            return 99

        workflow = WorkflowSpec(
            name="cache",
            tasks=(TaskSpec(name="expensive", handler=expensive, cache_ttl_seconds=30.0),),
        )

        engine = WorkflowEngine()
        first = await engine.run(workflow)
        second = await engine.run(workflow)

        self.assertEqual(first.outputs["expensive"], 99)
        self.assertEqual(second.outputs["expensive"], 99)
        self.assertEqual(calls["count"], 1)
        self.assertEqual(second.summary.cached, ("expensive",))

    async def test_failure_skips_downstream_tasks(self) -> None:
        async def explode(_: ExecutionContext) -> int:
            raise ValueError("boom")

        async def never(_: ExecutionContext) -> int:
            return 1

        workflow = WorkflowSpec(
            name="failure",
            tasks=(
                TaskSpec(name="explode", handler=explode),
                TaskSpec(name="never", handler=never, dependencies=("explode",)),
            ),
        )

        result = await WorkflowEngine().run(workflow)

        self.assertEqual(result.summary.failed, ("explode",))
        self.assertEqual(result.summary.skipped, ("never",))

    async def test_detects_cycles(self) -> None:
        async def noop(_: ExecutionContext) -> None:
            return None

        workflow = WorkflowSpec(
            name="cycle",
            tasks=(
                TaskSpec(name="a", handler=noop, dependencies=("b",)),
                TaskSpec(name="b", handler=noop, dependencies=("a",)),
            ),
        )

        with self.assertRaises(WorkflowValidationError):
            await WorkflowEngine().run(workflow)

