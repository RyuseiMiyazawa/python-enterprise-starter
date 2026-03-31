from __future__ import annotations

import unittest

from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.domain.models import ExecutionContext, TaskSpec, WorkflowSpec


class EventStreamIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_log_contains_lifecycle_markers(self) -> None:
        async def alpha(_: ExecutionContext) -> str:
            return "a"

        async def beta(context: ExecutionContext) -> str:
            return context.inputs["alpha"] + "b"

        workflow = WorkflowSpec(
            name="events",
            tasks=(
                TaskSpec(name="alpha", handler=alpha),
                TaskSpec(name="beta", handler=beta, dependencies=("alpha",)),
            ),
        )

        result = await WorkflowEngine().run(workflow)
        event_kinds = [event.kind for event in result.events]

        self.assertIn("task_started", event_kinds)
        self.assertIn("task_succeeded", event_kinds)
        self.assertEqual(result.outputs["beta"], "ab")

