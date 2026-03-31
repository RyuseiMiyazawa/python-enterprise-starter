from __future__ import annotations

import unittest

from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.interfaces.cli import demo_workflow


class DemoWorkflowE2ETests(unittest.IsolatedAsyncioTestCase):
    async def test_demo_workflow_builds_report(self) -> None:
        result = await WorkflowEngine().run(demo_workflow())

        self.assertIn("Workflow=prime-analytics", result.outputs["build_report"])
        self.assertGreater(result.summary.critical_path_seconds, 0.0)
