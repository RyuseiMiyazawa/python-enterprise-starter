from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.application.plugins import WorkflowRegistry
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore
from enterprise_starter.interfaces.cli import build_registry, demo_workflow


class PersistenceAndPluginTests(unittest.IsolatedAsyncioTestCase):
    async def test_sqlite_store_persists_run_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            result = await WorkflowEngine(run_store=store).run(demo_workflow())

            runs = store.list_runs(limit=5)

        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].run_id, result.summary.run_id)
        self.assertEqual(runs[0].workflow_name, "prime-analytics")

    async def test_registry_loads_plugin_module(self) -> None:
        registry = WorkflowRegistry()
        registry.register("prime-analytics", demo_workflow)
        registry.load_from_module("enterprise_starter.plugins.builtin")

        workflow = registry.create("advanced-analytics")

        self.assertEqual(workflow.name, "advanced-analytics")
        self.assertIn("advanced-analytics", registry.names())

    async def test_builtin_registry_contains_plugin_workflows(self) -> None:
        registry = build_registry()
        self.assertEqual(registry.names(), ("advanced-analytics", "prime-analytics"))
