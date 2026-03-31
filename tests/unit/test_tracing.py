from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore
from enterprise_starter.infrastructure.tracing import RunTraceExporter
from enterprise_starter.interfaces.cli import demo_workflow


class TracingTests(unittest.IsolatedAsyncioTestCase):
    async def test_exporter_builds_trace_from_persisted_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            result = await WorkflowEngine(run_store=store).run(demo_workflow())
            persisted = store.get_run(result.summary.run_id)

        assert persisted is not None
        trace = RunTraceExporter().export(persisted)

        self.assertEqual(trace["trace_id"], result.summary.run_id)
        self.assertEqual(trace["root_span"]["name"], "prime-analytics")
        self.assertTrue(any(span["name"] == "fetch_numbers" for span in trace["spans"]))
