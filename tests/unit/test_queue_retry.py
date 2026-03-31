from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from enterprise_starter.application.plugins import WorkflowRegistry
from enterprise_starter.application.worker import QueueWorker
from enterprise_starter.domain.models import ExecutionContext, TaskSpec, WorkflowSpec
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore


class QueueRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_job_becomes_retryable_then_dead_letter(self) -> None:
        async def explode(_: ExecutionContext) -> int:
            raise RuntimeError("boom")

        registry = WorkflowRegistry()
        registry.register(
            "always-fail",
            lambda: WorkflowSpec(name="always-fail", tasks=(TaskSpec(name="explode", handler=explode),)),
        )

        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            job = store.enqueue_run("always-fail", {"workflow_name": "always-fail"}, max_attempts=2)
            worker = QueueWorker(
                worker_id="retry-worker",
                store=store,
                registry=registry,
                lease_seconds=1,
                retry_backoff_seconds=0,
            )

            first = await worker.run_once()
            second = await worker.run_once()
            persisted = store.get_queue_job(job["job_id"])

        assert first is not None
        assert second is not None
        assert persisted is not None
        self.assertEqual(first["status"], "retryable")
        self.assertEqual(second["status"], "dead_letter")
        self.assertEqual(persisted["dead_letter_reason"], "max_attempts_exceeded")
