from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from enterprise_starter.application.worker import QueueWorker
from enterprise_starter.infrastructure.live_events import LiveEventHub
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore
from enterprise_starter.interfaces.cli import build_registry


class QueueWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_claims_processes_and_completes_job(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            job = store.enqueue_run("prime-analytics", {"workflow_name": "prime-analytics"})
            worker = QueueWorker(
                worker_id="worker-a",
                store=store,
                registry=build_registry(),
                event_hub=LiveEventHub(),
                lease_seconds=2,
            )

            completed = await worker.run_once()
            persisted_job = store.get_queue_job(job["job_id"])

        assert completed is not None
        assert persisted_job is not None
        self.assertEqual(persisted_job["status"], "succeeded")
        self.assertIsNotNone(persisted_job["run_id"])
