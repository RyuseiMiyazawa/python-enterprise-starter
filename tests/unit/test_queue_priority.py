from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore


class QueuePriorityTests(unittest.TestCase):
    def test_claim_prefers_lower_priority_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            low = store.enqueue_run("a", {"workflow_name": "a"}, priority=200)
            high = store.enqueue_run("b", {"workflow_name": "b"}, priority=10)

            claimed = store.claim_next_run("worker-x", lease_seconds=30)

        assert claimed is not None
        self.assertEqual(claimed["job_id"], high["job_id"])
        self.assertNotEqual(claimed["job_id"], low["job_id"])

    def test_claim_respects_scheduled_at(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
            store.enqueue_run("later", {"workflow_name": "later"}, priority=1, scheduled_at_utc=future)

            claimed = store.claim_next_run("worker-x", lease_seconds=30)

        self.assertIsNone(claimed)

    def test_aging_allows_older_job_to_overtake_newer_one(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            old_job = store.enqueue_run("old", {"workflow_name": "old"}, priority=50)
            store.enqueue_run("new", {"workflow_name": "new"}, priority=5)
            old_created = datetime.now(timezone.utc) - timedelta(hours=24)

            with store._connect() as connection:  # type: ignore[attr-defined]
                connection.execute(
                    "UPDATE queued_runs SET created_at_utc = ?, updated_at_utc = ? WHERE job_id = ?",
                    (old_created.isoformat(), old_created.isoformat(), old_job["job_id"]),
                )
                connection.commit()

            claimed = store.claim_next_run("worker-x", lease_seconds=30)

        assert claimed is not None
        self.assertEqual(claimed["job_id"], old_job["job_id"])
