from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from enterprise_starter.interfaces.cli import build_registry
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore
from enterprise_starter.interfaces.http_api import ControlPlaneApp


class HttpApiIntegrationTests(unittest.TestCase):
    def test_control_plane_serves_workflows_and_runs_dsl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            app = ControlPlaneApp(build_registry(), store)
            _, workflows_payload, _ = app.handle_get("/workflows")
            _, run_payload, _ = app.handle_post(
                "/dsl/runs",
                {
                    "name": "api-dsl",
                    "tasks": [
                        {"name": "seed", "operation": "const", "params": {"value": [1, 2, 3]}},
                        {"name": "squares", "operation": "map_square", "dependencies": ["seed"]},
                        {
                            "name": "sum_squares",
                            "operation": "reduce_sum",
                            "dependencies": ["squares"],
                        },
                    ],
                },
            )
            _, history_payload, _ = app.handle_get("/runs?limit=5")

        self.assertIn("advanced-analytics", workflows_payload["workflows"])
        self.assertEqual(run_payload["outputs"]["sum_squares"], 14.0)
        self.assertEqual(history_payload["runs"][0]["workflow_name"], "api-dsl")

    def test_control_plane_enforces_bearer_token_and_serves_trace_and_ui(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            app = ControlPlaneApp(build_registry(), store, api_token="secret")

            status, payload, _ = app.handle_get("/workflows")
            self.assertEqual(status.value, 401)
            self.assertEqual(payload["error"], "unauthorized")

            _, run_payload, _ = app.handle_post(
                "/runs/prime-analytics",
                {},
                headers={"Authorization": "Bearer secret"},
            )
            run_id = run_payload["run_id"]
            trace_status, trace_payload, _ = app.handle_get(
                f"/traces/{run_id}",
                headers={"Authorization": "Bearer secret"},
            )
            ui_status, ui_payload, ui_type = app.handle_get(
                "/ui",
                headers={"Authorization": "Bearer secret"},
            )

        self.assertEqual(trace_status.value, 200)
        self.assertEqual(trace_payload["trace_id"], run_id)
        self.assertEqual(ui_status.value, 200)
        self.assertEqual(ui_type, "text/html; charset=utf-8")
        self.assertIn("Workflow Control Plane", ui_payload)

    def test_control_plane_queue_endpoints_manage_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            app = ControlPlaneApp(build_registry(), store)
            enqueue_status, enqueue_payload, _ = app.handle_post("/queue/prime-analytics", {})
            job_id = enqueue_payload["job"]["job_id"]
            _, queue_payload, _ = app.handle_get("/queue?limit=5")
            _, claim_payload, _ = app.handle_post(
                "/queue/claim",
                {"worker_id": "worker-1", "lease_seconds": 5},
            )
            _, heartbeat_payload, _ = app.handle_post(
                "/queue/heartbeat",
                {"job_id": job_id, "worker_id": "worker-1", "lease_seconds": 5},
            )
            _, complete_payload, _ = app.handle_post(
                "/queue/complete",
                {"job_id": job_id, "worker_id": "worker-1", "status": "succeeded", "run_id": "r-1"},
            )

        self.assertEqual(enqueue_status.value, 202)
        self.assertEqual(queue_payload["jobs"][0]["job_id"], job_id)
        self.assertEqual(claim_payload["job"]["job_id"], job_id)
        self.assertEqual(heartbeat_payload["job"]["job_id"], job_id)
        self.assertEqual(complete_payload["job"]["status"], "succeeded")

    def test_control_plane_exposes_openapi_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            app = ControlPlaneApp(build_registry(), store)
            status, payload, content_type = app.handle_get("/openapi.json")

        self.assertEqual(status.value, 200)
        self.assertEqual(content_type, "application/json")
        self.assertIn("/queue/process", payload["paths"])

    def test_queue_enqueue_accepts_priority_and_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            app = ControlPlaneApp(build_registry(), store)
            status, payload, _ = app.handle_post(
                "/queue/prime-analytics",
                {"priority": 7, "scheduled_at_utc": "2099-01-01T00:00:00+00:00"},
            )

        self.assertEqual(status.value, 202)
        self.assertEqual(payload["job"]["priority"], 7)
        self.assertEqual(payload["job"]["scheduled_at_utc"], "2099-01-01T00:00:00+00:00")

    def test_control_plane_can_cancel_job(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteWorkflowRunStore(Path(directory) / "runs.sqlite3")
            app = ControlPlaneApp(build_registry(), store)
            _, enqueue_payload, _ = app.handle_post("/queue/prime-analytics", {})
            job_id = enqueue_payload["job"]["job_id"]
            status, payload, _ = app.handle_post(
                "/queue/cancel",
                {"job_id": job_id, "reason": "user_stop"},
            )

        self.assertEqual(status.value, 200)
        self.assertEqual(payload["job"]["status"], "cancelled")
        self.assertEqual(payload["job"]["last_error"], "user_stop")
