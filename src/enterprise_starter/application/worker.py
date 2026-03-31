from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from enterprise_starter.application.dsl import JsonWorkflowCompiler
from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.application.operations import build_operation_registry
from enterprise_starter.application.plugins import WorkflowRegistry
from enterprise_starter.domain.models import WorkflowSpec
from enterprise_starter.infrastructure.live_events import LiveEventHub
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore


WorkflowResolver = Callable[[str, dict[str, Any]], WorkflowSpec]


@dataclass(slots=True)
class QueueWorker:
    worker_id: str
    store: SQLiteWorkflowRunStore
    registry: WorkflowRegistry
    event_hub: LiveEventHub | None = None
    lease_seconds: int = 30
    retry_backoff_seconds: int = 5

    async def run_once(self) -> dict[str, Any] | None:
        job = await asyncio.to_thread(self.store.claim_next_run, self.worker_id, self.lease_seconds)
        if job is None:
            return None
        self._publish({"kind": "job_claimed", "job_id": job["job_id"], "worker_id": self.worker_id})
        try:
            workflow = self._resolve_workflow(job["workflow_name"], job["payload"])
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(job["job_id"]))
            try:
                result = await WorkflowEngine(run_store=self.store).run(workflow)
            finally:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if result.summary.failed:
                failed = await asyncio.to_thread(
                    self.store.retry_or_dead_letter_job,
                    job["job_id"],
                    self.worker_id,
                    f"workflow_failed:{','.join(result.summary.failed)}",
                    self.retry_backoff_seconds,
                )
                self._publish(
                    {
                        "kind": "job_failed",
                        "job_id": job["job_id"],
                        "worker_id": self.worker_id,
                        "run_id": result.summary.run_id,
                        "status": None if failed is None else failed["status"],
                    }
                )
                return failed
            completed = await asyncio.to_thread(
                self.store.complete_run_job,
                job["job_id"],
                self.worker_id,
                "succeeded",
                result.summary.run_id,
            )
            self._publish(
                {
                    "kind": "job_completed",
                    "job_id": job["job_id"],
                    "worker_id": self.worker_id,
                    "run_id": result.summary.run_id,
                    "status": completed["status"] if completed is not None else "unknown",
                }
            )
            return completed
        except Exception as exc:
            failed = await asyncio.to_thread(
                self.store.retry_or_dead_letter_job,
                job["job_id"],
                self.worker_id,
                repr(exc),
                self.retry_backoff_seconds,
            )
            self._publish(
                {
                    "kind": "job_failed",
                    "job_id": job["job_id"],
                    "worker_id": self.worker_id,
                    "status": None if failed is None else failed["status"],
                }
            )
            return failed

    async def _heartbeat_loop(self, job_id: str) -> None:
        while True:
            await asyncio.sleep(max(1, self.lease_seconds // 3))
            heartbeat = await asyncio.to_thread(
                self.store.heartbeat_run,
                job_id,
                self.worker_id,
                self.lease_seconds,
            )
            if heartbeat is None:
                return
            self._publish(
                {
                    "kind": "job_heartbeat",
                    "job_id": job_id,
                    "worker_id": self.worker_id,
                    "lease_expires_at_utc": heartbeat["lease_expires_at_utc"],
                }
            )

    def _resolve_workflow(self, workflow_name: str, payload: dict[str, Any]) -> WorkflowSpec:
        if workflow_name == "__dsl__":
            compiler = JsonWorkflowCompiler(build_operation_registry())
            return compiler.compile_dict(payload)
        return self.registry.create(workflow_name)

    def _publish(self, event: dict[str, Any]) -> None:
        if self.event_hub is not None:
            self.event_hub.publish(event)
