from __future__ import annotations

import asyncio
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Mapping
from urllib.parse import parse_qs, urlparse

from enterprise_starter.application.dsl import JsonWorkflowCompiler
from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.application.operations import build_operation_registry
from enterprise_starter.application.plugins import WorkflowRegistry
from enterprise_starter.application.worker import QueueWorker
from enterprise_starter.infrastructure.live_events import LiveEventHub
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore
from enterprise_starter.infrastructure.tracing import RunTraceExporter
from enterprise_starter.interfaces.web_ui import render_dashboard_html


def build_openapi_spec() -> dict[str, object]:
    return {
        "openapi": "3.1.0",
        "info": {"title": "Workflow Control Plane", "version": "0.1.0"},
        "paths": {
            "/workflows": {"get": {"summary": "List workflows"}},
            "/runs": {"get": {"summary": "List persisted runs"}},
            "/runs/{workflow_name}": {"post": {"summary": "Run workflow immediately"}},
            "/traces/{run_id}": {"get": {"summary": "Export reconstructed trace"}},
            "/queue": {"get": {"summary": "List queued jobs"}},
            "/queue/{workflow_name}": {"post": {"summary": "Enqueue registered workflow"}},
            "/queue/dsl": {"post": {"summary": "Enqueue DSL workflow"}},
            "/queue/process": {"post": {"summary": "Process next queued job"}},
            "/queue/cancel": {"post": {"summary": "Cancel queued or leased job"}},
            "/events": {"get": {"summary": "Server-sent event stream"}},
            "/ui": {"get": {"summary": "Static dashboard"}},
        },
    }


class ControlPlaneApp:
    def __init__(
        self,
        registry: WorkflowRegistry,
        store: SQLiteWorkflowRunStore,
        api_token: str | None = None,
        event_hub: LiveEventHub | None = None,
    ) -> None:
        self._registry = registry
        self._store = store
        self._api_token = api_token
        self._traces = RunTraceExporter()
        self._event_hub = event_hub or LiveEventHub()

    def handle_get(
        self,
        path: str,
        headers: Mapping[str, str] | None = None,
    ) -> tuple[HTTPStatus, dict[str, object] | str, str]:
        unauthorized = self._authorize(headers or {}, path)
        if unauthorized is not None:
            return unauthorized
        parsed = urlparse(path)
        if parsed.path == "/ui":
            return HTTPStatus.OK, render_dashboard_html(), "text/html; charset=utf-8"
        if parsed.path == "/openapi.json":
            return HTTPStatus.OK, build_openapi_spec(), "application/json"
        if parsed.path == "/events":
            return HTTPStatus.OK, {"stream": "sse"}, "text/event-stream"
        if parsed.path == "/workflows":
            return HTTPStatus.OK, {"workflows": list(self._registry.names())}, "application/json"
        if parsed.path == "/queue":
            query = parse_qs(parsed.query)
            limit = int(query.get("limit", ["20"])[0])
            return HTTPStatus.OK, {"jobs": self._store.list_queue_jobs(limit=limit)}, "application/json"
        if parsed.path == "/runs":
            query = parse_qs(parsed.query)
            limit = int(query.get("limit", ["10"])[0])
            runs = [
                {
                    "run_id": run.run_id,
                    "workflow_name": run.workflow_name,
                    "status": run.status,
                    "started_at_utc": run.started_at_utc,
                    "finished_at_utc": run.finished_at_utc,
                    "total_wall_time_seconds": run.total_wall_time_seconds,
                    "critical_path_seconds": run.critical_path_seconds,
                }
                for run in self._store.list_runs(limit=limit)
            ]
            return HTTPStatus.OK, {"runs": runs}, "application/json"
        if parsed.path.startswith("/traces/"):
            run_id = parsed.path.removeprefix("/traces/")
            run = self._store.get_run(run_id)
            if run is None:
                return HTTPStatus.NOT_FOUND, {"error": "run not found"}, "application/json"
            return HTTPStatus.OK, self._traces.export(run), "application/json"
        return HTTPStatus.NOT_FOUND, {"error": "not found"}, "application/json"

    def handle_post(
        self,
        path: str,
        payload: dict[str, object],
        headers: Mapping[str, str] | None = None,
    ) -> tuple[HTTPStatus, dict[str, object], str]:
        unauthorized = self._authorize(headers or {}, path)
        if unauthorized is not None:
            return unauthorized
        parsed = urlparse(path)
        if parsed.path == "/queue/claim":
            worker_id = str(payload.get("worker_id", "worker-default"))
            lease_seconds = int(payload.get("lease_seconds", 30))
            job = self._store.claim_next_run(worker_id, lease_seconds)
            if job is None:
                return HTTPStatus.OK, {"job": None}, "application/json"
            self._event_hub.publish({"kind": "job_claimed_api", "job_id": job["job_id"], "worker_id": worker_id})
            return HTTPStatus.OK, {"job": job}, "application/json"
        if parsed.path == "/queue/heartbeat":
            worker_id = str(payload.get("worker_id", "worker-default"))
            job_id = str(payload["job_id"])
            lease_seconds = int(payload.get("lease_seconds", 30))
            job = self._store.heartbeat_run(job_id, worker_id, lease_seconds)
            if job is None:
                return HTTPStatus.NOT_FOUND, {"error": "job not found"}, "application/json"
            self._event_hub.publish({"kind": "job_heartbeat_api", "job_id": job_id, "worker_id": worker_id})
            return HTTPStatus.OK, {"job": job}, "application/json"
        if parsed.path == "/queue/complete":
            worker_id = str(payload.get("worker_id", "worker-default"))
            job_id = str(payload["job_id"])
            status_value = str(payload.get("status", "succeeded"))
            run_id = payload.get("run_id")
            job = self._store.complete_run_job(job_id, worker_id, status_value, None if run_id is None else str(run_id))
            if job is None:
                return HTTPStatus.NOT_FOUND, {"error": "job not found"}, "application/json"
            self._event_hub.publish({"kind": "job_completed_api", "job_id": job_id, "worker_id": worker_id})
            return HTTPStatus.OK, {"job": job}, "application/json"
        if parsed.path == "/queue/cancel":
            job_id = str(payload["job_id"])
            reason = str(payload.get("reason", "cancelled_by_user"))
            job = self._store.cancel_job(job_id, reason)
            if job is None:
                return HTTPStatus.NOT_FOUND, {"error": "job not found"}, "application/json"
            self._event_hub.publish({"kind": "job_cancelled", "job_id": job_id, "reason": reason})
            return HTTPStatus.OK, {"job": job}, "application/json"
        if parsed.path == "/queue/process":
            worker_id = str(payload.get("worker_id", "worker-default"))
            lease_seconds = int(payload.get("lease_seconds", 30))
            retry_backoff_seconds = int(payload.get("retry_backoff_seconds", 5))
            worker = QueueWorker(
                worker_id=worker_id,
                store=self._store,
                registry=self._registry,
                event_hub=self._event_hub,
                lease_seconds=lease_seconds,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            job = asyncio.run(worker.run_once())
            return HTTPStatus.OK, {"job": job}, "application/json"
        if parsed.path.startswith("/queue/"):
            workflow_name = parsed.path.removeprefix("/queue/")
            try:
                self._registry.create(workflow_name)
            except KeyError as exc:
                return HTTPStatus.NOT_FOUND, {"error": str(exc)}, "application/json"
            job = self._store.enqueue_run(
                workflow_name,
                {"workflow_name": workflow_name},
                max_attempts=int(payload.get("max_attempts", 3)),
                priority=int(payload.get("priority", 100)),
                scheduled_at_utc=None if payload.get("scheduled_at_utc") is None else str(payload["scheduled_at_utc"]),
            )
            self._event_hub.publish({"kind": "job_enqueued", "job_id": job["job_id"], "workflow_name": workflow_name})
            return HTTPStatus.ACCEPTED, {"job": job}, "application/json"
        if parsed.path.startswith("/runs/"):
            workflow_name = parsed.path.removeprefix("/runs/")
            try:
                result = asyncio.run(
                    WorkflowEngine(run_store=self._store).run(self._registry.create(workflow_name))
                )
            except KeyError as exc:
                return HTTPStatus.NOT_FOUND, {"error": str(exc)}, "application/json"
            return HTTPStatus.CREATED, {
                "run_id": result.summary.run_id,
                "workflow_name": result.summary.workflow_name,
                "outputs": result.outputs,
            }, "application/json"
        if parsed.path == "/dsl/runs":
            compiler = JsonWorkflowCompiler(build_operation_registry())
            workflow = compiler.compile_dict(payload)
            result = asyncio.run(WorkflowEngine(run_store=self._store).run(workflow))
            return HTTPStatus.CREATED, {
                "run_id": result.summary.run_id,
                "workflow_name": result.summary.workflow_name,
                "outputs": result.outputs,
            }, "application/json"
        if parsed.path == "/queue/dsl":
            compiler = JsonWorkflowCompiler(build_operation_registry())
            workflow = compiler.compile_dict(payload)
            job = self._store.enqueue_run(
                "__dsl__",
                payload,
                max_attempts=int(payload.get("max_attempts", 3)),
                priority=int(payload.get("priority", 100)),
                scheduled_at_utc=None if payload.get("scheduled_at_utc") is None else str(payload["scheduled_at_utc"]),
            )
            self._event_hub.publish({"kind": "job_enqueued", "job_id": job["job_id"], "workflow_name": workflow.name})
            return HTTPStatus.ACCEPTED, {"job": job, "workflow_name": workflow.name}, "application/json"
        return HTTPStatus.NOT_FOUND, {"error": "not found"}, "application/json"

    def event_stream(self) -> object:
        return self._event_hub.stream()

    def _authorize(
        self,
        headers: Mapping[str, str],
        path: str | None = None,
    ) -> tuple[HTTPStatus, dict[str, object], str] | None:
        if self._api_token is None:
            return None
        if headers.get("Authorization") == f"Bearer {self._api_token}":
            return None
        if path is not None:
            parsed = urlparse(path)
            if parse_qs(parsed.query).get("access_token", [None])[0] == self._api_token:
                return None
        return HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"}, "application/json"


class ControlPlaneHandler(BaseHTTPRequestHandler):
    app: ControlPlaneApp

    def do_GET(self) -> None:  # noqa: N802
        status, payload, content_type = self.app.handle_get(self.path, dict(self.headers.items()))
        if content_type == "text/event-stream":
            self._write_sse(status)
            return
        self._write(status, payload, content_type)

    def do_POST(self) -> None:  # noqa: N802
        status, payload, content_type = self.app.handle_post(
            self.path,
            self._read_json_body(),
            dict(self.headers.items()),
        )
        self._write(status, payload, content_type)

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def _read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        return json.loads(raw_body.decode("utf-8"))

    def _write(self, status: HTTPStatus, payload: dict[str, object] | str, content_type: str) -> None:
        if content_type.startswith("application/json"):
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
        else:
            body = str(payload).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_sse(self, status: HTTPStatus) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        for chunk in self.app.event_stream():
            self.wfile.write(chunk.encode("utf-8"))
            self.wfile.flush()


def create_server(
    host: str,
    port: int,
    registry: WorkflowRegistry,
    store: SQLiteWorkflowRunStore,
    api_token: str | None = None,
    event_hub: LiveEventHub | None = None,
) -> ThreadingHTTPServer:
    app = ControlPlaneApp(registry, store, api_token=api_token, event_hub=event_hub)
    handler = type(
        "BoundControlPlaneHandler",
        (ControlPlaneHandler,),
        {"app": app},
    )
    return ThreadingHTTPServer((host, port), handler)
