from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from pprint import pprint

from enterprise_starter.application.dsl import JsonWorkflowCompiler
from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.application.operations import build_operation_registry
from enterprise_starter.application.plugins import WorkflowRegistry
from enterprise_starter.application.worker import QueueWorker
from enterprise_starter.domain.events import WorkflowEvent
from enterprise_starter.domain.models import ExecutionContext, RetryPolicy, TaskSpec, WorkflowSpec
from enterprise_starter.infrastructure.live_events import LiveEventHub
from enterprise_starter.infrastructure.persistence import SQLiteWorkflowRunStore
from enterprise_starter.interfaces.http_api import create_server


async def fetch_numbers(context: ExecutionContext) -> list[int]:
    await asyncio.sleep(0.05)
    numbers = [2, 3, 5, 7, 11]
    await context.emit(WorkflowEvent(kind="debug", task_name=context.task_name, payload={"numbers": numbers}))
    return numbers


async def compute_squares(context: ExecutionContext) -> list[int]:
    await asyncio.sleep(0.02)
    numbers = context.inputs["fetch_numbers"]
    return [value * value for value in numbers]


async def aggregate_stats(context: ExecutionContext) -> dict[str, float]:
    await asyncio.sleep(0.02)
    squares = context.inputs["compute_squares"]
    return {
        "count": float(len(squares)),
        "sum": float(sum(squares)),
        "mean": float(sum(squares) / len(squares)),
    }


async def build_report(context: ExecutionContext) -> str:
    stats = context.inputs["aggregate_stats"]
    return (
        f"Workflow={context.workflow_name} count={stats['count']:.0f} "
        f"sum={stats['sum']:.0f} mean={stats['mean']:.2f}"
    )


def demo_workflow() -> WorkflowSpec:
    return WorkflowSpec(
        name="prime-analytics",
        max_concurrency=3,
        tasks=(
            TaskSpec(name="fetch_numbers", handler=fetch_numbers, cache_ttl_seconds=60.0),
            TaskSpec(name="compute_squares", handler=compute_squares, dependencies=("fetch_numbers",)),
            TaskSpec(
                name="aggregate_stats",
                handler=aggregate_stats,
                dependencies=("compute_squares",),
                retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.01),
            ),
            TaskSpec(name="build_report", handler=build_report, dependencies=("aggregate_stats",)),
        ),
    )


def build_registry() -> WorkflowRegistry:
    registry = WorkflowRegistry()
    registry.register("prime-analytics", demo_workflow)
    registry.load_from_module("enterprise_starter.plugins.builtin")
    return registry


def default_store() -> SQLiteWorkflowRunStore:
    return SQLiteWorkflowRunStore(Path(".runtime/workflow_runs.sqlite3"))


def default_event_hub() -> LiveEventHub:
    return LiveEventHub()


async def _run_workflow(workflow_name: str) -> None:
    registry = build_registry()
    engine = WorkflowEngine(run_store=default_store())
    result = await engine.run(registry.create(workflow_name))
    _print_result(result.outputs, result.summary, result.events)


async def _run_workflow_spec(workflow: WorkflowSpec) -> None:
    engine = WorkflowEngine(run_store=default_store())
    result = await engine.run(workflow)
    _print_result(result.outputs, result.summary, result.events)


def _print_result(outputs: object, summary: object, events: object) -> None:
    print("Outputs:")
    pprint(outputs)
    print("\nSummary:")
    pprint(summary)
    print("\nEvents:")
    for event in events:
        print(f"- {event.kind}: {event.task_name} {event.payload}")


def _print_history(limit: int) -> None:
    for run in default_store().list_runs(limit=limit):
        print(
            f"- run_id={run.run_id} workflow={run.workflow_name} status={run.status} "
            f"wall={run.total_wall_time_seconds:.4f}s critical={run.critical_path_seconds:.4f}s"
        )


def _print_workflows() -> None:
    for name in build_registry().names():
        print(f"- {name}")


def _run_dsl(path: str) -> None:
    compiler = JsonWorkflowCompiler(build_operation_registry())
    asyncio.run(_run_workflow_spec(compiler.compile_file(path)))


def _serve(host: str, port: int) -> None:
    server = create_server(host, port, build_registry(), default_store(), event_hub=default_event_hub())
    print(f"listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the advanced workflow demo.")
    subparsers = parser.add_subparsers(dest="command", required=False)

    run_parser = subparsers.add_parser("run", help="Run a registered workflow.")
    run_parser.add_argument("workflow", nargs="?", default="prime-analytics")

    history_parser = subparsers.add_parser("history", help="Show persisted run history.")
    history_parser.add_argument("--limit", type=int, default=10)

    subparsers.add_parser("workflows", help="List registered workflows.")

    dsl_parser = subparsers.add_parser("dsl-run", help="Run a workflow defined in JSON DSL.")
    dsl_parser.add_argument("path")

    serve_parser = subparsers.add_parser("serve", help="Start the HTTP control plane.")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8080)
    serve_parser.add_argument("--api-token", default=None)

    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue a registered workflow.")
    enqueue_parser.add_argument("workflow")
    enqueue_parser.add_argument("--priority", type=int, default=100)
    enqueue_parser.add_argument("--scheduled-at-utc", default=None)

    work_parser = subparsers.add_parser("work", help="Process one queued workflow as a worker.")
    work_parser.add_argument("--worker-id", default="cli-worker")
    work_parser.add_argument("--lease-seconds", type=int, default=30)

    cancel_parser = subparsers.add_parser("cancel", help="Cancel a queued job.")
    cancel_parser.add_argument("job_id")
    cancel_parser.add_argument("--reason", default="cancelled_by_user")

    args = parser.parse_args()
    command = args.command or "run"

    if command == "run":
        asyncio.run(_run_workflow(args.workflow))
        return
    if command == "history":
        _print_history(args.limit)
        return
    if command == "workflows":
        _print_workflows()
        return
    if command == "dsl-run":
        _run_dsl(args.path)
        return
    if command == "serve":
        server = create_server(
            args.host,
            args.port,
            build_registry(),
            default_store(),
            api_token=args.api_token,
            event_hub=default_event_hub(),
        )
        print(f"listening on http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return
    if command == "enqueue":
        try:
            build_registry().create(args.workflow)
        except KeyError as exc:
            raise ValueError(str(exc)) from exc
        job = default_store().enqueue_run(
            args.workflow,
            {"workflow_name": args.workflow},
            priority=args.priority,
            scheduled_at_utc=args.scheduled_at_utc,
        )
        pprint(job)
        return
    if command == "work":
        worker = QueueWorker(
            worker_id=args.worker_id,
            store=default_store(),
            registry=build_registry(),
            event_hub=default_event_hub(),
            lease_seconds=args.lease_seconds,
        )
        pprint(asyncio.run(worker.run_once()))
        return
    if command == "cancel":
        pprint(default_store().cancel_job(args.job_id, args.reason))
        return
    raise ValueError(f"unknown command: {command}")
