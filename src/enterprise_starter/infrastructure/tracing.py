from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from enterprise_starter.domain.models import PersistedRun


@dataclass(slots=True, frozen=True)
class TraceSpan:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    started_at: str | None
    ended_at: str | None
    attributes: dict[str, Any]


class RunTraceExporter:
    def export(self, run: PersistedRun) -> dict[str, Any]:
        payload = json.loads(run.payload_json)
        records: dict[str, dict[str, Any]] = payload["records"]
        events: list[dict[str, Any]] = payload["events"]
        summary: dict[str, Any] = payload["summary"]
        trace_id = run.run_id

        event_by_task: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            event_by_task.setdefault(event["task_name"], []).append(event)

        spans = [
            TraceSpan(
                trace_id=trace_id,
                span_id=f"{trace_id}:{task_name}",
                parent_span_id=trace_id,
                name=task_name,
                started_at=self._find_event_timestamp(event_by_task.get(task_name, []), "task_started"),
                ended_at=self._find_end_timestamp(event_by_task.get(task_name, [])),
                attributes={
                    "state": record["state"],
                    "attempts": record["attempts"],
                    "error": record["error"],
                },
            )
            for task_name, record in records.items()
        ]
        return {
            "trace_id": trace_id,
            "root_span": {
                "trace_id": trace_id,
                "span_id": trace_id,
                "parent_span_id": None,
                "name": run.workflow_name,
                "started_at": run.started_at_utc,
                "ended_at": run.finished_at_utc,
                "attributes": {
                    "status": run.status,
                    "critical_path_seconds": run.critical_path_seconds,
                    "completed": summary["completed"],
                    "failed": summary["failed"],
                },
            },
            "spans": [asdict(span) for span in spans],
        }

    def _find_event_timestamp(self, events: list[dict[str, Any]], kind: str) -> str | None:
        for event in events:
            if event["kind"] == kind:
                return event["timestamp"]
        return None

    def _find_end_timestamp(self, events: list[dict[str, Any]]) -> str | None:
        for kind in ("task_succeeded", "task_failed", "task_exhausted", "task_cached"):
            timestamp = self._find_event_timestamp(events, kind)
            if timestamp is not None:
                return timestamp
        return None
