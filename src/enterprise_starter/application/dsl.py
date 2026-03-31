from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from enterprise_starter.domain.models import RetryPolicy, TaskSpec, WorkflowSpec

HandlerFactory = Callable[[dict[str, Any]], Callable[[Any], Awaitable[Any]]]


@dataclass(slots=True)
class OperationRegistry:
    _builders: dict[str, HandlerFactory] = field(default_factory=dict)

    def register(self, name: str, builder: HandlerFactory) -> None:
        if name in self._builders:
            raise ValueError(f"operation already registered: {name}")
        self._builders[name] = builder

    def build(self, name: str, params: dict[str, Any]) -> Callable[[Any], Awaitable[Any]]:
        try:
            builder = self._builders[name]
        except KeyError as exc:
            raise KeyError(f"unknown operation: {name}") from exc
        return builder(params)


class JsonWorkflowCompiler:
    def __init__(self, operations: OperationRegistry) -> None:
        self._operations = operations

    def compile_dict(self, payload: dict[str, Any]) -> WorkflowSpec:
        tasks: list[TaskSpec] = []
        for raw_task in payload["tasks"]:
            retry = raw_task.get("retry", {})
            tasks.append(
                TaskSpec(
                    name=raw_task["name"],
                    handler=self._operations.build(raw_task["operation"], raw_task.get("params", {})),
                    dependencies=tuple(raw_task.get("dependencies", [])),
                    retry_policy=RetryPolicy(
                        max_attempts=int(retry.get("max_attempts", 1)),
                        backoff_seconds=float(retry.get("backoff_seconds", 0.0)),
                        exponential=bool(retry.get("exponential", False)),
                    ),
                    cache_ttl_seconds=raw_task.get("cache_ttl_seconds"),
                    metadata={"operation": raw_task["operation"]},
                )
            )
        return WorkflowSpec(
            name=payload["name"],
            tasks=tuple(tasks),
            max_concurrency=int(payload.get("max_concurrency", 4)),
            fail_fast=bool(payload.get("fail_fast", True)),
        )

    def compile_file(self, path: str | Path) -> WorkflowSpec:
        return self.compile_dict(json.loads(Path(path).read_text()))

