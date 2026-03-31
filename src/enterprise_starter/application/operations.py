from __future__ import annotations

from statistics import fmean, median, pstdev
from string import Template
from typing import Any

from enterprise_starter.application.dsl import OperationRegistry
from enterprise_starter.domain.models import ExecutionContext


def build_operation_registry() -> OperationRegistry:
    registry = OperationRegistry()

    def const_builder(params: dict[str, Any]):
        value = params["value"]

        async def handler(_: ExecutionContext) -> Any:
            return value

        return handler

    def add_builder(params: dict[str, Any]):
        left = params["left"]
        right = params["right"]

        async def handler(context: ExecutionContext) -> Any:
            return _resolve_operand(left, context) + _resolve_operand(right, context)

        return handler

    def multiply_builder(params: dict[str, Any]):
        left = params["left"]
        right = params["right"]

        async def handler(context: ExecutionContext) -> Any:
            return _resolve_operand(left, context) * _resolve_operand(right, context)

        return handler

    def map_square_builder(_: dict[str, Any]):
        async def handler(context: ExecutionContext) -> list[float]:
            values = _single_dependency_input(context)
            return [value * value for value in values]

        return handler

    def reduce_sum_builder(_: dict[str, Any]):
        async def handler(context: ExecutionContext) -> float:
            values = _single_dependency_input(context)
            return float(sum(values))

        return handler

    def stats_builder(_: dict[str, Any]):
        async def handler(context: ExecutionContext) -> dict[str, float]:
            values = _single_dependency_input(context)
            return {
                "count": float(len(values)),
                "mean": float(fmean(values)),
                "median": float(median(values)),
                "pstdev": float(pstdev(values)),
            }

        return handler

    def format_template_builder(params: dict[str, Any]):
        template = Template(params["template"])

        async def handler(context: ExecutionContext) -> str:
            flattened: dict[str, Any] = {}
            for dependency_name, dependency_value in context.inputs.items():
                if isinstance(dependency_value, dict):
                    for key, value in dependency_value.items():
                        flattened[f"{dependency_name}_{key}"] = value
                flattened[dependency_name] = dependency_value
            return template.safe_substitute(flattened)

        return handler

    registry.register("const", const_builder)
    registry.register("add", add_builder)
    registry.register("multiply", multiply_builder)
    registry.register("map_square", map_square_builder)
    registry.register("reduce_sum", reduce_sum_builder)
    registry.register("stats", stats_builder)
    registry.register("format_template", format_template_builder)
    return registry


def _resolve_operand(value: Any, context: ExecutionContext) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return context.inputs[value[1:]]
    return value


def _single_dependency_input(context: ExecutionContext) -> Any:
    if len(context.inputs) != 1:
        raise ValueError("operation requires exactly one dependency")
    return next(iter(context.inputs.values()))

