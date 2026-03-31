from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Callable

from enterprise_starter.domain.models import WorkflowSpec

Factory = Callable[[], WorkflowSpec]


@dataclass(slots=True)
class WorkflowRegistry:
    _factories: dict[str, Factory] = field(default_factory=dict)

    def register(self, name: str, factory: Factory) -> None:
        if name in self._factories:
            raise ValueError(f"workflow already registered: {name}")
        self._factories[name] = factory

    def create(self, name: str) -> WorkflowSpec:
        try:
            factory = self._factories[name]
        except KeyError as exc:
            raise KeyError(f"unknown workflow: {name}") from exc
        return factory()

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))

    def load_from_module(self, module_name: str) -> None:
        module = importlib.import_module(module_name)
        register = getattr(module, "register_workflows", None)
        if register is None:
            raise ValueError(f"module {module_name} does not define register_workflows(registry)")
        register(self)
