from __future__ import annotations

import unittest

from enterprise_starter.application.dsl import JsonWorkflowCompiler
from enterprise_starter.application.engine import WorkflowEngine
from enterprise_starter.application.operations import build_operation_registry


class JsonWorkflowCompilerTests(unittest.IsolatedAsyncioTestCase):
    async def test_compiles_and_runs_json_workflow(self) -> None:
        compiler = JsonWorkflowCompiler(build_operation_registry())
        workflow = compiler.compile_dict(
            {
                "name": "json-workflow",
                "tasks": [
                    {"name": "seed", "operation": "const", "params": {"value": [2, 4, 6]}},
                    {"name": "squares", "operation": "map_square", "dependencies": ["seed"]},
                    {"name": "sum_squares", "operation": "reduce_sum", "dependencies": ["squares"]},
                    {
                        "name": "report",
                        "operation": "format_template",
                        "dependencies": ["sum_squares"],
                        "params": {"template": "total=${sum_squares}"},
                    },
                ],
            }
        )

        result = await WorkflowEngine().run(workflow)

        self.assertEqual(result.outputs["sum_squares"], 56.0)
        self.assertEqual(result.outputs["report"], "total=56.0")
