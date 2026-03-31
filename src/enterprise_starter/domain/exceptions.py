class WorkflowError(Exception):
    """Base workflow exception."""


class WorkflowValidationError(WorkflowError):
    """Raised when a workflow definition is invalid."""


class TaskExecutionError(WorkflowError):
    """Raised when a task exhausts retries."""

    def __init__(self, task_name: str, attempts: int, message: str) -> None:
        super().__init__(f"task={task_name} attempts={attempts}: {message}")
        self.task_name = task_name
        self.attempts = attempts
        self.message = message

