class SchedulerError(Exception):
    """Base class for scheduler domain errors."""


class TaskNotFoundError(SchedulerError):
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"Task '{task_id}' was not found")


class InvalidTaskOperationError(SchedulerError):
    """Raised when an operation is not valid for the current task state."""


class DuplicateTaskError(SchedulerError):
    """Raised when a task identifier already exists."""


class PersistenceError(SchedulerError):
    """Raised when scheduler state cannot be loaded or saved safely."""
