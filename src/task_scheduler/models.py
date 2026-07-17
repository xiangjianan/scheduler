from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class TaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ExecutionMode(StrEnum):
    PARALLEL = "parallel"
    SERIAL = "serial"
    EXCLUSIVE = "exclusive"


TERMINAL_STATUSES = {
    TaskStatus.CANCELLED,
    TaskStatus.SUCCEEDED,
    TaskStatus.FAILED,
}

SYSTEM_OWNED_FIELDS = {
    "status",
    "queue_sequence",
    "created_at",
    "started_at",
    "finished_at",
    "attempt",
    "result",
    "error",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def task_sort_key(task: dict[str, Any]) -> tuple[int, int, int]:
    return (
        1 if task["run_last"] else 0,
        int(task["priority"]),
        int(task["queue_sequence"]),
    )
