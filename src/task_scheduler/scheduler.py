import asyncio
import copy
import inspect
from collections import Counter
from collections.abc import Mapping
from contextlib import suppress
from typing import Any
from uuid import uuid4

from .config import SchedulerConfig
from .exceptions import InvalidTaskOperationError, PersistenceError, TaskNotFoundError
from .executor import ExecutionContext, TaskCancelled, TaskExecutor
from .models import (
    SYSTEM_OWNED_FIELDS,
    TERMINAL_STATUSES,
    ExecutionMode,
    TaskStatus,
    task_sort_key,
    utc_now,
)
from .persistence import SCHEMA_VERSION, StateStore


class Scheduler:
    """In-memory scheduling core with a platform-defined task executor."""

    def __init__(
        self,
        executor: TaskExecutor,
        *,
        max_concurrency: int = 4,
        config: SchedulerConfig | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        self._config = config or SchedulerConfig(max_concurrency=max_concurrency)
        self._executor = executor
        self._state_store = state_store
        self._tasks: dict[str, dict[str, Any]] = {}
        self._next_sequence = 1
        self._condition = asyncio.Condition()
        self._running: dict[str, tuple[asyncio.Task[None], ExecutionContext]] = {}
        self._dispatcher: asyncio.Task[None] | None = None
        self._flusher: asyncio.Task[None] | None = None
        self._started = False
        self._stopping = False
        self._revision = 0
        self._saved_revision = 0
        self._loaded = False

    @property
    def max_concurrency(self) -> int:
        return self._config.max_concurrency

    async def create_task(self, data: Mapping[str, Any]) -> dict[str, Any]:
        incoming = copy.deepcopy(dict(data))
        invalid_fields = SYSTEM_OWNED_FIELDS.intersection(incoming)
        if "id" in incoming:
            invalid_fields.add("id")
        if invalid_fields:
            names = ", ".join(sorted(invalid_fields))
            raise InvalidTaskOperationError(f"Scheduler-owned fields cannot be supplied: {names}")

        priority = self._validate_priority(incoming.pop("priority", 100))
        run_last = incoming.pop("run_last", False)
        if not isinstance(run_last, bool):
            raise InvalidTaskOperationError("run_last must be a boolean")

        try:
            execution_mode = ExecutionMode(incoming.pop("execution_mode", ExecutionMode.PARALLEL))
        except ValueError as exc:
            raise InvalidTaskOperationError("execution_mode is invalid") from exc
        serial_group = incoming.pop("serial_group", None)
        if execution_mode == ExecutionMode.SERIAL and not serial_group:
            raise InvalidTaskOperationError("serial tasks require a non-empty serial_group")
        if serial_group is not None and not isinstance(serial_group, str):
            raise InvalidTaskOperationError("serial_group must be a string or null")

        async with self._condition:
            task_id = str(uuid4())
            created_at = utc_now()
            task: dict[str, Any] = {
                **incoming,
                "id": task_id,
                "status": TaskStatus.QUEUED,
                "priority": priority,
                "run_last": run_last,
                "queue_sequence": self._take_sequence_locked(),
                "execution_mode": execution_mode,
                "serial_group": serial_group,
                "created_at": created_at,
                "started_at": None,
                "finished_at": None,
                "attempt": 0,
                "result": None,
                "error": None,
            }
            self._tasks[task_id] = task
            self._changed_locked()
            return copy.deepcopy(task)

    async def get_task(self, task_id: str) -> dict[str, Any]:
        async with self._condition:
            return copy.deepcopy(self._get_task_locked(task_id))

    async def list_tasks(self, status: TaskStatus | str | None = None) -> list[dict[str, Any]]:
        if status is not None:
            try:
                status = TaskStatus(status)
            except ValueError as exc:
                raise InvalidTaskOperationError("status is invalid") from exc
        async with self._condition:
            selected = [
                task for task in self._tasks.values() if status is None or task["status"] == status
            ]
            selected.sort(key=self._list_sort_key)
            return copy.deepcopy(selected)

    async def update_task(
        self,
        task_id: str,
        *,
        priority: int | None = None,
        run_last: bool | None = None,
    ) -> dict[str, Any]:
        async with self._condition:
            task = self._get_task_locked(task_id)
            if task["status"] != TaskStatus.QUEUED:
                raise InvalidTaskOperationError("Only queued tasks can be reordered")
            if priority is not None:
                task["priority"] = self._validate_priority(priority)
            if run_last is not None:
                if not isinstance(run_last, bool):
                    raise InvalidTaskOperationError("run_last must be a boolean")
                task["run_last"] = run_last
            self._changed_locked()
            return copy.deepcopy(task)

    async def cancel_task(self, task_id: str) -> dict[str, Any]:
        async with self._condition:
            task = self._get_task_locked(task_id)
            status = TaskStatus(task["status"])
            if status == TaskStatus.QUEUED:
                task["status"] = TaskStatus.CANCELLED
                task["finished_at"] = utc_now()
                self._changed_locked()
            elif status == TaskStatus.RUNNING:
                task["status"] = TaskStatus.CANCELLING
                running = self._running.get(task_id)
                if running is not None:
                    running[1].cancel()
                self._changed_locked()
            elif status == TaskStatus.CANCELLING:
                pass
            elif status in TERMINAL_STATUSES:
                raise InvalidTaskOperationError(f"Cannot cancel a {status.value} task")
            return copy.deepcopy(task)

    async def retry_task(self, task_id: str) -> dict[str, Any]:
        async with self._condition:
            task = self._get_task_locked(task_id)
            if TaskStatus(task["status"]) not in {TaskStatus.FAILED, TaskStatus.CANCELLED}:
                raise InvalidTaskOperationError("Only failed or cancelled tasks can be retried")
            task.update(
                status=TaskStatus.QUEUED,
                queue_sequence=self._take_sequence_locked(),
                started_at=None,
                finished_at=None,
                result=None,
                error=None,
            )
            self._changed_locked()
            return copy.deepcopy(task)

    async def set_max_concurrency(self, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("max_concurrency must be positive")
        async with self._condition:
            self._config.max_concurrency = value
            self._changed_locked()
            return value

    async def start(self) -> None:
        """Start dispatching queued tasks."""
        await self._load_state_once()
        async with self._condition:
            if self._started:
                return
            self._started = True
            self._stopping = False
            self._dispatcher = asyncio.create_task(
                self._dispatch_loop(), name="task-scheduler-dispatcher"
            )
            if self._state_store is not None:
                self._flusher = asyncio.create_task(
                    self._flush_loop(), name="task-scheduler-json-flusher"
                )
            self._condition.notify_all()

    async def stop(self) -> None:
        """Stop dispatching and wait for already-running work to finish."""
        async with self._condition:
            dispatcher = None
            if self._started:
                self._stopping = True
                self._condition.notify_all()
                dispatcher = self._dispatcher
            flusher = self._flusher

        if flusher is not None:
            flusher.cancel()
            with suppress(asyncio.CancelledError):
                await flusher

        if dispatcher is not None:
            await dispatcher

        async with self._condition:
            running = [handle for handle, _ in self._running.values()]
        if running:
            await asyncio.gather(*running)

        async with self._condition:
            self._started = False
            self._dispatcher = None
            self._flusher = None
        await self.flush()

    async def wait_for_idle(self, *, timeout: float | None = None) -> None:
        """Wait until no queued or running tasks remain."""

        async def wait() -> None:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: (
                        not self._running
                        and not any(
                            task["status"] == TaskStatus.QUEUED for task in self._tasks.values()
                        )
                    )
                )

        if timeout is None:
            await wait()
        else:
            await asyncio.wait_for(wait(), timeout=timeout)

    async def flush(self) -> bool:
        """Persist one dirty snapshot, returning whether a write occurred."""
        if self._state_store is None:
            return False
        async with self._condition:
            if self._revision == self._saved_revision:
                return False
            revision = self._revision
            snapshot = {
                "schema_version": SCHEMA_VERSION,
                "saved_at": utc_now(),
                "next_queue_sequence": self._next_sequence,
                "config": {"max_concurrency": self._config.max_concurrency},
                "tasks": copy.deepcopy(self._tasks),
            }
        await self._state_store.save(snapshot)
        async with self._condition:
            if self._revision == revision:
                self._saved_revision = revision
        return True

    async def stats(self) -> dict[str, Any]:
        async with self._condition:
            counts = Counter(str(task["status"]) for task in self._tasks.values())
            return {
                "counts": dict(counts),
                "queued": counts[TaskStatus.QUEUED],
                "running": counts[TaskStatus.RUNNING] + counts[TaskStatus.CANCELLING],
                "max_concurrency": self._config.max_concurrency,
            }

    async def _dispatch_loop(self) -> None:
        while True:
            async with self._condition:
                task = self._select_next_locked()
                while task is None:
                    if self._stopping:
                        return
                    await self._condition.wait()
                    task = self._select_next_locked()

                context = ExecutionContext()
                task["status"] = TaskStatus.RUNNING
                task["started_at"] = utc_now()
                task["finished_at"] = None
                task["attempt"] += 1
                snapshot = copy.deepcopy(task)
                handle = asyncio.create_task(
                    self._execute_task(task["id"], snapshot, context),
                    name=f"task-scheduler-{task['id']}",
                )
                self._running[task["id"]] = (handle, context)
                self._changed_locked()

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.flush_interval)
            await self.flush()

    async def _load_state_once(self) -> None:
        if self._loaded:
            return
        snapshot = await self._state_store.load() if self._state_store is not None else None
        async with self._condition:
            if self._loaded:
                return
            if snapshot is not None:
                if self._tasks:
                    raise PersistenceError(
                        "Cannot load persisted state after tasks were created in memory"
                    )
                tasks = copy.deepcopy(snapshot["tasks"])
                recovered = False
                for task_id, task in tasks.items():
                    if not isinstance(task, dict) or task.get("id") != task_id:
                        raise PersistenceError("Persisted task identifiers are inconsistent")
                    try:
                        task["status"] = TaskStatus(task["status"])
                        task["execution_mode"] = ExecutionMode(task["execution_mode"])
                    except (KeyError, ValueError) as exc:
                        raise PersistenceError(f"Persisted task '{task_id}' is invalid") from exc
                    if task["status"] in {TaskStatus.RUNNING, TaskStatus.CANCELLING}:
                        task["status"] = TaskStatus.FAILED
                        task["finished_at"] = utc_now()
                        task["result"] = None
                        task["error"] = {
                            "type": "SchedulerInterrupted",
                            "message": "Scheduler stopped while the task was running",
                        }
                        recovered = True
                self._tasks = tasks
                highest_sequence = max(
                    (int(task["queue_sequence"]) for task in tasks.values()), default=0
                )
                self._next_sequence = max(
                    int(snapshot.get("next_queue_sequence", 1)), highest_sequence + 1
                )
                stored_max = snapshot.get("config", {}).get("max_concurrency")
                if isinstance(stored_max, int) and stored_max > 0:
                    self._config.max_concurrency = stored_max
                if recovered:
                    self._revision = 1
                    self._saved_revision = 0
            self._loaded = True

    def _select_next_locked(self) -> dict[str, Any] | None:
        if self._stopping or len(self._running) >= self._config.max_concurrency:
            return None

        running_tasks = [self._tasks[task_id] for task_id in self._running]
        if any(task["execution_mode"] == ExecutionMode.EXCLUSIVE for task in running_tasks):
            return None

        running_serial_groups = {
            task["serial_group"]
            for task in running_tasks
            if task["execution_mode"] == ExecutionMode.SERIAL
        }
        queued = sorted(
            (task for task in self._tasks.values() if task["status"] == TaskStatus.QUEUED),
            key=task_sort_key,
        )
        for task in queued:
            mode = ExecutionMode(task["execution_mode"])
            if mode == ExecutionMode.EXCLUSIVE:
                # Once the highest eligible exclusive task is encountered, drain existing work
                # instead of starting lower-ranked tasks forever.
                return task if not running_tasks else None
            if mode == ExecutionMode.SERIAL and task["serial_group"] in running_serial_groups:
                continue
            return task
        return None

    async def _execute_task(
        self,
        task_id: str,
        task_snapshot: dict[str, Any],
        context: ExecutionContext,
    ) -> None:
        result: Any = None
        error: dict[str, str] | None = None
        cancelled = False
        try:
            if inspect.iscoroutinefunction(self._executor):
                result = await self._executor(task_snapshot, context)
            else:
                result = await asyncio.to_thread(self._executor, task_snapshot, context)
                if inspect.isawaitable(result):
                    result = await result
        except (TaskCancelled, asyncio.CancelledError):
            cancelled = True
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}

        async with self._condition:
            task = self._tasks[task_id]
            cancellation_requested = task["status"] == TaskStatus.CANCELLING
            if cancelled or cancellation_requested:
                task["status"] = TaskStatus.CANCELLED
                task["result"] = None
                task["error"] = None
            elif error is not None:
                task["status"] = TaskStatus.FAILED
                task["result"] = None
                task["error"] = error
            else:
                task["status"] = TaskStatus.SUCCEEDED
                task["result"] = copy.deepcopy(result)
                task["error"] = None
            task["finished_at"] = utc_now()
            self._running.pop(task_id, None)
            self._changed_locked()

    def _get_task_locked(self, task_id: str) -> dict[str, Any]:
        try:
            return self._tasks[task_id]
        except KeyError as exc:
            raise TaskNotFoundError(task_id) from exc

    def _take_sequence_locked(self) -> int:
        sequence = self._next_sequence
        self._next_sequence += 1
        return sequence

    def _changed_locked(self) -> None:
        self._revision += 1
        self._condition.notify_all()

    @staticmethod
    def _validate_priority(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise InvalidTaskOperationError("priority must be an integer")
        return value

    @staticmethod
    def _list_sort_key(task: dict[str, Any]) -> tuple[int, int, int, int]:
        if task["status"] == TaskStatus.QUEUED:
            tier, priority, sequence = task_sort_key(task)
            return (0, tier, priority, sequence)
        return (1, 0, 0, int(task["queue_sequence"]))
