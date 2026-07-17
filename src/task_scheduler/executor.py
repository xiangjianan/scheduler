import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeAlias


class TaskCancelled(Exception):
    """Cooperative cancellation raised inside a platform executor."""


class ExecutionContext:
    """Cancellation state supplied to a platform-defined executor."""

    def __init__(self) -> None:
        self._cancelled = asyncio.Event()

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise TaskCancelled

    async def wait_cancelled(self) -> None:
        await self._cancelled.wait()

    def cancel(self) -> None:
        self._cancelled.set()


TaskExecutor: TypeAlias = Callable[[dict[str, Any], ExecutionContext], Any | Awaitable[Any]]
