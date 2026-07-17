from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Self

from fastapi import FastAPI, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .exceptions import InvalidTaskOperationError, TaskNotFoundError
from .models import SYSTEM_OWNED_FIELDS, ExecutionMode, TaskStatus
from .scheduler import Scheduler


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="allow")

    priority: int = 100
    run_last: bool = False
    execution_mode: ExecutionMode = ExecutionMode.PARALLEL
    serial_group: str | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_scheduler_owned_fields(cls, value: Any) -> Any:
        if isinstance(value, dict):
            invalid = ({"id"} | SYSTEM_OWNED_FIELDS).intersection(value)
            if invalid:
                raise ValueError(
                    "scheduler-owned fields cannot be supplied: " + ", ".join(sorted(invalid))
                )
        return value


class TaskUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority: int | None = None
    run_last: bool | None = None

    @model_validator(mode="after")
    def require_change(self) -> Self:
        if self.priority is None and self.run_last is None:
            raise ValueError("at least one ordering field is required")
        return self


class ConcurrencyUpdate(BaseModel):
    max_concurrency: int = Field(ge=1)


def create_app(scheduler: Scheduler) -> FastAPI:
    """Create an API around a configured scheduler and executor."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await scheduler.start()
        try:
            yield
        finally:
            await scheduler.stop()

    app = FastAPI(title="Platform Task Scheduler", version="0.1.0", lifespan=lifespan)
    app.state.scheduler = scheduler

    @app.exception_handler(TaskNotFoundError)
    async def task_not_found(_request: Request, exc: TaskNotFoundError) -> JSONResponse:
        return _error_response(status.HTTP_404_NOT_FOUND, "task_not_found", str(exc))

    @app.exception_handler(InvalidTaskOperationError)
    async def invalid_operation(_request: Request, exc: InvalidTaskOperationError) -> JSONResponse:
        return _error_response(status.HTTP_409_CONFLICT, "invalid_task_operation", str(exc))

    @app.post("/api/v1/tasks", status_code=status.HTTP_201_CREATED)
    async def create_task(request: TaskCreate) -> dict[str, Any]:
        return await scheduler.create_task(request.model_dump())

    @app.get("/api/v1/tasks")
    async def list_tasks(
        task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    ) -> list[dict[str, Any]]:
        return await scheduler.list_tasks(status=task_status)

    @app.get("/api/v1/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        return await scheduler.get_task(task_id)

    @app.patch("/api/v1/tasks/{task_id}")
    async def update_task(task_id: str, request: TaskUpdate) -> dict[str, Any]:
        return await scheduler.update_task(
            task_id, priority=request.priority, run_last=request.run_last
        )

    @app.post("/api/v1/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, Any]:
        return await scheduler.cancel_task(task_id)

    @app.post("/api/v1/tasks/{task_id}/retry")
    async def retry_task(task_id: str) -> dict[str, Any]:
        return await scheduler.retry_task(task_id)

    @app.get("/api/v1/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/stats")
    async def stats() -> dict[str, Any]:
        return await scheduler.stats()

    @app.get("/api/v1/config")
    async def get_config() -> dict[str, int]:
        return {"max_concurrency": scheduler.max_concurrency}

    @app.patch("/api/v1/config/concurrency")
    async def update_concurrency(request: ConcurrencyUpdate) -> dict[str, int]:
        value = await scheduler.set_max_concurrency(request.max_concurrency)
        return {"max_concurrency": value}

    return app


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message}},
    )
