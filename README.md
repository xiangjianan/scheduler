# Platform Task Scheduler

**English** | [简体中文](README.zh-CN.md)

A Python scheduler that only handles task scheduling and does not constrain what tasks actually execute. It can be embedded into an existing platform as a Python package, or expose an HTTP API to the outside via FastAPI.

The current version targets single-process, single-machine deployment; state is persisted to a local JSON file as low-frequency atomic snapshots.

## Core Behavior

- A task enters `queued` immediately after creation; no pause or resume functionality is provided.
- The queue sort key is `(run_last, priority, queue_sequence)`: regular tasks first, lower numeric priority runs first, FIFO within the same priority.
- `run_last=true` is a persistent run-last tier; regular tasks added afterwards still sort ahead of it.
- Supports a global concurrency limit, serial tasks within the same group, and exclusive tasks.
- Queued tasks can be cancelled immediately; running tasks are cancelled cooperatively through the execution context.
- Tasks may carry arbitrary extra JSON fields, which are passed as-is to the user executor.
- Multiple state changes are coalesced via a dirty flag and written to JSON once per configured interval; a graceful shutdown forces a flush to disk.

## Installation & Development

The project requires Python 3.11 or later.

```bash
uv sync --extra dev
uv run pytest
```

## Embedding into a Python Platform

The executor is a function you implement freely; it may be an async function:

```python
import asyncio

from task_scheduler import ExecutionContext, JsonStateStore, Scheduler, SchedulerConfig


async def execute(task: dict, context: ExecutionContext):
    # The task contains scheduling fields and also keeps all user-defined fields.
    for item in task["items"]:
        context.raise_if_cancelled()
        await asyncio.sleep(0.1)
    return {"processed": len(task["items"])}


async def main():
    scheduler = Scheduler(
        executor=execute,
        state_store=JsonStateStore("./data/scheduler.json"),
        config=SchedulerConfig(max_concurrency=4, flush_interval=10),
    )

    # When using persistence, call start first so the scheduler restores existing state, then create new tasks.
    await scheduler.start()
    task = await scheduler.create_task(
        {
            "priority": 100,
            "execution_mode": "serial",
            "serial_group": "project:42",
            "project_id": 42,
            "items": [1, 2, 3],
        }
    )
    print(task["id"])

    await scheduler.wait_for_idle()
    await scheduler.stop()


asyncio.run(main())
```

Synchronous executors are also supported; the scheduler runs them in a thread to avoid blocking the asyncio event loop.

## Exposing an HTTP API

Inject your executor in your platform's own `my_app.py`:

```python
from task_scheduler import JsonStateStore, Scheduler, create_app


async def execute(task, context):
    # Call platform methods, HTTP services, container systems, or any custom logic here.
    return {"handled": task.get("type")}


scheduler = Scheduler(
    executor=execute,
    state_store=JsonStateStore("./data/scheduler.json"),
    max_concurrency=8,
)
app = create_app(scheduler)
```

Start it:

```bash
uv run uvicorn my_app:app --host 0.0.0.0 --port 8000
```

Create a task with arbitrary business fields:

```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "priority": 50,
    "run_last": false,
    "type": "video-render",
    "video_id": 123,
    "options": {"resolution": "4k"}
  }'
```

Main endpoints:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/tasks` | Create and enqueue |
| `GET` | `/api/v1/tasks` | List in effective queue order, filterable by `status` |
| `GET` | `/api/v1/tasks/{id}` | Query a task |
| `PATCH` | `/api/v1/tasks/{id}` | Modify `priority`/`run_last` of a queued task |
| `POST` | `/api/v1/tasks/{id}/cancel` | Cancel a task |
| `POST` | `/api/v1/tasks/{id}/retry` | Re-enqueue a failed or cancelled task |
| `GET` | `/api/v1/config` | Query the runtime config |
| `PATCH` | `/api/v1/config/concurrency` | Dynamically adjust max concurrency |
| `GET` | `/api/v1/stats` | Query status statistics |
| `GET` | `/api/v1/health` | Health check |

FastAPI also automatically serves `/docs` and `/openapi.json`.

## Task Fields

The core system-managed fields include:

```json
{
  "id": "generated-uuid",
  "status": "queued",
  "priority": 100,
  "run_last": false,
  "queue_sequence": 1,
  "execution_mode": "parallel",
  "serial_group": null,
  "created_at": "2026-07-16T00:00:00+00:00",
  "started_at": null,
  "finished_at": null,
  "attempt": 0,
  "result": null,
  "error": null
}
```

Besides the system fields, creation requests may include arbitrary JSON fields. System-owned fields such as `status`, timestamps, and results cannot be forged by clients.

## Execution Modes

| Mode | Behavior |
|---|---|
| `parallel` | Constrained only by the global concurrency limit |
| `serial` | At most one running within the same non-empty `serial_group`; different groups may run in parallel |
| `exclusive` | Waits for current tasks to drain, then runs alone; no other task starts while it runs |

When the group of the highest-ranked serial task is busy, the scheduler skips it so other groups keep running. When an exclusive task reaches the highest schedulable position, the scheduler stops starting further tasks and waits for the current ones to drain.

## Cancellation Semantics

Cancelling a queued task never invokes the executor. A running task first enters `cancelling`, and the executor must check the context:

```python
async def execute(task, context):
    for item in task["items"]:
        context.raise_if_cancelled()
        await process(item)
```

Python cannot safely force-terminate arbitrary user functions. If the executor never checks the cancellation status, the task only finally becomes `cancelled` after the function returns.

## JSON Persistence Limits

- The in-memory state is the runtime source of truth; JSON is only used for restart recovery.
- Only one scheduler process may write to the same file.
- The file is saved via "temp file + `fsync` + atomic replace", so a half-written JSON is never exposed.
- Multiple changes within `flush_interval` are coalesced; if the process exits abnormally, changes after the last snapshot may be lost.
- A graceful shutdown forces one write of dirty state.
- On restart, tasks that were previously `running`/`cancelling` become `failed` to avoid automatically re-executing non-idempotent tasks; an explicit retry call is required.

If your business requires multiple instances, a zero-loss window, or large-scale task volumes, extend the `StateStore` abstraction with SQLite, PostgreSQL, or similar stores instead of sharing the JSON file.

## Quality Checks

```bash
uv run pytest --cov=task_scheduler --cov-report=term-missing
uv run ruff check .
uv run ruff format --check .
openspec validate implement-task-scheduler --strict
```

The specs, design, and implementation checklists live in `openspec/changes/implement-task-scheduler/`.
