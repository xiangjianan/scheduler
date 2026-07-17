## Context

The repository is empty and the scheduler is a new, embeddable Python component. It must make scheduling decisions independently of task business logic, expose those decisions through an HTTP API, keep arbitrary user fields intact, and operate without an external database. The initial deployment target is one Python process with one local JSON state file.

## Goals / Non-Goals

**Goals:**

- Provide deterministic queue ordering with priority, FIFO ties, and a persistent last-running tier.
- Enforce global concurrency, serial-group mutual exclusion, and exclusive execution without coupling the scheduler to task behavior.
- Expose a small async/sync Python executor contract and a FastAPI application factory.
- Persist recoverable state through buffered, atomic JSON snapshots.
- Keep the scheduling core independently testable with deterministic async tests.

**Non-Goals:**

- Pause/resume, cron scheduling, task dependency graphs, forced process termination, authentication, multi-tenancy, and a web UI.
- Multiple scheduler processes sharing one JSON file.
- Durable, zero-loss semantics between JSON snapshots.
- Executing arbitrary code supplied through the HTTP API.

## Decisions

### The in-memory scheduler is the runtime source of truth

The scheduler owns a task dictionary protected by an `asyncio` lock and condition. JSON is a recovery snapshot rather than a live queue. This keeps queue operations fast and makes write coalescing straightforward. Directly editing JSON on every state transition was rejected because it would increase filesystem activity and couple runtime correctness to file I/O latency.

### Ordering is derived rather than stored as mutable positions

Every queued task has a monotonically increasing `queue_sequence`. The effective ordering key is `(run_last_tier, priority, queue_sequence)`, with lower numeric priority running first. `run_last=true` remains behind every ordinary queued task, including ordinary tasks added later. Positions are calculated for API responses and never persisted as mutable indexes.

### Runnable selection separates order from execution constraints

The scheduler scans tasks in effective order and chooses the first task compatible with current runtime constraints. A busy serial group can be skipped so unrelated work proceeds. An exclusive task at the head of its tier causes dispatch to drain current work before it starts; while exclusive work runs, no other task starts. This prevents exclusive-task starvation caused by continually launching lower-ranked work.

### Task dictionaries preserve arbitrary user fields

Reserved scheduler fields are validated by Pydantic request models configured to allow extra properties. Extra properties are stored and passed unchanged to the executor. Clients cannot mutate scheduler-owned lifecycle fields through the update endpoint. A nested payload is supported by convention but not required.

### Execution is supplied by the host platform

The Python library accepts a callable with `(task_dict, execution_context)`. Async functions are awaited; synchronous functions run in a worker thread so they do not block scheduling. The context exposes cooperative cancellation. The HTTP API schedules data only and never accepts executable code.

### Cancellation is cooperative for running tasks

Queued tasks become `cancelled` immediately. Running tasks become `cancelling` and their execution context is signalled. If the executor exits after cancellation was requested, the final state is `cancelled`. Python cannot safely terminate arbitrary user code, so an executor that ignores the context can delay cancellation until it returns.

### JSON persistence uses dirty revisions and atomic snapshots

Mutations increment an in-memory revision and mark state dirty. A background loop flushes no more frequently than a configurable interval, while shutdown performs a final flush. The state is copied under the scheduler lock and written outside it to a temporary file, flushed and atomically replaced. Dirty state is cleared only if the revision did not change during the write.

On restart, `queued` tasks remain queued, terminal tasks remain terminal, and `running` or `cancelling` tasks become `failed` with a scheduler-interruption error. Re-running interrupted non-idempotent work automatically was rejected as unsafe.

### FastAPI wraps, but does not own, the scheduler core

An application factory receives a configured scheduler and manages its lifecycle. Routes delegate to public scheduler methods and translate domain exceptions to HTTP responses. This permits both embedded library usage and standalone Uvicorn deployment.

### TDD maps scenarios to behavior-focused tests

Tests are introduced before implementation in layers: pure ordering/state tests, async execution-constraint tests, persistence/recovery tests, then API contract tests. Fake executors and asyncio events make concurrency tests deterministic instead of relying on sleeps.

## Risks / Trade-offs

- [A process crash can lose changes since the last snapshot] → Document the durability window, configure the flush interval, and always flush on graceful shutdown.
- [A large task set makes full JSON serialization expensive] → Serialize in a background thread and keep `StateStore` as an abstraction that can later support SQLite or PostgreSQL.
- [A non-cooperative executor cannot be cancelled promptly] → Expose cancellation state clearly and document the executor contract.
- [A continuous stream of ordinary work can starve `run_last` tasks] → Treat this as the explicit meaning of the persistent last-running tier; clients can remove `run_last` through the update API.
- [One JSON file cannot coordinate multiple processes] → Enforce and document a single-writer deployment model.

## Migration Plan

This is a new system, so no data migration is required. State files include a schema version. A deployment can roll back by stopping the service and retaining its JSON file; incompatible future schema versions must fail with a clear error instead of silently loading.

## Open Questions

None for the first release. Authentication, database persistence, and distributed workers are intentionally deferred.
