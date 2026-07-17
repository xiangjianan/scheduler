## Why

Platforms need a small, reusable scheduling component that controls task ordering and concurrency without prescribing what a task does. The first release should provide a Python-native integration surface and HTTP API while remaining simple enough to embed in a single process and persist locally.

## What Changes

- Add an execution-agnostic scheduler that accepts dictionary-shaped tasks with reserved scheduling fields and arbitrary user-defined fields.
- Add stable priority ordering, a persistent `run_last` tier, FIFO ordering for ties, serial execution groups, exclusive tasks, and a runtime-adjustable global concurrency limit.
- Add task creation, retrieval, listing, cancellation, retry, priority/order updates, scheduler statistics, and concurrency configuration APIs.
- Add a Python executor callback contract so integrating platforms retain complete control over task behavior.
- Add low-frequency, atomic JSON snapshot persistence with configurable flush timing and deterministic restart recovery.
- Explicitly omit task pause and resume behavior.

## Capabilities

### New Capabilities
- `task-scheduling`: Task lifecycle, stable queue ordering, cancellation, retries, concurrency limits, serial groups, and exclusive execution.
- `custom-task-execution`: Python callback contract and execution context for platform-defined task behavior and cooperative cancellation.
- `json-state-persistence`: Buffered local JSON snapshots, atomic replacement, shutdown flushing, and restart recovery.
- `scheduler-http-api`: FastAPI endpoints for task operations, scheduler statistics, and runtime concurrency configuration.

### Modified Capabilities

None.

## Impact

- Introduces a Python package, FastAPI application factory, tests, and packaging metadata.
- Adds runtime dependencies on FastAPI, Pydantic, and Uvicorn, plus pytest tooling for development.
- The JSON backend is deliberately limited to one scheduler process writing a given state file; a future repository abstraction can support database-backed deployments.
