## 1. Project Foundation

- [x] 1.1 Add Python packaging metadata, runtime/dev dependencies, and the scheduler package layout
- [x] 1.2 Define task models, domain exceptions, execution modes, and the executor context contract

## 2. Scheduling Core — TDD

- [x] 2.1 Write failing tests for priority/FIFO/run-last ordering and queued-task metadata updates
- [x] 2.2 Implement task creation, derived queue ordering, retrieval, listing, updates, cancellation, and retry until ordering/lifecycle tests pass
- [x] 2.3 Write failing deterministic async tests for concurrency limits, serial groups, exclusive execution, result/error handling, and cooperative cancellation
- [x] 2.4 Implement worker dispatch, runtime concurrency updates, execution constraints, sync/async executors, and shutdown until execution tests pass

## 3. JSON Persistence — TDD

- [x] 3.1 Write failing tests for atomic snapshots, schema validation, recovery, dirty-write coalescing, and shutdown flushing
- [x] 3.2 Implement the JSON state store and scheduler persistence loop until persistence tests pass

## 4. HTTP API — TDD

- [x] 4.1 Write failing API tests for task CRUD-style operations, arbitrary fields, cancellation/retry, missing pause endpoints, configuration validation, health, and statistics
- [x] 4.2 Implement the FastAPI application factory, request validation, domain error mapping, and versioned routes until API tests pass

## 5. Verification and Documentation

- [x] 5.1 Add README usage examples for embedded and HTTP operation, executor cancellation, ordering semantics, and JSON durability limits
- [x] 5.2 Run the full test suite, coverage report, static checks, and strict OpenSpec validation; fix all discovered issues
