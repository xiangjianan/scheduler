## ADDED Requirements

### Requirement: Task management API
The service SHALL expose versioned HTTP endpoints to create, retrieve, list, update, cancel, and retry tasks using JSON representations that preserve arbitrary user-defined fields.

#### Scenario: Create custom task
- **WHEN** a client posts valid scheduling fields and arbitrary additional JSON fields to `/api/v1/tasks`
- **THEN** the service SHALL return HTTP 201 with a queued task containing those fields

#### Scenario: Missing task
- **WHEN** a client requests an unknown task identifier
- **THEN** the service SHALL return HTTP 404 with a structured error

#### Scenario: Pause endpoint is absent
- **WHEN** a client attempts to pause or resume a task
- **THEN** the service SHALL return HTTP 404 or method-not-allowed because no such operation is exposed

### Requirement: Runtime configuration API
The service SHALL expose the current scheduler configuration and SHALL allow clients to set a positive global concurrency maximum.

#### Scenario: Update concurrency
- **WHEN** a client submits a positive `max_concurrency`
- **THEN** the service SHALL apply and return the new limit

#### Scenario: Reject invalid concurrency
- **WHEN** a client submits a zero or negative `max_concurrency`
- **THEN** the service SHALL return HTTP 422

### Requirement: Scheduler observability API
The service SHALL expose health and statistics endpoints containing counts by task state, running count, queued count, and the configured concurrency maximum.

#### Scenario: Read scheduler statistics
- **WHEN** a client requests `/api/v1/stats`
- **THEN** the service SHALL return current state counts and concurrency data
