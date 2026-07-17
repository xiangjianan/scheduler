## ADDED Requirements

### Requirement: Buffered snapshot persistence
The JSON state store SHALL persist scheduler state from memory only after state is dirty and a configured flush opportunity occurs, and SHALL flush dirty state during graceful shutdown.

#### Scenario: Multiple mutations are coalesced
- **WHEN** multiple task mutations occur before the next flush opportunity
- **THEN** the store SHALL persist one snapshot containing the latest state rather than one file write per mutation

#### Scenario: Clean state is not rewritten
- **WHEN** a flush opportunity occurs without state changes
- **THEN** the store SHALL not rewrite the JSON file

### Requirement: Atomic JSON replacement
The JSON store SHALL write a complete temporary file, flush it, and atomically replace the configured state file.

#### Scenario: Snapshot is readable after save
- **WHEN** a snapshot save completes successfully
- **THEN** the configured file SHALL contain valid JSON representing the complete snapshot

### Requirement: Versioned state recovery
The scheduler SHALL persist a schema version and queue sequence counter. On load, it SHALL preserve queued and terminal tasks and convert tasks left `running` or `cancelling` to `failed` with a scheduler-interruption error.

#### Scenario: Interrupted running task is not repeated automatically
- **WHEN** a state file contains a running task during startup
- **THEN** the task SHALL load as failed and SHALL not execute until explicitly retried

#### Scenario: Unsupported schema is rejected
- **WHEN** a state file contains an unsupported schema version
- **THEN** loading SHALL fail with a clear persistence error

