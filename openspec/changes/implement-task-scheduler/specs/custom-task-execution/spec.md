## ADDED Requirements

### Requirement: Platform-defined executor
The scheduler SHALL invoke a host-supplied Python callable for each dispatched task and SHALL pass a dictionary containing all scheduler and user-defined fields plus an execution context.

#### Scenario: Arbitrary fields reach executor
- **WHEN** a task containing fields unknown to the scheduler is dispatched
- **THEN** the executor SHALL receive those fields unchanged

#### Scenario: Async executor
- **WHEN** the supplied executor is asynchronous
- **THEN** the scheduler SHALL await it without blocking other task executions

#### Scenario: Synchronous executor
- **WHEN** the supplied executor is synchronous
- **THEN** the scheduler SHALL run it outside the event-loop thread

### Requirement: Cooperative cancellation
The execution context SHALL expose whether cancellation was requested and a method that raises a cancellation exception. Cancelling a running task SHALL signal its context and transition the task to `cancelling` until execution exits.

#### Scenario: Cooperative executor observes cancellation
- **WHEN** a running executor checks its context after cancellation is requested
- **THEN** it SHALL be able to stop and the task SHALL finish as `cancelled`

