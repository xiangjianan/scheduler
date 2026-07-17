## ADDED Requirements

### Requirement: Stable priority queue ordering
The scheduler SHALL order queued tasks by ordinary versus last-running tier, ascending numeric priority, and FIFO queue sequence. Tasks marked `run_last` SHALL remain behind all ordinary queued tasks, including ordinary tasks added later.

#### Scenario: New task is inserted by priority
- **WHEN** queued tasks have priorities 10 and 100 and a new ordinary task with priority 50 is created
- **THEN** the effective queue order SHALL be 10, 50, 100

#### Scenario: Equal priorities remain FIFO
- **WHEN** two ordinary tasks with the same priority are created in sequence
- **THEN** the task created first SHALL appear first in the effective queue

#### Scenario: Last-running task remains last
- **WHEN** a task marked `run_last` is queued and a new ordinary task is subsequently created
- **THEN** the new ordinary task SHALL appear before the last-running task

### Requirement: Task lifecycle
Creating a task SHALL enqueue it immediately. The scheduler SHALL support `queued`, `running`, `cancelling`, `cancelled`, `succeeded`, and `failed` states and SHALL NOT expose pause or resume operations.

#### Scenario: Task completes successfully
- **WHEN** an executor returns normally for a queued task
- **THEN** the task SHALL transition through `running` to `succeeded` and retain its result

#### Scenario: Task fails
- **WHEN** an executor raises an exception
- **THEN** the task SHALL transition to `failed` and expose a serializable error description

#### Scenario: Queued task is cancelled
- **WHEN** cancellation is requested for a queued task
- **THEN** the task SHALL become `cancelled` without invoking its executor

### Requirement: Queue metadata updates
The scheduler SHALL allow priority and `run_last` changes only while a task is queued and SHALL recalculate effective ordering after an update.

#### Scenario: Reprioritize queued task
- **WHEN** a queued task priority is changed to a value ahead of another queued task
- **THEN** the updated task SHALL move ahead in the effective order

#### Scenario: Reject running task update
- **WHEN** a priority or `run_last` update is requested for a running task
- **THEN** the scheduler SHALL reject the update without changing the task

### Requirement: Global concurrency
The scheduler SHALL limit simultaneously executing tasks to the configured positive maximum and SHALL support changing that maximum at runtime without terminating running tasks.

#### Scenario: Concurrency limit is enforced
- **WHEN** more tasks are runnable than the configured maximum
- **THEN** no more than the configured maximum SHALL execute simultaneously

#### Scenario: Concurrency limit is reduced
- **WHEN** the maximum is reduced below the number of currently running tasks
- **THEN** running tasks SHALL continue and no new task SHALL start until capacity becomes available under the new maximum

### Requirement: Serial execution groups
Tasks in the same non-empty serial group SHALL NOT execute concurrently, while tasks in different groups MAY execute concurrently when global capacity exists.

#### Scenario: Same-group tasks serialize
- **WHEN** two runnable tasks use the same serial group
- **THEN** the second task SHALL not start before the first task finishes

#### Scenario: Blocked group does not block other groups
- **WHEN** the highest-ranked waiting task belongs to a busy serial group and a later task belongs to another group
- **THEN** the later task SHALL be eligible to start

### Requirement: Exclusive execution
An exclusive task SHALL execute only when no other task is running, and no other task SHALL start while it runs.

#### Scenario: Exclusive task drains current work
- **WHEN** an exclusive task becomes the highest-ranked waiting task while another task runs
- **THEN** the scheduler SHALL wait for current work to finish before starting the exclusive task and SHALL not start lower-ranked tasks during the drain

### Requirement: Manual retry
The scheduler SHALL allow failed or cancelled tasks to be requeued explicitly while preserving their identity and user fields and assigning a new queue sequence.

#### Scenario: Retry failed task
- **WHEN** retry is requested for a failed task
- **THEN** the task SHALL return to `queued` with cleared result and error fields and a new queue sequence

