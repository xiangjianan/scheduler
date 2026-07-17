import pytest

from task_scheduler import ExecutionMode, Scheduler, TaskStatus
from task_scheduler.exceptions import InvalidTaskOperationError, TaskNotFoundError


async def unused_executor(task, context):  # pragma: no cover
    raise AssertionError("executor should not run in core-only tests")


@pytest.fixture
def scheduler() -> Scheduler:
    return Scheduler(executor=unused_executor, max_concurrency=2)


@pytest.mark.asyncio
async def test_tasks_are_ordered_by_tier_priority_and_fifo(scheduler: Scheduler) -> None:
    first_default = await scheduler.create_task({"priority": 100, "name": "first"})
    last = await scheduler.create_task({"priority": -100, "run_last": True, "name": "last"})
    urgent = await scheduler.create_task({"priority": 10, "name": "urgent"})
    second_default = await scheduler.create_task({"priority": 100, "name": "second"})

    tasks = await scheduler.list_tasks(status=TaskStatus.QUEUED)

    assert [task["id"] for task in tasks] == [
        urgent["id"],
        first_default["id"],
        second_default["id"],
        last["id"],
    ]
    assert tasks[-1]["run_last"] is True


@pytest.mark.asyncio
async def test_custom_fields_are_preserved_and_inputs_are_copied(scheduler: Scheduler) -> None:
    source = {"name": "render", "project_id": 42, "options": {"quality": "4k"}}

    created = await scheduler.create_task(source)
    source["options"]["quality"] = "low"

    stored = await scheduler.get_task(created["id"])
    assert stored["project_id"] == 42
    assert stored["options"] == {"quality": "4k"}
    assert stored["status"] == TaskStatus.QUEUED
    assert stored["execution_mode"] == ExecutionMode.PARALLEL


@pytest.mark.asyncio
async def test_update_reorders_only_queued_tasks(scheduler: Scheduler) -> None:
    ordinary = await scheduler.create_task({"priority": 100})
    last = await scheduler.create_task({"priority": 0, "run_last": True})

    updated = await scheduler.update_task(last["id"], priority=-10, run_last=False)

    assert updated["priority"] == -10
    assert updated["run_last"] is False
    assert [task["id"] for task in await scheduler.list_tasks()] == [
        last["id"],
        ordinary["id"],
    ]


@pytest.mark.asyncio
async def test_cancel_queued_task_and_retry_it(scheduler: Scheduler) -> None:
    task = await scheduler.create_task({"name": "retry-me"})
    original_sequence = task["queue_sequence"]

    cancelled = await scheduler.cancel_task(task["id"])
    assert cancelled["status"] == TaskStatus.CANCELLED
    assert cancelled["finished_at"] is not None

    retried = await scheduler.retry_task(task["id"])
    assert retried["status"] == TaskStatus.QUEUED
    assert retried["queue_sequence"] > original_sequence
    assert retried["result"] is None
    assert retried["error"] is None
    assert retried["name"] == "retry-me"


@pytest.mark.asyncio
async def test_invalid_operations_and_missing_tasks_raise_domain_errors(
    scheduler: Scheduler,
) -> None:
    task = await scheduler.create_task({})

    with pytest.raises(InvalidTaskOperationError):
        await scheduler.retry_task(task["id"])

    with pytest.raises(TaskNotFoundError):
        await scheduler.get_task("missing")
