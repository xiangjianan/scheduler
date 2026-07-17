import asyncio
import threading

import pytest

from task_scheduler import Scheduler, TaskStatus


@pytest.mark.asyncio
async def test_global_concurrency_limit_and_runtime_reduction() -> None:
    release = asyncio.Event()
    two_started = asyncio.Event()
    running = 0
    maximum_seen = 0

    async def executor(task, context):
        nonlocal running, maximum_seen
        running += 1
        maximum_seen = max(maximum_seen, running)
        if running == 2:
            two_started.set()
        await release.wait()
        running -= 1
        return task["name"]

    scheduler = Scheduler(executor=executor, max_concurrency=2)
    tasks = [await scheduler.create_task({"name": str(index)}) for index in range(3)]
    await scheduler.start()
    await asyncio.wait_for(two_started.wait(), timeout=1)

    await scheduler.set_max_concurrency(1)
    assert (await scheduler.stats())["running"] == 2
    assert (await scheduler.get_task(tasks[2]["id"]))["status"] == TaskStatus.QUEUED

    release.set()
    await scheduler.wait_for_idle(timeout=1)
    await scheduler.stop()

    assert maximum_seen == 2
    assert [task["status"] for task in await scheduler.list_tasks()] == [
        TaskStatus.SUCCEEDED,
        TaskStatus.SUCCEEDED,
        TaskStatus.SUCCEEDED,
    ]


@pytest.mark.asyncio
async def test_serial_group_skips_blocked_task_and_runs_other_group() -> None:
    started: set[str] = set()
    first_wave = asyncio.Event()
    releases: dict[str, asyncio.Event] = {}

    async def executor(task, context):
        started.add(task["name"])
        releases[task["name"]] = asyncio.Event()
        if {"a1", "b1"}.issubset(started):
            first_wave.set()
        await releases[task["name"]].wait()

    scheduler = Scheduler(executor=executor, max_concurrency=3)
    await scheduler.create_task(
        {"name": "a1", "execution_mode": "serial", "serial_group": "group-a"}
    )
    await scheduler.create_task(
        {"name": "a2", "execution_mode": "serial", "serial_group": "group-a"}
    )
    await scheduler.create_task(
        {"name": "b1", "execution_mode": "serial", "serial_group": "group-b"}
    )

    await scheduler.start()
    await asyncio.wait_for(first_wave.wait(), timeout=1)
    assert "a2" not in started

    releases["a1"].set()
    await _wait_until(lambda: "a2" in started)
    releases["a2"].set()
    releases["b1"].set()
    await scheduler.wait_for_idle(timeout=1)
    await scheduler.stop()


@pytest.mark.asyncio
async def test_exclusive_task_drains_running_work_and_blocks_lower_ranked_work() -> None:
    started: list[str] = []
    releases: dict[str, asyncio.Event] = {}

    async def executor(task, context):
        started.append(task["name"])
        releases[task["name"]] = asyncio.Event()
        await releases[task["name"]].wait()

    scheduler = Scheduler(executor=executor, max_concurrency=2)
    await scheduler.create_task({"name": "first", "priority": 0})
    await scheduler.start()
    await _wait_until(lambda: started == ["first"])

    await scheduler.create_task(
        {"name": "exclusive", "priority": 10, "execution_mode": "exclusive"}
    )
    await scheduler.create_task({"name": "later", "priority": 100})
    await asyncio.sleep(0)
    assert started == ["first"]

    releases["first"].set()
    await _wait_until(lambda: started == ["first", "exclusive"])
    assert "later" not in started

    releases["exclusive"].set()
    await _wait_until(lambda: started == ["first", "exclusive", "later"])
    releases["later"].set()
    await scheduler.wait_for_idle(timeout=1)
    await scheduler.stop()


@pytest.mark.asyncio
async def test_results_errors_and_arbitrary_fields_reach_executor() -> None:
    received = []

    async def executor(task, context):
        received.append(task)
        if task["kind"] == "failure":
            raise RuntimeError("boom")
        return {"echo": task["custom"]}

    scheduler = Scheduler(executor=executor, max_concurrency=2)
    success = await scheduler.create_task({"kind": "success", "custom": 42})
    failure = await scheduler.create_task({"kind": "failure", "custom": 7})

    await scheduler.start()
    await scheduler.wait_for_idle(timeout=1)
    await scheduler.stop()

    success = await scheduler.get_task(success["id"])
    failure = await scheduler.get_task(failure["id"])
    assert success["result"] == {"echo": 42}
    assert failure["status"] == TaskStatus.FAILED
    assert failure["error"] == {"type": "RuntimeError", "message": "boom"}
    assert {task["custom"] for task in received} == {7, 42}


@pytest.mark.asyncio
async def test_running_task_is_cancelled_cooperatively() -> None:
    started = asyncio.Event()

    async def executor(task, context):
        started.set()
        await context.wait_cancelled()
        context.raise_if_cancelled()

    scheduler = Scheduler(executor=executor, max_concurrency=1)
    task = await scheduler.create_task({})
    await scheduler.start()
    await asyncio.wait_for(started.wait(), timeout=1)

    cancelling = await scheduler.cancel_task(task["id"])
    assert cancelling["status"] == TaskStatus.CANCELLING

    await scheduler.wait_for_idle(timeout=1)
    await scheduler.stop()
    assert (await scheduler.get_task(task["id"]))["status"] == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_synchronous_executor_runs_outside_event_loop_thread() -> None:
    caller_thread = threading.get_ident()
    executor_threads: list[int] = []

    def executor(task, context):
        executor_threads.append(threading.get_ident())
        return "ok"

    scheduler = Scheduler(executor=executor, max_concurrency=1)
    task = await scheduler.create_task({})
    await scheduler.start()
    await scheduler.wait_for_idle(timeout=1)
    await scheduler.stop()

    assert executor_threads[0] != caller_thread
    assert (await scheduler.get_task(task["id"]))["result"] == "ok"


async def _wait_until(predicate, timeout: float = 1.0) -> None:
    async def wait() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=timeout)
