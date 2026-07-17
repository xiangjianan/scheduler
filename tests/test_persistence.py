import json
from pathlib import Path

import pytest

from task_scheduler import Scheduler, SchedulerConfig, TaskStatus
from task_scheduler.exceptions import PersistenceError
from task_scheduler.persistence import JsonStateStore


class RecordingStore:
    def __init__(self, initial=None) -> None:
        self.initial = initial
        self.saved: list[dict] = []

    async def load(self):
        return self.initial

    async def save(self, snapshot):
        self.saved.append(snapshot)


async def unused_executor(task, context):  # pragma: no cover
    return None


@pytest.mark.asyncio
async def test_json_store_writes_complete_atomic_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "state" / "scheduler.json"
    store = JsonStateStore(path)
    snapshot = {
        "schema_version": 1,
        "saved_at": "2026-07-16T00:00:00+00:00",
        "next_queue_sequence": 2,
        "config": {"max_concurrency": 1},
        "tasks": {"one": {"id": "one", "status": "queued"}},
    }

    await store.save(snapshot)

    assert json.loads(path.read_text(encoding="utf-8")) == snapshot
    assert not path.with_suffix(".json.tmp").exists()


@pytest.mark.asyncio
async def test_json_store_rejects_unsupported_schema(tmp_path: Path) -> None:
    path = tmp_path / "scheduler.json"
    path.write_text('{"schema_version": 999, "tasks": {}}', encoding="utf-8")

    with pytest.raises(PersistenceError, match="schema version"):
        await JsonStateStore(path).load()


@pytest.mark.asyncio
async def test_scheduler_recovers_interrupted_tasks_as_failed() -> None:
    initial = {
        "schema_version": 1,
        "saved_at": "2026-07-16T00:00:00+00:00",
        "next_queue_sequence": 3,
        "config": {"max_concurrency": 2},
        "tasks": {
            "running": _stored_task("running", "running", 1),
            "cancelling": _stored_task("cancelling", "cancelling", 2),
        },
    }
    store = RecordingStore(initial)
    scheduler = Scheduler(executor=unused_executor, state_store=store)

    await scheduler.start()

    for task_id in ("running", "cancelling"):
        task = await scheduler.get_task(task_id)
        assert task["status"] == TaskStatus.FAILED
        assert task["error"]["type"] == "SchedulerInterrupted"
    await scheduler.stop()


@pytest.mark.asyncio
async def test_multiple_mutations_coalesce_and_clean_flush_does_not_write() -> None:
    store = RecordingStore()
    scheduler = Scheduler(executor=unused_executor, state_store=store)

    await scheduler.create_task({"name": "one"})
    await scheduler.create_task({"name": "two"})
    await scheduler.create_task({"name": "three"})

    assert await scheduler.flush() is True
    assert len(store.saved) == 1
    assert len(store.saved[0]["tasks"]) == 3
    assert await scheduler.flush() is False
    assert len(store.saved) == 1


@pytest.mark.asyncio
async def test_stop_flushes_dirty_state_without_high_frequency_writes() -> None:
    store = RecordingStore()
    scheduler = Scheduler(
        executor=unused_executor,
        state_store=store,
        config=SchedulerConfig(max_concurrency=1, flush_interval=60),
    )
    await scheduler.create_task({"name": "waiting"})

    await scheduler.stop()

    assert len(store.saved) == 1
    assert next(iter(store.saved[0]["tasks"].values()))["name"] == "waiting"


def _stored_task(task_id: str, status: str, sequence: int) -> dict:
    return {
        "id": task_id,
        "status": status,
        "priority": 100,
        "run_last": False,
        "queue_sequence": sequence,
        "execution_mode": "parallel",
        "serial_group": None,
        "created_at": "2026-07-16T00:00:00+00:00",
        "started_at": "2026-07-16T00:01:00+00:00",
        "finished_at": None,
        "attempt": 1,
        "result": None,
        "error": None,
    }
