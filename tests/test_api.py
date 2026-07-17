import pytest
from httpx import ASGITransport, AsyncClient

from task_scheduler import Scheduler
from task_scheduler.api import create_app


async def unused_executor(task, context):  # pragma: no cover
    return None


@pytest.fixture
def scheduler() -> Scheduler:
    return Scheduler(executor=unused_executor, max_concurrency=2)


@pytest.fixture
async def client(scheduler: Scheduler):
    app = create_app(scheduler)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        yield http


@pytest.mark.asyncio
async def test_create_list_get_and_update_custom_task(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={
            "priority": 100,
            "run_last": True,
            "name": "render",
            "project_id": 42,
            "options": {"quality": "4k"},
        },
    )
    assert response.status_code == 201
    task = response.json()
    assert task["status"] == "queued"
    assert task["project_id"] == 42
    assert task["options"] == {"quality": "4k"}

    response = await client.patch(
        f"/api/v1/tasks/{task['id']}", json={"priority": -10, "run_last": False}
    )
    assert response.status_code == 200
    assert response.json()["priority"] == -10
    assert response.json()["run_last"] is False

    assert (await client.get(f"/api/v1/tasks/{task['id']}")).json()["name"] == "render"
    listed = (await client.get("/api/v1/tasks", params={"status": "queued"})).json()
    assert [item["id"] for item in listed] == [task["id"]]


@pytest.mark.asyncio
async def test_cancel_and_retry_task(client: AsyncClient) -> None:
    task = (await client.post("/api/v1/tasks", json={"name": "work"})).json()

    cancelled = await client.post(f"/api/v1/tasks/{task['id']}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    retried = await client.post(f"/api/v1/tasks/{task['id']}/retry")
    assert retried.status_code == 200
    assert retried.json()["status"] == "queued"


@pytest.mark.asyncio
async def test_missing_task_has_structured_404(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/missing")
    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "task_not_found",
            "message": "Task 'missing' was not found",
        }
    }


@pytest.mark.asyncio
async def test_pause_and_resume_are_not_exposed(client: AsyncClient) -> None:
    task = (await client.post("/api/v1/tasks", json={})).json()

    assert (await client.post(f"/api/v1/tasks/{task['id']}/pause")).status_code == 404
    assert (await client.post(f"/api/v1/tasks/{task['id']}/resume")).status_code == 404


@pytest.mark.asyncio
async def test_configuration_health_and_stats(client: AsyncClient) -> None:
    await client.post("/api/v1/tasks", json={"name": "waiting"})

    assert (await client.get("/api/v1/health")).json() == {"status": "ok"}
    assert (await client.get("/api/v1/config")).json() == {"max_concurrency": 2}

    updated = await client.patch("/api/v1/config/concurrency", json={"max_concurrency": 5})
    assert updated.status_code == 200
    assert updated.json() == {"max_concurrency": 5}

    invalid = await client.patch("/api/v1/config/concurrency", json={"max_concurrency": 0})
    assert invalid.status_code == 422

    stats = (await client.get("/api/v1/stats")).json()
    assert stats["queued"] == 1
    assert stats["running"] == 0
    assert stats["max_concurrency"] == 5


@pytest.mark.asyncio
async def test_scheduler_owned_fields_are_rejected(client: AsyncClient) -> None:
    response = await client.post("/api/v1/tasks", json={"status": "succeeded"})
    assert response.status_code == 422
