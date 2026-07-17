# Platform Task Scheduler

一个只负责任务调度、不限定任务执行内容的 Python 调度器。它既可以作为 Python 包嵌入现有平台，也可以通过 FastAPI 对外提供 HTTP API。

当前版本适用于单进程、单机部署，状态以低频原子快照的方式存入本地 JSON 文件。

## 核心行为

- 创建任务后立即进入 `queued`，不提供暂停或恢复功能。
- 队列排序键为 `(run_last, priority, queue_sequence)`：普通任务优先，数字较小的优先级先运行，同优先级先进先出。
- `run_last=true` 是持续性的最后运行层级；之后加入的普通任务仍会排在它前面。
- 支持全局并发上限、同组串行任务和独占任务。
- 排队任务可立即取消；运行任务通过执行上下文协作取消。
- 任务可以包含任意额外 JSON 字段，这些字段会原样传给用户执行器。
- 多次状态变化由脏标记合并，按照配置间隔写入一次 JSON；正常关闭时强制落盘。

## 安装与开发

项目要求 Python 3.11 或更高版本。

```bash
uv sync --extra dev
uv run pytest
```

## 嵌入 Python 平台

执行器是用户自由实现的函数，可以是异步函数：

```python
import asyncio

from task_scheduler import ExecutionContext, JsonStateStore, Scheduler, SchedulerConfig


async def execute(task: dict, context: ExecutionContext):
    # task 中包含调度字段，也保留了所有用户自定义字段。
    for item in task["items"]:
        context.raise_if_cancelled()
        await asyncio.sleep(0.1)
    return {"processed": len(task["items"])}


async def main():
    scheduler = Scheduler(
        executor=execute,
        state_store=JsonStateStore("./data/scheduler.json"),
        config=SchedulerConfig(max_concurrency=4, flush_interval=10),
    )

    # 使用持久化时先 start，让调度器恢复已有状态，再创建新任务。
    await scheduler.start()
    task = await scheduler.create_task(
        {
            "priority": 100,
            "execution_mode": "serial",
            "serial_group": "project:42",
            "project_id": 42,
            "items": [1, 2, 3],
        }
    )
    print(task["id"])

    await scheduler.wait_for_idle()
    await scheduler.stop()


asyncio.run(main())
```

同步执行器同样受支持，调度器会将它放入线程中运行，避免阻塞 asyncio 事件循环。

## 提供 HTTP API

在平台自己的 `my_app.py` 中注入执行器：

```python
from task_scheduler import JsonStateStore, Scheduler, create_app


async def execute(task, context):
    # 在这里调用平台方法、HTTP 服务、容器系统或任何自定义逻辑。
    return {"handled": task.get("type")}


scheduler = Scheduler(
    executor=execute,
    state_store=JsonStateStore("./data/scheduler.json"),
    max_concurrency=8,
)
app = create_app(scheduler)
```

启动：

```bash
uv run uvicorn my_app:app --host 0.0.0.0 --port 8000
```

创建带任意业务字段的任务：

```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "priority": 50,
    "run_last": false,
    "type": "video-render",
    "video_id": 123,
    "options": {"resolution": "4k"}
  }'
```

主要端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/v1/tasks` | 创建并入队 |
| `GET` | `/api/v1/tasks` | 按有效队列顺序列出，可用 `status` 过滤 |
| `GET` | `/api/v1/tasks/{id}` | 查询任务 |
| `PATCH` | `/api/v1/tasks/{id}` | 修改排队任务的 `priority`/`run_last` |
| `POST` | `/api/v1/tasks/{id}/cancel` | 取消任务 |
| `POST` | `/api/v1/tasks/{id}/retry` | 重新入队失败或取消的任务 |
| `GET` | `/api/v1/config` | 查询运行配置 |
| `PATCH` | `/api/v1/config/concurrency` | 动态调整最大并行数 |
| `GET` | `/api/v1/stats` | 查询状态统计 |
| `GET` | `/api/v1/health` | 健康检查 |

FastAPI 同时自动提供 `/docs` 和 `/openapi.json`。

## 任务字段

系统管理的核心字段包括：

```json
{
  "id": "generated-uuid",
  "status": "queued",
  "priority": 100,
  "run_last": false,
  "queue_sequence": 1,
  "execution_mode": "parallel",
  "serial_group": null,
  "created_at": "2026-07-16T00:00:00+00:00",
  "started_at": null,
  "finished_at": null,
  "attempt": 0,
  "result": null,
  "error": null
}
```

除了系统字段，创建请求可以加入任意 JSON 字段。`status`、时间戳、结果等系统拥有字段不能由客户端伪造。

## 执行模式

| 模式 | 行为 |
|---|---|
| `parallel` | 只受全局并发上限约束 |
| `serial` | 同一个非空 `serial_group` 中最多运行一个，不同组可以并行 |
| `exclusive` | 等当前任务排空后独占运行；运行期间不启动其他任务 |

当最高顺位的串行任务所属分组正忙时，调度器会跳过它，让其他分组继续运行。当最高可调度顺位出现独占任务时，调度器会停止启动后续任务，等待当前任务排空。

## 取消语义

排队任务取消后不会调用执行器。运行中的任务先进入 `cancelling`，执行器需要检查上下文：

```python
async def execute(task, context):
    for item in task["items"]:
        context.raise_if_cancelled()
        await process(item)
```

Python 无法安全强制终止任意用户函数。如果执行器不检查取消状态，任务会在函数返回后才最终成为 `cancelled`。

## JSON 持久化限制

- 内存状态是运行时事实来源，JSON 仅用于重启恢复。
- 只允许一个调度器进程写同一个文件。
- 文件通过“临时文件 + `fsync` + 原子替换”保存，不会暴露半份 JSON。
- `flush_interval` 内的多次变化会合并；进程异常退出时，最后一次快照之后的变化可能丢失。
- 正常关闭会强制写入一次脏状态。
- 重启时，原先的 `running`/`cancelling` 任务会变为 `failed`，避免自动重复执行非幂等任务；需要显式调用 retry。

若业务要求多实例、零丢失窗口或大规模任务，应通过 `StateStore` 抽象扩展 SQLite、PostgreSQL 等存储，而不是共享 JSON 文件。

## 质量检查

```bash
uv run pytest --cov=task_scheduler --cov-report=term-missing
uv run ruff check .
uv run ruff format --check .
openspec validate implement-task-scheduler --strict
```

规格、设计和实施清单位于 `openspec/changes/implement-task-scheduler/`。
