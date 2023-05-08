import asyncio
import datetime
from unittest.mock import MagicMock

import pytest
from prometheus_client import generate_latest

from pyncette import Context
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.prometheus import use_prometheus
from pyncette.sqlite import sqlite_repository

from conftest import wrap_factory


@pytest.mark.asyncio
async def test_prometheus_metrics(timemachine):
    app = Pyncette(repository_factory=wrap_factory(sqlite_repository, timemachine))
    use_prometheus(app)

    counter = MagicMock()

    @app.dynamic_task(failure_mode=FailureMode.UNLOCK)
    async def dynamic_task_1(context: Context) -> None:
        counter.execute()
        raise Exception("test")

    @app.task(interval=datetime.timedelta(seconds=2))
    async def task_1(context: Context) -> None:
        await context.heartbeat()
        counter.execute()

    async with app.create() as ctx:
        await ctx.schedule_task(dynamic_task_1, "1", interval=datetime.timedelta(seconds=2))
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        await ctx.unschedule_task(dynamic_task_1, "1")

        ctx.shutdown()
        await task
        await timemachine.unwind()

    metrics = generate_latest().decode("ascii").splitlines()

    assert 'pyncette_repository_ops_total{operation="unlock_task",task_name="dynamic_task_1"} 9.0' in metrics
    assert 'pyncette_repository_ops_total{operation="commit_task",task_name="task_1"} 5.0' in metrics
    assert 'pyncette_repository_ops_total{operation="poll_dynamic_task",task_name="dynamic_task_1"} 11.0' in metrics
    assert 'pyncette_repository_ops_total{operation="poll_task",task_name="dynamic_task_1"} 9.0' in metrics
    assert 'pyncette_repository_ops_total{operation="poll_task",task_name="task_1"} 11.0' in metrics
    assert 'pyncette_repository_ops_total{operation="extend_lease",task_name="task_1"} 5.0' in metrics
    assert 'pyncette_repository_ops_total{operation="register_task",task_name="dynamic_task_1"} 1.0' in metrics
    assert 'pyncette_repository_ops_total{operation="unregister_task",task_name="dynamic_task_1"} 1.0' in metrics
    assert 'pyncette_tasks_total{task_name="dynamic_task_1"} 9.0' in metrics
    assert 'pyncette_tasks_total{task_name="task_1"} 5.0' in metrics
