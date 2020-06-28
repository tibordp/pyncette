import asyncio
import datetime
from unittest.mock import MagicMock

import pytest
from conftest import wrap_factory
from prometheus_client import generate_latest

from pyncette import Context
from pyncette import Pyncette
from pyncette.prometheus import use_prometheus
from pyncette.sqlite import sqlite_repository


@pytest.mark.asyncio
async def test_successful_task_interval(timemachine):
    app = Pyncette(repository_factory=wrap_factory(sqlite_repository, timemachine))
    use_prometheus(app)

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    metrics = generate_latest().decode("ascii").splitlines()
    assert 'pyncette_tasks_total{task_name="successful_task"} 5.0' in metrics
    assert (
        'pyncette_repository_ops_total{operation="poll_task",task_name="successful_task"} 11.0'
        in metrics
    )
    assert (
        'pyncette_repository_ops_total{operation="commit_task",task_name="successful_task"} 5.0'
        in metrics
    )
