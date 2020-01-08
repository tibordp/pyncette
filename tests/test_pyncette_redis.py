import asyncio
import datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.redis import redis_repository


@pytest.mark.asyncio
@pytest.mark.integration
async def test_successful_task_interval(monkeypatch):
    app = Pyncette(
        redis_url="redis://localhost",
        redis_namespace=str(uuid4()),
        repository_factory=redis_repository,
    )

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=5))
    async def successful_task(context: Context) -> None:
        counter()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.sleep(10.5)
        ctx.shutdown()
        await task

    assert counter.call_count == 2


@pytest.mark.asyncio
@pytest.mark.integration
async def test_dynamic(monkeypatch):
    app = Pyncette(
        redis_url="redis://localhost",
        redis_namespace=str(uuid4()),
        repository_factory=redis_repository,
    )

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        getattr(counter, context.username)()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.gather(
            ctx.schedule_task(
                hello, "1", interval=datetime.timedelta(seconds=2), username="bill"
            ),
            ctx.schedule_task(
                hello, "2", interval=datetime.timedelta(seconds=3), username="steve"
            ),
            ctx.schedule_task(
                hello, "3", interval=datetime.timedelta(seconds=4), username="tibor"
            ),
        )
        await asyncio.sleep(2.5)
        await ctx.unschedule_task(hello, "1")
        await asyncio.sleep(8)
        ctx.shutdown()
        await task

    assert counter.bill.call_count == 1
    assert counter.steve.call_count == 3
    assert counter.tibor.call_count == 2


@pytest.mark.asyncio
@pytest.mark.integration
async def test_failing_task_interval(monkeypatch):
    app = Pyncette(
        redis_url="redis://localhost",
        redis_namespace=str(uuid4()),
        repository_factory=redis_repository,
    )

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=5), failure_mode=FailureMode.UNLOCK,
    )
    async def failing_task(context: Context) -> None:
        counter()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.sleep(10.5)
        ctx.shutdown()
        await task

    assert counter.call_count == 6


@pytest.mark.asyncio
@pytest.mark.integration
async def test_failing_task_interval_best_effort(monkeypatch):
    app = Pyncette(
        redis_url="redis://localhost",
        redis_namespace=str(uuid4()),
        repository_factory=redis_repository,
    )

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=5),
        execution_mode=ExecutionMode.BEST_EFFORT,
    )
    async def failing_task(context: Context) -> None:
        counter()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.sleep(10.5)
        ctx.shutdown()
        await task

    assert counter.call_count == 2
