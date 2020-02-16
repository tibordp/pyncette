# flake8: noqa

import asyncio
import datetime
import time
from unittest.mock import MagicMock

import dateutil.tz
import pytest
from timemachine import timemachine

import pyncette
from pyncette import Context
from pyncette import Pyncette


@pytest.mark.asyncio
async def test_successful_task_interval_dynamic(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.gather(
            ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=2)),
            ctx.schedule_task(hello, "2", interval=datetime.timedelta(seconds=2)),
            ctx.schedule_task(hello, "3", interval=datetime.timedelta(seconds=2)),
        )
        await timemachine.step(datetime.timedelta(seconds=10))
        await asyncio.gather(
            ctx.unschedule_task(hello, "1"),
            ctx.unschedule_task(hello, "2"),
            ctx.unschedule_task(hello, "3"),
        )
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.close()

    assert counter.execute.call_count == 12


@pytest.mark.asyncio
async def test_successful_task_interval_dynamic_extra_args(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute(context.args["username"])

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.gather(
            ctx.schedule_task(
                hello, "1", interval=datetime.timedelta(seconds=2), username="bill"
            ),
            ctx.schedule_task(
                hello, "2", interval=datetime.timedelta(seconds=2), username="steve"
            ),
            ctx.schedule_task(
                hello, "3", interval=datetime.timedelta(seconds=2), username="tibor"
            ),
        )
        await timemachine.step(datetime.timedelta(seconds=10))
        await asyncio.gather(
            ctx.unschedule_task(hello, "1"),
            ctx.unschedule_task(hello, "2"),
            ctx.unschedule_task(hello, "3"),
        )
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.close()

    counter.execute.assert_any_call("bill")
    counter.execute.assert_any_call("steve")
    counter.execute.assert_any_call("tibor")


@pytest.mark.asyncio
async def test_execute_once(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()
        await context.app_context.unschedule_task(context.task)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=1))
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.close()

    assert counter.execute.call_count == 1
