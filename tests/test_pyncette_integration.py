import asyncio
import datetime
from typing import Awaitable
from typing import Callable
from unittest.mock import MagicMock
from unittest.mock import call

import dateutil.tz
import pytest

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette import PyncetteContext
from pyncette.errors import LeaseLostException
from pyncette.errors import PyncetteException
from pyncette.utils import with_heartbeat


@pytest.mark.asyncio
async def test_successful_task_interval(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))
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

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_successful_task_cronspec(timemachine, backend):
    app = Pyncette(
        **backend.get_args(timemachine), poll_interval=datetime.timedelta(seconds=30)
    )

    counter = MagicMock()

    @app.task(schedule="* * * * *")
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(minutes=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 10
