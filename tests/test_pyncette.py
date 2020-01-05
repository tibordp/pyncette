import asyncio
import datetime
from unittest.mock import MagicMock

import pytest
from croniter.croniter import CroniterBadCronError
from timemachine import TimeMachine

import pyncette
from pyncette import Context
from pyncette import ExecutionMode
from pyncette import Pyncette


@pytest.fixture
def timemachine(monkeypatch):
    timemachine = TimeMachine(datetime.datetime(2019, 1, 1, 0, 0, 0))
    monkeypatch.setattr(pyncette.pyncette, "current_time", timemachine.utcnow)
    monkeypatch.setattr(asyncio, "sleep", timemachine.sleep)
    return timemachine


def test_invalid_configuration():
    app = Pyncette()

    with pytest.raises(ValueError):

        @app.task()
        def _dummy1(context: Context):
            pass

    with pytest.raises(ValueError):

        @app.task(interval=datetime.timedelta(seconds=2), schedule="* * * * *")
        def _dummy2(context: Context):
            pass

    with pytest.raises(CroniterBadCronError):

        @app.task(schedule="abracadabra")
        def _dummy3(context: Context):
            pass

    with pytest.raises(ValueError):

        @app.task(execution_mode=ExecutionMode.BEST_EFFORT, commit_on_failure=True)
        def _dummy4(context: Context):
            pass


@pytest.mark.asyncio
async def test_successful_task_interval(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter()

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 5


@pytest.mark.asyncio
async def test_successful_task_cronspec(timemachine):
    app = Pyncette(poll_interval=datetime.timedelta(seconds=30))

    counter = MagicMock()

    @app.task(schedule="* * * * *")
    async def successful_task(context: Context) -> None:
        counter()

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(minutes=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=60))
    await task

    assert counter.call_count == 10


@pytest.mark.asyncio
async def test_failed_task_retried_on_every_tick(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def failing_task(context: Context) -> None:
        counter()
        raise RuntimeError("Oops")

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 9


@pytest.mark.asyncio
async def test_failed_task_not_retried_if_commit_on_failure(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), commit_on_failure=True)
    async def failing_task(context: Context) -> None:
        counter()
        raise RuntimeError("Oops")

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 5


@pytest.mark.asyncio
async def test_failed_task_not_retried_if_best_effort(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2), execution_mode=ExecutionMode.BEST_EFFORT
    )
    async def failing_task(context: Context) -> None:
        counter()
        raise RuntimeError("Oops")

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 5


@pytest.mark.asyncio
async def test_locked_while_executing(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter()
        await asyncio.sleep(5)

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 2


@pytest.mark.asyncio
async def test_not_locked_while_executing_if_best_effort_is_used(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2), execution_mode=ExecutionMode.BEST_EFFORT
    )
    async def successful_task(context: Context) -> None:
        counter()
        await asyncio.sleep(5)

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=10))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 5


@pytest.mark.asyncio
async def test_catches_up_with_stale_executions(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def once_failing_task(context: Context) -> None:
        counter()
        if counter.call_count == 1:
            await asyncio.sleep(10)
            raise RuntimeError("Oops")

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=20))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 10


@pytest.mark.asyncio
async def test_does_not_catch_up_with_stale_executions_if_fast_forward_used(
    timemachine,
):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), fast_forward=True)
    async def once_failing_task(context: Context) -> None:
        counter()
        if counter.call_count == 1:
            await asyncio.sleep(10)
            raise RuntimeError("Oops")

    task = asyncio.create_task(app.run())
    await timemachine.step(datetime.timedelta(seconds=20))
    app.shutdown()
    await timemachine.step(datetime.timedelta(seconds=10))
    await task

    assert counter.call_count == 7
