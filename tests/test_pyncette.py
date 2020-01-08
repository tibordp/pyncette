import asyncio
import datetime
from unittest.mock import MagicMock

import pytest
from croniter.croniter import CroniterBadCronError
from timemachine import TimeMachine

import pyncette
from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette


@pytest.fixture
def timemachine(monkeypatch):
    timemachine = TimeMachine(datetime.datetime(2019, 1, 1, 0, 0, 0))
    monkeypatch.setattr(pyncette.pyncette, "_current_time", timemachine.utcnow)
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

    with pytest.raises(ValueError):

        @app.task(interval=datetime.timedelta(seconds=2), name="task1")
        def _dummy3(context: Context):
            pass

        @app.task(interval=datetime.timedelta(seconds=2), name="task1")
        def _dummy4(context: Context):
            pass

    with pytest.raises(CroniterBadCronError):

        @app.task(schedule="abracadabra")
        def _dummy5(context: Context):
            pass

    with pytest.raises(ValueError):

        @app.task(
            execution_mode=ExecutionMode.BEST_EFFORT, failure_mode=FailureMode.UNLOCK
        )
        def _dummy6(context: Context):
            pass


@pytest.mark.asyncio
async def test_successful_task_interval(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_successful_task_cronspec(timemachine):
    app = Pyncette(poll_interval=datetime.timedelta(seconds=30))

    counter = MagicMock()

    @app.task(schedule="* * * * *")
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(minutes=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=60))
        await task

    assert counter.execute.call_count == 10


@pytest.mark.asyncio
async def test_failed_task_retried_on_every_tick_if_unlock(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), failure_mode=FailureMode.UNLOCK)
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 9


@pytest.mark.asyncio
async def test_failed_task_retried_after_lease_over_if_failure_mode_none(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        lease_duration=datetime.timedelta(seconds=5),
    )
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=20))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 4


@pytest.mark.asyncio
async def test_failed_task_not_retried_if_commit_on_failure(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), failure_mode=FailureMode.COMMIT)
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_failed_task_not_retried_if_best_effort(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2), execution_mode=ExecutionMode.BEST_EFFORT
    )
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_locked_while_executing(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(5)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 2


@pytest.mark.asyncio
async def test_lease_is_taken_over_if_expired(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        lease_duration=datetime.timedelta(seconds=2),
    )
    async def successful_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(10)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_not_locked_while_executing_if_best_effort_is_used(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2), execution_mode=ExecutionMode.BEST_EFFORT
    )
    async def successful_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(5)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_catches_up_with_stale_executions(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), failure_mode=FailureMode.UNLOCK)
    async def once_failing_task(context: Context) -> None:
        counter.execute()
        if counter.execute.call_count == 1:
            await asyncio.sleep(10)
            raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=20))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 10


@pytest.mark.asyncio
async def test_does_not_catch_up_with_stale_executions_if_fast_forward_used(
    timemachine,
):
    app = Pyncette()

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        fast_forward=True,
        failure_mode=FailureMode.UNLOCK,
    )
    async def once_failing_task(context: Context) -> None:
        counter.execute()
        if counter.execute.call_count == 1:
            await asyncio.sleep(10)
            raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=20))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 7


@pytest.mark.asyncio
async def test_fixture(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.fixture()
    async def hello():
        counter.entered()
        yield counter.executed
        counter.exited()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        context.hello()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.entered.call_count == 1
    assert counter.executed.call_count == 5
    assert counter.exited.call_count == 1


@pytest.mark.asyncio
async def test_extra_args(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), foo="bar", quux=123)
    async def successful_task(context: Context) -> None:
        counter.foo = context.foo
        counter.quux = context.quux

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.foo == "bar"
    assert counter.quux == 123


@pytest.mark.asyncio
async def test_multi_task(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=1), name="task1")
    @app.task(interval=datetime.timedelta(seconds=2), name="task2")
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 15
