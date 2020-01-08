import asyncio
import datetime
from unittest.mock import MagicMock

import dateutil.tz
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
    timemachine = TimeMachine(
        datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=dateutil.tz.UTC)
    )
    monkeypatch.setattr(pyncette.pyncette, "_current_time", timemachine.utcnow)
    monkeypatch.setattr(asyncio, "sleep", timemachine.sleep)
    return timemachine


def test_invalid_configuration():
    app = Pyncette()

    with pytest.raises(
        ValueError, match="One of schedule or interval must be specified"
    ):

        @app.task()
        def _dummy1(context: Context):
            pass

    with pytest.raises(
        ValueError, match="schedule and interval are mutually exclusive"
    ):

        @app.task(interval=datetime.timedelta(seconds=2), schedule="* * * * *")
        def _dummy2(context: Context):
            pass

    with pytest.raises(ValueError, match="Duplicate task name task1"):

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

    with pytest.raises(
        ValueError,
        match="failure_mode is not applicable when execution_mode is BEST_EFFORT",
    ):

        @app.task(
            execution_mode=ExecutionMode.BEST_EFFORT, failure_mode=FailureMode.UNLOCK
        )
        def _dummy6(context: Context):
            pass

    with pytest.raises(
        ValueError, match="Invalid timezone specifier 'Gondwana/Atlantis'."
    ):

        @app.task(schedule="* * * * *", timezone="Gondwana/Atlantis")
        def _dummy7(context: Context):
            pass

    with pytest.raises(
        ValueError, match="Schedule must not be specified on dynamic task definitions"
    ):

        @app.dynamic_task(schedule="* * * * *")
        def _dummy8(context: Context):
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
async def test_context_scheduled_at(timemachine):
    app = Pyncette()

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def once_failing_task(context: Context) -> None:
        counter.offset(context.scheduled_at)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.offset.call_count == 5
    counter.offset.assert_any_call(
        datetime.datetime(2019, 1, 1, 0, 0, 2, tzinfo=dateutil.tz.UTC)
    )
    counter.offset.assert_any_call(
        datetime.datetime(2019, 1, 1, 0, 0, 4, tzinfo=dateutil.tz.UTC)
    )
    counter.offset.assert_any_call(
        datetime.datetime(2019, 1, 1, 0, 0, 6, tzinfo=dateutil.tz.UTC)
    )
    counter.offset.assert_any_call(
        datetime.datetime(2019, 1, 1, 0, 0, 8, tzinfo=dateutil.tz.UTC)
    )
    counter.offset.assert_any_call(
        datetime.datetime(2019, 1, 1, 0, 0, 10, tzinfo=dateutil.tz.UTC)
    )


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


@pytest.mark.asyncio
async def test_timezone_support(timemachine):
    app = Pyncette(poll_interval=datetime.timedelta(minutes=10))

    counter = MagicMock()

    @app.task(schedule="0 0 * * *", timezone="UTC-12")
    async def task1(context: Context) -> None:
        counter.execute_minus_12()

    @app.task(schedule="0 0 * * *", timezone="UTC-6")
    async def task2(context: Context) -> None:
        counter.execute_minus_6()

    @app.task(schedule="0 0 * * *", timezone="UTC+6")
    async def task3(context: Context) -> None:
        counter.execute_plus_6()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(hours=6))
        assert counter.execute_minus_6.call_count == 1
        assert counter.execute_minus_12.call_count == 0
        assert counter.execute_plus_6.call_count == 0

        await timemachine.step(datetime.timedelta(hours=6))
        assert counter.execute_minus_6.call_count == 1
        assert counter.execute_minus_12.call_count == 1
        assert counter.execute_plus_6.call_count == 0

        await timemachine.step(datetime.timedelta(hours=6))
        assert counter.execute_minus_6.call_count == 1
        assert counter.execute_minus_12.call_count == 1
        assert counter.execute_plus_6.call_count == 1

        ctx.shutdown()
        await timemachine.step(datetime.timedelta(minutes=10))
        await task


@pytest.mark.asyncio
async def test_concurrency_limit(timemachine,):
    app = Pyncette(concurrency_limit=1)

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=1), execution_mode=ExecutionMode.BEST_EFFORT
    )
    async def long_running_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(5)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        assert counter.execute.call_count == 2
        ctx.shutdown()
        await timemachine.step(datetime.timedelta(seconds=10))
        await task

    assert counter.execute.call_count == 3  # One extra call after
