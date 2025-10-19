import asyncio
import datetime
from collections.abc import Awaitable
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
from pyncette.errors import TaskLockedException
from pyncette.executor import SynchronousExecutor
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
    app = Pyncette(**backend.get_args(timemachine), poll_interval=datetime.timedelta(seconds=30))

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


@pytest.mark.asyncio
async def test_failed_task_retried_on_every_tick_if_unlock(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), failure_mode=FailureMode.UNLOCK)
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 9


@pytest.mark.asyncio
async def test_unlock_lease_expires(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        lease_duration=datetime.timedelta(seconds=2),
        failure_mode=FailureMode.UNLOCK,
    )
    async def failing_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(3)
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_failed_task_retried_after_lease_over_if_failure_mode_none(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

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
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 4


@pytest.mark.asyncio
async def test_failed_task_not_retried_if_commit_on_failure(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), failure_mode=FailureMode.COMMIT)
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_failed_task_not_retried_if_best_effort(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        execution_mode=ExecutionMode.AT_MOST_ONCE,
    )
    async def failing_task(context: Context) -> None:
        counter.execute()
        raise RuntimeError("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_locked_while_executing(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(5)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 2


@pytest.mark.asyncio
async def test_lease_is_taken_over_if_expired(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

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
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_not_locked_while_executing_if_best_effort_is_used(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        execution_mode=ExecutionMode.AT_MOST_ONCE,
    )
    async def successful_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(5)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_catches_up_with_stale_executions(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

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
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 10


@pytest.mark.asyncio
async def test_context_scheduled_at(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def once_failing_task(context: Context) -> None:
        counter.offset(context.scheduled_at)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.offset.call_count == 5
    counter.offset.assert_any_call(datetime.datetime(2019, 1, 1, 0, 0, 2, tzinfo=dateutil.tz.UTC))
    counter.offset.assert_any_call(datetime.datetime(2019, 1, 1, 0, 0, 4, tzinfo=dateutil.tz.UTC))
    counter.offset.assert_any_call(datetime.datetime(2019, 1, 1, 0, 0, 6, tzinfo=dateutil.tz.UTC))
    counter.offset.assert_any_call(datetime.datetime(2019, 1, 1, 0, 0, 8, tzinfo=dateutil.tz.UTC))
    counter.offset.assert_any_call(datetime.datetime(2019, 1, 1, 0, 0, 10, tzinfo=dateutil.tz.UTC))


@pytest.mark.asyncio
async def test_does_not_catch_up_with_stale_executions_if_fast_forward_used(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=3),
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
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 4


@pytest.mark.asyncio
async def test_fast_forward_cronspec(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(schedule="* * * * * */3", fast_forward=True)
    async def once_long_task(context: Context) -> None:
        counter.execute()
        if counter.execute.call_count == 1:
            await asyncio.sleep(10)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=20))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 3


@pytest.mark.asyncio
async def test_does_not_catch_up_with_stale_executions_if_fast_forward_used_cronspec(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        schedule="* * * * * */3",
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
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 4


@pytest.mark.asyncio
async def test_fixture(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.fixture()
    async def hello(app_context: PyncetteContext):
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
        await task
        await timemachine.unwind()

    assert counter.entered.call_count == 1
    assert counter.executed.call_count == 5
    assert counter.exited.call_count == 1


@pytest.mark.asyncio
async def test_extra_args(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), foo="bar", quux=123)
    async def successful_task(context: Context) -> None:
        counter.foo = context.args["foo"]
        counter.quux = context.args["quux"]

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.foo == "bar"
    assert counter.quux == 123


@pytest.mark.asyncio
async def test_multi_task(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=1), name="task1")
    @app.task(interval=datetime.timedelta(seconds=2), name="task2")
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 15


@pytest.mark.asyncio
async def test_timezone_support(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), poll_interval=datetime.timedelta(hours=1))

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
        await task
        await timemachine.unwind()


@pytest.mark.asyncio
async def test_concurrency_limit(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), concurrency_limit=1)

    counter = MagicMock()
    release = asyncio.Event()

    @app.task(
        interval=datetime.timedelta(seconds=1),
        execution_mode=ExecutionMode.AT_MOST_ONCE,
    )
    async def long_running_task(context: Context) -> None:
        counter.execute()
        await release.wait()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        assert counter.execute.call_count == 1
        ctx.shutdown()
        release.set()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 2  # One extra call after


@pytest.mark.asyncio
async def test_synchronous_executor(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), executor_cls=SynchronousExecutor)

    counter = MagicMock()
    release = asyncio.Event()

    @app.task(
        interval=datetime.timedelta(seconds=1),
        execution_mode=ExecutionMode.AT_MOST_ONCE,
    )
    async def long_running_task(context: Context) -> None:
        counter.execute()
        await release.wait()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        assert counter.execute.call_count == 1
        ctx.shutdown()
        release.set()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_cancelling_run_should_cancel_executing_tasks(
    timemachine,
    backend,
):
    app = Pyncette(**backend.get_args(timemachine), concurrency_limit=1)

    counter = MagicMock()
    release = asyncio.Event()

    @app.task(
        interval=datetime.timedelta(seconds=1),
        execution_mode=ExecutionMode.AT_MOST_ONCE,
    )
    async def long_running_task(context: Context) -> None:
        counter.execute()
        await release.wait()

    with pytest.raises(asyncio.CancelledError):
        async with app.create() as ctx:
            task = asyncio.create_task(ctx.run())
            await timemachine.step(datetime.timedelta(seconds=1.5))
            task.cancel()
            await task

    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_middlewares(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))
    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def task1(context: Context) -> None:
        pass

    @app.task(interval=datetime.timedelta(seconds=2))
    async def task2(context: Context) -> None:
        raise Exception

    @app.middleware
    async def switch1(context: Context, next: Callable[[], Awaitable[None]]):
        c = getattr(counter, context.task.name)
        try:
            c.enter(1)
            await next()
            c.success(1)
        except Exception:  # pragma: no cover
            c.caught(1)

    @app.middleware
    async def switch2(context: Context, next: Callable[[], Awaitable[None]]):
        c = getattr(counter, context.task.name)
        try:
            c.enter(2)
            await next()
            c.success(2)
        except Exception:
            c.caught(2)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=2))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.task1.mock_calls == [
        call.enter(1),
        call.enter(2),
        call.success(2),
        call.success(1),
    ]
    assert counter.task2.mock_calls == [
        call.enter(1),
        call.enter(2),
        call.caught(2),
        call.success(1),
    ]


@pytest.mark.asyncio
async def test_dynamic_successful_task_interval(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

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
        await timemachine.unwind()

    assert counter.execute.call_count == 15


@pytest.mark.asyncio
async def test_dynamic_successful_task_interval_small_batch_size(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), batch_size=1)

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
        await timemachine.unwind()

    assert counter.execute.call_count == 15


@pytest.mark.asyncio
async def test_dynamic_successful_task_interval_invalid_batch_size(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), batch_size=0)
    with pytest.raises(ValueError):
        async with app.create():
            pass  # pragma: no cover


@pytest.mark.asyncio
async def test_dynamic_successful_task_interval_extra_args(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute(context.args["username"])

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.gather(
            ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=2), username="bill"),
            ctx.schedule_task(hello, "2", interval=datetime.timedelta(seconds=2), username="steve"),
            ctx.schedule_task(hello, "3", interval=datetime.timedelta(seconds=2), username="tibor"),
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
        await timemachine.unwind()

    counter.execute.assert_has_calls([call("bill"), call("steve"), call("tibor")], any_order=True)


@pytest.mark.asyncio
async def test_dynamic_execute_once(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

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
        await timemachine.unwind()

    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_dynamic_execute_once_unlock(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()
        await context.app_context.unschedule_task(context.task)
        raise Exception("Oops")

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=1))
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_dynamic_register_again(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=1))
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=2))
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_dynamic_poll_after_unregister(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()  # pragma: no cover

    async with app.create() as ctx:
        task_instance = await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=1))
        await ctx.unschedule_task(hello, "1")

        with pytest.raises(PyncetteException, match="not found"):
            await ctx._repository.poll_task(timemachine.utcnow(), task_instance)

        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=60))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 0


@pytest.mark.asyncio
async def test_persistence(timemachine, backend):
    if not backend.is_persistent:
        pytest.skip("This test requires persistence.")

    app = Pyncette(**backend.get_args(timemachine))
    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        await ctx.schedule_task(hello, "1", schedule="* * * * *")

    await timemachine.jump_to(datetime.timedelta(seconds=30))
    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step()
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 0

    await timemachine.jump_to(datetime.timedelta(seconds=60))
    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step()
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_coordination(timemachine, backend):
    if not backend.is_persistent:
        pytest.skip("This test requires persistence.")

    args = backend.get_args(timemachine)
    app1 = Pyncette(**args)
    app2 = Pyncette(**args)

    counter = MagicMock()

    @app1.task(interval=datetime.timedelta(seconds=2))
    @app2.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app1.create() as ctx1, app2.create() as ctx2:
        task1 = asyncio.create_task(ctx1.run())
        task2 = asyncio.create_task(ctx2.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx1.shutdown()
        ctx2.shutdown()
        await asyncio.gather(task1, task2)
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_dynamic_coordination(timemachine, backend):
    if not backend.is_persistent:
        pytest.skip("This test requires persistence.")

    args = backend.get_args(timemachine)
    app1 = Pyncette(**args)
    app2 = Pyncette(**args)

    counter = MagicMock()

    @app1.dynamic_task()
    @app2.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    async with app1.create() as ctx1, app2.create() as ctx2:
        task1 = asyncio.create_task(ctx1.run())
        task2 = asyncio.create_task(ctx2.run())
        await ctx1.schedule_task(hello, "1", interval=datetime.timedelta(seconds=2))
        await timemachine.step(datetime.timedelta(seconds=10))
        await ctx2.unschedule_task(hello, "1")
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx1.shutdown()
        ctx2.shutdown()
        await asyncio.gather(task1, task2)
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_dynamic_default_args(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task(username="default")
    async def hello1(context: Context) -> None:
        counter.task1.execute(context.args["username"])

    @app.dynamic_task(interval=datetime.timedelta(seconds=1))
    async def hello2(context: Context) -> None:
        counter.task2.execute(context.args["username"])

    @app.dynamic_task(schedule="* * * * * *")
    async def hello3(context: Context) -> None:
        counter.task3.execute(context.args["username"])

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello1, "1", interval=datetime.timedelta(seconds=1), username="rajeev")
        await ctx.schedule_task(hello1, "2", interval=datetime.timedelta(seconds=1), username="jeethu")
        await ctx.schedule_task(hello1, "3", interval=datetime.timedelta(seconds=1))
        await ctx.schedule_task(hello2, "4", username="imaana")
        await ctx.schedule_task(hello3, "5", username="laibuta")
        await timemachine.step(datetime.timedelta(seconds=1))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    counter.task1.execute.assert_has_calls([call("rajeev"), call("jeethu"), call("default")], any_order=True)
    counter.task2.execute.assert_has_calls([call("imaana")], any_order=True)
    counter.task3.execute.assert_has_calls([call("laibuta")], any_order=True)


@pytest.mark.asyncio
async def test_execute_at(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    now = timemachine.utcnow()
    async with app.create() as ctx:
        await asyncio.gather(
            ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=1)),
            ctx.schedule_task(hello, "2", execute_at=now + datetime.timedelta(seconds=2)),
            ctx.schedule_task(hello, "3", execute_at=now + datetime.timedelta(seconds=3)),
        )
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 3


@pytest.mark.asyncio
async def test_execute_at_retry(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task(failure_mode=FailureMode.UNLOCK)
    async def hello(context: Context) -> None:
        getattr(counter, context.task.name).execute()
        raise RuntimeError("Oops")

    now = timemachine.utcnow()
    async with app.create() as ctx:
        await asyncio.gather(
            ctx.schedule_task(hello, "task1", execute_at=now + datetime.timedelta(seconds=1)),
            ctx.schedule_task(hello, "task2", execute_at=now + datetime.timedelta(seconds=2)),
            ctx.schedule_task(hello, "task3", execute_at=now + datetime.timedelta(seconds=3)),
        )
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=5))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.task1.execute.call_count == 5
    assert counter.task2.execute.call_count == 4
    assert counter.task3.execute.call_count == 3


@pytest.mark.asyncio
async def test_execute_at_at_most_once(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task(execution_mode=ExecutionMode.AT_MOST_ONCE)
    async def hello(context: Context) -> None:
        counter.execute()

    now = timemachine.utcnow()
    async with app.create() as ctx:
        await asyncio.gather(
            ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=1)),
            ctx.schedule_task(hello, "2", execute_at=now + datetime.timedelta(seconds=2)),
            ctx.schedule_task(hello, "3", execute_at=now + datetime.timedelta(seconds=3)),
        )
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 3


@pytest.mark.asyncio
async def test_lease_is_not_lost_if_heartbeating(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=3),
        lease_duration=datetime.timedelta(seconds=2),
    )
    async def successful_task(context: Context) -> None:
        counter.execute()
        for _ in range(6):
            await asyncio.sleep(1)
            await context.heartbeat()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 2


@pytest.mark.asyncio
async def test_heartbeating_noop_on_best_effort_tasks(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=6),
        execution_mode=ExecutionMode.AT_MOST_ONCE,
    )
    async def successful_task(context: Context) -> None:
        counter.lease(context._lease)
        await context.heartbeat()
        counter.lease(context._lease)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    lease1 = counter.lease.call_args_list[0][0][0]
    lease2 = counter.lease.call_args_list[1][0][0]

    assert lease1 is lease2


@pytest.mark.asyncio
async def test_heartbeat_fails_if_lease_lost(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.task(
        interval=datetime.timedelta(seconds=2),
        lease_duration=datetime.timedelta(seconds=1),
    )
    async def successful_task(context: Context) -> None:
        await asyncio.sleep(5)
        try:
            await context.heartbeat()
            counter.successes()
        except LeaseLostException:
            counter.failures()
            raise

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.failures.call_count == 8
    assert counter.successes.call_count == 1


@pytest.mark.asyncio
async def test_automatic_heartbeating_lease_expired(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    leases = []

    @app.task(
        interval=datetime.timedelta(seconds=1),
        lease_duration=datetime.timedelta(seconds=1),
    )
    @with_heartbeat(cancel_on_lease_lost=False)
    async def successful_task(context: Context) -> None:
        counter.started()

        # Fake getting an old lease for the 2nd execution
        leases.append(context._lease)
        context._lease = leases[0]

        await asyncio.sleep(6)
        counter.finished()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.started.call_count == 4
    assert counter.finished.call_count == 4


@pytest.mark.asyncio
async def test_automatic_heartbeating_cancel_on_lease_expired(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    leases = []

    @app.task(
        interval=datetime.timedelta(seconds=1),
        lease_duration=datetime.timedelta(seconds=1),
    )
    @with_heartbeat(cancel_on_lease_lost=True)
    async def successful_task(context: Context) -> None:
        counter.started()

        # Fake getting an old lease for the 2nd execution
        leases.append(context._lease)
        context._lease = leases[0]

        await asyncio.sleep(6)
        counter.finished()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.started.call_count == 4
    assert counter.finished.call_count == 1


@pytest.mark.asyncio
async def test_heartbeating_after_task_deleted(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()
        await context.app_context.unschedule_task(context.task)
        try:
            await context.heartbeat()
        except Exception:
            counter.failure()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=1))
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 1
    assert counter.failure.call_count == 1


PARTITION_TASK_NAMES = [
    "festive_raman",
    "sad_germain",
    "determined_nash",
    "compassionate_chandrasekhar",
    "suspicious_matsumoto",
]


@pytest.mark.asyncio
async def test_partitioned_successful_task_interval(timemachine, backend):
    timemachine.spin_iterations = 20

    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.partitioned_task(partition_count=5)
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.gather(*[ctx.schedule_task(hello, name, interval=datetime.timedelta(seconds=2)) for name in PARTITION_TASK_NAMES])
        await timemachine.step(datetime.timedelta(seconds=10))
        await asyncio.gather(*[ctx.unschedule_task(hello, name) for name in PARTITION_TASK_NAMES])
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 25


@pytest.mark.asyncio
async def test_partitioned_successful_task_interval_selective(timemachine, backend):
    timemachine.spin_iterations = 20

    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.partitioned_task(partition_count=5, enabled_partitions=[0, 1])
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await asyncio.gather(*[ctx.schedule_task(hello, name, interval=datetime.timedelta(seconds=2)) for name in PARTITION_TASK_NAMES])
        await timemachine.step(datetime.timedelta(seconds=10))
        await asyncio.gather(*[ctx.unschedule_task(hello, name) for name in PARTITION_TASK_NAMES])
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 10


@pytest.mark.asyncio
async def test_disabled_task(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))
    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2), enabled=True)
    async def successful_task_1(context: Context) -> None:
        counter.execute()

    @app.task(interval=datetime.timedelta(seconds=2), enabled=False)
    async def successful_task_2(context: Context) -> None:
        counter.execute()  # pragma: no cover

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_disabled_dynamic_task(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task(enabled=True)
    async def hello_1(context: Context) -> None:
        counter.execute()

    @app.dynamic_task(enabled=False)
    async def hello_2(context: Context) -> None:
        counter.execute()  # pragma: no cover

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello_1, "1", interval=datetime.timedelta(seconds=2))
        await ctx.schedule_task(hello_2, "1", interval=datetime.timedelta(seconds=2))
        await timemachine.step(datetime.timedelta(seconds=10))
        await ctx.unschedule_task(hello_1, "1")
        await ctx.unschedule_task(hello_2, "1")
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_disabled_partitioned_task(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.partitioned_task(partition_count=1, enabled=True)
    async def hello_1(context: Context) -> None:
        counter.execute()  # pragma: no cover

    @app.partitioned_task(partition_count=1, enabled=False)
    async def hello_2(context: Context) -> None:
        counter.execute()  # pragma: no cover

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(hello_1, "1", interval=datetime.timedelta(seconds=2))
        await ctx.schedule_task(hello_2, "1", interval=datetime.timedelta(seconds=2))
        await timemachine.step(datetime.timedelta(seconds=10))
        await ctx.unschedule_task(hello_1, "1")
        await ctx.unschedule_task(hello_2, "1")
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 5


@pytest.mark.asyncio
async def test_schedule_task_fails_when_locked_without_force(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def long_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(30)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(long_task, "1", interval=datetime.timedelta(seconds=2))
        await timemachine.step(datetime.timedelta(seconds=5))

        # Task should be executing now, so re-scheduling should fail
        try:
            with pytest.raises(TaskLockedException):
                await ctx.schedule_task(long_task, "1", interval=datetime.timedelta(seconds=10))
        finally:
            ctx.shutdown()
            await task
            await timemachine.unwind()

    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_schedule_task_force_overrides_lock(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def long_task(context: Context) -> None:
        counter.execute()
        await asyncio.sleep(30)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())
        await ctx.schedule_task(long_task, "1", interval=datetime.timedelta(seconds=2))
        await timemachine.step(datetime.timedelta(seconds=5))

        # Task should be executing, but force=True should override
        await ctx.schedule_task(long_task, "1", interval=datetime.timedelta(seconds=10), force=True)

        # Step forward to allow new schedule to execute
        await timemachine.step(datetime.timedelta(seconds=15))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Original task execution + new execution after force override
    assert counter.execute.call_count >= 2


@pytest.mark.asyncio
async def test_schedule_task_preserves_sooner_time_when_updating_to_later(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    now = timemachine.utcnow()
    async with app.create() as ctx:
        # Schedule to execute in 5 seconds
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=5))

        # Update to execute in 1 minute (later)
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(minutes=1))

        task = asyncio.create_task(ctx.run())
        # Step forward 10 seconds - should execute at original 5 second mark
        await timemachine.step(datetime.timedelta(seconds=10))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Should have executed at the sooner time (5 seconds)
    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_schedule_task_preserves_sooner_time_when_updating_to_sooner(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    now = timemachine.utcnow()
    async with app.create() as ctx:
        # Schedule to execute in 1 minute
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(minutes=1))

        # Update to execute in 5 seconds (sooner)
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=5))

        task = asyncio.create_task(ctx.run())
        # Step forward 10 seconds - should execute at the sooner time
        await timemachine.step(datetime.timedelta(seconds=10))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Should have executed at the sooner time (5 seconds)
    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_schedule_task_force_uses_new_time_not_min(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    now = timemachine.utcnow()
    async with app.create() as ctx:
        # Schedule to execute in 5 seconds
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=5))

        # Force update to execute in 1 minute (later) - should use new time, not MIN
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(minutes=1), force=True)

        task = asyncio.create_task(ctx.run())
        # Step forward 10 seconds - should NOT execute (new time is 1 minute)
        await timemachine.step(datetime.timedelta(seconds=10))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Should NOT have executed yet (new schedule is 1 minute)
    assert counter.execute.call_count == 0


@pytest.mark.asyncio
async def test_schedule_task_updates_spec_while_preserving_schedule(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute(context.args.get("username", "default"))

    now = timemachine.utcnow()
    async with app.create() as ctx:
        # Schedule with username "alice" to execute in 5 seconds
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=5), username="alice")

        # Update username to "bob" with later execution time
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(minutes=1), username="bob")

        task = asyncio.create_task(ctx.run())
        # Step forward 10 seconds - should execute at sooner time (5s) with updated spec (bob)
        await timemachine.step(datetime.timedelta(seconds=10))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Should have executed with updated username
    counter.execute.assert_called_once_with("bob")


@pytest.mark.asyncio
async def test_schedule_task_interval_preserves_sooner_schedule(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())

        # Schedule with 2 second interval
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=2))

        # Immediately reschedule with 10 second interval (longer)
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=10))

        # Step forward 5 seconds - should execute at 2 second mark (original sooner schedule)
        await timemachine.step(datetime.timedelta(seconds=5))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Should have executed 1 time (at 2s), next execution uses new interval (would be at 12s)
    assert counter.execute.call_count == 1


@pytest.mark.asyncio
async def test_schedule_task_after_one_time_task_completes(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    now = timemachine.utcnow()
    async with app.create() as ctx:
        # Schedule as one-time task
        await ctx.schedule_task(hello, "1", execute_at=now + datetime.timedelta(seconds=2))

        task = asyncio.create_task(ctx.run())

        # Let it execute and complete
        await timemachine.step(datetime.timedelta(seconds=5))
        assert counter.execute.call_count == 1

        # Now reschedule the same task with a recurring interval
        await ctx.schedule_task(hello, "1", interval=datetime.timedelta(seconds=3))

        # Step forward - should execute again with new schedule
        await timemachine.step(datetime.timedelta(seconds=5))

        ctx.shutdown()
        await task
        await timemachine.unwind()

    # Should have executed original one-time + at least one recurring execution
    assert counter.execute.call_count >= 2


@pytest.mark.asyncio
async def test_get_task_existing(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Schedule a task
        await ctx.schedule_task(hello, "test_instance", interval=datetime.timedelta(seconds=5), foo="bar")

        # Get the task state
        task_state = await ctx.get_task(hello, "test_instance")

        # Verify task state
        assert task_state is not None
        assert task_state.task.name == "test_instance"
        assert task_state.task.extra_args.get("foo") == "bar"
        assert task_state.scheduled_at is not None
        assert task_state.locked_until is None
        assert task_state.locked_by is None

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_get_task_nonexistent(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Try to get a task that doesn't exist
        task_state = await ctx.get_task(hello, "nonexistent")

        # Should return None
        assert task_state is None

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_get_task_static(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), poll_interval=datetime.timedelta(seconds=1))

    @app.task(interval=datetime.timedelta(seconds=5))
    async def regular_task(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Start the scheduler to initialize the task state
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=1))

        # Query the static task state - this should work
        task_state = await ctx.get_task(regular_task)

        # Should return a TaskState
        assert task_state is not None
        assert task_state.task == regular_task
        assert task_state.scheduled_at is not None

        # Providing instance_name with a static task should raise ValueError
        with pytest.raises(ValueError, match="instance_name provided but task is not dynamic"):
            await ctx.get_task(regular_task, "instance")

        ctx.shutdown()
        await task
        await timemachine.unwind()


@pytest.mark.asyncio
async def test_get_task_dynamic_concrete(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Schedule a task
        await ctx.schedule_task(hello, "test1", interval=datetime.timedelta(seconds=5))

        # Get the concrete instance
        concrete_task = hello.instantiate("test1", interval=datetime.timedelta(seconds=5))

        # Query using the concrete instance (no instance_name)
        task_state = await ctx.get_task(concrete_task)

        # Should return the task state
        assert task_state is not None
        assert task_state.task.parent_task == hello
        assert task_state.task.name == "test1"

        # Providing instance_name with a concrete instance should raise ValueError
        with pytest.raises(ValueError, match="Cannot provide instance_name with already-instantiated dynamic task"):
            await ctx.get_task(concrete_task, "test1")

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_list_tasks_empty(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # List tasks when none exist
        response = await ctx.list_tasks(hello)

        # Should return empty list
        assert response.tasks == []
        assert response.continuation_token is None

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_list_tasks_multiple(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Schedule multiple tasks
        await asyncio.gather(
            ctx.schedule_task(hello, "task1", interval=datetime.timedelta(seconds=5), foo="bar1"),
            ctx.schedule_task(hello, "task2", interval=datetime.timedelta(seconds=10), foo="bar2"),
            ctx.schedule_task(hello, "task3", interval=datetime.timedelta(seconds=15), foo="bar3"),
        )

        # List all tasks
        response = await ctx.list_tasks(hello)

        # Should return all 3 tasks
        assert len(response.tasks) == 3

        # Verify task details (order not guaranteed)
        task_names = {task.task.name for task in response.tasks}
        assert task_names == {"task1", "task2", "task3"}

        # Verify extra args are preserved
        for task_state in response.tasks:
            assert "foo" in task_state.task.extra_args

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_list_tasks_pagination(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Schedule 5 tasks
        await asyncio.gather(*[ctx.schedule_task(hello, f"task{i}", interval=datetime.timedelta(seconds=5)) for i in range(5)])

        # List with limit of 2
        response = await ctx.list_tasks(hello, limit=2)

        # Should return 2 tasks
        assert len(response.tasks) == 2

        all_tasks = response.tasks.copy()
        while response.continuation_token is not None:
            response = await ctx.list_tasks(hello, limit=2, continuation_token=response.continuation_token)
            all_tasks.extend(response.tasks)

        # Should eventually get all 5 tasks
        assert len(all_tasks) == 5

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_list_tasks_non_dynamic_fails(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

    @app.task(interval=datetime.timedelta(seconds=5))
    async def regular_task(context: Context) -> None:
        pass

    async with app.create() as ctx:
        # Should raise ValueError for non-dynamic task
        with pytest.raises(ValueError, match="Cannot list instances of non-dynamic task"):
            await ctx.list_tasks(regular_task)

        await timemachine.unwind()


@pytest.mark.asyncio
async def test_get_task_locked_state(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine), poll_interval=datetime.timedelta(seconds=1))

    @app.dynamic_task()
    async def slow_task(context: Context) -> None:
        await asyncio.sleep(30)

    async with app.create() as ctx:
        task = asyncio.create_task(ctx.run())

        # Schedule a task that will run for a while
        await ctx.schedule_task(slow_task, "test", interval=datetime.timedelta(seconds=5))

        await timemachine.step(datetime.timedelta(seconds=10))

        # Query the task state while it's locked
        task_state = await ctx.get_task(slow_task, "test")

        # Should show as locked
        assert task_state is not None
        assert task_state.locked_until is not None
        assert task_state.locked_by is not None

        ctx.shutdown()
        await task
        await timemachine.unwind()
