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
from pyncette.errors import PyncetteException


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
async def test_failed_task_retried_after_lease_over_if_failure_mode_none(
    timemachine, backend
):
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
    timemachine, backend
):
    app = Pyncette(**backend.get_args(timemachine))

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
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 7


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
    app = Pyncette(
        **backend.get_args(timemachine), poll_interval=datetime.timedelta(hours=1)
    )

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
async def test_cancelling_run_should_cancel_executing_tasks(
    timemachine, backend,
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
        raise Exception()

    @app.middleware
    async def switch1(context: Context, next: Callable[[], Awaitable[None]]):
        c = getattr(counter, context.task.name)
        try:
            c.enter(1)
            await next()
            c.success(1)
        except:  # noqa: E722
            c.caught(1)

    @app.middleware
    async def switch2(context: Context, next: Callable[[], Awaitable[None]]):
        c = getattr(counter, context.task.name)
        try:
            c.enter(2)
            await next()
            c.success(2)
        except:  # noqa: E722
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
async def test_dynamic_successful_task_interval_extra_args(timemachine, backend):
    app = Pyncette(**backend.get_args(timemachine))

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
        await timemachine.unwind()

    counter.execute.assert_has_calls(
        [call("bill"), call("steve"), call("tibor")], any_order=True
    )


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
        counter.execute()

    async with app.create() as ctx:
        task_instance = await ctx.schedule_task(
            hello, "1", interval=datetime.timedelta(seconds=1)
        )
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

    app = Pyncette(**backend.get_args(timemachine))
    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx1, app.create() as ctx2:
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

    app = Pyncette(**backend.get_args(timemachine))
    counter = MagicMock()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        counter.execute()

    async with app.create() as ctx1, app.create() as ctx2:
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
        await ctx.schedule_task(
            hello1, "1", interval=datetime.timedelta(seconds=1), username="rajeev"
        )
        await ctx.schedule_task(
            hello1, "2", interval=datetime.timedelta(seconds=1), username="jeethu"
        )
        await ctx.schedule_task(hello1, "3", interval=datetime.timedelta(seconds=1))
        await ctx.schedule_task(hello2, "4", username="imaana")
        await ctx.schedule_task(hello3, "5", username="laibuta")
        await timemachine.step(datetime.timedelta(seconds=1))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    counter.task1.execute.assert_has_calls(
        [call("rajeev"), call("jeethu"), call("default")], any_order=True
    )
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
            ctx.schedule_task(
                hello, "1", execute_at=now + datetime.timedelta(seconds=1)
            ),
            ctx.schedule_task(
                hello, "2", execute_at=now + datetime.timedelta(seconds=2)
            ),
            ctx.schedule_task(
                hello, "3", execute_at=now + datetime.timedelta(seconds=3)
            ),
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
            ctx.schedule_task(
                hello, "task1", execute_at=now + datetime.timedelta(seconds=1)
            ),
            ctx.schedule_task(
                hello, "task2", execute_at=now + datetime.timedelta(seconds=2)
            ),
            ctx.schedule_task(
                hello, "task3", execute_at=now + datetime.timedelta(seconds=3)
            ),
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
            ctx.schedule_task(
                hello, "1", execute_at=now + datetime.timedelta(seconds=1)
            ),
            ctx.schedule_task(
                hello, "2", execute_at=now + datetime.timedelta(seconds=2)
            ),
            ctx.schedule_task(
                hello, "3", execute_at=now + datetime.timedelta(seconds=3)
            ),
        )
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.execute.call_count == 3
