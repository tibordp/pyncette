import asyncio
import datetime
from unittest.mock import MagicMock

import pytest
from croniter.croniter import CroniterBadCronError

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette import PyncetteContext
from pyncette.errors import LeaseLostException
from pyncette.task import _default_partition_selector
from pyncette.utils import with_heartbeat

from conftest import DefaultBackend


def test_invalid_configuration():
    async def dummy(context: Context):
        pass  # pragma: no cover

    # Exactly one of the following must be specified: schedule, interval, execute_at
    with pytest.raises(ValueError):
        app = Pyncette()
        app.task()(dummy)

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task(execute_at=datetime.datetime.now(tz=datetime.timezone.utc))(dummy)

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task(interval=datetime.timedelta(seconds=2), schedule="* * * * *")(dummy)

    with pytest.raises(ValueError, match="Duplicate task name"):
        app = Pyncette()
        app.task(interval=datetime.timedelta(seconds=2), name="task1")(dummy)
        app.task(interval=datetime.timedelta(seconds=2), name="task1")(dummy)

    with pytest.raises(CroniterBadCronError):
        app = Pyncette()
        app.task(schedule="abracadabra")(dummy)

    with pytest.raises(
        ValueError,
        match="failure_mode is not applicable when execution_mode is AT_MOST_ONCE",
    ):
        app = Pyncette()
        app.task(execution_mode=ExecutionMode.AT_MOST_ONCE, failure_mode=FailureMode.UNLOCK)(dummy)

    with pytest.raises(ValueError, match=r"Invalid timezone specifier 'Gondwana/Atlantis'"):
        app = Pyncette()
        app.task(schedule="* * * * *", timezone="Gondwana/Atlantis")(dummy)

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task(interval=datetime.timedelta(seconds=2), timezone="Europe/Dublin")(dummy)

    with pytest.raises(ValueError, match="Extra parameters must be JSON serializable"):
        app = Pyncette()
        app.task(schedule="* * * * *", extra_arg=object())(dummy)

    with pytest.raises(ValueError, match="Unable to determine name for the task"):
        app = Pyncette()
        app.task(schedule="* * * * *")(object())  # ty: ignore[invalid-argument-type]

    with pytest.raises(ValueError, match="Unable to determine name for the fixture"):
        app = Pyncette()
        app.fixture()(object())


def test_instantiate_non_dynamic_task():
    async def dummy(context: Context):
        pass  # pragma: no cover

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task(schedule="* * * * *")(dummy).instantiate(name="foo")


def test_heartbeat_invalid_configuration():
    async def dummy(context: Context):
        pass  # pragma: no cover

    with pytest.raises(ValueError):
        with_heartbeat(lease_remaining_ratio=-1)

    with pytest.raises(ValueError):
        with_heartbeat(lease_remaining_ratio=2)


@pytest.mark.asyncio
async def test_dynamic_successful_task_interval():
    app = Pyncette()

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        pass  # pragma: no cover

    with pytest.raises(ValueError, match="instance name must be provided"):
        async with app.create() as ctx:
            await ctx.unschedule_task(hello)


@pytest.mark.asyncio
async def test_continues_heartbeating_after_exception(timemachine):
    context = MagicMock()
    counter = MagicMock()

    async def _heartbeat():
        counter.heartbeat()
        raise Exception("Fail")

    context.heartbeat = _heartbeat
    context.task.lease_duration = datetime.timedelta(seconds=2)

    @with_heartbeat()
    async def hello(context: Context) -> None:
        await asyncio.sleep(10)

    task = asyncio.create_task(hello(context))
    await timemachine.step(datetime.timedelta(seconds=10))
    await task
    await timemachine.unwind()

    assert counter.heartbeat.call_count == 9


@pytest.mark.asyncio
async def test_stops_heartbeating_if_lease_lost(timemachine):
    context = MagicMock()
    counter = MagicMock()

    async def _heartbeat():
        counter.heartbeat()
        raise LeaseLostException(context.task)

    context.heartbeat = _heartbeat
    context.task.lease_duration = datetime.timedelta(seconds=2)

    @with_heartbeat()
    async def hello(context: Context) -> None:
        await asyncio.sleep(10)

    task = asyncio.create_task(hello(context))
    await timemachine.step(datetime.timedelta(seconds=10))
    await task
    await timemachine.unwind()

    assert counter.heartbeat.call_count == 1


def test_fixture_name_invalid():
    app = Pyncette()

    async def dummy(app_context: PyncetteContext):
        pass  # pragma: no cover

    with pytest.raises(ValueError):
        app.use_fixture("scheduled_at", dummy)

    app.use_fixture("duplicate", dummy)
    with pytest.raises(ValueError):
        app.use_fixture("duplicate", dummy)


@pytest.mark.asyncio
async def test_add_to_context(timemachine):
    app = Pyncette(**DefaultBackend().get_args(timemachine))

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        context.hello()

    async with app.create() as ctx:
        ctx.add_to_context("hello", counter)
        task = asyncio.create_task(ctx.run())
        await timemachine.step(datetime.timedelta(seconds=10))
        ctx.shutdown()
        await task
        await timemachine.unwind()

    assert counter.call_count == 5


@pytest.mark.asyncio
async def test_add_to_context_invalid_name():
    app = Pyncette()

    @app.fixture()
    async def fixture(app_context: PyncetteContext):
        yield None

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        context.hello()

    async with app.create() as ctx:
        ctx.add_to_context("duplicate", counter)
        with pytest.raises(ValueError):
            ctx.add_to_context("duplicate", counter)

        with pytest.raises(ValueError):
            ctx.add_to_context("fixture", counter)

        with pytest.raises(ValueError):
            ctx.add_to_context("scheduled_at", counter)


def test_partition_count_invalid():
    app = Pyncette()

    with pytest.raises(ValueError, match="Partition count must be greater than or equal to 1"):

        @app.partitioned_task(partition_count=0)
        async def hello(context: Context) -> None:
            pass  # pragma: no cover


def test_default_partition_selector_does_not_change():
    # BE CAREFUL IF THIS TEST BREAKS.
    # This is a regression test that ensures that the default
    # partition key is not changed, as that could lead to all users'
    # partitions being remapped.
    assert _default_partition_selector(1000000000000, "Lorem ipsum dolor sit amet") == 222413034928
