import datetime

import pytest
from croniter.croniter import CroniterBadCronError

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.utils import with_heartbeat


def test_invalid_configuration():
    async def dummy(context: Context):
        pass  # pragma: no cover

    # Exactly one of the following must be specified: schedule, interval, execute_at
    with pytest.raises(ValueError):
        app = Pyncette()
        app.task()(dummy)

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task(execute_at=datetime.datetime.utcnow())(dummy)

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
        app.task(
            execution_mode=ExecutionMode.AT_MOST_ONCE, failure_mode=FailureMode.UNLOCK
        )(dummy)

    with pytest.raises(
        ValueError, match="Invalid timezone specifier 'Gondwana/Atlantis'."
    ):
        app = Pyncette()
        app.task(schedule="* * * * *", timezone="Gondwana/Atlantis")(dummy)

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task(interval=datetime.timedelta(seconds=2), timezone="Europe/Dublin")(
            dummy
        )

    with pytest.raises(ValueError, match="Extra parameters must be JSON serializable"):
        app = Pyncette()
        app.task(schedule="* * * * *", extra_arg=object())(dummy)

    with pytest.raises(ValueError, match="Unable to determine name for the task"):
        app = Pyncette()
        app.task(schedule="* * * * *")(object())

    with pytest.raises(ValueError, match="Unable to determine name for the fixture"):
        app = Pyncette()
        app.fixture()(object())


def test_instantiate_non_dynamic_task():
    async def dummy(context: Context):
        pass  # pragma: no cover

    with pytest.raises(ValueError):
        app = Pyncette()
        app.task()(dummy).instantiate(name="foo")


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
