import datetime

import pytest
from croniter.croniter import CroniterBadCronError

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette


def test_invalid_configuration():
    async def dummy(context: Context):
        pass

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
