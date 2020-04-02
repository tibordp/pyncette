import datetime

import pytest
from croniter.croniter import CroniterBadCronError

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette


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
        match="failure_mode is not applicable when execution_mode is AT_MOST_ONCE",
    ):

        @app.task(
            execution_mode=ExecutionMode.AT_MOST_ONCE, failure_mode=FailureMode.UNLOCK
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
        ValueError, match="Schedule may not be specified on dynamic task definitions"
    ):

        @app.dynamic_task(schedule="* * * * *")
        def _dummy8(context: Context):
            pass
