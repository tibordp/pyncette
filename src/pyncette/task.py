import datetime
from typing import Optional

from croniter import croniter

from .model import ExecutionMode
from .model import FailureMode
from .model import TaskFunc


def generate_interval(base_time: datetime.datetime, interval: datetime.timedelta):
    current_time = base_time
    while True:
        current_time = current_time + interval
        yield current_time


class Task:
    name: str
    task_func: TaskFunc
    schedule: str
    interval: datetime.timedelta
    fast_forward: bool
    failure_mode: FailureMode
    execution_mode: ExecutionMode
    lease_duration: datetime.timedelta

    def __init__(self, name: str, func: TaskFunc, **kwargs):
        self.name = name
        self.task_func = func

        self.schedule = kwargs.get("schedule", None)
        self.interval = kwargs.get("interval", None)
        self.fast_forward = kwargs.get("fast_forward", False)
        self.failure_mode = kwargs.get("failure_mode", FailureMode.NONE)
        self.execution_mode = kwargs.get("execution_mode", ExecutionMode.RELIABLE)
        self.lease_duration = kwargs.get(
            "lease_duration", datetime.timedelta(seconds=60)
        )

        self._validate()

    def _validate(self):
        if (
            self.execution_mode == ExecutionMode.BEST_EFFORT
            and self.failure_mode != FailureMode.NONE
        ):
            raise ValueError(
                "failure_mode is not applicable when execution_mode is BEST_EFFORT"
            )
        if self.schedule is not None and self.interval is not None:
            raise ValueError("schedule and interval are mutually exclusive")
        if self.schedule is None and self.interval is None:
            raise ValueError("One of schedule or interval must be specified")
        if self.schedule is not None:
            croniter.expand(self.schedule)

    def _get_future_runs(
        self, utc_now: datetime.datetime, last_execution: Optional[datetime.datetime],
    ):
        base_time = last_execution if last_execution is not None else utc_now
        if self.schedule:
            return croniter(
                self.schedule, start_time=base_time, ret_type=datetime.datetime,
            )
        else:
            return generate_interval(base_time, self.interval)

    def get_next_execution(
        self, utc_now: datetime.datetime, last_execution: Optional[datetime.datetime],
    ):
        for run in self._get_future_runs(utc_now, last_execution):
            if not self.fast_forward or run >= utc_now:
                return run

    def __str__(self):
        return self.name
