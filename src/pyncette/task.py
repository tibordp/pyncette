from __future__ import annotations

import datetime
import json
from typing import Any
from typing import Awaitable
from typing import Dict
from typing import Iterator
from typing import Optional

from croniter import croniter

from .model import Context
from .model import ExecutionMode
from .model import FailureMode
from .model import TaskFunc
from .model import TaskName


def generate_interval(
    base_time: datetime.datetime, interval: datetime.timedelta
) -> Iterator[datetime.datetime]:
    current_time = base_time
    while True:
        current_time = current_time + interval
        yield current_time


class Task:
    """The base unit of execution"""

    name: TaskName
    task_func: TaskFunc
    schedule: Optional[str]
    interval: Optional[datetime.timedelta]
    fast_forward: bool
    failure_mode: FailureMode
    execution_mode: ExecutionMode
    lease_duration: datetime.timedelta
    parent_task: Optional["Task"]
    extra_args: Dict[str, Any]

    def __init__(
        self,
        name: TaskName,
        func: TaskFunc,
        dynamic: bool = False,
        parent_task: "Task" = None,
        schedule: Optional[str] = None,
        interval: Optional[datetime.timedelta] = None,
        fast_forward: bool = False,
        failure_mode: FailureMode = FailureMode.NONE,
        execution_mode: ExecutionMode = ExecutionMode.RELIABLE,
        lease_duration: datetime.timedelta = datetime.timedelta(seconds=60),
        **kwargs: Any,
    ):
        self.name = name
        self.task_func = func

        self.dynamic = dynamic
        self.parent_task = parent_task

        self.schedule = schedule
        self.interval = interval
        self.fast_forward = fast_forward
        self.failure_mode = failure_mode
        self.execution_mode = execution_mode
        self.lease_duration = lease_duration
        self.extra_args = kwargs

        self._validate()

    def _validate(self) -> None:
        if (
            self.execution_mode == ExecutionMode.BEST_EFFORT
            and self.failure_mode != FailureMode.NONE
        ):
            raise ValueError(
                "failure_mode is not applicable when execution_mode is BEST_EFFORT"
            )

        if not self.dynamic:
            if self.schedule is not None and self.interval is not None:
                raise ValueError("schedule and interval are mutually exclusive")
            if self.schedule is None and self.interval is None:
                raise ValueError("One of schedule or interval must be specified")
            if self.schedule is not None:
                croniter.expand(self.schedule)

        try:
            json.dumps(self.extra_args)
        except Exception as e:
            raise ValueError(f"Extra parameters must be JSON serializable ({e})")

    def _get_future_runs(
        self, utc_now: datetime.datetime, last_execution: Optional[datetime.datetime],
    ) -> Iterator[datetime.datetime]:
        base_time = last_execution if last_execution is not None else utc_now
        if self.schedule is not None:
            return croniter(
                self.schedule, start_time=base_time, ret_type=datetime.datetime,
            )
        elif self.interval is not None:
            return generate_interval(base_time, self.interval)

        assert False

    def get_next_execution(
        self, utc_now: datetime.datetime, last_execution: Optional[datetime.datetime],
    ) -> datetime.datetime:
        for run in self._get_future_runs(utc_now, last_execution):
            if not self.fast_forward or run >= utc_now:
                return run

        assert False

    def instantiate(self, name: TaskName, **kwargs: Any) -> Task:
        """Creates a concrete instance of a dynamic task"""

        if not self.dynamic:
            raise ValueError("Cannot instantiate a dynamic task")

        return Task(
            name=name,
            func=self.task_func,
            fast_forward=self.fast_forward,
            failure_mode=self.failure_mode,
            execution_mode=self.execution_mode,
            lease_duration=self.lease_duration,
            parent_task=self,
            **kwargs,
        )

    def __call__(self, context: Context) -> Awaitable[None]:
        return self.task_func(context)

    def __str__(self) -> str:
        return self.name
