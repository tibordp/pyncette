from __future__ import annotations

import datetime
import json
import logging
from typing import Any
from typing import Awaitable
from typing import Dict
from typing import Iterator
from typing import Optional

import dateutil.tz
from croniter import croniter

from .model import Context
from .model import ExecutionMode
from .model import FailureMode
from .model import TaskFunc

logger = logging.getLogger(__name__)


class Task:
    """The base unit of execution"""

    name: str
    task_func: TaskFunc
    schedule: Optional[str]
    interval: Optional[datetime.timedelta]
    execute_at: Optional[datetime.datetime]
    timezone: Optional[str]
    fast_forward: bool
    failure_mode: FailureMode
    execution_mode: ExecutionMode
    lease_duration: datetime.timedelta
    parent_task: Optional["Task"]
    extra_args: Dict[str, Any]

    def __init__(
        self,
        name: str,
        func: TaskFunc,
        dynamic: bool = False,
        parent_task: "Task" = None,
        schedule: Optional[str] = None,
        interval: Optional[datetime.timedelta] = None,
        execute_at: Optional[datetime.datetime] = None,
        timezone: Optional[str] = None,
        fast_forward: bool = False,
        failure_mode: FailureMode = FailureMode.NONE,
        execution_mode: ExecutionMode = ExecutionMode.AT_LEAST_ONCE,
        lease_duration: datetime.timedelta = datetime.timedelta(seconds=60),
        **kwargs: Any,
    ):
        self.name = name
        self.task_func = func

        self.dynamic = dynamic
        self.parent_task = parent_task

        self.schedule = schedule
        self.interval = interval
        self.timezone = timezone
        self.fast_forward = fast_forward
        self.failure_mode = failure_mode
        self.execute_at = execute_at
        self.execution_mode = execution_mode
        self.lease_duration = lease_duration
        self.extra_args = kwargs

        self._validate()

    def _validate(self) -> None:
        if (
            self.execution_mode == ExecutionMode.AT_MOST_ONCE
            and self.failure_mode != FailureMode.NONE
        ):
            raise ValueError(
                "failure_mode is not applicable when execution_mode is AT_MOST_ONCE"
            )

        if not self.dynamic:
            schedule_specs = [
                spec
                for spec in [self.schedule, self.interval, self.execute_at]
                if spec is not None
            ]
            if len(schedule_specs) != 1:
                raise ValueError(
                    "Exactly one of the following must be specified: schedule, interval, execute_at"
                )
            if self.schedule is None and self.timezone is not None:
                raise ValueError(
                    "Timezone may only be specified when cron schedule is used"
                )
            if self.schedule is not None:
                croniter.expand(self.schedule)

        if self.parent_task is None and self.execute_at is not None:
            raise ValueError("execute_at is only supported for dynamic tasks")

        if dateutil.tz.gettz(self.timezone) is None:
            raise ValueError(f"Invalid timezone specifier '{self.timezone}'.")

        try:
            json.dumps(self.extra_args)
        except Exception as e:
            raise ValueError(f"Extra parameters must be JSON serializable ({e})")

    def _get_future_runs(
        self, utc_now: datetime.datetime, last_execution: Optional[datetime.datetime],
    ) -> Iterator[datetime.datetime]:
        current_time = last_execution if last_execution is not None else utc_now
        current_time = current_time.astimezone(dateutil.tz.gettz(self.timezone))
        while True:
            if self.schedule is not None:
                cron = croniter(
                    self.schedule, start_time=current_time, ret_type=datetime.datetime
                )
                current_time = cron.get_next()
            elif self.interval is not None:
                current_time = current_time + self.interval
            else:
                assert False

            yield current_time.astimezone(dateutil.tz.UTC)

        assert False

    def get_next_execution(
        self, utc_now: datetime.datetime, last_execution: Optional[datetime.datetime],
    ) -> Optional[datetime.datetime]:
        if self.execute_at is not None:
            return (
                self.execute_at.astimezone(dateutil.tz.UTC)
                if last_execution is None
                else None
            )
        for run in self._get_future_runs(utc_now, last_execution):
            utc_run = run.astimezone(dateutil.tz.UTC)
            if not self.fast_forward or utc_run >= utc_now:
                return utc_run

        assert False

    def instantiate(self, name: str, **kwargs: Any) -> Task:
        """Creates a concrete instance of a dynamic task"""

        if not self.dynamic:
            raise ValueError("Cannot instantiate a non-dynamic task")

        extra_args: Dict[str, Any] = {
            "schedule": self.schedule,
            "interval": self.interval,
            "timezone": self.timezone,
            "execute_at": self.execute_at,
            **self.extra_args,
            **kwargs,
        }

        return Task(
            name=name,
            func=self.task_func,
            fast_forward=self.fast_forward,
            failure_mode=self.failure_mode,
            execution_mode=self.execution_mode,
            lease_duration=self.lease_duration,
            parent_task=self,
            **extra_args,
        )

    @property
    def canonical_name(self) -> str:
        """A unique identifier for a task instance"""
        if self.parent_task is not None:
            return "{}:{}".format(
                self.parent_task.name.replace(":", "::"), self.name.replace(":", "::")
            )
        else:
            return self.name.replace(":", "::")

    def as_spec(self) -> Dict[str, Any]:
        """Serializes all the attributes to task spec"""
        return {
            "name": self.name,
            "schedule": self.schedule,
            "interval": self.interval.total_seconds()
            if self.interval is not None
            else None,
            "execute_at": self.execute_at.isoformat()
            if self.execute_at is not None
            else None,
            "timezone": self.timezone,
            "extra_args": self.extra_args,
        }

    def instantiate_from_spec(self, task_spec: Dict[str, Any]) -> Task:
        """Deserializes all the attributes from task spec"""
        return self.instantiate(
            name=task_spec["name"],
            schedule=task_spec["schedule"],
            interval=datetime.timedelta(seconds=task_spec["interval"])
            if task_spec["interval"] is not None
            else None,
            timezone=task_spec["timezone"],
            execute_at=datetime.datetime.fromisoformat(task_spec["execute_at"])
            if task_spec["execute_at"] is not None
            else None,
            **task_spec["extra_args"],
        )

    def __call__(self, context: Context) -> Awaitable[None]:
        return self.task_func(context)

    def __str__(self) -> str:
        return self.name
