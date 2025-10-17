from __future__ import annotations

import datetime
import hashlib
import json
import logging
from typing import Any
from collections.abc import Awaitable

import dateutil.tz
from croniter import croniter

from .model import Context
from .model import ExecutionMode
from .model import FailureMode
from .model import PartitionSelector
from .model import TaskFunc

logger = logging.getLogger(__name__)


class Task:
    """The base unit of execution"""

    name: str
    task_func: TaskFunc
    schedule: str | None
    interval: datetime.timedelta | None
    execute_at: datetime.datetime | None
    timezone: str | None
    fast_forward: bool
    failure_mode: FailureMode
    execution_mode: ExecutionMode
    lease_duration: datetime.timedelta
    parent_task: Task | None
    extra_args: dict[str, Any]
    _enabled: bool

    def __init__(
        self,
        *,
        name: str,
        func: TaskFunc,
        enabled: bool = True,
        dynamic: bool = False,
        parent_task: Task | None = None,
        schedule: str | None = None,
        interval: datetime.timedelta | None = None,
        execute_at: datetime.datetime | None = None,
        timezone: str | None = None,
        fast_forward: bool = False,
        failure_mode: FailureMode = FailureMode.NONE,
        execution_mode: ExecutionMode = ExecutionMode.AT_LEAST_ONCE,
        lease_duration: datetime.timedelta = datetime.timedelta(seconds=60),
        **kwargs: Any,
    ):
        self._enabled = enabled
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
        if self.execution_mode == ExecutionMode.AT_MOST_ONCE and self.failure_mode != FailureMode.NONE:
            raise ValueError("failure_mode is not applicable when execution_mode is AT_MOST_ONCE")

        if not self.dynamic:
            schedule_specs = [spec for spec in [self.schedule, self.interval, self.execute_at] if spec is not None]
            if len(schedule_specs) != 1:
                raise ValueError("Exactly one of the following must be specified: schedule, interval, execute_at")
            if self.schedule is None and self.timezone is not None:
                raise ValueError("Timezone may only be specified when cron schedule is used")
            if self.schedule is not None:
                croniter.expand(self.schedule)

        if self.parent_task is None and self.execute_at is not None:
            raise ValueError("execute_at is only supported for dynamic tasks")

        if dateutil.tz.gettz(self.timezone) is None:
            raise ValueError(f"Invalid timezone specifier '{self.timezone}'.")

        try:
            json.dumps(self.extra_args)
        except Exception as e:
            raise ValueError(f"Extra parameters must be JSON serializable ({e})") from None

    def get_next_execution(
        self,
        utc_now: datetime.datetime,
        last_execution: datetime.datetime | None,
    ) -> datetime.datetime | None:
        if self.execute_at is not None:
            return self.execute_at.astimezone(dateutil.tz.UTC) if last_execution is None else None

        current_time = last_execution if last_execution is not None else utc_now

        if self.interval is not None:
            if not last_execution or not self.fast_forward:
                return current_time + self.interval
            else:
                count = (utc_now - last_execution) // self.interval + 1
                return last_execution + (self.interval * count)

        if self.schedule is not None:
            if self.timezone:
                current_time = current_time.astimezone(dateutil.tz.gettz(self.timezone))

            cron = croniter(self.schedule, start_time=current_time, ret_type=datetime.datetime)

            while True:
                next_execution = cron.get_next()
                if not next_execution:
                    return None
                if not self.fast_forward or next_execution >= utc_now:
                    return next_execution.astimezone(dateutil.tz.UTC)

        raise AssertionError

    def instantiate(self, name: str, **kwargs: Any) -> Task:
        """Creates a concrete instance of a dynamic task"""

        if not self.dynamic:
            raise ValueError("Cannot instantiate a non-dynamic task")

        extra_args: dict[str, Any] = {
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
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    @property
    def canonical_name(self) -> str:
        """A unique identifier for a task instance"""
        if self.parent_task is not None:
            return "{}:{}".format(
                self.parent_task.canonical_name,
                self.name.replace(":", "::"),
            )
        else:
            return self.name.replace(":", "::")

    def as_spec(self) -> dict[str, Any]:
        """Serializes all the attributes to task spec"""
        return {
            "name": self.name,
            "schedule": self.schedule,
            "interval": self.interval.total_seconds() if self.interval is not None else None,
            "execute_at": self.execute_at.isoformat() if self.execute_at is not None else None,
            "timezone": self.timezone,
            "extra_args": self.extra_args,
        }

    def instantiate_from_spec(self, task_spec: dict[str, Any]) -> Task:
        """Deserializes all the attributes from task spec"""
        return self.instantiate(
            name=task_spec["name"],
            schedule=task_spec["schedule"],
            interval=datetime.timedelta(seconds=task_spec["interval"]) if task_spec["interval"] is not None else None,
            timezone=task_spec["timezone"],
            execute_at=datetime.datetime.fromisoformat(task_spec["execute_at"]) if task_spec["execute_at"] is not None else None,
            **task_spec["extra_args"],
        )

    def __call__(self, context: Context) -> Awaitable[None]:
        return self.task_func(context)

    def __str__(self) -> str:
        return self.canonical_name


def _default_partition_selector(partition_count: int, task_id: str) -> int:
    algo = hashlib.sha1()  # noqa: S324
    algo.update(task_id.encode("utf-8"))
    max_value = int.from_bytes(b"\xff" * algo.digest_size, "big") + 1
    digest = int.from_bytes(algo.digest(), "big")

    return (digest * partition_count) // max_value


class _TaskPartition(Task):
    partition_id: int
    _parent: PartitionedTask

    def __init__(self, parent: PartitionedTask, partition_id: int, **kwargs: Any):
        super().__init__(dynamic=True, **kwargs)
        self._parent = parent
        self.partition_id = partition_id

    @property
    def enabled(self) -> bool:
        return self._parent.enabled and (self._parent.enabled_partitions is None or self.partition_id in self._parent.enabled_partitions)

    @enabled.setter
    def enabled(self, value: bool) -> None:
        raise ValueError("Use enabled_partitions to disable polling a partition.")

    @property
    def canonical_name(self) -> str:
        """A unique identifier for a task instance"""

        assert self.parent_task is None
        return "{}:{}".format(self.name.replace(":", "::"), self.partition_id)


class PartitionedTask(Task):
    _kwargs: Any
    partition_count: int
    partition_selector: PartitionSelector
    enabled_partitions: list[int] | None

    def __init__(
        self,
        *,
        partition_count: int,
        partition_selector: PartitionSelector = _default_partition_selector,
        enabled_partitions: list[int] | None = None,
        **kwargs: Any,
    ):
        if partition_count < 1:
            raise ValueError("Partition count must be greater than or equal to 1")

        super().__init__(dynamic=True, **kwargs)

        self.partition_count = partition_count
        self.partition_selector = partition_selector
        self.enabled_partitions = enabled_partitions
        self._kwargs = kwargs

    def get_partitions(self) -> list[Task]:
        return [_TaskPartition(self, partition_id=partition_id, **self._kwargs) for partition_id in range(self.partition_count)]

    def instantiate(self, name: str, **kwargs: Any) -> Task:
        """Creates a concrete instance of a dynamic task"""

        partition_id = self.partition_selector(self.partition_count, name)
        shard = _TaskPartition(self, partition_id=partition_id, **self._kwargs)

        return shard.instantiate(name, **kwargs)
