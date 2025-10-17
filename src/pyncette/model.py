from __future__ import annotations

import datetime
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from typing import Callable
from typing import NewType
from typing import Protocol
from typing import TypeVar

T = TypeVar("T")
Decorator = Callable[[T], T]
Lease = NewType("Lease", object)
ContinuationToken = NewType("ContinuationToken", object)

# https://github.com/python/mypy/issues/708


class Heartbeater(Protocol):
    def __call__(self) -> Awaitable[None]:
        "Heartbeats on the message"


class Context:
    """Task execution context. This class can have dynamic attributes."""

    app_context: pyncette.PyncetteContext
    task: pyncette.task.Task
    scheduled_at: datetime.datetime
    _lease: Lease | None
    heartbeat: Heartbeater
    args: dict[str, Any]

    if TYPE_CHECKING:

        def __getattr__(self, name: str) -> Any: ...

        def __setattr__(self, name: str, value: Any) -> Any: ...


class TaskFunc(Protocol):
    def __call__(self, context: Context) -> Awaitable[None]:
        "Executes the task"


class PartitionSelector(Protocol):
    def __call__(self, partition_count: int, task_id: str) -> int:
        "Gets the partition number for a given task id"


class NextFunc(Protocol):
    def __call__(self) -> Awaitable[None]:
        "Enter the next middleware or the task body"


class MiddlewareFunc(Protocol):
    def __call__(self, context: Context, next: NextFunc) -> Awaitable[None]:
        "Executes the middleware"


class FixtureFunc(Protocol):
    def __call__(self, app_context: pyncette.PyncetteContext) -> AsyncIterator[Any]:
        "Executes the fixture"


class ResultType(Enum):
    """Status returned by polling the task"""

    MISSING = 0
    PENDING = 1
    READY = 2
    LOCKED = 3
    LEASE_MISMATCH = 4


class ExecutionMode(Enum):
    """The execution mode for a Pyncette task."""

    AT_LEAST_ONCE = 0
    AT_MOST_ONCE = 1


class FailureMode(Enum):
    """What should happen when a task fails."""

    NONE = 0
    UNLOCK = 1
    COMMIT = 2


@dataclass
class PollResponse:
    """The result of a task poll"""

    result: ResultType
    scheduled_at: datetime.datetime
    lease: Lease | None


@dataclass
class QueryResponse:
    """The result of a task query"""

    tasks: list[tuple[pyncette.task.Task, Lease]]
    continuation_token: ContinuationToken | None


if TYPE_CHECKING:
    import pyncette
    import pyncette.task
