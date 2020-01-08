from __future__ import annotations

import datetime
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import List
from typing import NewType
from typing import Optional
from typing import TypeVar

from typing_extensions import Protocol

T = TypeVar("T")
Decorator = Callable[[T], T]
Lease = NewType("Lease", object)
TaskName = NewType("TaskName", str)


class Context:
    """Task execution context. This class can have dynamic attributes."""

    scheduled_at: datetime.datetime


class TaskFunc(Protocol):
    def __call__(self, context: Context) -> Awaitable[None]:
        ...


class FixtureFunc(Protocol):
    def __call__(self) -> AsyncIterator[Any]:
        ...


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


if TYPE_CHECKING:
    import pyncette.task


@dataclass
class PollResponse:
    """The result of a task poll"""

    result: ResultType
    scheduled_at: datetime.datetime
    lease: Optional[Lease]


@dataclass
class QueryResponse:
    """The result of a task query"""

    tasks: List[pyncette.task.Task]
    has_more: bool
