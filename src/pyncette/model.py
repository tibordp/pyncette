from enum import Enum
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import NewType
from typing import TypeVar

from typing_extensions import Protocol

T = TypeVar("T")
Decorator = Callable[[T], T]
Lease = NewType("Lease", object)

TaskName = NewType("TaskName", str)


class Context:
    pass


class TaskFunc(Protocol):
    def __call__(self, context: Context) -> Awaitable[None]:
        ...


class FixtureFunc(Protocol):
    def __call__(self) -> AsyncIterator[Any]:
        ...


class ResultType(Enum):
    MISSING = 0
    PENDING = 1
    READY = 2
    LOCKED = 3
    LEASE_MISMATCH = 4


class ExecutionMode(Enum):
    """The execution mode for a Pyncette task."""

    RELIABLE = 0
    BEST_EFFORT = 1


class FailureMode(Enum):
    """What should happen when a task fails."""

    NONE = 0
    UNLOCK = 1
    COMMIT = 2
