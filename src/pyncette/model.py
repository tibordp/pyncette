from enum import Enum
from typing import Any
from typing import AsyncGenerator
from typing import Awaitable
from typing import Callable
from typing import NewType
from typing import TypeVar

from typing_extensions import Protocol

T = TypeVar("T")
Decorator = Callable[[T], T]
Lease = NewType("Lease", object)


class Context:
    pass


class TaskFunc(Protocol):
    def __call__(self, context: Context) -> Awaitable[None]:
        ...


class FixtureFunc(Protocol):
    def __call__(self) -> AsyncGenerator[Any, None]:
        ...


class ResultType(Enum):
    PENDING = 0
    READY = 1
    LOCKED = 2
    LEASE_MISMATCH = 3


class ExecutionMode(Enum):
    """The execution mode for a Pyncette task."""

    RELIABLE = 0
    BEST_EFFORT = 1
