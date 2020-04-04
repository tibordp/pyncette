import abc
import datetime
import logging
from typing import Any
from typing import AsyncContextManager
from typing import Optional

from typing_extensions import Protocol

from .model import Lease
from .model import PollResponse
from .model import QueryResponse
from .task import Task

logger = logging.getLogger(__name__)


class Repository(abc.ABC):
    """Abstract base class representing a store for Pyncette tasks"""

    @abc.abstractmethod
    async def poll_dynamic_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> QueryResponse:
        """Queries the dynamic tasks for execution"""

    @abc.abstractmethod
    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Registers a dynamic task"""

    @abc.abstractmethod
    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Deregisters a dynamic task implementation"""

    @abc.abstractmethod
    async def poll_task(
        self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None
    ) -> PollResponse:
        """Polls the task to determine whether it is ready for execution"""

    @abc.abstractmethod
    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Commits the task, which signals a successful run."""

    @abc.abstractmethod
    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Unlocks the task, making it eligible for retries in case execution failed."""


class RepositoryFactory(Protocol):
    """A factory context manager for creating a repository"""

    def __call__(self, **kwargs: Any) -> AsyncContextManager[Repository]:
        """Creates a context manager managing the lifecycle of the repository."""
