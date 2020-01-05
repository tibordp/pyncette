import abc
import datetime
from typing import AsyncContextManager
from typing import Optional
from typing import Tuple

from typing_extensions import Protocol

from pyncette.model import Lease
from pyncette.model import ResultType
from pyncette.task import Task


class Repository(abc.ABC):
    """Abstract base class representing a store for Pyncette tasks"""

    @abc.abstractmethod
    async def poll_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> Tuple[ResultType, Optional[Lease]]:
        """Polls the task to determine whether it is ready for execution"""
        pass

    @abc.abstractmethod
    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Commits the task, which signals a successful run."""
        pass

    @abc.abstractmethod
    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Unlocks the task, making it eligible for retries in case execution failed."""
        pass


class RepositoryFactory(Protocol):
    """A factory context manager for creating a repository"""

    def __call__(self, **kwargs) -> AsyncContextManager[Repository]:
        ...


__all__ = ["Repository", "RepositoryFactory"]
