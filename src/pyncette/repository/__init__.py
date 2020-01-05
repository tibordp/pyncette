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
    @abc.abstractmethod
    async def poll_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> Tuple[ResultType, Optional[Lease]]:
        pass

    @abc.abstractmethod
    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        pass

    @abc.abstractmethod
    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        pass


class RepositoryFactory(Protocol):
    def __call__(self, **kwargs) -> AsyncContextManager[Repository]:
        ...


__all__ = ["Repository", "RepositoryFactory"]
