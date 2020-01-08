import abc
import contextlib
import datetime
import logging
from collections import defaultdict
from typing import Any
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from typing_extensions import Protocol

from .model import ExecutionMode
from .model import Lease
from .model import ResultType
from .model import TaskName
from .task import Task

logger = logging.getLogger(__name__)


class Repository(abc.ABC):
    """Abstract base class representing a store for Pyncette tasks"""

    @abc.abstractmethod
    async def query_task(self, utc_now: datetime.datetime, task: Task) -> List[Task]:
        """Queries the dynamic tasks for execution"""
        pass

    @abc.abstractmethod
    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Registers a dynamic task"""
        pass

    @abc.abstractmethod
    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Deregisters a dynamic task implementation"""
        pass

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

    def __call__(self, **kwargs: Any) -> AsyncContextManager[Repository]:
        ...


class InMemoryRepository(Repository):
    """In-memory store for Pyncete task execution data"""

    _data: Dict[TaskName, Any]
    _dynamic_tasks: DefaultDict[TaskName, Dict[TaskName, Task]]

    def __init__(self) -> None:
        self._data = {}
        self._dynamic_tasks = defaultdict(dict)

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> List[Task]:
        return list(self._dynamic_tasks[task.name].values())

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        assert task.parent_task is not None
        self._dynamic_tasks[task.parent_task.name][task.name] = task

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        assert task.parent_task is not None
        self._dynamic_tasks[task.parent_task.name].pop(task.name, None)

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> Tuple[ResultType, Optional[Lease]]:
        task_data = self._data.get(task.name, None)

        if task_data is None:
            task_data = {"execute_after": task.get_next_execution(utc_now, None)}
            self._data[task.name] = task_data

        logger.debug(
            f"task={task} locked_until={task_data.get('locked_until', None)} execute_after={task_data.get('execute_after', None)}"
        )

        locked_until = task_data.get("locked_until", None)
        if locked_until is not None and locked_until > utc_now:
            return (ResultType.LOCKED, None)

        if task_data["execute_after"] <= utc_now:
            if task.execution_mode == ExecutionMode.BEST_EFFORT:
                task_data["locked_until"] = None
                task_data["execute_after"] = task.get_next_execution(
                    utc_now, task_data["execute_after"]
                )
                return (ResultType.READY, None)
            elif task.execution_mode == ExecutionMode.RELIABLE:
                lease = Lease(object())
                task_data["locked_until"] = utc_now + task.lease_duration
                task_data["locked_by"] = lease
                return (ResultType.READY, lease)

        return (ResultType.PENDING, None)

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        task_data = self._data.get(task.name, None)

        if task_data is None:
            logger.warning(f"Task {task} not found, skipping.")
            return

        if task_data.get("locked_by", None) != lease:
            logger.warning(f"Lease expired on task {task}, skipping.")
            return

        task_data["locked_until"] = None
        task_data["locked_by"] = None

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        task_data = self._data.get(task.name, None)

        if task_data is None:
            logger.warning(f"Task {task} not found, skipping.")
            return

        if task_data.get("locked_by", None) != lease:
            logger.warning(f"Lease expired on task {task}, skipping.")
            return

        task_data["locked_until"] = None
        task_data["locked_by"] = None
        task_data["execute_after"] = task.get_next_execution(
            utc_now, task_data["execute_after"]
        )


@contextlib.asynccontextmanager
async def in_memory_repository(**kwargs: Any) -> AsyncIterator[InMemoryRepository]:
    """Factory context manager for in-memory repository"""
    yield InMemoryRepository()
