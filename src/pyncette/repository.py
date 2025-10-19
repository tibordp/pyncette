import abc
import datetime
import logging
from typing import Any
from typing import AsyncContextManager
from typing import Optional
from typing import Protocol

from .model import ContinuationToken
from .model import Lease
from .model import ListTasksResponse
from .model import PollResponse
from .model import QueryResponse
from .model import TaskState
from .task import Task

logger = logging.getLogger(__name__)


class Repository(abc.ABC):
    """Abstract base class representing a store for Pyncette tasks"""

    @abc.abstractmethod
    async def poll_dynamic_task(
        self,
        utc_now: datetime.datetime,
        task: Task,
        continuation_token: Optional[ContinuationToken] = None,
    ) -> QueryResponse:
        """Queries the dynamic tasks for execution"""

    @abc.abstractmethod
    async def register_task(self, utc_now: datetime.datetime, task: Task, force: bool = False) -> None:
        """Registers a dynamic task

        Args:
            utc_now: Current UTC timestamp
            task: The task instance to register
            force: If False, fails if task is locked. If True, overwrites everything including locks.

        Raises:
            TaskLockedException: If force=False and task is currently locked
        """

    @abc.abstractmethod
    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Deregisters a dynamic task implementation"""

    @abc.abstractmethod
    async def poll_task(self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None) -> PollResponse:
        """Polls the task to determine whether it is ready for execution"""

    @abc.abstractmethod
    async def commit_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        """Commits the task, which signals a successful run."""

    @abc.abstractmethod
    async def extend_lease(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> Optional[Lease]:
        """Extends the lease on the task. Returns the new lease if lease was still valid."""

    @abc.abstractmethod
    async def unlock_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        """Unlocks the task, making it eligible for retries in case execution failed."""

    @abc.abstractmethod
    async def get_task_state(
        self,
        utc_now: datetime.datetime,
        task: Task,
    ) -> Optional[TaskState]:
        """Retrieve state of a task instance

        Args:
            utc_now: Current UTC timestamp
            task: The concrete task instance (static task, or instantiated dynamic task)

        Returns:
            TaskState if found, None otherwise
        """

    @abc.abstractmethod
    async def list_task_states(
        self,
        utc_now: datetime.datetime,
        parent_task: Task,
        limit: Optional[int] = None,
        continuation_token: Optional[ContinuationToken] = None,
    ) -> ListTasksResponse:
        """List all instances of a dynamic task with pagination

        Args:
            utc_now: Current UTC timestamp
            parent_task: The dynamic task template
            limit: Maximum number of tasks to return (backend may return less)
            continuation_token: Token from previous response to get next page

        Returns:
            ListTasksResponse with tasks and optional continuation token

        Note:
            Results are not guaranteed to be in any particular order.
            The continuation_token is backend-specific and opaque to callers.
        """


class RepositoryFactory(Protocol):
    """A factory context manager for creating a repository"""

    def __call__(self, **kwargs: Any) -> AsyncContextManager[Repository]:
        """Creates a context manager managing the lifecycle of the repository."""
