import asyncio
import contextlib
import datetime
import logging
from typing import Any
from typing import AsyncGenerator
from typing import Dict
from typing import Optional
from typing import Tuple

from pyncette.model import ExecutionMode
from pyncette.model import Lease
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


class InMemoryRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _data: Dict[str, Any]

    def __init__(self):
        self._data = {}
        # Locking is technically not required if we are on a single-threaded event loop, since
        # we do not yield while the lock is held, but just to be on the safe side.
        self._lock = asyncio.Lock()

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> Tuple[ResultType, Optional[Lease]]:
        async with self._lock:
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

    async def unlock_task(self, utc_now: datetime.datetime, task: Task, lease: Lease):
        async with self._lock:
            task_data = self._data.get(task.name, None)

            if task_data is None:
                logger.warning(f"Task {task} not found, skipping.")
                return

            if task_data.get("locked_by", None) != lease:
                logger.warning(f"Lease expired on task {task}, skipping.")
                return

            task_data["locked_until"] = None
            task_data["locked_by"] = None

    async def commit_task(self, utc_now: datetime.datetime, task: Task, lease: Lease):
        async with self._lock:
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
async def in_memory_repository(**kwargs,) -> AsyncGenerator[InMemoryRepository, None]:
    """Factory context manager for in-memory repository"""
    yield InMemoryRepository()
