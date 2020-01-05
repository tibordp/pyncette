import asyncio
import datetime
import logging
import signal
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .model import Context
from .model import Decorator
from .model import ExecutionMode
from .model import Lease
from .model import ResultType
from .model import TaskFunc
from .repository import Repository
from .repository import RepositoryFactory
from .repository.in_memory import in_memory_repository
from .scheduler import DefaultScheduler
from .task import Task

logger = logging.getLogger(__name__)


def current_time() -> datetime.datetime:
    return datetime.datetime.utcnow()


class Pyncette:
    _discovered_tasks: List[Task]
    _shutting_down: bool
    _repository_factory: RepositoryFactory
    _commit_on_failure: bool
    _poll_interval: datetime.timedelta
    _configuration: Dict[str, Any]

    def __init__(
        self,
        repository_factory: RepositoryFactory = in_memory_repository,
        commit_on_failure: bool = False,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
        **kwargs,
    ):
        self._discovered_tasks = []
        self._shutting_down = False
        self._poll_interval = poll_interval
        self._repository_factory = repository_factory
        self._configuration = kwargs

    def task(self, **kwargs) -> Decorator[TaskFunc]:
        def _func(func: TaskFunc) -> TaskFunc:
            self._discovered_tasks.append(Task.from_function(func, **kwargs))
            return func

        return _func

    async def _execute_task(
        self, repository: Repository, task: Task, lease: Optional[Lease]
    ):
        try:
            await task.task_func(Context(self, task))
            execution_suceeded = True
        except Exception as e:
            logger.warning(f"Task {task} failed", exc_info=e)
            execution_suceeded = False

        if task.execution_mode == ExecutionMode.BEST_EFFORT:
            return

        assert lease is not None
        utc_now = current_time()
        try:
            if execution_suceeded or task.commit_on_failure:
                await repository.commit_task(utc_now, task, lease)
            else:
                await repository.unlock_task(utc_now, task, lease)
        except Exception as e:
            logger.warning(
                "Failed to commit task {task}, it will likely execute again.",
                exc_info=e,
            )

    async def _tick(self, repository: Repository, scheduler: DefaultScheduler):
        utc_now = current_time()

        for task in self._discovered_tasks:
            poll_result, lease = await repository.poll_task(utc_now, task)
            if poll_result == ResultType.READY:
                logger.info(f"Executing task {task}")
                scheduler.spawn_task(self._execute_task(repository, task, lease))
            elif poll_result == ResultType.PENDING:
                logger.debug(
                    f"Not executing task {task}, because it is not yet scheduled."
                )
            elif poll_result == ResultType.LOCKED:
                logger.debug(f"Not executing task {task}, because it is locked.")

    async def run(self):
        async with self._repository_factory(
            **self._configuration
        ) as repository, DefaultScheduler() as scheduler:
            while not self._shutting_down:
                try:
                    await self._tick(repository, scheduler)
                except Exception as e:
                    logger.warning("Polling tasks failed.", exc_info=e)

                await asyncio.sleep(self._poll_interval.total_seconds())

    def shutdown(self):
        logger.info("Initiating graceful shutdown")
        self._shutting_down = True

    def _setup_signal_handler(self):
        def handler(signum, frame):
            if not self._shutting_down:
                self.shutdown()
            else:
                logger.warning("Terminating")
                logging.shutdown()
                sys.exit(1)

        signal.signal(signal.SIGINT, handler)

    def main(self):
        # Setup logging
        # Setup signal handling
        self._setup_signal_handler()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())
