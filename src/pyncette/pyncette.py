import asyncio
import contextlib
import copy
import datetime
import logging
import signal
import sys
from typing import Any
from typing import AsyncContextManager
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .model import Context
from .model import Decorator
from .model import ExecutionMode
from .model import FixtureFunc
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
    """Pyncette application."""

    _discovered_tasks: List[Task]
    _startup_tasks: List[Task]
    _shutdown_tasks: List[Task]
    _shutting_down: bool
    _fixtures: List[Tuple[str, Callable[..., AsyncContextManager[Any]]]]
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
        self._fixtures = []
        self._shutting_down = False
        self._poll_interval = poll_interval
        self._repository_factory = repository_factory
        self._configuration = kwargs

    def task(self, **kwargs) -> Decorator[TaskFunc]:
        """Decorator for marking the coroutine as a task"""

        def _func(func: TaskFunc) -> TaskFunc:
            task_kwargs = {
                **kwargs,
                "name": kwargs.get("name", None) or getattr(func, "__name__", None),
            }
            if task_kwargs["name"] is not None:
                self._discovered_tasks.append(Task(func=func, **task_kwargs))
            else:
                raise ValueError("Unable to determine name for the task")
            return func

        return _func

    def fixture(self, name: Optional[str] = None) -> Decorator[FixtureFunc]:
        """Decorator for marking the generator as a fixture"""

        def _func(func: FixtureFunc) -> FixtureFunc:
            fixture_name = name or getattr(func, "__name__", None)
            if fixture_name is not None:
                self._fixtures.append(
                    (fixture_name, contextlib.asynccontextmanager(func))
                )
            else:
                raise ValueError("Unable to determine name for the fixture")
            return func

        return _func

    async def _execute_task(
        self,
        repository: Repository,
        task: Task,
        lease: Optional[Lease],
        root_context: Context,
    ):
        try:
            context = copy.copy(root_context)
            await task.task_func(context)
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

    async def _tick(
        self, repository: Repository, scheduler: DefaultScheduler, root_context: Context
    ):
        utc_now = current_time()

        for task in self._discovered_tasks:
            poll_result, lease = await repository.poll_task(utc_now, task)
            if poll_result == ResultType.READY:
                logger.info(f"Executing task {task}")
                scheduler.spawn_task(
                    self._execute_task(repository, task, lease, root_context)
                )
            elif poll_result == ResultType.PENDING:
                logger.debug(
                    f"Not executing task {task}, because it is not yet scheduled."
                )
            elif poll_result == ResultType.LOCKED:
                logger.debug(f"Not executing task {task}, because it is locked.")

    async def run(self):
        """Runs the Pyncette's main event loop."""
        async with self._repository_factory(
            **self._configuration
        ) as repository, DefaultScheduler() as scheduler, contextlib.AsyncExitStack() as stack:
            root_context = await self._create_context(stack)

            while not self._shutting_down:
                try:
                    await self._tick(repository, scheduler, root_context)
                except Exception as e:
                    logger.warning("Polling tasks failed.", exc_info=e)

                await asyncio.sleep(self._poll_interval.total_seconds())

    async def _create_context(self, stack: contextlib.AsyncExitStack) -> Context:
        context = Context()
        for name, callback in self._fixtures:
            setattr(context, name, await stack.enter_async_context(callback()))
        return context

    def shutdown(self):
        """Initiates a graceful shutdown"""
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
        """Convenience entrypoint for console apps, which sets up logging and signal handling."""
        # Setup logging
        self._setup_signal_handler()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())
