from __future__ import annotations

import asyncio
import contextlib
import copy
import datetime
import logging
import signal
import sys
from functools import partial
from itertools import chain
from typing import Any
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import cast

import dateutil.tz

from .model import Context
from .model import Decorator
from .model import ExecutionMode
from .model import FailureMode
from .model import FixtureFunc
from .model import Lease
from .model import MiddlewareFunc
from .model import PollResponse
from .model import ResultType
from .model import TaskFunc
from .model import TaskName
from .repository import Repository
from .repository import RepositoryFactory
from .repository import in_memory_repository
from .scheduler import DefaultScheduler
from .task import Task

logger = logging.getLogger(__name__)


def _current_time() -> datetime.datetime:
    u = datetime.datetime.utcnow()
    u = u.replace(tzinfo=dateutil.tz.UTC)
    return u


class PyncetteContext:
    """Execution context of a Pyncette app"""

    _app: Pyncette
    _repository: Repository
    _root_context: Context
    _scheduler: DefaultScheduler
    _shutting_down: asyncio.Event

    def __init__(
        self,
        app: Pyncette,
        repository: Repository,
        scheduler: DefaultScheduler,
        root_context: Context,
    ):
        self._repository = repository
        self._scheduler = scheduler
        self._root_context = root_context
        self._app = app
        self._shutting_down = asyncio.Event()

    async def schedule_task(
        self, task: Task, instance_name: TaskName, **kwargs: Any
    ) -> Task:
        """Schedules a conrete instance of a dynamic task"""
        task_name = cast(TaskName, f"{task.name}:{instance_name}")
        concrete_task = task.instantiate(task_name, **kwargs)
        utc_now = _current_time()

        await self._repository.register_task(utc_now, concrete_task)
        return concrete_task

    async def unschedule_task(
        self, task: Task, instance_name: Optional[TaskName] = None
    ) -> None:
        """Removes the concrete instance of a dynamic task"""

        # If we are passed a concrete instance of a dynamic task, we can directly use it,
        # otherwise we instantiate a dummy one on the fly.
        if task.parent_task:
            concrete_task = task
        else:
            if not instance_name:
                raise ValueError("instance name must be provided")
            task_name = cast(TaskName, f"{task.name}:{instance_name}")
            concrete_task = task.instantiate(task_name, interval=datetime.timedelta())

        utc_now = _current_time()
        await self._repository.unregister_task(utc_now, concrete_task)

    def _populate_context(self, task: Task, poll_response: PollResponse) -> Context:
        context = copy.copy(self._root_context)
        context.app_context = self
        context.task = task
        tz = (
            dateutil.tz.UTC
            if task.timezone is None
            else dateutil.tz.gettz(task.timezone)
        )
        context.scheduled_at = poll_response.scheduled_at.astimezone(tz)
        context.args = copy.copy(task.extra_args)

        return context

    async def _execute_task(self, task: Task, poll_response: PollResponse) -> None:
        try:
            context = self._populate_context(task, poll_response)
            task_func: Callable[[], Awaitable[None]] = partial(task, context)
            for middleware in reversed(self._app._middlewares):
                task_func = partial(middleware, context, task_func)
            await task_func()
        except Exception as e:
            logger.warning(f"Task {task} failed", exc_info=e)
            execution_suceeded = False
        else:
            execution_suceeded = True

        if task.execution_mode == ExecutionMode.AT_MOST_ONCE:
            return

        assert poll_response.lease is not None
        utc_now = _current_time()
        try:
            if execution_suceeded or task.failure_mode == FailureMode.COMMIT:
                await self._repository.commit_task(utc_now, task, poll_response.lease)
            elif task.failure_mode == FailureMode.UNLOCK:
                await self._repository.unlock_task(utc_now, task, poll_response.lease)
        except Exception as e:
            logger.warning(
                "Failed to commit task {task}, it will likely execute again.",
                exc_info=e,
            )

    async def _get_active_tasks(
        self, utc_now: datetime.datetime
    ) -> AsyncIterator[Tuple[Task, Optional[Lease]]]:
        for task in self._app._concrete_tasks:
            yield (task, None)
        for task in self._app._dynamic_tasks:
            while not self._shutting_down.is_set():
                query_response = await self._repository.query_task(utc_now, task)
                for task_and_lease in query_response.tasks:
                    yield task_and_lease

                if not query_response.has_more:
                    break

                logger.debug(f"Dynamic {task} has more due instances, looping.")

    async def _tick(self) -> None:
        utc_now = _current_time()

        async for task, lease in self._get_active_tasks(utc_now):
            if self._shutting_down.is_set():
                break

            poll_response = await self._repository.poll_task(utc_now, task, lease)
            if poll_response.result == ResultType.READY:
                logger.info(f"Executing task {task} with {task.extra_args}")
                await self._scheduler.spawn_task(
                    self._execute_task(task, poll_response)
                )
            elif poll_response.result == ResultType.PENDING:
                logger.debug(
                    f"Not executing task {task}, because it is not yet scheduled."
                )
            elif poll_response.result == ResultType.LOCKED:
                logger.debug(f"Not executing task {task}, because it is locked.")

    async def run(self) -> None:
        """Runs the Pyncette's main event loop."""
        while not self._shutting_down.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Polling tasks failed.", exc_info=e)

            try:
                await asyncio.wait_for(
                    self._shutting_down.wait(),
                    timeout=self._app._poll_interval.total_seconds(),
                )
            except asyncio.TimeoutError:
                pass

    def shutdown(self) -> None:
        """Initiates graceful shutdown, terminating the main loop, but allowing all executing tasks to finish."""
        logger.info("Initiating graceful shutdown")
        self._shutting_down.set()


class Pyncette:
    """Pyncette application."""

    _concrete_tasks: List[Task]
    _dynamic_tasks: List[Task]
    _fixtures: List[Tuple[str, Callable[..., AsyncContextManager[Any]]]]
    _middlewares: List[MiddlewareFunc]
    _repository_factory: RepositoryFactory
    _poll_interval: datetime.timedelta
    _concurrency_limit: int
    _configuration: Dict[str, Any]

    def __init__(
        self,
        repository_factory: RepositoryFactory = in_memory_repository,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
        concurrency_limit: int = 100,
        **kwargs: Any,
    ) -> None:
        self._concrete_tasks = []
        self._dynamic_tasks = []
        self._fixtures = []
        self._middlewares = []
        self._poll_interval = poll_interval
        self._concurrency_limit = concurrency_limit
        self._repository_factory = repository_factory
        self._configuration = kwargs

    def task(self, **kwargs: Any) -> Decorator[TaskFunc]:
        """Decorator for marking the coroutine as a task"""

        def _func(func: TaskFunc) -> TaskFunc:
            task_kwargs = {
                **kwargs,
                "name": kwargs.get("name", None) or getattr(func, "__name__", None),
            }
            self._check_task_name(task_kwargs["name"])
            task = Task(func=func, dynamic=False, **task_kwargs)
            self._concrete_tasks.append(task)
            return task

        return _func

    def dynamic_task(self, **kwargs: Any) -> Decorator[TaskFunc]:
        """Decorator for marking the coroutine as a dynamic task"""

        def _func(func: TaskFunc) -> TaskFunc:
            task_kwargs = {
                **kwargs,
                "name": kwargs.get("name", None) or getattr(func, "__name__", None),
            }

            self._check_task_name(task_kwargs["name"])
            task = Task(func=func, dynamic=True, **task_kwargs)
            self._dynamic_tasks.append(task)
            return task

        return _func

    def _check_task_name(self, name: Optional[str]) -> None:
        if name is None:
            raise ValueError("Unable to determine name for the task")

        for task in chain(self._concrete_tasks, self._dynamic_tasks):
            if task.name == name:
                raise ValueError(f"Duplicate task name {name}")

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

    def middleware(self, func: MiddlewareFunc) -> MiddlewareFunc:
        """Decorator for marking the function as a middleware"""
        self._middlewares.append(func)
        return func

    @contextlib.asynccontextmanager
    async def create(self) -> AsyncIterator[PyncetteContext]:
        """Creates the execution context."""
        async with self._repository_factory(
            **self._configuration
        ) as repository, DefaultScheduler(
            self._concurrency_limit
        ) as scheduler, contextlib.AsyncExitStack() as stack:
            root_context = await self._create_root_context(repository, stack)

            yield PyncetteContext(self, repository, scheduler, root_context)

    def _setup_signal_handler(self, context: PyncetteContext) -> None:
        def handler(signum: Any, frame: Any) -> None:
            if not context._shutting_down.is_set():
                context.shutdown()
            else:
                logger.warning("Terminating")
                logging.shutdown()
                sys.exit(1)

        signal.signal(signal.SIGINT, handler)

    async def _create_root_context(
        self, repository: Repository, stack: contextlib.AsyncExitStack
    ) -> Context:
        context = Context()

        for name, callback in self._fixtures:
            setattr(context, name, await stack.enter_async_context(callback()))
        return context

    async def _run_main(self) -> None:
        async with self.create() as context:
            self._setup_signal_handler(context)
            await context.run()

    def main(self) -> None:
        """Convenience entrypoint for console apps, which sets up logging and signal handling."""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._run_main())
