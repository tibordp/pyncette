from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any
from typing import Awaitable

logger = logging.getLogger(__name__)


class SynchronousExecutor(contextlib.AbstractAsyncContextManager):
    def __init__(self, **kwargs: dict[str, Any]):
        pass

    async def __aenter__(self) -> SynchronousExecutor:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any | None,
    ) -> None:
        pass

    async def spawn_task(self, task: Awaitable) -> None:
        await task


class DefaultExecutor(contextlib.AbstractAsyncContextManager):
    """Manages the spawned tasks running in background"""

    _tasks: dict[object, asyncio.Task]
    _semaphore: asyncio.Semaphore

    def __init__(self, **kwargs: Any) -> None:
        self._tasks = {}
        concurrency_limit = kwargs.get("concurrency_limit", 100)
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def __aenter__(self) -> DefaultExecutor:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any | None,
    ) -> None:
        if self._tasks:
            logging.debug(f"{exc_type}, {exc_value}, {traceback}")
            if exc_type == asyncio.CancelledError:
                logger.warning("Cancelling remaining tasks.")
                for task in self._tasks.values():
                    task.cancel()

            logger.info("Waiting for remaining tasks to finish.")
            await asyncio.wait(self._tasks.values())

    async def spawn_task(self, task: Awaitable) -> None:
        identity = object()

        async def _task_wrapper(awaitable: Awaitable) -> None:
            try:
                await awaitable
            finally:
                self._tasks.pop(identity)
                self._semaphore.release()

        await self._semaphore.acquire()
        self._tasks[identity] = asyncio.create_task(_task_wrapper(task))
