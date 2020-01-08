from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from typing import Awaitable
from typing import Dict
from typing import Optional
from typing import Type


class DefaultScheduler(contextlib.AbstractAsyncContextManager):
    """Manages the spawned tasks running in background"""

    _tasks: Dict[object, Awaitable]
    _semaphore: asyncio.Semaphore

    def __init__(self, concurrency_limit: int) -> None:
        self._tasks = dict()
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def __aenter__(self) -> DefaultScheduler:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        await asyncio.gather(*self._tasks.values())

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
