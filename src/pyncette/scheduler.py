from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from typing import Awaitable
from typing import Dict
from typing import Optional
from typing import Type


class DefaultScheduler(contextlib.AbstractAsyncContextManager):
    _tasks: Dict[object, Awaitable]

    def __init__(self) -> None:
        self._tasks = dict()

    async def __aenter__(self) -> DefaultScheduler:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        await asyncio.gather(*self._tasks.values())

    def spawn_task(self, task: Awaitable) -> None:
        identity = object()

        async def _task_wrapper(awaitable: Awaitable) -> None:
            try:
                await awaitable
            finally:
                self._tasks.pop(identity)

        self._tasks[identity] = asyncio.create_task(_task_wrapper(task))
