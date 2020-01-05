import asyncio
from typing import Awaitable
from typing import Dict


class DefaultScheduler:
    _tasks: Dict[object, Awaitable]

    def __init__(self):
        self._tasks = dict()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await asyncio.gather(*self._tasks.values())

    def spawn_task(self, task):
        identity = object()

        async def _task_wrapper(awaitable: Awaitable):
            try:
                await awaitable
            finally:
                self._tasks.pop(identity)

        self._tasks[identity] = asyncio.create_task(_task_wrapper(task))
