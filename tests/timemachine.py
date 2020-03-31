import asyncio
import datetime
import heapq
import inspect
import logging
from functools import total_ordering

logger = logging.getLogger(__name__)


@total_ordering
class ScheduledTask:
    def __init__(self, execute_at, future):
        self.execute_at = execute_at
        self.future = future

    def __lt__(self, other):
        return self.execute_at.__lt__(other.execute_at)

    def __eq__(self, other):
        return self.execute_at.__eq__(other.execute_at)


SPIN_ITERATIONS = 10


class TimeMachine:
    """Utility class that allows us to mock real time in a way that plays well with asyncio without implementing a custom event loop."""

    def __init__(self, base_time):
        self.callbacks = []
        self.io_tasks = []
        self.base_time = base_time
        self.offset = datetime.timedelta(seconds=0)

    def decorate_io(self, obj):
        """
        Decorates the class or function in a way the way that Timemachine actually waits for I/O
        operations to complete before proceeding with time shifting. From the application's point
        of view all decorated I/O operations happen instantaneously.
        """

        def wrapper(func):
            async def wrapped(*args, **kwargs):
                future = asyncio.Future()
                self.io_tasks.append(future)
                try:
                    return await func(*args, **kwargs)
                finally:
                    future.set_result(None)

            return wrapped

        if inspect.iscoroutinefunction(obj):
            return wrapper(obj)
        else:
            for name, fn in inspect.getmembers(obj):
                if inspect.iscoroutinefunction(fn):
                    setattr(obj, name, wrapper(fn))
            return obj

    def sleep(self, delay, *args, **kwargs):
        future = asyncio.Future()
        heapq.heappush(
            self.callbacks,
            ScheduledTask(self.offset + datetime.timedelta(seconds=delay), future),
        )
        future.add_done_callback(self._remove_cancelled_sleep)
        logger.info(
            f"Registering sleep {id(future)} for {delay}s (resume at T+{self.offset + datetime.timedelta(seconds=delay)})"
        )
        return future

    def wait_for(self, fut, timeout, *args, **kwargs):
        if timeout is None:
            return fut

        future = asyncio.Future()
        fut = asyncio.ensure_future(fut)
        wait_handle = self.sleep(timeout)

        def _on_timeout(f):
            try:
                future.set_exception(asyncio.TimeoutError())
            except asyncio.InvalidStateError:
                pass

        def _on_completion(f):
            try:
                future.set_result(f.result())
                wait_handle.cancel()
            except asyncio.CancelledError:
                pass
            except asyncio.InvalidStateError:
                pass

        wait_handle.add_done_callback(_on_timeout)
        fut.add_done_callback(_on_completion)
        return future

    def perf_counter(self):
        return self.offset.total_seconds()

    def utcnow(self):
        return self.base_time + self.offset

    async def unwind(self):
        """Jumps to "infinity". I.e. continues executing until no more sleeps appear"""
        await self._spin()
        while len(self.callbacks) > 0:
            task = heapq.heappop(self.callbacks)
            self._update_offset(task.execute_at)
            try:
                task.future.set_result(None)
            except asyncio.InvalidStateError:
                pass
            await self._spin()

    async def step(self, delta=None):
        if delta is None:
            await self._spin()
        else:
            await self.jump_to(self.offset + delta)

    async def jump_to(self, offset):
        if offset < self.offset:
            raise ValueError("Cannot go back in time (yet)!")

        await self._spin()
        while len(self.callbacks) > 0:
            if self.callbacks[0].execute_at > offset:
                break
            task = heapq.heappop(self.callbacks)
            self._update_offset(task.execute_at)
            try:
                task.future.set_result(None)
            except asyncio.InvalidStateError:
                pass
            await self._spin()
        self._update_offset(offset)

    def _remove_cancelled_sleep(self, fut):
        if fut.cancelled:
            try:
                self.callbacks = [
                    callback
                    for callback in self.callbacks
                    if callback.future is not fut
                ]
                heapq.heapify(self.callbacks)
                logger.info(f"Removed cancelled sleep {id(fut)}")
            except ValueError:
                pass

    async def _spin(self):
        for _ in range(SPIN_ITERATIONS):
            # First we wait for any pending I/O futures to complete
            if self.io_tasks:
                io_tasks = self.io_tasks
                self.io_tasks = []
                await asyncio.gather(*io_tasks)

            # Then we just jump to the back of the callback queue before completing
            future = asyncio.Future()
            loop = asyncio.get_event_loop()
            loop.call_soon(future.set_result, None)
            await future

    def _update_offset(self, new_offset):
        if self.offset != new_offset:
            self.offset = new_offset
            logger.info(f"Jumped to T+{new_offset.total_seconds()}s")
