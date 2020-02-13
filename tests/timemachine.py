import asyncio
import datetime
import heapq
import logging
import time
from functools import total_ordering

import dateutil
import pytest

import pyncette

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


class TimeMachine:
    """Utility class that allows us to mock real time in a way that plays well with asyncio without implementing a custom event loop."""

    def __init__(self, base_time):
        self.callbacks = []
        self.base_time = base_time
        self.closed = False
        self.offset = datetime.timedelta(seconds=0)

    def sleep(self, seconds):
        future = asyncio.Future()
        if self.closed:
            future.set_result(None)
            return future

        heapq.heappush(
            self.callbacks,
            ScheduledTask(self.offset + datetime.timedelta(seconds=seconds), future),
        )
        logger.info(
            f"Registering sleep for {seconds}s (resume at T+{self.offset + datetime.timedelta(seconds=seconds)})"
        )
        return future

    def wait_for(self, awaitable, timeout):
        future = asyncio.Future()
        awaitable = asyncio.ensure_future(awaitable)
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
        awaitable.add_done_callback(_on_completion)
        return future

    def perf_counter(self):
        return self.offset.total_seconds()

    def utcnow(self):
        return self.base_time + self.offset

    async def _spin(self):
        """A hack to ensure that all the callbacks have executed after we advance the time, so we can assert immediately after"""
        for _ in range(10):
            future = asyncio.Future()
            loop = asyncio.get_event_loop()
            loop.call_soon(future.set_result, None)
            await future

    async def close(self):
        await self._spin()
        self._closed = True

        while len(self.callbacks) > 0:
            task = heapq.heappop(self.callbacks)
            try:
                task.future.set_result(None)
            except asyncio.InvalidStateError:
                pass
            await self._spin()

    async def step(self, delta):
        await self._spin()
        initial_offset = self.offset
        while len(self.callbacks) > 0:
            if self.callbacks[0].execute_at > initial_offset + delta:
                break
            task = heapq.heappop(self.callbacks)
            self.offset = task.execute_at
            logger.info(f"Jumped to T+{task.execute_at.total_seconds()}s")
            try:
                task.future.set_result(None)
            except asyncio.InvalidStateError:
                pass
            await self._spin()

        self.offset = initial_offset + delta
        logger.info(f"Jumped to T+{(initial_offset + delta).total_seconds()}s")


@pytest.fixture
def timemachine(monkeypatch):
    timemachine = TimeMachine(
        datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=dateutil.tz.UTC)
    )
    monkeypatch.setattr(pyncette.pyncette, "_current_time", timemachine.utcnow)
    monkeypatch.setattr(asyncio, "sleep", timemachine.sleep)
    monkeypatch.setattr(asyncio, "wait_for", timemachine.wait_for)
    monkeypatch.setattr(time, "perf_counter", timemachine.perf_counter)
    return timemachine
