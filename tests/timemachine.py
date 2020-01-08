import asyncio
import datetime
import logging

logger = logging.getLogger(__name__)


async def defer():
    """Schedules the continuation to run after all the currently queued continuations in the event loop"""
    future = asyncio.Future()
    loop = asyncio.get_event_loop()
    loop.call_soon(future.set_result, None)
    await future


class TimeMachine:
    def __init__(self, base_time):
        self.callbacks = []
        self.base_time = base_time
        self.offset = datetime.timedelta(seconds=0)

    def sleep(self, seconds):
        future = asyncio.Future()

        self.callbacks.append(
            (self.offset + datetime.timedelta(seconds=seconds), future)
        )
        logger.info(
            f"Registering sleep for {seconds}s (resume at T+{self.offset + datetime.timedelta(seconds=seconds)})"
        )
        return future

    def utcnow(self):
        return self.base_time + self.offset

    async def step(self, delta):
        await defer()
        initial_offset = self.offset
        while len(self.callbacks) > 0:
            self.callbacks.sort(key=lambda x: x[0])
            if self.callbacks[0][0] > initial_offset + delta:
                break
            resume_at, future = self.callbacks.pop(0)
            self.offset = resume_at
            logger.info(f"Jumped to T+{resume_at.total_seconds()}s")
            future.set_result(None)
            await defer()

        self.offset = initial_offset + delta
        logger.info(f"Jumped to T+{(initial_offset + delta).total_seconds()}s")
