"""

This example [ab]uses Pyncette to implement a reliable delay queue. It achieves this by scheduling
a dynamic task that unchedules itself after successful execution. If the task fails, it will keep
retrying ad infinitum every 60 seconds until it succeeds (the lease duration).

This example should uphold all the guarantees one would expect from a reliable distributed queue,
but as the Redis repository implementation is geared towards recurring tasks, it is not the most 
efficient or most elegant way of doing it.

"""

import asyncio
import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.redis import redis_repository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Pyncette(repository_factory=redis_repository, redis_url="redis://localhost")


@app.dynamic_task()
async def execute_once(context: Context):
    logger.info(f"Hello, {context.args['username']}")
    await context.app_context.unschedule_task(context.task)

    # Pyncette will try to commit the task at this point, but since it no longer exists,
    # there will be a lease mismatch and nothing will happen. However, it does incur an extra
    # round-trip to Redis, so we can raise a dummy exception instead, which should be slightly
    # more efficient at the expense of elegance.

    # raise Exception("don't commit me")


@app.task(interval=datetime.timedelta(seconds=1))
async def enqueue_periodically(context: Context):
    await context.app_context.schedule_task(
        execute_once,
        str(uuid.uuid4()),
        # The task will execute once after a random 1-5 second delay.
        interval=datetime.timedelta(seconds=random.randint(1, 5)),
        username=random.choice(["Alice", "Bob", "Charlie", "Dave", "Eve"]),
    )


if __name__ == "__main__":
    app.main()
