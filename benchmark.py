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
import sys
import time

from collections import defaultdict
from pyncette import Context
from pyncette import FailureMode, ExecutionMode
from pyncette import Pyncette
from pyncette.redis import redis_repository
from pyncette.postgres import postgres_repository

# app = Pyncette(repository_factory=redis_repository, redis_url="redis://localhost", redis_namespace="benchmark")
app = Pyncette(
    repository_factory=postgres_repository,
    postgres_url="postgres://postgres@localhost/pyncette",
    postgres_table_name="benchmark",
)

# app = Pyncette(sqlite_database="/tmp/ramdisk/benchmark.db")

counter = 0
staleness = datetime.timedelta(0)


@app.dynamic_task(execution_mode=ExecutionMode.AT_MOST_ONCE)
async def increment_counter(context: Context):
    global counter, staleness
    staleness = datetime.datetime.utcnow() - context.scheduled_at.replace(tzinfo=None)
    counter += 1


async def populate():
    async with app.create() as context:
        for _ in range(10000):
            await context.schedule_task(
                increment_counter,
                str(uuid.uuid4()),
                interval=datetime.timedelta(seconds=(2 + random.random() * 298)),
                username=random.choice(["Alice", "Bob", "Charlie", "Dave", "Eve"]),
            )


async def run():
    global counter, staleness

    async with app.create() as context:
        asyncio.create_task(context.run())

        previous_time = time.perf_counter()
        previous_value = 0

        while True:
            current_time = time.perf_counter()
            print(
                f"Executed {counter - previous_value} tasks ({(counter - previous_value) / (current_time - previous_time)}/s) (staleness={staleness.total_seconds()}s)"
            )

            previous_value = counter
            previous_time = current_time

            await asyncio.sleep(5)


if __name__ == "__main__":
    if sys.argv[1] == "populate":
        asyncio.run(populate())
    elif sys.argv[1] == "run":
        asyncio.run(run())
