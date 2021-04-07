"""

This example uses Pyncette to implement a reliable delay queue (persistence is needed for durability
or for running multiple instances of the app at the same time, see examples/persistence-*.py for details)

After the task instance suceeds it will not be scheduled again as with recurrent tasks, however, 
if an exception is raised, it will be retried if ExecutionMode.AT_LEAST_ONCE is used.

"""

import argparse
import asyncio
import datetime
import logging
import multiprocessing
import random
import time
import uuid
from typing import List

import coloredlogs
import uvloop

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.dynamodb import dynamodb_repository
from pyncette.executor import SynchronousExecutor
from pyncette.redis import redis_repository

logger = logging.getLogger(__name__)

app = Pyncette(
    #repository_factory=dynamodb_repository,
    #dynamodb_region_name="eu-west-1",
    #dynamodb_table_name="pyncette",
    repository_factory=redis_repository,
    redis_url="redis://localhost",
    redis_namespace="example1",
    # executor_cls=SynchronousExecutor
    batch_size=100
)


@app.partitioned_task(partition_count=8, execution_mode=ExecutionMode.AT_MOST_ONCE)
async def benchmark_task(context: Context):
    context.hit_count.value += 1
    context.staleness.value = (
        datetime.datetime.now(datetime.timezone.utc) - context.app_context.last_tick
    ).total_seconds()


async def populate(n: int, parallel: int) -> None:
    async with app.create() as app_context:
        tasks = []
        for i in range(n):
            interval = datetime.timedelta(seconds=random.randrange(10, 3600))
            tasks.append(
                app_context.schedule_task(
                    benchmark_task, str(uuid.uuid4()), interval=interval
                )
            )

            if len(tasks) == parallel:
                await asyncio.gather(*tasks)
                tasks = []

            if (i + 1) % 1000 == 0:
                logger.info(f"Scheduled {i+1} tasks")

        await asyncio.gather(*tasks)
        logger.info(f"DONE!")


async def run(
    hit_count: multiprocessing.RawValue, staleness: multiprocessing.RawValue
) -> None:
    async with app.create() as app_context:
        app_context._root_context.hit_count = hit_count
        app_context._root_context.staleness = staleness

        await app_context.run()


async def report(
    hit_counts: List[multiprocessing.RawValue],
    stalenesses: List[multiprocessing.RawValue],
) -> None:
    previous_hit_count = 0
    previous_sample = time.perf_counter()

    while True:
        await asyncio.sleep(1)

        hit_count = sum(c.value for c in hit_counts)
        staleness = max(c.value for c in stalenesses)
        now = time.perf_counter()

        print(
            "{:,} RPS, {}".format(
                (hit_count - previous_hit_count) / (now - previous_sample), staleness
            )
        )

        previous_hit_count = hit_count
        previous_sample = now


def _run(*args, **kwargs) -> None:
    asyncio.run(run(*args, **kwargs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    populate_option = subparsers.add_parser("populate")
    populate_option.add_argument("-n", "--number", type=int, default=10000)
    populate_option.add_argument("-p", "--parallel", type=int, default=20)
    run_option = subparsers.add_parser("run")
    run_option.add_argument("--processes", type=int, default=1)

    options = parser.parse_args()
    coloredlogs.install(level=options.log_level, milliseconds=True)
    uvloop.install()

    if options.command == "run":
        hit_count = [multiprocessing.RawValue("l", 0) for _ in range(options.processes)]
        staleness = [multiprocessing.RawValue("f", 0) for _ in range(options.processes)]

        for i in range(options.processes):
            job = multiprocessing.Process(
                target=_run, name=str(i), args=(hit_count[i], staleness[i])
            )
            job.start()

        asyncio.run(report(hit_count, staleness))

    elif options.command == "populate":
        asyncio.run(populate(options.number, options.parallel))
