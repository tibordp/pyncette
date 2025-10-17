"""
This example schedules a large number of dynamic tasks and then runs them (in multiple processes)
as a way to gauge the total throughput of Pyncette for a particular backend.

To run this example, configure the selected backend in the Pyncette constructor, then run populate the database.

    python examples/benchmark.py populate -n <number of tasks to insert>

While the tasks are populating you can run

    python examples/benchmark.py run --processes <# of processes>

The process will continuously print the overall throughput (task executions per second) and the lag
(seconds since the last successful tick).
"""

import argparse
import asyncio
import datetime
import logging
import random
import time
import uuid
from multiprocessing import Process
from multiprocessing.sharedctypes import RawValue  # type: ignore
from typing import Any
from typing import Optional

import coloredlogs

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import Pyncette
from pyncette.redis import redis_repository

logger = logging.getLogger(__name__)

# Adjust the values below
app = Pyncette(
    repository_factory=redis_repository,
    redis_url="redis://localhost",
    redis_namespace="benchmark",
    batch_size=100,
)

PARTITION_COUNT = 32


@app.partitioned_task(partition_count=PARTITION_COUNT, execution_mode=ExecutionMode.AT_LEAST_ONCE)
async def benchmark_task(context: Context) -> None:
    context.hit_count.value += 1
    if context.app_context.last_tick is not None:
        context.staleness.value = (datetime.datetime.now(datetime.timezone.utc) - context.app_context.last_tick).total_seconds()


async def populate(n: int, parallel: int) -> None:
    """Populates the database with n instances of the dynamic tasks"""

    async with app.create() as app_context:
        tasks = []
        for i in range(n):
            interval = datetime.timedelta(seconds=random.randrange(10, 3600))
            tasks.append(app_context.schedule_task(benchmark_task, str(uuid.uuid4()), interval=interval))

            if len(tasks) == parallel:
                await asyncio.gather(*tasks)
                tasks = []

            if (i + 1) % 1000 == 0:
                logger.info(f"Scheduled {i + 1} tasks")

        await asyncio.gather(*tasks)
        logger.info("DONE!")


async def run(
    hit_count: Any,
    staleness: Any,
    enabled_partitions: Optional[list[int]],
) -> None:
    async with app.create() as app_context:
        app_context.add_to_context("hit_count", hit_count)
        app_context.add_to_context("staleness", staleness)
        benchmark_task.enabled_partitions = enabled_partitions

        logger.info(f"Starting to poll following partitions {enabled_partitions}")
        await app_context.run()


def _run(log_level: str, *args: Any, **kwargs: Any) -> None:
    # On Windows we need to setup logging again as forking is not supported
    setup(log_level)
    asyncio.run(run(*args, **kwargs))


def setup(log_level: str) -> None:
    # Make sure that this module logger always logs no matter what
    # the selected level is.
    coloredlogs.install(level="DEBUG", milliseconds=True)
    logging.getLogger().setLevel(log_level)
    logger.setLevel("INFO")

    try:
        import uvloop

        uvloop.install()
    except ImportError:
        logger.info("uvloop is not available, ignoring.")


async def report(
    hit_counts: list[Any],
    stalenesses: list[Any],
) -> None:
    previous_hit_count = 0
    previous_sample = time.perf_counter()

    while True:
        await asyncio.sleep(5)

        hit_count = sum(c.value for c in hit_counts)
        staleness = max(c.value for c in stalenesses)
        now = time.perf_counter()

        logger.info(f"{(hit_count - previous_hit_count) / (now - previous_sample):10.2f} RPS, Staleness {staleness:.2f}s")

        previous_hit_count = hit_count
        previous_sample = now


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--log-level", default="WARNING")
    subparsers = parser.add_subparsers(dest="command", required=True)

    populate_option = subparsers.add_parser("populate", help="Populate the backend with a large number of tasks")
    populate_option.add_argument("-n", "--number", type=int, default=10000, help="Number of tasks to insert")
    populate_option.add_argument(
        "-p",
        "--parallelism",
        type=int,
        default=50,
        help="How many tasks to insert in parallel",
    )
    run_option = subparsers.add_parser("run", help="Run the Pyncette app")
    run_option.add_argument("--processes", type=int, default=1, help="Number of processes to run")
    run_option.add_argument(
        "--partition-count",
        type=int,
        default=PARTITION_COUNT,
        help="How many partitions each process should poll",
    )

    options = parser.parse_args()
    setup(options.log_level)

    if options.command == "run":
        hit_count = [RawValue("l", 0) for _ in range(options.processes)]
        staleness = [RawValue("f", 0) for _ in range(options.processes)]

        if options.partition_count * options.processes < PARTITION_COUNT:
            logger.warning(f"partition_count * processes < {PARTITION_COUNT}. Not all partitions will be processed.")

        for i in range(options.processes):
            enabled_partitions = sorted((i * options.partition_count + j) % PARTITION_COUNT for j in range(options.partition_count))

            job = Process(
                target=_run,
                name=str(i),
                args=(
                    options.log_level,
                    hit_count[i],
                    staleness[i],
                    list(enabled_partitions),
                ),
            )
            job.start()

        asyncio.run(report(hit_count, staleness))

    elif options.command == "populate":
        asyncio.run(populate(options.number, options.parallelism))
