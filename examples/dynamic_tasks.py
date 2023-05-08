"""
This example illustrates dynamic tasks i.e. tasks that are not pre-defined in code and
can be scheduled at runtime.

Marking the function with @app.dynamic_task serves as a template and individual task
instances can be scheduled with schedule_task (and unscheduled with unschedule_task).

Using a persistent backend, Pyncette supports efficient execution of a large number of
dynamic task instances.

"""

import asyncio
import datetime
import logging
import pathlib
import random
import sys

import coloredlogs

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import Pyncette

logger = logging.getLogger(__name__)

app = Pyncette(poll_interval=datetime.timedelta(seconds=0.1))


@app.dynamic_task(execution_mode=ExecutionMode.AT_MOST_ONCE)
async def greeter(context: Context) -> None:
    logger.info(f"Hello from {context.args['username']}.")

    if random.random() < 0.2:
        # 1/5 chance that the task will unschedule itself. If this
        # example is run for long enough, no tasks should be left.
        logger.warning(f"Unscheduling {context.args['username']}")
        await context.app_context.unschedule_task(context.task)


async def main() -> None:
    async with app.create() as ctx:
        with (pathlib.Path(sys.path[0]) / "data" / "usernames.txt").open() as f:
            usernames = f.read().splitlines()

        for username in usernames:
            interval = datetime.timedelta(seconds=random.uniform(5, 20))
            logger.info(f"Scheduling {username} to run every {interval}")
            await ctx.schedule_task(
                greeter,
                # Mandatory unique name for the task instance
                username,
                interval=interval,
                # All the extra parameters will be available to the
                # the task in context.args
                username=username,
            )

        await ctx.run()


if __name__ == "__main__":
    coloredlogs.install(level="INFO", milliseconds=True, logger=logger)
    asyncio.run(main())
