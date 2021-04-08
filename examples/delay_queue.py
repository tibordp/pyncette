"""

This example uses Pyncette to implement a reliable delay queue (persistence is needed for durability
or for running multiple instances of the app at the same time, see examples/persistence.py for details)

After the task instance suceeds it will not be scheduled again as with recurrent tasks, however,
if an exception is raised, it will be retried if ExecutionMode.AT_LEAST_ONCE is used.

"""

import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import Pyncette

logger = logging.getLogger(__name__)

app = Pyncette()


@app.dynamic_task(execution_mode=ExecutionMode.AT_LEAST_ONCE)
async def execute_once_reliable(context: Context) -> None:
    logger.info(
        f"I am {context.args['username']}. If I fail, I will be retried, otherwise I will never be seen again."
        f"(I was scheduled to run at {context.scheduled_at})"
    )

    if random.choice([True, False]):
        raise Exception("Oops")


@app.dynamic_task(execution_mode=ExecutionMode.AT_MOST_ONCE)
async def execute_once_best_effort(context: Context) -> None:
    logger.info(
        f"I am {context.args['username']}. I will never be seen again "
        f"(I was scheduled to run at {context.scheduled_at})"
    )

    if random.choice([True, False]):
        raise Exception("Oops")


@app.task(interval=datetime.timedelta(seconds=2))
async def enqueue_periodically(context: Context) -> None:
    execute_at = context.scheduled_at + datetime.timedelta(seconds=random.randint(1, 5))

    await context.app_context.schedule_task(
        execute_once_reliable,
        str(uuid.uuid4()),
        execute_at=execute_at,
        username=random.choice(["Alice", "Bob", "Charlie", "Dave", "Eve"]),
    )

    await context.app_context.schedule_task(
        execute_once_best_effort,
        str(uuid.uuid4()),
        execute_at=execute_at,
        username=random.choice(["Alice", "Bob", "Charlie", "Dave", "Eve"]),
    )


if __name__ == "__main__":
    app.main()
