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

logger = logging.getLogger(__name__)
app = Pyncette(sqlite_database="pyncette.db")


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context):
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
