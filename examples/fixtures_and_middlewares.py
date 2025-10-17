"""

This example illustrates the use of fixtures and middlewares.

Middlewares are functions that wrap the execution of every defined task, so they are a good
place to put cross-cutting concerns such as logging, database session management, metrics, ...

Fixtures can be thought of application-level middlewares. They wrap the lifecycle of the entire
Pyncette app and can be used to perform initialization, cleanup and can inject resources such as
service clients to the task context.

"""

import asyncio
import logging
import pathlib
import random
import time
from collections.abc import AsyncIterator
from typing import TextIO

from pyncette import Context
from pyncette import Pyncette
from pyncette import PyncetteContext
from pyncette.model import NextFunc

logger = logging.getLogger(__name__)

app = Pyncette()


@app.fixture(name="log")
async def logfile_fixture(app_context: PyncetteContext) -> AsyncIterator[TextIO]:
    logger.info("Using log file logfile.txt")

    with pathlib.Path("./logfile.txt").open("a") as f:
        # Yielding from fixture gives an object that will be available in
        # context.<fixture name> for all tasks (and middlewares)
        yield f

    # This will run on graceful shutdown of the Pyncette app
    logger.info("Log file closed")


@app.middleware
async def timer_middleware(context: Context, next: NextFunc) -> None:
    start_time = time.time()
    try:
        await next()
    finally:
        duration = time.time() - start_time
        print(f"Task {context.task.name} took {duration:,.2}s.", file=context.log)
        context.log.flush()


@app.task(schedule="* * * * * */2")
async def slow_task(context: Context) -> None:
    await asyncio.sleep(random.uniform(0, 1))


@app.task(schedule="* * * * * */2")
async def fast_task(context: Context) -> None:
    pass


if __name__ == "__main__":
    app.main()
