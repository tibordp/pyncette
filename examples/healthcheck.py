"""

This example illustrates the use of healthcheck HTTP server. It exposes the /health endpoint
which returns 200 if last successfull poll was less than 2 poll intervals ago, 500 otherwise.

    curl localhost:8080/health

"""

import asyncio
import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import Pyncette
from pyncette.executor import SynchronousExecutor
from pyncette.healthcheck import use_healthcheck_server

logger = logging.getLogger(__name__)

# We use the SynchronousExecutor so long-running tasks will delay
# cause polling to stall and simulate unhealthiness.
app = Pyncette(executor_cls=SynchronousExecutor)
use_healthcheck_server(app, port=8080)


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context):
    if random.choice([True, False]):
        await asyncio.sleep(4)
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
