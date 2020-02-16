"""

Pyncette ships with an optional Prometheus instrumentation based on the official prometheus_client
Python package. It includes the following metrics:

- Task duration [Histogram]
- Task volume [Counter]
- Task failures [Counter]
- Number of currently executing tasks [Gauge]
- Task run staleness (i.e. how far behind the scheduled time the actual executions are) [Histogram]
- Repository operation duration [Histogram]
- Repository operation volume [Counter]
- Repository operation volume [Failures]
- Number of currently repository operations [Gauge]

It pushes the metrics to default registry (prometheus_client.REGISTRY), so it can be combined with other
code alongside it.

To see the exported metrics while running this example, use something like

    curl localhost:5000/metrics

"""

import asyncio
import datetime
import logging
import random
import uuid

from prometheus_client import start_http_server

from pyncette import Context
from pyncette import FailureMode
from pyncette import Pyncette

# pip install pyncette[prometheus]
from pyncette.prometheus import prometheus_middleware
from pyncette.prometheus import prometheus_repository
from pyncette.repository import in_memory_repository

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Pyncette(repository_factory=prometheus_repository(in_memory_repository))
app.middleware(prometheus_middleware)


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context):
    logger.info("Hello, world!")


@app.task(schedule="* * * * * */2")
async def sleepy_time(context: Context):
    logger.info("Hello, bed!")
    await asyncio.sleep(random.random() * 5)


@app.task(schedule="* * * * * */2", failure_mode=FailureMode.UNLOCK)
async def oopsie_daisy(context: Context):
    if random.choice([True, False]):
        raise Exception("Something went wrong :(")


@app.dynamic_task()
async def execute_once(context: Context):
    logger.info(f"Hello, world from {context.task}")
    await context.app_context.unschedule_task(context.task)


@app.task(interval=datetime.timedelta(seconds=1))
async def schedule_execute_once(context: Context):
    await context.app_context.schedule_task(
        execute_once, str(uuid.uuid4()), interval=datetime.timedelta(seconds=1)
    )


if __name__ == "__main__":
    start_http_server(port=5000, addr="0.0.0.0")
    app.main()
