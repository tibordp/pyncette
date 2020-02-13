import asyncio
import datetime
import logging
import random

from prometheus_client import start_http_server

from pyncette import Context
from pyncette import FailureMode
from pyncette import Pyncette
# pip install pyncette[prometheus]
from pyncette.prometheus import metering_middleware
from pyncette.prometheus import metering_repository
from pyncette.repository import in_memory_repository

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Pyncette(
    repository_factory=metering_repository(in_memory_repository)
)
app.middleware(metering_middleware)

@app.task(schedule="* * * * * */2")
async def hello_world(context : Context):
    logger.info("Hello, world!")

@app.task(schedule="* * * * * */2")
async def sleepy_time(context : Context):
    logger.info("Hello, bed!")
    await asyncio.sleep(random.random() * 5)

@app.task(schedule="* * * * * */2", failure_mode=FailureMode.UNLOCK)
async def oopsie_daisy(context : Context):
    if random.choice([True, False]):
        raise Exception("Something went wrong :(")

if __name__ == "__main__":
    start_http_server(port=5000, addr="0.0.0.0")
    app.main()
