"""

This example demonstrates the heartbeating functionality, which allows for the lease on the 
task to be extended. This can be useful if tasks have an unpredictable run time to minimize
the risk of another instance taking over the lease.

Heartbeating can be either cooperative or automatic.

"""

import asyncio
import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.utils import with_heartbeat

logger = logging.getLogger(__name__)
app = Pyncette()


@app.task(schedule="* * * * * */2", lease_duration=datetime.timedelta(seconds=2))
async def cooperative_heartbeat(context: Context):
    logger.info("Hello, world!")
    for _ in range(5):
        await asyncio.sleep(1)
        await context.heartbeat()
    logger.info("Goodbye, world!")


@app.task(schedule="* * * * * */2", lease_duration=datetime.timedelta(seconds=2))
async def cooperative_heartbeat_lease_expired(context: Context):
    logger.info("Hello, world!")
    await asyncio.sleep(3)
    # This will raise an exception as we no longer have lease at this point
    await context.heartbeat()
    logger.info("Goodbye, world!")


@app.task(schedule="* * * * * */2", lease_duration=datetime.timedelta(seconds=2))
@with_heartbeat()
async def automatic_heartbeat(context: Context):
    """
    Tasks decorated with with_heartbeat will automatically heartbeat in background
    whenever we have less than 1/2 of the time remaining on the lease
    """

    logger.info("Hello, world!")
    await asyncio.sleep(5)
    logger.info("Goodbye, world!")


if __name__ == "__main__":
    app.main()
