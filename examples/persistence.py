"""

This example stores the state of the scheduler in a local SQLite database.

It is safe to run multiple instances of the app on the same machine, as the DB will be used for coordination.

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
