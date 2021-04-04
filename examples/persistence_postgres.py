"""

This example stores the state of the scheduler in a PostgreSQL database.

It is safe to run multiple instances of the app, as the DB will be used for coordination.

"""

import asyncio
import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.postgres import postgres_repository

logger = logging.getLogger(__name__)
app = Pyncette(
    repository_factory=postgres_repository,
    # PostgreSQL connection string
    postgres_url="postgres://postgres@localhost/pyncette",
    # The table name
    postgres_table_name="example",
    # If set to true, Pyncette will assume the table exists and will not try to create it
    postgres_skip_table_create=False,
    # Batch size for querying dynamic tasks
    batch_size=10,
)


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context):
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
