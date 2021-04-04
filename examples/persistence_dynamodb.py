"""

This example stores the state of the scheduler in a Amazon DynamoDB database.

It is safe to run multiple instances of the app, as the DB will be used for coordination.

"""

import asyncio
import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import ExecutionMode
from pyncette import FailureMode
from pyncette import Pyncette
from pyncette.dynamodb import dynamodb_repository

logger = logging.getLogger(__name__)

app = Pyncette(
    repository_factory=dynamodb_repository,
    # Optional endpoint URL (if e.g. using Localstack instead of actual DynamoDB)
    dynamodb_endpoint=None,
    # AWS region name
    dynamodb_region_name="eu-west-1",
    # The name of the DynamoDB table.
    dynamodb_table_name="pyncette",
    # Optional partition key prefix allowing multiple independent Pyncette instances
    # to use the same table.
    dynamodb_partition_prefix="example123",
    # If set to true, Pyncette will assume the table exists and will not try to create it
    dynamodb_skip_table_create=False,
    # Batch size for querying dynamic tasks
    batch_size=10,
)


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context):
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
