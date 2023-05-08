"""

This example stores the state of the scheduler in a variety of backends supported by Pyncette

By having a persistent backend, you can run multiple multiple processes and they will coordinate
execution among them, making sure that tasks are only executed by one of them on schedule.

"""

import logging

from pyncette import Context
from pyncette import Pyncette
from pyncette.dynamodb import dynamodb_repository
from pyncette.mysql import mysql_repository
from pyncette.postgres import postgres_repository
from pyncette.redis import redis_repository

logger = logging.getLogger(__name__)

sqlite_app = Pyncette(sqlite_database="pyncette.db")

postgres_app = Pyncette(
    repository_factory=postgres_repository,
    # PostgreSQL connection string
    postgres_url="postgres://postgres@localhost/pyncette",
    # The table name
    postgres_table_name="example123",
    # If set to true, Pyncette will assume the table exists and will not try to create it
    postgres_skip_table_create=False,
    # Batch size for querying dynamic tasks
    batch_size=10,
)

dynamodb_app = Pyncette(
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

redis_app = Pyncette(
    repository_factory=redis_repository,
    # Redis URL
    redis_url="redis://localhost",
    # Key prefix in Redis, allowing multiple Pyncette apps to share the same
    # Redis instance
    redis_namespace="example123",
    # Timeout in seconds for Redis operations
    redis_timeout=10,
    # Batch size for querying dynamic tasks
    batch_size=10,
)


mysql_app = Pyncette(
    repository_factory=mysql_repository,  # type: ignore
    # MySQL host
    mysql_host="localhost",
    # MySQL database name
    mysql_database="pyncette",
    # MySQL username
    mysql_user="pyncette",
    # Optional MySQL password
    mysql_password="password",  # noqa: S106
    # The table name
    mysql_table_name="example123",
    # Optional MySQL port
    mysql_port=3306,
    # If set to true, Pyncette will assume the table exists and will not try to create it
    mysql_skip_table_create=False,
    # Batch size for querying dynamic tasks
    batch_size=10,
)

# Choose one of the above
app = sqlite_app


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context) -> None:
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
