"""

This example stores the state of the scheduler in Redis.

It is safe to run multiple instances of the app on the same machine, as the DB will be 
used for coordination.

"""

import asyncio
import datetime
import logging
import random
import uuid

from pyncette import Context
from pyncette import FailureMode
from pyncette.redis import redis_repository

logger = logging.getLogger(__name__)

app = Pyncette(
    repository_factory=redis_repository,
    # Redis URL
    redis_url="redis://localhost",
    # Key prefix in Redis, allowing multiple Pyncette apps to share the same
    # Redis instance
    redis_namespace="example123",
    # Timeout in seconds for Redis operations
    redis_timeout=10,
)


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context):
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
