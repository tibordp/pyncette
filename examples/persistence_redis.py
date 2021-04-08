"""

This example stores the state of the scheduler in Redis.

It is safe to run multiple instances of the app, as the DB will be used for coordination.

"""

import logging

from pyncette import Context
from pyncette import Pyncette
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
    # Batch size for querying dynamic tasks
    batch_size=10,
)


@app.task(schedule="* * * * * */2")
async def hello_world(context: Context) -> None:
    logger.info("Hello, world!")


if __name__ == "__main__":
    app.main()
