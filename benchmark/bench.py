import argparse
import asyncio
import datetime
import logging
from multiprocessing import Pool
from uuid import uuid4

from pyncette import Context
from pyncette import Pyncette
from pyncette.redis import redis_repository

app = Pyncette(
    redis_url="redis://localhost",
    redis_namespace="benchmark",
    repository_factory=redis_repository,
    redis_batch_size=50,
)


@app.dynamic_task()
async def hello(context: Context) -> None:
    pass


async def setup(hour):
    num_per_second = 100

    async with app.create() as ctx:
        for minute in range(60):
            for second in range(60):
                await asyncio.gather(
                    *[
                        ctx.schedule_task(
                            hello,
                            str(uuid4()),
                            schedule=f"{minute} {hour} * * * {second}",
                        )
                        for _ in range(num_per_second)
                    ]
                )
            print(f"Inserting {hour}:{minute} done")


def f(hour):
    asyncio.run(setup(hour))


parser = argparse.ArgumentParser(description="Pyncette benchmark")
parser.add_argument("--setup", help="Prepare the tasks", action="store_true")
parser.add_argument("--run", help="Run the tasks", action="store_true")
args = parser.parse_args()

if args.setup:
    p = Pool(5)
    current_hour = datetime.datetime.utcnow().hour - 1
    p.map(f, list(range(current_hour, current_hour + 5)))

if args.run:
    logging.basicConfig(level=logging.DEBUG)
    app.main()
