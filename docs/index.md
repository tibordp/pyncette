# Pyncette

A reliable distributed scheduler with pluggable storage backends for Async Python.

## Overview

Pyncette is designed for reliable (at-least-once or at-most-once) execution of recurring tasks (think cronjobs) whose
lifecycles are managed dynamically, but can work effectively for non-reccuring tasks too.

Example use cases:

- You want to perform a database backup every day at noon
- You want a report to be generated daily for your 10M users at the time of their choosing
- You want currency conversion rates to be refreshed every 10 seconds
- You want to allow your users to schedule non-recurring emails to be sent at an arbitrary time in the future

Pyncette might not be a good fit if:

- You want your tasks to be scheduled to run (ideally) once as soon as possible. It is doable, but you will be better served by a general purpose reliable queue like RabbitMQ or Amazon SQS.
- You need tasks to execute at sub-second intervals with low jitter. Pyncette coordinates execution on a per task-instance basis and this corrdination can add overhead and jitter.

## Quick Start

Simple in-memory scheduler (does not persist state)

```python
from pyncette import Pyncette, Context

app = Pyncette()


@app.task(schedule="* * * * *")
async def foo(context: Context):
    print("This will run every minute")


if __name__ == "__main__":
    app.main()
```

Persistent distributed cron using Redis (coordinates execution with parallel instances and survives restarts)

```python
from pyncette import Pyncette, Context
from pyncette.redis import redis_repository

app = Pyncette(repository_factory=redis_repository, redis_url="redis://localhost")


@app.task(schedule="* * * * * */10")
async def foo(context: Context):
    print("This will run every 10 seconds")


if __name__ == "__main__":
    app.main()
```

See the `examples` directory for more examples of usage.

## Supported backends

Pyncette comes with an implementation for the following backends (used for persistence and coordination) out-of-the-box:

- SQLite (included)
- Redis (`pip install pyncette[redis]`)
- PostgreSQL (`pip install pyncette[postgres]`)
- MySQL 8.0+ (`pip install pyncette[mysql]`)
- Amazon DynamoDB (`pip install pyncette[dynamodb]`)

Pyncette imposes few requirements on the underlying datastores, so it can be extended to support other databases or
custom storage formats / integrations with existing systems. For best results, the backend needs to provide:

- Some sort of serialization mechanism, e.g. traditional transactions, atomic stored procedures or compare-and-swap
- Efficient range queries over a secondary index, which can be eventually consistent

## Features

- **Reliable execution**: At-least-once or at-most-once execution guarantees
- **Distributed coordination**: Run multiple instances without duplicate task execution
- **Flexible scheduling**: Cron-like syntax or interval-based scheduling
- **Dynamic tasks**: Register and unregister tasks at runtime
- **Timezone support**: Schedule tasks in different timezones
- **Heartbeating**: Keep long-running tasks alive with cooperative or automatic heartbeating
- **Middleware support**: Add custom logic around task execution
- **Pluggable backends**: SQLite, Redis, PostgreSQL, MySQL, and DynamoDB support

## License

Free software: MIT license
