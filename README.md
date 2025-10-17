# Overview

[![Documentation](https://img.shields.io/badge/docs-github%20pages-blue)](https://tibordp.github.io/pyncette/)
[![Github Actions Build Status](https://github.com/tibordp/pyncette/workflows/Python%20package/badge.svg?branch=master)](https://github.com/tibordp/pyncette/actions?query=branch%3Amaster+workflow%3A%22Python+package%22)
[![Coverage Status](https://codecov.io/gh/tibordp/pyncette/branch/master/graphs/badge.svg?branch=master)](https://codecov.io/github/tibordp/pyncette)
[![PyPI Package latest release](https://img.shields.io/pypi/v/pyncette.svg)](https://pypi.org/project/pyncette)
[![PyPI Wheel](https://img.shields.io/pypi/wheel/pyncette.svg)](https://pypi.org/project/pyncette)
[![Supported versions](https://img.shields.io/pypi/pyversions/pyncette.svg)](https://pypi.org/project/pyncette)
[![Supported implementations](https://img.shields.io/pypi/implementation/pyncette.svg)](https://pypi.org/project/pyncette)
[![Commits since latest release](https://img.shields.io/github/commits-since/tibordp/pyncette/v1.0.0.svg)](https://github.com/tibordp/pyncette/compare/v1.0.0...master)

A reliable distributed scheduler with pluggable storage backends for Async Python.

- Free software: MIT license

## Installation

Minimal installation (just SQLite persistence):

```bash
pip install pyncette
```

Full installation (all the backends and Prometheus metrics exporter):

```bash
pip install pyncette[all]
```

You can also install the in-development version with:

```bash
pip install https://github.com/tibordp/pyncette/archive/master.zip
```

## Documentation

https://tibordp.github.io/pyncette/

## Usage example

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

## Use cases

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

## Development

### Prerequisites

Install [uv](https://docs.astral.sh/uv/) for fast package management:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Setup Development Environment

Sync dependencies and install the package in editable mode:

```bash
uv sync --extra all --extra dev
```

### Running Tests

**Unit tests** (fast, no external dependencies):

```bash
uv run pytest -m "not integration" tests
```

**Integration tests** (requires Redis, PostgreSQL, MySQL, DynamoDB):

Using Docker Compose to set up all backends:

```bash
docker-compose up -d
docker-compose run --rm shell
uv run pytest tests
```

Or manually with services running locally:

```bash
uv run pytest tests
```

**Test on specific Python version**:

```bash
uv venv --python 3.11
uv sync --extra all --extra dev
uv run pytest tests
```

### Code Quality

Run linting and type checking:

```bash
uv run pre-commit run --all-files
uv run ty check src examples
```

### Building Documentation

```bash
uv run mkdocs build
# Or serve locally with live reload
uv run mkdocs serve
```

### Building the Package

```bash
uv build
```
