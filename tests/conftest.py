import os
import random
from contextlib import asynccontextmanager

from pyncette.postgres import postgres_repository
from pyncette.redis import redis_repository
from pyncette.sqlite import sqlite_repository


def wrap_factory(factory, timemachine):
    @asynccontextmanager
    async def wrapped_factory(*args, **kwargs):
        async with factory(*args, **kwargs) as repo:
            yield timemachine.decorate_io(repo)

    return wrapped_factory


def random_table_name():
    return "pyncette_{}".format(
        "".join([chr(random.randint(ord("a"), ord("z"))) for _ in range(10)])
    )


# Define new configurations here


def postgres(timemachine):
    return {
        "repository_factory": wrap_factory(postgres_repository, timemachine),
        "postgres_table_name": random_table_name(),
        "postgres_url": os.environ.get(
            "POSTGRES_URL", "postgres://postgres@localhost/pyncette"
        ),
    }


def redis(timemachine):
    return {
        "repository_factory": wrap_factory(redis_repository, timemachine),
        "redis_namespace": random_table_name(),
        "redis_timeout": 10,
        "redis_url": os.environ.get("REDIS_URL", "redis://localhost"),
    }


def sqlite_persisted(timemachine):
    return {
        "repository_factory": sqlite_repository,
        "sqlite_database": os.environ.get("SQLITE_DATABASE", "pyncette.db"),
        "sqlite_table_name": random_table_name(),
    }


def sqlite(timemachine):
    return {
        "repository_factory": sqlite_repository,
    }


def pytest_addoption(parser):
    parser.addoption(
        "--repository",
        action="append",
        default=["sqlite"],
        help="list of repositories to test with",
    )


def pytest_generate_tests(metafunc):
    if "create_args" in metafunc.fixturenames:
        all_repositories = [postgres, redis, sqlite, sqlite_persisted]
        metafunc.parametrize(
            "create_args",
            [
                repository
                for repository in all_repositories
                if repository.__name__ in metafunc.config.getoption("repository")
            ],
        )
