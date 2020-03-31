import asyncio
import datetime
import os
import random
import time
from contextlib import asynccontextmanager

import dateutil.tz
import pytest
from timemachine import TimeMachine

import pyncette
from pyncette.postgres import postgres_repository
from pyncette.redis import redis_repository
from pyncette.sqlite import sqlite_repository


@pytest.fixture
def timemachine(monkeypatch):
    timemachine = TimeMachine(
        datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=dateutil.tz.UTC)
    )
    monkeypatch.setattr(pyncette.pyncette, "_current_time", timemachine.utcnow)
    monkeypatch.setattr(asyncio, "sleep", timemachine.sleep)
    monkeypatch.setattr(asyncio, "wait_for", timemachine.wait_for)
    monkeypatch.setattr(time, "perf_counter", timemachine.perf_counter)
    return timemachine


def wrap_factory(factory, timemachine):
    @asynccontextmanager
    async def wrapped_factory(*args, **kwargs):
        async with factory(*args, **kwargs) as repo:
            yield timemachine.decorate_io(repo)

    return timemachine.decorate_io(wrapped_factory)


def random_table_name():
    return "pyncette_{}".format(
        "".join([chr(random.randint(ord("a"), ord("z"))) for _ in range(10)])
    )


# Define new configurations here


class PostgresBackend:
    __name__ = "postgres"
    is_persistent = True

    def get_args(self, timemachine):
        return {
            "repository_factory": wrap_factory(postgres_repository, timemachine),
            "postgres_table_name": random_table_name(),
            "postgres_url": os.environ.get(
                "POSTGRES_URL", "postgres://postgres@localhost/pyncette"
            ),
        }


class RedisBackend:
    __name__ = "redis"
    is_persistent = True

    def get_args(self, timemachine):
        return {
            "repository_factory": wrap_factory(redis_repository, timemachine),
            "redis_namespace": random_table_name(),
            "redis_timeout": 10,
            "redis_url": os.environ.get("REDIS_URL", "redis://localhost"),
        }


class SqlitePersistedBackend:
    __name__ = "sqlite_persisted"
    is_persistent = True

    def get_args(self, timemachine):
        return {
            "repository_factory": wrap_factory(sqlite_repository, timemachine),
            "sqlite_database": os.environ.get("SQLITE_DATABASE", "pyncette.db"),
            "sqlite_table_name": random_table_name(),
        }


class DefaultBackend:
    __name__ = "default"
    is_persistent = False

    def get_args(self, timemachine):
        return {"repository_factory": wrap_factory(sqlite_repository, timemachine)}


all_backends = [
    PostgresBackend(),
    RedisBackend(),
    DefaultBackend(),
    SqlitePersistedBackend(),
]


def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        action="append",
        default=[],
        help="list of repositories to test with",
    )


def pytest_generate_tests(metafunc):
    if "backend" in metafunc.fixturenames:
        metafunc.parametrize(
            "backend",
            [
                repository
                for repository in all_backends
                if repository.__name__ in metafunc.config.getoption("backend")
            ]
            or all_backends,
        )
