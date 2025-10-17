import datetime
import os

import pymysql
import pytest

from pyncette import mysql, Context
from pyncette.task import Task

from conftest import random_table_name


async def dummy_task(context: Context):
    pass  # pragma: no cover


DUMMY_TASK = Task(name="foo", func=dummy_task, schedule="* * * * *")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_invalid_table_name():
    with pytest.raises(ValueError):
        await mysql.mysql_repository(
            mysql_host=os.environ.get("MYSQL_HOST", "localhost"),
            mysql_database=os.environ.get("MYSQL_DATABASE", "pyncette"),
            mysql_user=os.environ.get("MYSQL_USER", "pyncette"),
            mysql_password=os.environ.get("MYSQL_PASSWORD", "password"),
            mysql_table_name="spaces in table name",
        ).__aenter__()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_skip_table_create():
    with pytest.raises(pymysql.err.ProgrammingError):
        async with mysql.mysql_repository(
            mysql_host=os.environ.get("MYSQL_HOST", "localhost"),
            mysql_database=os.environ.get("MYSQL_DATABASE", "pyncette"),
            mysql_user=os.environ.get("MYSQL_USER", "pyncette"),
            mysql_password=os.environ.get("MYSQL_PASSWORD", "password"),
            mysql_table_name=random_table_name(),
            mysql_skip_table_create=True,
        ) as repository:
            await repository.poll_task(
                datetime.datetime.now(tz=datetime.timezone.utc),
                DUMMY_TASK,
            )
