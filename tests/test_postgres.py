import datetime
import os

import asyncpg
import pytest

from pyncette import postgres
from pyncette.task import Task

from conftest import random_table_name

DUMMY_TASK = Task(name="foo", func=object(), schedule="* * * * *")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_invalid_table_name():
    with pytest.raises(ValueError, match="Table name"):
        await postgres.postgres_repository(
            postgres_url=os.environ.get("POSTGRES_URL", "postgres://postgres:postgres@localhost/pyncette"),
            postgres_table_name="spaces in table name",
        ).__aenter__()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_skip_table_create():
    with pytest.raises(asyncpg.exceptions.UndefinedTableError):
        async with postgres.postgres_repository(
            postgres_url=os.environ.get("POSTGRES_URL", "postgres://postgres:postgres@localhost/pyncette"),
            postgres_table_name=random_table_name(),
            postgres_skip_table_create=True,
        ) as repository:
            await repository.poll_task(
                datetime.datetime.now(tz=datetime.timezone.utc),
                DUMMY_TASK,
            )
