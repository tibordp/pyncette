import datetime
import os

import asyncpg
import pytest
from conftest import random_table_name

from pyncette import dynamodb
from pyncette.task import Task

DUMMY_TASK = Task(name="foo", func=object(), schedule="* * * * *")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_dynamodb_create():
    async with dynamodb.dynamodb_repository(
        dynamodb_table_name=random_table_name(),
        dynamodb_endpoint=os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:4566"),
        dynamodb_region_name="eu-west-1",
    ) as repository:

        assert "ACTIVE" == await repository._table.table_status
