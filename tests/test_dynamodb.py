import os

import pytest
from botocore.exceptions import ClientError

from pyncette import dynamodb

from conftest import random_table_name

DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:4566")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_dynamodb_create():
    async with dynamodb.dynamodb_repository(
        dynamodb_table_name=random_table_name(),
        dynamodb_endpoint=DYNAMODB_ENDPOINT,
        dynamodb_region_name="eu-west-1",
    ) as repository:
        table_status = await repository._table.table_status
        assert table_status == "ACTIVE"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_dynamodb_skip_table_create():
    async with dynamodb.dynamodb_repository(
        dynamodb_table_name=random_table_name(),
        dynamodb_endpoint=DYNAMODB_ENDPOINT,
        dynamodb_region_name="eu-west-1",
        dynamodb_skip_table_create=True,
    ) as repository:
        with pytest.raises(ClientError) as e:
            await repository._table.table_status

        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
