import pytest

from pyncette import sqlite


@pytest.mark.asyncio
async def test_invalid_table_name():
    with pytest.raises(ValueError):
        await sqlite.sqlite_repository(
            sqlite_table_name="spaces in table name",
        ).__aenter__()
