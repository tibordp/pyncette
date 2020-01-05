import asyncio
import datetime
from unittest.mock import MagicMock

import pytest


from uuid import uuid4
from pyncette import Context
from pyncette import ExecutionMode
from pyncette import Pyncette
from pyncette.repository.redis import redis_repository

@pytest.mark.asyncio
@pytest.mark.integration
async def test_successful_task_interval(monkeypatch):
    app = Pyncette(redis_url="redis://localhost", repository_factory=redis_repository)

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=5), name=str(uuid4()))
    async def successful_task(context: Context) -> None:
        counter()

    task = asyncio.create_task(app.run())
    await asyncio.sleep(10.5)
    app.shutdown()
    await task

    assert counter.call_count == 2

@pytest.mark.asyncio
@pytest.mark.integration
async def test_failing_task_interval(monkeypatch):
    app = Pyncette(redis_url="redis://localhost", repository_factory=redis_repository)

    counter = MagicMock()

    @app.task(interval=datetime.timedelta(seconds=5), name=str(uuid4()))
    async def successful_task(context: Context) -> None:
        counter()
        raise RuntimeError("Oops")

    task = asyncio.create_task(app.run())
    await asyncio.sleep(10.5)
    app.shutdown()
    await task

    assert counter.call_count == 6