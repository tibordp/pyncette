import asyncio

import aiohttp
import pytest
from conftest import wrap_factory

from pyncette import Pyncette
from pyncette.healthcheck import use_healthcheck_server
from pyncette.sqlite import sqlite_repository


def get_healthcheck_port(app_context):
    return app_context._root_context._healthcheck.sockets[0].getsockname()[1]


@pytest.mark.asyncio
async def test_healthcheck_server_success(timemachine):
    app = Pyncette(repository_factory=wrap_factory(sqlite_repository, timemachine))

    async def healthcheck_handler(app_context):
        return True

    # Bind on random port to avoid conflict
    use_healthcheck_server(
        app, port=0, bind_address="127.0.0.1", healthcheck_handler=healthcheck_handler
    )

    async with app.create() as ctx, aiohttp.ClientSession() as session:
        task = asyncio.create_task(ctx.run())
        async with session.get(
            f"http://127.0.0.1:{get_healthcheck_port(ctx)}/health"
        ) as resp:
            assert resp.status == 200
        ctx.shutdown()
        await task
        await timemachine.unwind()


@pytest.mark.asyncio
async def test_healthcheck_server_failure(timemachine):
    app = Pyncette(repository_factory=wrap_factory(sqlite_repository, timemachine))

    async def healthcheck_handler(app_context):
        return False

    # Bind on random port to avoid conflict
    use_healthcheck_server(
        app, port=0, bind_address="127.0.0.1", healthcheck_handler=healthcheck_handler
    )

    async with app.create() as ctx, aiohttp.ClientSession() as session:
        task = asyncio.create_task(ctx.run())
        async with session.get(
            f"http://127.0.0.1:{get_healthcheck_port(ctx)}/health"
        ) as resp:
            assert resp.status == 500
        ctx.shutdown()
        await task
        await timemachine.unwind()


@pytest.mark.asyncio
async def test_healthcheck_server_exception(timemachine):
    app = Pyncette(repository_factory=wrap_factory(sqlite_repository, timemachine))

    async def healthcheck_handler(app_context):
        raise Exception("oops")

    # Bind on random port to avoid conflict
    use_healthcheck_server(
        app, port=0, bind_address="127.0.0.1", healthcheck_handler=healthcheck_handler
    )

    async with app.create() as ctx, aiohttp.ClientSession() as session:
        task = asyncio.create_task(ctx.run())
        async with session.get(
            f"http://127.0.0.1:{get_healthcheck_port(ctx)}/health"
        ) as resp:
            assert resp.status == 500
        ctx.shutdown()
        await task
        await timemachine.unwind()
