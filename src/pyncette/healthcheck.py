import asyncio
import logging
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable

from aiohttp import web

from pyncette.pyncette import Pyncette
from pyncette.pyncette import PyncetteContext
from pyncette.pyncette import _current_time

logger = logging.getLogger(__name__)


async def default_healthcheck(app_context: PyncetteContext) -> bool:
    utcnow = _current_time()
    last_tick = app_context.last_tick
    grace_period = app_context._app._poll_interval

    return last_tick is not None and (utcnow - last_tick < grace_period)


def use_healthcheck_server(
    app: Pyncette,
    port: int = 8080,
    bind_address: str = None,
    healthcheck_handler: Callable[
        [PyncetteContext], Awaitable[bool]
    ] = default_healthcheck,
) -> None:
    """
    Decorate Pyncette app with a healthcheck endpoint served as a HTTP endpoint.

    :param app: Pyncette app
    :param port: The local port to bind to
    :param bind_address: The local address to bind to
    :healthcheck_handler: A coroutine that determines health status
    """

    async def healthcheck_fixture(
        app_context: PyncetteContext,
    ) -> AsyncIterator[asyncio.AbstractServer]:
        async def handler(request: web.BaseRequest) -> web.Response:
            if request.method != "GET":
                return web.Response(status=405, text="Method not allowed")
            try:
                is_healthy = await healthcheck_handler(app_context)
            except Exception as e:
                logger.warning("Exception raised in healthcheck handler", e)
                is_healthy = False

            if is_healthy:
                return web.Response(status=200, text="OK")
            else:
                return web.Response(status=500, text="Not OK")

        loop = asyncio.get_event_loop()
        server = await loop.create_server(web.Server(handler), bind_address, port)
        logger.info(f"Healthcheck listening on {port}")

        try:
            yield server
        finally:
            server.close()
            await server.wait_closed()

    app.use_fixture("_healthcheck", healthcheck_fixture)
