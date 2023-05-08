import os

import pytest
from redis import asyncio as aioredis

from pyncette import redis


@pytest.mark.asyncio
@pytest.mark.integration
async def test_script_reload(monkeypatch):
    monkeypatch.setattr(redis, "read_text", lambda *args: 'return { "SUCCESS" }')

    redis_url = os.environ.get("REDIS_URL", "redis://localhost")
    redis_pool = aioredis.from_url(redis_url)

    lua_script = redis._LuaScript("dummy")
    result = await lua_script.register(redis_pool)
    await redis_pool.execute_command("SCRIPT", "FLUSH", "SYNC")

    result = await lua_script.execute(redis_pool, [], [])

    assert result == [b"SUCCESS"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_script_register(monkeypatch):
    monkeypatch.setattr(redis, "read_text", lambda *args: 'return { "SUCCESS" }')

    redis_url = os.environ.get("REDIS_URL", "redis://localhost")
    redis_pool = aioredis.from_url(redis_url)

    lua_script = redis._LuaScript("dummy")
    result = await lua_script.execute(redis_pool, [], [])

    assert result == [b"SUCCESS"]
