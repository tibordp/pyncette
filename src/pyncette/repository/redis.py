import contextlib
import datetime
import logging
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator
from typing import List
from typing import Optional
from typing import Tuple
from typing import cast

import aioredis

from pyncette.errors import PyncetteException
from pyncette.model import ExecutionMode
from pyncette.model import Lease
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


class LuaScript:
    _script: str
    _sha: Optional[str]

    def __init__(self, script: str):
        self._script = script
        self._sha = None

    async def register(self, client: aioredis.Redis):
        self._sha = await client.script_load(self._script)

    async def execute(
        self, client: aioredis.Redis, keys: List[Any] = [], args: List[Any] = [],
    ):
        if self._sha is None:
            await self.register(client)

        for _ in range(3):
            try:
                return await client.evalsha(self._sha, keys=keys, args=args)
            except aioredis.ReplyError as err:
                logger.warn("We seem to have lost the LUA script, reloading...")
                if str(err).startswith("NOSCRIPT"):
                    await self.register(client)
                else:
                    raise

        raise PyncetteException("Could not reload the Lua script.")


@dataclass
class _ScriptResponse:
    result: ResultType
    version: int
    execute_after: Optional[datetime.datetime]


class RedisRepository(Repository):
    _redis_client: aioredis.Redis
    _namespace: str
    _poll_script: LuaScript
    _commit_script: LuaScript

    POLL_LUA = """
    local mode, utc_now, incoming_version, incoming_execute_after, incoming_locked_until = unpack(ARGV)
    local version, execute_after, locked_until = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until'))
    if not version then
        version, execute_after = incoming_version, incoming_execute_after
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
    end

    local result
    if locked_until and utc_now < locked_until then
        result = "LOCKED"
    elseif execute_after < utc_now and version ~= incoming_version then
        result = "LEASE_MISMATCH"
    elseif execute_after < utc_now and mode == 'BEST_EFFORT' then
        version, execute_after, locked_until = version + 1, incoming_execute_after, nil
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('hdel', KEYS[1], 'locked_until')
        result = "READY"
    elseif execute_after < utc_now and mode == 'RELIABLE' then
        version, locked_until = version + 1, incoming_locked_until
        redis.call('hmset', KEYS[1], 'version', version, 'locked_until', incoming_locked_until)
        result = "READY"
    else
        result = "PENDING"
    end

    return { result, version, execute_after }
    """

    COMMIT_LUA = """
    local incoming_version, incoming_execute_after = unpack(ARGV)
    local version, execute_after, locked_until = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until'))

    local result
    if incoming_execute_after and version == incoming_version then
        version, execute_after, locked_until = version + 1, incoming_execute_after, nil
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('hdel', KEYS[1], 'locked_until')
        result = "READY"
    elseif not incoming_execute_after and version == incoming_version then
        version, locked_until = version + 1, nil
        redis.call('hset', KEYS[1], 'version', version)
        redis.call('hdel', KEYS[1], 'locked_until')
        result = "READY"
    else
        result = "LEASE_MISMATCH"
    end

    return { result, version, execute_after }
    """

    def __init__(self, redis_client: aioredis.Redis, **kwargs):
        self._redis_client = redis_client
        self._namespace = kwargs.get("redis_namespace", "")
        self._poll_script = LuaScript(self.POLL_LUA)
        self._commit_script = LuaScript(self.COMMIT_LUA)

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> Tuple[ResultType, Optional[Lease]]:
        # Nominally, we need at least two round-trips to Redis since the next execute_after is calculated
        # in Python code due to extra flexibility. This is why we have optimistic locking below to ensure that
        # the next execution time was calculated using a correct base if another process modified it in between.
        # In most cases, however, we can assume that the base time has not changed since the last invocation,
        # so by caching it, we can poll a task using a single round-trip (if we are wrong, the loop below will still
        # ensure corretness as the version will not match).
        last_lease = getattr(task, "_last_lease", None)
        if last_lease is not None:
            logger.debug("Using cached values for execute_after")
            version, execute_after = (
                last_lease.version,
                last_lease.execute_after,
            )
        else:
            version, execute_after = 0, None

        new_locked_until = utc_now + datetime.timedelta(seconds=10)
        for _ in range(5):
            next_execution = task.get_next_execution(utc_now, execute_after)
            response = await self._poll_record(
                task.execution_mode,
                task.name,
                utc_now,
                version,
                next_execution,
                new_locked_until,
            )
            task._last_lease = response  # type: ignore

            if response.result != ResultType.LEASE_MISMATCH:
                return (response.result, Lease(response))
            else:
                logger.debug(f"Lease mismatch, retrying.")
                execute_after = response.execute_after
                version = response.version

        raise PyncetteException(
            "Unable to acquire the lock on the task due to contention"
        )

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        poll_response = cast(_ScriptResponse, lease)
        response = await self._commit_record(
            task.name,
            poll_response.version,
            task.get_next_execution(utc_now, poll_response.execute_after),
        )
        task._last_lease = response  # type: ignore
        if response.result == ResultType.LEASE_MISMATCH:
            logger.info("Not commiting, as we have lost the lease")

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        poll_response = cast(_ScriptResponse, lease)
        response = await self._commit_record(task.name, poll_response.version, None)
        task._last_lease = response  # type: ignore
        if response.result == ResultType.LEASE_MISMATCH:
            logger.info("Not unlocking, as we have lost the lease")

    async def _poll_record(
        self,
        mode: ExecutionMode,
        task_name: str,
        utc_now: datetime.datetime,
        version: int,
        execute_after: datetime.datetime,
        locked_until: datetime.datetime,
    ) -> _ScriptResponse:
        response = await self._poll_script.execute(
            self._redis_client,
            keys=[f"pyncette:{self._namespace}:{task_name}"],
            args=[
                mode.name,
                utc_now.isoformat(),
                version,
                execute_after.isoformat(),
                locked_until.isoformat(),
            ],
        )
        logger.debug(f"poll_lua script returned {response}")
        return self._parse_response(response)

    async def _commit_record(
        self, task_name: str, version: int, execute_after: Optional[datetime.datetime],
    ) -> _ScriptResponse:
        response = await self._commit_script.execute(
            self._redis_client,
            keys=[f"pyncette:{self._namespace}:{task_name}"],
            args=[
                version,
                *([] if execute_after is None else [execute_after.isoformat()]),
            ],
        )
        logger.debug(f"commit_lua script returned {response}")
        return self._parse_response(response)

    async def register_scripts(self):
        await self._poll_script.register(self._redis_client)
        await self._commit_script.register(self._redis_client)

    def _parse_response(self, response: List[bytes]) -> _ScriptResponse:
        return _ScriptResponse(
            result=ResultType[response[0].decode()],
            version=int(response[1] or 0),
            execute_after=None
            if response[2] is None
            else datetime.datetime.fromisoformat(response[2].decode()),
        )


@contextlib.asynccontextmanager
async def redis_repository(**kwargs) -> AsyncGenerator[RedisRepository, None]:
    redis_pool = await aioredis.create_redis_pool(kwargs["redis_url"])
    try:
        repository = RedisRepository(redis_pool, **kwargs)
        await repository.register_scripts()
        yield repository
    finally:
        redis_pool.close()
        await redis_pool.wait_closed()
