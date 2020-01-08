import contextlib
import datetime
import json
import logging
from dataclasses import dataclass
from typing import Any
from typing import AsyncIterator
from typing import Dict
from typing import List
from typing import Optional
from typing import cast

import aioredis

from .errors import PyncetteException
from .model import ExecutionMode
from .model import Lease
from .model import PollResponse
from .model import QueryResponse
from .model import ResultType
from .repository import Repository
from .task import Task

logger = logging.getLogger(__name__)


class _LuaScript:
    """A wrapper for Redis lua scripts that automaticaly reloads it if e.g. SCRIPT FLUSH is invoked"""

    _script: str
    _sha: Optional[str]

    def __init__(self, script: str):
        self._script = script
        self._sha = None

    async def register(self, client: aioredis.Redis) -> None:
        self._sha = await client.script_load(self._script)

    async def execute(
        self, client: aioredis.Redis, keys: List[Any] = [], args: List[Any] = [],
    ) -> Any:
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
    locked_until: Optional[datetime.datetime]
    task_spec: Optional[Dict[str, Any]]


class RedisRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _redis_client: aioredis.Redis
    _namespace: str
    _poll_script: _LuaScript
    _commit_script: _LuaScript
    _query_script: _LuaScript

    _QUERY_LUA = """
    local utc_now, limit, incoming_locked_until = unpack(ARGV)
    limit = tonumber(limit)

    local tasksets = redis.call('zrangebylex', KEYS[1], '-', '(' .. utc_now .. '`', 'LIMIT', 0, limit + 1)
    local results = { "READY" }

    local function getTasksetKey(key, a1, a2)
      if not a1 or a1 < a2 then
        return a2 .. '_' .. key
      else
        return a1 .. '_' .. key
      end
    end

    for key,value in pairs(tasksets) do
        local task_name = value:gmatch('_(.*)')()
        local version, execute_after, locked_until, task_spec = unpack(redis.call('hmget', task_name, 'version', 'execute_after', 'locked_until', 'task_spec'))

        redis.call('zrem', KEYS[1], getTasksetKey(task_name, locked_until, execute_after))
        version, locked_until = version + 1, incoming_locked_until
        redis.call('hmset', task_name, 'version', version, 'locked_until', incoming_locked_until)
        redis.call('zadd', KEYS[1], 0, getTasksetKey(task_name, locked_until, execute_after))

        results[key + 1] = { "READY", version, execute_after, locked_until, task_spec }
        if key == limit then
            results[1] = "HAS_MORE"
            break
        end
    end

    return results
    """

    _UNREGISTER_LUA = """
    local version, execute_after, locked_until, task_spec = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'task_spec'))
    local function getTasksetKey(key, a1, a2)
      if not a1 or a1 < a2 then
        return a2 .. '_' .. key
      else
        return a1 .. '_' .. key
      end
    end

    local result
    if execute_after then
        redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
        redis.call('del', KEYS[1])
        result = "READY"
    else
        result = "MISSING"
    end

    return { result, version, execute_after, locked_until, task_spec }
    """

    _POLL_LUA = """
    local mode, utc_now, incoming_version, incoming_execute_after, incoming_locked_until = unpack(ARGV)
    local version, execute_after, locked_until, task_spec = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'task_spec'))

    local function getTasksetKey(key, a1, a2)
      if not a1 or a1 < a2 then
        return a2 .. '_' .. key
      else
        return a1 .. '_' .. key
      end
    end

    if not version then
        version, execute_after = incoming_version, incoming_execute_after
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
    end

    local result
    if locked_until and utc_now < locked_until and version == incoming_version then
        result = "READY"
    elseif locked_until and utc_now < locked_until then
        result = "LOCKED"
    elseif execute_after < utc_now and version ~= incoming_version then
        result = "LEASE_MISMATCH"
    elseif execute_after < utc_now and mode == 'AT_MOST_ONCE' then
        redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
        version, execute_after, locked_until = version + 1, incoming_execute_after, nil
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('hdel', KEYS[1], 'locked_until')
        redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
        result = "READY"
    elseif execute_after < utc_now and mode == 'AT_LEAST_ONCE' then
        redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
        version, locked_until = version + 1, incoming_locked_until
        redis.call('hmset', KEYS[1], 'version', version, 'locked_until', incoming_locked_until)
        redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
        result = "READY"
    else
        result = "PENDING"
    end

    return { result, version, execute_after, locked_until, task_spec }
    """

    _COMMIT_LUA = """
    local incoming_version, incoming_execute_after = unpack(ARGV)
    local version, execute_after, locked_until, task_spec = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'task_spec'))
    local taskset_suffix = '_' .. KEYS[1]

    local function getTasksetKey(key, a1, a2)
      if not a1 or a1 < a2 then
        return a2 .. '_' .. key
      else
        return a1 .. '_' .. key
      end
    end

    local result
    if incoming_execute_after and version == incoming_version then
        redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
        version, execute_after, locked_until = version + 1, incoming_execute_after, nil
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('hdel', KEYS[1], 'locked_until')
        redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
        result = "READY"
    elseif not incoming_execute_after and version == incoming_version then
        redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
        version, locked_until = version + 1, nil
        redis.call('hset', KEYS[1], 'version', version)
        redis.call('hdel', KEYS[1], 'locked_until')
        redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
        result = "READY"
    else
        result = "LEASE_MISMATCH"
    end

    return { result, version, execute_after, locked_until, task_spec }
    """

    def __init__(self, redis_client: aioredis.Redis, **kwargs: Any):
        self._redis_client = redis_client
        self._namespace = kwargs.get("redis_namespace", "")
        self._batch_size = kwargs.get("redis_batch_size", 100)
        self._poll_script = _LuaScript(self._POLL_LUA)
        self._commit_script = _LuaScript(self._COMMIT_LUA)
        self._query_script = _LuaScript(self._QUERY_LUA)
        self._unregister_script = _LuaScript(self._UNREGISTER_LUA)

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> QueryResponse:
        new_locked_until = utc_now + task.lease_duration
        response = await self._query_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:taskset:{task.name if task else '__global__'}"
            ],
            args=[utc_now.isoformat(), self._batch_size, new_locked_until.isoformat(),],
        )
        logger.debug(f"query_lua script returned [{self._batch_size}] {response}")

        return QueryResponse(
            tasks=[
                self._create_dynamic_task(task, response_data)
                for response_data in response[1:]
            ],
            has_more=response[0] == b"HAS_MORE",
        )

    def _create_dynamic_task(self, task: Task, response_data: List[bytes]) -> Task:
        task_data = self._parse_response(response_data)
        task_spec = task_data.task_spec
        assert task_spec is not None

        task = task.instantiate(
            name=task_spec["name"],
            schedule=task_spec["schedule"],
            interval=datetime.timedelta(seconds=task_spec["interval"])
            if task_spec["interval"] is not None
            else None,
            **task_spec["extra_args"],
            timezone=task_spec["timezone"],
        )

        task._last_lease = task_data  # type: ignore
        return task

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Registers a dynamic task"""
        await self._redis_client.hset(
            f"pyncette:{self._namespace}:{task.name}",
            "task_spec",
            json.dumps(
                {
                    "name": task.name,
                    "schedule": task.schedule,
                    "interval": task.interval.total_seconds()
                    if task.interval is not None
                    else None,
                    "timezone": task.timezone,
                    "extra_args": task.extra_args,
                }
            ),
        )
        await self.poll_task(utc_now, task)

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        # FIXME: There is a potential race condition if we unregister the task while it is executing, it could be added
        # back to the taskset set by the commit method.
        response = await self._unregister_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:{task.name}",
                f"pyncette:{self._namespace}:taskset:{task.parent_task.name if task.parent_task else '__global__'}",
            ],
            args=[utc_now.isoformat(),],
        )
        logger.debug(f"unregister_lua script returned {response}")

    async def poll_task(self, utc_now: datetime.datetime, task: Task) -> PollResponse:
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
            # By default we assume that the task is brand new
            version, execute_after = 0, None

        new_locked_until = utc_now + task.lease_duration
        for _ in range(5):
            next_execution = task.get_next_execution(utc_now, execute_after)
            response = await self._poll_record(
                task.execution_mode,
                task,
                utc_now,
                version,
                next_execution,
                new_locked_until,
            )
            task._last_lease = response  # type: ignore

            if response.result != ResultType.LEASE_MISMATCH:
                return PollResponse(
                    result=response.result,
                    scheduled_at=execute_after,
                    lease=Lease(response),
                )
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
            task,
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
        response = await self._commit_record(task, poll_response.version, None)
        task._last_lease = response  # type: ignore
        if response.result == ResultType.LEASE_MISMATCH:
            logger.info("Not unlocking, as we have lost the lease")

    async def _poll_record(
        self,
        mode: ExecutionMode,
        task: Task,
        utc_now: datetime.datetime,
        version: int,
        execute_after: datetime.datetime,
        locked_until: datetime.datetime,
    ) -> _ScriptResponse:
        response = await self._poll_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:{task.name}",
                f"pyncette:{self._namespace}:taskset:{task.parent_task.name if task.parent_task else '__global__'}",
            ],
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
        self, task: Task, version: int, execute_after: Optional[datetime.datetime],
    ) -> _ScriptResponse:
        response = await self._commit_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:{task.name}",
                f"pyncette:{self._namespace}:taskset:{task.parent_task.name if task.parent_task else '__global__'}",
            ],
            args=[
                version,
                *([] if execute_after is None else [execute_after.isoformat()]),
            ],
        )
        logger.debug(f"commit_lua script returned {response}")
        return self._parse_response(response)

    async def register_scripts(self) -> None:
        """Registers the Lua scripts used by the implementation ahead of time"""
        await self._poll_script.register(self._redis_client)
        await self._commit_script.register(self._redis_client)
        await self._query_script.register(self._redis_client)
        await self._unregister_script.register(self._redis_client)

    def _parse_response(self, response: List[bytes]) -> _ScriptResponse:
        return _ScriptResponse(
            result=ResultType[response[0].decode()],
            version=int(response[1] or 0),
            execute_after=None
            if len(response) <= 2 or response[2] is None
            else datetime.datetime.fromisoformat(response[2].decode()),
            locked_until=None
            if len(response) <= 3 or response[3] is None
            else datetime.datetime.fromisoformat(response[3].decode()),
            task_spec=None
            if len(response) <= 4 or response[4] is None
            else json.loads(response[4]),
        )


@contextlib.asynccontextmanager
async def redis_repository(**kwargs: Any) -> AsyncIterator[RedisRepository]:
    """Factory context manager for Redis repository that initializes the connection to Redis"""
    redis_pool = await aioredis.create_redis_pool(kwargs["redis_url"])
    try:
        repository = RedisRepository(redis_pool, **kwargs)
        await repository.register_scripts()
        yield repository
    finally:
        redis_pool.close()
        await redis_pool.wait_closed()
