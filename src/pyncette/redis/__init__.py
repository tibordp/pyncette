import contextlib
import datetime
import json
import logging
import uuid
from dataclasses import dataclass
from importlib.resources import read_text
from typing import Any
from typing import AsyncIterator
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import aioredis

from pyncette.errors import PyncetteException
from pyncette.model import ExecutionMode
from pyncette.model import Lease
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


class _LuaScript:
    """A wrapper for Redis lua scripts that automaticaly reloads it if e.g. SCRIPT FLUSH is invoked"""

    _script: str
    _sha: Optional[str]

    def __init__(self, script_path: str):
        self._script = read_text(__name__, script_path)
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
                if str(err).startswith("NOSCRIPT"):
                    logger.warn("We seem to have lost the LUA script, reloading...")
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
    locked_by: Optional[str]


class RedisRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _redis_client: aioredis.Redis
    _namespace: str
    _poll_script: _LuaScript
    _commit_script: _LuaScript
    _query_script: _LuaScript
    _unregister_script: _LuaScript

    def __init__(self, redis_client: aioredis.Redis, **kwargs: Any):
        self._redis_client = redis_client
        self._namespace = kwargs.get("redis_namespace", "")
        self._batch_size = kwargs.get("redis_batch_size", 100)
        self._poll_script = _LuaScript("poll.lua")
        self._commit_script = _LuaScript("commit.lua")
        self._query_script = _LuaScript("query.lua")
        self._unregister_script = _LuaScript("unregister.lua")

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> QueryResponse:
        new_locked_until = utc_now + task.lease_duration
        response = await self._query_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:taskset:{task.name if task else '__global__'}"
            ],
            args=[
                utc_now.isoformat(),
                self._batch_size,
                new_locked_until.isoformat(),
                str(uuid.uuid4()),
            ],
        )
        logger.debug(f"query_lua script returned [{self._batch_size}] {response}")

        return QueryResponse(
            tasks=[
                self._create_dynamic_task(task, response_data)
                for response_data in response[1:]
            ],
            has_more=response[0] == b"HAS_MORE",
        )

    def _create_dynamic_task(
        self, task: Task, response_data: List[bytes]
    ) -> Tuple[Task, Lease]:
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
        return (task, Lease(task_data))

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
        response = await self._unregister_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:{task.name}",
                f"pyncette:{self._namespace}:taskset:{task.parent_task.name if task.parent_task else '__global__'}",
            ],
            args=[utc_now.isoformat(),],
        )
        logger.debug(f"unregister_lua script returned {response}")

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None
    ) -> PollResponse:
        # Nominally, we need at least two round-trips to Redis since the next execute_after is calculated
        # in Python code due to extra flexibility. This is why we have optimistic locking below to ensure that
        # the next execution time was calculated using a correct base if another process modified it in between.
        # In most cases, however, we can assume that the base time has not changed since the last invocation,
        # so by caching it, we can poll a task using a single round-trip (if we are wrong, the loop below will still
        # ensure corretness as the version will not match).
        last_lease = getattr(task, "_last_lease", None)
        if isinstance(lease, _ScriptResponse):
            version, execute_after, locked_by = (
                lease.version,
                lease.execute_after,
                lease.locked_by,
            )
        elif last_lease is not None:
            logger.debug("Using cached values for execute_after")
            version, execute_after, locked_by = (
                last_lease.version,
                last_lease.execute_after,
                str(uuid.uuid4()),
            )
        else:
            # By default we assume that the task is brand new
            version, execute_after, locked_by = 0, None, str(uuid.uuid4())

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
                locked_by,
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
        assert isinstance(lease, _ScriptResponse)
        assert lease.locked_by is not None

        response = await self._commit_record(
            task,
            lease.version,
            task.get_next_execution(utc_now, lease.execute_after),
            lease.locked_by,
        )
        task._last_lease = response  # type: ignore
        if response.result == ResultType.LEASE_MISMATCH:
            logger.info("Not commiting, as we have lost the lease")

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        assert isinstance(lease, _ScriptResponse)
        assert lease.locked_by is not None

        response = await self._commit_record(task, lease.version, None, lease.locked_by)
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
        locked_by: Optional[str],
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
                locked_by,
            ],
        )
        logger.debug(f"poll_lua script returned {response}")
        return self._parse_response(response)

    async def _commit_record(
        self,
        task: Task,
        version: int,
        execute_after: Optional[datetime.datetime],
        locked_by: str,
    ) -> _ScriptResponse:
        response = await self._commit_script.execute(
            self._redis_client,
            keys=[
                f"pyncette:{self._namespace}:{task.name}",
                f"pyncette:{self._namespace}:taskset:{task.parent_task.name if task.parent_task else '__global__'}",
            ],
            args=[
                version,
                locked_by,
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
            locked_by=None
            if len(response) <= 4 or response[4] is None
            else response[4].decode(),
            task_spec=None
            if len(response) <= 5 or response[5] is None
            else json.loads(response[5]),
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
