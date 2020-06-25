from __future__ import annotations

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
class _ManageScriptResponse:
    result: ResultType
    version: int
    execute_after: Optional[datetime.datetime]
    locked_until: Optional[datetime.datetime]
    task_spec: Optional[Dict[str, Any]]
    locked_by: Optional[str]

    @classmethod
    def from_response(cls, response: List[bytes]) -> _ManageScriptResponse:
        return cls(
            result=ResultType[response[0].decode()],
            version=int(response[1] or 0),
            execute_after=None
            if response[2] is None
            else datetime.datetime.fromisoformat(response[2].decode()),
            locked_until=None
            if response[3] is None
            else datetime.datetime.fromisoformat(response[3].decode()),
            locked_by=None if response[4] is None else response[4].decode(),
            task_spec=None if response[5] is None else json.loads(response[5]),
        )


def _create_dynamic_task(task: Task, response_data: List[bytes]) -> Tuple[Task, Lease]:
    task_data = _ManageScriptResponse.from_response(response_data)
    assert task_data.task_spec is not None

    return (task.instantiate_from_spec(task_data.task_spec), Lease(task_data))


class RedisRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _redis_client: aioredis.Redis
    _namespace: str
    _manage_script: _LuaScript
    _poll_dynamic_script: _LuaScript

    def __init__(self, redis_client: aioredis.Redis, **kwargs: Any):
        self._redis_client = redis_client
        self._namespace = kwargs.get("redis_namespace", "")
        self._batch_size = kwargs.get("batch_size", 100)
        self._poll_dynamic_script = _LuaScript("poll_dynamic.lua")
        self._manage_script = _LuaScript("manage.lua")

    async def register_scripts(self) -> None:
        """Registers the Lua scripts used by the implementation ahead of time"""
        await self._poll_dynamic_script.register(self._redis_client)
        await self._manage_script.register(self._redis_client)

    async def poll_dynamic_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> QueryResponse:
        new_locked_until = utc_now + task.lease_duration
        response = await self._poll_dynamic_script.execute(
            self._redis_client,
            keys=[self._get_task_index_key(task)],
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
                _create_dynamic_task(task, response_data)
                for response_data in response[1:]
            ],
            has_more=response[0] == b"HAS_MORE",
        )

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        execute_after = task.get_next_execution(utc_now, None)
        assert execute_after is not None

        await self._manage_record(
            task, "REGISTER", execute_after.isoformat(), json.dumps(task.as_spec()),
        )

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        await self._manage_record(task, "UNREGISTER")

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
        if isinstance(lease, _ManageScriptResponse):
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
            version, execute_after, locked_by = (
                0,
                None,
                str(uuid.uuid4()),
            )

        new_locked_until = utc_now + task.lease_duration
        for _ in range(5):
            next_execution = task.get_next_execution(utc_now, execute_after)
            response = await self._manage_record(
                task,
                "POLL",
                task.execution_mode.name,
                "REGULAR" if task.parent_task is None else "DYNAMIC",
                utc_now.isoformat(),
                version,
                next_execution.isoformat() if next_execution is not None else "",
                new_locked_until.isoformat(),
                locked_by,
            )
            task._last_lease = response  # type: ignore

            if response.result == ResultType.LEASE_MISMATCH:
                logger.debug("Lease mismatch, retrying.")
                execute_after = response.execute_after
                version = response.version
            elif response.result == ResultType.MISSING:
                raise PyncetteException("Task not found")
            else:
                return PollResponse(
                    result=response.result,
                    scheduled_at=execute_after,
                    lease=Lease(response),
                )

        raise PyncetteException(
            "Unable to acquire the lock on the task due to contention"
        )

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        assert isinstance(lease, _ManageScriptResponse)
        next_execution = task.get_next_execution(utc_now, lease.execute_after)
        response = await self._manage_record(
            task,
            "COMMIT",
            lease.version,
            lease.locked_by,
            next_execution.isoformat() if next_execution is not None else "",
        )
        task._last_lease = response  # type: ignore
        if response.result == ResultType.LEASE_MISMATCH:
            logger.info("Not commiting, as we have lost the lease")

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        assert isinstance(lease, _ManageScriptResponse)
        response = await self._manage_record(
            task, "UNLOCK", lease.version, lease.locked_by
        )
        task._last_lease = response  # type: ignore
        if response.result == ResultType.LEASE_MISMATCH:
            logger.info("Not unlocking, as we have lost the lease")

    async def _manage_record(self, task: Task, *args: Any) -> _ManageScriptResponse:
        response = await self._manage_script.execute(
            self._redis_client,
            keys=[
                self._get_task_record_key(task),
                self._get_task_index_key(task.parent_task),
            ],
            args=list(args),
        )
        logger.debug(f"manage_lua script returned {response}")
        return _ManageScriptResponse.from_response(response)

    def _get_task_record_key(self, task: Task) -> str:
        return f"pyncette:{self._namespace}:task:{task.canonical_name}"

    def _get_task_index_key(self, task: Optional[Task]) -> str:
        # A prefix-coded index key, so there are no restrictions on task names.
        index_name = f"index:{task.canonical_name}" if task else "index"
        return f"pyncette:{self._namespace}:{index_name}"


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
