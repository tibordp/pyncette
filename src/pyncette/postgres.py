import contextlib
import datetime
import json
import logging
import re
import uuid
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Optional

import asyncpg

from pyncette.model import ExecutionMode
from pyncette.model import Lease
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


class PostgresRepository(Repository):
    _pool: asyncpg.pool.Pool
    _batch_size: int
    _table_name: str

    def __init__(
        self, pool: asyncpg.pool.Pool, **kwargs: Any,
    ):
        self._pool = pool
        self._table_name = kwargs.get("postgres_table_name", "pyncette_tasks")
        self._batch_size = kwargs.get("batch_size", 100)

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")
        if not re.match(r"^[a-z_]+$", self._table_name):
            raise ValueError(
                "Table name can only contain lower-case letters and underscores"
            )

    async def initialize(self) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._table_name} (
                        name text PRIMARY KEY,
                        parent_name text,
                        locked_until timestamptz,
                        locked_by uuid,
                        execute_after timestamptz,
                        task_spec json
                    );
                    CREATE INDEX IF NOT EXISTS due_tasks_{self._table_name}
                    ON {self._table_name} (parent_name, GREATEST(locked_until, execute_after));
                    """
                )

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                yield connection

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> QueryResponse:
        async with self.transaction() as connection:
            locked_by = uuid.uuid4()
            locked_until = utc_now + task.lease_duration

            ready_tasks = await connection.fetch(
                f"""SELECT * FROM {self._table_name}
                WHERE parent_name = $1 AND GREATEST(locked_until, execute_after) < $2
                LIMIT $3
                FOR UPDATE SKIP LOCKED
                """,
                task.name,
                utc_now,
                self._batch_size,
            )
            logger.debug(f"query_task returned {ready_tasks}")
            concrete_tasks = [
                task.instantiate_from_spec(json.loads(task_data["task_spec"]))
                for task_data in ready_tasks
            ]
            await connection.executemany(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = $2,
                    locked_by = $3
                WHERE name = $1
                """,
                [
                    (concrete_task.name, locked_until, locked_by)
                    for concrete_task in concrete_tasks
                ],
            )
            return QueryResponse(
                tasks=[
                    (concrete_task, Lease(locked_by))
                    for concrete_task in concrete_tasks
                ],
                # May result in an extra round-trip if there were exactly
                # batch_size tasks available, but we deem this an acceptable
                # tradeoff.
                has_more=len(concrete_tasks) == self._batch_size,
            )

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        assert task.parent_task is not None

        async with self.transaction() as connection:
            result = await connection.execute(
                f"""
                INSERT INTO {self._table_name} (name, parent_name, task_spec, execute_after)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (name) DO UPDATE
                SET
                    task_spec = $3,
                    execute_after = $4
                """,
                task.name,
                task.parent_task.name,
                json.dumps(task.as_spec()),
                task.get_next_execution(utc_now, None),
            )
            logger.debug(f"register_task returned {result}")

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self.transaction() as connection:
            await connection.execute(
                f"DELETE FROM {self._table_name} WHERE name = $1", task.name
            )

    async def _update_record(
        self,
        connection: asyncpg.Connection,
        task: Task,
        locked_until: Optional[datetime.datetime],
        locked_by: Optional[uuid.UUID],
        execute_after: Optional[datetime.datetime],
    ) -> None:
        result = await connection.execute(
            f"""
            INSERT INTO {self._table_name} (name, locked_until, locked_by, execute_after)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO UPDATE
            SET
                locked_until = $2,
                locked_by = $3,
                execute_after = $4
            """,
            task.name,
            locked_until,
            locked_by,
            execute_after,
        )
        logger.debug(f"update_record returned {result}")

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None
    ) -> PollResponse:
        async with self.transaction() as connection:
            task_data = await connection.fetchrow(
                f"SELECT * FROM {self._table_name} WHERE name = $1 FOR UPDATE",
                task.name,
            )
            logger.debug(f"poll_task returned {task_data}")

            update = False
            if task_data is None:
                execute_after = task.get_next_execution(utc_now, None)
                locked_until = None
                locked_by = None
                update = True
            else:
                execute_after = task_data["execute_after"]
                locked_until = task_data["locked_until"]
                locked_by = task_data["locked_by"]

            # We need the original scheduled date for later
            scheduled_at = execute_after

            if (
                locked_until is not None
                and locked_until > utc_now
                and (lease != locked_by)
            ):
                result = ResultType.LOCKED
            elif (
                execute_after <= utc_now
                and task.execution_mode == ExecutionMode.AT_MOST_ONCE
            ):
                execute_after = task.get_next_execution(utc_now, execute_after)
                result = ResultType.READY
                update = True
            elif (
                execute_after <= utc_now
                and task.execution_mode == ExecutionMode.AT_LEAST_ONCE
            ):
                locked_until = utc_now + task.lease_duration
                locked_by = uuid.uuid4()
                result = ResultType.READY
                update = True
            else:
                result = ResultType.PENDING

            if update:
                await self._update_record(
                    connection, task, locked_until, locked_by, execute_after,
                )

            return PollResponse(
                result=result, scheduled_at=scheduled_at, lease=locked_by
            )

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        async with self.transaction() as connection:
            task_data = await connection.fetchrow(
                f"SELECT * FROM {self._table_name} WHERE name = $1 FOR UPDATE",
                task.name,
            )
            logger.debug(f"commit_task returned {task_data}")

            if not task_data:
                logger.warning(f"Task {task} not found, skipping.")
                return

            if task_data["locked_by"] != lease:
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            await self._update_record(
                connection,
                task,
                None,
                None,
                task.get_next_execution(utc_now, task_data["execute_after"]),
            )

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        async with self.transaction() as connection:
            result = await connection.execute(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_by = NULL,
                    locked_until = NULL
                WHERE name = $1 AND locked_by = $2
                """,
                task.name,
                lease,
            )
            logger.debug(f"unlock_task returned {result}")


@contextlib.asynccontextmanager
async def postgres_repository(**kwargs: Any) -> AsyncIterator[PostgresRepository]:
    """Factory context manager for Redis repository that initializes the connection to Postgres"""
    postgres_pool = await asyncpg.create_pool(kwargs["postgres_url"])
    try:
        repository = PostgresRepository(postgres_pool, **kwargs)
        await repository.initialize()

        yield repository
    finally:
        await postgres_pool.close()
