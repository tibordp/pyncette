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

import asyncpg
from contextlib import asynccontextmanager

from pyncette.errors import PyncetteException
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

    def __init__(
        self, pool: asyncpg.pool.Pool, postgres_batch_size: int = 50, **kwargs: Any
    ):
        self._pool = pool
        self._batch_size = postgres_batch_size

    async def initialize(self):
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    DROP TABLE IF EXISTS tasks;
                    CREATE TABLE IF NOT EXISTS tasks (
                        name text PRIMARY KEY,
                        parent_name text,
                        locked_until timestamptz,
                        locked_by uuid,
                        execute_after timestamptz,
                        task_spec json
                    );
                    CREATE INDEX IF NOT EXISTS due_tasks ON tasks(parent_name, GREATEST(locked_until, execute_after));
                    """
                )

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                yield connection

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> QueryResponse:
        async with self.transaction() as connection:
            rows = await connection.fetch(
                """SELECT * FROM tasks 
                WHERE parent_name = $1 AND GREATEST(locked_until, execute_after) < $2 
                LIMIT $3
                FOR UPDATE SKIP LOCKED
                """,
                task.name,
                utc_now,
                self._batch_size,
            )
            logger.debug(f"query_task returned {rows}")

            new_locked_until = utc_now + task.lease_duration
            tasks = []
            for row in rows:
                task_spec = json.loads(row["task_spec"])
                concrete_task = task.instantiate(
                    name=task_spec["name"],
                    schedule=task_spec["schedule"],
                    interval=datetime.timedelta(seconds=task_spec["interval"])
                    if task_spec["interval"] is not None
                    else None,
                    timezone=task_spec["timezone"],
                    **task_spec["extra_args"],
                )
                locked_by = uuid.uuid4()
                await self._update_record(
                    connection,
                    concrete_task,
                    new_locked_until,
                    locked_by,
                    row["execute_after"],
                )
                tasks.append((concrete_task, locked_by))

            return QueryResponse(tasks=tasks, has_more=len(rows) == self._batch_size)

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self.transaction() as connection:
            result = await connection.execute(
                """INSERT INTO tasks 
                    (name, parent_name, task_spec, execute_after) 
                    VALUES ($1, $2, $3, $4) 
                    ON CONFLICT (name) DO UPDATE 
                        SET task_spec = $3, execute_after = $4""",
                task.name,
                task.parent_task.name,
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
                task.get_next_execution(utc_now, None),
            )
            logger.debug(f"register_task returned {result}")

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self.transaction() as connection:
            await connection.execute("DELETE FROM tasks WHERE name = $1", task.name)

    async def _update_record(
        self, connection, task, locked_until, locked_by, execute_after
    ):
        result = await connection.execute(
            """INSERT INTO tasks 
                (name, locked_until, locked_by, execute_after) 
                VALUES ($1, $2, $3, $4) 
                ON CONFLICT (name) DO UPDATE 
                    SET locked_until = $2, 
                    locked_by = $3, execute_after = $4""",
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
            row = await connection.fetchrow(
                "SELECT * FROM tasks WHERE name = $1 FOR UPDATE", task.name
            )
            logger.debug(f"poll_task returned {row}")

            update = False
            if row is None:
                execute_after = task.get_next_execution(utc_now, None)
                locked_until = None
                locked_by = None
                update = True
            else:
                execute_after = row["execute_after"]
                locked_until = row["locked_until"]
                locked_by = row["locked_by"]

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
            row = await connection.fetchrow(
                "SELECT * FROM tasks WHERE name = $1 FOR UPDATE", task.name
            )
            logger.debug(f"commit_task returned {row}")

            if not row:
                logger.warning(f"Task {task} not found, skipping.")
                return

            if row["locked_by"] != lease:
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            await self._update_record(
                connection,
                task,
                None,
                None,
                task.get_next_execution(utc_now, row["execute_after"]),
            )

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        async with self.transaction() as connection:
            result = await connection.execute(
                """UPDATE tasks 
                    SET locked_by = NULL, locked_until = NULL 
                    WHERE name = $1 AND locked_by = $2""",
                task.name,
                lease,
            )
            logger.debug(f"unlock_task returned {result}")


@contextlib.asynccontextmanager
async def postgres_repository(**kwargs: Any) -> AsyncIterator[PostgresRepository]:
    """Factory context manager for Redis repository that initializes the connection to Redis"""
    postgres_pool = await asyncpg.create_pool(kwargs["postgres_url"])
    try:
        repository = PostgresRepository(postgres_pool, **kwargs)
        await repository.initialize()

        yield repository
    finally:
        await postgres_pool.close()
