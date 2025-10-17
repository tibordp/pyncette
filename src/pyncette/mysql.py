import asyncio
import contextlib
import datetime
import json
import logging
import re
import uuid
from contextlib import asynccontextmanager
from typing import Any
from collections.abc import AsyncIterator
from typing import Optional

import aiomysql
import dateutil.tz
import pymysql

from pyncette.errors import PyncetteException
from pyncette.model import ContinuationToken
from pyncette.model import ExecutionMode
from pyncette.model import Lease
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


def _from_timestamp(timestamp: Optional[float]) -> Optional[datetime.datetime]:
    if timestamp is None:
        return None
    else:
        return datetime.datetime.fromtimestamp(timestamp, dateutil.tz.UTC)


def _to_timestamp(date: Optional[datetime.datetime]) -> Optional[float]:
    if date is None:
        return None
    else:
        return date.timestamp()


_CONTINUATION_TOKEN = ContinuationToken(object())


class MySQLRepository(Repository):
    _pool: aiomysql.Pool
    _batch_size: int
    _table_name: str

    def __init__(
        self,
        pool: aiomysql.Pool,
        **kwargs: Any,
    ):
        self._pool = pool
        self._table_name = kwargs.get("mysql_table_name", "pyncette_tasks")
        self._batch_size = kwargs.get("batch_size", 100)

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")
        if not re.match(r"^[a-z_]+$", self._table_name):
            raise ValueError("Table name can only contain lower-case letters and underscores")

    async def initialize(self) -> None:
        async with self._transaction() as cursor:
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table_name} (
                    name VARCHAR(256) PRIMARY KEY,
                    parent_name VARCHAR(256),
                    locked_until DOUBLE,
                    locked_by VARCHAR(256),
                    execute_after DOUBLE,
                    task_spec TEXT
                );
                """
            )

            try:
                await cursor.execute(
                    f"""
                    CREATE INDEX due_tasks_{self._table_name}
                    ON {self._table_name} (parent_name, (GREATEST(COALESCE(locked_until, 0), COALESCE(execute_after, 0))));
                    """
                )
            except pymysql.err.OperationalError as e:
                code, _msg = e.args
                # Index already exists
                if code != 1061:
                    raise

    async def poll_dynamic_task(
        self,
        utc_now: datetime.datetime,
        task: Task,
        continuation_token: Optional[ContinuationToken] = None,
    ) -> QueryResponse:
        async with self._transaction() as cursor:
            locked_by = str(uuid.uuid4())
            locked_until = utc_now + task.lease_duration

            await cursor.execute(
                f"""
                SELECT name, task_spec FROM {self._table_name}
                WHERE parent_name = %s AND GREATEST(COALESCE(locked_until, 0), COALESCE(execute_after, 0)) <= %s
                ORDER BY GREATEST(COALESCE(locked_until, 0), COALESCE(execute_after, 0)) ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    task.canonical_name,
                    _to_timestamp(utc_now),
                    self._batch_size,
                ),
            )
            ready_tasks = await cursor.fetchall()

            await cursor.executemany(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = %s,
                    locked_by = %s
                WHERE name = %s
                """,
                [(_to_timestamp(locked_until), locked_by, record["name"]) for record in ready_tasks],
            )

            logger.debug(f"poll_dynamic_task returned {ready_tasks}")

            return QueryResponse(
                tasks=[
                    (
                        task.instantiate_from_spec(json.loads(record["task_spec"])),
                        Lease(locked_by),
                    )
                    for record in ready_tasks
                ],
                # May result in an extra round-trip if there were exactly
                # batch_size tasks available, but we deem this an acceptable
                # tradeoff.
                continuation_token=_CONTINUATION_TOKEN if len(ready_tasks) == self._batch_size else None,
            )

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        assert task.parent_task is not None

        async with self._transaction() as cursor:
            execute_at = _to_timestamp(task.get_next_execution(utc_now, None))
            task_spec = json.dumps(task.as_spec())

            await cursor.execute(
                f"""
                INSERT INTO {self._table_name} (name, parent_name, task_spec, execute_after)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    task_spec = %s,
                    execute_after = %s,
                    locked_by = NULL,
                    locked_until = NULL
                """,
                (
                    task.canonical_name,
                    task.parent_task.canonical_name,
                    task_spec,
                    execute_at,
                    task_spec,
                    execute_at,
                ),
            )

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self._transaction() as cursor:
            await cursor.execute(
                f"DELETE FROM {self._table_name} WHERE name = %s",
                (task.canonical_name,),
            )

    async def poll_task(self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None) -> PollResponse:
        async with self._transaction() as cursor:
            await cursor.execute(
                f"SELECT * FROM {self._table_name} WHERE name = %s FOR UPDATE",
                (task.canonical_name,),
            )
            record = await cursor.fetchone()
            logger.debug(f"poll_task returned {record}")

            update = False
            if record is None:
                # Regular (non-dynamic) tasks will be implicitly created on first poll,
                # but dynamic task instances must be explicitely created to prevent spurious
                # poll from re-creating them after being deleted.
                if task.parent_task is not None:
                    raise PyncetteException("Task not found")

                execute_after = task.get_next_execution(utc_now, None)
                locked_until = None
                locked_by = None
                update = True
            else:
                execute_after = _from_timestamp(record["execute_after"])
                locked_until = _from_timestamp(record["locked_until"])
                locked_by = record["locked_by"]

            assert execute_after is not None
            scheduled_at = execute_after

            if locked_until is not None and locked_until > utc_now and (lease != locked_by):
                result = ResultType.LOCKED
            elif execute_after <= utc_now and task.execution_mode == ExecutionMode.AT_MOST_ONCE:
                execute_after = task.get_next_execution(utc_now, execute_after)
                result = ResultType.READY
                locked_until = None
                locked_by = None
                update = True
            elif execute_after <= utc_now and task.execution_mode == ExecutionMode.AT_LEAST_ONCE:
                locked_until = utc_now + task.lease_duration
                locked_by = str(uuid.uuid4())
                result = ResultType.READY
                update = True
            else:
                result = ResultType.PENDING

            if update:
                await self._update_record(
                    cursor,
                    task,
                    locked_until,
                    locked_by,
                    execute_after,
                )

            return PollResponse(result=result, scheduled_at=scheduled_at, lease=locked_by)

    async def commit_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        async with self._transaction() as cursor:
            await cursor.execute(
                f"SELECT * FROM {self._table_name} WHERE name = %s FOR UPDATE",
                (task.canonical_name,),
            )

            record = await cursor.fetchone()
            logger.debug(f"commit_task returned {record}")

            if not record:
                logger.warning(f"Task {task} not found, skipping.")
                return

            if record["locked_by"] != lease:
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            await self._update_record(
                cursor,
                task,
                None,
                None,
                task.get_next_execution(utc_now, _from_timestamp(record["execute_after"])),
            )

    async def extend_lease(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> Optional[Lease]:
        async with self._transaction() as cursor:
            locked_until = utc_now + task.lease_duration
            await cursor.execute(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = %s
                WHERE name = %s AND locked_by = %s
                """,
                (
                    _to_timestamp(locked_until),
                    task.canonical_name,
                    lease,
                ),
            )
            if cursor.rowcount == 1:
                return lease
            else:
                return None

    async def unlock_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        async with self._transaction() as cursor:
            await cursor.execute(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_by = NULL,
                    locked_until = NULL
                WHERE name = %s AND locked_by = %s
                """,
                (
                    task.canonical_name,
                    lease,
                ),
            )

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[aiomysql.Cursor]:
        async with self._pool.acquire() as connection:
            try:
                async with connection.cursor(aiomysql.DictCursor) as cursor:
                    yield cursor
            except Exception:
                await connection.rollback()
                raise
            else:
                await connection.commit()

    async def _update_record(
        self,
        cursor: aiomysql.Cursor,
        task: Task,
        locked_until: Optional[datetime.datetime],
        locked_by: Optional[str],
        execute_after: Optional[datetime.datetime],
    ) -> None:
        if execute_after is None:
            await cursor.execute(
                f"DELETE FROM {self._table_name} WHERE name = %s",
                (task.canonical_name,),
            )
        else:
            await cursor.execute(
                f"""
                INSERT INTO {self._table_name} (name, locked_until, locked_by, execute_after)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    locked_until = %s,
                    locked_by = %s,
                    execute_after = %s
                """,
                (
                    task.canonical_name,
                    _to_timestamp(locked_until),
                    locked_by,
                    _to_timestamp(execute_after),
                    _to_timestamp(locked_until),
                    locked_by,
                    _to_timestamp(execute_after),
                ),
            )


@contextlib.asynccontextmanager
async def mysql_repository(
    *,
    mysql_host: str,
    mysql_user: str,
    mysql_database: str,
    mysql_password: Optional[str] = None,
    mysql_port: int = 3306,
    **kwargs: Any,
) -> AsyncIterator[MySQLRepository]:
    """Factory context manager that initializes the connection to MySQL"""
    mysql_pool = await aiomysql.create_pool(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_database,
        loop=asyncio.get_running_loop(),
    )
    try:
        repository = MySQLRepository(mysql_pool, **kwargs)
        if not kwargs.get("mysql_skip_table_create", False):
            await repository.initialize()

        yield repository
    finally:
        mysql_pool.close()
        await mysql_pool.wait_closed()
