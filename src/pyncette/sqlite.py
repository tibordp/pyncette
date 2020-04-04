import asyncio
import contextlib
import datetime
import json
import logging
import re
import uuid
from typing import Any
from typing import AsyncIterator
from typing import Optional
from typing import cast

import aiosqlite
import dateutil.tz

from pyncette.errors import PyncetteException
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


class SqliteRepository(Repository):
    _connection: aiosqlite.Connection
    _batch_size: int
    _table_name: str
    _lock: asyncio.Lock

    def __init__(
        self, connection: aiosqlite.Connection, **kwargs: Any,
    ):
        self._connection = connection
        self._table_name = kwargs.get("sqlite_table_name", "pyncette_tasks")
        self._batch_size = kwargs.get("batch_size", 100)
        self._lock = asyncio.Lock()

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")
        if not re.match(r"^[a-z_]+$", self._table_name):
            raise ValueError(
                "Table name can only contain lower-case letters and underscores"
            )

    async def initialize(self) -> None:
        async with self._transaction():
            await self._connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table_name} (
                    name PRIMARY KEY,
                    parent_name,
                    locked_until timestamp,
                    locked_by,
                    execute_after timestamp,
                    task_spec
                );
                CREATE INDEX IF NOT EXISTS due_tasks_{self._table_name}
                ON {self._table_name} (parent_name, MAX(COALESCE(locked_until, 0), COALESCE(execute_after, 0)));
                """
            )

    async def poll_dynamic_task(
        self, utc_now: datetime.datetime, task: Task
    ) -> QueryResponse:
        async with self._transaction(explicit_begin=True):
            locked_by = uuid.uuid4()
            locked_until = utc_now + task.lease_duration

            ready_tasks = await self._connection.execute_fetchall(
                f"""SELECT * FROM {self._table_name}
                WHERE parent_name = $1 AND MAX(COALESCE(locked_until, 0), COALESCE(execute_after, 0)) <= $2
                ORDER BY MAX(COALESCE(locked_until, 0), COALESCE(execute_after, 0)) ASC
                LIMIT $3
                """,
                (task.canonical_name, _to_timestamp(utc_now), self._batch_size),
            )

            concrete_tasks = [
                task.instantiate_from_spec(json.loads(record["task_spec"]))
                for record in ready_tasks
            ]

            await self._connection.executemany(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = $2,
                    locked_by = $3
                WHERE name = $1
                """,
                [
                    (
                        concrete_task.canonical_name,
                        _to_timestamp(locked_until),
                        str(locked_by),
                    )
                    for concrete_task in concrete_tasks
                ],
            )
            return QueryResponse(
                tasks=[
                    (concrete_task, Lease(locked_by))
                    for concrete_task in concrete_tasks
                ],
                has_more=len(concrete_tasks) == self._batch_size,
            )

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self._transaction(explicit_begin=True):
            assert task.parent_task is not None
            record = await self._connection.execute_fetchall(
                f"SELECT 1 FROM {self._table_name} WHERE name = ?",
                (task.canonical_name,),
            )

            if record:
                await self._connection.execute_fetchall(
                    f"""
                    UPDATE {self._table_name}
                    SET
                        task_spec = :task_spec,
                        execute_after = :execute_after,
                        locked_until = NULL,
                        locked_by = NULL
                    WHERE
                        name = :name
                    """,
                    {
                        "name": task.canonical_name,
                        "task_spec": json.dumps(task.as_spec()),
                        "execute_after": _to_timestamp(
                            task.get_next_execution(utc_now, None)
                        ),
                    },
                )
            else:
                await self._connection.execute_fetchall(
                    f"""
                    INSERT INTO {self._table_name} (name, parent_name, task_spec, execute_after)
                    VALUES (:name, :parent_name, :task_spec, :execute_after)
                    """,
                    {
                        "name": task.canonical_name,
                        "parent_name": task.parent_task.canonical_name,
                        "task_spec": json.dumps(task.as_spec()),
                        "execute_after": _to_timestamp(
                            task.get_next_execution(utc_now, None)
                        ),
                    },
                )

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self._transaction():
            await self._connection.execute_fetchall(
                f"DELETE FROM {self._table_name} WHERE name = $1",
                (task.canonical_name,),
            )

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None
    ) -> PollResponse:
        async with self._transaction(explicit_begin=True):
            record = await self._connection.execute_fetchall(
                f"SELECT * FROM {self._table_name} WHERE name = ?",
                (task.canonical_name,),
            )

            if not record:
                # Regular (non-dynamic) tasks will be implicitly created on first poll,
                # but dynamic task instances must be explicitely created to prevent spurious
                # poll from re-creating them after being deleted.
                if task.parent_task is not None:
                    raise PyncetteException("Task not found")

                locked_until = None
                locked_by = None
                execute_after = task.get_next_execution(utc_now, None)
                await self._connection.execute_fetchall(
                    f"""
                    INSERT INTO {self._table_name} (name, execute_after)
                    VALUES (:name, :locked_until)
                    """,
                    (task.canonical_name, _to_timestamp(execute_after)),
                )
            else:
                record = record[0]
                execute_after = cast(
                    datetime.datetime, _from_timestamp(record["execute_after"])
                )
                locked_until = _from_timestamp(record["locked_until"])
                locked_by = record["locked_by"]

            assert execute_after is not None
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
                locked_until = None
                locked_by = None
                await self._update_record(
                    task, locked_until, locked_by, execute_after,
                )
            elif (
                execute_after <= utc_now
                and task.execution_mode == ExecutionMode.AT_LEAST_ONCE
            ):
                locked_until = utc_now + task.lease_duration
                locked_by = uuid.uuid4()
                result = ResultType.READY
                await self._update_record(
                    task, locked_until, locked_by, execute_after,
                )
            else:
                result = ResultType.PENDING

            return PollResponse(
                result=result, scheduled_at=scheduled_at, lease=locked_by
            )

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        async with self._transaction(explicit_begin=True):
            record = await self._connection.execute_fetchall(
                f"SELECT * FROM {self._table_name} WHERE name = $1",
                (task.canonical_name,),
            )

            if not record:
                logger.warning(f"Task {task} not found, skipping.")
                return

            record = record[0]
            if record["locked_by"] != str(lease):
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            execute_after = (
                datetime.datetime.fromtimestamp(
                    record["execute_after"], dateutil.tz.UTC
                )
                if record["execute_after"]
                else None
            )
            await self._update_record(
                task, None, None, task.get_next_execution(utc_now, execute_after),
            )

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        async with self._transaction():
            await self._connection.execute_fetchall(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_by = NULL,
                    locked_until = NULL
                WHERE name = ? AND locked_by = ?
                """,
                (task.canonical_name, str(lease)),
            )

    async def _update_record(
        self,
        task: Task,
        locked_until: Optional[datetime.datetime],
        locked_by: Optional[uuid.UUID],
        execute_after: Optional[datetime.datetime],
    ) -> None:
        if execute_after is None:
            await self._connection.execute_fetchall(
                f"DELETE FROM {self._table_name} WHERE name = $1",
                (task.canonical_name,),
            )
        else:
            await self._connection.execute_fetchall(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = :locked_until,
                    locked_by = :locked_by,
                    execute_after = :execute_after
                WHERE name = :name
                """,
                {
                    "name": task.canonical_name,
                    "locked_until": _to_timestamp(locked_until),
                    "locked_by": str(locked_by),
                    "execute_after": _to_timestamp(execute_after),
                },
            )

    @contextlib.asynccontextmanager
    async def _transaction(self, explicit_begin: bool = False) -> AsyncIterator[None]:
        async with self._lock:
            # If we only execute a single DML statement, the transaction will be implicitly open
            # but if we start with a SELECT, we need to be in a transaction explicitely.
            await self._connection.execute_fetchall("BEGIN")
            try:
                yield
            except Exception:
                await self._connection.rollback()
                raise
            else:
                await self._connection.commit()


@contextlib.asynccontextmanager
async def sqlite_repository(**kwargs: Any) -> AsyncIterator[SqliteRepository]:
    """Factory context manager for Sqlite repository that initializes the connection to Sqlite"""

    async with aiosqlite.connect(
        kwargs.get("sqlite_database", ":memory:")
    ) as connection:
        connection.row_factory = aiosqlite.Row
        repository = SqliteRepository(connection, **kwargs)
        await repository.initialize()
        yield repository
