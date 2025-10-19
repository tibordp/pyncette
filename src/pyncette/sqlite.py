import asyncio
import contextlib
import datetime
import json
import logging
import re
import uuid
from typing import Any
from collections.abc import AsyncIterator
from typing import Optional
from typing import cast

import aiosqlite
import dateutil.tz

from pyncette.errors import PyncetteException
from pyncette.errors import TaskLockedException
from pyncette.model import ContinuationToken
from pyncette.model import ExecutionMode
from pyncette.model import Lease
from pyncette.model import ListTasksResponse
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.model import ResultType
from pyncette.model import TaskState
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


class SqliteRepository(Repository):
    _connection: aiosqlite.Connection
    _batch_size: int
    _table_name: str
    _lock: asyncio.Lock

    def __init__(
        self,
        connection: aiosqlite.Connection,
        **kwargs: Any,
    ):
        self._connection = connection
        self._table_name = kwargs.get("sqlite_table_name", "pyncette_tasks")
        self._batch_size = kwargs.get("batch_size", 100)
        self._lock = asyncio.Lock()

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")
        if not re.match(r"^[a-z_]+$", self._table_name):
            raise ValueError("Table name can only contain lower-case letters and underscores")

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
        self,
        utc_now: datetime.datetime,
        task: Task,
        continuation_token: Optional[ContinuationToken] = None,
    ) -> QueryResponse:
        async with self._transaction(explicit_begin=True):
            locked_by = uuid.uuid4()
            locked_until = utc_now + task.lease_duration

            ready_tasks = await self._connection.execute_fetchall(
                f"""SELECT * FROM {self._table_name}
                WHERE parent_name = :parent_name AND MAX(COALESCE(locked_until, 0), COALESCE(execute_after, 0)) <= :utc_now
                ORDER BY MAX(COALESCE(locked_until, 0), COALESCE(execute_after, 0)) ASC
                LIMIT :batch_size
                """,
                {
                    "parent_name": task.canonical_name,
                    "utc_now": _to_timestamp(utc_now),
                    "batch_size": self._batch_size,
                },
            )

            concrete_tasks = [task.instantiate_from_spec(json.loads(record["task_spec"])) for record in ready_tasks]
            await self._connection.executemany(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = :locked_until,
                    locked_by = :locked_by
                WHERE name = :name
                """,
                [
                    {
                        "name": concrete_task.canonical_name,
                        "locked_until": _to_timestamp(locked_until),
                        "locked_by": str(locked_by),
                    }
                    for concrete_task in concrete_tasks
                ],
            )

            return QueryResponse(
                tasks=[(concrete_task, Lease(locked_by)) for concrete_task in concrete_tasks],
                continuation_token=_CONTINUATION_TOKEN if len(concrete_tasks) == self._batch_size else None,
            )

    async def register_task(self, utc_now: datetime.datetime, task: Task, force: bool = False) -> None:
        async with self._transaction(explicit_begin=True):
            assert task.parent_task is not None
            records = await self._connection.execute_fetchall(
                f"SELECT * FROM {self._table_name} WHERE name = :name",
                {"name": task.canonical_name},
            )

            new_execute_after = task.get_next_execution(utc_now, None)

            if records:
                record = next(iter(records))
                existing_locked_until = _from_timestamp(record["locked_until"])
                existing_execute_after = _from_timestamp(record["execute_after"])

                # Check if task is currently locked
                if not force and existing_locked_until is not None and existing_locked_until > utc_now:
                    raise TaskLockedException(task, existing_locked_until)

                # Determine the execute_after to use
                if force:
                    # Force mode: use new schedule and clear locks
                    execute_after = new_execute_after
                    locked_until = None
                    locked_by = None
                else:
                    # Safe mode: keep sooner schedule to avoid starvation
                    if existing_execute_after is not None and new_execute_after is not None:
                        execute_after = min(existing_execute_after, new_execute_after)
                    else:
                        execute_after = new_execute_after or existing_execute_after
                    # Preserve lock state (though we already checked it's not locked)
                    locked_until = existing_locked_until
                    locked_by = record["locked_by"]

                await self._connection.execute_fetchall(
                    f"""
                    UPDATE {self._table_name}
                    SET
                        task_spec = :task_spec,
                        execute_after = :execute_after,
                        locked_until = :locked_until,
                        locked_by = :locked_by
                    WHERE
                        name = :name
                    """,
                    {
                        "name": task.canonical_name,
                        "task_spec": json.dumps(task.as_spec()),
                        "execute_after": _to_timestamp(execute_after),
                        "locked_until": _to_timestamp(locked_until),
                        "locked_by": locked_by,
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
                        "execute_after": _to_timestamp(new_execute_after),
                    },
                )

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        async with self._transaction():
            await self._connection.execute_fetchall(
                f"DELETE FROM {self._table_name} WHERE name = :name",
                {"name": task.canonical_name},
            )

    async def poll_task(self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None) -> PollResponse:
        async with self._transaction(explicit_begin=True):
            records = await self._connection.execute_fetchall(
                f"SELECT * FROM {self._table_name} WHERE name = :name",
                {"name": task.canonical_name},
            )

            if not records:
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
                    VALUES (:name, :execute_after)
                    """,
                    {
                        "name": task.canonical_name,
                        "execute_after": _to_timestamp(execute_after),
                    },
                )
            else:
                record = next(iter(records))
                execute_after = cast(datetime.datetime, _from_timestamp(record["execute_after"]))
                locked_until = _from_timestamp(record["locked_until"])
                locked_by = record["locked_by"]

            assert execute_after is not None
            scheduled_at = execute_after

            if locked_until is not None and locked_until > utc_now and (str(lease) != locked_by):
                result = ResultType.LOCKED
            elif execute_after <= utc_now and task.execution_mode == ExecutionMode.AT_MOST_ONCE:
                execute_after = task.get_next_execution(utc_now, execute_after)
                result = ResultType.READY
                locked_until = None
                locked_by = None
                await self._update_record(
                    task,
                    locked_until,
                    locked_by,
                    execute_after,
                )
            elif execute_after <= utc_now and task.execution_mode == ExecutionMode.AT_LEAST_ONCE:
                locked_until = utc_now + task.lease_duration
                locked_by = uuid.uuid4()
                result = ResultType.READY
                await self._update_record(
                    task,
                    locked_until,
                    locked_by,
                    execute_after,
                )
            else:
                result = ResultType.PENDING

            return PollResponse(result=result, scheduled_at=scheduled_at, lease=locked_by)

    async def commit_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        async with self._transaction(explicit_begin=True):
            records = await self._connection.execute_fetchall(
                f"SELECT * FROM {self._table_name} WHERE name = :name",
                {"name": task.canonical_name},
            )

            if not records:
                logger.warning(f"Task {task} not found, skipping.")
                return

            record = next(iter(records))
            if record["locked_by"] != str(lease):
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            execute_after = datetime.datetime.fromtimestamp(record["execute_after"], dateutil.tz.UTC) if record["execute_after"] else None
            await self._update_record(
                task,
                None,
                None,
                task.get_next_execution(utc_now, execute_after),
            )

    async def extend_lease(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> Optional[Lease]:
        async with self._transaction():
            locked_until = utc_now + task.lease_duration
            async with await self._connection.execute(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_until = :locked_until
                WHERE name = :name AND locked_by = :locked_by
                """,
                {
                    "locked_until": _to_timestamp(locked_until),
                    "name": task.canonical_name,
                    "locked_by": str(lease),
                },
            ) as cursor:
                if cursor.rowcount == 1:
                    return lease
                else:
                    return None

    async def unlock_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        async with self._transaction():
            await self._connection.execute_fetchall(
                f"""
                UPDATE {self._table_name}
                SET
                    locked_by = NULL,
                    locked_until = NULL
                WHERE name = :name AND locked_by = :locked_by
                """,
                {
                    "name": task.canonical_name,
                    "locked_by": str(lease),
                },
            )

    async def get_task_state(
        self,
        utc_now: datetime.datetime,
        task: Task,
    ) -> Optional[TaskState]:
        async with self._transaction():
            records = await self._connection.execute_fetchall(
                f"SELECT * FROM {self._table_name} WHERE name = :name",
                {"name": task.canonical_name},
            )

            if not records:
                return None

            record = next(iter(records))

            # For dynamic tasks, re-instantiate from spec to ensure we have fresh parameters
            if task.parent_task is not None:
                task_spec = json.loads(record["task_spec"]) if record["task_spec"] else None
                if not task_spec:
                    raise PyncetteException(f"Task {task.canonical_name} has no task_spec stored")
                instantiated_task = task.parent_task.instantiate_from_spec(task_spec)
            else:
                # Static task - use as-is (no task_spec stored)
                instantiated_task = task

            scheduled_at = _from_timestamp(record["execute_after"])
            assert scheduled_at is not None, "execute_after should not be None for existing tasks"

            return TaskState(
                task=instantiated_task,
                scheduled_at=scheduled_at,
                locked_until=_from_timestamp(record["locked_until"]),
                locked_by=record["locked_by"],
            )

    async def list_task_states(
        self,
        utc_now: datetime.datetime,
        parent_task: Task,
        limit: Optional[int] = None,
        continuation_token: Optional[ContinuationToken] = None,
    ) -> ListTasksResponse:
        if limit is None:
            limit = self._batch_size

        async with self._transaction():
            if continuation_token is not None:
                # Continuation token is the last name seen
                last_name = continuation_token
                records = await self._connection.execute_fetchall(
                    f"""
                    SELECT * FROM {self._table_name}
                    WHERE parent_name = :parent_name AND name > :last_name
                    ORDER BY name
                    LIMIT :limit
                    """,
                    {
                        "parent_name": parent_task.canonical_name,
                        "last_name": last_name,
                        "limit": limit + 1,  # Fetch one extra to check if there's more
                    },
                )
            else:
                records = await self._connection.execute_fetchall(
                    f"""
                    SELECT * FROM {self._table_name}
                    WHERE parent_name = :parent_name
                    ORDER BY name
                    LIMIT :limit
                    """,
                    {
                        "parent_name": parent_task.canonical_name,
                        "limit": limit + 1,  # Fetch one extra to check if there's more
                    },
                )

            has_more = len(records) > limit
            if has_more:
                records = records[:limit]

            tasks = []
            for record in records:
                task_spec = json.loads(record["task_spec"]) if record["task_spec"] else None
                if not task_spec:
                    logger.warning(f"Task {record['name']} has no task_spec, skipping")
                    continue

                instantiated_task = parent_task.instantiate_from_spec(task_spec)

                scheduled_at = _from_timestamp(record["execute_after"])
                assert scheduled_at is not None, "execute_after should not be None for existing tasks"

                tasks.append(
                    TaskState(
                        task=instantiated_task,
                        scheduled_at=scheduled_at,
                        locked_until=_from_timestamp(record["locked_until"]),
                        locked_by=record["locked_by"],
                    )
                )

            next_token = ContinuationToken(records[-1]["name"]) if has_more and records else None

            return ListTasksResponse(
                tasks=tasks,
                continuation_token=next_token,
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
                f"DELETE FROM {self._table_name} WHERE name = :name",
                {"name": task.canonical_name},
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

    async with aiosqlite.connect(kwargs.get("sqlite_database", ":memory:")) as connection:
        connection.row_factory = aiosqlite.Row
        repository = SqliteRepository(connection, **kwargs)
        await repository.initialize()
        yield repository
