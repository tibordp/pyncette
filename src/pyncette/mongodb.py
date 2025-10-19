# src/pyncette/mongodb.py
from __future__ import annotations

import contextlib
import copy
import datetime
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any
from collections.abc import AsyncIterator
from typing import cast

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING

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

MAX_OPTIMISTIC_RETRY_COUNT = 5


@dataclass
class _TaskRecord:
    execute_after: datetime.datetime | None
    locked_until: datetime.datetime | None
    locked_by: str | None
    version: int

    @staticmethod
    def _parse_date(s: str | None) -> datetime.datetime | None:
        return datetime.datetime.fromisoformat(s) if s else None

    @classmethod
    def from_mongo_doc(cls, doc: dict[str, Any] | None) -> _TaskRecord | None:
        if not doc:
            return None
        return cls(
            execute_after=cls._parse_date(doc.get("execute_after")),
            locked_until=cls._parse_date(doc.get("locked_until")),
            locked_by=doc.get("locked_by"),
            version=int(doc.get("version", 0)),
        )


class MongoDBRepository(Repository):
    """
    MongoDB-backed store for Pyncette task execution data.

    Collection schema (documents in a single collection):
        {
          "partition_id": str,
          "task_id": str,
          "task_spec": str | None,        # JSON for dynamic tasks
          "version": int,
          "locked_by": str | None,
          "locked_until": str | None,     # ISO 8601 (kept as strings like DynamoDB)
          "execute_after": str | None,    # ISO 8601
          "ready_at": str,                # f"{max(execute_after, locked_until)}_{task_id}" (lex-queue)
        }

    Indexes:
      - Unique:        (partition_id, task_id)
      - Ready queue:   (partition_id, ready_at)
    """

    _client: AsyncIOMotorClient
    _db: AsyncIOMotorDatabase
    _coll: AsyncIOMotorCollection
    _batch_size: int
    _partition_prefix: str

    def __init__(
        self,
        mongo_client: AsyncIOMotorClient,
        skip_collection_create: bool,
        partition_prefix: str,
        **kwargs: Any,
    ):
        # Config pulled similarly to how Dynamo driver reads kwargs
        self._client = mongo_client
        self._db = self._client[kwargs.get("mongodb_database_name", "pyncette")]
        self._coll = self._db[kwargs.get("mongodb_collection_name", "tasks")]
        self._batch_size = kwargs.get("batch_size", 100)
        self._partition_prefix = partition_prefix

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")

    # ------------------------ helpers ------------------------

    def _get_partition_id(self, task: Task) -> str:
        prefix = (self._partition_prefix or "").replace(":", "::")
        if task.parent_task:
            return f"{prefix}:{task.parent_task.canonical_name}"
        else:
            return f"{prefix}:{task.canonical_name}"

    async def _retrieve_item(self, task: Task) -> _TaskRecord | None:
        doc = await self._coll.find_one(
            {"partition_id": self._get_partition_id(task), "task_id": task.canonical_name},
            projection={"_id": False},
        )
        return _TaskRecord.from_mongo_doc(doc)

    async def _update_item(
        self,
        task: Task,
        record: _TaskRecord,
        update_task_spec: bool = False,
        set_last_lease: bool = True,
        allow_upsert: bool = False,
    ) -> bool:
        """
        Optimistic CAS: match on version and bump version on write.
        Deletion when execute_after is None.
        """
        current_version = record.version
        pid = self._get_partition_id(task)
        tid = task.canonical_name

        # Build the version-match predicate.
        if current_version == 0:
            version_pred = {"$or": [{"version": {"$exists": False}}, {"version": 0}]}
        else:
            version_pred = {"version": current_version}

        if record.execute_after is None:
            # Delete with CAS
            res = await self._coll.delete_one({"partition_id": pid, "task_id": tid, **version_pred})
            ok = res.deleted_count == 1
            if ok and set_last_lease:
                task._last_lease = None  # type: ignore[attr-defined]
            return ok

        # Compute ready_at like in Dynamo: max(execute_after, locked_until) or execute_after
        ready_when = record.execute_after
        if record.locked_until is not None and (ready_when is None or record.locked_until > ready_when):
            ready_when = record.locked_until
        assert ready_when is not None
        ready_at = f"{ready_when.isoformat()}_{tid}"

        # Build the update doc
        set_fields: dict[str, Any] = {
            "execute_after": record.execute_after.isoformat() if record.execute_after else None,
            "locked_until": record.locked_until.isoformat() if record.locked_until else None,
            "locked_by": record.locked_by,
            "ready_at": ready_at,
            "version": current_version + 1,
        }
        if update_task_spec:
            set_fields["task_spec"] = json.dumps(task.as_spec())

        update_doc = {"$set": set_fields}
        if allow_upsert:
            update_doc["$setOnInsert"] = {"partition_id": pid, "task_id": tid}

        res = await self._coll.update_one(
            {"partition_id": pid, "task_id": tid, **version_pred},
            update_doc,
            upsert=allow_upsert,
        )
        ok = res.matched_count == 1 or res.upserted_id is not None
        if ok:
            record.version = current_version + 1
            if set_last_lease:
                task._last_lease = record  # type: ignore[attr-defined]
        return ok

    # ------------------ Repository interface ------------------

    async def poll_dynamic_task(
        self,
        utc_now: datetime.datetime,
        task: Task,
        continuation_token: ContinuationToken | None = None,
    ) -> QueryResponse:
        """
        Query earliest-due dynamic tasks from a partition (enabled via ready_at < now`).
        Mirrors dynamodb: use lexicographic bound 'now`' and return a continuation token.
        """
        pid = self._get_partition_id(task)
        upper = f"{utc_now.isoformat()}`"  # same backtick trick

        # Continuation token schema for Mongo:
        # {"partition_id": <pid>, "ready_at": <last_ready_at>, "task_id": <last_task_id>}
        filt: dict[str, Any] = {
            "partition_id": pid,
            "ready_at": {"$lt": upper},
        }
        if continuation_token is not None:
            tok = cast(dict[str, Any], continuation_token)
            # Start strictly AFTER the last seen (ready_at, task_id)
            ra = tok.get("ready_at")
            tid = tok.get("task_id")
            if ra is not None and tid is not None:
                filt["$or"] = [
                    {"ready_at": {"$gt": ra}},
                    {"ready_at": ra, "task_id": {"$gt": tid}},
                ]

        cursor = (
            self._coll.find(filt, projection={"_id": False}).sort([("ready_at", ASCENDING), ("task_id", ASCENDING)]).limit(self._batch_size)
        )
        items = [doc async for doc in cursor]

        next_token: ContinuationToken | None = None
        if len(items) == self._batch_size:
            last = items[-1]
            next_token = {
                "partition_id": pid,
                "ready_at": last["ready_at"],
                "task_id": last["task_id"],
            }

        return QueryResponse(
            tasks=[
                (
                    task.instantiate_from_spec(json.loads(doc["task_spec"])),
                    Lease(_TaskRecord.from_mongo_doc(doc)),
                )
                for doc in items
            ],
            continuation_token=next_token,
        )

    async def register_task(self, utc_now: datetime.datetime, task: Task, force: bool = False) -> None:
        new_execute_after = task.get_next_execution(utc_now, None)
        assert new_execute_after is not None

        pid = self._get_partition_id(task)
        tid = task.canonical_name

        if force:
            ready_at = new_execute_after
            doc = {
                "partition_id": pid,
                "task_id": tid,
                "task_spec": json.dumps(task.as_spec()),
                "version": 0,
                "locked_by": None,
                "locked_until": None,
                "execute_after": new_execute_after.isoformat(),
                "ready_at": f"{ready_at.isoformat()}_{tid}",
            }
            # Upsert unconditional
            await self._coll.update_one(
                {"partition_id": pid, "task_id": tid},
                {"$set": doc},
                upsert=True,
            )
            return

        # Safe mode: CAS loop with optimistic locking
        for _ in range(MAX_OPTIMISTIC_RETRY_COUNT):
            existing_record = await self._retrieve_item(task)

            # Check lock
            if existing_record and existing_record.locked_until and existing_record.locked_until > utc_now:
                raise TaskLockedException(task, existing_record.locked_until)

            if existing_record:
                record = _TaskRecord(
                    execute_after=min(existing_record.execute_after, new_execute_after)
                    if existing_record.execute_after
                    else new_execute_after,
                    locked_until=existing_record.locked_until,
                    locked_by=existing_record.locked_by,
                    version=existing_record.version,
                )
            else:
                record = _TaskRecord(
                    execute_after=new_execute_after,
                    locked_until=None,
                    locked_by=None,
                    version=0,
                )

            if await self._update_item(task, record, update_task_spec=True, set_last_lease=False, allow_upsert=(existing_record is None)):
                return

        raise PyncetteException(f"Unable to register task {task.canonical_name} due to contention")

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        await self._coll.delete_one({"partition_id": self._get_partition_id(task), "task_id": task.canonical_name})

    async def poll_task(self, utc_now: datetime.datetime, task: Task, lease: Lease | None = None) -> PollResponse:
        last_lease = getattr(task, "_last_lease", None)

        # Try to use cached state (as in Dynamo version)
        record: _TaskRecord | None
        potentially_stale = False

        if lease is not None:
            assert isinstance(lease, _TaskRecord)
            record = cast(_TaskRecord, copy.copy(lease))
        elif last_lease is not None:
            logger.debug("Using cached values for last lease")
            assert isinstance(last_lease, _TaskRecord)
            record = cast(_TaskRecord, copy.copy(last_lease))
            potentially_stale = True
        else:
            record = await self._retrieve_item(task)

        for _ in range(MAX_OPTIMISTIC_RETRY_COUNT):
            must_revalidate = False
            update = False

            if record is None:
                if task.parent_task is not None:
                    raise PyncetteException("Task not found")
                record = _TaskRecord(
                    execute_after=task.get_next_execution(utc_now, None),
                    locked_until=None,
                    locked_by=None,
                    version=0,
                )
                update = True

            assert record.execute_after is not None
            scheduled_at = record.execute_after

            if record.locked_until is not None and record.locked_until > utc_now and (lease is None or lease.locked_by != record.locked_by):
                result = ResultType.LOCKED
                if potentially_stale:
                    must_revalidate = True
            elif record.execute_after <= utc_now and task.execution_mode == ExecutionMode.AT_MOST_ONCE:
                result = ResultType.READY
                record.execute_after = task.get_next_execution(utc_now, record.execute_after)
                record.locked_until = None
                record.locked_by = None
                update = True
            elif record.execute_after <= utc_now and task.execution_mode == ExecutionMode.AT_LEAST_ONCE:
                result = ResultType.READY
                record.locked_until = utc_now + task.lease_duration
                record.locked_by = str(uuid.uuid4())
                update = True
            else:
                result = ResultType.PENDING

            if must_revalidate or (
                update
                and not await self._update_item(task, record, allow_upsert=(last_lease is None and lease is None and record.version == 0))
            ):
                logger.debug("Revalidating against MongoDB")
                record = await self._retrieve_item(task)
                potentially_stale = False
                continue

            return PollResponse(result=result, scheduled_at=scheduled_at, lease=Lease(record))

        raise PyncetteException("Unable to acquire the lock on the task due to contention")

    async def commit_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        assert isinstance(lease, _TaskRecord)
        record: _TaskRecord | None = cast(_TaskRecord, copy.copy(lease))

        for _ in range(MAX_OPTIMISTIC_RETRY_COUNT):
            if not record:
                logger.warning(f"Task {task} no longer exists, skipping.")
                return
            if record.locked_by != lease.locked_by:
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            record.execute_after = task.get_next_execution(utc_now, lease.execute_after)
            record.locked_by = None
            record.locked_until = None

            if await self._update_item(task, record, allow_upsert=False):
                return

            record = await self._retrieve_item(task)

        raise PyncetteException("Unable to acquire the lock on the task due to contention")

    async def unlock_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        assert isinstance(lease, _TaskRecord)
        record: _TaskRecord | None = cast(_TaskRecord, copy.copy(lease))

        for _ in range(MAX_OPTIMISTIC_RETRY_COUNT):
            if not record:
                logger.warning(f"Task {task} no longer exists, skipping.")
                return
            if record.locked_by != lease.locked_by:
                logger.warning(f"Lease lost on task {task}, skipping.")
                return

            record.locked_by = None
            record.locked_until = None

            if await self._update_item(task, record, allow_upsert=False):
                return

            record = await self._retrieve_item(task)

        raise PyncetteException("Unable to acquire the lock on the task due to contention")

    async def get_task_state(
        self,
        utc_now: datetime.datetime,
        task: Task,
    ) -> TaskState | None:
        doc = await self._coll.find_one(
            {"partition_id": self._get_partition_id(task), "task_id": task.canonical_name},
            projection={"_id": False},
        )
        if not doc:
            return None

        # Dynamic tasks: re-instantiate from spec
        if task.parent_task is not None:
            task_spec = json.loads(doc["task_spec"]) if doc.get("task_spec") else None
            if not task_spec:
                raise PyncetteException(f"Task {task.canonical_name} has no task_spec stored")
            instantiated_task = task.parent_task.instantiate_from_spec(task_spec)
        else:
            instantiated_task = task

        execute_after = doc.get("execute_after")
        assert execute_after, "execute_after should not be empty for existing tasks"
        scheduled_at = datetime.datetime.fromisoformat(execute_after)

        return TaskState(
            task=instantiated_task,
            scheduled_at=scheduled_at,
            locked_until=datetime.datetime.fromisoformat(doc["locked_until"]) if doc.get("locked_until") else None,
            locked_by=doc.get("locked_by"),
        )

    async def list_task_states(
        self,
        utc_now: datetime.datetime,
        parent_task: Task,
        limit: int | None = None,
        continuation_token: ContinuationToken | None = None,
    ) -> ListTasksResponse:
        if limit is None:
            limit = self._batch_size

        prefix = (self._partition_prefix or "").replace(":", "::")
        partition_id = f"{prefix}:{parent_task.canonical_name}"

        filt: dict[str, Any] = {"partition_id": partition_id}
        if continuation_token is not None:
            tok = cast(dict[str, Any], continuation_token)
            last_task_id = tok.get("task_id")
            if last_task_id:
                filt["task_id"] = {"$gt": last_task_id}

        cursor = self._coll.find(filt, projection={"_id": False}).sort([("task_id", ASCENDING)]).limit(limit)
        items = [doc async for doc in cursor]

        tasks: list[TaskState] = []
        for item in items:
            task_spec = json.loads(item["task_spec"]) if item.get("task_spec") else None
            if not task_spec:
                logger.warning(f"Task {item.get('task_id')} has no task_spec, skipping")
                continue
            instantiated_task = parent_task.instantiate_from_spec(task_spec)
            execute_after = item.get("execute_after")
            assert execute_after, "execute_after should not be empty for existing tasks"
            scheduled_at = datetime.datetime.fromisoformat(execute_after)
            tasks.append(
                TaskState(
                    task=instantiated_task,
                    scheduled_at=scheduled_at,
                    locked_until=datetime.datetime.fromisoformat(item["locked_until"]) if item.get("locked_until") else None,
                    locked_by=item.get("locked_by"),
                )
            )

        next_token: ContinuationToken | None = None
        if len(items) == limit:
            next_token = {"partition_id": partition_id, "task_id": items[-1]["task_id"]}

        return ListTasksResponse(tasks=tasks, continuation_token=next_token)

    async def extend_lease(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> Lease | None:
        assert isinstance(lease, _TaskRecord)
        record: _TaskRecord | None = cast(_TaskRecord, copy.copy(lease))

        for _ in range(MAX_OPTIMISTIC_RETRY_COUNT):
            if not record:
                logger.warning(f"Task {task} no longer exists, skipping.")
                return None
            if record.locked_by != lease.locked_by:
                logger.warning(f"Lease lost on task {task}, skipping.")
                return None

            record.locked_until = utc_now + task.lease_duration

            if await self._update_item(task, record, allow_upsert=False):
                return Lease(record)

            record = await self._retrieve_item(task)

        raise PyncetteException("Unable to acquire the lock on the task due to contention")

    # ---------------------- lifecycle ----------------------

    async def initialize(self) -> None:
        # Indexes are idempotent, always ensure they exist
        await self._coll.create_index(
            [("partition_id", ASCENDING), ("task_id", ASCENDING)],
            name="partition_task_unique",
            unique=True,
        )
        await self._coll.create_index(
            [("partition_id", ASCENDING), ("ready_at", ASCENDING)],
            name="ready_at_queue_idx",
            partialFilterExpression={"task_spec": {"$exists": True}},
        )


@contextlib.asynccontextmanager
async def mongodb_repository(
    *,
    mongodb_uri: str | None = None,
    mongodb_database_name: str = "pyncette",
    mongodb_collection_name: str = "tasks",
    mongodb_skip_collection_create: bool = False,
    mongodb_partition_prefix: str = "",
    **kwargs: Any,
) -> AsyncIterator[MongoDBRepository]:
    """
    Factory context manager for MongoDB repository mirroring the DynamoDB one.
    """
    client = AsyncIOMotorClient(mongodb_uri or "mongodb://localhost:27017")
    try:
        repository = MongoDBRepository(
            client,
            skip_collection_create=mongodb_skip_collection_create,
            partition_prefix=mongodb_partition_prefix,
            mongodb_database_name=mongodb_database_name,
            mongodb_collection_name=mongodb_collection_name,
            **kwargs,
        )
        await repository.initialize()
        yield repository
    finally:
        client.close()
