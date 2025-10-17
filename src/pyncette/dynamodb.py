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

import aioboto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

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
    def from_dynamo_response(cls, response: Any) -> _TaskRecord | None:
        if "Item" not in response:
            return None

        return cls.from_dynamo_item(response["Item"])

    @classmethod
    def from_dynamo_item(cls, record: dict[str, Any]) -> _TaskRecord:
        return cls(
            execute_after=cls._parse_date(record.get("execute_after")),
            locked_until=cls._parse_date(record.get("locked_until")),
            locked_by=record["locked_by"],
            version=int(record["version"]),
        )


class DynamoDBRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _dynamo_resource: Any
    _table_name: str
    _batch_size: int
    _skip_table_create: bool
    _partition_prefix: str

    def __init__(
        self,
        dynamo_resource: Any,
        skip_table_create: bool,
        partition_prefix: str,
        **kwargs: Any,
    ):
        self._dynamo_resource = dynamo_resource
        self._table_name = kwargs.get("dynamodb_table_name", "")
        self._batch_size = kwargs.get("batch_size", 100)
        self._skip_table_create = skip_table_create
        self._partition_prefix = partition_prefix

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")

    async def poll_dynamic_task(
        self,
        utc_now: datetime.datetime,
        task: Task,
        continuation_token: ContinuationToken | None = None,
    ) -> QueryResponse:
        response = await self._table.query(
            IndexName="ready_at",
            Select="ALL_ATTRIBUTES",
            Limit=self._batch_size,
            KeyConditionExpression=Key("partition_id").eq(self._get_partition_id(task)) & Key("ready_at").lt(f"{utc_now.isoformat()}`"),
            **({"ExclusiveStartKey": continuation_token} if continuation_token is not None else {}),
        )

        return QueryResponse(
            tasks=[
                (
                    task.instantiate_from_spec(json.loads(record["task_spec"])),
                    Lease(_TaskRecord.from_dynamo_item(record)),
                )
                for record in response["Items"]
            ],
            continuation_token=response.get("LastEvaluatedKey", None),
        )

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        execute_after = task.get_next_execution(utc_now, None)
        assert execute_after is not None

        await self._table.put_item(
            Item={
                "partition_id": self._get_partition_id(task),
                "task_id": task.canonical_name,
                "task_spec": json.dumps(task.as_spec()),
                "version": 0,
                "locked_by": None,
                "locked_until": None,
                "execute_after": execute_after.isoformat(),
                "ready_at": f"{execute_after.isoformat()}_{task.canonical_name}",
            },
        )

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        await self._table.delete_item(
            Key={
                "partition_id": self._get_partition_id(task),
                "task_id": task.canonical_name,
            }
        )

    def _get_partition_id(self, task: Task) -> str:
        prefix = (self._partition_prefix or "").replace(":", "::")
        if task.parent_task:
            return f"{prefix}:{task.parent_task.canonical_name}"
        else:
            return f"{prefix}:{task.canonical_name}"

    async def _retreive_item(self, task: Task, consistent_read: bool = False) -> _TaskRecord | None:
        result = _TaskRecord.from_dynamo_response(
            await self._table.get_item(
                Key={
                    "partition_id": self._get_partition_id(task),
                    "task_id": task.canonical_name,
                },
                ConsistentRead=consistent_read,
            )
        )
        return result

    async def _update_item(self, task: Task, record: _TaskRecord) -> bool:
        current_version = record.version
        try:
            if record.execute_after is None:
                await self._table.delete_item(
                    Key={
                        "partition_id": self._get_partition_id(task),
                        "task_id": task.canonical_name,
                    },
                    ConditionExpression=(
                        (Attr("version").not_exists() | Attr("version").eq(0))
                        if current_version == 0
                        else Attr("version").eq(current_version)
                    ),
                )
                task._last_lease = None  # type: ignore
            else:
                ready_at = max(record.execute_after, record.locked_until) if record.locked_until is not None else record.execute_after
                await self._table.update_item(
                    Key={
                        "partition_id": self._get_partition_id(task),
                        "task_id": task.canonical_name,
                    },
                    UpdateExpression="""
                    set execute_after=:execute_after,
                        locked_until=:locked_until,
                        locked_by=:locked_by,
                        ready_at=:ready_at,
                        version=:version
                    """,
                    ExpressionAttributeValues={
                        ":execute_after": record.execute_after.isoformat() if record.execute_after is not None else "",
                        ":locked_until": record.locked_until.isoformat() if record.locked_until is not None else "",
                        ":locked_by": record.locked_by,
                        # Add a suffix to ready_at to guarantee uniqueness of the secondary index
                        ":ready_at": f"{ready_at.isoformat()}_{task.canonical_name}",
                        ":version": current_version + 1,
                    },
                    ConditionExpression=(
                        (Attr("version").not_exists() | Attr("version").eq(0))
                        if current_version == 0
                        else Attr("version").eq(current_version)
                    ),
                )
                record.version = current_version + 1
                task._last_lease = record  # type: ignore
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            else:
                raise
        else:
            return True

    async def poll_task(self, utc_now: datetime.datetime, task: Task, lease: Lease | None = None) -> PollResponse:
        last_lease = getattr(task, "_last_lease", None)

        # Similar logic as in Redis repository. If we have previously processed this
        # task in any manner, we try to reuse the latest state of the task we have at hand
        # from cache (or lease) to avoid two roundtrips to DynamoDB in the optimistic case.
        # If we are wrong, we will get a version mismatch, whereby we will load the current
        # state from DB.
        # However, in case the task would be PENDING or LOCKED, this will result in no requests
        # being made to the DB at all. For pending this is OK, but for locked, we want to revalidate
        # in order to be able to execute the task as soon as it is unlocked.
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
            record = await self._retreive_item(task)

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

            if must_revalidate or (update and not await self._update_item(task, record)):
                logger.debug("Using cached values for last lease")
                record = await self._retreive_item(task, consistent_read=True)
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

            if await self._update_item(task, record):
                return

            # If the update fails due to version mismatch
            record = await self._retreive_item(task, consistent_read=True)

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

            if await self._update_item(task, record):
                return

            # If the update fails due to version mismatch
            record = await self._retreive_item(task, consistent_read=True)

        raise PyncetteException("Unable to acquire the lock on the task due to contention")

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

            if await self._update_item(task, record):
                return Lease(record)

            # If the update fails due to version mismatch
            record = await self._retreive_item(task, consistent_read=True)

        raise PyncetteException("Unable to acquire the lock on the task due to contention")

    async def initialize(self) -> None:
        self._table = await self._dynamo_resource.Table(self._table_name)
        if not self._skip_table_create:
            try:
                await self._table.load()
                logger.info("Table already exists...")
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    self._table = await self._dynamo_resource.create_table(
                        TableName=self._table_name,
                        KeySchema=[
                            {"AttributeName": "partition_id", "KeyType": "HASH"},
                            {"AttributeName": "task_id", "KeyType": "RANGE"},
                        ],
                        AttributeDefinitions=[
                            {"AttributeName": "partition_id", "AttributeType": "S"},
                            {"AttributeName": "task_id", "AttributeType": "S"},
                            {"AttributeName": "ready_at", "AttributeType": "S"},
                        ],
                        LocalSecondaryIndexes=[
                            {
                                "IndexName": "ready_at",
                                "KeySchema": [
                                    {
                                        "AttributeName": "partition_id",
                                        "KeyType": "HASH",
                                    },
                                    {"AttributeName": "ready_at", "KeyType": "RANGE"},
                                ],
                                "Projection": {"ProjectionType": "ALL"},
                            }
                        ],
                        BillingMode="PAY_PER_REQUEST",
                    )
                    logger.info("Waiting until table is created...")
                    await self._table.wait_until_exists()
                else:
                    raise


@contextlib.asynccontextmanager
async def dynamodb_repository(
    *,
    dynamodb_endpoint: str | None = None,
    dynamodb_region_name: str | None = None,
    dynamodb_skip_table_create: bool = False,
    dynamodb_partition_prefix: str = "",
    **kwargs: Any,
) -> AsyncIterator[DynamoDBRepository]:
    """Factory context manager for Redis repository that initializes the connection to Redis"""
    session = aioboto3.Session()
    async with session.resource(
        "dynamodb",
        region_name=dynamodb_region_name,
        endpoint_url=dynamodb_endpoint,
    ) as dynamo_resource:
        repository = DynamoDBRepository(
            dynamo_resource,
            skip_table_create=dynamodb_skip_table_create,
            partition_prefix=dynamodb_partition_prefix,
            **kwargs,
        )
        await repository.initialize()
        yield repository
