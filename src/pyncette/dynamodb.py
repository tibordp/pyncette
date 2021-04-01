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

import aioboto3
from boto3.dynamodb.conditions import Key

from pyncette.errors import PyncetteException
from pyncette.model import Lease
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


@dataclass
class _DynamoDBLease:
    pass


class DynamoDBRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _dynamo_resource: Any
    _table_name: str
    _batch_size: int
    _skip_table_create: bool

    def __init__(self, dynamo_resource: Any, skip_table_create: bool, **kwargs: Any):
        self._dynamo_resource = dynamo_resource
        self._table_name = kwargs.get("dynamodb_table_name", "")
        self._batch_size = kwargs.get("batch_size", 100)
        self._skip_table_create = skip_table_create

        if self._batch_size < 1:
            raise ValueError("Batch size must be greater than 0")

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
            task,
            "REGISTER",
            execute_after.isoformat(),
            json.dumps(task.as_spec()),
        )

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        await self._manage_record(task, "UNREGISTER")

    def _get_partition_id(self, task: Task):
        if task.parent_task:
            return task.parent_task.canonical_name
        else:
            return task.canonical_name

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None
    ) -> PollResponse:
        record = await _table.get_item(
            Key={
                "partition_id": _get_partition_id(task),
                "task_id": task.canonical_name,
            }
        )

        if "Item" not in records:
            if task.parent_task is not None:
                raise PyncetteException("Task not found")

            execute_after = task.get_next_execution(utc_now, None)
            locked_until = None
            locked_by = None
            update = True
            version = 0
        else:
            record = records["Item"]
            execute_after = record["execute_after"]
            locked_until = record["locked_until"]
            locked_by = record["locked_by"]
            version = record["version"]

        assert execute_after is not None
        scheduled_at = execute_after

        if locked_until is not None and locked_until > utc_now and (lease != locked_by):
            result = ResultType.LOCKED
        elif (
            execute_after <= utc_now
            and task.execution_mode == ExecutionMode.AT_MOST_ONCE
        ):
            execute_after = task.get_next_execution(utc_now, execute_after)
            result = ResultType.READY
            locked_until = None
            locked_by = None
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
                connection,
                task,
                locked_until,
                locked_by,
                execute_after,
            )

        return PollResponse(result=result, scheduled_at=scheduled_at, lease=locked_by)

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

    async def extend_lease(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> Optional[Lease]:
        assert isinstance(lease, _ManageScriptResponse)
        new_locked_until = utc_now + task.lease_duration
        response = await self._manage_record(
            task, "EXTEND", lease.version, lease.locked_by, new_locked_until.isoformat()
        )
        task._last_lease = response  # type: ignore

        if response.result == ResultType.READY:
            return Lease(response)
        else:
            return None

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

    async def initialize(self):
        if not self._skip_table_create:
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
                        "IndexName": "execute_at",
                        "KeySchema": [
                            {"AttributeName": "partition_id", "KeyType": "HASH"},
                            {"AttributeName": "ready_at", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 3, "WriteCapacityUnits": 3},
            )
            logger.info("Waiting until table is created...")
            await self._table.wait_until_exists()
        else:
            self._table = await self._dynamo_resource.Table(self._table_name)


@contextlib.asynccontextmanager
async def dynamodb_repository(
    dynamodb_endpoint: Optional[str] = None,
    dynamodb_region_name: Optional[str] = None,
    dynamodb_skip_table_create: bool = False,
    **kwargs: Any,
) -> AsyncIterator[RedisRepository]:
    """Factory context manager for Redis repository that initializes the connection to Redis"""
    async with aioboto3.resource(
        "dynamodb",
        region_name=dynamodb_region_name,
        endpoint_url=dynamodb_endpoint,
    ) as dynamo_resource:
        repository = DynamoDBRepository(
            dynamo_resource, skip_table_create=dynamodb_skip_table_create, **kwargs
        )
        await repository.initialize()
        yield repository
