from __future__ import annotations

import contextlib
import datetime
import logging
from typing import Any
from collections.abc import AsyncIterator

from pyncette.model import ContinuationToken
from pyncette.model import Lease
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.model import ResultType
from pyncette.repository import Repository
from pyncette.task import Task

logger = logging.getLogger(__name__)


_LEASE = Lease(object())
_TASK_SPEC = {
    "name": "fake",
    "interval": None,
    "timezone": None,
    "execute_at": None,
    "extra_args": {},
    "schedule": "* * * * * *",
}


class FakeRepository(Repository):
    """Redis-backed store for Pyncete task execution data"""

    _batch_size: int
    _records_per_tick: int

    def __init__(self, batch_size: int, records_per_tick: int):
        self._batch_size = batch_size
        self._records_per_tick = records_per_tick

    async def poll_dynamic_task(
        self,
        utc_now: datetime.datetime,
        task: Task,
        continuation_token: ContinuationToken | None = None,
    ) -> QueryResponse:
        if isinstance(continuation_token, int):
            remaining = self._records_per_tick - continuation_token
        else:
            remaining = self._records_per_tick

        result_count = max(remaining, self._batch_size)
        remaining -= result_count

        return QueryResponse(
            tasks=[
                (
                    task.instantiate_from_spec(_TASK_SPEC),
                    _LEASE,
                )
                for _ in range(result_count)
            ],
            continuation_token=remaining if remaining else None,
        )

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        pass

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        pass

    async def poll_task(self, utc_now: datetime.datetime, task: Task, lease: Lease | None = None) -> PollResponse:
        return PollResponse(result=ResultType.READY, scheduled_at=utc_now, lease=_LEASE)

    async def commit_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        pass

    async def unlock_task(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> None:
        pass

    async def extend_lease(self, utc_now: datetime.datetime, task: Task, lease: Lease) -> Lease | None:
        return lease


@contextlib.asynccontextmanager
async def fake_repository(
    batch_size: int = 100,
    records_per_tick: int = 1000,
    **kwargs: Any,
) -> AsyncIterator[FakeRepository]:
    yield FakeRepository(batch_size, records_per_tick)
