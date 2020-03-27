import contextlib
import datetime
import math
import time
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

from .model import Context
from .model import Lease
from .model import PollResponse
from .model import QueryResponse
from .pyncette import _current_time
from .repository import Repository
from .repository import RepositoryFactory
from .task import Task

TASK_LABELS = ["task_name"]


def _get_task_labels(task: Task) -> Dict[str, str]:
    # Instances of dynamic tasks can have high cardinality, so we choose the task template name
    return {"task_name": task.parent_task.name if task.parent_task else task.name}


class OperationMetricSet:
    """Collection of Prometheus metrics representing a logical operation"""

    requests: Counter
    requests_duration: Histogram
    exceptions: Counter
    requests_in_progress: Gauge

    def __init__(self, operation_name: str, labels: List[str]):
        self.requests = Counter(
            f"pyncette_{operation_name}_total",
            f"Total count of {operation_name} operations",
            labels,
        )
        self.requests_duration = Histogram(
            f"pyncette_{operation_name}_duration_seconds",
            f"Histogram of {operation_name} processing time",
            labels,
        )
        self.exceptions = Counter(
            f"pyncette_{operation_name}_failures_total",
            f"Total count of failed {operation_name} failures",
            [*labels, "exception_type"],
        )
        self.requests_in_progress = Gauge(
            f"pyncette_{operation_name}_in_progress",
            f"Gauge of {operation_name} operations currently being processed",
            labels,
        )

    @contextlib.asynccontextmanager
    async def measure(self, **labels: Dict[str, str]) -> AsyncIterator[None]:
        """An async context manager that measures the execution of the wrapped code"""

        self.requests_in_progress.labels(**labels).inc()
        self.requests.labels(**labels).inc()
        before_time = time.perf_counter()
        try:
            yield
        except Exception as e:
            self.exceptions.labels(**labels, exception_type=type(e).__name__).inc()
            raise e from None
        finally:
            self.requests_duration.labels(**labels).observe(
                time.perf_counter() - before_time
            )
            self.requests_in_progress.labels(**labels).dec()


class MeteredRepository(Repository):
    """A wrapper for repository that exposes metrics to Prometheus"""

    def __init__(self, metric_set: OperationMetricSet, inner_repository: Repository):
        self._metric_set = metric_set
        self._inner = inner_repository

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> QueryResponse:
        """Queries the dynamic tasks for execution"""
        async with self._metric_set.measure(
            operation="query_task", **_get_task_labels(task)
        ):
            return await self._inner.query_task(utc_now, task)

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Registers a dynamic task"""
        async with self._metric_set.measure(
            operation="register_task", **_get_task_labels(task)
        ):
            return await self._inner.register_task(utc_now, task)

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Deregisters a dynamic task implementation"""
        async with self._metric_set.measure(
            operation="unregister_task", **_get_task_labels(task)
        ):
            return await self._inner.unregister_task(utc_now, task)

    async def poll_task(
        self, utc_now: datetime.datetime, task: Task, lease: Optional[Lease] = None
    ) -> PollResponse:
        """Polls the task to determine whether it is ready for execution"""
        async with self._metric_set.measure(
            operation="poll_task", **_get_task_labels(task)
        ):
            return await self._inner.poll_task(utc_now, task, lease)

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Commits the task, which signals a successful run."""
        async with self._metric_set.measure(
            operation="commit_task", **_get_task_labels(task)
        ):
            return await self._inner.commit_task(utc_now, task, lease)

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Unlocks the task, making it eligible for retries in case execution failed."""
        async with self._metric_set.measure(
            operation="unlock_task", **_get_task_labels(task)
        ):
            return await self._inner.unlock_task(utc_now, task, lease)


_task_metric_set = OperationMetricSet("tasks", TASK_LABELS)
_task_staleness = Histogram(
    f"pyncette_tasks_staleness_seconds",
    f"Histogram of staleness of task executions (difference between scheduled and actual time)",
    TASK_LABELS,
    buckets=(
        0.05,
        0.1,
        0.25,
        0.5,
        0.75,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
        25.0,
        50.0,
        75.0,
        100.0,
        250.0,
        500.0,
        750.0,
        1000.0,
        math.inf,
    ),
)


async def prometheus_middleware(
    context: Context, next: Callable[[], Awaitable[None]]
) -> None:
    """Middleware that exposes task execution metrics to Prometheus"""
    labels = _get_task_labels(context.task)
    staleness = _current_time() - context.scheduled_at
    _task_staleness.labels(**labels).observe(staleness.total_seconds())
    async with _task_metric_set.measure(**labels):
        await next()


_repository_metric_set = OperationMetricSet(
    "repository_ops", ["operation", *TASK_LABELS]
)


def prometheus_repository(repository_factory: RepositoryFactory) -> RepositoryFactory:
    """Wraps the repository factory into one that exposes the metrics via Prometheus"""

    @contextlib.asynccontextmanager
    async def _repository_factory(**kwargs: Any) -> AsyncIterator[MeteredRepository]:
        async with repository_factory(**kwargs) as inner_repository:
            yield MeteredRepository(_repository_metric_set, inner_repository)

    return _repository_factory
