import contextlib
import datetime
import time
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

from pyncette.model import Context
from pyncette.model import Lease
from pyncette.model import PollResponse
from pyncette.model import QueryResponse
from pyncette.repository import Repository
from pyncette.repository import RepositoryFactory
from pyncette.task import Task


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
        before_time = time.time()
        try:
            yield
        except Exception as e:
            self.exceptions.labels(**labels, exception_type=type(e).__name__).inc()
            raise e from None
        finally:
            self.requests_in_progress.labels(**labels).dec()
            self.requests_duration.labels(**labels).observe(time.time() - before_time)


class MeteredRepository(Repository):
    """A wrapper for repository that exposes metrics to Prometheus"""

    def __init__(self, metric_set: OperationMetricSet, inner_repository: Repository):
        self._metric_set = metric_set
        self._inner = inner_repository

    async def query_task(self, utc_now: datetime.datetime, task: Task) -> QueryResponse:
        """Queries the dynamic tasks for execution"""
        async with self._metric_set.measure(
            operation="query_task", task_name=task.name
        ):
            return await self._inner.query_task(utc_now, task)

    async def register_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Registers a dynamic task"""
        async with self._metric_set.measure(
            operation="register_task", task_name=task.name
        ):
            return await self._inner.register_task(utc_now, task)

    async def unregister_task(self, utc_now: datetime.datetime, task: Task) -> None:
        """Deregisters a dynamic task implementation"""
        async with self._metric_set.measure(
            operation="unregister_task", task_name=task.name
        ):
            return await self._inner.unregister_task(utc_now, task)

    async def poll_task(self, utc_now: datetime.datetime, task: Task) -> PollResponse:
        """Polls the task to determine whether it is ready for execution"""
        async with self._metric_set.measure(operation="poll_task", task_name=task.name):
            return await self._inner.poll_task(utc_now, task)

    async def commit_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Commits the task, which signals a successful run."""
        async with self._metric_set.measure(
            operation="commit_task", task_name=task.name
        ):
            return await self._inner.commit_task(utc_now, task, lease)

    async def unlock_task(
        self, utc_now: datetime.datetime, task: Task, lease: Lease
    ) -> None:
        """Unlocks the task, making it eligible for retries in case execution failed."""
        async with self._metric_set.measure(
            operation="unlock_task", task_name=task.name
        ):
            return await self._inner.unlock_task(utc_now, task, lease)


_task_metric_set = OperationMetricSet("tasks", ["task_name"])


async def prometheus_middleware(
    context: Context, next: Callable[[], Awaitable[None]]
) -> None:
    """Middleware that exposes task execution metrics to Prometheus"""
    async with _task_metric_set.measure(task_name=context.task.name):
        await next()


_repository_metric_set = OperationMetricSet(
    "repository_ops", ["operation", "task_name"]
)


def prometheus_repository(repository_factory: RepositoryFactory) -> RepositoryFactory:
    """Wraps the repository factory into one that exposes the metrics via Prometheus"""

    @contextlib.asynccontextmanager
    async def _repository_factory(**kwargs: Any) -> AsyncIterator[MeteredRepository]:
        """Factory context manager for Redis repository that initializes the connection to Redis"""
        async with repository_factory(**kwargs) as inner_repository:
            yield MeteredRepository(_repository_metric_set, inner_repository)

    return _repository_factory
