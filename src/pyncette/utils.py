import asyncio
import logging
from functools import wraps

from .errors import LeaseLostException
from .model import Context
from .model import Decorator
from .model import TaskFunc

logger = logging.getLogger(__name__)

DEFAULT_LEASE_REMAINING_RATIO = 0.5


def with_heartbeat(
    lease_remaining_ratio: float = DEFAULT_LEASE_REMAINING_RATIO,
    cancel_on_lease_lost: bool = False,
) -> Decorator[TaskFunc]:
    """
    Decorate the task to use automatic heartbeating in background.

    :param lease_remaining_ratio: Number between 0 and 1. The ratio between elapsed time and the lease duration when heartbeating will be performed. Default is 0.5.
    :param cancel_on_lease_lost: Whether the task should be cancelled if lease expires. Default is False.
    """
    if lease_remaining_ratio <= 0 or lease_remaining_ratio >= 1:
        raise ValueError("Lease remaining ratio must be in (0, 1)")

    def decorator(func: TaskFunc) -> TaskFunc:
        @wraps(func)
        async def _func(context: Context) -> None:
            body = asyncio.ensure_future(func(context))

            async def _heartbeater() -> None:
                delay_duration = (
                    context.task.lease_duration.total_seconds() * lease_remaining_ratio
                )
                while True:
                    await asyncio.sleep(delay_duration)
                    try:
                        await asyncio.shield(context.heartbeat())
                    except LeaseLostException:
                        if cancel_on_lease_lost:
                            body.cancel()
                        # Regardless of whether we want the task body to continue
                        # executing, it makes no sense to continue heartbeating
                        # since the lease has already been lost.
                        return
                    except Exception as e:
                        # There may be transient errors while heartbeating. In this case
                        # ignore them until the next heartbeat interval.
                        logger.warning(
                            f"Heartbeating on {context.task} failed", exc_info=e
                        )

            heartbeater = asyncio.create_task(_heartbeater())
            try:
                await body
            finally:
                heartbeater.cancel()
                try:
                    await heartbeater
                except asyncio.CancelledError:
                    pass

        return _func

    return decorator
