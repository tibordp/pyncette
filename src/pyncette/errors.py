from __future__ import annotations

from typing import TYPE_CHECKING


class PyncetteException(Exception):
    """Base exception for Pyncette"""


class LeaseLostException(PyncetteException):
    """Signals that the lease on the task was lost"""

    task: Task

    def __init__(self, task: Task):
        super(LeaseLostException, self).__init__(
            f"Lease on the task {task.name} was lost."
        )
        self.task = task


if TYPE_CHECKING:
    from pyncette.task import Task
