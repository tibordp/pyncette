from __future__ import annotations

from typing import TYPE_CHECKING


class PyncetteException(Exception):
    """Base exception for Pyncette"""


class LeaseLostException(PyncetteException):
    """Signals that the lease on the task was lost"""

    task: Task

    def __init__(self, task: Task):
        super().__init__(f"Lease on the task {task.canonical_name} was lost.")
        self.task = task


class TaskLockedException(PyncetteException):
    """Signals that a task is currently locked and cannot be updated"""

    task: Task

    def __init__(self, task: Task, locked_until: datetime.datetime | None = None):
        if locked_until:
            super().__init__(
                f"Cannot update task {task.canonical_name} while it is locked. "
                f"Task is locked until {locked_until}. Use force=True to override."
            )
        else:
            super().__init__(f"Cannot update task {task.canonical_name} while it is locked. Use force=True to override.")
        self.task = task


if TYPE_CHECKING:
    import datetime

    from pyncette.task import Task
