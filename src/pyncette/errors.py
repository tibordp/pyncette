from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyncette.task import Task


class PyncetteException(Exception):
    """Base exception for Pyncette"""


class TaskNotFoundException(PyncetteException):
    def __init__(self, task: Task):
        super().__init__("")
