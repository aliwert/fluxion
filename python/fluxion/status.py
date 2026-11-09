"""Typed lifecycle states exposed by Fluxion."""

from __future__ import annotations

from enum import Enum


class TaskStatus(str, Enum):
    """Lifecycle state of a submitted task.

    Members are ``str`` subclasses, so comparisons such as
    ``handle.status == "completed"`` continue to work. Use the enum members for
    type-safe comparisons and IDE discovery.
    """

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    def __str__(self) -> str:
        return self.value


class EngineState(str, Enum):
    """Lifecycle state of an :class:`fluxion.Engine`."""

    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"

    def __str__(self) -> str:
        return self.value


__all__ = ["EngineState", "TaskStatus"]
