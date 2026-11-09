"""Public exception hierarchy for Fluxion.

The hierarchy keeps compatibility with the built-in exceptions used by earlier
releases: engine errors remain catchable as :class:`RuntimeError`, task panics
and worker disconnections remain catchable as :class:`RuntimeError`, sync task
timeouts remain catchable as :class:`TimeoutError` and
:class:`concurrent.futures.TimeoutError`, and sync task cancellation remains
catchable as :class:`concurrent.futures.CancelledError`.
"""

from __future__ import annotations

import concurrent.futures


class FluxionError(Exception):
    """Base class for errors raised by Fluxion itself."""


class EngineError(FluxionError, RuntimeError):
    """Base class for engine-level failures."""


class EngineClosedError(EngineError):
    """The engine has already been closed."""


class QueueFullError(EngineError):
    """The engine's shared submission queue is full."""


class ForkedProcessError(EngineError):
    """The engine or task handle belongs to another process.

    Fluxion engines contain native worker threads and cannot be used after a
    ``fork()``. Create a new engine in the child process instead.
    """


class TaskError(FluxionError):
    """Base class for failures produced by a Fluxion task."""


class TaskCancelledError(TaskError, concurrent.futures.CancelledError):
    """The task was cancelled before it started running."""


if concurrent.futures.TimeoutError is TimeoutError:

    class TaskTimeoutError(TaskError, TimeoutError):
        """The task did not complete before the requested timeout."""

else:

    class TaskTimeoutError(TaskError, TimeoutError, concurrent.futures.TimeoutError):
        """The task did not complete before the requested timeout.

        On Python 3.10, ``concurrent.futures.TimeoutError`` is not an alias of
        the built-in ``TimeoutError``. This class inherits both so existing
        handlers continue to work across supported Python versions.
        """


class TaskPanicError(TaskError, RuntimeError):
    """The task panicked while running."""


class WorkerDisconnectedError(TaskError, RuntimeError):
    """A worker exited before the task produced a result."""


__all__ = [
    "FluxionError",
    "EngineError",
    "EngineClosedError",
    "QueueFullError",
    "ForkedProcessError",
    "TaskError",
    "TaskCancelledError",
    "TaskTimeoutError",
    "TaskPanicError",
    "WorkerDisconnectedError",
]
