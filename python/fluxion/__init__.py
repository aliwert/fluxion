"""Python interface for the Fluxion execution engine.

Fluxion submits Python callables to a Rust-managed worker pool. Callbacks run
on worker threads while attached to the Python interpreter and must acquire the
GIL, so under standard CPython CPU-bound Python callbacks are serialized and do
not execute in parallel. The Rust core can execute native workloads in
parallel, but this binding currently exposes Python callbacks only.

Task handles expose lifecycle information and support cancellation before a
task starts. ``wait`` blocks until a result is available, optionally with a
timeout, and re-raises exceptions from the callback. A handle may be waited on
once; a timed-out wait does not consume the handle, so it can be waited on
again. ``status`` is a :class:`TaskStatus` member, which also compares equal to
the corresponding lowercase string for compatibility with earlier releases.

``Engine.close`` is idempotent and drains accepted tasks. ``Engine.shutdown``
is a backward-compatible alias for ``close``. Both block until worker threads
have drained and joined; call them from outside an event-loop thread. Await
``Engine.aclose`` or use ``async with`` to drain without blocking a running
asyncio loop. Shutdown must be requested from outside the engine's worker
callbacks. Calling ``close`` or ``shutdown`` from a callback raises an
:class:`EngineError` without closing the engine. If the final ``Engine``
reference is released inside a callback, the remaining workers drain accepted
tasks and shut down safely.

``Engine.submit_async`` is available only while an asyncio event loop is running.
It accepts a synchronous callable, runs it on a Fluxion worker, and returns an
``asyncio.Future`` bound to that loop. Results are delivered on the loop through
``call_soon_threadsafe``; worker threads never complete futures directly.
Coroutine functions and callables that return awaitable objects are rejected
with ``TypeError``; use ``asyncio.create_task`` for coroutine execution.
Cancelling the future before execution starts cancels the queued Rust task,
while a callback that has already started is allowed to finish. Awaiting a
cancelled future raises ``asyncio.CancelledError``.

Engines own native worker threads and are not fork-safe. Submit, wait, and close
operations in a forked child raise :class:`ForkedProcessError`; create a new
engine in the child process instead.

Example:
    import asyncio
    from fluxion import Engine

    with Engine(worker_count=4, queue_capacity=1024) as engine:
        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42
        assert handle.status == "completed"

        async def compute():
            future = engine.submit_async(lambda: 21 * 2)
            return await future

        assert asyncio.run(compute()) == 42

    async def drain():
        engine = Engine(worker_count=1)
        try:
            assert await engine.submit_async(lambda: 42) == 42
        finally:
            await engine.aclose()

    asyncio.run(drain())

    engine = Engine(worker_count=1)
    try:
        handle = engine.submit(lambda: 42)
        if handle.cancel():
            assert handle.cancelled()
    finally:
        engine.close()
"""

from ._core import Engine, TaskHandle, __version__
from .errors import (
    EngineClosedError,
    EngineError,
    FluxionError,
    ForkedProcessError,
    QueueFullError,
    TaskCancelledError,
    TaskError,
    TaskPanicError,
    TaskTimeoutError,
    WorkerDisconnectedError,
)
from .status import EngineState, TaskStatus

__all__ = [
    "Engine",
    "TaskHandle",
    "TaskStatus",
    "EngineState",
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
    "__version__",
]
