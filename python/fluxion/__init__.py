"""Python interface for the Fluxion execution engine.

Fluxion submits Python callables to a Rust-managed worker pool. Callbacks run
on worker threads while attached to the Python interpreter and must acquire the
GIL, so under standard CPython CPU-bound Python callbacks are serialized and do
not execute in parallel. The Rust core can execute native workloads in
parallel, but this binding currently exposes Python callbacks only.

Task handles expose lifecycle information and support cancellation before a
task starts. `wait` blocks until a result is available, optionally with a
timeout, and re-raises exceptions from the callback. Shutdown errors raised by
`Engine.__exit__` take precedence over exceptions from the with-block and are
chained through `__context__`. Calling `shutdown()` manually inside the block
is allowed: a normal block exit ignores the duplicate shutdown, while an
exceptional exit still reports the shutdown error.

Shutdown must be requested from outside the engine's worker callbacks. Calling
`shutdown()` from a callback raises `RuntimeError` without closing the engine.
If the final `Engine` reference is released inside a callback, the remaining
workers drain accepted tasks and shut down safely.

`Engine.submit_async` is available only while an asyncio event loop is running.
It returns an `asyncio.Future` bound to that loop and must be awaited from the
same loop. Results are delivered on the loop through `call_soon_threadsafe`;
worker threads never complete futures directly. Cancelling the future before
execution starts cancels the queued Rust task, while a callback that has already
started is allowed to finish. Awaiting a cancelled future raises
`asyncio.CancelledError`.

Example:
    import asyncio
    from fluxion import Engine

    with Engine(worker_count=4, queue_capacity=1024) as engine:
        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42

        async def compute():
            future = engine.submit_async(lambda: 21 * 2)
            return await future

        assert asyncio.run(compute()) == 42

    engine = Engine()
    try:
        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42
        assert handle.status == "completed"
        assert handle.done()
    finally:
        engine.shutdown()

    engine = Engine(worker_count=1)
    try:
        handle = engine.submit(lambda: 42)
        if handle.cancel():
            assert handle.cancelled()
    finally:
        engine.shutdown()
"""

from ._core import Engine, TaskHandle, __version__

__all__ = ["Engine", "TaskHandle", "__version__"]
