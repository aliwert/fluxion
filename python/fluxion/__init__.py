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

Example:
    from fluxion import Engine

    with Engine(worker_count=4, queue_capacity=1024) as engine:
        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42

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
