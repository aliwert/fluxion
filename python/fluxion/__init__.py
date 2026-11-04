"""Python interface for the Fluxion execution engine.

Fluxion submits Python callables to a Rust-managed worker pool. Callbacks run
on worker threads while attached to the Python interpreter and must acquire the
GIL, so under standard CPython CPU-bound Python callbacks are serialized and do
not execute in parallel. The Rust core can execute native workloads in
parallel, but this binding currently exposes Python callbacks only.

Example:
    from fluxion import Engine

    engine = Engine(worker_count=4, queue_capacity=1024)
    try:
        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42
    finally:
        engine.shutdown()
"""

from ._core import Engine, TaskHandle, __version__

__all__ = ["Engine", "TaskHandle", "__version__"]
