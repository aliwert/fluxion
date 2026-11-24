"""Trusted native operations for Fluxion.

Operations in this module are implemented in Rust and execute on Fluxion's
worker threads without holding the GIL. They are opaque descriptors: they
cannot be constructed, subclassed, or used to register arbitrary native code.

Example:
    from fluxion import Engine
    from fluxion.native import sum_squares

    with Engine(worker_count=4) as engine:
        result = engine.submit_native(sum_squares, range(10)).result()
        assert result == 285
"""

from ._core import NativeOperation, sum_squares

__all__ = ["NativeOperation", "sum_squares"]
