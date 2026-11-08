"""Internal asyncio helpers for Fluxion."""

import asyncio


class _FluxionFuture(asyncio.Future):
    """Future that forwards cancellation to an accepted Fluxion task."""

    def __init__(self, loop, cancel):
        super().__init__(loop=loop)
        self._fluxion_cancel = cancel

    def cancel(self, *args, **kwargs):
        cancel = self._fluxion_cancel
        cancelled = super().cancel(*args, **kwargs)
        if cancelled:
            self._fluxion_cancel = None
            if cancel is not None:
                cancel()
        elif self.done():
            self._fluxion_cancel = None
        return cancelled
