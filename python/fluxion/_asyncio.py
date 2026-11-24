"""Internal asyncio helpers for Fluxion."""

import asyncio
from collections.abc import Callable
from typing import Any


class _FluxionFuture(asyncio.Future[Any]):
    """Future that forwards cancellation to an accepted Fluxion task."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        cancel: Callable[[], Any] | None,
    ) -> None:
        super().__init__(loop=loop)
        self._fluxion_cancel: Callable[[], Any] | None = cancel

    def cancel(self, *args: Any, **kwargs: Any) -> bool:
        cancel = self._fluxion_cancel
        cancelled = super().cancel(*args, **kwargs)
        if cancelled:
            self._fluxion_cancel = None
            if cancel is not None:
                cancel()
        elif self.done():
            self._fluxion_cancel = None
        return cancelled
