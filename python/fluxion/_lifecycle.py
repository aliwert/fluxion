"""Internal coroutine helpers for the Engine async lifecycle API."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import Engine


async def _aclose(engine: Engine) -> None:
    engine._ensure_async_close_allowed()
    await asyncio.to_thread(engine.close)


async def _aenter(engine: Engine) -> Engine:
    engine.__enter__()
    return engine


async def _aexit(engine: Engine) -> bool:
    engine._ensure_async_close_allowed()
    await asyncio.to_thread(engine.close)
    return False
