from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Callable, Coroutine, Generic, TypeVar, final

from .errors import (
    EngineClosedError as EngineClosedError,
    EngineError as EngineError,
    FluxionError as FluxionError,
    ForkedProcessError as ForkedProcessError,
    QueueFullError as QueueFullError,
    TaskCancelledError as TaskCancelledError,
    TaskError as TaskError,
    TaskPanicError as TaskPanicError,
    TaskTimeoutError as TaskTimeoutError,
    WorkerDisconnectedError as WorkerDisconnectedError,
)
from .status import EngineState as EngineState, TaskStatus as TaskStatus

__version__: str

_T = TypeVar("_T")

@final
class Engine:
    def __new__(
        cls,
        worker_count: int | None = None,
        queue_capacity: int | None = None,
    ) -> Engine: ...
    @property
    def worker_count(self) -> int: ...
    @property
    def queue_capacity(self) -> int: ...
    @property
    def state(self) -> EngineState: ...
    @property
    def closed(self) -> bool: ...
    def submit(self, callable: Callable[[], _T]) -> TaskHandle[_T]: ...
    def submit_async(self, callable: Callable[[], _T]) -> asyncio.Future[_T]: ...
    def close(self) -> None: ...
    def shutdown(self) -> None: ...
    def aclose(self) -> Coroutine[Any, Any, None]: ...
    def __enter__(self) -> Engine: ...
    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool: ...
    def __aenter__(self) -> Coroutine[Any, Any, Engine]: ...
    def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Coroutine[Any, Any, bool]: ...
    def __repr__(self) -> str: ...

@final
class TaskHandle(Generic[_T]):
    @property
    def status(self) -> TaskStatus: ...
    def done(self) -> bool: ...
    def cancelled(self) -> bool: ...
    def cancel(self) -> bool: ...
    def wait(self, timeout: float | None = None) -> _T: ...
    def __repr__(self) -> str: ...

__all__: list[str]
