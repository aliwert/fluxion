import asyncio
from collections.abc import Callable, Coroutine, Iterable
from types import TracebackType
from typing import Any, Generic, ParamSpec, TypeVar, final

from ._core import NativeOperation
from .errors import (
    EngineClosedError as EngineClosedError,
)
from .errors import (
    EngineError as EngineError,
)
from .errors import (
    FluxionError as FluxionError,
)
from .errors import (
    ForkedProcessError as ForkedProcessError,
)
from .errors import (
    NativeExecutionError as NativeExecutionError,
)
from .errors import (
    QueueFullError as QueueFullError,
)
from .errors import (
    TaskCancelledError as TaskCancelledError,
)
from .errors import (
    TaskError as TaskError,
)
from .errors import (
    TaskPanicError as TaskPanicError,
)
from .errors import (
    TaskTimeoutError as TaskTimeoutError,
)
from .errors import (
    WorkerDisconnectedError as WorkerDisconnectedError,
)
from .status import EngineState as EngineState
from .status import TaskStatus as TaskStatus

__version__: str

_P = ParamSpec("_P")
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
    def submit(
        self,
        callable: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> TaskHandle[_T]: ...
    def submit_native(
        self,
        operation: NativeOperation,
        values: Iterable[int],
    ) -> TaskHandle[int]: ...
    def submit_async(
        self,
        callable: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> asyncio.Future[_T]: ...
    def close(self) -> None: ...
    def shutdown(self) -> None: ...
    def _ensure_async_close_allowed(self) -> None: ...
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

@final
class TaskHandle(Generic[_T]):
    @classmethod
    def __class_getitem__(cls, key: Any) -> Any: ...
    @property
    def status(self) -> TaskStatus: ...
    def done(self) -> bool: ...
    def cancelled(self) -> bool: ...
    def running(self) -> bool: ...
    def cancel(self) -> bool: ...
    def result(self, timeout: float | None = None) -> _T: ...
    def exception(self, timeout: float | None = None) -> BaseException | None: ...
    def wait(self, timeout: float | None = None) -> _T: ...

__all__ = [
    "Engine",
    "EngineClosedError",
    "EngineError",
    "EngineState",
    "FluxionError",
    "ForkedProcessError",
    "NativeExecutionError",
    "QueueFullError",
    "TaskCancelledError",
    "TaskError",
    "TaskHandle",
    "TaskPanicError",
    "TaskStatus",
    "TaskTimeoutError",
    "WorkerDisconnectedError",
    "__version__",
]
