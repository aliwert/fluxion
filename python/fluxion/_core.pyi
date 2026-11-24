from typing import final

from . import Engine as Engine
from . import TaskHandle as TaskHandle

__version__: str

@final
class NativeOperation: ...

sum_squares: NativeOperation

__all__ = ["Engine", "NativeOperation", "TaskHandle", "__version__", "sum_squares"]
