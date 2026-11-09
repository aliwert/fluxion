from enum import Enum

class TaskStatus(str, Enum):
    def __new__(cls, value: str) -> TaskStatus: ...
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    def __str__(self) -> str: ...

class EngineState(str, Enum):
    def __new__(cls, value: str) -> EngineState: ...
    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"
    def __str__(self) -> str: ...

__all__: list[str]
