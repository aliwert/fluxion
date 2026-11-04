use fluxion_core::{EngineError, TaskError};
use pyo3::PyErr;
use pyo3::exceptions::{PyRuntimeError, PyValueError};

pub(crate) fn engine_error(error: EngineError) -> PyErr {
    let message = error.to_string();

    match error {
        EngineError::InvalidConfiguration { .. } => PyValueError::new_err(message),
        EngineError::AlreadyShutdown
        | EngineError::QueueFull
        | EngineError::WorkerSpawnFailed { .. }
        | EngineError::WorkerFailed { .. } => PyRuntimeError::new_err(message),
    }
}

pub(crate) fn task_error(error: TaskError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
