use fluxion_core::{EngineError, TaskError};
use pyo3::PyErr;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyType};

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

pub(crate) fn task_error(py: Python<'_>, error: TaskError) -> PyErr {
    match error {
        TaskError::Cancelled => cancelled_error(py),
        TaskError::Panicked | TaskError::WorkerDisconnected => {
            PyRuntimeError::new_err(error.to_string())
        }
    }
}

pub(crate) fn timeout_error(py: Python<'_>) -> PyErr {
    futures_exception(py, "TimeoutError", "task wait timed out")
}

pub(crate) fn cancelled_error(py: Python<'_>) -> PyErr {
    futures_exception(py, "CancelledError", "task was cancelled")
}

fn futures_exception(py: Python<'_>, name: &'static str, message: &'static str) -> PyErr {
    let result = (|| -> PyResult<PyErr> {
        let module = py.import("concurrent.futures")?;
        let exception_type = module.getattr(name)?.cast_into::<PyType>()?;
        Ok(PyErr::from_type(exception_type, (message,)))
    })();

    result.unwrap_or_else(|error| {
        PyRuntimeError::new_err(format!(
            "failed to construct concurrent.futures.{name}: {error}"
        ))
    })
}
