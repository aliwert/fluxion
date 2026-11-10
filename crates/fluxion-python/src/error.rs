use fluxion_core::{EngineError, TaskError};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyType};

pub(crate) fn engine_error(py: Python<'_>, error: EngineError) -> PyErr {
    let message = error.to_string();

    match error {
        EngineError::InvalidConfiguration { .. } => PyValueError::new_err(message),
        EngineError::AlreadyShutdown => fluxion_exception(py, "EngineClosedError", &message),
        EngineError::QueueFull => fluxion_exception(py, "QueueFullError", &message),
        EngineError::ShutdownFromWorker
        | EngineError::WorkerSpawnFailed { .. }
        | EngineError::WorkerFailed { .. } => fluxion_exception(py, "EngineError", &message),
    }
}

pub(crate) fn task_error(py: Python<'_>, error: TaskError) -> PyErr {
    let message = error.to_string();

    match error {
        TaskError::Cancelled => fluxion_exception(py, "TaskCancelledError", &message),
        TaskError::Panicked => fluxion_exception(py, "TaskPanicError", &message),
        TaskError::WorkerDisconnected => fluxion_exception(py, "WorkerDisconnectedError", &message),
    }
}

pub(crate) fn timeout_error(py: Python<'_>) -> PyErr {
    fluxion_exception(py, "TaskTimeoutError", "task wait timed out")
}

pub(crate) fn forked_process_error(py: Python<'_>) -> PyErr {
    fluxion_exception(
        py,
        "ForkedProcessError",
        "engine cannot be used from a forked process; create a new engine in the child process",
    )
}

fn fluxion_exception(py: Python<'_>, name: &str, message: &str) -> PyErr {
    let result = (|| -> PyResult<PyErr> {
        let module = py.import("fluxion.errors")?;
        let exception_type = module.getattr(name)?.cast_into::<PyType>()?;
        Ok(PyErr::from_type(exception_type, (message.to_owned(),)))
    })();

    result.unwrap_or_else(|error| {
        PyRuntimeError::new_err(format!(
            "{message} (failed to construct fluxion.errors.{name}: {error})"
        ))
    })
}
