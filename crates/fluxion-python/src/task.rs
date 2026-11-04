use std::sync::{Mutex, MutexGuard};

use fluxion_core::TaskHandle;
use pyo3::PyErr;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::error::task_error;

pub(crate) type PythonTaskResult = Result<Py<PyAny>, PyErr>;

/// Handle to a task submitted to a Fluxion engine.
///
/// A handle may be waited on exactly once. Dropping a handle does not cancel
/// the task; the task still runs and its result is discarded.
#[pyclass(name = "TaskHandle", module = "fluxion._core")]
pub(crate) struct PyTaskHandle {
    inner: Mutex<Option<TaskHandle<PythonTaskResult>>>,
}

impl PyTaskHandle {
    pub(crate) fn new(handle: TaskHandle<PythonTaskResult>) -> Self {
        Self {
            inner: Mutex::new(Some(handle)),
        }
    }

    fn take_handle(&self) -> PyResult<TaskHandle<PythonTaskResult>> {
        let mut inner = self.lock_inner();
        inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("task handle has already been consumed"))
    }

    fn lock_inner(&self) -> MutexGuard<'_, Option<TaskHandle<PythonTaskResult>>> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[pymethods]
impl PyTaskHandle {
    /// Blocks until the task completes and returns its Python result.
    ///
    /// The GIL is released while waiting so Python callbacks on Fluxion worker
    /// threads can make progress. If the callback raised an exception, that
    /// same exception is re-raised here. A task panic or a disconnected worker
    /// raises `RuntimeError`. Calling `wait` a second time raises
    /// `RuntimeError`.
    fn wait(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let handle = self.take_handle()?;

        match py.detach(move || handle.wait()) {
            Ok(result) => result,
            Err(error) => Err(task_error(error)),
        }
    }
}
