use std::sync::{Mutex, MutexGuard};

use fluxion_core::{Engine, EngineConfig};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::error::engine_error;
use crate::task::{PyTaskHandle, PythonTaskResult};

/// Runs Python callables on Fluxion's Rust-managed worker threads.
///
/// Python callbacks require interpreter access and run while holding the GIL.
/// Under standard CPython, CPU-bound Python callbacks are therefore serialized
/// and do not execute in parallel. Fluxion's Rust core can execute native work
/// in parallel, but this binding exposes Python callbacks only.
#[pyclass(name = "Engine", module = "fluxion._core")]
pub(crate) struct PyEngine {
    config: EngineConfig,
    inner: Mutex<Option<Engine>>,
}

impl PyEngine {
    fn lock_inner(&self) -> MutexGuard<'_, Option<Engine>> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[pymethods]
impl PyEngine {
    /// Creates an engine with the supplied worker count and injection-queue
    /// capacity.
    ///
    /// Omitted values use the Rust core defaults. Invalid values raise
    /// `ValueError`.
    #[new]
    #[pyo3(signature = (worker_count=None, queue_capacity=None))]
    fn new(worker_count: Option<usize>, queue_capacity: Option<usize>) -> PyResult<Self> {
        let defaults = EngineConfig::default();
        let config = EngineConfig::new(
            worker_count.unwrap_or(defaults.worker_count),
            queue_capacity.unwrap_or(defaults.queue_capacity),
        )
        .map_err(engine_error)?;
        let engine = Engine::new(config.clone()).map_err(engine_error)?;

        Ok(Self {
            config,
            inner: Mutex::new(Some(engine)),
        })
    }

    /// Number of worker threads configured for this engine.
    #[getter]
    fn worker_count(&self) -> usize {
        self.config.worker_count
    }

    /// Capacity of the shared injection queue.
    ///
    /// Workers may additionally hold a small fixed batch of prefetched tasks.
    #[getter]
    fn queue_capacity(&self) -> usize {
        self.config.queue_capacity
    }

    /// Submits a zero-argument Python callable and returns a task handle.
    ///
    /// The callable executes on a Fluxion worker thread. It is called while
    /// attached to the Python interpreter and holding the GIL. A non-callable
    /// argument raises `TypeError`; submitting after shutdown or while the
    /// queue is full raises `RuntimeError`. Exceptions raised by the callback
    /// are preserved and re-raised by `TaskHandle.wait`.
    fn submit(&self, py: Python<'_>, callable: Py<PyAny>) -> PyResult<PyTaskHandle> {
        if !callable.bind(py).is_callable() {
            return Err(PyTypeError::new_err("submitted object is not callable"));
        }

        let inner = self.lock_inner();
        let engine = inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("engine has already been shut down"))?;

        let handle = engine
            .submit(move || -> PythonTaskResult {
                match Python::try_attach(|py| callable.call0(py)) {
                    Some(result) => result,
                    None => Err(PyRuntimeError::new_err(
                        "Python interpreter is not available to execute callback",
                    )),
                }
            })
            .map_err(engine_error)?;

        Ok(PyTaskHandle::new(handle))
    }

    /// Shuts the engine down gracefully and waits for accepted tasks.
    ///
    /// The GIL is released while waiting so pending Python callbacks can
    /// complete. Results remain available through existing task handles.
    /// Calling `shutdown` again raises `RuntimeError`. Do not call `shutdown`
    /// from inside a callback submitted to this engine.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let mut engine = {
            let mut inner = self.lock_inner();
            inner
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("engine has already been shut down"))?
        };

        py.detach(move || engine.shutdown()).map_err(engine_error)
    }
}

impl Drop for PyEngine {
    fn drop(&mut self) {
        let engine = self.lock_inner().take();

        if let Some(engine) = engine {
            Python::try_attach(|py| py.detach(move || drop(engine)));
        }
    }
}
