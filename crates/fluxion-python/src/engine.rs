use std::sync::{Mutex, MutexGuard};

use fluxion_core::{Engine, EngineConfig, EngineError};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::error::engine_error;
use crate::task::{CallbackFailure, PyTaskHandle, PythonTaskResult};

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

    fn shutdown_inner(&self, py: Python<'_>) -> Result<(), EngineError> {
        let engine = {
            let mut inner = self.lock_inner();

            if inner
                .as_ref()
                .is_some_and(|engine| engine.is_worker_thread())
            {
                return Err(EngineError::ShutdownFromWorker);
            }

            inner.take().ok_or(EngineError::AlreadyShutdown)?
        };

        let (result, engine) = py.detach(move || {
            let mut engine = engine;
            let result = engine.shutdown();
            (result, engine)
        });

        if matches!(&result, Err(EngineError::ShutdownFromWorker)) {
            let mut inner = self.lock_inner();
            debug_assert!(inner.is_none());
            *inner = Some(engine);
        }

        result
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

        let callback_failure = CallbackFailure::new();
        let task_failure = callback_failure.clone();
        let handle = engine
            .submit(move || -> PythonTaskResult {
                let result = match Python::try_attach(move |py| {
                    let result = callable.call0(py);
                    drop(callable);
                    result
                }) {
                    Some(result) => result,
                    None => Err(PyRuntimeError::new_err(
                        "Python interpreter is not available to execute callback",
                    )),
                };

                if result.is_err() {
                    task_failure.mark();
                }

                result
            })
            .map_err(engine_error)?;

        Ok(PyTaskHandle::new(handle, callback_failure))
    }

    /// Enters the engine context manager and returns the engine.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Exits the engine context and shuts the engine down.
    ///
    /// Accepted tasks are drained by the normal shutdown path. If shutdown
    /// fails, the shutdown error is raised. When the with-block is already
    /// propagating an exception, that exception is chained as the shutdown
    /// error's ``__context__``, so shutdown errors take precedence.
    ///
    /// If ``shutdown`` was already called manually and the block exits
    /// normally, the duplicate-shutdown condition is treated as success. If
    /// the block is already raising, shutdown errors still take precedence and
    /// a duplicate shutdown is not ignored. Other shutdown failures are never
    /// suppressed.
    fn __exit__(
        &self,
        py: Python<'_>,
        exception_type: Option<&Bound<'_, PyAny>>,
        _exception_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        match self.shutdown_inner(py) {
            Ok(()) => Ok(false),
            Err(EngineError::AlreadyShutdown) if exception_type.is_none() => Ok(false),
            Err(error) => Err(engine_error(error)),
        }
    }

    /// Shuts the engine down gracefully and waits for accepted tasks.
    ///
    /// The GIL is released while waiting so pending Python callbacks can
    /// complete. Results remain available through existing task handles.
    /// Calling `shutdown` again raises `RuntimeError`.
    ///
    /// Calling `shutdown` from one of this engine's worker callbacks raises
    /// ``RuntimeError`` without closing the queue or changing engine state.
    /// The engine remains usable and shutdown must be requested from another
    /// thread.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        self.shutdown_inner(py).map_err(engine_error)
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
