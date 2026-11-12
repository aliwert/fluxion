use std::panic::{self, AssertUnwindSafe};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fluxion_core::{Engine, EngineConfig, EngineError};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool, PyDict, PyInt, PyTuple, PyTupleMethods};

use crate::asyncio::{AsyncCompletion, AsyncTaskHandle, cancellation_callback, new_future};
use crate::convert::engine_state;
use crate::error::{engine_error, forked_process_error};
use crate::task::{
    CallbackFailure, PyTaskHandle, PythonTaskResult, ensure_sync_callable, reject_awaitable_result,
};

const STATE_RUNNING: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;

/// Runs Python callables on Fluxion's Rust-managed worker threads.
///
/// Python callbacks require interpreter access and run while holding the GIL.
/// Under standard CPython, CPU-bound Python callbacks are therefore serialized
/// and do not execute in parallel. Fluxion's Rust core can execute native work
/// in parallel, but this binding exposes Python callbacks only.
///
/// Engines contain native worker threads and are not fork-safe. Create a new
/// engine in the child process after ``fork()``.
#[pyclass(name = "Engine", module = "fluxion")]
pub(crate) struct PyEngine {
    config: EngineConfig,
    inner: Mutex<Option<Engine>>,
    state: AtomicU8,
    owner_pid: u32,
    worker_thread_ids: Vec<std::thread::ThreadId>,
}

impl PyEngine {
    fn lock_inner(&self) -> MutexGuard<'_, Option<Engine>> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn is_forked(&self) -> bool {
        self.owner_pid != process::id()
    }

    fn ensure_process(&self, py: Python<'_>) -> PyResult<()> {
        if self.is_forked() {
            Err(forked_process_error(py))
        } else {
            Ok(())
        }
    }

    fn state_name(&self) -> &'static str {
        if self.is_forked() {
            return "closed";
        }

        match self.state.load(Ordering::Acquire) {
            STATE_RUNNING => "running",
            STATE_CLOSING => "closing",
            _ => "closed",
        }
    }

    fn ensure_running(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_process(py)?;
        if self.state.load(Ordering::Acquire) == STATE_RUNNING {
            Ok(())
        } else {
            Err(engine_error(py, EngineError::AlreadyShutdown))
        }
    }

    fn ensure_async_close_allowed(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_process(py)?;
        if self.state.load(Ordering::Acquire) != STATE_CLOSED
            && self
                .worker_thread_ids
                .contains(&std::thread::current().id())
        {
            Err(engine_error(py, EngineError::ShutdownFromWorker))
        } else {
            Ok(())
        }
    }

    fn close_inner(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_process(py)?;

        let engine = loop {
            let mut inner = self.lock_inner();

            let state = self.state.load(Ordering::Acquire);
            if state == STATE_CLOSED {
                return Ok(());
            }

            if self
                .worker_thread_ids
                .contains(&std::thread::current().id())
            {
                return Err(engine_error(py, EngineError::ShutdownFromWorker));
            }

            if state == STATE_CLOSING {
                drop(inner);
                py.detach(|| std::thread::sleep(Duration::from_millis(1)));
                continue;
            }

            self.state.store(STATE_CLOSING, Ordering::Release);

            match inner.take() {
                Some(engine) => break engine,
                None => {
                    self.state.store(STATE_CLOSED, Ordering::Release);
                    return Ok(());
                }
            }
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
            self.state.store(STATE_RUNNING, Ordering::Release);
            return Err(engine_error(py, EngineError::ShutdownFromWorker));
        }

        self.state.store(STATE_CLOSED, Ordering::Release);
        result.map_err(|error| engine_error(py, error))
    }
}

#[pymethods]
impl PyEngine {
    /// Creates an engine with the supplied worker count and injection-queue
    /// capacity.
    ///
    /// Omitted values use the Rust core defaults. ``worker_count`` and
    /// ``queue_capacity`` must be positive integers; booleans, strings, zero,
    /// negative values, and out-of-range integers raise ``ValueError`` or
    /// ``TypeError``.
    #[new]
    #[pyo3(signature = (worker_count=None, queue_capacity=None))]
    fn new(
        py: Python<'_>,
        worker_count: Option<Py<PyAny>>,
        queue_capacity: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let worker_count = parse_positive(
            worker_count.as_ref().map(|value| value.bind(py)),
            "worker_count",
        )?;
        let queue_capacity = parse_positive(
            queue_capacity.as_ref().map(|value| value.bind(py)),
            "queue_capacity",
        )?;

        let defaults = EngineConfig::default();
        let config = EngineConfig::new(
            worker_count.unwrap_or(defaults.worker_count),
            queue_capacity.unwrap_or(defaults.queue_capacity),
        )
        .map_err(|error| engine_error(py, error))?;
        let engine = Engine::new(config.clone()).map_err(|error| engine_error(py, error))?;
        let worker_thread_ids = engine.worker_thread_ids();

        Ok(Self {
            config,
            inner: Mutex::new(Some(engine)),
            state: AtomicU8::new(STATE_RUNNING),
            owner_pid: process::id(),
            worker_thread_ids,
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

    /// Current lifecycle state as a :class:`fluxion.EngineState`.
    #[getter]
    fn state(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        engine_state(py, self.state_name())
    }

    /// Whether the engine has begun or completed shutdown.
    #[getter]
    fn closed(&self) -> bool {
        self.is_forked() || self.state.load(Ordering::Acquire) != STATE_RUNNING
    }

    /// Submits a Python callable and returns a task handle.
    ///
    /// Positional and keyword arguments are forwarded to the callable using
    /// normal Python calling semantics. The callable executes on a Fluxion
    /// worker thread. It is called while attached to the Python interpreter and
    /// holding the GIL. A non-callable argument raises ``TypeError``; submitting
    /// after close or while the queue is full raises
    /// :class:`fluxion.EngineClosedError` or :class:`fluxion.QueueFullError`.
    /// Argument-binding errors raised by Python during invocation are reported
    /// through the resulting :class:`TaskHandle`. Exceptions raised by the
    /// callback are preserved and re-raised by :meth:`TaskHandle.wait`.
    #[pyo3(signature = (callable, *args, **kwargs))]
    fn submit(
        &self,
        py: Python<'_>,
        callable: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<PyTaskHandle> {
        self.ensure_running(py)?;

        if !callable.bind(py).is_callable() {
            return Err(PyTypeError::new_err("submitted object is not callable"));
        }

        let inner = self.lock_inner();
        let engine = inner
            .as_ref()
            .ok_or_else(|| engine_error(py, EngineError::AlreadyShutdown))?;

        let callback_failure = CallbackFailure::new();
        let task_failure = callback_failure.clone();
        let handle = engine
            .submit(move || -> PythonTaskResult {
                let result = match Python::try_attach(move |py| {
                    let result = invoke_python(py, &callable, &args, kwargs.as_ref());
                    drop(callable);
                    drop(args);
                    drop(kwargs);
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
            .map_err(|error| engine_error(py, error))?;

        Ok(PyTaskHandle::new(handle, callback_failure, self.owner_pid))
    }

    /// Submits a synchronous Python callable and returns an asyncio Future.
    ///
    /// Positional and keyword arguments are forwarded to the callable using
    /// normal Python calling semantics. The method must be called from a thread
    /// with a running asyncio event loop. The returned Future is bound to that
    /// loop and must be awaited from the same loop. It is completed through
    /// ``loop.call_soon_threadsafe``; worker threads never complete or cancel
    /// the Future directly.
    ///
    /// Coroutine functions and callables that return awaitable objects are
    /// rejected with ``TypeError``. Use ``asyncio.create_task`` for coroutine
    /// execution. Argument-binding errors are delivered through the Future.
    /// Cancelling the Future before execution starts cancels the queued Rust
    /// task. Once the callback has started, cancellation does not interrupt it;
    /// the Future is still cancelled normally and any later result is
    /// discarded. Callback exceptions are preserved, while task panics and
    /// worker disconnections raise :class:`fluxion.TaskPanicError` or
    /// :class:`fluxion.WorkerDisconnectedError`.
    #[pyo3(signature = (callable, *args, **kwargs))]
    fn submit_async(
        &self,
        py: Python<'_>,
        callable: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_running(py)?;

        if !callable.bind(py).is_callable() {
            return Err(PyTypeError::new_err("submitted object is not callable"));
        }
        ensure_sync_callable(py, callable.bind(py))?;

        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.getattr("get_running_loop")?.call0()?;

        let active = Arc::new(AtomicBool::new(false));
        let handle_slot: AsyncTaskHandle = Arc::new(Mutex::new(None));
        let cancellation =
            cancellation_callback(py, Arc::clone(&handle_slot), Arc::clone(&active))?;
        let future = new_future(py, &event_loop, cancellation)?;

        let mut completion = AsyncCompletion::new(
            event_loop.unbind(),
            future.clone_ref(py),
            Arc::clone(&active),
        );

        let task = move || -> PythonTaskResult {
            let completion_ref = &mut completion;
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                match Python::try_attach(move |py| {
                    let result = invoke_python(py, &callable, &args, kwargs.as_ref());
                    drop(callable);
                    drop(args);
                    drop(kwargs);

                    let result = match result {
                        Ok(value) => match reject_awaitable_result(py, value.bind(py)) {
                            Ok(()) => Ok(value),
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    };

                    completion_ref.complete(py, &result);
                    result
                }) {
                    Some(result) => result,
                    None => Err(PyRuntimeError::new_err(
                        "Python interpreter is not available to execute callback",
                    )),
                }
            }));

            match outcome {
                Ok(result) => result,
                Err(payload) => {
                    completion.panicked();
                    panic::resume_unwind(payload);
                }
            }
        };

        let inner = self.lock_inner();
        let engine = inner
            .as_ref()
            .ok_or_else(|| engine_error(py, EngineError::AlreadyShutdown))?;

        active.store(true, Ordering::Release);
        let submit_result = engine.submit(task).map_err(|error| engine_error(py, error));
        drop(inner);

        let handle = match submit_result {
            Ok(handle) => handle,
            Err(error) => {
                let _ = future.bind(py).call_method0("cancel");
                return Err(error);
            }
        };

        match handle_slot.lock() {
            Ok(mut guard) => *guard = Some(handle),
            Err(poisoned) => *poisoned.into_inner() = Some(handle),
        }

        Ok(future)
    }

    /// Enters the engine context manager and returns the engine.
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        slf.ensure_running(slf.py())?;
        Ok(slf)
    }

    /// Exits the engine context and closes the engine.
    ///
    /// Accepted tasks are drained by the normal close path. If close fails,
    /// the close error is raised. When the with-block is already propagating an
    /// exception, Python chains that exception as the close error's
    /// ``__context__``, so close errors take precedence. Closing an engine that
    /// is already closed is a no-op.
    fn __exit__(
        &self,
        py: Python<'_>,
        _exception_type: Option<&Bound<'_, PyAny>>,
        _exception_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close_inner(py)?;
        Ok(false)
    }

    /// Gracefully closes the engine and waits for accepted tasks.
    ///
    /// ``close`` is idempotent. The GIL is released while waiting so pending
    /// Python callbacks can complete. Results remain available through existing
    /// task handles.
    ///
    /// Calling ``close`` from one of this engine's worker callbacks raises
    /// :class:`fluxion.EngineError` without closing the queue or changing
    /// engine state. The engine remains usable and close must be requested from
    /// another thread.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.close_inner(py)
    }

    /// Backward-compatible alias for :meth:`close`.
    ///
    /// Like ``close``, this method is idempotent and blocks until accepted
    /// tasks have drained.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        self.close_inner(py)
    }

    #[pyo3(name = "_ensure_async_close_allowed")]
    fn check_async_close_allowed(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_async_close_allowed(py)
    }

    /// Returns a coroutine that closes the engine without blocking the event
    /// loop.
    fn aclose(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slf.ensure_async_close_allowed(py)?;
        lifecycle_coroutine(py, "_aclose", slf)
    }

    /// Enters the asynchronous context manager and returns the engine.
    fn __aenter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slf.ensure_process(py)?;
        lifecycle_coroutine(py, "_aenter", slf)
    }

    /// Exits the asynchronous context manager and closes the engine.
    fn __aexit__(
        slf: PyRef<'_, Self>,
        py: Python<'_>,
        _exception_type: Option<&Bound<'_, PyAny>>,
        _exception_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        slf.ensure_async_close_allowed(py)?;
        lifecycle_coroutine(py, "_aexit", slf)
    }

    fn __repr__(&self) -> String {
        format!(
            "Engine(worker_count={}, queue_capacity={}, state='{}')",
            self.config.worker_count,
            self.config.queue_capacity,
            self.state_name()
        )
    }
}

impl Drop for PyEngine {
    fn drop(&mut self) {
        if self.is_forked() {
            let inner = std::mem::replace(&mut self.inner, Mutex::new(None));
            std::mem::forget(inner);
            return;
        }

        let engine = self.lock_inner().take();

        if let Some(engine) = engine {
            Python::try_attach(|py| py.detach(move || drop(engine)));
        }
    }
}

fn invoke_python(
    py: Python<'_>,
    callable: &Py<PyAny>,
    args: &Py<PyTuple>,
    kwargs: Option<&Py<PyDict>>,
) -> PyResult<Py<PyAny>> {
    let callable = callable.bind(py);
    let args = args.bind(py);
    let kwargs = kwargs.map(|kwargs| kwargs.bind(py));

    let result = if args.is_empty() && kwargs.is_none() {
        callable.call0()
    } else {
        callable.call(args, kwargs)
    };

    result.map(|value| value.unbind())
}

fn lifecycle_coroutine(
    py: Python<'_>,
    name: &str,
    engine: PyRef<'_, PyEngine>,
) -> PyResult<Py<PyAny>> {
    let module = py.import("fluxion._lifecycle")?;
    let engine: Py<PyEngine> = engine.into();
    Ok(module.getattr(name)?.call1((engine,))?.unbind())
}

fn parse_positive(value: Option<&Bound<'_, PyAny>>, field: &str) -> PyResult<Option<usize>> {
    let Some(value) = value else {
        return Ok(None);
    };

    if value.is_none() {
        return Ok(None);
    }

    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err(format!(
            "{field} must be a positive integer, not bool"
        )));
    }

    if !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(format!(
            "{field} must be an integer or None"
        )));
    }

    let negative = value
        .call_method1("__lt__", (0,))?
        .extract::<bool>()
        .unwrap_or(false);
    if negative {
        return Err(PyValueError::new_err(format!(
            "{field} must be greater than zero"
        )));
    }

    let parsed = value
        .extract::<usize>()
        .map_err(|_| PyValueError::new_err(format!("{field} is out of range")))?;
    if parsed == 0 {
        return Err(PyValueError::new_err(format!(
            "{field} must be greater than zero"
        )));
    }

    Ok(Some(parsed))
}
