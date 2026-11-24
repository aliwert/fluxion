use std::process;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fluxion_core::{SharedTaskHandle, TaskError, TaskHandle, TaskStatus};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyFunction};

use crate::convert::{status_name, task_status};
use crate::error::{forked_process_error, native_error, task_error, timeout_error};
use crate::native::{NativeError, NativeOutput};

pub(crate) type PythonTaskResult = Result<Py<PyAny>, PyErr>;
pub(crate) type NativeTaskResult = Result<NativeOutput, NativeError>;

/// Core handle stored by a Python task handle.
///
/// Python callback and native operation results have different Rust types, so
/// each handle dispatches to the core handle for its task kind.
enum HandleKind {
    Python(SharedTaskHandle<PythonTaskResult>),
    Native(SharedTaskHandle<NativeTaskResult>),
}

impl HandleKind {
    fn status(&self) -> TaskStatus {
        match self {
            Self::Python(handle) => handle.status(),
            Self::Native(handle) => handle.status(),
        }
    }

    fn wait_until_ready(&self, timeout: Option<Duration>) -> bool {
        match self {
            Self::Python(handle) => handle.wait_until_ready(timeout),
            Self::Native(handle) => handle.wait_until_ready(timeout),
        }
    }

    fn cancel(&self) -> bool {
        match self {
            Self::Python(handle) => handle.cancel(),
            Self::Native(handle) => handle.cancel(),
        }
    }

    fn done(&self) -> bool {
        match self {
            Self::Python(handle) => handle.done(),
            Self::Native(handle) => handle.done(),
        }
    }

    fn cancelled(&self) -> bool {
        match self {
            Self::Python(handle) => handle.cancelled(),
            Self::Native(handle) => handle.cancelled(),
        }
    }
}

/// Rejects coroutine functions and callables whose ``__call__`` is a coroutine
/// function before a task is submitted to the engine.
pub(crate) fn ensure_sync_callable(py: Python<'_>, callable: &Bound<'_, PyAny>) -> PyResult<()> {
    if !callable.is_callable() {
        return Err(PyTypeError::new_err(
            "submit_async() requires a synchronous callable",
        ));
    }

    if let Ok(function) = callable.cast::<PyFunction>() {
        let code = function.getattr("__code__")?;
        let flags = code.getattr("co_flags")?.extract::<i32>()?;
        if flags & (pyo3::ffi::CO_COROUTINE | pyo3::ffi::CO_ITERABLE_COROUTINE) != 0 {
            return Err(coroutine_function_error());
        }
        return Ok(());
    }

    let inspect = py.import("inspect")?;
    if inspect
        .call_method1("iscoroutinefunction", (callable,))?
        .extract::<bool>()?
    {
        return Err(coroutine_function_error());
    }

    if let Ok(call_method) = callable.getattr("__call__") {
        if inspect
            .call_method1("iscoroutinefunction", (call_method,))?
            .extract::<bool>()?
        {
            return Err(coroutine_function_error());
        }
    }

    Ok(())
}

/// Converts an awaitable callback result into a ``TypeError`` and closes a
/// coroutine object so Python does not emit an unawaited-coroutine warning.
pub(crate) fn reject_awaitable_result(py: Python<'_>, result: &Bound<'_, PyAny>) -> PyResult<()> {
    let inspect = py.import("inspect")?;
    if !inspect
        .call_method1("isawaitable", (result,))?
        .extract::<bool>()?
    {
        return Ok(());
    }

    if inspect
        .call_method1("iscoroutine", (result,))?
        .extract::<bool>()?
    {
        let _ = result.call_method0("close");
    }

    Err(PyTypeError::new_err(
        "submit_async() requires a synchronous callable; the callable returned an awaitable. Use asyncio.create_task() for coroutine execution.",
    ))
}

fn coroutine_function_error() -> PyErr {
    PyTypeError::new_err(
        "submit_async() requires a synchronous callable; coroutine functions are not supported. Use asyncio.create_task() for coroutine execution.",
    )
}

/// Shared Python-level flag indicating that a task failed.
///
/// The Rust core sees a Python exception or native error as a successfully
/// returned `Result::Err`, so this flag preserves the Python-facing distinction
/// between a completed task and a failed task without changing the core task
/// lifecycle.
#[derive(Clone)]
pub(crate) struct TaskFailure {
    failed: Arc<Mutex<bool>>,
}

impl TaskFailure {
    pub(crate) fn new() -> Self {
        Self {
            failed: Arc::new(Mutex::new(false)),
        }
    }

    pub(crate) fn mark(&self) {
        *lock_flag(&self.failed) = true;
    }

    fn load(&self) -> bool {
        *lock_flag(&self.failed)
    }
}

fn lock_flag(flag: &Mutex<bool>) -> MutexGuard<'_, bool> {
    match flag.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Python-visible task outcome extracted while the core result lock is held.
enum PyOutcome {
    Value(Py<PyAny>),
    CallbackError(PyErr),
    NativeError(NativeError),
    CoreError(TaskError),
}

/// Handle to a task submitted to a Fluxion engine.
///
/// A handle supports repeatable result access for Python callbacks and trusted
/// native operations, including concurrent waits from multiple Python threads.
/// Dropping a handle does not cancel the task; the task still runs and its
/// result is discarded when the last handle is dropped.
///
/// Handles belong to the process that created their engine. Using a handle in
/// a forked child raises :class:`fluxion.ForkedProcessError`.
#[pyclass(name = "TaskHandle", module = "fluxion", generic)]
pub(crate) struct PyTaskHandle {
    handle: HandleKind,
    task_failure: TaskFailure,
    owner_pid: u32,
    mapped_failure: Mutex<Option<PyErr>>,
}

impl PyTaskHandle {
    pub(crate) fn new_python(
        handle: TaskHandle<PythonTaskResult>,
        task_failure: TaskFailure,
        owner_pid: u32,
    ) -> Self {
        Self::new(
            HandleKind::Python(handle.into_shared()),
            task_failure,
            owner_pid,
        )
    }

    pub(crate) fn new_native(
        handle: TaskHandle<NativeTaskResult>,
        task_failure: TaskFailure,
        owner_pid: u32,
    ) -> Self {
        Self::new(
            HandleKind::Native(handle.into_shared()),
            task_failure,
            owner_pid,
        )
    }

    fn new(handle: HandleKind, task_failure: TaskFailure, owner_pid: u32) -> Self {
        Self {
            handle,
            task_failure,
            owner_pid,
            mapped_failure: Mutex::new(None),
        }
    }

    fn ensure_process(&self, py: Python<'_>) -> PyResult<()> {
        if self.owner_pid == process::id() {
            Ok(())
        } else {
            Err(forked_process_error(py))
        }
    }

    fn is_forked(&self) -> bool {
        self.owner_pid != process::id()
    }

    fn lock_mapped_failure(&self) -> MutexGuard<'_, Option<PyErr>> {
        match self.mapped_failure.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lifecycle_status(&self) -> TaskStatus {
        let core_status = self.handle.status();

        if core_status == TaskStatus::Completed && self.task_failure.load() {
            TaskStatus::Failed
        } else {
            core_status
        }
    }

    fn wait_result(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        self.ensure_process(py)?;
        let timeout = parse_timeout(timeout)?;
        let ready = py.detach(|| self.handle.wait_until_ready(timeout));

        if !ready {
            return Err(timeout_error(py));
        }

        match self.outcome(py)? {
            PyOutcome::Value(value) => Ok(value),
            PyOutcome::CallbackError(error) => Err(error),
            PyOutcome::NativeError(error) => Err(self.mapped_native_failure(py, error)),
            PyOutcome::CoreError(error) => Err(self.mapped_failure(py, error)),
        }
    }

    fn outcome(&self, py: Python<'_>) -> PyResult<PyOutcome> {
        let outcome = match &self.handle {
            HandleKind::Python(handle) => {
                let Some(view) = handle.result_view() else {
                    return Err(missing_result_error());
                };

                match &*view {
                    Ok(Ok(value)) => PyOutcome::Value(value.clone_ref(py)),
                    Ok(Err(error)) => PyOutcome::CallbackError(error.clone_ref(py)),
                    Err(error) => PyOutcome::CoreError(error.clone()),
                }
            }
            HandleKind::Native(handle) => {
                let Some(view) = handle.result_view() else {
                    return Err(missing_result_error());
                };

                match &*view {
                    Ok(Ok(output)) => PyOutcome::Value(output.to_python(py)?),
                    Ok(Err(error)) => PyOutcome::NativeError(*error),
                    Err(error) => PyOutcome::CoreError(error.clone()),
                }
            }
        };

        Ok(outcome)
    }

    fn mapped_failure(&self, py: Python<'_>, error: TaskError) -> PyErr {
        self.mapped_error(py, |py| task_error(py, error))
    }

    fn mapped_native_failure(&self, py: Python<'_>, error: NativeError) -> PyErr {
        self.mapped_error(py, |py| native_error(py, error))
    }

    fn mapped_error<F>(&self, py: Python<'_>, map: F) -> PyErr
    where
        F: FnOnce(Python<'_>) -> PyErr,
    {
        if let Some(error) = self.lock_mapped_failure().as_ref() {
            return error.clone_ref(py);
        }

        let mapped = map(py);
        let mut cached = self.lock_mapped_failure();

        if let Some(error) = cached.as_ref() {
            return error.clone_ref(py);
        }

        *cached = Some(mapped.clone_ref(py));
        mapped
    }
}

#[pymethods]
impl PyTaskHandle {
    /// Blocks until the task completes and returns its result.
    ///
    /// This is a repeatable alias of :meth:`result`.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        self.wait_result(py, timeout)
    }

    /// Blocks until the task completes and returns its result.
    ///
    /// With ``timeout=None`` the wait is indefinite. A finite timeout raises
    /// :class:`fluxion.TaskTimeoutError`, which is also catchable as
    /// ``TimeoutError`` and ``concurrent.futures.TimeoutError``. A timeout does
    /// not cancel the task or consume the handle. Zero and negative timeouts
    /// use immediate-timeout semantics; NaN and infinite values raise
    /// ``ValueError``.
    ///
    /// The GIL is released while waiting. If a Python callback raised an
    /// exception, that same exception is re-raised here. A native operation
    /// failure raises :class:`fluxion.NativeExecutionError`. A task panic or
    /// disconnected worker raises :class:`fluxion.TaskPanicError` or
    /// :class:`fluxion.WorkerDisconnectedError`, both also catchable as
    /// ``RuntimeError``. A cancelled task raises
    /// :class:`fluxion.TaskCancelledError`, also catchable as
    /// ``concurrent.futures.CancelledError``.
    ///
    /// This method may be called multiple times and from multiple threads.
    #[pyo3(signature = (timeout=None))]
    fn result(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        self.wait_result(py, timeout)
    }

    /// Blocks until the task completes and returns its exception, if any.
    ///
    /// Returns ``None`` on success. For failed tasks, returns the stored
    /// exception object without raising it. For cancelled tasks, returns a
    /// :class:`fluxion.TaskCancelledError`. A timeout raises
    /// :class:`fluxion.TaskTimeoutError`; it does not cancel the task.
    #[pyo3(signature = (timeout=None))]
    fn exception(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        self.ensure_process(py)?;
        let timeout = parse_timeout(timeout)?;
        let ready = py.detach(|| self.handle.wait_until_ready(timeout));

        if !ready {
            return Err(timeout_error(py));
        }

        let exception = match self.outcome(py)? {
            PyOutcome::Value(_) => return Ok(py.None()),
            PyOutcome::CallbackError(error) => error.into_value(py).into_any(),
            PyOutcome::NativeError(error) => self
                .mapped_native_failure(py, error)
                .into_value(py)
                .into_any(),
            PyOutcome::CoreError(error) => self.mapped_failure(py, error).into_value(py).into_any(),
        };

        Ok(exception)
    }

    /// Attempts to cancel the task before it starts running.
    ///
    /// Returns ``True`` only when the task was still queued and this call
    /// cancelled it. Running and terminal tasks cannot be interrupted.
    /// Cancellation works independently of pending waiters.
    fn cancel(&self, py: Python<'_>) -> PyResult<bool> {
        self.ensure_process(py)?;
        Ok(self.handle.cancel())
    }

    /// Returns whether the task outcome has been fully published.
    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.ensure_process(py)?;
        Ok(self.handle.done())
    }

    /// Returns whether the task was cancelled before it started.
    fn cancelled(&self, py: Python<'_>) -> PyResult<bool> {
        self.ensure_process(py)?;
        Ok(self.handle.cancelled())
    }

    /// Returns whether the task is currently running.
    fn running(&self, py: Python<'_>) -> PyResult<bool> {
        self.ensure_process(py)?;
        Ok(self.handle.status() == TaskStatus::Running)
    }

    /// Current lifecycle status.
    ///
    /// The value is a :class:`fluxion.TaskStatus` member. It also compares
    /// equal to the stable lowercase strings ``"queued"``, ``"running"``,
    /// ``"completed"``, ``"failed"``, and ``"cancelled"``. A terminal status
    /// may be observed slightly before ``done()`` becomes true; use ``done()``
    /// or ``result(timeout=0)`` to determine whether the outcome is available.
    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_process(py)?;
        task_status(py, self.lifecycle_status())
    }

    fn __repr__(&self) -> String {
        if self.is_forked() {
            "TaskHandle(status='forked-process')".to_string()
        } else {
            format!(
                "TaskHandle(status='{}')",
                status_name(self.lifecycle_status())
            )
        }
    }
}

fn missing_result_error() -> PyErr {
    PyRuntimeError::new_err("task result is not published after the task was reported done")
}

fn parse_timeout(timeout: Option<f64>) -> PyResult<Option<Duration>> {
    let Some(seconds) = timeout else {
        return Ok(None);
    };

    if !seconds.is_finite() {
        return Err(PyValueError::new_err(
            "timeout must be a finite number of seconds or None",
        ));
    }

    if seconds <= 0.0 {
        return Ok(Some(Duration::ZERO));
    }

    Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| PyValueError::new_err("timeout is out of range"))
}
