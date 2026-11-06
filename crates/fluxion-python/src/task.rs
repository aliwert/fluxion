use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fluxion_core::{TaskError, TaskHandle, TaskStatus, TaskWaitOutcome};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::error::{task_error, timeout_error};

pub(crate) type PythonTaskResult = Result<Py<PyAny>, PyErr>;

/// Shared Python-level flag indicating that a callback raised.
///
/// The Rust core sees a Python exception as a successfully returned
/// `Result::Err`, so this flag preserves the Python-facing distinction between
/// a completed callback and a failed callback without changing the core task
/// lifecycle.
#[derive(Clone)]
pub(crate) struct CallbackFailure {
    failed: Arc<Mutex<bool>>,
}

impl CallbackFailure {
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

enum HandleState {
    Ready(TaskHandle<PythonTaskResult>),
    Waiting(TaskStatus),
    Consumed(TaskStatus),
}

/// Handle to a task submitted to a Fluxion engine.
///
/// A handle may be waited on once. Dropping a handle does not cancel the task;
/// the task still runs and its result is discarded. A timed-out wait does not
/// consume the handle, so it can be waited on again.
#[pyclass(name = "TaskHandle", module = "fluxion._core")]
pub(crate) struct PyTaskHandle {
    state: Mutex<HandleState>,
    callback_failure: CallbackFailure,
}

impl PyTaskHandle {
    pub(crate) fn new(
        handle: TaskHandle<PythonTaskResult>,
        callback_failure: CallbackFailure,
    ) -> Self {
        Self {
            state: Mutex::new(HandleState::Ready(handle)),
            callback_failure,
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, HandleState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn take_handle_for_wait(&self) -> PyResult<TaskHandle<PythonTaskResult>> {
        let mut state = self.lock_state();
        let snapshot = match &*state {
            HandleState::Ready(handle) => handle.status(),
            HandleState::Waiting(_) => {
                return Err(PyRuntimeError::new_err(
                    "task handle cannot be waited on while another wait is in progress",
                ));
            }
            HandleState::Consumed(_) => {
                return Err(PyRuntimeError::new_err(
                    "task handle has already been consumed",
                ));
            }
        };

        let previous = std::mem::replace(&mut *state, HandleState::Waiting(snapshot));
        match previous {
            HandleState::Ready(handle) => Ok(handle),
            _ => unreachable!("handle state was checked above"),
        }
    }

    fn restore_after_timeout(&self, handle: TaskHandle<PythonTaskResult>) {
        let mut state = self.lock_state();
        debug_assert!(matches!(*state, HandleState::Waiting(_)));
        *state = HandleState::Ready(handle);
    }

    fn mark_consumed(&self, status: TaskStatus) {
        *self.lock_state() = HandleState::Consumed(status);
    }

    fn lifecycle_status(&self) -> TaskStatus {
        let core_status = match &*self.lock_state() {
            HandleState::Ready(handle) => handle.status(),
            HandleState::Waiting(status) | HandleState::Consumed(status) => *status,
        };

        if core_status == TaskStatus::Completed && self.callback_failure.load() {
            TaskStatus::Failed
        } else {
            core_status
        }
    }
}

#[pymethods]
impl PyTaskHandle {
    /// Blocks until the task completes and returns its Python result.
    ///
    /// With ``timeout=None`` the wait is indefinite. A finite timeout raises
    /// ``concurrent.futures.TimeoutError`` only if the task has not completed
    /// before the timeout elapses; it does not cancel the task or consume the
    /// handle. Zero and negative timeouts use immediate-timeout semantics; NaN
    /// and infinite values raise ``ValueError``.
    ///
    /// The GIL is released while waiting so Python callbacks on Fluxion worker
    /// threads can make progress. If the callback raised an exception, that
    /// same exception is re-raised here. A task panic or disconnected worker
    /// raises ``RuntimeError`` with distinct messages (``"task panicked"`` and
    /// ``"task worker disconnected before producing a result"``). A cancelled
    /// task raises ``concurrent.futures.CancelledError``. Calling ``wait``
    /// after the result has already been consumed raises ``RuntimeError``.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let timeout = parse_timeout(timeout)?;
        let handle = self.take_handle_for_wait()?;

        let outcome = match timeout {
            None => py.detach(move || handle.wait()),
            Some(timeout) => match py.detach(move || handle.wait_timeout(timeout)) {
                TaskWaitOutcome::Completed(result) => result,
                TaskWaitOutcome::TimedOut(handle) => {
                    self.restore_after_timeout(handle);
                    return Err(timeout_error(py));
                }
            },
        };

        let status = match &outcome {
            Ok(Ok(_)) if !self.callback_failure.load() => TaskStatus::Completed,
            Ok(Ok(_)) | Ok(Err(_)) => TaskStatus::Failed,
            Err(TaskError::Cancelled) => TaskStatus::Cancelled,
            Err(_) => TaskStatus::Failed,
        };
        self.mark_consumed(status);

        match outcome {
            Ok(result) => result,
            Err(error) => Err(task_error(py, error)),
        }
    }

    /// Attempts to cancel the task before it starts running.
    ///
    /// Returns ``True`` only when the task was still queued and this call
    /// cancelled it. Running and terminal tasks cannot be interrupted. If
    /// another thread is currently waiting on this handle, ``cancel`` returns
    /// ``False`` because the handle is reserved by that wait.
    fn cancel(&self) -> bool {
        match &mut *self.lock_state() {
            HandleState::Ready(handle) => handle.cancel(),
            HandleState::Waiting(_) | HandleState::Consumed(_) => false,
        }
    }

    /// Returns whether the task has reached a terminal state.
    fn done(&self) -> bool {
        self.lifecycle_status().is_terminal()
    }

    /// Returns whether the task was cancelled before it started.
    fn cancelled(&self) -> bool {
        self.lifecycle_status() == TaskStatus::Cancelled
    }

    /// Current lifecycle status.
    ///
    /// Values are stable lowercase strings: ``"queued"``, ``"running"``,
    /// ``"completed"``, ``"failed"``, and ``"cancelled"``.
    #[getter]
    fn status(&self) -> &'static str {
        status_name(self.lifecycle_status())
    }
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

fn status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}
