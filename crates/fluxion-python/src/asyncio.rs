use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use fluxion_core::{TaskError, TaskHandle};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyCFunction};

use crate::error::task_error;
use crate::task::PythonTaskResult;

pub(crate) type AsyncTaskHandle = Arc<Mutex<Option<TaskHandle<PythonTaskResult>>>>;

pub(crate) fn new_future(
    py: Python<'_>,
    event_loop: &Bound<'_, PyAny>,
    cancellation: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let module = py.import("fluxion._asyncio")?;
    let future_type = module.getattr("_FluxionFuture")?;
    let future = future_type.call1((event_loop, cancellation))?;

    Ok(future.unbind())
}

pub(crate) fn cancellation_callback(
    py: Python<'_>,
    handle: AsyncTaskHandle,
    active: Arc<AtomicBool>,
) -> PyResult<Py<PyAny>> {
    let callback =
        PyCFunction::new_closure(py, None, None, move |_args, _kwargs| -> PyResult<bool> {
            active.store(false, Ordering::Release);

            let handle = match handle.lock() {
                Ok(mut guard) => guard.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };

            Ok(handle.is_some_and(|handle| handle.cancel()))
        })?;

    Ok(callback.into_any().unbind())
}

enum CompletionPayload {
    Value(Py<PyAny>),
    Exception(PyErr),
}

/// Delivers one asynchronous task outcome to the submitting event loop.
///
/// The guard is armed while the task is queued or running. If the task closure
/// is dropped without producing a result, `Drop` reports a disconnected worker
/// unless cancellation has already been observed.
pub(crate) struct AsyncCompletion {
    event_loop: Py<PyAny>,
    future: Py<PyAny>,
    active: Arc<AtomicBool>,
    completed: bool,
}

impl AsyncCompletion {
    pub(crate) fn new(event_loop: Py<PyAny>, future: Py<PyAny>, active: Arc<AtomicBool>) -> Self {
        Self {
            event_loop,
            future,
            active,
            completed: false,
        }
    }

    pub(crate) fn complete(&mut self, py: Python<'_>, result: &PythonTaskResult) {
        self.completed = true;

        let payload = match result {
            Ok(value) => CompletionPayload::Value(value.clone_ref(py)),
            Err(error) => CompletionPayload::Exception(error.clone_ref(py)),
        };
        self.schedule(py, payload);
    }

    pub(crate) fn panicked(&mut self) {
        self.completed = true;
        self.schedule_error(TaskError::Panicked);
    }

    fn schedule_error(&self, error: TaskError) {
        let _ = Python::try_attach(|py| {
            self.schedule(py, CompletionPayload::Exception(task_error(py, error)));
        });
    }

    fn schedule(&self, py: Python<'_>, payload: CompletionPayload) {
        let future = self.future.clone_ref(py);
        let payload = Mutex::new(Some(payload));
        let callback =
            PyCFunction::new_closure(py, None, None, move |_args, _kwargs| -> PyResult<()> {
                let py = _args.py();
                let payload = match payload.lock() {
                    Ok(mut guard) => guard.take(),
                    Err(poisoned) => poisoned.into_inner().take(),
                };

                if let Some(payload) = payload {
                    apply_payload(py, &future, payload);
                }

                Ok(())
            });

        let Ok(callback) = callback else {
            return;
        };

        let _ = self
            .event_loop
            .bind(py)
            .call_method1("call_soon_threadsafe", (callback,));
    }
}

impl Drop for AsyncCompletion {
    fn drop(&mut self) {
        if !self.completed && self.active.load(Ordering::Acquire) {
            self.schedule_error(TaskError::WorkerDisconnected);
        }
    }
}

fn apply_payload(py: Python<'_>, future: &Py<PyAny>, payload: CompletionPayload) {
    let future = future.bind(py);
    let is_done = future
        .call_method0("done")
        .and_then(|done| done.extract::<bool>())
        .unwrap_or(true);

    if !is_done {
        match payload {
            CompletionPayload::Value(value) => {
                let _ = future.call_method1("set_result", (value,));
            }
            CompletionPayload::Exception(error) => {
                let _ = future.call_method1("set_exception", (error,));
            }
        }
    }

    let _ = future.setattr("_fluxion_cancel", py.None());
}
