use fluxion_core::TaskStatus;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

/// Returns the stable lowercase name for a core task status.
pub(crate) fn status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

/// Constructs the public Python ``TaskStatus`` member for a core status.
pub(crate) fn task_status(py: Python<'_>, status: TaskStatus) -> PyResult<Py<PyAny>> {
    enum_member(py, "TaskStatus", status_name(status))
}

/// Constructs the public Python ``EngineState`` member for a lifecycle name.
pub(crate) fn engine_state(py: Python<'_>, state: &str) -> PyResult<Py<PyAny>> {
    enum_member(py, "EngineState", state)
}

fn enum_member(py: Python<'_>, class_name: &str, value: &str) -> PyResult<Py<PyAny>> {
    let module = py.import("fluxion.status")?;
    let class = module.getattr(class_name)?;
    Ok(class.call1((value,))?.unbind())
}
