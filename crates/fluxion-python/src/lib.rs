//! Python bindings for the Fluxion execution engine.
//!
//! The binding layer exposes the existing Rust engine without adding
//! scheduling or execution behavior. Python callbacks are invoked on Fluxion
//! worker threads while attached to the Python interpreter, so they remain
//! subject to Python's GIL and are not automatically parallelized.
//!
//! Trusted native operations execute on the same worker threads without
//! attaching to the interpreter during computation. Inputs are copied into
//! Rust-owned values before submission, and outputs are converted back only
//! after the GIL is reacquired.
//!
//! Asynchronous submissions create an asyncio Future bound to the submitting
//! event loop. Worker threads publish outcomes through
//! `loop.call_soon_threadsafe`, while Future cancellation is translated into
//! the core task's queued-only cancellation mechanism.

mod asyncio;
mod convert;
mod engine;
mod error;
mod native;
mod task;
#[cfg(debug_assertions)]
mod testing;

use pyo3::prelude::*;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<engine::PyEngine>()?;
    m.add_class::<task::PyTaskHandle>()?;
    native::register(m.py(), m)?;
    m.add(
        "__all__",
        vec![
            "__version__",
            "Engine",
            "TaskHandle",
            "NativeOperation",
            "sum_squares",
        ],
    )?;

    Ok(())
}
