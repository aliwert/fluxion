//! Python bindings for the Fluxion execution engine.
//!
//! The binding layer exposes the existing Rust engine without adding
//! scheduling or execution behavior. Python callbacks are invoked on Fluxion
//! worker threads while attached to the Python interpreter, so they remain
//! subject to Python's GIL and are not automatically parallelized.

mod engine;
mod error;
mod task;

use pyo3::prelude::*;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<engine::PyEngine>()?;
    m.add_class::<task::PyTaskHandle>()?;

    Ok(())
}
