//! Trusted, statically registered native operations.
//!
//! Native operations are implemented by Fluxion and exposed as opaque
//! descriptors. Python can submit a descriptor through
//! [`crate::engine::PyEngine::submit_native`], but cannot construct descriptors,
//! register new workloads, or supply native code.
//!
//! Each operation converts Python inputs into Rust-owned values before
//! submission. The task closure that runs on a worker contains only Rust-owned
//! data and never attaches to the Python interpreter.

use std::fmt;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyAnyMethods, PyBool, PyByteArray, PyBytes, PyInt, PyMemoryView, PyString, PyType,
    PyTypeMethods,
};

#[cfg(debug_assertions)]
use crate::testing::{NativeProbe, PyNativeProbe};

/// The closed set of native operations available in this release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeOperationKind {
    SumSquares,
    #[cfg(debug_assertions)]
    TestProbe,
    #[cfg(debug_assertions)]
    TestPanic,
}

/// A statically defined registry entry.
struct NativeOperationSpec {
    name: &'static str,
    kind: NativeOperationKind,
}

/// Registry of trusted operations exposed by Fluxion.
///
/// Public operations are deliberately limited. Plugin registration and
/// user-defined native workloads are out of scope.
const REGISTRY: &[NativeOperationSpec] = &[NativeOperationSpec {
    name: "sum_squares",
    kind: NativeOperationKind::SumSquares,
}];

/// Rust-owned input accepted by a native operation.
pub(crate) enum NativeInput {
    I64Values(Vec<i64>),
    #[cfg(debug_assertions)]
    Probe(std::sync::Arc<NativeProbe>),
    #[cfg(debug_assertions)]
    Empty,
}

/// Rust-owned output produced by a native operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeOutput {
    I128(i128),
    #[cfg(debug_assertions)]
    Bool(bool),
}

impl NativeOutput {
    /// Converts the Rust-owned output into a Python object.
    ///
    /// This is called only by the handle after the GIL has been reacquired.
    pub(crate) fn to_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::I128(value) => Ok(PyInt::new(py, value).into_any().unbind()),
            #[cfg(debug_assertions)]
            Self::Bool(value) => Ok(PyBool::new(py, value).to_owned().into_any().unbind()),
        }
    }
}

/// Errors produced while executing a native operation.
///
/// These values remain Rust-owned until the Python binding converts them to a
/// public exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeError {
    IntegerOverflow,
    #[cfg(debug_assertions)]
    InvalidInput,
    #[cfg(debug_assertions)]
    ProbeTimeout,
}

impl fmt::Display for NativeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IntegerOverflow => {
                formatter.write_str("native operation overflowed its 128-bit integer output")
            }
            #[cfg(debug_assertions)]
            Self::InvalidInput => formatter.write_str("native operation received invalid input"),
            #[cfg(debug_assertions)]
            Self::ProbeTimeout => {
                formatter.write_str("native test probe timed out before reaching synchronization")
            }
        }
    }
}

impl NativeOperationKind {
    fn name(self) -> &'static str {
        match self {
            Self::SumSquares => "sum_squares",
            #[cfg(debug_assertions)]
            Self::TestProbe => "_test_probe",
            #[cfg(debug_assertions)]
            Self::TestPanic => "_test_panic",
        }
    }

    /// Validates and converts Python input into Rust-owned data.
    ///
    /// This runs while the caller holds the interpreter lock. The returned
    /// value contains no Python references.
    pub(crate) fn prepare(self, values: &Bound<'_, PyAny>) -> PyResult<NativeInput> {
        match self {
            Self::SumSquares => Ok(NativeInput::I64Values(extract_i64_values(values)?)),
            #[cfg(debug_assertions)]
            Self::TestProbe => {
                let probe = values.extract::<PyRef<'_, PyNativeProbe>>().map_err(|_| {
                    PyTypeError::new_err("_test_probe requires a _NativeProbe instance")
                })?;
                Ok(NativeInput::Probe(probe.shared()))
            }
            #[cfg(debug_assertions)]
            Self::TestPanic => Ok(NativeInput::Empty),
        }
    }

    /// Executes a native operation using Rust-owned input.
    ///
    /// This function must never access Python objects or attach to the
    /// interpreter. It is called directly by a Fluxion worker.
    pub(crate) fn execute(self, input: NativeInput) -> Result<NativeOutput, NativeError> {
        match self {
            Self::SumSquares => match input {
                NativeInput::I64Values(values) => sum_squares(values).map(NativeOutput::I128),
                #[cfg(debug_assertions)]
                _ => Err(NativeError::InvalidInput),
            },
            #[cfg(debug_assertions)]
            Self::TestProbe => match input {
                NativeInput::Probe(probe) => {
                    if probe.arrive_and_wait() {
                        Ok(NativeOutput::Bool(true))
                    } else {
                        Err(NativeError::ProbeTimeout)
                    }
                }
                _ => Err(NativeError::InvalidInput),
            },
            #[cfg(debug_assertions)]
            Self::TestPanic => panic!("intentional native operation panic"),
        }
    }
}

/// Opaque Python descriptor for a trusted native operation.
///
/// Instances are created only by Fluxion's module initializer. The type has no
/// Python constructor and cannot be subclassed or mutated.
#[pyclass(name = "NativeOperation", module = "fluxion.native", frozen)]
pub(crate) struct PyNativeOperation {
    kind: NativeOperationKind,
}

impl PyNativeOperation {
    const fn new(kind: NativeOperationKind) -> Self {
        Self { kind }
    }

    #[cfg(debug_assertions)]
    pub(crate) const fn test_probe() -> Self {
        Self::new(NativeOperationKind::TestProbe)
    }

    #[cfg(debug_assertions)]
    pub(crate) const fn test_panic() -> Self {
        Self::new(NativeOperationKind::TestPanic)
    }

    pub(crate) const fn kind(&self) -> NativeOperationKind {
        self.kind
    }
}

#[pymethods]
impl PyNativeOperation {
    fn __repr__(&self) -> String {
        format!("NativeOperation(name='{}')", self.kind.name())
    }
}

/// Registers the public native operation class and static registry entries.
pub(crate) fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNativeOperation>()?;

    for spec in REGISTRY {
        module.add(spec.name, Py::new(py, PyNativeOperation::new(spec.kind))?)?;
    }

    #[cfg(debug_assertions)]
    crate::testing::register(py, module)?;

    Ok(())
}

const MAX_INITIAL_CAPACITY: usize = 4096;

static ARRAY_ARRAY_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static NUMPY_NDARRAY_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

fn is_array_array(values: &Bound<'_, PyAny>) -> bool {
    let ty = values.get_type_ptr().cast::<pyo3::ffi::PyObject>();
    let py = values.py();
    if let Some(expected) = ARRAY_ARRAY_TYPE.get(py) {
        return ty == expected.as_ptr();
    }

    ARRAY_ARRAY_TYPE
        .import(py, "array", "array")
        .is_ok_and(|expected| ty == expected.as_ptr())
}

fn is_numpy_ndarray(values: &Bound<'_, PyAny>) -> bool {
    let ty = values.get_type_ptr().cast::<pyo3::ffi::PyObject>();
    let py = values.py();
    if let Some(expected) = NUMPY_NDARRAY_TYPE.get(py) {
        return ty == expected.as_ptr();
    }

    let type_object = values.get_type();
    let Ok(module) = type_object.module() else {
        return false;
    };
    let Ok(name) = type_object.name() else {
        return false;
    };
    if !(module.eq("numpy") && name.eq("ndarray")) {
        return false;
    }

    NUMPY_NDARRAY_TYPE
        .import(py, "numpy", "ndarray")
        .is_ok_and(|expected| ty == expected.as_ptr())
}

fn is_known_buffer_container(values: &Bound<'_, PyAny>) -> bool {
    values.is_instance_of::<PyMemoryView>() || is_array_array(values) || is_numpy_ndarray(values)
}

fn is_native_signed_i64_format(format: &std::ffi::CStr) -> bool {
    let bytes = format.to_bytes();
    let (prefix, type_char) = match bytes {
        [type_char] => (None, *type_char),
        [prefix, type_char] => (Some(*prefix), *type_char),
        _ => return false,
    };
    let native_endian = match prefix {
        None | Some(b'@' | b'=') => true,
        Some(b'<') => cfg!(target_endian = "little"),
        Some(b'>' | b'!') => cfg!(target_endian = "big"),
        Some(_) => false,
    };

    native_endian && matches!(type_char, b'l' | b'q' | b'n')
}

fn try_extract_i64_buffer(values: &Bound<'_, PyAny>) -> PyResult<Option<Vec<i64>>> {
    if unsafe { pyo3::ffi::PyObject_CheckBuffer(values.as_ptr()) } == 0 {
        return Ok(None);
    }

    if !is_known_buffer_container(values) {
        return Ok(None);
    }

    let buffer = match PyBuffer::<i64>::get(values) {
        Ok(buffer) => buffer,
        Err(_) => return Ok(None),
    };

    if buffer.dimensions() != 1
        || !buffer.is_c_contiguous()
        || buffer.item_size() != 8
        || !is_native_signed_i64_format(buffer.format())
    {
        return Ok(None);
    }

    let item_count = buffer.item_count();
    if buffer.len_bytes() != item_count.saturating_mul(8)
        || (buffer.len_bytes() != 0 && buffer.buf_ptr().is_null())
        || buffer.shape().first().copied() != Some(item_count)
    {
        return Ok(None);
    }

    Ok(Some(buffer.to_vec(values.py())?))
}

fn extract_i64_values(values: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    if values.is_instance_of::<PyString>()
        || values.is_instance_of::<PyBytes>()
        || values.is_instance_of::<PyByteArray>()
    {
        return Err(PyTypeError::new_err(
            "sum_squares() values must be an iterable of integers, not a string or byte sequence",
        ));
    }

    if let Some(converted) = try_extract_i64_buffer(values)? {
        return Ok(converted);
    }

    let iterator = values.try_iter().map_err(|_| {
        PyTypeError::new_err("sum_squares() values must be an iterable of integers")
    })?;
    // Do not trust a user-defined ``__len__`` for unbounded preallocation.
    let initial_capacity = values.len().unwrap_or(0).min(MAX_INITIAL_CAPACITY);
    let mut converted = Vec::with_capacity(initial_capacity);

    for (index, item) in iterator.enumerate() {
        let item = item?;

        if item.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(format!(
                "sum_squares() value at index {index} must be an integer, not bool"
            )));
        }

        if !item.is_instance_of::<PyInt>() {
            return Err(PyTypeError::new_err(format!(
                "sum_squares() value at index {index} must be an integer"
            )));
        }

        let value = item.extract::<i64>().map_err(|_| {
            PyOverflowError::new_err(format!(
                "sum_squares() value at index {index} is outside the signed 64-bit range"
            ))
        })?;
        converted.push(value);
    }

    Ok(converted)
}

fn sum_squares(values: Vec<i64>) -> Result<i128, NativeError> {
    let mut total = 0_i128;

    for value in values {
        let value = i128::from(value);
        let square = value
            .checked_mul(value)
            .ok_or(NativeError::IntegerOverflow)?;
        total = total
            .checked_add(square)
            .ok_or(NativeError::IntegerOverflow)?;
    }

    Ok(total)
}
