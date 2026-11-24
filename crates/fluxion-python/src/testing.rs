//! Debug-only synchronization probes used by Python concurrency tests.
//!
//! These types are intentionally absent from public stubs. Release builds do not
//! compile or register them.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native::PyNativeOperation;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

struct ProbeState {
    arrived: usize,
    released: bool,
}

/// A controllable native synchronization point.
///
/// Without release blocking, participants wait until the configured number of
/// tasks arrive. With release blocking, each participant arrives, reports
/// readiness, and then waits until Python calls `release`.
pub(crate) struct NativeProbe {
    participants: usize,
    block_until_release: bool,
    timeout: Duration,
    state: Mutex<ProbeState>,
    ready: Condvar,
}

impl NativeProbe {
    fn new(participants: usize, block_until_release: bool, timeout: Duration) -> Self {
        Self {
            participants,
            block_until_release,
            timeout,
            state: Mutex::new(ProbeState {
                arrived: 0,
                released: false,
            }),
            ready: Condvar::new(),
        }
    }

    pub(crate) fn arrive_and_wait(&self) -> bool {
        let deadline = Instant::now().checked_add(self.timeout);
        let mut state = self.lock_state();
        state.arrived += 1;
        self.ready.notify_all();

        loop {
            if self.reached_target(&state) {
                return true;
            }

            let Some(deadline) = deadline else {
                return false;
            };
            let now = Instant::now();

            if now >= deadline {
                return false;
            }

            state = wait_for(&self.ready, state, deadline - now);
        }
    }

    pub(crate) fn wait_started(&self, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.lock_state();

        loop {
            if state.arrived >= self.participants {
                return true;
            }

            let Some(deadline) = deadline else {
                return false;
            };
            let now = Instant::now();

            if now >= deadline {
                return false;
            }

            state = wait_for(&self.ready, state, deadline - now);
        }
    }

    pub(crate) fn release(&self) -> bool {
        let mut state = self.lock_state();

        if state.released {
            return false;
        }

        state.released = true;
        self.ready.notify_all();
        true
    }

    fn reached_target(&self, state: &ProbeState) -> bool {
        if self.block_until_release {
            state.released
        } else {
            state.arrived >= self.participants
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ProbeState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// Hidden Python handle for controlling a [`NativeProbe`].
#[pyclass(name = "_NativeProbe", module = "fluxion._core", frozen)]
pub(crate) struct PyNativeProbe {
    probe: Arc<NativeProbe>,
}

impl PyNativeProbe {
    pub(crate) fn shared(&self) -> Arc<NativeProbe> {
        Arc::clone(&self.probe)
    }
}

#[pymethods]
impl PyNativeProbe {
    #[new]
    #[pyo3(signature = (participants, block_until_release=false, timeout=None))]
    fn new(participants: usize, block_until_release: bool, timeout: Option<f64>) -> PyResult<Self> {
        if participants == 0 {
            return Err(PyValueError::new_err(
                "probe participants must be greater than zero",
            ));
        }

        let timeout = parse_timeout(timeout)?;

        Ok(Self {
            probe: Arc::new(NativeProbe::new(participants, block_until_release, timeout)),
        })
    }

    #[pyo3(signature = (timeout=None))]
    fn wait_started(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let timeout = parse_timeout(timeout)?;
        Ok(py.detach(|| self.probe.wait_started(timeout)))
    }

    fn release(&self) -> bool {
        self.probe.release()
    }
}

/// Registers debug-only native probes and their test operations.
pub(crate) fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNativeProbe>()?;
    module.add("_test_probe", Py::new(py, PyNativeOperation::test_probe())?)?;
    module.add("_test_panic", Py::new(py, PyNativeOperation::test_panic())?)?;
    Ok(())
}

fn wait_for<'a>(
    ready: &Condvar,
    state: MutexGuard<'a, ProbeState>,
    timeout: Duration,
) -> MutexGuard<'a, ProbeState> {
    match ready.wait_timeout(state, timeout) {
        Ok((guard, _)) => guard,
        Err(poisoned) => poisoned.into_inner().0,
    }
}

fn parse_timeout(timeout: Option<f64>) -> PyResult<Duration> {
    let Some(seconds) = timeout else {
        return Ok(DEFAULT_TIMEOUT);
    };

    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(PyValueError::new_err(
            "probe timeout must be a positive finite number of seconds",
        ));
    }

    Duration::try_from_secs_f64(seconds)
        .map_err(|_| PyValueError::new_err("probe timeout is out of range"))
}
