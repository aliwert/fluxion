use thiserror::Error;

/// Errors produced by the Fluxion execution engine.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    /// The supplied configuration failed validation.
    #[error("invalid configuration for `{field}`: {reason}")]
    InvalidConfiguration {
        /// Name of the configuration field that failed validation.
        field: &'static str,
        /// Human-readable explanation of the violated constraint.
        reason: &'static str,
    },

    /// The engine has already completed shutdown.
    #[error("engine has already been shut down")]
    AlreadyShutdown,

    /// The task queue has reached its configured capacity.
    #[error("task queue is full")]
    QueueFull,

    /// A worker thread could not be created.
    #[error("failed to spawn worker {worker_index}: {kind:?}")]
    WorkerSpawnFailed {
        /// Index of the worker that could not be started.
        worker_index: usize,
        /// Operating system error kind reported by the thread builder.
        kind: std::io::ErrorKind,
    },

    /// A worker thread terminated unexpectedly.
    #[error("worker {worker_index} terminated unexpectedly")]
    WorkerFailed {
        /// Index of the worker that terminated.
        worker_index: usize,
    },
}
