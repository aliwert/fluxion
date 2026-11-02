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
}
