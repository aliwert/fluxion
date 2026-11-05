//! Core execution engine for Fluxion.
//!
//! This crate contains the Rust-native engine lifecycle, task queue, worker
//! pool, and task handle primitives. It is intentionally independent of PyO3
//! and Python-specific types so that the execution core remains independently
//! testable.

pub mod config;
pub mod engine;
pub mod error;
pub mod task;

#[cfg(feature = "experimental-work-stealing")]
mod experimental;
mod queue;
mod worker;

pub use config::{DEFAULT_QUEUE_CAPACITY, EngineConfig};
pub use engine::{Engine, EngineState};
pub use error::EngineError;
#[cfg(feature = "experimental-work-stealing")]
#[doc(hidden)]
pub use experimental::{StealStatsSnapshot, WorkStealingEngine};
pub use task::{TaskError, TaskHandle};
