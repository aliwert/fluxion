//! Core execution engine for Fluxion.
//!
//! This crate contains the Rust-native engine lifecycle, task queue, and worker
//! pool primitives. It is intentionally independent of PyO3 and Python-specific
//! types so that the execution core remains independently testable.

pub mod config;
pub mod engine;
pub mod error;

mod queue;
mod worker;

pub use config::{DEFAULT_QUEUE_CAPACITY, EngineConfig};
pub use engine::{Engine, EngineState};
pub use error::EngineError;
