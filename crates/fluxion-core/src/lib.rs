//! Core execution engine for Fluxion.
//!
//! This crate contains the Rust-native engine lifecycle and configuration
//! primitives. It is intentionally independent of PyO3 and Python-specific
//! types so that the execution core remains independently testable.

pub mod config;
pub mod engine;
pub mod error;

pub use config::{DEFAULT_QUEUE_CAPACITY, EngineConfig};
pub use engine::{Engine, EngineState};
pub use error::EngineError;
