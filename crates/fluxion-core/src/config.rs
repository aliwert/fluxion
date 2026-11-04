use std::thread::available_parallelism;

use crate::error::EngineError;

/// Default capacity of the engine's task queue.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Number of tasks a worker may claim into its private prefetch buffer.
pub(crate) const PREFETCH_BATCH: usize = 8;

/// Runtime configuration for the execution engine.
///
/// Configuration is validated when an [`crate::Engine`] is created. Values can
/// also be checked ahead of time with [`EngineConfig::validate`] or by using
/// [`EngineConfig::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    /// Number of worker threads the engine may use.
    pub worker_count: usize,
    /// Maximum number of tasks waiting in the shared injection queue.
    ///
    /// Workers may additionally hold a fixed batch of prefetched tasks in
    /// private buffers. The maximum number of pending tasks is therefore
    /// `queue_capacity + worker_count * 8`.
    pub queue_capacity: usize,
}

impl EngineConfig {
    /// Creates a configuration and validates it immediately.
    pub fn new(worker_count: usize, queue_capacity: usize) -> Result<Self, EngineError> {
        let config = Self {
            worker_count,
            queue_capacity,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates that all configuration values are usable.
    pub fn validate(&self) -> Result<(), EngineError> {
        if self.worker_count == 0 {
            return Err(EngineError::InvalidConfiguration {
                field: "worker_count",
                reason: "worker count must be greater than zero",
            });
        }

        if self.queue_capacity == 0 {
            return Err(EngineError::InvalidConfiguration {
                field: "queue_capacity",
                reason: "queue capacity must be greater than zero",
            });
        }

        Ok(())
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            worker_count: default_worker_count(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

fn default_worker_count() -> usize {
    match available_parallelism() {
        Ok(parallelism) => parallelism.get(),
        Err(_) => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = EngineConfig::default();

        assert!(config.worker_count >= 1);
        assert_eq!(config.queue_capacity, DEFAULT_QUEUE_CAPACITY);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn custom_config_accepts_valid_values() {
        let config = EngineConfig::new(4, 256).expect("configuration should be valid");

        assert_eq!(config.worker_count, 4);
        assert_eq!(config.queue_capacity, 256);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn zero_worker_count_is_rejected() {
        let result = EngineConfig::new(0, 128);

        assert_eq!(
            result,
            Err(EngineError::InvalidConfiguration {
                field: "worker_count",
                reason: "worker count must be greater than zero",
            })
        );
    }

    #[test]
    fn zero_queue_capacity_is_rejected() {
        let result = EngineConfig::new(2, 0);

        assert_eq!(
            result,
            Err(EngineError::InvalidConfiguration {
                field: "queue_capacity",
                reason: "queue capacity must be greater than zero",
            })
        );
    }

    #[test]
    fn validate_rejects_manually_constructed_invalid_config() {
        let config = EngineConfig {
            worker_count: 1,
            queue_capacity: 0,
        };

        assert_eq!(
            config.validate(),
            Err(EngineError::InvalidConfiguration {
                field: "queue_capacity",
                reason: "queue capacity must be greater than zero",
            })
        );
    }
}
