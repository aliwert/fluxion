use crate::{config::EngineConfig, error::EngineError};

/// Lifecycle state of an [`Engine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    /// The engine has been created and configured successfully.
    Initialized,
    /// The engine has been shut down and cannot be used again.
    Shutdown,
}

/// Primary entry point into the Fluxion execution runtime.
///
/// The engine owns its configuration and lifecycle state. It is created with
/// validated configuration and must be shut down explicitly once it is no
/// longer needed.
#[derive(Debug)]
pub struct Engine {
    config: EngineConfig,
    state: EngineState,
}

impl Engine {
    /// Creates an engine from the supplied configuration.
    ///
    /// The configuration is validated before any engine state is created.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        config.validate()?;

        Ok(Self {
            config,
            state: EngineState::Initialized,
        })
    }

    /// Returns the validated configuration owned by this engine.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Returns the current lifecycle state.
    pub fn state(&self) -> EngineState {
        self.state
    }

    /// Shuts the engine down.
    ///
    /// Shutdown is explicit and may only be performed once. Calling this method
    /// again returns [`EngineError::AlreadyShutdown`].
    pub fn shutdown(&mut self) -> Result<(), EngineError> {
        match self.state {
            EngineState::Initialized => {
                self.state = EngineState::Shutdown;
                Ok(())
            }
            EngineState::Shutdown => Err(EngineError::AlreadyShutdown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_initializes_with_default_config() {
        let config = EngineConfig::default();
        let engine = Engine::new(config.clone()).expect("default configuration should be valid");

        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.config(), &config);
    }

    #[test]
    fn engine_initializes_with_custom_config() {
        let config = EngineConfig::new(3, 64).expect("configuration should be valid");
        let engine = Engine::new(config.clone()).expect("configuration should be valid");

        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.config().worker_count, 3);
        assert_eq!(engine.config().queue_capacity, 64);
    }

    #[test]
    fn engine_rejects_invalid_config() {
        let config = EngineConfig {
            worker_count: 0,
            queue_capacity: 32,
        };

        let result = Engine::new(config);

        assert!(matches!(
            result,
            Err(EngineError::InvalidConfiguration {
                field: "worker_count",
                ..
            })
        ));
    }

    #[test]
    fn shutdown_transitions_engine_to_shutdown_state() {
        let mut engine =
            Engine::new(EngineConfig::default()).expect("configuration should be valid");

        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.shutdown(), Ok(()));
        assert_eq!(engine.state(), EngineState::Shutdown);
    }

    #[test]
    fn repeated_shutdown_is_rejected() {
        let mut engine =
            Engine::new(EngineConfig::default()).expect("configuration should be valid");

        engine.shutdown().expect("first shutdown should succeed");

        assert_eq!(engine.shutdown(), Err(EngineError::AlreadyShutdown));
        assert_eq!(engine.state(), EngineState::Shutdown);
    }
}
