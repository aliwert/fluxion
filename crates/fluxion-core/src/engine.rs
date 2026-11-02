use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::queue::QueueError;
use crate::worker::WorkerPool;

/// Lifecycle state of an [`Engine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    /// The engine has been created, configured, and its workers are running.
    Initialized,
    /// The engine has been shut down and cannot be used again.
    Shutdown,
}

/// Primary entry point into the Fluxion execution runtime.
///
/// The engine owns its configuration, lifecycle state, task queue, and worker
/// threads. It is created with validated configuration and must be shut down
/// explicitly once it is no longer needed.
#[derive(Debug)]
pub struct Engine {
    config: EngineConfig,
    state: EngineState,
    workers: WorkerPool,
}

impl Engine {
    /// Creates an engine from the supplied configuration.
    ///
    /// The configuration is validated before any engine state is created. On
    /// success, the configured number of worker threads is started.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        config.validate()?;
        let workers = WorkerPool::new(&config)?;

        Ok(Self {
            config,
            state: EngineState::Initialized,
            workers,
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

    /// Submits a native Rust task for execution.
    ///
    /// Submission is fire-and-forget: this method returns once the task has
    /// been accepted by the bounded queue. The task's result, if any, must be
    /// communicated through the task's own synchronization primitives.
    ///
    /// Returns [`EngineError::QueueFull`] when the queue has no capacity and
    /// [`EngineError::AlreadyShutdown`] after the engine has been shut down.
    pub fn submit<F>(&self, task: F) -> Result<(), EngineError>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.state == EngineState::Shutdown {
            return Err(EngineError::AlreadyShutdown);
        }

        self.workers.submit(task).map_err(|error| match error {
            QueueError::Full => EngineError::QueueFull,
            QueueError::Closed => EngineError::AlreadyShutdown,
        })
    }

    /// Shuts the engine down gracefully.
    ///
    /// New submissions are rejected, tasks already accepted by the queue are
    /// completed, and all worker threads are joined. Shutdown may only be
    /// performed once. Calling this method again returns
    /// [`EngineError::AlreadyShutdown`].
    pub fn shutdown(&mut self) -> Result<(), EngineError> {
        if self.state == EngineState::Shutdown {
            return Err(EngineError::AlreadyShutdown);
        }

        let result = self.workers.shutdown();
        self.state = EngineState::Shutdown;
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn test_config(worker_count: usize, queue_capacity: usize) -> EngineConfig {
        EngineConfig::new(worker_count, queue_capacity).expect("test configuration should be valid")
    }

    #[test]
    fn engine_initializes_with_default_config() {
        let config = EngineConfig::default();
        let mut engine =
            Engine::new(config.clone()).expect("default configuration should be valid");

        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.config(), &config);
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn engine_initializes_with_custom_config() {
        let config = EngineConfig::new(3, 64).expect("configuration should be valid");
        let mut engine = Engine::new(config.clone()).expect("configuration should be valid");

        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.config().worker_count, 3);
        assert_eq!(engine.config().queue_capacity, 64);
        assert_eq!(engine.shutdown(), Ok(()));
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
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");

        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.shutdown(), Ok(()));
        assert_eq!(engine.state(), EngineState::Shutdown);
    }

    #[test]
    fn repeated_shutdown_is_rejected() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");

        engine.shutdown().expect("first shutdown should succeed");

        assert_eq!(engine.shutdown(), Err(EngineError::AlreadyShutdown));
        assert_eq!(engine.state(), EngineState::Shutdown);
    }

    #[test]
    fn executes_submitted_task() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");
        let (sender, receiver) = mpsc::channel();

        engine
            .submit(move || sender.send(42).expect("receiver should be available"))
            .expect("task should be accepted");

        assert_eq!(
            receiver
                .recv_timeout(TEST_TIMEOUT)
                .expect("task should complete"),
            42
        );
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn multiple_workers_execute_tasks_concurrently() {
        const WORKER_COUNT: usize = 3;

        let mut engine = Engine::new(test_config(WORKER_COUNT, WORKER_COUNT))
            .expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        for _ in 0..WORKER_COUNT {
            let started_sender = started_sender.clone();
            let gate = Arc::clone(&gate);

            engine
                .submit(move || {
                    started_sender
                        .send(())
                        .expect("started receiver should be available");
                    let (lock, condition) = &*gate;
                    let mut released = lock.lock().expect("gate lock should not be poisoned");
                    while !*released {
                        released = condition.wait(released).expect("gate wait should not fail");
                    }
                })
                .expect("task should be accepted");
        }

        drop(started_sender);

        let mut started_count = 0;
        for _ in 0..WORKER_COUNT {
            if started_receiver.recv_timeout(TEST_TIMEOUT).is_ok() {
                started_count += 1;
            } else {
                break;
            }
        }

        let (lock, condition) = &*gate;
        *lock.lock().expect("gate lock should not be poisoned") = true;
        condition.notify_all();

        assert_eq!(
            started_count, WORKER_COUNT,
            "all configured workers should run tasks concurrently"
        );
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn queue_rejects_submissions_when_full() {
        let mut engine = Engine::new(test_config(1, 1)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let (completed_sender, completed_receiver) = mpsc::channel();

        engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
            })
            .expect("first task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("first task should start");

        let second = engine.submit(move || {
            completed_sender
                .send(())
                .expect("completed receiver should be available");
        });
        let third = engine.submit(|| {});

        release_sender
            .send(())
            .expect("blocking task should be released");
        let shutdown = engine.shutdown();

        assert_eq!(second, Ok(()));
        assert_eq!(third, Err(EngineError::QueueFull));
        assert_eq!(shutdown, Ok(()));
        assert!(
            completed_receiver.recv_timeout(TEST_TIMEOUT).is_ok(),
            "queued task should run during shutdown"
        );
    }

    #[test]
    fn shutdown_waits_for_accepted_tasks() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let (completed_sender, completed_receiver) = mpsc::channel();

        engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
            })
            .expect("first task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("first task should start");

        engine
            .submit(move || {
                completed_sender
                    .send(())
                    .expect("completed receiver should be available");
            })
            .expect("second task should be accepted");

        let shutdown = thread::spawn(move || engine.shutdown());
        release_sender
            .send(())
            .expect("blocking task should be released");

        let shutdown_result = shutdown.join().expect("shutdown thread should not panic");

        assert_eq!(shutdown_result, Ok(()));
        assert!(
            completed_receiver.recv_timeout(TEST_TIMEOUT).is_ok(),
            "accepted task should complete before shutdown returns"
        );
    }

    #[test]
    fn submissions_are_rejected_after_shutdown() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");
        engine.shutdown().expect("shutdown should succeed");

        assert_eq!(engine.submit(|| {}), Err(EngineError::AlreadyShutdown));
    }

    #[test]
    fn task_panic_does_not_terminate_worker_pool() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");
        let (sender, receiver) = mpsc::channel();

        engine
            .submit(|| panic!("intentional task panic"))
            .expect("panicking task should be accepted");
        engine
            .submit(move || {
                sender.send(()).expect("receiver should be available");
            })
            .expect("subsequent task should be accepted");

        assert!(
            receiver.recv_timeout(TEST_TIMEOUT).is_ok(),
            "worker should continue running tasks after a task panic"
        );
        assert_eq!(engine.state(), EngineState::Initialized);
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn supports_concurrent_task_submission() {
        const SUBMITTERS: usize = 4;
        const TASKS_PER_SUBMITTER: usize = 50;
        const TOTAL_TASKS: usize = SUBMITTERS * TASKS_PER_SUBMITTER;

        let mut engine = Engine::new(test_config(2, 256)).expect("configuration should be valid");
        let completed = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            let engine = &engine;

            for _ in 0..SUBMITTERS {
                let completed = Arc::clone(&completed);
                let failures = Arc::clone(&failures);

                scope.spawn(move || {
                    for _ in 0..TASKS_PER_SUBMITTER {
                        let completed = Arc::clone(&completed);
                        if engine
                            .submit(move || {
                                completed.fetch_add(1, Ordering::SeqCst);
                            })
                            .is_err()
                        {
                            failures.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }
        });

        assert_eq!(failures.load(Ordering::SeqCst), 0);
        assert_eq!(engine.shutdown(), Ok(()));
        assert_eq!(completed.load(Ordering::SeqCst), TOTAL_TASKS);
    }
}
