use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc;

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::queue::QueueError;
use crate::task::{TaskError, TaskHandle};
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

    /// Submits a native Rust task and returns a handle to its result.
    ///
    /// The task starts executing once a worker is available. Every submitted
    /// task receives an independent handle, and handles may be waited on in any
    /// order. Dropping a handle without waiting is allowed; the task still
    /// runs, and its result is discarded without blocking the worker.
    ///
    /// Task panics are caught and reported through the handle as
    /// [`TaskError::Panicked`]. Returns [`EngineError::QueueFull`] when the
    /// queue has no capacity and [`EngineError::AlreadyShutdown`] after the
    /// engine has been shut down.
    pub fn submit<F, T>(&self, task: F) -> Result<TaskHandle<T>, EngineError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if self.state == EngineState::Shutdown {
            return Err(EngineError::AlreadyShutdown);
        }

        let (sender, receiver) = mpsc::sync_channel::<Result<T, TaskError>>(1);
        let task = move || {
            let result = match panic::catch_unwind(AssertUnwindSafe(task)) {
                Ok(value) => Ok(value),
                Err(_) => Err(TaskError::Panicked),
            };
            let _ = sender.send(result);
        };

        self.workers.submit(task).map_err(|error| match error {
            QueueError::Full => EngineError::QueueFull,
            QueueError::Closed => EngineError::AlreadyShutdown,
        })?;

        Ok(TaskHandle::new(receiver))
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
    fn executes_submitted_task_and_returns_result() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");

        let handle = engine.submit(|| 21 * 2).expect("task should be accepted");

        assert_eq!(handle.wait(), Ok(42));
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn different_result_types_are_supported() {
        let mut engine = Engine::new(test_config(2, 8)).expect("configuration should be valid");

        let string_handle = engine
            .submit(|| String::from("fluxion"))
            .expect("string task should be accepted");
        let vector_handle = engine
            .submit(|| vec![1_u64, 2, 3])
            .expect("vector task should be accepted");
        let tuple_handle = engine
            .submit(|| (7_i32, true))
            .expect("tuple task should be accepted");

        assert_eq!(string_handle.wait(), Ok(String::from("fluxion")));
        assert_eq!(vector_handle.wait(), Ok(vec![1_u64, 2, 3]));
        assert_eq!(tuple_handle.wait(), Ok((7_i32, true)));
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn multiple_independent_task_handles_retrieve_results_out_of_order() {
        let mut engine = Engine::new(test_config(2, 4)).expect("configuration should be valid");
        let mut handles = Vec::new();

        for value in [10, 20, 30] {
            handles.push(
                engine
                    .submit(move || value)
                    .expect("task should be accepted"),
            );
        }

        assert_eq!(handles.remove(2).wait(), Ok(30));
        assert_eq!(handles.remove(0).wait(), Ok(10));
        assert_eq!(handles.remove(0).wait(), Ok(20));
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn dropped_handles_do_not_block_workers() {
        const DROPPED_TASKS: usize = 32;

        let mut engine = Engine::new(test_config(1, 64)).expect("configuration should be valid");
        let completed = Arc::new(AtomicUsize::new(0));

        for _ in 0..DROPPED_TASKS {
            let completed = Arc::clone(&completed);
            let handle = engine
                .submit(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                    completed.load(Ordering::SeqCst)
                })
                .expect("task should be accepted");
            drop(handle);
        }

        let marker = engine
            .submit(|| 99)
            .expect("marker task should be accepted");

        assert_eq!(marker.wait(), Ok(99));
        assert_eq!(completed.load(Ordering::SeqCst), DROPPED_TASKS);
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn supports_concurrent_result_retrieval() {
        const TASK_COUNT: usize = 8;

        let mut engine =
            Engine::new(test_config(2, TASK_COUNT)).expect("configuration should be valid");
        let handles: Vec<TaskHandle<usize>> = (0..TASK_COUNT)
            .map(|value| {
                engine
                    .submit(move || value * 3)
                    .expect("task should be accepted")
            })
            .collect();

        let results = thread::scope(|scope| {
            let waiters: Vec<_> = handles
                .into_iter()
                .enumerate()
                .map(|(index, handle)| scope.spawn(move || (index, handle.wait())))
                .collect();

            waiters
                .into_iter()
                .map(|waiter| waiter.join().expect("waiter thread should not panic"))
                .collect::<Vec<_>>()
        });

        for (index, result) in results {
            assert_eq!(result, Ok(index * 3));
        }
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn task_panic_is_propagated_without_terminating_worker_pool() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");

        let panic_handle = engine
            .submit(|| -> usize { panic!("intentional task panic") })
            .expect("panicking task should be accepted");
        let success_handle = engine
            .submit(|| 42)
            .expect("subsequent task should be accepted");

        assert_eq!(panic_handle.wait(), Err(TaskError::Panicked));
        assert_eq!(success_handle.wait(), Ok(42));
        assert_eq!(engine.state(), EngineState::Initialized);
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

        engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
                1
            })
            .expect("first task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("first task should start");

        let second = engine.submit(|| 2);
        let third = engine.submit(|| 3);

        release_sender
            .send(())
            .expect("blocking task should be released");
        let shutdown = engine.shutdown();

        assert!(second.is_ok());
        assert!(matches!(third, Err(EngineError::QueueFull)));
        assert_eq!(shutdown, Ok(()));

        let second = second.expect("second task should have been accepted");
        assert_eq!(second.wait(), Ok(2));
    }

    #[test]
    fn shutdown_waits_for_accepted_tasks_and_preserves_results() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let blocking_handle = engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
                1
            })
            .expect("first task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("first task should start");

        let queued_handle = engine.submit(|| 2).expect("second task should be accepted");

        let shutdown = thread::spawn(move || engine.shutdown());
        release_sender
            .send(())
            .expect("blocking task should be released");

        let shutdown_result = shutdown.join().expect("shutdown thread should not panic");

        assert_eq!(shutdown_result, Ok(()));
        assert_eq!(blocking_handle.wait(), Ok(1));
        assert_eq!(queued_handle.wait(), Ok(2));
    }

    #[test]
    fn submissions_are_rejected_after_shutdown() {
        let mut engine = Engine::new(test_config(1, 4)).expect("configuration should be valid");
        engine.shutdown().expect("shutdown should succeed");

        assert!(matches!(
            engine.submit(|| {}),
            Err(EngineError::AlreadyShutdown)
        ));
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
