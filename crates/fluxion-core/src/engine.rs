use crate::config::{EngineConfig, PREFETCH_BATCH};
use crate::error::EngineError;
use crate::queue::QueueError;
use crate::task::{TaskHandle, prepare_task};
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
        Self::build(config, PREFETCH_BATCH)
    }

    /// Creates an engine with an explicit internal prefetch batch.
    ///
    /// This constructor is hidden from the public documentation and exists only
    /// for benchmark and internal test builds. Applications should use
    /// [`Engine::new`].
    #[cfg(feature = "bench-internals")]
    #[doc(hidden)]
    pub fn new_with_prefetch(
        config: EngineConfig,
        prefetch_batch: usize,
    ) -> Result<Self, EngineError> {
        assert!(
            prefetch_batch > 0,
            "prefetch batch must be greater than zero"
        );
        Self::build(config, prefetch_batch)
    }

    fn build(config: EngineConfig, prefetch_batch: usize) -> Result<Self, EngineError> {
        config.validate()?;
        let workers = WorkerPool::new(&config, prefetch_batch)?;

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

    /// Returns whether the calling thread is one of this engine's workers.
    ///
    /// This can be used by integration layers to reject operations that would
    /// require a worker to join itself before taking ownership of the engine.
    pub fn is_worker_thread(&self) -> bool {
        self.workers.is_worker_thread(std::thread::current().id())
    }

    /// Submits a native Rust task and returns a handle to its result.
    ///
    /// The task starts executing once a worker is available. Every submitted
    /// task receives an independent handle, and handles may be waited on in any
    /// order. Dropping a handle without waiting is allowed; the task still
    /// runs, and its result is discarded without blocking the worker.
    ///
    /// A queued task can be cancelled through [`TaskHandle::cancel`]. Running
    /// and completed tasks are not interrupted, and a timed-out wait does not
    /// cancel the task. Task panics are caught and reported through the handle
    /// as [`crate::task::TaskError::Panicked`]. Returns [`EngineError::QueueFull`] when the
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

        let (handle, task) = prepare_task(task);

        self.workers.submit(task).map_err(|error| match error {
            QueueError::Full => EngineError::QueueFull,
            QueueError::Closed => EngineError::AlreadyShutdown,
        })?;

        Ok(handle)
    }

    /// Shuts the engine down gracefully.
    ///
    /// New submissions are rejected, tasks already accepted by the queue are
    /// completed, and all worker threads are joined. Shutdown may only be
    /// performed once. Calling this method again returns
    /// [`EngineError::AlreadyShutdown`].
    ///
    /// Calling this method from one of this engine's worker threads returns
    /// [`EngineError::ShutdownFromWorker`] before the queue is closed or the
    /// lifecycle state changes. A rejected shutdown leaves the engine usable.
    pub fn shutdown(&mut self) -> Result<(), EngineError> {
        if self.state == EngineState::Shutdown {
            return Err(EngineError::AlreadyShutdown);
        }

        let result = self.workers.shutdown();

        if matches!(&result, Err(EngineError::ShutdownFromWorker)) {
            return result;
        }

        self.state = EngineState::Shutdown;
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::task::{TaskError, TaskStatus, TaskWaitOutcome};

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
    fn shutdown_from_worker_is_rejected_and_engine_remains_usable() {
        let engine = Arc::new(Mutex::new(
            Engine::new(test_config(1, 4)).expect("configuration should be valid"),
        ));
        let worker_engine = Arc::clone(&engine);
        let handle = engine
            .lock()
            .expect("engine lock should not be poisoned")
            .submit(move || {
                worker_engine
                    .lock()
                    .expect("engine lock should not be poisoned")
                    .shutdown()
            })
            .expect("task should be accepted");

        assert_eq!(handle.wait(), Ok(Err(EngineError::ShutdownFromWorker)));
        assert_eq!(
            engine
                .lock()
                .expect("engine lock should not be poisoned")
                .state(),
            EngineState::Initialized
        );

        let marker = engine
            .lock()
            .expect("engine lock should not be poisoned")
            .submit(|| 42)
            .expect("engine should remain usable");
        assert_eq!(marker.wait(), Ok(42));
        assert_eq!(
            engine
                .lock()
                .expect("engine lock should not be poisoned")
                .shutdown(),
            Ok(())
        );
    }

    #[test]
    fn multiple_workers_cannot_shut_down_their_own_engine() {
        const WORKER_COUNT: usize = 4;

        let engine = Arc::new(Mutex::new(
            Engine::new(test_config(WORKER_COUNT, WORKER_COUNT))
                .expect("configuration should be valid"),
        ));
        let barrier = Arc::new(Barrier::new(WORKER_COUNT));

        let handles: Vec<_> = (0..WORKER_COUNT)
            .map(|_| {
                let worker_engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);

                engine
                    .lock()
                    .expect("engine lock should not be poisoned")
                    .submit(move || {
                        barrier.wait();
                        worker_engine
                            .lock()
                            .expect("engine lock should not be poisoned")
                            .shutdown()
                    })
                    .expect("task should be accepted")
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.wait(), Ok(Err(EngineError::ShutdownFromWorker)));
        }

        assert_eq!(
            engine
                .lock()
                .expect("engine lock should not be poisoned")
                .state(),
            EngineState::Initialized
        );

        let marker = engine
            .lock()
            .expect("engine lock should not be poisoned")
            .submit(|| 7)
            .expect("engine should remain usable");
        assert_eq!(marker.wait(), Ok(7));
        assert_eq!(
            engine
                .lock()
                .expect("engine lock should not be poisoned")
                .shutdown(),
            Ok(())
        );
    }

    #[test]
    fn dropping_final_engine_reference_on_worker_drains_accepted_tasks() {
        let engine = Arc::new(Mutex::new(
            Engine::new(test_config(2, 32)).expect("configuration should be valid"),
        ));
        let (blocker_started_sender, blocker_started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let blocker = engine
            .lock()
            .expect("engine lock should not be poisoned")
            .submit(move || {
                blocker_started_sender
                    .send(())
                    .expect("blocker start receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocker should be released");
                1
            })
            .expect("blocker should be accepted");
        blocker_started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocker should start");

        let (drop_started_sender, drop_started_receiver) = mpsc::channel();
        let (drop_proceed_sender, drop_proceed_receiver) = mpsc::channel();
        let (drop_done_sender, drop_done_receiver) = mpsc::channel();
        let worker_engine = Arc::clone(&engine);
        let destructor = engine
            .lock()
            .expect("engine lock should not be poisoned")
            .submit(move || {
                drop_started_sender
                    .send(())
                    .expect("destructor start receiver should be available");
                drop_proceed_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("destructor should be released");
                drop(worker_engine);
                drop_done_sender
                    .send(())
                    .expect("destructor completion receiver should be available");
            })
            .expect("destructor should be accepted");
        drop_started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("destructor should start");

        let queued: Vec<_> = (0..8)
            .map(|value| {
                engine
                    .lock()
                    .expect("engine lock should not be poisoned")
                    .submit(move || value)
                    .expect("queued task should be accepted")
            })
            .collect();

        drop(engine);
        drop_proceed_sender
            .send(())
            .expect("destructor should be released");
        drop_done_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("destructor should drop the final engine reference");

        release_sender.send(()).expect("blocker should be released");

        assert_eq!(blocker.wait(), Ok(1));
        assert_eq!(destructor.wait(), Ok(()));
        for (index, handle) in queued.into_iter().enumerate() {
            assert_eq!(handle.wait(), Ok(index));
        }
    }

    #[test]
    fn concurrent_submit_and_shutdown_admit_or_reject_deterministically() {
        const SUBMITTERS: usize = 4;
        const TASKS_PER_SUBMITTER: usize = 64;
        const TOTAL_TASKS: usize = SUBMITTERS * TASKS_PER_SUBMITTER;

        let engine = Arc::new(Mutex::new(
            Engine::new(test_config(2, TOTAL_TASKS)).expect("configuration should be valid"),
        ));
        let barrier = Arc::new(Barrier::new(SUBMITTERS + 1));
        let accepted = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let handles = Arc::new(Mutex::new(Vec::new()));

        thread::scope(|scope| {
            for _ in 0..SUBMITTERS {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                let accepted = Arc::clone(&accepted);
                let rejected = Arc::clone(&rejected);
                let handles = Arc::clone(&handles);

                scope.spawn(move || {
                    barrier.wait();

                    for _ in 0..TASKS_PER_SUBMITTER {
                        match engine
                            .lock()
                            .expect("engine lock should not be poisoned")
                            .submit(|| 1)
                        {
                            Ok(handle) => {
                                accepted.fetch_add(1, Ordering::SeqCst);
                                handles
                                    .lock()
                                    .expect("handles lock should not be poisoned")
                                    .push(handle);
                            }
                            Err(EngineError::AlreadyShutdown) => {
                                rejected.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(error) => panic!("unexpected submission error: {error}"),
                        }
                    }
                });
            }

            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                assert_eq!(
                    engine
                        .lock()
                        .expect("engine lock should not be poisoned")
                        .shutdown(),
                    Ok(())
                );
            });
        });

        assert_eq!(
            accepted.load(Ordering::SeqCst) + rejected.load(Ordering::SeqCst),
            TOTAL_TASKS
        );
        let handles = Arc::try_unwrap(handles)
            .expect("handles should have a single owner")
            .into_inner()
            .expect("handles lock should not be poisoned");
        assert_eq!(handles.len(), accepted.load(Ordering::SeqCst));

        for handle in handles {
            assert_eq!(handle.wait(), Ok(1));
        }
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
    fn shutdown_drains_queued_and_prefetched_tasks() {
        const TASKS: usize = 32;

        let mut engine = Engine::new(test_config(1, 64)).expect("configuration should be valid");
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
                0
            })
            .expect("blocking task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocking task should start");

        let handles: Vec<_> = (1..=TASKS)
            .map(|value| {
                engine
                    .submit(move || value)
                    .expect("queued task should be accepted")
            })
            .collect();

        let shutdown = thread::spawn(move || engine.shutdown());
        release_sender
            .send(())
            .expect("blocking task should be released");

        assert_eq!(
            shutdown.join().expect("shutdown thread should not panic"),
            Ok(())
        );
        assert_eq!(blocking_handle.wait(), Ok(0));

        for (index, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.wait(), Ok(index + 1));
        }
    }

    #[test]
    fn repeated_batched_submission_completes_all_tasks() {
        const ROUNDS: usize = 25;
        const TASKS_PER_ROUND: usize = 256;

        let mut engine =
            Engine::new(test_config(8, TASKS_PER_ROUND)).expect("configuration should be valid");
        let completed = Arc::new(AtomicUsize::new(0));

        for _ in 0..ROUNDS {
            let handles: Vec<_> = (0..TASKS_PER_ROUND)
                .map(|_| {
                    let completed = Arc::clone(&completed);
                    engine
                        .submit(move || {
                            completed.fetch_add(1, Ordering::SeqCst);
                        })
                        .expect("task should be accepted")
                })
                .collect();

            for handle in handles {
                handle.wait().expect("task should complete successfully");
            }
        }

        assert_eq!(completed.load(Ordering::SeqCst), ROUNDS * TASKS_PER_ROUND);
        assert_eq!(engine.shutdown(), Ok(()));
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

    #[test]
    fn cancelling_queued_task_prevents_execution() {
        let mut engine = Engine::new(test_config(1, 8)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let blocker = engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
            })
            .expect("blocking task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocking task should start");

        let executed = Arc::new(AtomicUsize::new(0));
        let task_executed = Arc::clone(&executed);
        let handle = engine
            .submit(move || {
                task_executed.fetch_add(1, Ordering::SeqCst);
            })
            .expect("queued task should be accepted");

        assert_eq!(handle.status(), TaskStatus::Queued);
        assert!(handle.cancel());
        assert!(handle.cancelled());
        assert!(handle.done());
        assert_eq!(handle.wait(), Err(TaskError::Cancelled));

        release_sender
            .send(())
            .expect("blocking task should be released");
        assert_eq!(blocker.wait(), Ok(()));

        let marker = engine
            .submit(|| 17)
            .expect("marker task should be accepted");
        assert_eq!(marker.wait(), Ok(17));
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn wait_timeout_does_not_cancel_queued_task() {
        let mut engine = Engine::new(test_config(1, 8)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let blocker = engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
            })
            .expect("blocking task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocking task should start");

        let handle = engine
            .submit(|| 42)
            .expect("queued task should be accepted");

        let handle = match handle.wait_timeout(Duration::from_millis(10)) {
            TaskWaitOutcome::TimedOut(handle) => handle,
            TaskWaitOutcome::Completed(result) => {
                panic!("queued task should not complete: {result:?}")
            }
        };

        assert_eq!(handle.status(), TaskStatus::Queued);
        release_sender
            .send(())
            .expect("blocking task should be released");

        assert_eq!(blocker.wait(), Ok(()));
        assert_eq!(handle.wait(), Ok(42));
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn wait_timeout_does_not_interrupt_running_task() {
        let mut engine = Engine::new(test_config(1, 8)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let handle = engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("running task should be released");
                33
            })
            .expect("task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("task should start");

        let handle = match handle.wait_timeout(Duration::from_millis(10)) {
            TaskWaitOutcome::TimedOut(handle) => handle,
            TaskWaitOutcome::Completed(result) => {
                panic!("running task should not complete: {result:?}")
            }
        };

        assert_eq!(handle.status(), TaskStatus::Running);
        assert!(!handle.cancel());

        release_sender
            .send(())
            .expect("running task should be released");
        assert_eq!(handle.wait(), Ok(33));
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn task_can_be_cancelled_after_cancellation_timeout() {
        let mut engine = Engine::new(test_config(1, 8)).expect("configuration should be valid");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let blocker = engine
            .submit(move || {
                started_sender
                    .send(())
                    .expect("started receiver should be available");
                release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
            })
            .expect("blocking task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocking task should start");

        let handle = engine
            .submit(|| 11)
            .expect("queued task should be accepted");
        let handle = match handle.wait_timeout(Duration::ZERO) {
            TaskWaitOutcome::TimedOut(handle) => handle,
            TaskWaitOutcome::Completed(result) => {
                panic!("queued task should not complete: {result:?}")
            }
        };

        assert!(handle.cancel());
        assert_eq!(handle.wait(), Err(TaskError::Cancelled));

        release_sender
            .send(())
            .expect("blocking task should be released");
        assert_eq!(blocker.wait(), Ok(()));
        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[test]
    fn concurrent_cancellation_executes_uncancelled_tasks_exactly_once() {
        const SUBMITTERS: usize = 4;
        const TASKS_PER_SUBMITTER: usize = 128;
        const CANCELLERS: usize = 8;
        const TOTAL_TASKS: usize = SUBMITTERS * TASKS_PER_SUBMITTER;

        let mut engine =
            Engine::new(test_config(4, TOTAL_TASKS)).expect("configuration should be valid");
        let executions: Arc<Vec<AtomicUsize>> =
            Arc::new((0..TOTAL_TASKS).map(|_| AtomicUsize::new(0)).collect());

        let handles = thread::scope(|scope| {
            let submitters: Vec<_> = (0..SUBMITTERS)
                .map(|submitter| {
                    let engine = &engine;
                    let executions = Arc::clone(&executions);

                    scope.spawn(move || {
                        (0..TASKS_PER_SUBMITTER)
                            .map(|offset| {
                                let index = submitter * TASKS_PER_SUBMITTER + offset;
                                let executions = Arc::clone(&executions);
                                engine
                                    .submit(move || {
                                        executions[index].fetch_add(1, Ordering::SeqCst);
                                    })
                                    .expect("task should be accepted")
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();

            submitters
                .into_iter()
                .flat_map(|submitter| submitter.join().expect("submitter should not panic"))
                .collect::<Vec<_>>()
        });

        let mut buckets: Vec<Vec<(usize, TaskHandle<()>)>> =
            (0..CANCELLERS).map(|_| Vec::new()).collect();
        for (index, handle) in handles.into_iter().enumerate() {
            buckets[index % CANCELLERS].push((index, handle));
        }

        let cancelled = thread::scope(|scope| {
            let cancellers: Vec<_> = buckets
                .into_iter()
                .map(|bucket| {
                    scope.spawn(move || {
                        bucket
                            .into_iter()
                            .map(|(index, handle)| {
                                let was_cancelled = handle.cancel();
                                let result = handle.wait();
                                (index, was_cancelled, result)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();

            cancellers
                .into_iter()
                .flat_map(|canceller| canceller.join().expect("canceller should not panic"))
                .collect::<Vec<_>>()
        });

        for (index, was_cancelled, result) in cancelled {
            if was_cancelled {
                assert_eq!(result, Err(TaskError::Cancelled));
                assert_eq!(executions[index].load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(result, Ok(()));
                assert_eq!(executions[index].load(Ordering::SeqCst), 1);
            }
        }

        assert_eq!(engine.shutdown(), Ok(()));
    }

    #[cfg(feature = "bench-internals")]
    #[test]
    fn cancelling_prefetched_task_prevents_execution() {
        let mut engine =
            Engine::new_with_prefetch(test_config(1, 8), 2).expect("configuration should be valid");
        let (blocker_started_sender, blocker_started_receiver) = mpsc::channel();
        let (blocker_release_sender, blocker_release_receiver) = mpsc::channel();

        let blocker = engine
            .submit(move || {
                blocker_started_sender
                    .send(())
                    .expect("started receiver should be available");
                blocker_release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("blocking task should be released");
            })
            .expect("blocking task should be accepted");
        blocker_started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocking task should start");

        let (running_started_sender, running_started_receiver) = mpsc::channel();
        let (running_release_sender, running_release_receiver) = mpsc::channel();
        let running = engine
            .submit(move || {
                running_started_sender
                    .send(())
                    .expect("started receiver should be available");
                running_release_receiver
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("running task should be released");
            })
            .expect("running task should be accepted");

        let executed = Arc::new(AtomicUsize::new(0));
        let prefetched_executed = Arc::clone(&executed);
        let prefetched = engine
            .submit(move || {
                prefetched_executed.fetch_add(1, Ordering::SeqCst);
            })
            .expect("prefetched task should be accepted");

        blocker_release_sender
            .send(())
            .expect("blocking task should be released");
        assert_eq!(blocker.wait(), Ok(()));
        running_started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("running task should start");

        assert_eq!(prefetched.status(), TaskStatus::Queued);
        assert!(prefetched.cancel());
        assert_eq!(prefetched.wait(), Err(TaskError::Cancelled));

        running_release_sender
            .send(())
            .expect("running task should be released");
        assert_eq!(running.wait(), Ok(()));
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(engine.shutdown(), Ok(()));
    }
}
