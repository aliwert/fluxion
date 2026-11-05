//! Rejected Phase 6D work-stealing experiment.
//!
//! The implementation remains feature-gated for reproducibility and future
//! investigation. It is not part of the production scheduler and is not
//! exposed through the Python API.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use crossbeam_deque::{Steal, Stealer, Worker};

use crate::config::{EngineConfig, PREFETCH_BATCH};
use crate::engine::EngineState;
use crate::error::EngineError;
use crate::queue::{ClaimOutcome, QueueError, Task, TaskQueue};
use crate::task::{TaskError, TaskHandle};

const MAX_STEAL_RETRIES: usize = 8;

/// Snapshot of experimental work-stealing counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StealStatsSnapshot {
    /// Total calls into a stealer.
    pub attempts: u64,
    /// Steals that returned a task.
    pub successes: u64,
    /// Steals that observed an empty victim queue.
    pub failures: u64,
    /// Steals that reported transient contention.
    pub retries: u64,
}

#[derive(Debug, Default)]
struct StealStats {
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    retries: AtomicU64,
}

impl StealStats {
    fn record_attempt(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StealStatsSnapshot {
        StealStatsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
        }
    }
}

/// Experimental engine that combines bounded global admission with
/// per-worker stealable queues.
///
/// This type is hidden from public documentation and is only available with
/// the `experimental-work-stealing` feature.
pub struct WorkStealingEngine {
    config: EngineConfig,
    state: EngineState,
    workers: WorkStealingWorkerPool,
}

impl WorkStealingEngine {
    /// Creates an experimental engine with the default prefetch batch.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        Self::build(config, PREFETCH_BATCH)
    }

    /// Creates an experimental engine with an explicit prefetch batch.
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
        let workers = WorkStealingWorkerPool::new(&config, prefetch_batch)?;

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

    /// Returns a snapshot of work-stealing counters.
    pub fn steal_stats(&self) -> StealStatsSnapshot {
        self.workers.steal_stats()
    }

    /// Submits a native Rust task and returns a handle to its result.
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

        self.workers
            .submit(Box::new(task))
            .map_err(|error| match error {
                QueueError::Full => EngineError::QueueFull,
                QueueError::Closed => EngineError::AlreadyShutdown,
            })?;

        Ok(TaskHandle::new(receiver))
    }

    /// Shuts the experimental engine down gracefully.
    pub fn shutdown(&mut self) -> Result<(), EngineError> {
        if self.state == EngineState::Shutdown {
            return Err(EngineError::AlreadyShutdown);
        }

        let result = self.workers.shutdown();
        self.state = EngineState::Shutdown;
        result
    }
}

impl fmt::Debug for WorkStealingEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkStealingEngine")
            .field("config", &self.config)
            .field("state", &self.state)
            .field("workers", &self.workers)
            .finish()
    }
}

struct WorkStealingWorkerPool {
    queue: Arc<TaskQueue>,
    signal: Arc<WorkSignal>,
    stats: Arc<StealStats>,
    workers: Vec<JoinHandle<()>>,
}

impl WorkStealingWorkerPool {
    fn new(config: &EngineConfig, prefetch_batch: usize) -> Result<Self, EngineError> {
        debug_assert!(prefetch_batch > 0);

        let queue = Arc::new(TaskQueue::new(config.queue_capacity));
        let signal = Arc::new(WorkSignal::new());
        let budget = Arc::new(TaskBudget::new(Arc::clone(&queue), Arc::clone(&signal)));
        let stats = Arc::new(StealStats::default());
        let worker_count = config.worker_count;
        let mut local_workers = Vec::with_capacity(worker_count);
        let mut stealers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let worker = Worker::new_lifo();
            stealers.push(worker.stealer());
            local_workers.push(worker);
        }

        let stealers = Arc::new(stealers);
        let mut workers = Vec::with_capacity(worker_count);

        for (worker_index, worker) in local_workers.into_iter().enumerate() {
            let context = WorkerContext {
                worker_id: worker_index,
                worker_count,
                prefetch_batch,
                queue: Arc::clone(&queue),
                signal: Arc::clone(&signal),
                budget: Arc::clone(&budget),
                stats: Arc::clone(&stats),
                stealers: Arc::clone(&stealers),
            };

            let spawn_result = thread::Builder::new()
                .name(format!("fluxion-worker-{worker_index}"))
                .spawn(move || worker_loop(worker, context));

            match spawn_result {
                Ok(handle) => workers.push(handle),
                Err(source) => {
                    queue.close();
                    signal.publish_all();
                    for worker in workers.drain(..) {
                        let _ = worker.join();
                    }
                    return Err(EngineError::WorkerSpawnFailed {
                        worker_index,
                        kind: source.kind(),
                    });
                }
            }
        }

        Ok(Self {
            queue,
            signal,
            stats,
            workers,
        })
    }

    fn submit(&self, task: Task) -> Result<(), QueueError> {
        self.queue.push(task)?;
        self.signal.publish();
        Ok(())
    }

    fn steal_stats(&self) -> StealStatsSnapshot {
        self.stats.snapshot()
    }

    fn shutdown(&mut self) -> Result<(), EngineError> {
        self.queue.close();
        self.signal.publish_all();

        match self.join_workers() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn join_workers(&mut self) -> Option<EngineError> {
        let mut first_failure = None;

        for (worker_index, worker) in self.workers.drain(..).enumerate() {
            if worker.join().is_err() && first_failure.is_none() {
                first_failure = Some(EngineError::WorkerFailed { worker_index });
            }
        }

        first_failure
    }
}

impl Drop for WorkStealingWorkerPool {
    fn drop(&mut self) {
        self.queue.close();
        self.signal.publish_all();
        let _ = self.join_workers();
    }
}

impl fmt::Debug for WorkStealingWorkerPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkStealingWorkerPool")
            .field("workers", &self.workers.len())
            .field("steal_stats", &self.stats.snapshot())
            .finish_non_exhaustive()
    }
}

struct WorkerContext {
    worker_id: usize,
    worker_count: usize,
    prefetch_batch: usize,
    queue: Arc<TaskQueue>,
    signal: Arc<WorkSignal>,
    budget: Arc<TaskBudget>,
    stats: Arc<StealStats>,
    stealers: Arc<Vec<Stealer<Task>>>,
}

fn worker_loop(worker: Worker<Task>, context: WorkerContext) {
    let mut local = worker;
    let mut staging = VecDeque::with_capacity(context.prefetch_batch);
    let mut scan_round = 0_usize;

    let failure = panic::catch_unwind(AssertUnwindSafe(|| {
        loop {
            if let Some(task) = local.pop() {
                execute_task(task, &context.budget);
                continue;
            }

            let observed = context.signal.snapshot();

            let claim_outcome = context.queue.try_claim_into(
                &mut staging,
                context.prefetch_batch,
                context.worker_count,
                |claim_count| context.budget.record_claim(claim_count),
            );

            if claim_outcome == ClaimOutcome::Claimed {
                while let Some(task) = staging.pop_front() {
                    local.push(task);
                }
                context.signal.publish();
                continue;
            }

            let current_scan = scan_round;
            scan_round = scan_round.wrapping_add(1);

            match steal_scan(
                context.worker_id,
                &context.stealers,
                current_scan,
                &context.stats,
            ) {
                StealScan::Task(task) => {
                    execute_task(task, &context.budget);
                    continue;
                }
                StealScan::Contended => {
                    thread::yield_now();
                    continue;
                }
                StealScan::Empty => {}
            }

            if context.queue.is_closed_and_empty() && context.budget.is_zero() {
                break;
            }

            context.signal.wait(observed);
        }
    }));

    if let Err(payload) = failure {
        drain_local(&mut local, &context.budget);
        drain_staging(&mut staging, &context.budget);
        context.signal.publish_all();
        panic::resume_unwind(worker_failure(context.worker_id, payload));
    }
}

enum StealScan {
    Task(Task),
    Empty,
    Contended,
}

fn steal_scan(
    worker_id: usize,
    stealers: &[Stealer<Task>],
    scan_round: usize,
    stats: &StealStats,
) -> StealScan {
    if stealers.len() <= 1 {
        return StealScan::Empty;
    }

    let start = scan_round % stealers.len();
    let mut contended = false;

    for offset in 0..stealers.len() {
        let index = (start + offset) % stealers.len();
        if index == worker_id {
            continue;
        }

        let mut retries = 0;

        loop {
            stats.record_attempt();

            match stealers[index].steal() {
                Steal::Success(task) => {
                    stats.record_success();
                    return StealScan::Task(task);
                }
                Steal::Empty => {
                    stats.record_failure();
                    break;
                }
                Steal::Retry => {
                    stats.record_retry();
                    retries += 1;

                    if retries >= MAX_STEAL_RETRIES {
                        contended = true;
                        break;
                    }

                    std::hint::spin_loop();
                }
            }
        }
    }

    if contended {
        StealScan::Contended
    } else {
        StealScan::Empty
    }
}

fn execute_task(task: Task, budget: &TaskBudget) {
    let _ = panic::catch_unwind(AssertUnwindSafe(task));
    budget.record_completion();
}

fn drain_local(local: &mut Worker<Task>, budget: &TaskBudget) {
    while let Some(task) = local.pop() {
        execute_task(task, budget);
    }
}

fn drain_staging(staging: &mut VecDeque<Task>, budget: &TaskBudget) {
    while let Some(task) = staging.pop_front() {
        execute_task(task, budget);
    }
}

fn worker_failure(worker_id: usize, payload: Box<dyn Any + Send>) -> Box<dyn Any + Send> {
    let message = match payload.downcast_ref::<&str>() {
        Some(message) => format!("worker {worker_id} failed: {message}"),
        None => match payload.downcast_ref::<String>() {
            Some(message) => format!("worker {worker_id} failed: {message}"),
            None => format!("worker {worker_id} failed"),
        },
    };

    Box::new(message)
}

struct TaskBudget {
    remaining: AtomicUsize,
    queue: Arc<TaskQueue>,
    signal: Arc<WorkSignal>,
}

impl TaskBudget {
    fn new(queue: Arc<TaskQueue>, signal: Arc<WorkSignal>) -> Self {
        Self {
            remaining: AtomicUsize::new(0),
            queue,
            signal,
        }
    }

    fn record_claim(&self, count: usize) {
        self.remaining.fetch_add(count, Ordering::AcqRel);
    }

    fn record_completion(&self) {
        let previous = self.remaining.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "task budget underflow");

        if previous == 1 && self.queue.is_closed() {
            self.signal.publish_all();
        }
    }

    fn is_zero(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }
}

struct SignalState {
    generation: u64,
    waiters: usize,
}

struct WorkSignal {
    state: Mutex<SignalState>,
    available: Condvar,
}

impl WorkSignal {
    fn new() -> Self {
        Self {
            state: Mutex::new(SignalState {
                generation: 0,
                waiters: 0,
            }),
            available: Condvar::new(),
        }
    }

    fn snapshot(&self) -> u64 {
        self.lock_state().generation
    }

    fn publish(&self) {
        let mut state = self.lock_state();
        state.generation = state.generation.wrapping_add(1);

        if state.waiters > 0 {
            self.available.notify_one();
        }
    }

    fn publish_all(&self) {
        let mut state = self.lock_state();
        state.generation = state.generation.wrapping_add(1);
        self.available.notify_all();
    }

    fn wait(&self, observed: u64) {
        let mut state = self.lock_state();

        while state.generation == observed {
            state.waiters += 1;
            let wait_result = self.available.wait(state);
            state = match wait_result {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.waiters -= 1;
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, SignalState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, Mutex};
    use std::time::Duration;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn config(worker_count: usize, queue_capacity: usize) -> EngineConfig {
        EngineConfig::new(worker_count, queue_capacity).expect("test configuration should be valid")
    }

    #[test]
    fn all_tasks_execute_exactly_once_across_workers() {
        const TASKS: usize = 2_000;

        let mut engine =
            WorkStealingEngine::new(config(4, TASKS)).expect("engine should initialize");
        let executions: Arc<Vec<AtomicUsize>> =
            Arc::new((0..TASKS).map(|_| AtomicUsize::new(0)).collect());
        let mut handles = Vec::with_capacity(TASKS);

        for index in 0..TASKS {
            let executions = Arc::clone(&executions);
            handles.push(
                engine
                    .submit(move || {
                        executions[index].fetch_add(1, Ordering::SeqCst);
                        index
                    })
                    .expect("task should be accepted"),
            );
        }

        for (index, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.wait(), Ok(index));
        }

        assert!(
            executions
                .iter()
                .all(|count| count.load(Ordering::SeqCst) == 1)
        );
        engine.shutdown().expect("engine should shut down");
    }

    #[test]
    fn task_panic_does_not_strand_other_tasks() {
        let mut engine = WorkStealingEngine::new(config(4, 64)).expect("engine should initialize");
        let completed = Arc::new(AtomicUsize::new(0));
        let panicking = engine
            .submit(|| panic!("intentional task panic"))
            .expect("task should be accepted");
        let mut handles = Vec::new();

        for _ in 0..32 {
            let completed = Arc::clone(&completed);
            handles.push(
                engine
                    .submit(move || {
                        completed.fetch_add(1, Ordering::SeqCst);
                    })
                    .expect("task should be accepted"),
            );
        }

        assert_eq!(panicking.wait(), Err(TaskError::Panicked));

        for handle in handles {
            assert_eq!(handle.wait(), Ok(()));
        }

        assert_eq!(completed.load(Ordering::SeqCst), 32);
        engine.shutdown().expect("engine should shut down");
    }

    #[test]
    fn shutdown_waits_for_gated_global_tasks() {
        const TASKS: usize = 512;

        let mut engine =
            WorkStealingEngine::new(config(4, TASKS + 4)).expect("engine should initialize");
        let release = Arc::new(Barrier::new(5));
        let (started_sender, started_receiver) = mpsc::sync_channel(4);

        for _ in 0..4 {
            let started_sender = started_sender.clone();
            let release = Arc::clone(&release);
            engine
                .submit(move || {
                    let _ = started_sender.send(());
                    release.wait();
                })
                .expect("blocking task should be accepted");
        }

        for _ in 0..4 {
            started_receiver
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking task should start");
        }

        let completed = Arc::new(AtomicUsize::new(0));

        for _ in 0..TASKS {
            let completed = Arc::clone(&completed);
            engine
                .submit(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                })
                .expect("task should be accepted");
        }

        release.wait();
        engine.shutdown().expect("engine should shut down");
        assert_eq!(completed.load(Ordering::SeqCst), TASKS);
    }

    #[test]
    fn idle_engine_shuts_down() {
        let mut engine = WorkStealingEngine::new(config(4, 32)).expect("engine should initialize");
        let (sender, receiver) = mpsc::sync_channel(1);

        thread::spawn(move || {
            let result = engine.shutdown();
            let _ = sender.send(result);
        });

        assert_eq!(
            receiver
                .recv_timeout(TEST_TIMEOUT)
                .expect("idle engine should shut down"),
            Ok(())
        );
    }

    #[test]
    fn queue_full_and_closed_are_reported() {
        let mut engine = WorkStealingEngine::new(config(1, 1)).expect("engine should initialize");
        let release = Arc::new(Barrier::new(2));
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let worker_release = Arc::clone(&release);

        engine
            .submit(move || {
                let _ = started_sender.send(());
                worker_release.wait();
            })
            .expect("blocking task should be accepted");
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("blocking task should start");

        engine
            .submit(|| {})
            .expect("queued task should be accepted");
        assert!(matches!(engine.submit(|| {}), Err(EngineError::QueueFull)));

        release.wait();
        engine.shutdown().expect("engine should shut down");
        assert!(matches!(
            engine.submit(|| {}),
            Err(EngineError::AlreadyShutdown)
        ));
    }

    #[test]
    fn stealing_removes_the_oldest_task() {
        let worker = Worker::new_lifo();
        let order = Arc::new(Mutex::new(Vec::new()));
        let record = |value: usize| {
            let order = Arc::clone(&order);
            move || {
                order
                    .lock()
                    .expect("order lock should not be poisoned")
                    .push(value);
            }
        };

        worker.push(Box::new(record(1)));
        worker.push(Box::new(record(2)));
        worker.push(Box::new(record(3)));

        let stolen = match worker.stealer().steal() {
            Steal::Success(task) => task,
            Steal::Empty => panic!("expected a stolen task, got an empty queue"),
            Steal::Retry => panic!("expected a stolen task, got transient contention"),
        };
        stolen();

        let owned = worker.pop().expect("owner should retain a task");
        owned();

        assert_eq!(
            *order.lock().expect("order lock should not be poisoned"),
            [1, 3]
        );
    }

    #[test]
    fn work_signal_wakes_a_waiter_after_publish() {
        let signal = Arc::new(WorkSignal::new());
        let observed = signal.snapshot();
        let waiter_signal = Arc::clone(&signal);
        let (sender, receiver) = mpsc::sync_channel(1);

        thread::spawn(move || {
            waiter_signal.wait(observed);
            let _ = sender.send(());
        });

        thread::sleep(Duration::from_millis(25));
        signal.publish();
        receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("waiter should wake");
    }
}
