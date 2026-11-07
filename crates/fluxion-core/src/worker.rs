use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle, ThreadId};

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::queue::{QueueError, TaskQueue};

struct WorkerThread {
    index: usize,
    id: ThreadId,
    handle: JoinHandle<()>,
}

/// Owns the engine's worker threads and their shared task queue.
pub(crate) struct WorkerPool {
    queue: Arc<TaskQueue>,
    workers: Vec<WorkerThread>,
}

impl WorkerPool {
    pub(crate) fn new(config: &EngineConfig, prefetch_batch: usize) -> Result<Self, EngineError> {
        debug_assert!(prefetch_batch > 0);

        let queue = Arc::new(TaskQueue::new(config.queue_capacity));
        let mut workers = Vec::with_capacity(config.worker_count);

        for worker_index in 0..config.worker_count {
            let worker_queue = Arc::clone(&queue);
            let worker_count = config.worker_count;
            let spawn_result = thread::Builder::new()
                .name(format!("fluxion-worker-{worker_index}"))
                .spawn(move || {
                    worker_loop(worker_index, worker_count, worker_queue, prefetch_batch);
                });

            match spawn_result {
                Ok(handle) => workers.push(WorkerThread {
                    index: worker_index,
                    id: handle.thread().id(),
                    handle,
                }),
                Err(source) => {
                    queue.close();
                    for worker in workers.drain(..) {
                        let _ = worker.handle.join();
                    }
                    return Err(EngineError::WorkerSpawnFailed {
                        worker_index,
                        kind: source.kind(),
                    });
                }
            }
        }

        Ok(Self { queue, workers })
    }

    pub(crate) fn submit<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.queue.push(Box::new(task))
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), EngineError> {
        if self.is_worker_thread(thread::current().id()) {
            return Err(EngineError::ShutdownFromWorker);
        }

        self.queue.close();

        match self.join_workers() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) fn is_worker_thread(&self, thread_id: ThreadId) -> bool {
        self.workers.iter().any(|worker| worker.id == thread_id)
    }

    fn join_workers(&mut self) -> Option<EngineError> {
        join_worker_threads(std::mem::take(&mut self.workers))
    }

    /// Drops a pool whose final owner is one of its own workers.
    ///
    /// The current worker cannot join itself. A dedicated reaper thread closes
    /// the queue, drains accepted tasks through the existing workers, and joins
    /// every worker after the callback returns. If the reaper cannot be
    /// spawned, the queue is closed and the remaining handles are detached so
    /// workers can still drain accepted tasks and exit.
    fn shutdown_from_current_worker(&mut self) {
        let queue = Arc::clone(&self.queue);
        let workers = std::mem::take(&mut self.workers);
        let spawn_result = thread::Builder::new()
            .name(String::from("fluxion-worker-reaper"))
            .spawn(move || {
                queue.close();
                let _ = join_worker_threads(workers);
            });

        if spawn_result.is_err() {
            self.queue.close();
        }
    }
}

fn join_worker_threads(workers: Vec<WorkerThread>) -> Option<EngineError> {
    let mut first_failure = None;

    for worker in workers {
        if worker.handle.join().is_err() && first_failure.is_none() {
            first_failure = Some(EngineError::WorkerFailed {
                worker_index: worker.index,
            });
        }
    }

    first_failure
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        if self.is_worker_thread(thread::current().id()) {
            self.shutdown_from_current_worker();
        } else {
            self.queue.close();
            let _ = self.join_workers();
        }
    }
}

impl fmt::Debug for WorkerPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerPool")
            .field("workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

fn worker_loop(
    worker_id: usize,
    worker_count: usize,
    queue: Arc<TaskQueue>,
    prefetch_batch: usize,
) {
    let mut prefetched = VecDeque::with_capacity(prefetch_batch);

    // An internal failure must not strand tasks already claimed by this worker.
    // Finish the local buffer before resuming the panic so the pool reports the
    // worker failure while preserving task completion guarantees.
    let failure = panic::catch_unwind(AssertUnwindSafe(|| {
        loop {
            if prefetched.is_empty()
                && !queue.claim_into(&mut prefetched, prefetch_batch, worker_count)
            {
                break;
            }

            while let Some(task) = prefetched.pop_front() {
                let _ = panic::catch_unwind(AssertUnwindSafe(task));
            }
        }
    }));

    if let Err(payload) = failure {
        drain_prefetched(&mut prefetched);
        panic::resume_unwind(worker_failure(worker_id, payload));
    }
}

fn drain_prefetched(prefetched: &mut VecDeque<crate::queue::Task>) {
    while let Some(task) = prefetched.pop_front() {
        let _ = panic::catch_unwind(AssertUnwindSafe(task));
    }
}

fn worker_failure(worker_id: usize, payload: Box<dyn Any + Send>) -> Box<dyn Any + Send> {
    // Preserve the panic message while recording which worker failed.
    let message = match payload.downcast_ref::<&str>() {
        Some(message) => format!("worker {worker_id} failed: {message}"),
        None => match payload.downcast_ref::<String>() {
            Some(message) => format!("worker {worker_id} failed: {message}"),
            None => format!("worker {worker_id} failed"),
        },
    };

    Box::new(message)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::queue::Task;

    fn record_task(records: Arc<Mutex<Vec<usize>>>, value: usize) -> Task {
        Box::new(move || {
            records
                .lock()
                .expect("record lock should not be poisoned")
                .push(value);
        })
    }

    fn run_worker(queue: Arc<TaskQueue>, prefetch_batch: usize) {
        worker_loop(0, 1, queue, prefetch_batch);
    }

    #[test]
    fn worker_executes_local_prefetch_before_claiming_more() {
        let queue = Arc::new(TaskQueue::new(32));
        let records = Arc::new(Mutex::new(Vec::new()));

        for value in 0..20 {
            queue
                .push(record_task(Arc::clone(&records), value))
                .expect("task should be accepted");
        }
        queue.close();

        let worker = {
            let queue = Arc::clone(&queue);
            thread::spawn(move || run_worker(queue, 8))
        };
        worker.join().expect("worker should not panic");

        assert_eq!(
            *records.lock().expect("record lock should not be poisoned"),
            (0..20).collect::<Vec<_>>()
        );
    }

    #[test]
    fn task_panic_does_not_discard_prefetched_tasks() {
        let queue = Arc::new(TaskQueue::new(8));
        let records = Arc::new(Mutex::new(Vec::new()));

        for value in 0..8 {
            if value == 2 {
                queue
                    .push(Box::new(|| panic!("intentional task panic")))
                    .expect("panicking task should be accepted");
            } else {
                queue
                    .push(record_task(Arc::clone(&records), value))
                    .expect("task should be accepted");
            }
        }
        queue.close();

        let worker = {
            let queue = Arc::clone(&queue);
            thread::spawn(move || run_worker(queue, 8))
        };
        worker
            .join()
            .expect("task panic should not terminate worker");

        assert_eq!(
            *records.lock().expect("record lock should not be poisoned"),
            [0, 1, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn closing_an_idle_queue_stops_worker() {
        let queue = Arc::new(TaskQueue::new(4));
        let worker = {
            let queue = Arc::clone(&queue);
            thread::spawn(move || run_worker(queue, 4))
        };

        queue.close();
        worker.join().expect("worker should not panic");
        assert!(queue.is_closed());
    }

    #[test]
    fn every_task_executes_exactly_once_across_prefetch_batches() {
        const TASKS: usize = 100;

        let queue = Arc::new(TaskQueue::new(TASKS));
        let completed = Arc::new(AtomicUsize::new(0));

        for _ in 0..TASKS {
            let completed = Arc::clone(&completed);
            queue
                .push(Box::new(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                }))
                .expect("task should be accepted");
        }
        queue.close();

        let worker = {
            let queue = Arc::clone(&queue);
            thread::spawn(move || run_worker(queue, 8))
        };
        worker.join().expect("worker should not panic");

        assert_eq!(completed.load(Ordering::SeqCst), TASKS);
    }
}
