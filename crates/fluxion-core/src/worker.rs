use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::queue::{QueueError, TaskQueue};

/// Owns the engine's worker threads and their shared task queue.
pub(crate) struct WorkerPool {
    queue: Arc<TaskQueue>,
    workers: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    pub(crate) fn new(config: &EngineConfig) -> Result<Self, EngineError> {
        let queue = Arc::new(TaskQueue::new(config.queue_capacity));
        let mut workers = Vec::with_capacity(config.worker_count);

        for worker_index in 0..config.worker_count {
            let worker_queue = Arc::clone(&queue);
            let spawn_result = thread::Builder::new()
                .name(format!("fluxion-worker-{worker_index}"))
                .spawn(move || worker_loop(worker_queue));

            match spawn_result {
                Ok(handle) => workers.push(handle),
                Err(source) => {
                    queue.close();
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

        Ok(Self { queue, workers })
    }

    pub(crate) fn submit<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.queue.push(Box::new(task))
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), EngineError> {
        self.queue.close();

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

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.queue.close();
        let _ = self.join_workers();
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

fn worker_loop(queue: Arc<TaskQueue>) {
    while let Some(task) = queue.pop() {
        let _ = panic::catch_unwind(AssertUnwindSafe(task));
    }
}
