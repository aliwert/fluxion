use std::fmt;
use std::sync::mpsc::Receiver;

use thiserror::Error;

/// Errors produced while waiting for a submitted task.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskError {
    /// The task panicked while it was running.
    #[error("task panicked")]
    Panicked,

    /// The worker exited without delivering a result.
    #[error("task worker disconnected before producing a result")]
    WorkerDisconnected,
}

/// Handle to the eventual result of a submitted task.
///
/// A handle may be dropped without affecting task execution. Dropping it only
/// discards the result and prevents the worker from delivering a value.
pub struct TaskHandle<T> {
    receiver: Receiver<Result<T, TaskError>>,
}

impl<T> TaskHandle<T> {
    pub(crate) fn new(receiver: Receiver<Result<T, TaskError>>) -> Self {
        Self { receiver }
    }

    /// Blocks until the task completes and returns its result.
    ///
    /// Task panics are returned as [`TaskError::Panicked`]. This method
    /// consumes the handle because a task produces exactly one result.
    pub fn wait(self) -> Result<T, TaskError> {
        match self.receiver.recv() {
            Ok(result) => result,
            Err(_) => Err(TaskError::WorkerDisconnected),
        }
    }
}

impl<T> fmt::Debug for TaskHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TaskHandle").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    #[test]
    fn wait_reports_worker_disconnect() {
        let (sender, receiver) = mpsc::sync_channel::<Result<(), TaskError>>(1);
        let handle = TaskHandle::new(receiver);

        drop(sender);

        assert_eq!(handle.wait(), Err(TaskError::WorkerDisconnected));
    }
}
