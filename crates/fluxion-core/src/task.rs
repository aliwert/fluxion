use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use thiserror::Error;

/// Lifecycle state of a submitted task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TaskStatus {
    /// The task has been accepted and has not started running.
    Queued,
    /// The task is currently running on a worker.
    Running,
    /// The task finished successfully.
    Completed,
    /// The task failed with an error or panic.
    Failed,
    /// The task was cancelled before it started running.
    Cancelled,
}

impl TaskStatus {
    /// Returns whether this state is terminal.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    fn as_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Queued,
            1 => Self::Running,
            2 => Self::Completed,
            3 => Self::Failed,
            4 => Self::Cancelled,
            _ => unreachable!("invalid task status"),
        }
    }
}

/// Errors produced while waiting for a submitted task.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskError {
    /// The task panicked while it was running.
    #[error("task panicked")]
    Panicked,

    /// The task was abandoned or its worker exited without producing a result.
    #[error("task worker disconnected before producing a result")]
    WorkerDisconnected,

    /// The task was cancelled before it started running.
    #[error("task was cancelled")]
    Cancelled,
}

/// Outcome of waiting for a task with a timeout.
///
/// A timed-out wait returns the same handle so the caller can wait again.
#[derive(Debug)]
pub enum TaskWaitOutcome<T> {
    /// The task reached a terminal state and produced a result.
    Completed(Result<T, TaskError>),
    /// The timeout elapsed before a result was available.
    TimedOut(TaskHandle<T>),
}

/// Handle to the eventual result of a submitted task.
///
/// A handle may be dropped without affecting task execution. The task still
/// runs, and its result is discarded when it finishes.
pub struct TaskHandle<T> {
    control: Arc<TaskControl<T>>,
}

impl<T> TaskHandle<T> {
    /// Attempts to cancel the task before it starts running.
    ///
    /// Returns `true` if this call cancelled the task, or `false` if the task
    /// had already started or reached a terminal state. Cancellation is
    /// cooperative at the task boundary: running and completed tasks are never
    /// interrupted.
    pub fn cancel(&self) -> bool {
        self.control.try_cancel()
    }

    /// Returns whether the task has reached a terminal state.
    pub fn done(&self) -> bool {
        self.control.status().is_terminal()
    }

    /// Returns whether the task was cancelled.
    pub fn cancelled(&self) -> bool {
        self.control.status() == TaskStatus::Cancelled
    }

    /// Returns the current task lifecycle state.
    pub fn status(&self) -> TaskStatus {
        self.control.status()
    }

    /// Blocks until the task completes and returns its result.
    ///
    /// Task panics are returned as [`TaskError::Panicked`], and cancelled tasks
    /// as [`TaskError::Cancelled`]. This method consumes the handle because a
    /// task produces exactly one result.
    pub fn wait(self) -> Result<T, TaskError> {
        self.control.wait_result()
    }

    /// Waits up to `timeout` for the task result.
    ///
    /// A timeout does not cancel the task. The returned [`TaskWaitOutcome`]
    /// contains the same handle so the caller can wait again.
    pub fn wait_timeout(self, timeout: Duration) -> TaskWaitOutcome<T> {
        match self.control.wait_result_timeout(timeout) {
            Some(result) => TaskWaitOutcome::Completed(result),
            None => TaskWaitOutcome::TimedOut(self),
        }
    }
}

impl<T> fmt::Debug for TaskHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskHandle")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

struct TaskSlot<T> {
    result: Option<Result<T, TaskError>>,
    waiter: Option<Thread>,
}

struct TaskControl<T> {
    status: AtomicU8,
    slot: Mutex<TaskSlot<T>>,
}

impl<T> TaskControl<T> {
    fn new() -> Self {
        Self {
            status: AtomicU8::new(TaskStatus::Queued.as_u8()),
            slot: Mutex::new(TaskSlot {
                result: None,
                waiter: None,
            }),
        }
    }

    fn status(&self) -> TaskStatus {
        TaskStatus::from_u8(self.status.load(Ordering::Relaxed))
    }

    fn try_start(&self) -> bool {
        self.status
            .compare_exchange(
                TaskStatus::Queued.as_u8(),
                TaskStatus::Running.as_u8(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    fn try_cancel(&self) -> bool {
        if self
            .status
            .compare_exchange(
                TaskStatus::Queued.as_u8(),
                TaskStatus::Cancelled.as_u8(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return false;
        }

        self.publish(Err(TaskError::Cancelled));
        true
    }

    fn complete(&self, result: Result<T, TaskError>) {
        let status = match result {
            Ok(_) => TaskStatus::Completed,
            Err(_) => TaskStatus::Failed,
        };
        self.status.store(status.as_u8(), Ordering::Relaxed);
        self.publish(result);
    }

    fn abandon(&self) {
        loop {
            let status = self.status();
            let expected = match status {
                TaskStatus::Queued => TaskStatus::Queued.as_u8(),
                TaskStatus::Running => TaskStatus::Running.as_u8(),
                _ => return,
            };

            if self
                .status
                .compare_exchange(
                    expected,
                    TaskStatus::Failed.as_u8(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                self.publish(Err(TaskError::WorkerDisconnected));
                return;
            }
        }
    }

    fn publish(&self, result: Result<T, TaskError>) {
        let waiter = {
            let mut slot = self.lock_slot();
            slot.result = Some(result);
            slot.waiter.take()
        };

        if let Some(waiter) = waiter {
            waiter.unpark();
        }
    }

    fn wait_result(&self) -> Result<T, TaskError> {
        let current = thread::current();
        let mut slot = self.lock_slot();

        loop {
            if let Some(result) = slot.result.take() {
                slot.waiter = None;
                return result;
            }

            slot.waiter = Some(current.clone());
            drop(slot);
            thread::park();
            slot = self.lock_slot();
        }
    }

    fn wait_result_timeout(&self, timeout: Duration) -> Option<Result<T, TaskError>> {
        let deadline = Instant::now().checked_add(timeout);
        let current = thread::current();
        let mut slot = self.lock_slot();

        loop {
            if let Some(result) = slot.result.take() {
                slot.waiter = None;
                return Some(result);
            }

            let Some(deadline) = deadline else {
                slot.waiter = Some(current.clone());
                drop(slot);
                thread::park();
                slot = self.lock_slot();
                continue;
            };

            let now = Instant::now();

            if now >= deadline {
                slot.waiter = None;
                return None;
            }

            slot.waiter = Some(current.clone());
            drop(slot);
            thread::park_timeout(deadline - now);
            slot = self.lock_slot();
        }
    }

    fn lock_slot(&self) -> MutexGuard<'_, TaskSlot<T>> {
        match self.slot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

struct CompletionGuard<T> {
    control: Arc<TaskControl<T>>,
    armed: bool,
}

impl<T> CompletionGuard<T> {
    fn new(control: Arc<TaskControl<T>>) -> Self {
        Self {
            control,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<T> Drop for CompletionGuard<T> {
    fn drop(&mut self) {
        if self.armed {
            self.control.abandon();
        }
    }
}

pub(crate) fn prepare_task<F, T>(task: F) -> (TaskHandle<T>, impl FnOnce() + Send + 'static)
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let control = Arc::new(TaskControl::new());
    let handle = TaskHandle {
        control: Arc::clone(&control),
    };
    let mut guard = CompletionGuard::new(control);

    let task = move || {
        if !guard.control.try_start() {
            return;
        }

        let result = match panic::catch_unwind(AssertUnwindSafe(task)) {
            Ok(value) => Ok(value),
            Err(_) => Err(TaskError::Panicked),
        };
        guard.control.complete(result);
        guard.disarm();
    };

    (handle, task)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    #[test]
    fn wait_reports_worker_disconnect_when_execution_is_abandoned() {
        let (handle, task) = prepare_task(|| ());

        drop(task);

        assert_eq!(handle.status(), TaskStatus::Failed);
        assert!(handle.done());
        assert_eq!(handle.wait(), Err(TaskError::WorkerDisconnected));
    }

    #[test]
    fn completed_task_reports_result_and_terminal_status() {
        let (handle, task) = prepare_task(|| 21 * 2);

        assert_eq!(handle.status(), TaskStatus::Queued);
        assert!(!handle.done());
        task();

        assert_eq!(handle.status(), TaskStatus::Completed);
        assert!(handle.done());
        assert!(!handle.cancelled());
        assert!(!handle.cancel());
        assert_eq!(handle.wait(), Ok(42));
    }

    #[test]
    fn task_panic_is_reported_as_failed_after_execution() {
        let (handle, task) = prepare_task(|| -> usize { panic!("intentional task panic") });

        task();

        assert_eq!(handle.status(), TaskStatus::Failed);
        assert_eq!(handle.wait(), Err(TaskError::Panicked));
    }

    #[test]
    fn cancelling_queued_task_prevents_execution() {
        let executed = Arc::new(AtomicBool::new(false));
        let task_executed = Arc::clone(&executed);
        let (handle, task) = prepare_task(move || {
            task_executed.store(true, Ordering::SeqCst);
        });

        assert!(handle.cancel());
        assert!(handle.cancelled());
        assert!(handle.done());
        assert!(!handle.cancel());
        assert_eq!(handle.status(), TaskStatus::Cancelled);
        assert_eq!(handle.wait(), Err(TaskError::Cancelled));

        task();
        assert!(!executed.load(Ordering::SeqCst));
    }

    #[test]
    fn running_task_cannot_be_cancelled_and_completes_normally() {
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let (handle, task) = prepare_task(move || {
            started_sender
                .send(())
                .expect("started receiver should be available");
            release_receiver
                .recv_timeout(TEST_TIMEOUT)
                .expect("running task should be released");
            7
        });

        let worker = thread::spawn(task);
        started_receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("task should start");

        assert_eq!(handle.status(), TaskStatus::Running);
        assert!(!handle.done());
        assert!(!handle.cancel());

        release_sender
            .send(())
            .expect("running task should be released");

        assert_eq!(handle.wait(), Ok(7));
        worker.join().expect("task thread should not panic");
    }

    #[test]
    fn timeout_preserves_handle_for_a_later_wait() {
        let (handle, task) = prepare_task(|| 5);

        let handle = match handle.wait_timeout(Duration::from_millis(10)) {
            TaskWaitOutcome::TimedOut(handle) => handle,
            TaskWaitOutcome::Completed(result) => {
                panic!("task should not complete before execution: {result:?}")
            }
        };

        assert_eq!(handle.status(), TaskStatus::Queued);
        task();
        assert_eq!(handle.wait(), Ok(5));
    }

    #[test]
    fn maximum_timeout_observes_completed_result() {
        let (handle, task) = prepare_task(|| 13);
        task();

        match handle.wait_timeout(Duration::MAX) {
            TaskWaitOutcome::Completed(result) => assert_eq!(result, Ok(13)),
            TaskWaitOutcome::TimedOut(_) => panic!("completed result should be observed"),
        }
    }

    #[test]
    fn zero_timeout_observes_ready_result_or_preserves_handle() {
        let (handle, task) = prepare_task(|| 9);

        let handle = match handle.wait_timeout(Duration::ZERO) {
            TaskWaitOutcome::TimedOut(handle) => handle,
            TaskWaitOutcome::Completed(result) => {
                panic!("task should not complete before execution: {result:?}")
            }
        };

        task();

        match handle.wait_timeout(Duration::ZERO) {
            TaskWaitOutcome::Completed(result) => assert_eq!(result, Ok(9)),
            TaskWaitOutcome::TimedOut(_) => panic!("ready result should be observed"),
        }
    }

    #[test]
    fn cancelled_task_delivers_one_result_without_executing() {
        let executions = Arc::new(AtomicUsize::new(0));
        let task_executions = Arc::clone(&executions);
        let (handle, task) = prepare_task(move || {
            task_executions.fetch_add(1, Ordering::SeqCst);
        });

        assert!(handle.cancel());
        task();

        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert_eq!(handle.wait(), Err(TaskError::Cancelled));
    }
}
