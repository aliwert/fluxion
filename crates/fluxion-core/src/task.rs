use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
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
#[derive(Debug, Clone, Error, PartialEq, Eq)]
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

    /// Returns whether the task outcome has been fully published.
    pub fn done(&self) -> bool {
        self.control.done()
    }

    /// Returns whether the task was cancelled and its outcome published.
    pub fn cancelled(&self) -> bool {
        self.control.cancelled()
    }

    /// Returns the current task lifecycle state.
    ///
    /// A terminal status may be observed slightly before [`TaskHandle::done`]
    /// becomes true. Use `done` to determine whether the result is available.
    pub fn status(&self) -> TaskStatus {
        self.control.status()
    }

    /// Converts this handle into a cloneable handle for repeated result access.
    ///
    /// This consumes the one-shot handle. The task result is retained until
    /// all shared handles are dropped; unlike [`TaskHandle::wait`], shared
    /// handles never remove the stored result.
    pub fn into_shared(self) -> SharedTaskHandle<T> {
        SharedTaskHandle {
            control: self.control,
        }
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

/// Cloneable handle for repeated result access and multiple waiters.
///
/// Create one with [`TaskHandle::into_shared`]. The result remains available
/// for every shared handle until the last handle and result view are dropped.
pub struct SharedTaskHandle<T> {
    control: Arc<TaskControl<T>>,
}

impl<T> Clone for SharedTaskHandle<T> {
    fn clone(&self) -> Self {
        Self {
            control: Arc::clone(&self.control),
        }
    }
}

impl<T> SharedTaskHandle<T> {
    /// Attempts to cancel the task before it starts running.
    pub fn cancel(&self) -> bool {
        self.control.try_cancel()
    }

    /// Returns whether the task outcome has been fully published.
    pub fn done(&self) -> bool {
        self.control.done()
    }

    /// Returns whether the task was cancelled and its outcome published.
    pub fn cancelled(&self) -> bool {
        self.control.cancelled()
    }

    /// Returns the current task lifecycle state.
    ///
    /// A terminal status may be observed slightly before [`Self::done`] becomes
    /// true. Use `done` or a result view to determine whether the result is
    /// available.
    pub fn status(&self) -> TaskStatus {
        self.control.status()
    }

    /// Waits indefinitely and returns a result view.
    ///
    /// The view holds the result lock for its lifetime. Inspect it through
    /// [`Deref`], then drop it to release the lock.
    pub fn wait_shared(&self) -> TaskResult<'_, T> {
        self.wait_until_ready(None);
        self.result_view()
            .expect("shared task result must be published after waiting")
    }

    /// Waits up to `timeout` and returns a result view if the outcome is ready.
    pub fn wait_shared_timeout(&self, timeout: Duration) -> Option<TaskResult<'_, T>> {
        if !self.wait_until_ready(Some(timeout)) {
            return None;
        }

        self.result_view()
    }

    /// Waits until the outcome is published without acquiring the result lock.
    ///
    /// This is useful for bindings that must release a foreign runtime lock
    /// while waiting and acquire the result only after re-entering the runtime.
    /// A `None` timeout waits indefinitely.
    pub fn wait_until_ready(&self, timeout: Option<Duration>) -> bool {
        self.control.wait_until_ready(timeout)
    }

    /// Acquires a published result view without waiting.
    ///
    /// Returns `None` if the outcome has not been published yet.
    pub fn result_view(&self) -> Option<TaskResult<'_, T>> {
        let guard = self.control.lock_slot();

        if guard.result.is_some() {
            Some(TaskResult { guard })
        } else {
            None
        }
    }
}

impl<T> fmt::Debug for SharedTaskHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedTaskHandle")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

/// Opaque view of a published task result.
///
/// The internal guard is intentionally not exposed. Inspect the result through
/// `Deref` without requiring `T: Clone` or `T: Sync`.
pub struct TaskResult<'a, T> {
    guard: MutexGuard<'a, TaskSlot<T>>,
}

impl<T> std::ops::Deref for TaskResult<'_, T> {
    type Target = Result<T, TaskError>;

    fn deref(&self) -> &Self::Target {
        self.guard
            .result
            .as_ref()
            .expect("task result view must contain a published result")
    }
}

impl<T> TaskResult<'_, T> {
    /// Returns the published result by reference.
    pub fn as_result(&self) -> Result<&T, &TaskError> {
        self.guard
            .result
            .as_ref()
            .expect("task result view must contain a published result")
            .as_ref()
    }
}

impl<T> fmt::Debug for TaskResult<'_, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskResult")
            .field("result", &self.as_result())
            .finish_non_exhaustive()
    }
}

struct TaskSlot<T> {
    result: Option<Result<T, TaskError>>,
}

struct TaskControl<T> {
    status: AtomicU8,
    published: AtomicBool,
    slot: Mutex<TaskSlot<T>>,
    ready: Condvar,
}

impl<T> TaskControl<T> {
    fn new() -> Self {
        Self {
            status: AtomicU8::new(TaskStatus::Queued.as_u8()),
            published: AtomicBool::new(false),
            slot: Mutex::new(TaskSlot { result: None }),
            ready: Condvar::new(),
        }
    }

    fn status(&self) -> TaskStatus {
        TaskStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    fn done(&self) -> bool {
        self.published.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> bool {
        self.done() && self.status() == TaskStatus::Cancelled
    }

    fn try_start(&self) -> bool {
        self.status
            .compare_exchange(
                TaskStatus::Queued.as_u8(),
                TaskStatus::Running.as_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn try_cancel(&self) -> bool {
        if self
            .status
            .compare_exchange(
                TaskStatus::Queued.as_u8(),
                TaskStatus::Cancelled.as_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }

        self.publish(Err(TaskError::Cancelled), TaskStatus::Cancelled);
        true
    }

    fn complete(&self, result: Result<T, TaskError>) {
        let status = match result {
            Ok(_) => TaskStatus::Completed,
            Err(_) => TaskStatus::Failed,
        };
        self.publish(result, status);
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
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.publish(Err(TaskError::WorkerDisconnected), TaskStatus::Failed);
                return;
            }
        }
    }

    fn publish(&self, result: Result<T, TaskError>, terminal: TaskStatus) {
        {
            let mut slot = self.lock_slot();
            debug_assert!(slot.result.is_none());
            slot.result = Some(result);
            self.status.store(terminal.as_u8(), Ordering::Release);
            self.published.store(true, Ordering::Release);
        }

        self.ready.notify_all();
    }

    fn wait_result(&self) -> Result<T, TaskError> {
        let mut slot = self.lock_slot();

        loop {
            if let Some(result) = slot.result.take() {
                return result;
            }

            slot = self.wait_on_ready(slot);
        }
    }

    fn wait_result_timeout(&self, timeout: Duration) -> Option<Result<T, TaskError>> {
        let deadline = Instant::now().checked_add(timeout);
        let mut slot = self.lock_slot();

        loop {
            if let Some(result) = slot.result.take() {
                return Some(result);
            }

            let Some(deadline) = deadline else {
                slot = self.wait_on_ready(slot);
                continue;
            };

            let now = Instant::now();

            if now >= deadline {
                return None;
            }

            slot = self.wait_on_ready_timeout(slot, deadline - now);
        }
    }

    fn wait_until_ready(&self, timeout: Option<Duration>) -> bool {
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let mut slot = self.lock_slot();

        loop {
            if slot.result.is_some() {
                return true;
            }

            let Some(deadline) = deadline else {
                slot = self.wait_on_ready(slot);
                continue;
            };

            let now = Instant::now();

            if now >= deadline {
                return false;
            }

            slot = self.wait_on_ready_timeout(slot, deadline - now);
        }
    }

    fn wait_on_ready<'a>(&self, slot: MutexGuard<'a, TaskSlot<T>>) -> MutexGuard<'a, TaskSlot<T>> {
        match self.ready.wait(slot) {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn wait_on_ready_timeout<'a>(
        &self,
        slot: MutexGuard<'a, TaskSlot<T>>,
        timeout: Duration,
    ) -> MutexGuard<'a, TaskSlot<T>> {
        match self.ready.wait_timeout(slot, timeout) {
            Ok((guard, _)) => guard,
            Err(poisoned) => poisoned.into_inner().0,
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

    #[test]
    fn shared_handle_publishes_abandonment_outcome() {
        let (handle, task) = prepare_task(|| 1);
        let shared = handle.into_shared();

        drop(task);

        assert!(shared.done());
        assert_eq!(shared.status(), TaskStatus::Failed);
        assert_eq!(
            shared.wait_shared().as_result(),
            Err(&TaskError::WorkerDisconnected)
        );
    }

    #[test]
    fn shared_handle_supports_multiple_waiters() {
        let (handle, task) = prepare_task(|| 42);
        let shared = handle.into_shared();
        let mut waiters = Vec::new();

        for _ in 0..4 {
            let waiter = shared.clone();
            waiters.push(thread::spawn(move || {
                let result = waiter
                    .wait_shared_timeout(TEST_TIMEOUT)
                    .expect("shared result should be published");
                assert_eq!(result.as_result(), Ok(&42));
            }));
        }

        task();

        for waiter in waiters {
            waiter.join().expect("waiter should not panic");
        }

        assert_eq!(shared.wait_shared().as_result(), Ok(&42));
    }

    #[test]
    fn shared_timeout_preserves_result_for_later_access() {
        let (handle, task) = prepare_task(|| 11);
        let shared = handle.into_shared();

        assert!(shared.result_view().is_none());
        assert!(
            shared
                .wait_shared_timeout(Duration::from_millis(10))
                .is_none()
        );

        task();

        assert_eq!(shared.wait_shared().as_result(), Ok(&11));
        assert_eq!(shared.wait_shared().as_result(), Ok(&11));
    }

    #[test]
    fn shared_result_does_not_require_clone_or_sync() {
        struct NotClone(u32);

        let (handle, task) = prepare_task(|| NotClone(7));
        let shared = handle.into_shared();
        task();

        let result = shared.wait_shared();
        assert_eq!(result.as_result().map(|value| value.0), Ok(7));
    }

    #[test]
    fn shared_cancel_wakes_and_completes_pending_waiters() {
        let (handle, task) = prepare_task(|| 3);
        let shared = handle.into_shared();
        let mut waiters = Vec::new();

        for _ in 0..3 {
            let waiter = shared.clone();
            waiters.push(thread::spawn(move || {
                let result = waiter
                    .wait_shared_timeout(TEST_TIMEOUT)
                    .expect("cancellation should publish a result");
                assert_eq!(result.as_result(), Err(&TaskError::Cancelled));
            }));
        }

        thread::sleep(Duration::from_millis(10));
        assert!(shared.cancel());
        assert!(shared.cancelled());

        for waiter in waiters {
            waiter.join().expect("waiter should not panic");
        }

        drop(task);
    }

    #[test]
    fn done_is_never_observed_before_the_result_is_available() {
        for value in 0..100 {
            let (handle, task) = prepare_task(move || value);
            let reader = handle.into_shared();
            let poller = {
                let reader = reader.clone();
                thread::spawn(move || {
                    loop {
                        if reader.done() {
                            let result = reader
                                .result_view()
                                .expect("done must imply a published result");
                            assert_eq!(result.as_result(), Ok(&value));
                            return;
                        }
                        thread::yield_now();
                    }
                })
            };

            task();
            poller.join().expect("poller should not panic");
        }
    }
}
