use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard};

/// A unit of native work accepted by the engine.
pub(crate) type Task = Box<dyn FnOnce() + Send + 'static>;

/// Reason a task could not be accepted by the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueError {
    /// The queue has reached its configured capacity.
    Full,
    /// The queue has been closed and no longer accepts tasks.
    Closed,
}

struct QueueState {
    tasks: VecDeque<Task>,
    closed: bool,
}

/// A thread-safe bounded FIFO queue with blocking consumers.
///
/// Producers never block: a full or closed queue rejects the task immediately.
/// Consumers block until a task is available or the queue is closed. Closing
/// the queue stops new submissions but still allows already accepted tasks to
/// be consumed.
pub(crate) struct TaskQueue {
    capacity: usize,
    state: Mutex<QueueState>,
    available: Condvar,
}

impl TaskQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(QueueState {
                tasks: VecDeque::new(),
                closed: false,
            }),
            available: Condvar::new(),
        }
    }

    pub(crate) fn push(&self, task: Task) -> Result<(), QueueError> {
        let mut state = self.lock_state();

        if state.closed {
            return Err(QueueError::Closed);
        }

        if state.tasks.len() >= self.capacity {
            return Err(QueueError::Full);
        }

        state.tasks.push_back(task);
        drop(state);
        self.available.notify_one();

        Ok(())
    }

    pub(crate) fn pop(&self) -> Option<Task> {
        let mut state = self.lock_state();

        loop {
            if let Some(task) = state.tasks.pop_front() {
                return Some(task);
            }

            if state.closed {
                return None;
            }

            state = match self.available.wait(state) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    pub(crate) fn close(&self) {
        let mut state = self.lock_state();
        state.closed = true;
        drop(state);
        self.available.notify_all();
    }

    fn lock_state(&self) -> MutexGuard<'_, QueueState> {
        // User tasks never run while the lock is held, so a poisoned lock does
        // not imply broken queue invariants.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.lock_state().closed
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;

    fn noop_task() -> Task {
        Box::new(|| {})
    }

    #[test]
    fn tasks_execute_in_fifo_order() {
        let queue = TaskQueue::new(2);
        let order = Arc::new(Mutex::new(Vec::new()));

        for value in [1, 2] {
            let order = Arc::clone(&order);
            queue
                .push(Box::new(move || {
                    order
                        .lock()
                        .expect("order lock should not be poisoned")
                        .push(value);
                }))
                .expect("task should be accepted");
        }

        for _ in 0..2 {
            let task = queue.pop().expect("task should be available");
            task();
        }

        assert_eq!(
            *order.lock().expect("order lock should not be poisoned"),
            [1, 2]
        );
    }

    #[test]
    fn bounded_queue_rejects_tasks_when_full() {
        let queue = TaskQueue::new(1);

        queue
            .push(noop_task())
            .expect("first task should be accepted");
        assert_eq!(queue.push(noop_task()), Err(QueueError::Full));

        assert!(queue.pop().is_some());
        queue
            .push(noop_task())
            .expect("queue should accept a task after capacity is freed");
    }

    #[test]
    fn close_drains_accepted_tasks_and_rejects_new_tasks() {
        let queue = TaskQueue::new(2);

        queue.push(noop_task()).expect("task should be accepted");
        queue.close();

        assert!(queue.is_closed());
        assert!(queue.pop().is_some());
        assert!(queue.pop().is_none());
        assert_eq!(queue.push(noop_task()), Err(QueueError::Closed));
    }
}
