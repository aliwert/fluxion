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

/// Result of a nonblocking batch claim.
#[cfg(feature = "experimental-work-stealing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimOutcome {
    Claimed,
    Empty,
    Closed,
}

struct QueueState {
    tasks: VecDeque<Task>,
    closed: bool,
    waiters: usize,
    notifications_pending: usize,
}

/// A thread-safe bounded FIFO injection queue with blocking consumers.
///
/// Producers never block: a full or closed queue rejects the task immediately.
/// Consumers block until work is available or the queue is closed. A consumer
/// may claim several tasks at once into its own local buffer, reducing shared
/// queue contention.
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
                waiters: 0,
                notifications_pending: 0,
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
        let notifications = notifications_needed(&mut state);
        drop(state);

        for _ in 0..notifications {
            self.available.notify_one();
        }

        Ok(())
    }

    /// Claims a batch of tasks into a worker-local buffer.
    ///
    /// The claim limit is bounded by `max_batch` and by a fair share of the
    /// current backlog. The destination must be empty and have enough capacity
    /// for `max_batch` tasks. Returns `false` when the queue is closed and no
    /// tasks remain; otherwise returns `true` after claiming at least one task.
    pub(crate) fn claim_into(
        &self,
        destination: &mut VecDeque<Task>,
        max_batch: usize,
        worker_count: usize,
    ) -> bool {
        debug_assert!(destination.is_empty());
        debug_assert!(max_batch > 0);

        let mut state = self.lock_state();

        loop {
            if !state.tasks.is_empty() {
                let backlog = state.tasks.len();
                // Avoid letting one worker drain a small burst while its peers
                // sleep; larger backlogs still allow full prefetch batches.
                let fair_share = backlog.div_ceil(worker_count.max(1));
                let claim_count = max_batch.min(fair_share);

                for _ in 0..claim_count {
                    if let Some(task) = state.tasks.pop_front() {
                        destination.push_back(task);
                    }
                }

                let notifications = notifications_needed(&mut state);
                drop(state);

                for _ in 0..notifications {
                    self.available.notify_one();
                }

                return true;
            }

            if state.closed {
                return false;
            }

            state.waiters += 1;
            let wait_result = self.available.wait(state);
            state = match wait_result {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.waiters -= 1;
            state.notifications_pending = state.notifications_pending.saturating_sub(1);
            if state.waiters == 0 {
                state.notifications_pending = 0;
            }
        }
    }

    /// Attempts to claim a batch without blocking.
    ///
    /// The callback runs while the queue lock is held so that a worker's
    /// quiescence count becomes visible before another worker can observe an
    /// empty closed queue.
    #[cfg(feature = "experimental-work-stealing")]
    pub(crate) fn try_claim_into<F>(
        &self,
        destination: &mut VecDeque<Task>,
        max_batch: usize,
        worker_count: usize,
        record_claim: F,
    ) -> ClaimOutcome
    where
        F: FnOnce(usize),
    {
        debug_assert!(destination.is_empty());
        debug_assert!(max_batch > 0);

        let mut state = self.lock_state();

        if let Some(claim_count) = claim_batch(&mut state, destination, max_batch, worker_count) {
            record_claim(claim_count);

            let notifications = notifications_needed(&mut state);
            drop(state);

            for _ in 0..notifications {
                self.available.notify_one();
            }

            return ClaimOutcome::Claimed;
        }

        if state.closed {
            ClaimOutcome::Closed
        } else {
            ClaimOutcome::Empty
        }
    }

    pub(crate) fn close(&self) {
        let mut state = self.lock_state();
        state.closed = true;
        state.notifications_pending = 0;
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
    pub(crate) fn len(&self) -> usize {
        self.lock_state().tasks.len()
    }

    #[cfg(any(test, feature = "experimental-work-stealing"))]
    pub(crate) fn is_closed(&self) -> bool {
        self.lock_state().closed
    }

    #[cfg(feature = "experimental-work-stealing")]
    pub(crate) fn is_closed_and_empty(&self) -> bool {
        let state = self.lock_state();
        state.closed && state.tasks.is_empty()
    }
}

#[cfg(feature = "experimental-work-stealing")]
fn claim_batch(
    state: &mut QueueState,
    destination: &mut VecDeque<Task>,
    max_batch: usize,
    worker_count: usize,
) -> Option<usize> {
    if state.tasks.is_empty() {
        return None;
    }

    let backlog = state.tasks.len();
    // Avoid letting one worker drain a small burst while its peers sleep;
    // larger backlogs still allow full prefetch batches.
    let fair_share = backlog.div_ceil(worker_count.max(1));
    let claim_count = max_batch.min(fair_share);
    let mut claimed = 0;

    for _ in 0..claim_count {
        if let Some(task) = state.tasks.pop_front() {
            destination.push_back(task);
            claimed += 1;
        }
    }

    Some(claimed)
}

fn notifications_needed(state: &mut QueueState) -> usize {
    const MAX_PENDING_NOTIFICATIONS: usize = 2;

    if state.waiters == 0 {
        state.notifications_pending = 0;
        return 0;
    }

    // Coalesce wakeups at one per waiting consumer and at one per unclaimed
    // task, capped to avoid waking the whole pool for short bursts. A woken
    // consumer notifies again if the backlog remains.
    let needed = state
        .tasks
        .len()
        .saturating_sub(state.notifications_pending)
        .min(state.waiters.saturating_sub(state.notifications_pending))
        .min(MAX_PENDING_NOTIFICATIONS.saturating_sub(state.notifications_pending));
    state.notifications_pending += needed;
    needed
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    use super::*;

    fn noop_task() -> Task {
        Box::new(|| {})
    }

    fn claim_tasks(queue: &TaskQueue, max_batch: usize, worker_count: usize) -> VecDeque<Task> {
        let mut destination = VecDeque::with_capacity(max_batch);
        queue.claim_into(&mut destination, max_batch, worker_count);
        destination
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

        let mut local = claim_tasks(&queue, 2, 1);

        while let Some(task) = local.pop_front() {
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

        let local = claim_tasks(&queue, 1, 1);
        assert_eq!(local.len(), 1);

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
        assert_eq!(claim_tasks(&queue, 2, 1).len(), 1);
        assert!(claim_tasks(&queue, 2, 1).is_empty());
        assert_eq!(queue.push(noop_task()), Err(QueueError::Closed));
    }

    #[test]
    fn claim_respects_prefetch_batch_bound() {
        let queue = TaskQueue::new(32);

        for _ in 0..32 {
            queue.push(noop_task()).expect("task should be accepted");
        }

        assert_eq!(claim_tasks(&queue, 8, 1).len(), 8);
        assert_eq!(queue.len(), 24);

        assert_eq!(claim_tasks(&queue, 8, 1).len(), 8);
        assert_eq!(queue.len(), 16);
    }

    #[test]
    fn claim_uses_fair_share_of_small_backlog() {
        let queue = TaskQueue::new(16);

        for _ in 0..10 {
            queue.push(noop_task()).expect("task should be accepted");
        }

        assert_eq!(claim_tasks(&queue, 8, 8).len(), 2);
        assert_eq!(queue.len(), 8);
    }

    #[test]
    fn claim_limits_single_task_when_backlog_is_smaller_than_workers() {
        let queue = TaskQueue::new(8);

        for _ in 0..3 {
            queue.push(noop_task()).expect("task should be accepted");
        }

        assert_eq!(claim_tasks(&queue, 8, 8).len(), 1);
        assert_eq!(queue.len(), 2);
    }

    #[cfg(feature = "experimental-work-stealing")]
    #[test]
    fn try_claim_reports_empty_claimed_and_closed() {
        let queue = TaskQueue::new(8);
        let recorded = AtomicUsize::new(0);
        let mut local = VecDeque::with_capacity(8);

        assert_eq!(
            queue.try_claim_into(&mut local, 8, 4, |count| {
                recorded.fetch_add(count, Ordering::SeqCst);
            }),
            ClaimOutcome::Empty
        );
        assert_eq!(recorded.load(Ordering::SeqCst), 0);

        for _ in 0..4 {
            queue.push(noop_task()).expect("task should be accepted");
        }

        assert_eq!(
            queue.try_claim_into(&mut local, 8, 4, |count| {
                recorded.fetch_add(count, Ordering::SeqCst);
            }),
            ClaimOutcome::Claimed
        );
        assert_eq!(local.len(), 1);
        assert_eq!(recorded.load(Ordering::SeqCst), 1);

        queue.close();

        for _ in 0..3 {
            local.clear();
            assert_eq!(
                queue.try_claim_into(&mut local, 8, 4, |count| {
                    recorded.fetch_add(count, Ordering::SeqCst);
                }),
                ClaimOutcome::Claimed
            );
            assert_eq!(local.len(), 1);
        }

        local.clear();
        assert_eq!(
            queue.try_claim_into(&mut local, 8, 4, |count| {
                recorded.fetch_add(count, Ordering::SeqCst);
            }),
            ClaimOutcome::Closed
        );
        assert_eq!(recorded.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn claim_waits_for_a_task() {
        let queue = Arc::new(TaskQueue::new(4));

        let waiter = {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let mut local = VecDeque::with_capacity(4);
                let claimed = queue.claim_into(&mut local, 4, 1);
                (claimed, local.len())
            })
        };

        queue.push(noop_task()).expect("task should be accepted");

        assert_eq!(
            waiter.join().expect("claim thread should not panic"),
            (true, 1)
        );
    }

    #[test]
    fn multiple_claimers_drain_all_tasks_after_close() {
        const CLAIMERS: usize = 8;
        const TASKS: usize = 4096;

        let queue = Arc::new(TaskQueue::new(TASKS));
        let completed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(CLAIMERS + 1));

        let claimers: Vec<_> = (0..CLAIMERS)
            .map(|_| {
                let queue = Arc::clone(&queue);
                let start = Arc::clone(&start);

                thread::spawn(move || {
                    start.wait();

                    loop {
                        let mut local = VecDeque::with_capacity(8);
                        if !queue.claim_into(&mut local, 8, CLAIMERS) {
                            break;
                        }

                        for task in local.drain(..) {
                            task();
                        }
                    }
                })
            })
            .collect();

        start.wait();
        for _ in 0..TASKS {
            let completed = Arc::clone(&completed);
            queue
                .push(Box::new(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                }))
                .expect("task should be accepted");
        }
        queue.close();

        for claimer in claimers {
            claimer.join().expect("claimer should not panic");
        }
        assert_eq!(completed.load(Ordering::SeqCst), TASKS);
    }

    #[test]
    fn push_close_race_accepts_or_rejects_deterministically() {
        const PRODUCERS: usize = 4;
        const TASKS_PER_PRODUCER: usize = 50;

        let queue = Arc::new(TaskQueue::new(1024));
        let accepted = Arc::new(AtomicUsize::new(0));
        let executed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(PRODUCERS + 1));

        {
            let executed = Arc::clone(&executed);
            queue
                .push(Box::new(move || {
                    executed.fetch_add(1, Ordering::SeqCst);
                }))
                .expect("initial task should be accepted");
        }
        accepted.store(1, Ordering::SeqCst);

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|_| {
                let queue = Arc::clone(&queue);
                let accepted = Arc::clone(&accepted);
                let executed = Arc::clone(&executed);
                let start = Arc::clone(&start);

                thread::spawn(move || {
                    start.wait();

                    for _ in 0..TASKS_PER_PRODUCER {
                        let executed = Arc::clone(&executed);
                        if queue
                            .push(Box::new(move || {
                                executed.fetch_add(1, Ordering::SeqCst);
                            }))
                            .is_ok()
                        {
                            accepted.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();

        start.wait();
        queue.close();

        for producer in producers {
            producer.join().expect("producer should not panic");
        }

        loop {
            let mut local = VecDeque::with_capacity(8);
            if !queue.claim_into(&mut local, 8, 1) {
                break;
            }

            for task in local.drain(..) {
                task();
            }
        }

        assert_eq!(
            executed.load(Ordering::SeqCst),
            accepted.load(Ordering::SeqCst)
        );
    }
}
