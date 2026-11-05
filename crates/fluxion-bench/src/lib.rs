//! Criterion benchmark suite for the Fluxion execution engine.
//!
//! # Running benchmarks
//!
//! Run the full suite:
//!
//! ```text
//! cargo bench -p fluxion-bench
//! ```
//!
//! Run a single group by filtering its name:
//!
//! ```text
//! cargo bench -p fluxion-bench -- task_throughput
//! cargo bench -p fluxion-bench -- prefetch_batch_sweep
//! cargo bench -p fluxion-bench -- multi_submitter
//! cargo bench -p fluxion-bench -- mixed_workload
//! cargo bench -p fluxion-bench -- queue_capacity
//! cargo bench -p fluxion-bench -- sequential_baseline
//! cargo bench -p fluxion-bench -- multi_producer
//! cargo bench -p fluxion-bench -- load_imbalance
//! cargo bench -p fluxion-bench -- shutdown_drain
//! ```
//!
//! Compare against a previously saved baseline by name:
//!
//! ```text
//! cargo bench -p fluxion-bench -- --baseline global-fifo
//! ```
//!
//! `cargo bench` builds with the `bench` profile, which inherits from
//! `release`. Reports are written to `target/criterion`, including saved
//! estimates and comparison history.
//!
//! # Interpreting results
//!
//! - `sequential_baseline` measures direct in-process execution of each
//!   workload. It is a reference for the cost of the work itself.
//! - `task_latency_1_worker` measures a complete `submit` and `wait` round
//!   trip for one task at a time. This is individual task latency, including
//!   queue handoff and result delivery overhead.
//! - `task_throughput` submits a fixed batch, then waits for every result.
//!   Criterion reports throughput as tasks per second. Compare worker-count
//!   variants to observe scaling.
//! - `prefetch_batch_sweep` varies the internal per-worker claim batch for
//!   `noop` and `small_cpu` workloads across worker counts. It uses a hidden
//!   benchmark-only constructor; applications cannot configure this value.
//! - `multi_submitter` varies the number of concurrent submitter threads
//!   sharing one engine, which exercises injection-queue contention.
//! - `mixed_workload` compares uniform and skewed task-duration mixes. The
//!   skewed mix is mostly `noop` with occasional `small_cpu` and `medium_cpu`
//!   tasks, making owner-local prefetch effects visible.
//! - `queue_capacity_effect` runs the same fixed batch with several injection
//!   queue capacities. Capacity changes allocation-independent queue behavior;
//!   at capacities at or above the batch size, differences may be within noise.
//! - `multi_producer/round` keeps producer threads alive across iterations and
//!   uses barriers, so thread creation is excluded. It measures submission and
//!   completion of the same aggregate batch for 1, 2, 4, and 8 producers.
//! - `multi_producer/submission` measures only the submission phase of the
//!   same producer rounds. Task completion is drained before the next measured
//!   round.
//! - `load_imbalance` runs 90% small / 10% medium, short-then-long,
//!   long-then-short, and deterministically shuffled mixes. Every worker is
//!   gated before the mix is submitted, so timed iterations measure queue drain
//!   rather than producer submission cost.
//! - `load_imbalance_tail` repeats those gated mixes with benchmark-side
//!   per-task timestamps. It prints median makespan, latency percentiles,
//!   utilization, per-worker mean task counts, medium-task counts, and tail
//!   idle time. The instrumentation is not intended for direct throughput
//!   comparison.
//! - `shutdown_drain` fills the queue with medium tasks while every worker is
//!   blocked, then measures release plus graceful shutdown drain time.
//!
//! Task throughput and task latency are not interchangeable: throughput is
//! batch-oriented work per second, while latency is the elapsed time for one
//! submit/wait cycle.
//!
//! Results are meaningful only when compared on the same machine, operating
//! system, Rust toolchain, build profile, worker count, queue capacity, and
//! workload configuration. Record hardware and toolchain details alongside
//! any reported numbers. Do not claim performance improvements without
//! reproducible measurements.
