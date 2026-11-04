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
//! cargo bench -p fluxion-bench -- queue_capacity
//! cargo bench -p fluxion-bench -- sequential_baseline
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
//! - `queue_capacity_effect` runs the same fixed batch with several queue
//!   capacities. Capacity changes allocation-independent queue behavior; at
//!   capacities at or above the batch size, differences may be within noise.
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
