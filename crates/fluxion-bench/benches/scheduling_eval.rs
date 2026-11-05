use std::cell::Cell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fluxion_core::{Engine, EngineConfig};

const MULTI_PRODUCER_TASKS: usize = 1024;
const MULTI_PRODUCER_WORKERS: usize = 4;
const PRODUCER_COUNTS: [usize; 4] = [1, 2, 4, 8];

const LOAD_TASKS: usize = 512;
const LOAD_WORKERS: [usize; 2] = [4, 8];
const MAX_WORKERS: usize = 8;
const MAX_RECORDED_ROUNDS: usize = 256;

const SHUTDOWN_TASKS: usize = 1024;
const SHUTDOWN_WORKERS: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Noop,
    SmallCpu,
    MediumCpu,
}

impl Workload {
    const ALL: [Self; 3] = [Self::Noop, Self::SmallCpu, Self::MediumCpu];

    fn name(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::SmallCpu => "small_cpu",
            Self::MediumCpu => "medium_cpu",
        }
    }

    fn execute(self, seed: u64) -> u64 {
        match self {
            Self::Noop => seed,
            Self::SmallCpu => busy_loop(seed, 1_000),
            Self::MediumCpu => busy_loop(seed, 50_000),
        }
    }
}

fn busy_loop(mut value: u64, iterations: usize) -> u64 {
    for _ in 0..iterations {
        value = value
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        value ^= value >> 33;
    }
    value
}

fn engine_config(worker_count: usize, queue_capacity: usize) -> EngineConfig {
    EngineConfig::new(worker_count, queue_capacity)
        .expect("benchmark configuration should be valid")
}

#[derive(Clone, Copy, Debug)]
enum LoadMix {
    Small90Medium10,
    ShortThenLong,
    LongThenShort,
    Shuffled,
}

impl LoadMix {
    const ALL: [Self; 4] = [
        Self::Small90Medium10,
        Self::ShortThenLong,
        Self::LongThenShort,
        Self::Shuffled,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Small90Medium10 => "small90_medium10",
            Self::ShortThenLong => "short_then_long",
            Self::LongThenShort => "long_then_short",
            Self::Shuffled => "shuffled_90_10",
        }
    }
}

struct MixPlan {
    workloads: Vec<Workload>,
}

impl MixPlan {
    fn new(mix: LoadMix, task_count: usize) -> Self {
        let medium_count = task_count / 10;
        let small_count = task_count - medium_count;
        let mut workloads = Vec::with_capacity(task_count);

        match mix {
            LoadMix::Small90Medium10 => {
                for index in 0..task_count {
                    workloads.push(if index % 10 == 9 {
                        Workload::MediumCpu
                    } else {
                        Workload::SmallCpu
                    });
                }
            }
            LoadMix::ShortThenLong => {
                workloads.extend(std::iter::repeat_n(Workload::SmallCpu, small_count));
                workloads.extend(std::iter::repeat_n(Workload::MediumCpu, medium_count));
            }
            LoadMix::LongThenShort => {
                workloads.extend(std::iter::repeat_n(Workload::MediumCpu, medium_count));
                workloads.extend(std::iter::repeat_n(Workload::SmallCpu, small_count));
            }
            LoadMix::Shuffled => {
                workloads.extend(std::iter::repeat_n(Workload::SmallCpu, small_count));
                workloads.extend(std::iter::repeat_n(Workload::MediumCpu, medium_count));
                deterministic_shuffle(&mut workloads);
            }
        }

        Self { workloads }
    }

    fn task_count(&self) -> usize {
        self.workloads.len()
    }

    fn workload(&self, index: usize) -> Workload {
        self.workloads[index]
    }
}

fn deterministic_shuffle(workloads: &mut [Workload]) {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;

    for index in (1..workloads.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let swap_index = (state >> 32) as usize % (index + 1);
        workloads.swap(index, swap_index);
    }
}

fn block_workers(engine: &Engine, worker_count: usize) -> Arc<Barrier> {
    let release = Arc::new(Barrier::new(worker_count + 1));

    for _ in 0..worker_count {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let worker_release = Arc::clone(&release);

        engine
            .submit(move || {
                let _ = started_sender.send(());
                worker_release.wait();
            })
            .expect("blocking benchmark task should be accepted");

        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking benchmark task should start");
    }

    release
}

fn run_gated_load_round(
    engine: &Engine,
    plan: &MixPlan,
    worker_count: usize,
    batch_index: u64,
) -> Duration {
    let release = block_workers(engine, worker_count);
    let task_count = plan.task_count();
    let base_seed = batch_index.wrapping_mul(task_count as u64);
    let mut handles = Vec::with_capacity(task_count);

    for index in 0..task_count {
        let workload = plan.workload(index);
        let seed = base_seed.wrapping_add(index as u64);
        handles.push(
            engine
                .submit(move || workload.execute(seed))
                .expect("benchmark task should be accepted"),
        );
    }

    let drain_start = Instant::now();
    release.wait();

    let mut checksum = 0_u64;
    for handle in handles {
        checksum = checksum.wrapping_add(
            handle
                .wait()
                .expect("benchmark task should complete successfully"),
        );
    }
    black_box(checksum);

    drain_start.elapsed()
}

#[derive(Clone, Copy)]
struct ProducerSignals<'a> {
    tasks_per_producer: usize,
    workload: Workload,
    start: &'a Barrier,
    submitted: &'a Barrier,
    finish: &'a Barrier,
    running: &'a AtomicBool,
}

fn producer_loop(engine: &Engine, producer_id: usize, signals: ProducerSignals<'_>) {
    let mut handles = Vec::with_capacity(signals.tasks_per_producer);
    let mut round = 0_u64;

    loop {
        signals.start.wait();
        if !signals.running.load(Ordering::Acquire) {
            break;
        }

        let base_seed = round
            .wrapping_mul(MULTI_PRODUCER_TASKS as u64)
            .wrapping_add((producer_id * signals.tasks_per_producer) as u64);

        for index in 0..signals.tasks_per_producer {
            let seed = base_seed.wrapping_add(index as u64);
            handles.push(
                engine
                    .submit(move || signals.workload.execute(seed))
                    .expect("benchmark task should be accepted"),
            );
        }

        signals.submitted.wait();

        let mut checksum = 0_u64;
        for handle in handles.drain(..) {
            checksum = checksum.wrapping_add(
                handle
                    .wait()
                    .expect("benchmark task should complete successfully"),
            );
        }
        black_box(checksum);

        round = round.wrapping_add(1);
        signals.finish.wait();
    }
}

fn multi_producer(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("multi_producer");
    group.throughput(Throughput::Elements(MULTI_PRODUCER_TASKS as u64));

    for producer_count in PRODUCER_COUNTS {
        let tasks_per_producer = MULTI_PRODUCER_TASKS / producer_count;
        assert_eq!(tasks_per_producer * producer_count, MULTI_PRODUCER_TASKS);

        for workload in Workload::ALL {
            let mut engine = Engine::new(engine_config(
                MULTI_PRODUCER_WORKERS,
                MULTI_PRODUCER_TASKS * 2,
            ))
            .expect("benchmark engine should initialize");
            let running = AtomicBool::new(true);
            let start = Barrier::new(producer_count + 1);
            let submitted = Barrier::new(producer_count + 1);
            let finish = Barrier::new(producer_count + 1);
            let label = format!("{}-{producer_count}p", workload.name());

            thread::scope(|scope| {
                let engine_ref = &engine;
                let start_ref = &start;
                let submitted_ref = &submitted;
                let finish_ref = &finish;
                let running_ref = &running;

                let signals = ProducerSignals {
                    tasks_per_producer,
                    workload,
                    start: start_ref,
                    submitted: submitted_ref,
                    finish: finish_ref,
                    running: running_ref,
                };
                let producers: Vec<_> = (0..producer_count)
                    .map(|producer_id| {
                        scope.spawn(move || producer_loop(engine_ref, producer_id, signals))
                    })
                    .collect();

                group.bench_function(BenchmarkId::new("round", &label), |bench| {
                    bench.iter(|| {
                        start.wait();
                        submitted.wait();
                        finish.wait();
                    });
                });

                group.bench_function(BenchmarkId::new("submission", &label), |bench| {
                    bench.iter_custom(|iterations| {
                        let mut measured = Duration::ZERO;

                        for _ in 0..iterations {
                            let begin = Instant::now();
                            start.wait();
                            submitted.wait();
                            measured += begin.elapsed();
                            finish.wait();
                        }

                        measured
                    });
                });

                running.store(false, Ordering::Release);
                start.wait();

                for producer in producers {
                    producer.join().expect("producer should not panic");
                }
            });

            engine
                .shutdown()
                .expect("benchmark engine should shut down");
        }
    }

    group.finish();
}

#[derive(Debug)]
struct Instrumentation {
    started_ns: Vec<AtomicU64>,
    completed_ns: Vec<AtomicU64>,
    worker_id: Vec<AtomicU64>,
}

impl Instrumentation {
    fn new(task_count: usize) -> Self {
        Self {
            started_ns: new_atomic_vec(task_count),
            completed_ns: new_atomic_vec(task_count),
            worker_id: new_atomic_vec(task_count),
        }
    }
}

fn new_atomic_vec(len: usize) -> Vec<AtomicU64> {
    (0..len).map(|_| AtomicU64::new(0)).collect()
}

#[derive(Debug)]
struct RoundSummary {
    makespan_ns: u64,
    latency_p50_ns: u64,
    latency_p99_ns: u64,
    wait_p99_ns: u64,
    utilization: f64,
    task_count_cv: f64,
    medium_count_cv: f64,
    task_counts: [u64; MAX_WORKERS],
    medium_counts: [u64; MAX_WORKERS],
    max_tail_idle_ns: u64,
}

fn run_instrumented_round(
    engine: &Engine,
    plan: &MixPlan,
    instrumentation: &Arc<Instrumentation>,
    worker_count: usize,
    batch_index: u64,
) -> RoundSummary {
    let release = block_workers(engine, worker_count);
    let task_count = plan.task_count();
    let base_seed = batch_index.wrapping_mul(task_count as u64);
    let drain_start: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());
    let mut handles = Vec::with_capacity(task_count);

    for index in 0..task_count {
        let workload = plan.workload(index);
        let seed = base_seed.wrapping_add(index as u64);
        let instrumentation = Arc::clone(instrumentation);
        let drain_start = Arc::clone(&drain_start);

        handles.push(
            engine
                .submit(move || {
                    let origin = *drain_start
                        .get()
                        .expect("drain start should be set before release");
                    let started_ns = origin.elapsed().as_nanos() as u64;
                    instrumentation.started_ns[index].store(started_ns, Ordering::Relaxed);
                    instrumentation.worker_id[index].store(current_worker_id(), Ordering::Relaxed);

                    let result = workload.execute(seed);
                    let completed_ns = origin.elapsed().as_nanos() as u64;
                    instrumentation.completed_ns[index].store(completed_ns, Ordering::Relaxed);
                    result
                })
                .expect("benchmark task should be accepted"),
        );
    }

    let _ = drain_start.set(Instant::now());
    release.wait();

    let mut checksum = 0_u64;
    for handle in handles {
        checksum = checksum.wrapping_add(
            handle
                .wait()
                .expect("benchmark task should complete successfully"),
        );
    }
    black_box(checksum);

    summarize_round(plan, instrumentation, worker_count, task_count)
}

fn summarize_round(
    plan: &MixPlan,
    instrumentation: &Instrumentation,
    worker_count: usize,
    task_count: usize,
) -> RoundSummary {
    let mut latencies = Vec::with_capacity(task_count);
    let mut waits = Vec::with_capacity(task_count);
    let mut task_counts = [0_u64; MAX_WORKERS];
    let mut medium_counts = [0_u64; MAX_WORKERS];
    let mut busy_ns = [0_u64; MAX_WORKERS];
    let mut last_completion_ns = [0_u64; MAX_WORKERS];
    let mut makespan_ns = 0_u64;

    for index in 0..task_count {
        let started_ns = instrumentation.started_ns[index].load(Ordering::Relaxed);
        let completed_ns = instrumentation.completed_ns[index].load(Ordering::Relaxed);
        let worker_id = instrumentation.worker_id[index].load(Ordering::Relaxed) as usize;

        latencies.push(completed_ns);
        waits.push(started_ns);
        makespan_ns = makespan_ns.max(completed_ns);

        if worker_id < worker_count {
            task_counts[worker_id] += 1;
            busy_ns[worker_id] += completed_ns.saturating_sub(started_ns);
            last_completion_ns[worker_id] = last_completion_ns[worker_id].max(completed_ns);
            if plan.workload(index) == Workload::MediumCpu {
                medium_counts[worker_id] += 1;
            }
        }
    }

    let total_busy_ns: u64 = busy_ns[..worker_count].iter().sum();
    let utilization = if makespan_ns == 0 {
        0.0
    } else {
        total_busy_ns as f64 / (makespan_ns as f64 * worker_count as f64)
    };
    let max_tail_idle_ns = (0..worker_count)
        .map(|worker| makespan_ns.saturating_sub(last_completion_ns[worker]))
        .max()
        .unwrap_or(0);

    latencies.sort_unstable();
    waits.sort_unstable();

    RoundSummary {
        makespan_ns,
        latency_p50_ns: percentile(&latencies, 50.0),
        latency_p99_ns: percentile(&latencies, 99.0),
        wait_p99_ns: percentile(&waits, 99.0),
        utilization,
        task_count_cv: coefficient_of_variation(&task_counts[..worker_count]),
        medium_count_cv: coefficient_of_variation(&medium_counts[..worker_count]),
        task_counts,
        medium_counts,
        max_tail_idle_ns,
    }
}

fn percentile(sorted_values: &[u64], percentile: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }

    let rank = ((percentile / 100.0) * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[rank]
}

fn coefficient_of_variation(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mean = values.iter().sum::<u64>() as f64 / values.len() as f64;
    if mean == 0.0 {
        return 0.0;
    }

    let variance = values
        .iter()
        .map(|value| {
            let difference = *value as f64 - mean;
            difference * difference
        })
        .sum::<f64>()
        / values.len() as f64;

    variance.sqrt() / mean
}

thread_local! {
    static CACHED_WORKER_ID: Cell<u64> = const { Cell::new(u64::MAX) };
}

fn current_worker_id() -> u64 {
    CACHED_WORKER_ID.with(|cached| {
        let current = cached.get();
        if current != u64::MAX {
            return current;
        }

        let worker_id = thread::current()
            .name()
            .and_then(|name| name.strip_prefix("fluxion-worker-"))
            .and_then(|index| index.parse().ok())
            .unwrap_or(u64::MAX);

        cached.set(worker_id);
        worker_id
    })
}

fn load_imbalance(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("load_imbalance");
    group.throughput(Throughput::Elements(LOAD_TASKS as u64));

    for worker_count in LOAD_WORKERS {
        for mix in LoadMix::ALL {
            let plan = MixPlan::new(mix, LOAD_TASKS);
            let mut engine = Engine::new(engine_config(worker_count, LOAD_TASKS))
                .expect("benchmark engine should initialize");
            let mut batch_index = 0_u64;

            group.bench_function(BenchmarkId::new(mix.name(), worker_count), |bench| {
                bench.iter_custom(|iterations| {
                    let mut measured = Duration::ZERO;

                    for _ in 0..iterations {
                        let current_batch = batch_index;
                        batch_index = batch_index.wrapping_add(1);
                        measured +=
                            run_gated_load_round(&engine, &plan, worker_count, current_batch);
                    }

                    measured
                });
            });

            engine
                .shutdown()
                .expect("benchmark engine should shut down");
        }
    }

    group.finish();
}

fn load_imbalance_tail(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("load_imbalance_tail");
    group.throughput(Throughput::Elements(LOAD_TASKS as u64));

    for worker_count in LOAD_WORKERS {
        for mix in LoadMix::ALL {
            let plan = MixPlan::new(mix, LOAD_TASKS);
            let instrumentation = Arc::new(Instrumentation::new(LOAD_TASKS));
            let mut engine = Engine::new(engine_config(worker_count, LOAD_TASKS))
                .expect("benchmark engine should initialize");
            let mut batch_index = 0_u64;
            let mut rounds = VecDeque::with_capacity(MAX_RECORDED_ROUNDS);

            group.bench_function(BenchmarkId::new(mix.name(), worker_count), |bench| {
                bench.iter(|| {
                    let current_batch = batch_index;
                    batch_index = batch_index.wrapping_add(1);
                    let summary = run_instrumented_round(
                        &engine,
                        &plan,
                        &instrumentation,
                        worker_count,
                        current_batch,
                    );

                    if rounds.len() == MAX_RECORDED_ROUNDS {
                        rounds.pop_front();
                    }
                    let makespan = summary.makespan_ns;
                    rounds.push_back(summary);
                    black_box(makespan)
                });
            });

            report_rounds(mix.name(), worker_count, &rounds);

            engine
                .shutdown()
                .expect("benchmark engine should shut down");
        }
    }

    group.finish();
}

fn report_rounds(label: &str, worker_count: usize, rounds: &VecDeque<RoundSummary>) {
    if rounds.is_empty() {
        return;
    }

    let mut makespans = Vec::with_capacity(rounds.len());
    let mut latency_p50 = Vec::with_capacity(rounds.len());
    let mut latency_p99 = Vec::with_capacity(rounds.len());
    let mut wait_p99 = Vec::with_capacity(rounds.len());
    let mut utilizations = Vec::with_capacity(rounds.len());
    let mut task_count_cvs = Vec::with_capacity(rounds.len());
    let mut medium_count_cvs = Vec::with_capacity(rounds.len());
    let mut tail_idles = Vec::with_capacity(rounds.len());
    let mut task_counts = vec![Vec::with_capacity(rounds.len()); worker_count];
    let mut medium_counts = vec![Vec::with_capacity(rounds.len()); worker_count];

    for round in rounds {
        makespans.push(round.makespan_ns);
        latency_p50.push(round.latency_p50_ns);
        latency_p99.push(round.latency_p99_ns);
        wait_p99.push(round.wait_p99_ns);
        utilizations.push(round.utilization);
        task_count_cvs.push(round.task_count_cv);
        medium_count_cvs.push(round.medium_count_cv);
        tail_idles.push(round.max_tail_idle_ns);

        for worker in 0..worker_count {
            task_counts[worker].push(round.task_counts[worker]);
            medium_counts[worker].push(round.medium_counts[worker]);
        }
    }

    let task_counts: Vec<String> = task_counts
        .iter()
        .map(|values| format!("{:.1}", mean_u64(values)))
        .collect();
    let medium_counts: Vec<String> = medium_counts
        .iter()
        .map(|values| format!("{:.1}", mean_u64(values)))
        .collect();

    eprintln!(
        "[phase6c] {label}/{worker_count}w rounds={} makespan_p50={:.3}ms latency_p50_p50={:.3}ms latency_p99_p50={:.3}ms wait_p99_p50={:.3}ms utilization_p50={:.1}% task_cv_p50={:.3} medium_cv_p50={:.3} task_count_means={:?} medium_count_means={:?} max_tail_idle_p50={:.3}ms",
        rounds.len(),
        nanos_to_ms(median_u64(&mut makespans)),
        nanos_to_ms(median_u64(&mut latency_p50)),
        nanos_to_ms(median_u64(&mut latency_p99)),
        nanos_to_ms(median_u64(&mut wait_p99)),
        median_f64(&mut utilizations) * 100.0,
        median_f64(&mut task_count_cvs),
        median_f64(&mut medium_count_cvs),
        task_counts,
        medium_counts,
        nanos_to_ms(median_u64(&mut tail_idles)),
    );
}

fn mean_u64(values: &[u64]) -> f64 {
    values.iter().sum::<u64>() as f64 / values.len() as f64
}

fn median_u64(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_f64(values: &mut [f64]) -> f64 {
    values.sort_by(|left, right| {
        left.partial_cmp(right)
            .expect("benchmark metrics should be finite")
    });
    values[values.len() / 2]
}

fn nanos_to_ms(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1_000_000.0
}

fn shutdown_drain(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("shutdown_drain");
    group.throughput(Throughput::Elements(SHUTDOWN_TASKS as u64));

    group.bench_function("medium_cpu-4w", |bench| {
        bench.iter_batched(
            setup_shutdown_case,
            |(mut engine, release)| {
                release.wait();
                engine
                    .shutdown()
                    .expect("benchmark engine should shut down");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn setup_shutdown_case() -> (Engine, Arc<Barrier>) {
    let engine = Engine::new(engine_config(
        SHUTDOWN_WORKERS,
        SHUTDOWN_TASKS + SHUTDOWN_WORKERS,
    ))
    .expect("benchmark engine should initialize");
    let release = Arc::new(Barrier::new(SHUTDOWN_WORKERS + 1));

    for _ in 0..SHUTDOWN_WORKERS {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let worker_release = Arc::clone(&release);

        engine
            .submit(move || {
                let _ = started_sender.send(());
                worker_release.wait();
                0_u64
            })
            .expect("blocking benchmark task should be accepted");

        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking benchmark task should start");
    }

    for index in 0..SHUTDOWN_TASKS {
        let seed = index as u64;
        engine
            .submit(move || Workload::MediumCpu.execute(seed))
            .expect("benchmark task should be accepted");
    }

    (engine, release)
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
}

criterion_group! {
    name = benches;
    config = configured_criterion();
    targets = multi_producer,
        load_imbalance,
        load_imbalance_tail,
        shutdown_drain
}
criterion_main!(benches);
