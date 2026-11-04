use std::hint::black_box;
use std::thread;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fluxion_core::{Engine, EngineConfig};

const TASK_BATCH_SIZE: usize = 256;
const QUEUE_BATCH_SIZE: usize = 64;
const WORKER_COUNTS: [usize; 4] = [1, 2, 4, 8];
const QUEUE_CAPACITIES: [usize; 3] = [64, 256, 1024];
const PREFETCH_BATCHES: [usize; 5] = [1, 4, 8, 16, 32];
const SUBMITTER_COUNTS: [usize; 4] = [1, 2, 4, 8];

#[derive(Clone, Copy, Debug)]
enum Workload {
    Noop,
    SmallCpu,
    MediumCpu,
}

impl Workload {
    const ALL: [Self; 3] = [Self::Noop, Self::SmallCpu, Self::MediumCpu];
    const QUEUE_CAPACITY_SUBSET: [Self; 2] = [Self::Noop, Self::SmallCpu];

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

#[derive(Clone, Copy, Debug)]
enum WorkloadMix {
    Uniform,
    Skewed,
}

impl WorkloadMix {
    const ALL: [Self; 2] = [Self::Uniform, Self::Skewed];

    fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Skewed => "skewed",
        }
    }

    fn workload(self, index: usize) -> Workload {
        match self {
            Self::Uniform => Workload::ALL[index % Workload::ALL.len()],
            Self::Skewed => match index % 10 {
                0..=7 => Workload::Noop,
                8 => Workload::SmallCpu,
                _ => Workload::MediumCpu,
            },
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

fn submit_batch(engine: &Engine, workload: Workload, batch_size: usize, batch_index: u64) -> u64 {
    let mut handles = Vec::with_capacity(batch_size);

    for index in 0..batch_size {
        let seed = batch_index
            .wrapping_mul(batch_size as u64)
            .wrapping_add(index as u64);
        handles.push(
            engine
                .submit(move || workload.execute(seed))
                .expect("benchmark task should be accepted"),
        );
    }

    let mut checksum = 0_u64;
    for handle in handles {
        checksum = checksum.wrapping_add(
            handle
                .wait()
                .expect("benchmark task should complete successfully"),
        );
    }
    checksum
}

fn submit_mixed_batch(
    engine: &Engine,
    mix: WorkloadMix,
    batch_size: usize,
    batch_index: u64,
) -> u64 {
    let mut handles = Vec::with_capacity(batch_size);

    for index in 0..batch_size {
        let workload = mix.workload(index);
        let seed = batch_index
            .wrapping_mul(batch_size as u64)
            .wrapping_add(index as u64);
        handles.push(
            engine
                .submit(move || workload.execute(seed))
                .expect("benchmark task should be accepted"),
        );
    }

    let mut checksum = 0_u64;
    for handle in handles {
        checksum = checksum.wrapping_add(
            handle
                .wait()
                .expect("benchmark task should complete successfully"),
        );
    }
    checksum
}

fn submit_from_threads(
    engine: &Engine,
    workload: Workload,
    batch_size: usize,
    submitter_count: usize,
    batch_index: u64,
) -> u64 {
    let tasks_per_submitter = batch_size / submitter_count;
    assert_eq!(tasks_per_submitter * submitter_count, batch_size);

    thread::scope(|scope| {
        let submitters: Vec<_> = (0..submitter_count)
            .map(|submitter| {
                scope.spawn(move || {
                    let mut handles = Vec::with_capacity(tasks_per_submitter);

                    for index in 0..tasks_per_submitter {
                        let task_index = submitter * tasks_per_submitter + index;
                        let seed = batch_index
                            .wrapping_mul(batch_size as u64)
                            .wrapping_add(task_index as u64);
                        handles.push(
                            engine
                                .submit(move || workload.execute(seed))
                                .expect("benchmark task should be accepted"),
                        );
                    }

                    let mut checksum = 0_u64;
                    for handle in handles {
                        checksum = checksum.wrapping_add(
                            handle
                                .wait()
                                .expect("benchmark task should complete successfully"),
                        );
                    }
                    checksum
                })
            })
            .collect();

        submitters
            .into_iter()
            .map(|submitter| submitter.join().expect("submitter should not panic"))
            .fold(0_u64, u64::wrapping_add)
    })
}

fn sequential_baseline(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("sequential_baseline");
    group.throughput(Throughput::Elements(1));

    for workload in Workload::ALL {
        group.bench_function(workload.name(), |bench| {
            let mut seed = 0_u64;
            bench.iter(|| {
                seed = seed.wrapping_add(1);
                black_box(workload.execute(black_box(seed)))
            });
        });
    }

    group.finish();
}

fn task_latency(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("task_latency_1_worker");
    group.throughput(Throughput::Elements(1));

    for workload in Workload::ALL {
        group.bench_function(workload.name(), |bench| {
            let mut engine = Engine::new(engine_config(1, QUEUE_BATCH_SIZE))
                .expect("benchmark engine should initialize");
            let mut seed = 0_u64;

            bench.iter(|| {
                seed = seed.wrapping_add(1);
                let seed = black_box(seed);
                let handle = engine
                    .submit(move || workload.execute(seed))
                    .expect("latency task should be accepted");
                black_box(
                    handle
                        .wait()
                        .expect("latency task should complete successfully"),
                )
            });

            engine
                .shutdown()
                .expect("benchmark engine should shut down");
        });
    }

    group.finish();
}

fn task_throughput(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("task_throughput");
    group.throughput(Throughput::Elements(TASK_BATCH_SIZE as u64));

    for worker_count in WORKER_COUNTS {
        for workload in Workload::ALL {
            group.bench_with_input(
                BenchmarkId::new(workload.name(), worker_count),
                &worker_count,
                |bench, &worker_count| {
                    let mut engine = Engine::new(engine_config(worker_count, TASK_BATCH_SIZE))
                        .expect("benchmark engine should initialize");
                    let mut batch_index = 0_u64;

                    bench.iter(|| {
                        let current_batch = batch_index;
                        batch_index = batch_index.wrapping_add(1);
                        black_box(submit_batch(
                            &engine,
                            workload,
                            TASK_BATCH_SIZE,
                            current_batch,
                        ))
                    });

                    engine
                        .shutdown()
                        .expect("benchmark engine should shut down");
                },
            );
        }
    }

    group.finish();
}

fn prefetch_batch_sweep(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("prefetch_batch_sweep");
    group.throughput(Throughput::Elements(TASK_BATCH_SIZE as u64));

    for worker_count in WORKER_COUNTS {
        for workload in [Workload::Noop, Workload::SmallCpu] {
            for prefetch_batch in PREFETCH_BATCHES {
                let mut engine = Engine::new_with_prefetch(
                    engine_config(worker_count, TASK_BATCH_SIZE),
                    prefetch_batch,
                )
                .expect("benchmark engine should initialize");
                let mut batch_index = 0_u64;
                let benchmark_id = BenchmarkId::new(
                    format!("{}-{worker_count}w", workload.name()),
                    prefetch_batch,
                );

                group.bench_function(benchmark_id, |bench| {
                    bench.iter(|| {
                        let current_batch = batch_index;
                        batch_index = batch_index.wrapping_add(1);
                        black_box(submit_batch(
                            &engine,
                            workload,
                            TASK_BATCH_SIZE,
                            current_batch,
                        ))
                    });
                });

                engine
                    .shutdown()
                    .expect("benchmark engine should shut down");
            }
        }
    }

    group.finish();
}

fn multi_submitter(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("multi_submitter");
    group.throughput(Throughput::Elements(TASK_BATCH_SIZE as u64));

    for submitter_count in SUBMITTER_COUNTS {
        let mut engine = Engine::new(engine_config(4, TASK_BATCH_SIZE * 4))
            .expect("benchmark engine should initialize");
        let mut batch_index = 0_u64;

        group.bench_function(BenchmarkId::new("small_cpu-4w", submitter_count), |bench| {
            bench.iter(|| {
                let current_batch = batch_index;
                batch_index = batch_index.wrapping_add(1);
                black_box(submit_from_threads(
                    &engine,
                    Workload::SmallCpu,
                    TASK_BATCH_SIZE,
                    submitter_count,
                    current_batch,
                ))
            });
        });

        engine
            .shutdown()
            .expect("benchmark engine should shut down");
    }

    group.finish();
}

fn mixed_workload(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(TASK_BATCH_SIZE as u64));

    for worker_count in WORKER_COUNTS {
        for mix in WorkloadMix::ALL {
            let mut engine = Engine::new(engine_config(worker_count, TASK_BATCH_SIZE))
                .expect("benchmark engine should initialize");
            let mut batch_index = 0_u64;

            group.bench_function(BenchmarkId::new(mix.name(), worker_count), |bench| {
                bench.iter(|| {
                    let current_batch = batch_index;
                    batch_index = batch_index.wrapping_add(1);
                    black_box(submit_mixed_batch(
                        &engine,
                        mix,
                        TASK_BATCH_SIZE,
                        current_batch,
                    ))
                });
            });

            engine
                .shutdown()
                .expect("benchmark engine should shut down");
        }
    }

    group.finish();
}

fn queue_capacity_effect(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("queue_capacity_effect");
    group.throughput(Throughput::Elements(QUEUE_BATCH_SIZE as u64));

    for queue_capacity in QUEUE_CAPACITIES {
        for workload in Workload::QUEUE_CAPACITY_SUBSET {
            group.bench_with_input(
                BenchmarkId::new(workload.name(), queue_capacity),
                &queue_capacity,
                |bench, &queue_capacity| {
                    let mut engine = Engine::new(engine_config(4, queue_capacity))
                        .expect("benchmark engine should initialize");
                    let mut batch_index = 0_u64;

                    bench.iter(|| {
                        let current_batch = batch_index;
                        batch_index = batch_index.wrapping_add(1);
                        black_box(submit_batch(
                            &engine,
                            workload,
                            QUEUE_BATCH_SIZE,
                            current_batch,
                        ))
                    });

                    engine
                        .shutdown()
                        .expect("benchmark engine should shut down");
                },
            );
        }
    }

    group.finish();
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
    targets = sequential_baseline,
        task_latency,
        task_throughput,
        prefetch_batch_sweep,
        multi_submitter,
        mixed_workload,
        queue_capacity_effect
}
criterion_main!(benches);
