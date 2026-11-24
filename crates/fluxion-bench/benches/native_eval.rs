use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const SIZES: [usize; 3] = [1_000, 100_000, 1_000_000];

fn sum_squares(values: &[i64]) -> i128 {
    let mut total = 0_i128;

    for &value in values {
        let value = i128::from(value);
        total += value
            .checked_mul(value)
            .expect("benchmark value must not overflow");
    }

    total
}

fn values(size: usize) -> Vec<i64> {
    (0..size)
        .map(|index| ((index as i64 * 2_654_435_761) % 2001) - 1000)
        .collect()
}

fn benchmark_native_sum_squares(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("native_sum_squares");

    for size in SIZES {
        let input = values(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &input,
            |bencher, input| {
                bencher.iter(|| black_box(sum_squares(black_box(input))));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_native_sum_squares);
criterion_main!(benches);
