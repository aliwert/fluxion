"""Reproducible native execution benchmark harness for Fluxion.

Run with a release build of the extension:

    maturin develop --release
    .venv/bin/python benchmarks/native_eval.py \
        --repeats 9 --json-out /tmp/fluxion-native.json

The harness compares sequential Python, ThreadPoolExecutor, Fluxion Python
callbacks, and Fluxion native operations on identical input chunks. It records
submission time separately from wait time for Fluxion scenarios. Pure native
arithmetic is measured by the Criterion benchmark in ``fluxion-bench``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from fluxion import Engine, __version__
from fluxion.native import sum_squares

WORKER_COUNTS = (1, 2, 4, 8)
DEFAULT_SIZES = (256, 4096, 65536)


@dataclass
class Run:
    elapsed_ns: int
    tasks: int
    submit_ns: int | None = None
    wait_ns: int | None = None


def python_sum_squares(values: list[int]) -> int:
    total = 0
    for value in values:
        total += value * value
    return total


def generate_values(size: int) -> list[int]:
    return [((index * 2_654_435_761) % 2001) - 1000 for index in range(size)]


def chunk_values(values: list[int], count: int) -> list[list[int]]:
    base, remainder = divmod(len(values), count)
    chunks: list[list[int]] = []
    start = 0
    for index in range(count):
        length = base + (1 if index < remainder else 0)
        chunks.append(values[start : start + length])
        start += length
    return chunks


def queue_capacity(tasks: int, workers: int) -> int:
    return max(1024, tasks + workers * 8)


def run_sequential_python(values: list[int], expected: int) -> Run:
    started = time.perf_counter_ns()
    result = python_sum_squares(values)
    elapsed = time.perf_counter_ns() - started
    assert result == expected
    return Run(elapsed, 1)


def run_threadpool_python(chunks: list[list[int]], workers: int, expected: int) -> Run:
    started = time.perf_counter_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(python_sum_squares, chunks))
    elapsed = time.perf_counter_ns() - started
    assert sum(results) == expected
    return Run(elapsed, len(chunks))


def run_fluxion_python(chunks: list[list[int]], workers: int, expected: int) -> Run:
    engine = Engine(
        worker_count=workers,
        queue_capacity=queue_capacity(len(chunks), workers),
    )
    try:
        submit_started = time.perf_counter_ns()
        handles = [engine.submit(python_sum_squares, chunk) for chunk in chunks]
        submit_ns = time.perf_counter_ns() - submit_started

        wait_started = time.perf_counter_ns()
        results = [handle.wait() for handle in handles]
        wait_ns = time.perf_counter_ns() - wait_started
    finally:
        engine.close()

    assert sum(results) == expected
    return Run(submit_ns + wait_ns, len(chunks), submit_ns, wait_ns)


def run_fluxion_native(chunks: list[list[int]], workers: int, expected: int) -> Run:
    engine = Engine(
        worker_count=workers,
        queue_capacity=queue_capacity(len(chunks), workers),
    )
    try:
        submit_started = time.perf_counter_ns()
        handles = [engine.submit_native(sum_squares, chunk) for chunk in chunks]
        submit_ns = time.perf_counter_ns() - submit_started

        wait_started = time.perf_counter_ns()
        results = [handle.wait() for handle in handles]
        wait_ns = time.perf_counter_ns() - wait_started
    finally:
        engine.close()

    assert sum(results) == expected
    return Run(submit_ns + wait_ns, len(chunks), submit_ns, wait_ns)


def run_native_noop(tasks: int, workers: int) -> Run:
    engine = Engine(worker_count=workers, queue_capacity=queue_capacity(tasks, workers))
    try:
        submit_started = time.perf_counter_ns()
        handles = [engine.submit_native(sum_squares, ()) for _ in range(tasks)]
        submit_ns = time.perf_counter_ns() - submit_started

        wait_started = time.perf_counter_ns()
        results = [handle.wait() for handle in handles]
        wait_ns = time.perf_counter_ns() - wait_started
    finally:
        engine.close()

    assert results == [0] * tasks
    return Run(submit_ns + wait_ns, tasks, submit_ns, wait_ns)


def percentile(values: list[int], percentage: float) -> float:
    ordered = sorted(values)
    index = round((percentage / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(index, len(ordered) - 1))] / 1_000


def summarize(name: str, workers: int, runs: list[Run]) -> dict:
    elapsed = [run.elapsed_ns for run in runs]
    tasks = runs[0].tasks
    record = {
        "name": name,
        "workers": workers,
        "tasks": tasks,
        "us_per_task": statistics.median(
            run.elapsed_ns / run.tasks / 1_000 for run in runs
        ),
        "tasks_per_second": statistics.median(
            run.tasks / (run.elapsed_ns / 1_000_000_000) for run in runs
        ),
        "elapsed_us_p50": percentile(elapsed, 50),
        "elapsed_us_p99": percentile(elapsed, 99),
        "runs": [
            {
                "elapsed_us": run.elapsed_ns / 1_000,
                "submit_us": (
                    run.submit_ns / 1_000 if run.submit_ns is not None else None
                ),
                "wait_us": run.wait_ns / 1_000 if run.wait_ns is not None else None,
            }
            for run in runs
        ],
    }

    submit_values = [run.submit_ns for run in runs if run.submit_ns is not None]
    wait_values = [run.wait_ns for run in runs if run.wait_ns is not None]
    if submit_values:
        record["submit_us_per_task"] = statistics.median(
            value / tasks / 1_000 for value in submit_values
        )
    if wait_values:
        record["wait_us_per_task"] = statistics.median(
            value / tasks / 1_000 for value in wait_values
        )
    return record


def benchmark_suite(scenarios: list[dict], repeats: int) -> list[dict]:
    runs: dict[str, list[Run]] = {scenario["name"]: [] for scenario in scenarios}

    for iteration in range(repeats + 1):
        gc.collect()
        order = scenarios if iteration % 2 == 0 else list(reversed(scenarios))
        for scenario in order:
            run = scenario["operation"]()
            if run.tasks != scenario["tasks"]:
                raise RuntimeError(
                    f"{scenario['name']}: expected {scenario['tasks']} tasks, "
                    f"observed {run.tasks}"
                )
            if iteration > 0:
                runs[scenario["name"]].append(run)

    return [
        summarize(
            scenario["name"],
            scenario["workers"],
            runs[scenario["name"]],
        )
        for scenario in scenarios
    ]


def print_results(results: list[dict]) -> None:
    print()
    print(
        f"{'benchmark':<34} {'workers':>7} {'tasks':>7} {'us/task':>10} "
        f"{'submit':>9} {'wait':>9} {'p50 us':>10} {'p99 us':>10}"
    )
    for result in results:
        print(
            f"{result['name']:<34} {result['workers']:>7} {result['tasks']:>7} "
            f"{result['us_per_task']:>10.3f} "
            f"{result.get('submit_us_per_task', 0):>9.3f} "
            f"{result.get('wait_us_per_task', 0):>9.3f} "
            f"{result['elapsed_us_p50']:>10.3f} "
            f"{result['elapsed_us_p99']:>10.3f}"
        )


def git_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            check=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=list(DEFAULT_SIZES),
        help="input lengths to benchmark",
    )
    parser.add_argument(
        "--task-multiplier",
        type=int,
        default=4,
        help="independent tasks per worker for Fluxion and thread-pool runs",
    )
    parser.add_argument("--noop-tasks", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=9)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--label", default="phase8e-native")
    args = parser.parse_args()

    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    if args.task_multiplier < 1:
        parser.error("--task-multiplier must be at least 1")
    if any(size < 0 for size in args.sizes):
        parser.error("--sizes values must be non-negative")

    results: list[dict] = []
    all_scenarios: list[dict] = []

    for size in args.sizes:
        values = generate_values(size)
        expected_sum = python_sum_squares(values)
        threadpool_tasks = args.task_multiplier * 4
        threadpool_chunks = chunk_values(values, threadpool_tasks)
        scenarios: list[dict] = [
            {
                "name": f"sequential_python_{size}",
                "workers": 1,
                "tasks": 1,
                "operation": lambda values=values, expected=expected_sum: (
                    run_sequential_python(values, expected)
                ),
            },
            {
                "name": f"threadpool_python_4w_{size}",
                "workers": 4,
                "tasks": threadpool_tasks,
                "operation": lambda chunks=threadpool_chunks, expected=expected_sum: (
                    run_threadpool_python(chunks, 4, expected)
                ),
            },
        ]

        for workers in WORKER_COUNTS:
            task_count = args.task_multiplier * workers
            chunks = chunk_values(values, task_count)
            scenarios.append(
                {
                    "name": f"fluxion_python_{workers}w_{size}",
                    "workers": workers,
                    "tasks": task_count,
                    "operation": lambda chunks=chunks, workers=workers, expected=expected_sum: (
                        run_fluxion_python(chunks, workers, expected)
                    ),
                }
            )
            scenarios.append(
                {
                    "name": f"fluxion_native_{workers}w_{size}",
                    "workers": workers,
                    "tasks": task_count,
                    "operation": lambda chunks=chunks, workers=workers, expected=expected_sum: (
                        run_fluxion_native(chunks, workers, expected)
                    ),
                }
            )

        all_scenarios.extend(scenarios)

    for workers in (1, 8):
        all_scenarios.append(
            {
                "name": f"native_noop_{workers}w",
                "workers": workers,
                "tasks": args.noop_tasks,
                "operation": lambda workers=workers: run_native_noop(
                    args.noop_tasks, workers
                ),
            }
        )

    results = benchmark_suite(all_scenarios, args.repeats)
    print_results(results)

    metadata = {
        "label": args.label,
        "fluxion_version": __version__,
        "git_commit": git_commit(),
        "python_version": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "timestamp_ns": time.time_ns(),
        "config": {
            "sizes": args.sizes,
            "task_multiplier": args.task_multiplier,
            "noop_tasks": args.noop_tasks,
            "repeats": args.repeats,
        },
        "results": results,
    }

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(metadata, indent=2) + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
