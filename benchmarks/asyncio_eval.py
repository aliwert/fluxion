#!/usr/bin/env python3
"""Reproducible asyncio benchmark and stress harness for Fluxion.

Run with the project virtual environment:

    .venv/bin/python benchmarks/asyncio_eval.py --json-out /tmp/fluxion-async.json

The harness measures synchronous baselines and asyncio submission on the same
Python callback workload. It reports the median run for each scenario; raw runs
are retained in the JSON output.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import platform
import statistics
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from fluxion import Engine, __version__


def noop() -> int:
    return 1


def sleep_task() -> int:
    time.sleep(0.001)
    return 1


def wait_handles(handles):
    return [handle.wait() for handle in handles]


class BlockingTask:
    def __init__(self, started: threading.Event, release: threading.Event):
        self.started = started
        self.release = release

    def __call__(self) -> int:
        self.started.set()
        assert self.release.wait(5)
        return 1


async def submit_test_task(engine: Engine, task: BlockingTask):
    return engine.submit_async(task)


@dataclass
class Run:
    elapsed_ns: int
    tasks: int
    latencies_ns: list[int] | None = None
    loop_gaps_ns: list[int] | None = None


def percentile(values: list[int], percentage: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = round((percentage / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(index, len(ordered) - 1))]


def queue_capacity(tasks: int, workers: int) -> int:
    return max(1024, tasks + workers * 8)


def run_sync_sequential(tasks: int) -> Run:
    engine = Engine(worker_count=1, queue_capacity=queue_capacity(tasks, 1))
    latencies: list[int] = []
    try:
        started = time.perf_counter_ns()
        for _ in range(tasks):
            task_started = time.perf_counter_ns()
            handle = engine.submit(noop)
            assert handle.wait() == 1
            latencies.append(time.perf_counter_ns() - task_started)
        elapsed = time.perf_counter_ns() - started
    finally:
        engine.shutdown()
    return Run(elapsed, tasks, latencies)


def run_sync_burst(tasks: int, workers: int) -> Run:
    engine = Engine(worker_count=workers, queue_capacity=queue_capacity(tasks, workers))
    try:
        started = time.perf_counter_ns()
        handles = [engine.submit(noop) for _ in range(tasks)]
        results = [handle.wait() for handle in handles]
        elapsed = time.perf_counter_ns() - started
        assert results == [1] * tasks
    finally:
        engine.shutdown()
    return Run(elapsed, tasks)


async def run_async_sequential(tasks: int) -> Run:
    engine = Engine(worker_count=1, queue_capacity=queue_capacity(tasks, 1))
    latencies: list[int] = []
    try:
        started = time.perf_counter_ns()
        for _ in range(tasks):
            task_started = time.perf_counter_ns()
            future = engine.submit_async(noop)
            assert await future == 1
            latencies.append(time.perf_counter_ns() - task_started)
        elapsed = time.perf_counter_ns() - started
    finally:
        engine.shutdown()
    return Run(elapsed, tasks, latencies)


async def run_async_burst(tasks: int, workers: int) -> Run:
    engine = Engine(worker_count=workers, queue_capacity=queue_capacity(tasks, workers))
    try:
        started = time.perf_counter_ns()
        futures = [engine.submit_async(noop) for _ in range(tasks)]
        results = await asyncio.gather(*futures)
        elapsed = time.perf_counter_ns() - started
        assert results == [1] * tasks
    finally:
        engine.shutdown()
    return Run(elapsed, tasks)


async def run_sleep_burst(tasks: int, workers: int) -> Run:
    engine = Engine(worker_count=workers, queue_capacity=queue_capacity(tasks, workers))
    try:
        started = time.perf_counter_ns()
        futures = [engine.submit_async(sleep_task) for _ in range(tasks)]
        results = await asyncio.gather(*futures)
        elapsed = time.perf_counter_ns() - started
        assert results == [1] * tasks
    finally:
        engine.shutdown()
    return Run(elapsed, tasks)


async def run_responsiveness(tasks: int, workers: int, task=noop) -> Run:
    engine = Engine(worker_count=workers, queue_capacity=queue_capacity(tasks, workers))
    gaps: list[int] = []
    stop = asyncio.Event()
    recording = False
    skip_next = True

    async def heartbeat() -> None:
        nonlocal skip_next
        previous = time.perf_counter_ns()
        while not stop.is_set():
            await asyncio.sleep(0.001)
            current = time.perf_counter_ns()
            if recording:
                if skip_next:
                    skip_next = False
                else:
                    gaps.append(current - previous)
            previous = current

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await asyncio.sleep(0.002)
        started = time.perf_counter_ns()
        futures = [engine.submit_async(task) for _ in range(tasks)]
        recording = True
        results = await asyncio.gather(*futures)
        elapsed = time.perf_counter_ns() - started
        assert results == [1] * tasks
    finally:
        stop.set()
        await heartbeat_task
        engine.shutdown()
    return Run(elapsed, tasks, loop_gaps_ns=gaps)


async def run_queued_cancellation(tasks: int) -> Run:
    engine = Engine(worker_count=1, queue_capacity=queue_capacity(tasks, 1))
    started = threading.Event()
    release = threading.Event()
    executed = 0

    def blocker() -> int:
        started.set()
        assert release.wait(5)
        return 0

    def counted_noop() -> int:
        nonlocal executed
        executed += 1
        return 1

    engine.submit(blocker)
    cancel_latencies: list[int] = []
    try:
        assert await asyncio.to_thread(started.wait, 5)
        elapsed_started = time.perf_counter_ns()
        for _ in range(tasks):
            future = engine.submit_async(counted_noop)
            cancel_started = time.perf_counter_ns()
            assert future.cancel()
            cancel_latencies.append(time.perf_counter_ns() - cancel_started)
            try:
                await future
            except asyncio.CancelledError:
                pass
        elapsed = time.perf_counter_ns() - elapsed_started
        assert executed == 0
    finally:
        release.set()
        engine.shutdown()
    return Run(elapsed, tasks, cancel_latencies)


async def run_engine_churn(cycles: int, tasks_per_cycle: int) -> Run:
    total_tasks = cycles * tasks_per_cycle
    started = time.perf_counter_ns()
    for _ in range(cycles):
        engine = Engine(
            worker_count=1, queue_capacity=queue_capacity(tasks_per_cycle, 1)
        )
        try:
            futures = [engine.submit_async(noop) for _ in range(tasks_per_cycle)]
            results = await asyncio.gather(*futures)
            assert results == [1] * tasks_per_cycle
        finally:
            engine.shutdown()
    elapsed = time.perf_counter_ns() - started
    return Run(elapsed, total_tasks)


async def run_mixed(tasks_per_kind: int) -> Run:
    workers = 4
    total_tasks = tasks_per_kind * 2
    engine = Engine(
        worker_count=workers,
        queue_capacity=queue_capacity(total_tasks, workers),
    )
    try:
        started = time.perf_counter_ns()
        sync_handles = [engine.submit(noop) for _ in range(tasks_per_kind)]
        async_futures = [engine.submit_async(noop) for _ in range(tasks_per_kind)]
        async_results = await asyncio.gather(*async_futures)
        sync_results = await asyncio.to_thread(wait_handles, sync_handles)
        elapsed = time.perf_counter_ns() - started
        assert async_results == [1] * tasks_per_kind
        assert sync_results == [1] * tasks_per_kind
    finally:
        engine.shutdown()
    return Run(elapsed, total_tasks)


def summarize(
    name: str, workers: int, tasks: int, runs: list[Run], latency_label: str
) -> dict:
    run_records = []
    for run in runs:
        per_task_ns = run.elapsed_ns / run.tasks
        record = {
            "us_per_task": per_task_ns / 1_000,
            "tasks_per_second": run.tasks / (run.elapsed_ns / 1_000_000_000),
        }
        if run.latencies_ns is not None:
            record["latency_p50_us"] = percentile(run.latencies_ns, 50) / 1_000
            record["latency_p99_us"] = percentile(run.latencies_ns, 99) / 1_000
        if run.loop_gaps_ns:
            record["loop_gap_p99_us"] = percentile(run.loop_gaps_ns, 99) / 1_000
            record["loop_gap_max_us"] = max(run.loop_gaps_ns) / 1_000
        run_records.append(record)

    result = {
        "name": name,
        "workers": workers,
        "tasks": tasks,
        "us_per_task": statistics.median(r["us_per_task"] for r in run_records),
        "tasks_per_second": statistics.median(
            r["tasks_per_second"] for r in run_records
        ),
        "runs": run_records,
    }
    for key in (
        "latency_p50_us",
        "latency_p99_us",
        "loop_gap_p99_us",
        "loop_gap_max_us",
    ):
        values = [r[key] for r in run_records if key in r]
        if values:
            result[key] = statistics.median(values)
    if latency_label and any("latency_p50_us" in r for r in run_records):
        result["latency_scope"] = latency_label
    return result


def benchmark(
    name: str,
    workers: int,
    tasks: int,
    operation,
    repeats: int,
    latency_label: str = "end_to_end",
) -> dict:
    runs: list[Run] = []
    for iteration in range(repeats + 1):
        gc.collect()
        run = operation()
        if run.tasks != tasks:
            raise RuntimeError(f"{name}: expected {tasks} tasks, observed {run.tasks}")
        if iteration > 0:
            runs.append(run)
    return summarize(name, workers, tasks, runs, latency_label)


async def stress_async(iterations: int, errors: list[dict]) -> None:
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    for iteration in range(iterations):
        engine = Engine(worker_count=4, queue_capacity=256)
        try:
            sync_handles = [engine.submit(noop) for _ in range(64)]
            futures = [engine.submit_async(noop) for _ in range(64)]
            for index, future in enumerate(futures):
                if index % 2 == 0:
                    assert future.cancel()
            async_results = await asyncio.gather(*futures, return_exceptions=True)
            sync_results = await asyncio.to_thread(wait_handles, sync_handles)

            for index, result in enumerate(async_results):
                if index % 2 == 0:
                    assert isinstance(result, asyncio.CancelledError)
                else:
                    assert result == 1
            assert sync_results == [1] * 64
        finally:
            engine.shutdown()


def stress_loop_close(iterations: int, errors: list[dict]) -> None:
    for _ in range(iterations):
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        task = BlockingTask(started, release)

        event_loop = asyncio.new_event_loop()
        event_loop.set_exception_handler(lambda _loop, context: errors.append(context))

        future = None
        try:
            future = event_loop.run_until_complete(submit_test_task(engine, task))
            assert started.wait(5)
            event_loop.close()
            release.set()
            engine.shutdown()
            assert future is not None and not future.done()
        finally:
            release.set()
            if not event_loop.is_closed():
                event_loop.close()


def run_stress(iterations: int, loop_close_iterations: int) -> dict:
    errors: list[dict] = []
    gc.collect()
    threads_before = threading.active_count()
    started = time.perf_counter_ns()
    asyncio.run(stress_async(iterations, errors))
    stress_loop_close(loop_close_iterations, errors)
    elapsed = time.perf_counter_ns() - started
    gc.collect()
    threads_after = threading.active_count()
    passed = not errors and threads_after == threads_before
    return {
        "iterations": iterations,
        "loop_close_iterations": loop_close_iterations,
        "elapsed_seconds": elapsed / 1_000_000_000,
        "threads_before": threads_before,
        "threads_after": threads_after,
        "unhandled_loop_errors": len(errors),
        "errors": errors,
        "passed": passed,
    }


def print_results(results: list[dict]) -> None:
    print()
    print(
        f"{'benchmark':<38} {'workers':>7} {'tasks':>7} {'us/task':>10} "
        f"{'p50 us':>9} {'p99 us':>9} {'tasks/s':>12} {'loop p99':>10} {'loop max':>10}"
    )
    for result in results:
        p50 = result.get("latency_p50_us")
        p99 = result.get("latency_p99_us")
        loop_p99 = result.get("loop_gap_p99_us")
        loop_max = result.get("loop_gap_max_us")
        print(
            f"{result['name']:<38} {result['workers']:>7} {result['tasks']:>7} "
            f"{result['us_per_task']:>10.3f} "
            f"{(f'{p50:.3f}' if p50 is not None else '-'):>9} "
            f"{(f'{p99:.3f}' if p99 is not None else '-'):>9} "
            f"{result['tasks_per_second']:>12,.0f} "
            f"{(f'{loop_p99:.3f}' if loop_p99 is not None else '-'):>10} "
            f"{(f'{loop_max:.3f}' if loop_max is not None else '-'):>10}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=4096)
    parser.add_argument("--sequential-tasks", type=int, default=2000)
    parser.add_argument("--cancellation-tasks", type=int, default=1000)
    parser.add_argument("--churn-cycles", type=int, default=20)
    parser.add_argument("--tasks-per-cycle", type=int, default=64)
    parser.add_argument("--mixed-tasks", type=int, default=2048)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--stress-iterations", type=int, default=100)
    parser.add_argument("--loop-close-iterations", type=int, default=10)
    parser.add_argument("--skip-stress", action="store_true")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--label", default="phase7f-asyncio-baseline")
    args = parser.parse_args()

    if args.repeats < 1:
        parser.error("--repeats must be at least 1")

    results = []
    results.append(
        benchmark(
            "sync_sequential_noop_1w",
            1,
            args.sequential_tasks,
            lambda: run_sync_sequential(args.sequential_tasks),
            args.repeats,
        )
    )
    results.append(
        benchmark(
            "async_sequential_noop_1w",
            1,
            args.sequential_tasks,
            lambda: asyncio.run(run_async_sequential(args.sequential_tasks)),
            args.repeats,
        )
    )

    for workers in (1, 2, 4, 8):
        results.append(
            benchmark(
                f"sync_burst_noop_{workers}w",
                workers,
                args.tasks,
                lambda workers=workers: run_sync_burst(args.tasks, workers),
                args.repeats,
            )
        )
        results.append(
            benchmark(
                f"async_burst_noop_{workers}w",
                workers,
                args.tasks,
                lambda workers=workers: asyncio.run(
                    run_async_burst(args.tasks, workers)
                ),
                args.repeats,
            )
        )
        results.append(
            benchmark(
                f"async_responsiveness_noop_{workers}w",
                workers,
                args.tasks,
                lambda workers=workers: asyncio.run(
                    run_responsiveness(args.tasks, workers)
                ),
                args.repeats,
            )
        )

    sleep_tasks = max(32, args.tasks // 16)
    results.append(
        benchmark(
            "async_burst_sleep_4w",
            4,
            sleep_tasks,
            lambda: asyncio.run(run_sleep_burst(sleep_tasks, 4)),
            args.repeats,
        )
    )
    results.append(
        benchmark(
            "async_completion_responsiveness_sleep_4w",
            4,
            sleep_tasks,
            lambda: asyncio.run(run_responsiveness(sleep_tasks, 4, sleep_task)),
            args.repeats,
        )
    )
    results.append(
        benchmark(
            "async_queued_cancellation_1w",
            1,
            args.cancellation_tasks,
            lambda: asyncio.run(run_queued_cancellation(args.cancellation_tasks)),
            args.repeats,
            latency_label="cancel_call",
        )
    )
    results.append(
        benchmark(
            "async_engine_churn_1w",
            1,
            args.churn_cycles * args.tasks_per_cycle,
            lambda: asyncio.run(
                run_engine_churn(args.churn_cycles, args.tasks_per_cycle)
            ),
            args.repeats,
        )
    )
    results.append(
        benchmark(
            "async_mixed_sync_async_4w",
            4,
            args.mixed_tasks * 2,
            lambda: asyncio.run(run_mixed(args.mixed_tasks)),
            args.repeats,
        )
    )

    stress = None
    if not args.skip_stress:
        stress = run_stress(args.stress_iterations, args.loop_close_iterations)

    metadata = {
        "label": args.label,
        "fluxion_version": __version__,
        "python_version": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "timestamp_ns": time.time_ns(),
        "config": {
            "tasks": args.tasks,
            "sequential_tasks": args.sequential_tasks,
            "cancellation_tasks": args.cancellation_tasks,
            "churn_cycles": args.churn_cycles,
            "tasks_per_cycle": args.tasks_per_cycle,
            "mixed_tasks": args.mixed_tasks,
            "repeats": args.repeats,
            "stress_iterations": args.stress_iterations,
            "loop_close_iterations": args.loop_close_iterations,
        },
    }

    print(json.dumps(metadata, indent=2))
    print_results(results)
    if stress is not None:
        print()
        print(
            f"stress: passed={stress['passed']} iterations={stress['iterations']} "
            f"loop_close_iterations={stress['loop_close_iterations']} "
            f"elapsed={stress['elapsed_seconds']:.3f}s "
            f"threads={stress['threads_before']}->{stress['threads_after']} "
            f"unhandled_errors={stress['unhandled_loop_errors']}"
        )

    if args.json_out is not None:
        payload = {
            "metadata": metadata,
            "benchmarks": results,
            "stress": stress,
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(payload, indent=2) + "\n")

    if stress is not None and not stress["passed"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
