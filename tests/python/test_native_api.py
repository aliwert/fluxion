import gc
import os
import subprocess
import sys
import textwrap
import threading
import weakref

import fluxion
import fluxion._core as core
import pytest
from fluxion import (
    Engine,
    NativeExecutionError,
    QueueFullError,
    TaskCancelledError,
    TaskError,
    TaskPanicError,
    TaskStatus,
)
from fluxion.native import NativeOperation, sum_squares


def run_isolated(source, timeout=10):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


def test_native_operation_is_opaque():
    assert type(sum_squares) is NativeOperation
    assert NativeOperation.__module__ == "fluxion.native"
    assert not callable(sum_squares)
    assert not hasattr(sum_squares, "__dict__")
    assert "sum_squares" in repr(sum_squares)

    with pytest.raises(TypeError):
        NativeOperation()

    with pytest.raises(TypeError):
        type("NativeOperationChild", (NativeOperation,), {})

    assert not hasattr(fluxion.native, "_test_probe")
    assert not hasattr(fluxion.native, "_test_panic")


def test_submit_native_rejects_non_operations():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        with pytest.raises(TypeError):
            engine.submit_native("sum_squares", [1, 2, 3])
        with pytest.raises(TypeError):
            engine.submit_native(lambda: 1, [1, 2, 3])
    finally:
        engine.close()


def test_native_error_is_a_task_error():
    assert issubclass(NativeExecutionError, TaskError)
    assert issubclass(NativeExecutionError, RuntimeError)
    assert "NativeExecutionError" in fluxion.__all__


def test_sum_squares_accepts_integer_iterables():
    engine = Engine(worker_count=2, queue_capacity=8)
    try:
        cases = [
            ([], 0),
            ([1, 2, 3], 14),
            (range(10), 285),
            ((-3, 4), 25),
            ((value for value in [2, 2]), 8),
            ([2**32], 2**64),
        ]

        for values, expected in cases:
            result = engine.submit_native(sum_squares, values).result(5)
            assert type(result) is int
            assert result == expected
    finally:
        engine.close()


@pytest.mark.parametrize(
    ("values", "exception"),
    [
        (None, TypeError),
        (1, TypeError),
        (object(), TypeError),
        ("123", TypeError),
        (b"123", TypeError),
        (bytearray(b"123"), TypeError),
        ([1, "2"], TypeError),
        ([1, 2.0], TypeError),
        ([True], TypeError),
        ([2**63], OverflowError),
        ([-(2**63) - 1], OverflowError),
    ],
)
def test_invalid_native_input_fails_before_execution(values, exception):
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        with pytest.raises(exception):
            engine.submit_native(sum_squares, values)

        assert engine.submit_native(sum_squares, [1, 2]).result(5) == 5
    finally:
        engine.close()


def test_untrusted_iterable_length_does_not_preallocate_unbounded():
    class LyingLength:
        def __len__(self):
            return 2**62

        def __iter__(self):
            return iter([1, 2, 3])

    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        assert engine.submit_native(sum_squares, LyingLength()).result(5) == 14
    finally:
        engine.close()


def test_native_input_is_copied_before_execution():
    engine = Engine(worker_count=1, queue_capacity=4)
    values = [1, 2, 3]
    try:
        handle = engine.submit_native(sum_squares, values)
        values[:] = [100, 200, 300]
        assert handle.result(5) == 14
    finally:
        engine.close()


def test_native_output_supports_128_bit_integers():
    engine = Engine(worker_count=1, queue_capacity=4)
    value = 2**63 - 1
    try:
        result = engine.submit_native(sum_squares, [value, -value]).result(5)
        assert result == 2 * value * value
        assert result > 2**126
    finally:
        engine.close()


def test_native_overflow_is_repeatable_and_worker_survives():
    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        handle = engine.submit_native(sum_squares, [2**63 - 1] * 3)

        with pytest.raises(NativeExecutionError, match="overflow") as caught:
            handle.result(5)
        exception = caught.value

        assert isinstance(exception, NativeExecutionError)
        assert handle.exception() is exception
        assert handle.exception() is exception
        with pytest.raises(NativeExecutionError) as caught_again:
            handle.wait(5)
        assert caught_again.value is exception
        assert handle.done()
        assert not handle.running()
        assert handle.status == TaskStatus.FAILED

        assert engine.submit_native(sum_squares, [1, 2, 3]).result(5) == 14
    finally:
        engine.close()


def test_native_results_and_exceptions_are_repeatable():
    engine = Engine(worker_count=2, queue_capacity=16)
    try:
        handle = engine.submit_native(sum_squares, range(100))
        first = handle.result(5)
        assert handle.result(5) == first
        assert handle.wait(5) == first
        assert handle.exception() is None
        assert handle.status == TaskStatus.COMPLETED

        failed = engine.submit_native(sum_squares, [2**63 - 1] * 3)
        exception = failed.exception(5)
        assert isinstance(exception, NativeExecutionError)
        assert failed.exception(5) is exception
    finally:
        engine.close()


def test_multiple_waiters_share_native_result():
    engine = Engine(worker_count=2, queue_capacity=16)
    barrier = threading.Barrier(5)
    results = []
    errors = []

    def waiter():
        barrier.wait()
        try:
            results.append(handle.result(5))
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    try:
        handle = engine.submit_native(sum_squares, range(10_000))
        threads = [threading.Thread(target=waiter) for _ in range(4)]
        for thread in threads:
            thread.start()

        barrier.wait()
        for thread in threads:
            thread.join(5)
            assert not thread.is_alive()

        expected = sum(value * value for value in range(10_000))
        assert errors == []
        assert results == [expected] * 4
        assert handle.result(5) == expected
    finally:
        engine.close()


def test_queued_native_task_can_be_cancelled():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocker():
        started.set()
        assert release.wait(5)

    try:
        blocking = engine.submit(blocker)
        assert started.wait(5)

        handle = engine.submit_native(sum_squares, [1, 2, 3])
        assert handle.cancel()
        with pytest.raises(TaskCancelledError):
            handle.result(5)
        assert handle.cancelled()
    finally:
        release.set()
        assert blocking.result(5) is None
        engine.close()


def test_running_native_task_cannot_be_cancelled():
    probe_type = getattr(core, "_NativeProbe", None)
    probe_operation = getattr(core, "_test_probe", None)
    if probe_type is None or probe_operation is None:
        pytest.skip("native probes are available only in debug builds")
    engine = Engine(worker_count=1, queue_capacity=8)
    probe = probe_type(participants=1, block_until_release=True, timeout=5)

    try:
        handle = engine.submit_native(probe_operation, probe)
        assert probe.wait_started(5)
        assert not handle.cancel()
        assert handle.running()

        assert probe.release()
        assert handle.result(5) is True
    finally:
        probe.release()
        engine.close()


def test_native_queue_full_releases_input():
    class TrackedValues:
        def __iter__(self):
            return iter([1, 2, 3])

    engine = Engine(worker_count=1, queue_capacity=1)
    started = threading.Event()
    release = threading.Event()

    def blocker():
        started.set()
        assert release.wait(5)

    values = TrackedValues()
    values_ref = weakref.ref(values)

    try:
        blocking = engine.submit(blocker)
        assert started.wait(5)
        queued = engine.submit_native(sum_squares, [4, 5])

        with pytest.raises(QueueFullError):
            engine.submit_native(sum_squares, values)

        del values
        gc.collect()
        assert values_ref() is None

        release.set()
        assert blocking.result(5) is None
        assert queued.result(5) == 41
    finally:
        release.set()
        engine.close()


def test_native_tasks_are_drained_by_close():
    engine = Engine(worker_count=2, queue_capacity=32)
    handles = [
        engine.submit_native(sum_squares, range(value + 1)) for value in range(20)
    ]
    engine.close()

    results = [handle.result(5) for handle in handles]
    assert results == [
        sum(item * item for item in range(value + 1)) for value in range(20)
    ]


def test_native_submission_after_close_is_rejected():
    engine = Engine(worker_count=1, queue_capacity=4)
    engine.close()

    with pytest.raises(fluxion.EngineClosedError):
        engine.submit_native(sum_squares, [1, 2, 3])


def test_many_native_tasks_complete_and_handles_are_stable():
    engine = Engine(worker_count=4, queue_capacity=2048)
    handles = [engine.submit_native(sum_squares, [value]) for value in range(2000)]
    engine.close()

    assert [handle.result(5) for handle in handles] == [
        value * value for value in range(2000)
    ]
    assert all(handle.done() for handle in handles)
    assert all(handle.exception(0) is None for handle in handles)


def test_python_and_native_submissions_can_be_mixed():
    engine = Engine(worker_count=2, queue_capacity=16)
    try:
        native_handle = engine.submit_native(sum_squares, range(10))
        python_handle = engine.submit(lambda: "python")

        def nested_submission():
            return engine.submit_native(sum_squares, [2, 3]).result(5)

        nested_handle = engine.submit(nested_submission)

        assert native_handle.result(5) == 285
        assert python_handle.result(5) == "python"
        assert nested_handle.result(5) == 13
    finally:
        engine.close()


def test_native_submissions_from_multiple_python_threads():
    engine = Engine(worker_count=4, queue_capacity=64)
    results = []
    errors = []

    def submit_batch(offset):
        try:
            handles = [
                engine.submit_native(sum_squares, range(value + 1))
                for value in range(offset, offset + 8)
            ]
            results.extend(handle.result(5) for handle in handles)
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    threads = [
        threading.Thread(target=submit_batch, args=(offset,))
        for offset in (0, 8, 16, 24)
    ]

    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        assert not errors
        assert all(not thread.is_alive() for thread in threads)
        assert sorted(results) == [
            sum(value * value for value in range(index + 1)) for index in range(32)
        ]
    finally:
        engine.close()


def test_native_handle_outlives_implicit_engine_destruction():
    result = run_isolated(
        """
        from fluxion import Engine
        from fluxion.native import sum_squares

        engine = Engine(worker_count=1, queue_capacity=4)
        handle = engine.submit_native(sum_squares, range(10))
        del engine
        assert handle.result(5) == 285
        """
    )

    assert result.returncode == 0, result.stderr


def test_native_panic_is_contained_and_worker_survives():
    panic_operation = getattr(core, "_test_panic", None)
    if panic_operation is None:
        pytest.skip("native panic probe is available only in debug builds")
    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        handle = engine.submit_native(panic_operation, [])
        with pytest.raises(TaskPanicError):
            handle.result(5)
        assert handle.status == TaskStatus.FAILED
        assert engine.submit_native(sum_squares, [1, 2, 3]).result(5) == 14
    finally:
        engine.close()


def test_native_probe_timeout_is_reported_and_worker_survives():
    probe_type = getattr(core, "_NativeProbe", None)
    probe_operation = getattr(core, "_test_probe", None)
    if probe_type is None or probe_operation is None:
        pytest.skip("native probes are available only in debug builds")
    engine = Engine(worker_count=1, queue_capacity=8)
    probe = probe_type(participants=2, timeout=0.05)

    try:
        handle = engine.submit_native(probe_operation, probe)
        with pytest.raises(NativeExecutionError, match="timed out"):
            handle.result(5)
        assert engine.submit_native(sum_squares, [1]).result(5) == 1
    finally:
        probe.release()
        engine.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
def test_native_submission_rejects_forked_child_process():
    result = run_isolated(
        """
        import os
        from fluxion import Engine, ForkedProcessError
        from fluxion.native import sum_squares

        engine = Engine(worker_count=1, queue_capacity=8)
        handle = engine.submit_native(sum_squares, [1, 2])
        assert handle.result(5) == 5

        pid = os.fork()
        if pid == 0:
            try:
                operations = [
                    lambda: engine.submit_native(sum_squares, [3]),
                    lambda: handle.result(0.1),
                    lambda: handle.exception(0.1),
                    lambda: handle.cancel(),
                ]
                for operation in operations:
                    try:
                        operation()
                    except ForkedProcessError:
                        pass
                    else:
                        raise AssertionError("forked native operation did not fail")
            except BaseException:
                os._exit(1)
            os._exit(0)

        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        engine.close()
        """
    )

    assert result.returncode == 0, result.stderr
