import subprocess
import sys
import textwrap
import threading
from importlib.metadata import version

import pytest

import fluxion
from fluxion import Engine, TaskHandle


def run_isolated(source, timeout=10):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_public_api_is_exposed():
    assert isinstance(fluxion.__version__, str)
    assert fluxion.__version__
    assert version("fluxion") == fluxion.__version__
    assert Engine.__name__ == "Engine"
    assert TaskHandle.__name__ == "TaskHandle"
    assert Engine.__module__ == "fluxion._core"
    assert TaskHandle.__module__ == "fluxion._core"


def test_engine_configuration_is_exposed():
    engine = Engine(worker_count=2, queue_capacity=16)
    try:
        assert engine.worker_count == 2
        assert engine.queue_capacity == 16
    finally:
        engine.shutdown()


def test_engine_uses_core_defaults():
    engine = Engine()
    try:
        assert engine.worker_count >= 1
        assert engine.queue_capacity == 1024
    finally:
        engine.shutdown()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"worker_count": 0},
        {"queue_capacity": 0},
    ],
)
def test_invalid_configuration_raises_value_error(kwargs):
    with pytest.raises(ValueError):
        Engine(**kwargs)


def test_negative_configuration_raises_overflow_error():
    with pytest.raises(OverflowError):
        Engine(worker_count=-1)


def test_submit_and_wait_return_python_objects():
    engine = Engine(worker_count=2, queue_capacity=8)
    try:
        sentinel = object()

        handle = engine.submit(lambda: sentinel)

        assert isinstance(handle, TaskHandle)
        assert handle.wait() is sentinel
        assert engine.submit(lambda: [1, 2, 3]).wait() == [1, 2, 3]
    finally:
        engine.shutdown()


def test_python_exception_propagates_and_worker_survives():
    class CallbackError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        error = CallbackError("callback failed")

        def fail():
            raise error

        handle = engine.submit(fail)

        with pytest.raises(CallbackError, match="callback failed") as caught:
            handle.wait()

        assert caught.value is error
        assert engine.submit(lambda: 7).wait() == 7
    finally:
        engine.shutdown()


def test_multiple_handles_can_be_waited_out_of_order():
    engine = Engine(worker_count=2, queue_capacity=16)
    try:
        handles = [
            engine.submit(lambda value=value: value * 2) for value in range(5)
        ]

        results = [handles[index].wait() for index in reversed(range(5))]

        assert results == [8, 6, 4, 2, 0]
    finally:
        engine.shutdown()


def test_wait_can_only_be_called_once():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: 1)

        assert handle.wait() == 1
        with pytest.raises(RuntimeError, match="already been consumed"):
            handle.wait()
    finally:
        engine.shutdown()


def test_dropped_handles_do_not_block_workers():
    engine = Engine(worker_count=1, queue_capacity=16)
    try:
        completed = []

        for value in range(5):
            engine.submit(lambda value=value: completed.append(value))

        assert engine.submit(lambda: "marker").wait() == "marker"
        assert completed == list(range(5))
    finally:
        engine.shutdown()


def test_shutdown_waits_for_accepted_tasks_and_preserves_results():
    engine = Engine(worker_count=2, queue_capacity=16)
    handles = [engine.submit(lambda value=value: value * value) for value in range(8)]

    engine.shutdown()

    assert [handle.wait() for handle in handles] == [0, 1, 4, 9, 16, 25, 36, 49]


def test_submit_after_shutdown_raises_runtime_error():
    engine = Engine(worker_count=1, queue_capacity=4)
    engine.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)

    with pytest.raises(RuntimeError, match="shut down"):
        engine.shutdown()


def test_submit_rejects_non_callable():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        with pytest.raises(TypeError, match="not callable"):
            engine.submit(42)
    finally:
        engine.shutdown()


def test_queue_full_maps_to_runtime_error():
    engine = Engine(worker_count=1, queue_capacity=1)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return "first"

    try:
        first = engine.submit(blocking_task)
        assert started.wait(5)

        second = engine.submit(lambda: "second")
        with pytest.raises(RuntimeError, match="queue is full"):
            engine.submit(lambda: "third")
    finally:
        release.set()

    assert first.wait() == "first"
    assert second.wait() == "second"
    engine.shutdown()


def test_multiple_python_threads_can_submit_and_wait():
    engine = Engine(worker_count=4, queue_capacity=64)
    results = []
    errors = []

    def submit_batch(offset):
        try:
            handles = [
                engine.submit(lambda value=value: value)
                for value in range(offset, offset + 5)
            ]
            results.extend(handle.wait() for handle in handles)
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(target=submit_batch, args=(offset,))
        for offset in (0, 5, 10, 15)
    ]

    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        assert not errors
        assert all(not thread.is_alive() for thread in threads)
        assert sorted(results) == list(range(20))
    finally:
        engine.shutdown()


def test_wait_releases_gil_while_callback_is_pending():
    result = run_isolated(
        """
        from fluxion import Engine

        engine = Engine(worker_count=1, queue_capacity=4)
        try:
            assert engine.submit(lambda: 42).wait() == 42
        finally:
            engine.shutdown()
        """
    )

    assert result.returncode == 0, result.stderr


def test_shutdown_releases_gil_while_waiting_for_callbacks():
    result = run_isolated(
        """
        from fluxion import Engine

        engine = Engine(worker_count=2, queue_capacity=8)
        handles = [
            engine.submit(lambda value=value: value) for value in range(8)
        ]
        engine.shutdown()
        assert [handle.wait() for handle in handles] == list(range(8))
        """
    )

    assert result.returncode == 0, result.stderr


def test_implicit_engine_destruction_releases_gil():
    result = run_isolated(
        """
        from fluxion import Engine

        engine = Engine(worker_count=1, queue_capacity=4)
        handle = engine.submit(lambda: 99)
        del engine
        assert handle.wait() == 99
        """
    )

    assert result.returncode == 0, result.stderr
