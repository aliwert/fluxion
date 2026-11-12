import asyncio
import concurrent.futures
import functools
import gc
import inspect
import os
import subprocess
import sys
import textwrap
import threading
import time
import warnings
import weakref
from importlib.metadata import version
from importlib.resources import files

import pytest

import fluxion
from fluxion import (
    Engine,
    EngineClosedError,
    EngineError,
    EngineState,
    FluxionError,
    ForkedProcessError,
    QueueFullError,
    TaskCancelledError,
    TaskError,
    TaskHandle,
    TaskPanicError,
    TaskStatus,
    TaskTimeoutError,
    WorkerDisconnectedError,
)


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
    assert Engine.__module__ == "fluxion"
    assert TaskHandle.__module__ == "fluxion"
    assert TaskStatus.QUEUED == "queued"
    assert EngineState.RUNNING == "running"


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


def test_negative_configuration_raises_value_error():
    with pytest.raises(ValueError):
        Engine(worker_count=-1)

    with pytest.raises(ValueError):
        Engine(queue_capacity=-1)

    with pytest.raises(ValueError):
        Engine(worker_count=True)

    with pytest.raises(TypeError):
        Engine(worker_count="2")


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


def test_wait_and_result_are_repeatable():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: [1, 2, 3])

        first = handle.wait()
        assert handle.result() is first
        assert handle.wait() is first
        assert first == [1, 2, 3]
    finally:
        engine.shutdown()


def test_multiple_waiters_share_the_same_result():
    engine = Engine(worker_count=2, queue_capacity=16)
    started = threading.Event()
    release = threading.Event()
    barrier = threading.Barrier(5)
    sentinel = object()
    results = []
    errors = []

    def task():
        started.set()
        assert release.wait(5)
        return sentinel

    def waiter():
        barrier.wait()
        try:
            results.append(handle.result(timeout=5))
        except BaseException as error:  # pragma: no cover - assertion helper
            errors.append(error)

    try:
        handle = engine.submit(task)
        workers = [threading.Thread(target=waiter) for _ in range(4)]
        for worker in workers:
            worker.start()

        assert started.wait(5)
        barrier.wait()
        release.set()

        for worker in workers:
            worker.join(5)
            assert not worker.is_alive()

        assert errors == []
        assert len(results) == 4
        assert all(result is sentinel for result in results)
        assert handle.exception() is None
        assert handle.result() is sentinel
        assert handle.done()
        assert not handle.running()
    finally:
        release.set()
        engine.shutdown()


def test_exception_returns_stored_callback_exception():
    class CallbackError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=4)
    error = CallbackError("callback failed")
    try:
        handle = engine.submit(lambda: (_ for _ in ()).throw(error))

        with pytest.raises(CallbackError, match="callback failed") as caught:
            handle.result()
        assert caught.value is error

        assert handle.exception() is error
        assert handle.exception() is error
        with pytest.raises(CallbackError) as caught:
            handle.wait()
        assert caught.value is error
    finally:
        engine.shutdown()


def test_exception_and_result_share_cancelled_error_identity():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocker():
        started.set()
        assert release.wait(5)
        return None

    try:
        first = engine.submit(blocker)
        assert started.wait(5)
        target = engine.submit(lambda: None)
        assert target.cancel()

        exception = target.exception()
        assert isinstance(exception, TaskCancelledError)
        assert target.exception() is exception
        with pytest.raises(TaskCancelledError) as caught:
            target.result()
        assert caught.value is exception
    finally:
        release.set()
        assert first.wait(5) is None
        engine.shutdown()


def test_cancel_while_another_thread_waits():
    engine = Engine(worker_count=1, queue_capacity=8)
    blocker_started = threading.Event()
    release_blocker = threading.Event()
    waiter_done = threading.Event()
    errors = []

    def blocker():
        blocker_started.set()
        assert release_blocker.wait(5)
        return None

    def waiter():
        try:
            target.result(timeout=5)
        except BaseException as error:
            errors.append(error)
        finally:
            waiter_done.set()

    try:
        blocker_handle = engine.submit(blocker)
        assert blocker_started.wait(5)
        target = engine.submit(lambda: None)

        worker = threading.Thread(target=waiter)
        worker.start()
        time.sleep(0.05)
        assert target.cancel()
        assert waiter_done.wait(5)
        worker.join(5)
        assert not worker.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], TaskCancelledError)
    finally:
        release_blocker.set()
        assert blocker_handle.wait(5) is None
        engine.shutdown()


def test_done_implies_result_is_immediately_available():
    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        for value in range(100):
            handle = engine.submit(lambda value=value: value)
            deadline = time.monotonic() + 5
            while not handle.done() and time.monotonic() < deadline:
                time.sleep(0.0001)
            assert handle.done()
            assert handle.result(timeout=0) == value
            assert handle.exception(timeout=0) is None
    finally:
        engine.shutdown()


def test_running_reflects_lifecycle():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def task():
        started.set()
        assert release.wait(5)
        return "done"

    try:
        handle = engine.submit(task)
        assert started.wait(5)
        assert handle.running()
        assert not handle.done()

        release.set()
        assert handle.result() == "done"
        assert not handle.running()
        assert handle.done()
    finally:
        release.set()
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

    assert engine.shutdown() is None


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


def test_shutdown_from_worker_callback_is_rejected_and_engine_remains_usable():
    engine = Engine(worker_count=1, queue_capacity=8)
    errors = []

    def callback():
        try:
            engine.shutdown()
        except RuntimeError as error:
            errors.append(error)
        return 42

    try:
        assert engine.submit(callback).wait() == 42
        assert len(errors) == 1
        assert "cannot be shut down from one of its worker threads" in str(errors[0])
        assert engine.submit(lambda: 7).wait() == 7
    finally:
        engine.shutdown()

    assert engine.shutdown() is None


def test_worker_close_during_concurrent_close_is_rejected_without_deadlock():
    engine = Engine(worker_count=1, queue_capacity=8)
    callback_started = threading.Event()
    release_callback = threading.Event()
    callback_done = threading.Event()
    close_done = threading.Event()
    errors = []

    def callback():
        callback_started.set()
        assert release_callback.wait(5)
        try:
            engine.close()
        except RuntimeError as error:
            errors.append(error)
        finally:
            callback_done.set()
        return 42

    handle = engine.submit(callback)
    assert callback_started.wait(5)

    def close_engine():
        engine.close()
        close_done.set()

    closer = threading.Thread(target=close_engine, daemon=True)
    closer.start()

    deadline = time.monotonic() + 5
    while engine.state is not EngineState.CLOSING:
        assert time.monotonic() < deadline, "engine did not enter closing state"
        time.sleep(0.001)

    release_callback.set()
    assert callback_done.wait(5), "worker close deadlocked"
    assert close_done.wait(5)
    closer.join(5)
    assert not closer.is_alive()

    assert handle.wait(5) == 42
    assert errors
    assert "cannot be shut down from one of its worker threads" in str(errors[0])
    assert engine.state is EngineState.CLOSED


def test_async_close_from_worker_callback_is_rejected():
    engine = Engine(worker_count=1, queue_capacity=8)
    errors = []

    def callback():
        try:
            engine.aclose()
        except RuntimeError as error:
            errors.append(error)
        return 42

    try:
        assert engine.submit(callback).wait() == 42
        assert len(errors) == 1
        assert "cannot be shut down from one of its worker threads" in str(errors[0])
        assert engine.submit(lambda: 7).wait() == 7
    finally:
        engine.shutdown()


def test_precreated_aclose_awaited_from_worker_fails_without_deadlock():
    result = run_isolated(
        """
        import asyncio
        from fluxion import Engine, EngineError

        engine = Engine(worker_count=1, queue_capacity=8)
        close_coroutine = engine.aclose()
        errors = []

        def callback():
            try:
                asyncio.run(close_coroutine)
            except EngineError as error:
                errors.append(error)
            else:
                raise AssertionError("pre-created aclose() did not fail")
            return 42

        try:
            assert engine.submit(callback).wait(5) == 42
            assert len(errors) == 1
            assert "cannot be shut down from one of its worker threads" in str(errors[0])
            assert engine.submit(lambda: 7).wait(5) == 7
        finally:
            engine.close()
        """,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr


def test_engine_destruction_from_worker_callback_drains_accepted_tasks():
    engine = Engine(worker_count=1, queue_capacity=8)
    callback_started = threading.Event()
    release_callback = threading.Event()
    completed = []

    def callback():
        nonlocal engine
        callback_started.set()
        assert release_callback.wait(5)
        del engine
        return "destroyed"

    destructor = engine.submit(callback)
    queued = [
        engine.submit(lambda value=value: completed.append(value) or value)
        for value in range(4)
    ]

    assert callback_started.wait(5)
    release_callback.set()

    assert destructor.wait() == "destroyed"
    assert [handle.wait() for handle in queued] == [0, 1, 2, 3]
    assert completed == [0, 1, 2, 3]


def test_concurrent_submit_and_shutdown_have_deterministic_admission():
    engine = Engine(worker_count=2, queue_capacity=512)
    barrier = threading.Barrier(5)
    accepted = []
    rejected = []
    accepted_lock = threading.Lock()

    def submit_batch():
        barrier.wait()
        local_handles = []

        for _ in range(32):
            try:
                local_handles.append(engine.submit(lambda: 1))
            except RuntimeError as error:
                with accepted_lock:
                    rejected.append(error)

        with accepted_lock:
            accepted.extend(local_handles)

    def shutdown():
        barrier.wait()
        engine.shutdown()

    threads = [
        threading.Thread(target=submit_batch),
        threading.Thread(target=submit_batch),
        threading.Thread(target=submit_batch),
        threading.Thread(target=submit_batch),
        threading.Thread(target=shutdown),
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert all(not thread.is_alive() for thread in threads)
    assert len(accepted) + len(rejected) == 128
    assert all("already been shut down" in str(error) for error in rejected)
    assert [handle.wait() for handle in accepted] == [1] * len(accepted)


def test_task_lifecycle_and_cancel_before_start():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()
    executed = []

    def blocker():
        started.set()
        assert release.wait(5)

    try:
        blocking_handle = engine.submit(blocker)
        assert started.wait(5)

        handle = engine.submit(lambda: executed.append(True))

        assert handle.status == "queued"
        assert not handle.done()
        assert not handle.cancelled()
        assert handle.cancel() is True
        assert handle.cancel() is False
        assert handle.status == "cancelled"
        assert handle.done()
        assert handle.cancelled()

        with pytest.raises(concurrent.futures.CancelledError):
            handle.wait()

        assert handle.status == "cancelled"
        assert executed == []

        release.set()
        assert blocking_handle.wait() is None
    finally:
        release.set()
        engine.shutdown()


def test_cancel_prefetched_task_prevents_execution():
    engine = Engine(worker_count=1, queue_capacity=32)
    blocker_started = threading.Event()
    release_blocker = threading.Event()
    running_started = threading.Event()
    release_running = threading.Event()
    target_executed = []

    def blocker():
        blocker_started.set()
        assert release_blocker.wait(5)

    def running():
        running_started.set()
        assert release_running.wait(5)

    try:
        blocking_handle = engine.submit(blocker)
        assert blocker_started.wait(5)

        running_handle = engine.submit(running)
        target = engine.submit(lambda: target_executed.append(True))
        fillers = [engine.submit(lambda: None) for _ in range(6)]

        release_blocker.set()
        assert running_started.wait(5)

        assert target.status == "queued"
        assert target.cancel() is True

        release_running.set()
        assert running_handle.wait() is None

        with pytest.raises(concurrent.futures.CancelledError):
            target.wait()

        assert target.status == "cancelled"
        assert target_executed == []
        assert blocking_handle.wait() is None
        assert all(handle.wait() is None for handle in fillers)
    finally:
        release_blocker.set()
        release_running.set()
        engine.shutdown()


def test_wait_timeout_preserves_handle_and_does_not_cancel_task():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return "finished"

    try:
        handle = engine.submit(blocking_task)
        assert started.wait(5)

        with pytest.raises(concurrent.futures.TimeoutError):
            handle.wait(timeout=0.05)

        assert handle.status == "running"
        assert not handle.done()
        assert handle.cancel() is False

        release.set()
        assert handle.wait(timeout=5) == "finished"
        assert handle.status == "completed"
        assert handle.done()
        assert not handle.cancelled()
    finally:
        release.set()
        engine.shutdown()


@pytest.mark.parametrize("timeout", [0.0, -1.0])
def test_zero_and_negative_timeouts_are_immediate(timeout):
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return "finished"

    try:
        handle = engine.submit(blocking_task)
        assert started.wait(5)

        with pytest.raises(concurrent.futures.TimeoutError):
            handle.wait(timeout=timeout)

        release.set()
        assert handle.wait(timeout=5) == "finished"
    finally:
        release.set()
        engine.shutdown()


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_timeouts_raise_value_error(timeout):
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: 42)

        with pytest.raises(ValueError, match="finite"):
            handle.wait(timeout=timeout)

        assert handle.wait(timeout=5) == 42
    finally:
        engine.shutdown()


def test_out_of_range_timeout_raises_value_error():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: 42)

        with pytest.raises(ValueError, match="out of range"):
            handle.wait(timeout=1e300)

        assert handle.wait(timeout=5) == 42
    finally:
        engine.shutdown()


def test_status_reports_callback_failure_before_wait():
    class CallbackError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=4)
    callback_finished = threading.Event()
    error = CallbackError("callback failed")

    def fail():
        callback_finished.set()
        raise error

    try:
        handle = engine.submit(fail)
        assert callback_finished.wait(5)

        deadline = time.monotonic() + 5
        while not handle.done() and time.monotonic() < deadline:
            time.sleep(0.001)

        assert handle.done()
        assert handle.status == "failed"
        assert not handle.cancelled()

        with pytest.raises(CallbackError, match="callback failed") as caught:
            handle.wait()

        assert caught.value is error
        assert handle.status == "failed"
    finally:
        engine.shutdown()


def test_completed_status_is_visible_before_wait():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: "done")

        deadline = time.monotonic() + 5
        while not handle.done() and time.monotonic() < deadline:
            time.sleep(0.001)

        assert handle.done()
        assert handle.status == "completed"
        assert not handle.cancelled()
        assert handle.wait() == "done"
        assert handle.status == "completed"
    finally:
        engine.shutdown()


def test_submit_async_returns_future_bound_to_running_loop():
    async def main():
        loop = asyncio.get_running_loop()
        engine = Engine(worker_count=2, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)

            assert isinstance(future, asyncio.Future)
            assert future.get_loop() is loop
            assert not future.done()
            assert await future == 42
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_from_background_event_loop_thread():
    started = threading.Event()
    result = {}
    errors = []

    def run_loop():
        async def main():
            loop = asyncio.get_running_loop()
            engine = Engine(worker_count=1, queue_capacity=8)
            try:
                future = engine.submit_async(lambda: 42)
                assert future.get_loop() is loop
                started.set()
                result["value"] = await future
            finally:
                engine.shutdown()

        try:
            asyncio.run(main())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run_loop)
    thread.start()
    try:
        assert started.wait(5)
        thread.join(5)
        assert not thread.is_alive()
        assert errors == []
        assert result["value"] == 42
    finally:
        thread.join(5)


def test_submit_async_requires_a_running_event_loop():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        with pytest.raises(RuntimeError, match="running event loop"):
            engine.submit_async(lambda: 1)
    finally:
        engine.shutdown()


def test_submit_async_rejects_non_callable_and_submissions_after_shutdown():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=4)
        try:
            with pytest.raises(TypeError, match="not callable"):
                engine.submit_async(42)

            engine.shutdown()
            with pytest.raises(RuntimeError, match="shut down"):
                engine.submit_async(lambda: 1)
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_queue_full_does_not_leave_unobserved_error():
    async def main():
        loop = asyncio.get_running_loop()
        messages = []
        loop.set_exception_handler(lambda _loop, context: messages.append(context))
        engine = Engine(worker_count=1, queue_capacity=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_task():
            started.set()
            assert release.wait(5)
            return "first"

        try:
            first = engine.submit(blocking_task)
            assert await asyncio.to_thread(started.wait, 5)
            second = engine.submit(lambda: "second")

            with pytest.raises(RuntimeError, match="queue is full"):
                engine.submit_async(lambda: "third")

            await asyncio.sleep(0)
            assert not messages

            release.set()
            assert await asyncio.to_thread(first.wait, 5) == "first"
            assert await asyncio.to_thread(second.wait, 5) == "second"
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_preserves_callback_exception_identity():
    class CallbackError(Exception):
        pass

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        error = CallbackError("async callback failed")

        def fail():
            raise error

        try:
            future = engine.submit_async(fail)

            with pytest.raises(CallbackError, match="async callback failed") as caught:
                await future

            assert caught.value is error
            assert caught.value.__traceback__ is not None
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_multiple_concurrent_submissions():
    async def main():
        engine = Engine(worker_count=2, queue_capacity=64)
        try:
            futures = [
                engine.submit_async(lambda value=value: value * 2)
                for value in range(16)
            ]

            assert await asyncio.gather(*futures) == [value * 2 for value in range(16)]
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_queued_cancellation_prevents_execution():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        executed = []

        def blocker():
            started.set()
            assert release.wait(5)

        try:
            blocker_handle = engine.submit(blocker)
            assert await asyncio.to_thread(started.wait, 5)

            future = engine.submit_async(lambda: executed.append(True))
            assert not future.done()
            assert future.cancel() is True

            with pytest.raises(asyncio.CancelledError):
                await future

            release.set()
            marker = engine.submit(lambda: 17)
            assert await asyncio.to_thread(marker.wait, 5) == 17
            assert await asyncio.to_thread(blocker_handle.wait, 5) is None
            assert executed == []
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancel_accepts_message_keyword():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)
            supports_cancel_message = (
                "msg" in inspect.signature(asyncio.Future.cancel).parameters
            )

            if not supports_cancel_message:
                with pytest.raises(TypeError):
                    future.cancel(msg="cancelled by test")
                assert await future == 42
            else:
                assert future.cancel(msg="cancelled by test") is True
                with pytest.raises(asyncio.CancelledError) as caught:
                    await future
                assert caught.value.args == ("cancelled by test",)
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_wait_for_timeout_cancels_task_awaiting_future():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        executed = []

        def callback():
            started.set()
            assert release.wait(5)
            executed.append(True)
            return "done"

        try:
            future = engine.submit_async(callback)
            assert await asyncio.to_thread(started.wait, 5)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(future, 0.05)
            assert future.cancelled()

            release.set()
            marker = engine.submit(lambda: 11)
            assert await asyncio.to_thread(marker.wait, 5) == 11
            assert executed == [True]
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancellation_after_start_does_not_interrupt_callback():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        executed = []

        def callback():
            started.set()
            assert release.wait(5)
            executed.append(True)
            return 7

        try:
            future = engine.submit_async(callback)
            assert await asyncio.to_thread(started.wait, 5)
            assert future.cancel() is True

            with pytest.raises(asyncio.CancelledError):
                await future

            release.set()
            marker = engine.submit(lambda: 11)
            assert await asyncio.to_thread(marker.wait, 5) == 11
            assert executed == [True]
            assert future.cancelled()
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancel_after_completion_preserves_result():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)
            assert await future == 42

            assert future.cancel() is False
            assert future.result() == 42
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancellation_completion_races_are_safe():
    async def main():
        engine = Engine(worker_count=2, queue_capacity=64)
        try:
            for _ in range(64):
                future = engine.submit_async(lambda: 1)
                await asyncio.sleep(0)

                if future.cancel():
                    with pytest.raises(asyncio.CancelledError):
                        await future
                else:
                    assert await future == 1

                await asyncio.sleep(0)
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_discards_result_after_event_loop_closed():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()
    loop = asyncio.new_event_loop()
    shutdown = False

    async def submit():
        def callback():
            started.set()
            assert release.wait(5)
            return "done"

        return engine.submit_async(callback)

    try:
        future = loop.run_until_complete(submit())
        assert started.wait(5)
        loop.close()
        release.set()

        engine.shutdown()
        shutdown = True

        assert not future.done()
    finally:
        release.set()
        if not loop.is_closed():
            loop.close()
        if not shutdown:
            engine.shutdown()


def test_submit_async_keeps_event_loop_responsive():
    async def main():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()
        engine = Engine(worker_count=1, queue_capacity=8)

        def callback():
            loop.call_soon_threadsafe(started.set)
            assert release.wait(5)
            return "done"

        try:
            future = engine.submit_async(callback)
            await asyncio.wait_for(started.wait(), 5)

            for _ in range(3):
                await asyncio.sleep(0)

            assert not future.done()
            release.set()
            assert await asyncio.wait_for(future, 5) == "done"
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_engine_shutdown_with_pending_async_tasks_completes_futures():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        shutdown = False

        def blocking_task():
            started.set()
            assert release.wait(5)
            return "first"

        try:
            first = engine.submit_async(blocking_task)
            second = engine.submit_async(lambda: "second")
            assert await asyncio.to_thread(started.wait, 5)

            shutdown_task = asyncio.create_task(asyncio.to_thread(engine.shutdown))
            release.set()
            await asyncio.wait_for(shutdown_task, 5)
            shutdown = True

            assert await asyncio.wait_for(first, 5) == "first"
            assert await asyncio.wait_for(second, 5) == "second"
        finally:
            release.set()
            if not shutdown:
                engine.shutdown()

    asyncio.run(main())


def test_submit_async_completes_future_on_event_loop_thread():
    async def main():
        loop_thread = threading.get_ident()
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)
            observed_threads = []
            set_result = future.set_result

            def checked_set_result(value):
                observed_threads.append(threading.get_ident())
                set_result(value)

            future.set_result = checked_set_result

            assert await future == 42
            assert observed_threads == [loop_thread]
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_engine_context_manager_shuts_down_on_exit():
    with Engine(worker_count=1, queue_capacity=4) as engine:
        assert engine.submit(lambda: 42).wait() == 42

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)

    assert engine.shutdown() is None


def test_engine_context_manager_propagates_block_exception_and_shuts_down():
    class BlockError(Exception):
        pass

    with pytest.raises(BlockError, match="block failed"):
        with Engine(worker_count=1, queue_capacity=4) as engine:
            handle = engine.submit(lambda: "accepted")
            raise BlockError("block failed")

    assert handle.wait() == "accepted"

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)


def test_engine_context_manager_ignores_duplicate_shutdown_on_clean_exit():
    engine = Engine(worker_count=1, queue_capacity=4)

    with engine as entered:
        assert entered is engine
        assert entered.submit(lambda: 42).wait() == 42
        engine.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)


def test_engine_context_manager_preserves_block_exception_after_duplicate_close():
    class BlockError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=4)

    with pytest.raises(BlockError, match="block failed"):
        with engine:
            engine.close()
            raise BlockError("block failed")

    assert engine.closed
    assert engine.state is EngineState.CLOSED


def test_submit_forwards_positional_arguments():
    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        assert engine.submit(pow, 2, 3).wait() == 8
        assert engine.submit(lambda left, right: left + right, 4, 5).wait() == 9
        assert engine.submit(lambda: 42).wait() == 42
        assert engine.submit(lambda value=3: value * 2, value=4).wait() == 8
    finally:
        engine.close()


def test_submit_forwards_keyword_only_and_mixed_arguments():
    def describe(prefix, value, *, suffix="", repeat=1):
        return (prefix + value + suffix) * repeat

    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        empty_kwargs: dict[str, object] = {}
        assert engine.submit(describe, "a", "b").wait() == "ab"
        assert engine.submit(describe, "a", "b", suffix="c").wait() == "abc"
        assert engine.submit(describe, "a", "b", repeat=2).wait() == "abab"
        assert engine.submit(describe, "a", "b", **empty_kwargs).wait() == "ab"
    finally:
        engine.close()


def test_submit_supports_bound_methods_callable_objects_and_partial():
    class Calculator:
        def __init__(self, base):
            self.base = base

        def scale(self, value, *, factor=1):
            return (value + self.base) * factor

        def __call__(self, value, *, factor=1):
            return (value - self.base) * factor

    def combine(left, right, *, extra=0):
        return left + right + extra

    calculator = Calculator(3)
    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        assert engine.submit(calculator.scale, 4, factor=2).wait() == 14
        assert engine.submit(calculator, 10, factor=2).wait() == 14
        assert engine.submit(functools.partial(combine, 2, extra=5), 7).wait() == 14
        assert engine.submit(callable=lambda: 21).wait() == 21
    finally:
        engine.close()


def test_submit_argument_binding_errors_are_task_failures():
    def takes_one(value):
        return value

    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        too_many = engine.submit(takes_one, 1, 2)
        with pytest.raises(TypeError):
            too_many.wait()
        assert isinstance(too_many.exception(), TypeError)
        assert too_many.status == "failed"

        unexpected = engine.submit(takes_one, 1, unexpected=True)
        with pytest.raises(TypeError):
            unexpected.wait()
        assert unexpected.status == "failed"
    finally:
        engine.close()


def test_submit_with_arguments_preserves_callback_exception_identity():
    class CallbackError(Exception):
        pass

    error = CallbackError("argument callback failed")

    def fail(value):
        raise error

    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        handle = engine.submit(fail, 1)

        with pytest.raises(CallbackError, match="argument callback failed") as caught:
            handle.wait()

        assert caught.value is error
        assert handle.exception() is error
    finally:
        engine.close()


def test_submit_async_forwards_positional_keyword_and_mixed_arguments():
    def describe(prefix, value, *, suffix=""):
        return prefix + value + suffix

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            positional = engine.submit_async(pow, 2, 5)
            assert isinstance(positional, asyncio.Future)
            assert await positional == 32

            keyword = engine.submit_async(pow, base=2, exp=5)
            assert await keyword == 32

            mixed = engine.submit_async(describe, "a", "b", suffix="c")
            assert await mixed == "abc"

            empty_kwargs: dict[str, object] = {}
            no_kwargs = engine.submit_async(describe, "a", "b", **empty_kwargs)
            assert await no_kwargs == "ab"
        finally:
            engine.close()

    asyncio.run(main())


def test_submit_async_argument_binding_errors_are_future_failures():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            too_few = engine.submit_async(pow, 2)
            with pytest.raises(TypeError):
                await too_few

            unexpected = engine.submit_async(pow, 2, 3, unexpected=True)
            with pytest.raises(TypeError):
                await unexpected
        finally:
            engine.close()

    asyncio.run(main())


def test_submit_async_with_arguments_preserves_callback_exception_identity():
    class CallbackError(Exception):
        pass

    error = CallbackError("async argument callback failed")

    def fail(value):
        raise error

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(fail, 1)

            with pytest.raises(
                CallbackError, match="async argument callback failed"
            ) as caught:
                await future

            assert caught.value is error
        finally:
            engine.close()

    asyncio.run(main())


def test_submit_async_argument_forwarding_keeps_coroutine_checks():
    async def coroutine_function(value):
        return value

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            with pytest.raises(TypeError, match="synchronous callable"):
                engine.submit_async(coroutine_function, 1)

            future = engine.submit_async(lambda value: coroutine_function(value), 1)
            with pytest.raises(TypeError, match="returned an awaitable"):
                await future
        finally:
            engine.close()

    asyncio.run(main())


def test_queue_full_rejection_releases_callable_and_arguments():
    class TrackedCallable:
        def __call__(self, argument):
            return argument

    class TrackedArgument:
        pass

    engine = Engine(worker_count=1, queue_capacity=1)

    def blocker():
        started.set()
        assert release.wait(5)

    started = threading.Event()
    release = threading.Event()
    try:
        first = engine.submit(blocker)
        assert started.wait(5)
        second = engine.submit(lambda: None)

        callable_object = TrackedCallable()
        argument = TrackedArgument()
        callable_ref = weakref.ref(callable_object)
        argument_ref = weakref.ref(argument)

        with pytest.raises(QueueFullError):
            engine.submit(callable_object, argument)

        del callable_object, argument
        gc.collect()
        assert callable_ref() is None
        assert argument_ref() is None
    finally:
        release.set()
        engine.close()

    assert first.wait() is None
    assert second.wait() is None


def test_queued_cancellation_releases_callable_and_arguments():
    class TrackedCallable:
        def __call__(self, argument):
            return argument

    class TrackedArgument:
        pass

    def blocker_task():
        started.set()
        assert release.wait(5)

    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()
    try:
        blocker = engine.submit(blocker_task)
        assert started.wait(5)

        callable_object = TrackedCallable()
        argument = TrackedArgument()
        callable_ref = weakref.ref(callable_object)
        argument_ref = weakref.ref(argument)

        handle = engine.submit(callable_object, argument)
        assert handle.cancel()

        marker = engine.submit(lambda: None)
        release.set()

        assert marker.wait() is None
        assert blocker.wait() is None
        del callable_object, argument
        gc.collect()
        assert callable_ref() is None
        assert argument_ref() is None
    finally:
        release.set()
        engine.close()


def test_shutdown_drains_and_releases_arguments():
    class TrackedCallable:
        def __call__(self, argument):
            return None

    class TrackedArgument:
        pass

    def blocker_task():
        started.set()
        assert release.wait(5)

    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()
    errors = []
    try:
        blocker = engine.submit(blocker_task)
        assert started.wait(5)

        callable_object = TrackedCallable()
        argument = TrackedArgument()
        callable_ref = weakref.ref(callable_object)
        argument_ref = weakref.ref(argument)
        handle = engine.submit(callable_object, argument)

        def close():
            try:
                engine.close()
            except BaseException as error:
                errors.append(error)

        closer = threading.Thread(target=close)
        closer.start()
        deadline = time.monotonic() + 5
        while not engine.closed and time.monotonic() < deadline:
            time.sleep(0.001)
        assert engine.closed
        release.set()
        closer.join(5)

        assert not closer.is_alive()
        assert errors == []
        assert blocker.wait() is None
        assert handle.wait() is None
        del callable_object, argument
        gc.collect()
        assert callable_ref() is None
        assert argument_ref() is None
    finally:
        release.set()
        engine.close()


def test_submit_async_with_arguments_releases_references_after_completion():
    class TrackedCallable:
        def __call__(self, argument):
            return argument.value

    class TrackedArgument:
        def __init__(self, value):
            self.value = value

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            callable_object = TrackedCallable()
            argument = TrackedArgument(42)
            callable_ref = weakref.ref(callable_object)
            argument_ref = weakref.ref(argument)

            future = engine.submit_async(callable_object, argument)
            assert await future == 42

            del callable_object, argument
            gc.collect()
            assert callable_ref() is None
            assert argument_ref() is None
        finally:
            engine.close()

    asyncio.run(main())


def test_packaged_typing_metadata_is_available():
    package = files("fluxion")
    assert (package / "py.typed").is_file()
    assert (package / "__init__.pyi").is_file()
    assert (package / "errors.pyi").is_file()
    assert (package / "status.pyi").is_file()
    assert "TaskHandle" in repr(TaskHandle[int])


def test_exception_hierarchy_is_public_and_backward_compatible():
    assert issubclass(FluxionError, Exception)
    assert issubclass(EngineError, RuntimeError)
    assert issubclass(EngineClosedError, EngineError)
    assert issubclass(QueueFullError, EngineError)
    assert issubclass(ForkedProcessError, EngineError)
    assert issubclass(ForkedProcessError, RuntimeError)
    assert issubclass(TaskError, FluxionError)
    assert issubclass(TaskCancelledError, concurrent.futures.CancelledError)
    assert issubclass(TaskTimeoutError, TimeoutError)
    assert issubclass(TaskTimeoutError, concurrent.futures.TimeoutError)
    assert issubclass(TaskPanicError, RuntimeError)
    assert issubclass(WorkerDisconnectedError, RuntimeError)


def test_status_and_engine_state_are_typed_and_string_compatible():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        assert engine.state is EngineState.RUNNING
        assert not engine.closed

        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42
        assert handle.status is TaskStatus.COMPLETED
        assert handle.status == "completed"
        assert str(handle.status) == "completed"
        assert "completed" in repr(handle)
        assert "state='running'" in repr(engine)
    finally:
        engine.close()

    assert engine.state is EngineState.CLOSED
    assert engine.closed
    assert "state='closed'" in repr(engine)


def test_configuration_rejects_bools_non_integers_and_out_of_range_values():
    with pytest.raises(ValueError):
        Engine(worker_count=True)

    with pytest.raises(ValueError):
        Engine(queue_capacity=False)

    with pytest.raises(TypeError):
        Engine(worker_count="2")

    with pytest.raises(TypeError):
        Engine(queue_capacity=1.5)

    with pytest.raises(ValueError):
        Engine(worker_count=10**100)


def test_timeout_and_cancellation_errors_have_public_types():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return 42

    try:
        handle = engine.submit(blocking_task)
        assert started.wait(5)

        with pytest.raises(TaskTimeoutError) as caught:
            handle.wait(timeout=0.01)

        assert isinstance(caught.value, TimeoutError)
        assert isinstance(caught.value, concurrent.futures.TimeoutError)

        queued = engine.submit(lambda: 42)
        assert queued.cancel()
        with pytest.raises(TaskCancelledError) as caught:
            queued.wait()

        assert isinstance(caught.value, concurrent.futures.CancelledError)
    finally:
        release.set()
        engine.close()


def test_submit_async_rejects_coroutine_functions_and_awaitable_results():
    async def coroutine_function():
        return 1

    class AsyncCallable:
        async def __call__(self):
            return 1

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            with pytest.raises(TypeError, match="synchronous callable"):
                engine.submit_async(coroutine_function)

            with pytest.raises(TypeError, match="synchronous callable"):
                engine.submit_async(AsyncCallable())

            future = engine.submit_async(lambda: coroutine_function())
            with pytest.raises(TypeError, match="returned an awaitable"):
                await future

            assert await engine.submit_async(lambda: 42) == 42
        finally:
            engine.close()

    asyncio.run(main())


def test_submit_async_closes_rejected_coroutine_results():
    async def coroutine_function():
        return 1

    async def main():
        engine = Engine(worker_count=1, queue_capacity=4)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                future = engine.submit_async(lambda: coroutine_function())
                with pytest.raises(TypeError, match="returned an awaitable"):
                    await future
                del future
                gc.collect()

            assert not [
                warning
                for warning in caught
                if issubclass(warning.category, RuntimeWarning)
            ]
        finally:
            engine.close()

    asyncio.run(main())


def test_aclose_and_async_context_manager_do_not_block_the_event_loop():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        ticks = 0

        def blocking_task():
            started.set()
            assert release.wait(5)
            return "finished"

        handle = engine.submit(blocking_task)
        assert await asyncio.to_thread(started.wait, 5)

        async def heartbeat():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.005)
                ticks += 1

        heartbeat_task = asyncio.create_task(heartbeat())
        close_task = asyncio.create_task(engine.aclose())
        await asyncio.sleep(0.05)
        assert ticks > 0

        release.set()
        await asyncio.wait_for(close_task, 5)
        heartbeat_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat_task

        assert handle.wait() == "finished"
        engine.close()
        engine.shutdown()
        assert engine.closed

        async with Engine(worker_count=1, queue_capacity=4) as entered:
            assert entered.state is EngineState.RUNNING
            assert await entered.submit_async(lambda: 21 * 2) == 42
        assert entered.closed

        with pytest.raises(ValueError, match="block failed"):
            async with Engine(worker_count=1, queue_capacity=4) as entered:
                raise ValueError("block failed")
        assert entered.closed

    asyncio.run(main())


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
def test_engine_and_handle_reject_forked_child_process():
    result = run_isolated(
        """
        import os
        from fluxion import Engine, EngineState, ForkedProcessError

        engine = Engine(worker_count=1, queue_capacity=8)
        handle = engine.submit(lambda: 42)
        assert handle.wait() == 42

        pid = os.fork()
        if pid == 0:
            try:
                assert engine.closed
                assert engine.state is EngineState.CLOSED

                operations = [
                    lambda: engine.submit(lambda: 1),
                    lambda: handle.wait(0.1),
                    lambda: handle.status,
                    lambda: handle.cancel(),
                    lambda: engine.close(),
                    lambda: engine.shutdown(),
                ]
                for operation in operations:
                    try:
                        operation()
                    except ForkedProcessError:
                        pass
                    else:
                        raise AssertionError("forked operation did not raise")
            except BaseException:
                os._exit(1)
            os._exit(0)

        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        engine.close()
        """
    )

    assert result.returncode == 0, result.stderr
